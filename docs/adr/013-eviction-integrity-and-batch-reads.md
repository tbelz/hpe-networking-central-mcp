# ADR-013: Eviction integrity, INV-8, FTS over properties, and batched reads

## Status

Accepted.

## Context

While running `scripts/build_knowledge_db.py` against the real Central
+ GLP specs, four shortcomings surfaced that the per-test-spec coverage
in the populator suite was missing:

1. **Orphaned `Property` nodes after richest-wins replacement.** ADR-011
   established that when the same `SchemaComponent` appears in two
   specs we keep the richer one and evict the lean one. The eviction
   filter in `_evict_component_descendants` only matched
   `{component_id}#prop:*` property ids — direct fields of the evicted
   component itself. It did **not** match the inline-allOf descendants
   (`{component_id}#allOf:N#prop:*`) that share the same root.

   At rebuild time those inline child SchemaComponents got DETACH-DELETEd
   along with the evicted root, but their property ids were still in the
   in-process `_seen_*` dedup set and were therefore never re-emitted by
   the richer rebuild. Worse, the parent `_seen_has_property` /
   `_seen_has_value_schema` edge sets still contained the (parent, child)
   pairs whose source SchemaComponent rows had been wiped. The next
   batch `COPY (..., ignore_errors=true)` silently dropped the orphaned
   edge rows, leaving ~42 % of the `Property` table unreachable from any
   `SchemaComponent`.

   The unit tests passed because each test populates one spec and never
   exercises richest-wins replacement.

2. **No post-COPY validation that all `Property` rows are reachable.**
   The invariants framework added in ADR-011 covered emptiness and
   cardinality of several tables but did not check structural
   reachability of properties through `HAS_PROPERTY`. The orphan bug
   above could therefore be merged again unnoticed.

3. **No free-text discovery over `Property`.** `api_fts` indexes
   endpoint metadata; `doc_fts` and `script_fts` cover their domains;
   but a query like "ntp server" or "vrf binding" — phrases that live
   on `Property.description` or `Property.yangPath`, not on endpoint
   summary — had to be answered with brittle `CONTAINS` predicates that
   miss BM25 ranking and tokenisation.

4. **N reads = N round trips.** Discovery flows hop endpoint → required
   parameters → body schema → property list. Each hop is a separate
   tool invocation through the MCP stdio stream. `call_central_api`
   already supports a `calls=[...]` batch shape; the read tools did
   not.

A secondary documentation gap appeared while diagnosing (1): the
canonical `component_id` shape `<provider>:<section>:<Name>` is not
written down anywhere except in the populator. Agents kept guessing
the wrong id for `get_raw_schema` and hitting `ToolError`.

## Decision

### 1. Eviction subsumes inline-descendant ids

`_evict_component_descendants` now uses the single `inline_prefix`
(`{component_id}#`) when filtering `stale_props`, `_seen_has_property`,
and `_seen_has_value_schema`. The prefix subsumes both
`{cid}#prop:*` (direct properties of the evicted component) and
`{cid}#allOf:N#prop:*` (properties of every inline child that shares
the same root). The previous narrower `prop_prefix` was removed.

A regression test
`TestEvictionPreservesGraphIntegrity.test_richer_inline_branches_are_reachable`
in `tests/test_schema_property_graph.py` populates a richer spec on top
of a lean one and asserts that every property of the richer inline
branches is reachable through `HAS_PROPERTY`. Without the fix it fails
with a missing `id` field; with the fix it passes.

### 2. INV-8: no orphaned `Property` nodes

`graph/invariants.py` adds `check_no_orphaned_properties`, registered
as the 7th entry in `_CHECKS`. It runs:

```cypher
MATCH (p:Property)
WHERE NOT EXISTS { MATCH (:SchemaComponent)-[:HAS_PROPERTY]->(p) }
RETURN p.property_id, p.name LIMIT 25
```

plus a `COUNT(p)` for the exact total. Any orphans surface as an
`InvariantViolation('no_orphaned_properties', ...)`. The check fires
automatically from every existing caller of `assert_graph_invariants`:
the `[3d/6] Auditing graph invariants` step of `build_knowledge_db.py`
and `tests/test_real_spec_ingest_smoke.py`.

### 3. `property_fts` index over `Property(name, description, yangPath)`

`_create_fts_indexes` in `scripts/build_knowledge_db.py` adds:

```python
("property_fts", "Property", ["name", "description", "yangPath"]),
```

`enumValues` is intentionally excluded: the underlying Kuzu FTS only
indexes scalar `STRING` columns, and enum membership is already cheap
via `$val IN p.enumValues` in plain Cypher.

`query_fts`'s docstring now documents the index in the available-
indexes table and ships a canonical recipe:

```cypher
CALL QUERY_FTS_INDEX('Property', 'property_fts', 'ntp server')
YIELD node AS p, score
MATCH (c:SchemaComponent)-[:HAS_PROPERTY]->(p)
OPTIONAL MATCH (e:ApiEndpoint)
         -[:HAS_REQUEST_BODY|:HAS_RESPONSE]->()
         -[:BODY_REFERENCES|:RESPONSE_REFERENCES]->(root:SchemaComponent)
         -[:COMPOSED_OF*0..5]->(c)
RETURN p.name, p.type, c.name AS declaredOn,
       e.method, e.path, score
ORDER BY score DESC LIMIT 25
```

This hops from a free-text hit on a Property straight to the API
endpoints that touch its owning component, in one query.

### 4. Optional `queries: list[dict]` on every read tool

Every `query_*` tool registered by `register_graph_tools`
(`query_graph`, `query_api_schema`, `query_fts`, `query_topology`,
`query_yang`) gains an optional `queries` parameter. Each item is a
`{cypher, parameters?, label?}` dict. The shared `_run_batch` helper:

- caps batch length via `MCP_GRAPH_BATCH_MAX_ITEMS` (default 25),
- runs items sequentially, continue-on-error,
- wraps each item in a `{ok, label, result|error}` envelope,
- delegates per-item execution to `_run_query` (so row + per-cell +
  per-response caps still apply per item),
- applies a top-level `MCP_GRAPH_BATCH_RESPONSE_BYTES` cap (default
  200 KB) by dropping trailing items and setting
  `truncated: true, kept_items: N`,
- returns `{batch: true, total, ok, failed, results: [...]}`.

When `queries` is set the single-call `cypher` / `parameters`
arguments are ignored, mirroring `call_central_api(calls=...)`.

### 5. `component_id` convention is documented

`get_raw_schema`'s docstring, its not-found `ToolError`, and the
`SchemaComponent` entry in `graph://schema` all spell out the
canonical id shape:

```
<provider>:<section>:<Name>          e.g. central:schemas:VlanInterface
<provider>:<section>:<Name>#allOf:N   inline allOf branch
<provider>:<section>:<Name>#item      inline array item
```

…plus the recommended name-lookup query
(`MATCH (c:SchemaComponent {name: $n}) RETURN c.component_id`).

## Consequences

- The orphan bug is fixed, has a regression test, and any future
  recurrence is caught structurally by INV-8.
- Agents can free-text search the property graph (`property_fts`) and
  hop directly to the endpoints that consume the matched fields.
- Discovery flows that needed 3–5 reads can be a single batch tool
  call. Per-item caps still bound individual responses; the aggregate
  cap stops a runaway batch from blowing the context budget.
- `get_raw_schema` failures stop being a guessing game — the format is
  documented in the tool, the schema resource, and the error message.

## Out of scope

- No write-side batching (`write_graph` keeps its single-statement
  shape). Writes are rarer and the audit story benefits from one
  Cypher per call.
- No FTS over `Property.enumValues` (Kuzu limitation). Enum hits
  remain `IN`-list checks in Cypher.
- The `size(supportedDeviceTypes) = 0` safety net introduced in
  ADR-012 stays for one more release.
