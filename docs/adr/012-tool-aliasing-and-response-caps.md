# ADR-012: Tool aliasing and response caps for graph queries

## Status

Accepted.

## Context

The graph query surface had three usability problems for LLM clients:

1. **Discoverability collapse**. A single `query_graph` tool carried
   every canned Cypher pattern (API discovery, FTS, topology, YANG) in
   one ~3 KB docstring. Some MCP clients truncate descriptions in their
   tool catalog; the canonical traversal patterns disappeared from
   context exactly when an agent was deciding which tool to call.

2. **Unbounded `bodyJson` returns**. `SchemaComponent.bodyJson` stores
   the full serialised OpenAPI component. A naive
   `MATCH (c:SchemaComponent) RETURN c.bodyJson LIMIT 5` returned
   hundreds of KB of JSON and blew the agent's context budget before
   any analysis happened. The 200/2000 row caps did not help — a
   single large cell was the problem.

3. **NULL vs empty list ambiguity for `supportedDeviceTypes`**. The
   populator emitted `[]` both when the property had no
   `x-supportedDeviceType` extension (meaning "applies to all device
   types") and when the extension was explicitly an empty list. A
   `WHERE $dt IN sdt OR size(sdt)=0` filter therefore included the
   "no info" case correctly only by accident.

## Decision

### 1. Five `query_*` tool aliases sharing one executor

`tools/graph.py` exposes five MCP tools that all delegate to one
private `_run_query(cypher, parameters, tool_label)` helper:

| Tool | Focus |
|------|-------|
| `query_graph` | Generic escape hatch (one-line intent, pointer to the four aliases) |
| `query_api_schema` | OpenAPI walk: `COMPOSED_OF*0..5 → HAS_PROPERTY`, `PROPERTY_OF_TYPE`, device-type filter |
| `query_fts` | `CALL QUERY_FTS_INDEX('table','index','query') YIELD node, score` |
| `query_topology` | Org / SiteCollection / Site / Device hierarchy, `CONNECTED_TO`, `LINKED_TO` |
| `query_yang` | `YangPath`, `PROPERTY_AT_YANG`, `CONFIGURES_YANG` |

Each docstring stays under 4000 characters so the per-tool description
budget of every MCP client we care about can hold it. All five are
annotated `readOnlyHint=True, idempotentHint=True`. Writes go through
`write_graph`.

The aliases are not new behaviour — every alias accepts the same
arbitrary Cypher as `query_graph`. They are documentation routing,
nothing more.

### 2. Per-cell and per-response byte caps

`_run_query` enforces two caps after the existing 200/2000 row caps:

- `MCP_GRAPH_PER_CELL_BYTES` (default 4096): any string cell longer
  than this is replaced in place with
  `{"_truncated": true, "preview": "<first 200 chars>", "size_bytes": N, "hint": "use get_raw_schema(component_id) for raw JSON; prefer COMPOSED_OF traversal"}`.
- `MCP_GRAPH_PER_RESPONSE_BYTES` (default 50000): after row capping and
  cell capping, total `len(json.dumps(...))` is measured. Over-cap
  responses come back as
  `{"truncated": true, "reason": "response_byte_cap", "cap_bytes": ..., "total_bytes": ..., "warning": "...", "rows": [...first K that fit...]}`.

The cap defaults were picked to fit comfortably in a 200 KB context
window with several tool turns of headroom.

### 3. `get_raw_schema(component_id)` escape hatch

Added a single small tool that the per-cell envelope's `hint` points
to. Returns the raw `bodyJson` for one `SchemaComponent`, capped at
`MCP_GRAPH_RAW_SCHEMA_MAX_BYTES` (default 200000). Above the cap the
tool refuses with a hint to walk
`(:SchemaComponent {component_id: $id})-[:COMPOSED_OF*0..5]->()-[:HAS_PROPERTY]->()`
instead.

### 4. `supportedDeviceTypes` is `NULL` when unannotated

The populator now emits `None` (SQL `NULL`) for `Property.supportedDeviceTypes`
when the source schema carries no `x-supportedDeviceType` vendor
extension. `[]` is reserved for "explicitly restricted to no device
types" (an inconsistency that should not appear in healthy specs).
The canonical filter in `get_schema_description` and the
`api_call_validation` tool description is:

```cypher
WHERE $deviceType = ''
   OR p.supportedDeviceTypes IS NULL
   OR size(p.supportedDeviceTypes) = 0
   OR $deviceType IN p.supportedDeviceTypes
```

The `size(...) = 0` clause is a one-release safety net for graphs
built against the pre-ADR-012 populator. Implementation details:
`_rows_to_pa` preserves `None` for the `supportedDeviceTypes` column
only; other list columns still coerce `None` to `[]` for backwards
compatibility.

### 5. Structured warnings for silent populator gaps

Two previously-silent failure modes in `oas_schema_graph.py` now emit
structured warnings:

- `schemacomponent_detach_delete_failed` (was a bare
  `except Exception: pass` around the in-batch DETACH DELETE flush).
- `requestbody_without_schema_root` (was a silent empty `root_ref`
  that produced a `RequestBody` with no `BODY_REFERENCES` edge).

Both warnings preserve the prior best-effort behaviour — they only
add observability.

## Consequences

- LLM clients see five focused tools in their catalog with short,
  targeted docstrings instead of one mega-tool.
- Returns that include `c.bodyJson` no longer crash the agent's
  context. The truncation envelope teaches agents to switch to
  property traversal or `get_raw_schema`.
- `query_*` tools are functionally interchangeable. Client routing is
  the agent's choice; there is no enforcement.
- The pre-ADR-012 `[]` graph still works because the canonical filter
  keeps `size(...) = 0`. We can drop that clause in a future release.
- Operators can now grep production logs for
  `requestbody_without_schema_root` to find malformed specs and for
  `schemacomponent_detach_delete_failed` to find DB-state regressions.

## Alternatives considered

- **Narrow per-domain tools that hide Cypher.** Rejected: every
  consumer we have needs the full Cypher surface eventually, and the
  cost of maintaining a hand-written API layer over the graph is high.
- **Removing `bodyJson` from the column projection.** Rejected:
  occasionally an agent legitimately needs the full body (e.g. to
  inspect `discriminator`); cap + escape hatch preserves that
  capability without the default footgun.
- **Migrating `[]` → `NULL` at read time in the query layer.**
  Rejected: the read layer cannot know whether `[]` means "old
  populator" or "explicit empty"; only the populator can preserve the
  distinction. Fixing it at write time is the right place.
