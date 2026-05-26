# ADR-011: Real-spec ingestion fixtures and post-flush invariants

## Status

Accepted.

## Context

The OAS → graph ingestion pipeline (`oas_normalize.py` →
`oas_schema_graph.py`) shipped two latent defects that the existing
test suite — built entirely on hand-written synthetic OpenAPI snippets
— failed to catch:

1. **Stub-wins ref resolution** (`_follow_ref`): when both the local
   spec's components map and the provider-wide resolution pool defined
   the same schema name, the resolver returned the first non-`None`
   hit. In real Aruba Central bundles the local operation spec often
   stubs a component (`{"type": "object"}`) that a sibling spec
   declares richly. The stub won, and the rich definition was lost.

2. **In-batch eviction skip** (`_Batch.add_component`): when a richer
   body arrived for a component that was already queued in the
   *current* batch (not yet flushed), the code mutated the row in
   place but skipped descendant eviction. The previously-emitted
   `HAS_PROPERTY` / `COMPOSED_OF` dedup keys persisted, so the richer
   body's re-emitted property subgraph was silently swallowed.

Symptom: thousands of named `SchemaComponent` rows shipped with
non-empty object `bodyJson` but zero outgoing decomposition edges.
End users (and LLM consumers) could see the body shape but not walk
into the properties — the principal point of the schema graph.

The synthetic tests passed because each scenario was constructed in
isolation; they never exercised:

- two specs declaring the same component name in one ingestion run
  with different richness
- both ingest orders (stub → rich, rich → stub) sharing a single
  batch buffer
- the integrative shape of the populated graph as a whole

## Decision

### Tier 1 — Real-spec fixtures, not handwritten OAS, for ingestion smoke

`tests/fixtures/oas/real_excerpts/` contains verbatim excerpts of
production specs (Central Config + GreenLake audit-log) copied from
the live `spec_cache/`. New ingestion-shape regressions must be
exercised against this corpus via `tests/test_real_spec_ingest_smoke.py`,
which:

- runs each fixture through the full `collect_into_batch` + `flush_batch`
  pipeline, and
- runs every fixture together in a single shared batch so cross-spec
  ref resolution and richest-wins merging are exercised end-to-end.

Hand-written synthetic specs are retained ONLY for pinning specific
orthogonal code paths (UnresolvedRefPlaceholder, InlineUnionPromotion,
AdditionalPropertiesMap, InheritedFromChain, YangPathIndex, BodyShape).
They MUST NOT be used to assert ingestion correctness on shapes that
also exist in the real corpus — the real corpus is the source of
truth.

### Tier 2 — Post-flush invariants gate

A new module `src/hpe_networking_central_mcp/graph/invariants.py`
defines four invariants that every populated knowledge DB must
satisfy:

| ID | Invariant |
|---|---|
| INV-1 | Every named, non-primitive `SchemaComponent` with an object/union/map body has ≥1 `HAS_PROPERTY`, `COMPOSED_OF`, or `HAS_VALUE_SCHEMA` edge. |
| INV-2 | `kind = 'primitive'` rows never carry object/union bodies. |
| INV-3 | `component_id` is globally unique (no duplicate PK rows). |
| INV-4 | Every `Property.inheritedFromChain` entry resolves to an existing `SchemaComponent` name. |

Invariants run in three contexts:

1. **`build_knowledge_db.py`** — always after `[3b/6]`; failures are
   warnings by default, hard errors under `--strict` (recommended in CI).
2. **`tests/test_real_spec_ingest_smoke.py`** — assertion-grade; any
   violation fails the test.
3. **Per-bug regression tests** in `tests/test_schema_overhaul.py`
   (`TestFollowRefRichnessAcrossScopes`, `TestInBatchReplacementEvictsDescendants`)
   — drive the failure modes through the production code paths and
   assert the resulting graph shape directly.

### Tier 3 — Shared richness scorer

Both `_follow_ref` (cross-scope ref resolution) and
`_Batch._compute_richness` (in-batch merge) now consult the same
metric: the serialised-payload length of the candidate node. Lifted
into `oas_normalize.schema_richness()` so future changes cannot drift.

## Rationale

- **Why not delete the synthetic suite outright?** Each retained file
  pins a specific, orthogonal behaviour (e.g. YANG-path indexing,
  `additionalProperties` → map). Those behaviours are nailed by code
  paths that don't depend on cross-spec ordering or merge semantics,
  so unit-level synthetic fixtures remain the cheapest way to lock
  them in. The mandate "real specs for ingestion correctness" is
  satisfied by adding `test_real_spec_ingest_smoke.py` *and* the
  invariants gate, which together catch any future stub-wins or
  eviction-skip regression class regardless of the surface shape.

- **Why a separate invariants module?** The invariants are read-only
  Cypher queries against the populated graph. They are reusable from
  CI, ad-hoc audits, and tests; coupling them to the ingestion code
  would tangle write and audit concerns.

- **Why `--strict` opt-in for local builds?** A full local rebuild
  takes ~25 minutes. Allowing the warning-only mode locally surfaces
  violations early in iterative work without failing the artifact;
  CI promotes them to errors.

## Consequences

- **Positive:** future ingestion changes that lose properties or
  duplicate components are caught in <5s by the smoke test, hours
  before a full rebuild would notice. ADR-007 and ADR-008
  (skeleton/glossary, schema-as-graph) get a real-spec coverage floor
  they previously lacked. ADR-010 (graph-only discovery) is bolstered:
  consumers can trust the graph rather than re-deriving from raw OAS.

- **Negative:** the fixture corpus must be refreshed when the upstream
  specs change shape. A short note in `docs/DEVELOPMENT.md` covers
  the refresh procedure (copy from `build/spec_cache/`, no
  hand-editing). New invariants tighten what is "valid"; adding one
  may require a one-time fixup of historical seeds.

- **Operational:** `build_knowledge_db.py` gains `--strict` and
  `--no-invariants` flags. CI must pass `--strict`.

## References

- ADR-007 — Skeleton/glossary split
- ADR-008 — Schema-as-graph
- ADR-010 — Graph-only discovery & validation
