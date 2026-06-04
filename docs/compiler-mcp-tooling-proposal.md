# Compiler-Backed MCP Tooling Proposal

Status: proposal, not implemented in the live MCP runtime.

This document sketches the tool surface that should sit on top of the
ADR-011 compiler pipeline once the compiler artifacts are ready to become
agent-facing. It intentionally does not change `tools/graph.py`, live tool
descriptions, schema versioning, or runtime database loading.

## Current State

The compiler path now builds three separate artifacts:

- `knowledge_db_ast`: lossless L1 OpenAPI AST plus L2 semantic overlay.
- `knowledge_db_compiler`: typed compiler projection shaped like the
  current API graph, with `CompilerProjectionMap` provenance rows.
- `knowledge_db`: the live legacy runtime graph, still used by MCP tools.

The important architectural point is that `knowledge_db_compiler` is a
traversal index, not the source of truth. Every projected row can be traced
back through provenance to L2 semantic nodes and L1 raw OpenAPI JSON.
If a future agent needs an OpenAPI field that was not projected, it should
ask for source detail through that provenance path instead of requiring a
new projection column.

Selected compiler-projection columns exist for frequent, deterministic
queries; they are not a completeness boundary. Constraint-bearing schema
and property rows also carry a generic `constraintsJson` payload, while
provenance remains the escape hatch for every source attribute and vendor
extension. Adding a convenience column must never be required merely to
prevent source-data loss.

Recently added compiler-only readers already prove this shape:

- `detail_reader.py`: resolves a typed projection row to projection data,
  provenance, optional semantic summary, and raw L1 OpenAPI JSON.
- `traversal_reader.py`: returns endpoint-centered and schema-centered
  contexts from the compiler projection, including provenance detail.
- `traversal_report.py` and `scripts/report_compiler_traversal.py`: sample
  persisted compiler artifacts and report traversal success/failures.

## Design Goal

Move agents from open-ended Cypher-first API discovery toward deterministic
compiler primitives, while keeping Cypher as an expert escape hatch.

The desired pattern is:

1. The agent translates intent into a small primitive call.
2. The server performs deterministic graph traversal over compiler artifacts.
3. The server returns compact semantic context plus provenance-backed raw
   OpenAPI detail when needed.
4. The agent uses Cypher only for unusual cross-cutting analysis, not for
   routine endpoint/schema discovery.

This keeps the useful parts of the current graph surface while avoiding the
old failure mode where a hand-maintained projection silently lost source
attributes.

Raw payload safety matters: the current compiler traversal loaders default
to `include_raw=True` because they are internal Python helpers. Future MCP
wrappers MUST pass `include_raw` explicitly to those loaders and must not
rely on loader defaults. Agent-facing tools should default to
`include_raw=false` unless their whole purpose is source-detail retrieval.

## Proposed Future MCP Primitives

These names are placeholders. The important contract is the shape and
responsibility of each primitive.

### `find_api_endpoints`

Purpose: locate candidate endpoints without requiring the agent to write
Cypher.

Inputs:

- `query`: optional keyword text.
- `method`: optional HTTP method.
- `path_contains`: optional path substring.
- `operation_id`: optional operationId.
- `limit`: default bounded result size.

Returns:

- endpoint IDs, method, path, summary, operationId, tags/category when
  available.
- enough fields to choose a candidate endpoint.

Implementation path:

- Initially backed by `knowledge_db_compiler` `ApiEndpoint` rows.
- Can later add FTS/BM25 ranking, but that should be deterministic and
  bounded.

### `get_api_endpoint_context`

Purpose: the primary replacement for most current `query_api_schema`
endpoint-walk recipes.

Inputs:

- `method`
- `path`
- `include_raw`: default `false`

Returns:

- endpoint projection row.
- parameters and their schema detail when linked.
- request bodies and referenced root schemas.
- responses and referenced root schemas.
- YANG links and CLI command links when present.
- provenance metadata for every returned entity.
- raw OpenAPI JSON only when requested.

Implementation path:

- Wrap `traversal_reader.load_endpoint_context`.
- Wrappers MUST pass `include_raw` explicitly to the loader and must not
  rely on the traversal loader default.
- Keep response caps similar to existing graph tools.
- If multiple compiler endpoints match, fail with an explicit ambiguity
  error rather than choosing one.

### `get_api_schema_context`

Purpose: schema-centric traversal without asking the agent to remember
`COMPOSED_OF`, `HAS_VALUE_SCHEMA`, `PROPERTY_OF_TYPE`, and `REFERENCES`
recipes.

Inputs:

- `component_id`
- `include_raw`: default `false`
- optional depth controls in a later iteration.

Returns:

- schema projection row.
- direct properties.
- property target schemas.
- array item schemas.
- composition branches.
- value schemas for arrays/maps.
- direct schema references.
- provenance metadata for every returned entity.

Implementation path:

- Wrap `traversal_reader.load_schema_context`.
- Wrappers MUST pass `include_raw` explicitly to the loader and must not
  rely on the traversal loader default.
- Add depth expansion only after single-hop behavior is stable and measured.

### `get_openapi_source_detail`

Purpose: the lossless escape hatch that prevents the new projection from
repeating the old lossy-projection problem.

Inputs:

- `table_name`
- `row_id`

Returns:

- typed projection row.
- `CompilerProjectionMap` provenance.
- L2 semantic node summary when one exists.
- L1 AST node metadata.
- raw OpenAPI object/scalar JSON.

Implementation path:

- Wrap `detail_reader.load_projection_detail`.
- Validate `table_name` against the allowed compiler projection tables.
- Keep this read-only and bounded; do not expose arbitrary L1 traversal
  as the first agent-facing primitive.
- This primitive intentionally returns source detail; it should still cap
  output and should not be substituted for default endpoint/schema context.

### `get_compiler_graph_health`

Purpose: expose build/report quality without requiring manual artifact
inspection.

Inputs:

- sample limits, or `full=true` only in local/admin contexts.

Returns:

- counts for compiler endpoints/schemas/parameters/bodies/responses.
- endpoint traversal success ratio.
- schema traversal success ratio.
- failure samples.
- carry-through metadata from the build manifest.

Implementation path:

- Wrap `traversal_report.load_compiler_traversal_report`.
- Health/report wrappers should request compact traversal contexts and
  should not include raw OpenAPI payloads in normal agent sessions.
- In normal agent sessions, return manifest/report summaries rather than
  running expensive full-corpus scans on demand.

## Tool Description Changes Later

Do not change live tool descriptions until the server can actually open the
compiler artifacts.

When that is true, update the descriptions in this direction:

- `query_api_schema`: describe it as an expert Cypher fallback. The first
  recommendation should become `find_api_endpoints`,
  `get_api_endpoint_context`, and `get_api_schema_context`.
- `query_graph`: keep as the cross-domain escape hatch, but do not teach
  routine API schema discovery primarily through Cypher examples.
- `get_raw_schema`: either keep for legacy `knowledge_db` or deprecate in
  favor of `get_openapi_source_detail` once compiler artifacts are live.
- `query_yang`: keep for explicit YANG graph analysis, but endpoint-to-YANG
  lookup should also be visible through endpoint context.

Suggested wording principle:

> Use deterministic compiler context tools for normal API discovery. Use
> Cypher only when you need custom graph analysis that the context tools do
> not yet provide.

## Migration Plan

This should be staged to stop short of changing agent behavior until all
evidence is available.

1. Keep building `knowledge_db_ast` and `knowledge_db_compiler` by default.
2. Keep running parity reporting between legacy and compiler projection.
3. Run `scripts/report_compiler_traversal.py` against hydrated/full build
   artifacts and record failure counts in PRs or release logs.
4. Add server-side ability to download/open compiler artifacts alongside
   the runtime DB, behind an environment flag. Do not expose tools yet.
5. Add the proposed MCP primitives behind an opt-in flag.
6. Run dark-read or local MCP sessions comparing current Cypher workflows
   with compiler primitive workflows.
7. Only after parity, traversal health, and manual smoke sessions are
   satisfactory, flip the runtime default and update tool descriptions.

## Cutover Gates

Before changing the live agent default, require:

- Compiler projection parity covers legacy API graph signatures for the
  relevant corpus.
- Compiler traversal report has zero failures on the full hydrated Central
  corpus, or any failures are explicitly triaged and accepted.
- The future primitives pass unit tests over synthetic specs and at least
  one real-spec smoke path.
- `get_openapi_source_detail` can recover source fields/extensions that are
  not projected into typed columns.
- Existing live MCP tools still pass their current tests.
- A manual agent session verifies endpoint discovery, request body
  traversal, response traversal, and raw OpenAPI detail retrieval.

## Non-Goals

- Do not expose raw L1 AST traversal directly as the default agent surface.
- Do not remove `query_graph` or focused Cypher aliases.
- Do not change `_KNOWLEDGE_SCHEMA_VERSION` before runtime cutover.
- Do not delete `oas_schema_graph.py` until the compiler path is the default
  and has passed the release-window criteria.
- Do not keep adding projection columns for every discovered OpenAPI field;
  prefer provenance-backed source detail unless the field is needed for
  frequent traversal/filtering.

## Open Questions

- Should compiler primitives be separate MCP tools, or should they be
  exposed as resources plus one generic `get_compiler_context` tool?
- Should `include_raw=true` be available to ordinary agents, or reserved for
  explicit detail calls to control response size?
- Should full traversal health be computed during build and written into
  `manifest.json`, or kept as an opt-in report to avoid slowing releases?
- How long should legacy and compiler runtime artifacts be downloaded side
  by side before flipping the default?
