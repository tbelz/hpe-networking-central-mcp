# ADR 007 — Skeleton + Glossary as the API endpoint detail surface

## Status

Accepted (2025-XX-XX, supersedes the four-view detail surface introduced
informally in 2024).

## Context

`get_api_endpoint_detail` previously exposed four `view` modes —
`compact`, `request-only`, `full`, and `raw` — each producing a different
projection of an OpenAPI operation:

* `compact` (default): trimmed structure + first-error `$ref` +
  `$components` side-table, soft 15 KB budget.
* `request-only`: just the request body schema + flat `required_paths`.
* `full`: every `$ref` resolved inline. Frequently >100 KB on heavy
  endpoints.
* `raw`: the operation as it appears in the spec, including provenance
  fields.

In practice the four-view surface had three problems:

1. **The agent had to choose a view before reading the data**, but the
   right choice depended on the answer. We frequently saw the agent fall
   back to `raw` "just to be safe", paying the full prose cost.
2. **The expensive payload is human-readable prose**, not structure.
   `description`, `title`, `example`, and `x-typeName` fields dominate
   the byte budget on Aruba's specs (often 70–85% of the bytes for
   nested config schemas), but the agent rarely needs them — it can
   usually map a value onto a field from the field name + type + enum
   alone.
3. **Two views diverged on shape** (`compact` carried `$components`,
   `full` did not), forcing per-view prompt logic.

## Decision

Replace the four views with **two complementary tools** that share a
single `(method, path)` / `endpoints=[...]` argument shape:

* `get_api_endpoint_detail(method, path)` — returns the full
  **structural skeleton**: parameters, request body schema, success and
  first-error response shapes, transitive `$components` side-table.
  All description-bearing keys (`description`, `title`, `example`,
  `examples`, `x-typeName`, `x-typeDescription`, `x-patternSources`,
  `summary` inside nested schemas) are stripped. Refs are preserved so
  the side-table dedups effectively.
* `get_api_endpoint_glossary(method, path, components=[...])` — returns
  the human-readable prose only, organised per-component, with an
  optional `components` filter to limit the response further.

The intended workflow is:

1. Scan the embedded **API Endpoint Catalog** for the `METHOD /path`.
2. Call `get_api_endpoint_detail` to learn the shape.
3. *Only if a field name is ambiguous*, call `get_api_endpoint_glossary`
   for that endpoint (optionally filtered to the unclear components).

This is a hard break: the `view` parameter is removed entirely, the four
projection functions are deleted from `oas_normalize.py`, and two new
columns (`bodySkeletonJson`, `bodyGlossaryJson`) are added to the
`ApiEndpoint` table. The knowledge DB schema version is bumped to **3**
and the server refuses to start against an older snapshot
(`raise SystemExit`) — there is no silent fallback.

## Consequences

* **Default payload shrinks substantially.** On the Aruba Central spec
  the median `compact` payload was ~12 KB; the equivalent skeleton is
  closer to 2–4 KB because the prose is gone.
* **Glossary is opt-in.** Most agent workflows never call it. When they
  do, the per-component filter keeps the payload focused.
* **One canonical shape per tool.** No more per-view prompt branching.
* **Build-time cost.** Two JSON blobs are generated and stored per
  endpoint instead of one. Coverage gate stays at 90% on each
  projection.
* **No backwards compatibility.** Older snapshots are rejected at
  server start; the workflow rebuilds the DB on its next scheduled run.
