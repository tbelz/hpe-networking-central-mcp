# ADR 009 — Graph as the primary API-discovery surface

## Status

Proposed (2026-05-…). Builds on ADR 007 (skeleton + glossary) and
ADR 008 (lazy components index). Supersedes the “must inspect before
calling” session gate introduced informally alongside ADR 008.

## Context

After ADR 007 + ADR 008 the schema-side surface stabilised at three
endpoint-scoped tools — `get_api_endpoint_detail`, `get_api_endpoint_glossary`,
`get_schema_component` — backed by per-endpoint JSON blobs on the
`ApiEndpoint` graph node (`bodySkeletonJson`, `bodyGlossaryJson`,
`bodyComponentsJson`).

That factoring solved the size problem (max detail call ≈16 KB instead
of 425 KB) but left several frictions for the agent:

1. **Discovery is opaque.** The catalog is a flat path tree; finding
   “every endpoint that touches a `Vlan` schema” or “every endpoint
   below `network-config/v1alpha1/` that takes a `scopeId` parameter”
   requires loading many skeletons and reading them by hand.
2. **Three schema tools all do roughly the same thing.** They differ
   only in *which key of the same JSON blob* they return. The agent
   must remember when to call which.
3. **The session gate is fragile.** `check_call_policy` blocks
   `call_central_api` until the agent has called
   `get_api_endpoint_detail` (or glossary) earlier in *the same
   process*. Hosts that share a single MCP server across many chat
   sessions (Claude Desktop reuses the process across tabs and across
   restarts of the same conversation) silently bypass the gate or
   trigger spurious blocks. The gate is also a state machine the agent
   has to model.
4. **Embedding columns and the `unified_search` machinery are dead
   code.** `ApiEndpoint.embedding FLOAT[384]` and `DocSection.embedding
   FLOAT[384]` were never populated; `tools/search.py` is consumed only
   by an internal `unified_search` closure that is not registered as a
   tool. They cost DDL surface and confuse the schema.

The user-stated goal for this phase: *“Make the graph tool the go-to
tool to explore and understand the API. Cypher gives the agents agency
and flexibility to explore in a precise form.”*

## Decision

Decompose the per-endpoint blob into a **schema subgraph** the agent
queries with `query_graph`, and make `query_graph` itself ergonomic
enough to be the primary discovery surface.

### Subgraph shape (medium decomposition)

New node tables (additions to `KNOWLEDGE_NODE_TABLES`):

* `Parameter` — one row per query / path / header / cookie parameter.
  Carries `name`, `in`, `required`, `type`, `format`, `enum STRING[]`,
  `pattern`, and a free-form `hint` (e.g. `"odata-filter"`,
  `"rfc3339"`, `"comma-list"`) inferred from the schema or pattern.
* `RequestBody` — one row per endpoint that has a body. Carries
  `content_type`, `required`, and a `root_component_ref` (the `$ref` of
  the top-level body schema, when one exists).
* `Response` — one row per `(endpoint, status)` pair the skeleton
  exposes. Carries `status`, `content_type`, `root_component_ref`.
* `SchemaComponent` — one row per OAS component (named or anonymous).
  PK is a deterministic ID: `f"{spec_source}:{section}:{name}"` for
  named components, `f"{spec_source}:anon:{sha1(body)[:12]}"` for
  inline schemas that needed a synthetic name. Carries `spec_source`
  (`central` | `glp`), `section` (`schemas` | `responses`), `name`,
  `type`, `kind` (`object` | `array` | `union` | `primitive`),
  `required STRING[]`, `enum STRING[]`, `bodyJson` (the
  prose-stripped full body — moved here from
  `ApiEndpoint.bodyComponentsJson`).
* `ApiEndpointSkeleton` — **(superseded in Phase 2E.)** Originally
  one row per endpoint holding the full pre-rendered
  `bodySkeletonJson` / `bodyGlossaryJson` blobs. Phase 2E removed this
  node together with the `get_api_endpoint_detail` /
  `get_api_endpoint_glossary` tools — the structured
  `SchemaComponent` + `Property` subgraph supersedes it for every
  discovery use case. `describe_endpoint_for_device(method, path,
  deviceType?)` is the body-assembly entry point; it walks the
  Property subgraph and returns request parameters plus every leaf
  body field, already flattened across `allOf` branches.

New relationship tables (additions to `KNOWLEDGE_REL_TABLES`):

* `HAS_PARAMETER (FROM ApiEndpoint TO Parameter)`
* `HAS_REQUEST_BODY (FROM ApiEndpoint TO RequestBody)`
* `HAS_RESPONSE (FROM ApiEndpoint TO Response)`
* `BODY_REFERENCES (FROM RequestBody TO SchemaComponent)`
* `RESPONSE_REFERENCES (FROM Response TO SchemaComponent)`
* `REFERENCES (FROM SchemaComponent TO SchemaComponent, via STRING)` —
  edge property records the structural site of the reference
  (`properties` | `items` | `allOf` | `oneOf` | `anyOf` |
  `additionalProperties`).
* `HAS_SKELETON (FROM ApiEndpoint TO ApiEndpointSkeleton)`

`ApiEndpoint` itself loses `bodySkeletonJson`, `bodyGlossaryJson`,
`bodyComponentsJson`, and `embedding`. `DocSection` loses `embedding`.

### `query_graph` ergonomics

* Accepts an optional `parameters` JSON-string argument (mirrors
  `write_graph`’s shape) and forwards it to the underlying Cypher
  engine.
* Soft cap at ~200 rows: returns `{"warning": "truncated", "rows":
  [...], "cap": 200}` when exceeded so the agent can refine the query.
* Hard cap at ~2000 rows: raises `ToolError` to prevent runaway scans.
* `graph://schema` resource is extended with an **API discovery**
  section containing 5–6 canned Cypher patterns (find endpoint by
  path substring; list parameters for an endpoint; walk `REFERENCES`
  from a component; find every endpoint that touches a component;
  inspect the success-response shape for a status), and the parameter-
  passing convention.

### Stateless dispatch validation

`check_call_policy` is replaced by a stateless
`validate_call_shape(method, path, query_params, body)` that:

1. Resolves the concrete path back to its catalog template via the
   existing `EndpointRegistry`.
2. Loads `Parameter` nodes for that endpoint and the
   `SchemaComponent` reachable from `RequestBody` (one short Cypher
   query each).
3. Returns `(ok, diff_or_None)` where `diff` is
   `{"missing_required": [...], "unknown_query_params": [...],
   "unknown_body_keys": [...], "expected_top_level_keys": [...]}`.

`call_central_api` raises `ToolError` with the diff embedded when
`ok` is false. There is no “must inspect first” mandate — the agent is
free to call directly, and gets a concrete, structural error if the
shape is wrong.

The legacy session gate is retained behind
`CENTRAL_MCP_LEGACY_GATE=1` for one release so operators can roll
back if the validator misjudges an edge case.

### Cleanups landed in the same change

* Drop `ApiEndpoint.embedding` and `DocSection.embedding` columns.
  No code populates them; no code reads them.
* Remove `tools/search.py` and the unregistered `unified_search`
  closure in `tools/api_catalog.py`. `list_api` is kept as the
  catalog fallback for hosts that drop the system instructions.
* Rewrite `instructions.py` to put `query_graph` first in the
  discovery workflow and document the validator’s diff format.
  The skeleton tools are still documented as the
  “before-I-POST” detail surface but are no longer mandatory.

`_KNOWLEDGE_SCHEMA_VERSION` bumps from 4 to 5; the server aborts
startup on mismatch, so the knowledge DB must be rebuilt.

## Consequences

* The agent has a single, expressive surface for cross-endpoint
  questions (Cypher), and three small endpoint-scoped tools for
  detail when it has narrowed the target.
* Three schema tools become a thin read-through over the new
  `ApiEndpointSkeleton` / `SchemaComponent` nodes; their public
  shape does not change.
* `call_central_api` failures become structural and actionable
  instead of session-state-dependent.
* Knowledge-DB rebuild is required (schema version bump). The spec
  cache is unchanged so the rebuild does not re-scrape upstream
  specs.
* Two dead code paths (embedding columns, FTS search helpers) are
  removed, simplifying the schema and the tools tree.

## Alternatives considered

* **Fine-grained decomposition** — one node per schema property
  (`Property`, `Field`). Rejected: explodes node count for marginal
  query benefit (the agent rarely needs to compare individual fields
  across endpoints; component-level granularity is enough).
* **Keep three schema tools as is, only add `query_graph` patterns.**
  Rejected: the blobs would still be the canonical store, and
  cross-endpoint Cypher would have to re-parse JSON in every query.
* **Drop the skeleton blob entirely**, render it on demand from the
  subgraph. Rejected for now: keeps the rebuild simple and the
  read-path cheap; revisit once the decomposition has proven itself.
