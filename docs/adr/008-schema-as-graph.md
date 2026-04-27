# ADR 008 — OpenAPI schema as a first-class subgraph

## Status

Superseded by [ADR 009](009-graph-as-primary-api-discovery.md). The
subgraph proposed here landed in production (`Parameter`, `RequestBody`,
`Response`, `SchemaComponent`, `Property` node tables); ADR 009 then
removed the per-endpoint JSON blobs (`bodySkeletonJson`,
`bodyGlossaryJson`, `bodyComponentsJson`) and the `get_schema_component`
tool in favour of `query_graph` directly. The motivation and design notes
below are retained for historical context.

## Context

ADR 007 made the API endpoint *detail* surface lean by stripping prose
from the structural view and surfacing descriptions through a separate
glossary tool.  Even after that work, the bottleneck on Aruba's heavier
endpoints turned out to be **structural duplication**, not prose:

* A single endpoint can transitively reach hundreds of schema components
  through `$ref` chains that fan out from the request body or response.
* Every endpoint that touches a popular component (a port profile, a
  VLAN object, a radio profile) re-ships the full body of that
  component — and of every component that body in turn references — on
  every detail call.
* The largest endpoints in the catalog accumulate >400 KB of skeleton
  bytes from this fan-out, which is the dominant cause of context
  pressure when an agent inspects more than one of them.

The slim-skeleton change in this PR addresses the *immediate* symptom:
detail calls now return only an *index* of referenced components
(name + type + enum + required + child refs) and a separate
`get_schema_component` tool fetches a single full body on demand.  This
keeps the detail surface stable while preserving the ability to drill in.

That change is sufficient to unblock current users, but it does not
remove the underlying duplication: the same component body is still
materialised from scratch every time it is fetched, and component
relationships (which schema embeds which, which alternatives a
discriminator picks between, which response shape an endpoint promises)
remain implicit in JSON blobs rather than queryable.

The MCP server already exposes a graph database for the operational
domain (Org / Site / Device / Config).  Agents are demonstrably good at
navigating that graph with Cypher — which is the same shape of problem
that schema reachability presents.  A natural next step is to model the
OpenAPI specs themselves as a subgraph that lives alongside the
operational graph and is refreshed whenever the spec snapshot is
rebuilt.

## Decision

Treat the OpenAPI specs as a queryable structural graph.  Each spec
build will, in addition to producing the existing endpoint /
skeleton / glossary / components rows, emit nodes and relationships
that capture:

* endpoints and their parameter / request-body / response slots;
* schema components (named and anonymous) and their composition
  relationships (`properties`, `items`, `allOf` / `oneOf` / `anyOf`,
  discriminator, references between components);
* the link from each endpoint slot to the schema(s) it commits to.

The exact node and relationship shapes, naming conventions, and
traversal helpers are deliberately left to the implementation pull
request.  This ADR records only the constraints below.

## Constraints

The schema subgraph must:

1. **Be lossless for structural composition.**  `allOf` / `oneOf` /
   `anyOf` / discriminators must be representable so that a traversal
   can reconstruct a complete component body without consulting the
   original blob.  Anonymous (inline) schemas need first-class
   identity in the graph; we cannot collapse them into their parent
   without losing the ability to reference them from sibling
   endpoints that compose the same shape differently.
2. **Strip prose from default queries.**  Descriptions, titles, and
   examples must remain accessible (likely through the existing
   glossary projection or a separate property) but must not be
   returned by the structural traversals an agent uses for planning.
3. **Cap recursion at the query layer.**  Schema graphs are cyclic
   (self-referential containers, mutually recursive shapes are
   common in network configs).  Every traversal helper exposed to
   agents must take an explicit depth or row cap and document it.
4. **Be rebuilt atomically with each spec snapshot.**  The schema
   subgraph is derived data, not user data; a partial rebuild that
   leaves stale components in place is worse than no graph at all.
   The build pipeline owns the lifecycle.
5. **Not regress operational graph performance.**  The schema
   subgraph will outsize the operational graph by several orders of
   magnitude.  Indexes, label segregation, and read-only query paths
   must keep operational queries unaffected.
6. **Be opt-in for agents at first.**  The slim skeleton plus
   `get_schema_component` is enough for the common case.  Schema-graph
   traversal helpers should be introduced as additional tools so the
   blast radius of the new model is bounded while we observe
   real-world query patterns.

## Interaction with the slim-skeleton work

The slim skeleton (this PR) and the schema subgraph (this ADR) are
complementary, not alternatives:

* The skeleton is the **commitment shape** — what the agent must
  produce to call an endpoint successfully.  It will remain the
  authoritative answer to "what does this endpoint expect?".
* The schema subgraph is the **discovery surface** — how the agent
  finds, compares, and reasons about shapes across endpoints.
* `get_schema_component` is the bridge: it serves a single full body
  on demand today, and would be the natural place to add a
  graph-backed implementation later without changing the tool
  contract.

If the schema subgraph proves out, a future ADR may consider whether
the standalone `bodyComponentsJson` column can be retired in favour of
on-demand reconstruction from the graph.  That decision is explicitly
deferred and depends on measured query latency.

## Open questions deferred to implementation

* Naming of schema nodes (named components are obvious; anonymous
  inline schemas need a stable identity scheme that survives spec
  rebuilds without colliding when an unrelated endpoint introduces a
  similar inline shape).
* Whether glossary text lives on schema nodes or in a parallel
  structure keyed by the same identity.
* Which query helpers (effective request shape for an endpoint,
  endpoints that consume a given component, components that share a
  discriminator) are worth surfacing as MCP tools versus left to raw
  Cypher against documented node shapes.
* Whether and how to expose the schema subgraph through the existing
  `query_graph` tool versus a dedicated read interface.

These are intentionally left open.  The constraints above are the
contract; the shapes are not.
