# ADR 011 — Compiler-based OAS-to-Graph Pipeline

## Status

Accepted  
Date: 2026-06-01  
Supersedes the *projection-shape* parts of: [ADR 008](008-schema-as-graph.md), [ADR 009](009-graph-as-primary-api-discovery.md)

## Context

ADRs 008–010 built a queryable OpenAPI subgraph in LadybugDB that agents
use to discover endpoints, parameters, and request-body shapes.  The
subgraph is produced by a bespoke hand-curated populator
(`oas_schema_graph.py`) that walks the OpenAPI specs and emits nodes and
edges directly into the final agent-visible tables.

Three structural problems have accumulated:

**1. Lossiness is silent.**  
The populator only materialises OAS keywords it was explicitly written to
handle.  When an agent encounters a gap — a missing `pattern` constraint,
an absent `x-enumDescriptions` payload, a missing `HAS_ITEM_SCHEMA` edge
for an array property — there is no error.  The populator succeeds and the
graph simply does not contain the fact.  The agent then asks an incorrect
question or hallucinates the answer.  Discovery of each gap requires a
full agent-session + code-review cycle.

**2. Curated projection leaks into the walker.**  
The populator simultaneously walks the raw spec *and* decides what the
agent should see.  These are separate concerns collapsed into one file.
Adding a new queryable fact (e.g. `defaultValue`) requires editing the
walker, the LadybugDB schema DDL, the invariant checklist, and the Cypher
recipes in the tool docstrings.  The change surface for any new keyword is
four places minimum.

**3. Multi-spec growth is blocked.**  
The design assumes a single known spec layout (Aruba Central + GreenLake
vendor extensions).  Onboarding a second spec family (e.g. Juniper Mist)
would require forking the populator or adding conditional branches
throughout.  Neither is maintainable.

## Decision

Replace the hand-curated populator with a four-stage compiler pipeline.
The pipeline is implemented and scoped as four independently shippable
tasks; the boundaries are chosen so each stage has a single clear
contract and minimal coupling to the others.

```
OAS spec (JSON/YAML)
    │
    ▼  Task 1 — Resolved Ingestion  (prance)
┌───────────────────────────────┐
│  resolved spec dict           │  in-memory; pure library wrapper
└───────────────┬───────────────┘
                │  Task 2 — Lossless AST Generator  (custom walker)
                ▼
┌───────────────────────────────┐
│  L1 — Lossless AST            │  build/knowledge_db_ast/
│  (LadybugDB, never agent-     │  one node per OAS construct;
│   visible)                    │  no domain logic
└───────────────┬───────────────┘
                │  Task 3 — Semantic Overlay  (graph-mutation rule packs)
                ▼
┌───────────────────────────────┐
│  L2 — Semantic Overlay        │  rule packs add typed edges and
│                               │  semantic nodes onto L1
└───────────────┬───────────────┘
                │  Task 4 — Agent Projection  (materializer + MCP tools)
                ▼
┌───────────────────────────────┐
│  L3 — Agent Projection        │  build/knowledge_db/  (current)
│  + MCP tool surface           │  Cypher tools / pre-flight validator
└───────────────────────────────┘
```

The four tasks map to the layer model as follows: Task 1 (ingestion)
produces the input to L1; Task 2 (AST emission) produces L1; Task 3
(overlay) produces L2; Task 4 (projection + tools) produces L3 and
preserves the existing agent surface.

The boundary between Task 1 and Task 2 matters: Task 1 is a thin
wrapper around an existing library, Task 2 is custom recursive code
that walks the resolved dict and emits nodes/edges. Bundling them would
hide that asymmetry and produce an awkward first PR.

The boundary between Task 3 and Task 4 mirrors the **Probability
Isolation** principle from recent agent-graph research (cf. arXiv
2510.06002, *Deterministic Legal Agents*): the LLM's stochastic
reasoning is confined to the initial intent → Cypher translation, and
all subsequent traversals run against a deterministic, audited graph
produced by Task 3. The pre-flight validator (ADR 010) is already an
instance of this pattern.

### L1 — Lossless AST

The compiler frontend (`compiler/frontend.py`) walks the resolved spec
and emits a generic LadybugDB (`build/knowledge_db_ast/`) that contains a
node for every OAS construct (Operation, Parameter, Schema, MediaType,
RequestBody, Response, Header, Discriminator, Example, EnumValue,
Constraint, Extension, …) and typed edges between them.

**Losslessness is enforced by design, not measured.**  If the walker
encounters an OAS keyword it does not have an explicit handler for, it
raises a hard error and aborts the build.  There is no coverage percentage
to track because the compiler either handles every keyword in the spec it
reads or it does not compile.  A new OAS keyword forces a new handler
before the build can succeed again.  This is the standard compiler
contract.

`x-*` vendor extensions are stored on generic `Extension` nodes attached
to their owner; they are not silently dropped.

L1 is never queried by agents.  It exists to allow L2 rule packs and the
L3 projection pass to iterate without re-parsing the spec.

### L2 — Semantic Overlay (rule packs)

`compiler/rules/` contains one module per rule-pack family:

- `rules/yang.py` — `x-path` → YangPath / YangModule / IN_MODULE
- `rules/cli.py` — `x-cliParam` → CliCommand / HAS_CLI_COMMAND
- `rules/composition.py` — allOf flattening, discriminator, array→items
- `rules/constraints.py` — pattern, enum, min/max, default,
  x-key, x-enumDescriptions

Each rule pack is a pure function `(L1_connection, output_buffer) → stats`.
Rule packs are registered explicitly in a manifest (not auto-discovered via
entry points) so ordering is deterministic and the build log shows exactly
which packs ran.

A second spec family would add its own pack entry (e.g.
`rules/mist.py`) without touching any other file.  This is aspirational
in V2 — no multi-vendor optimization is attempted — but the design does
not foreclose it.

### L3 — Agent Projection and MCP Tool Surface (Task 4)

`compiler/projections.py` materialises the same node-table shape the
existing agent tools read (`ApiEndpoint`, `Parameter`, `RequestBody`,
`Response`, `SchemaComponent`, `Property`, `YangPath`, `YangModule`,
`CliCommand`) into `build/knowledge_db/`.

The MCP tool surface (`tools/graph.py`, `tools/api_call.py`, the
pre-flight validator from ADR 010, `api://endpoint-catalog`) is part of
this layer. It is the *only* layer the agent sees. The tool contracts
do not change in this migration — that is the whole point of preserving
the L3 shape — but they are conceptually owned by Task 4.

L3 closes the gaps that surfaced in the hand-curated populator:

- `Property.pattern`, `.description`, `.defaultValue`,
  `.minimum`, `.maximum`, `.minLength`, `.maxLength`, `.enumDescriptions`
- `SchemaComponent.arrayKey`
- `HAS_ITEM_SCHEMA` relationship from array-typed Property to its items
  SchemaComponent

Existing Cypher recipes (`query_graph`, `query_yang`, `query_api_schema`)
are unchanged because the L3 table shape is preserved.

### Migration mechanics

The legacy populator and the compiler pipeline run in parallel for one
release cycle:

- `scripts/build_knowledge_db.py` gains `--projection={legacy,v2,both}`.
  Default is `both` during the migration window.
- `server.py` reads `MCP_KNOWLEDGE_PROJECTION={legacy,v2}` (default
  `legacy`) to decide which database to load.
- `tests/test_projection_parity.py` runs a curated set of agent-question
  Cypher queries against both databases and asserts that every row
  returned by the legacy projection is also returned by v2 (v2 may return
  additional richer rows).  This corpus lives in `tests/parity/` as
  individual `.cypher` files.

Cutover criteria (all required):

1. Parity tests pass on agent-visible behavior.
2. The gap list from the current populator is closed in L3 v2.
3. Invariants (`graph/invariants.py`) are green on v2 for three or
   more consecutive scheduled rebuilds.
4. One end-to-end smoke session (Claude Desktop, live API credentials)
   confirms tool behavior is unchanged.

When all criteria are met: flip `MCP_KNOWLEDGE_PROJECTION` default to
`v2`, bump `_KNOWLEDGE_SCHEMA_VERSION`.  The release *after* that
removes `oas_schema_graph.py`, the legacy build path, and the env var.

### Library choices

- **`prance`** for `$ref` resolution.  Handles external refs; supports
  lazy cycle detection.
- **LadybugDB** for L1 directly (PyArrow bulk-load path).  L1 lives in
  its own DB so L2/L3 iteration is cheap without re-parsing.
- **`jsonschema`** for constraint hooks.  Already used in the pre-flight
  validator (ADR 010).
- No new graph library.  NetworkX is not introduced.

## Consequences

### Breaking changes (none until cutover)

The legacy populator continues to produce `build/knowledge_db/` unchanged
during the migration window.  No tool signature, Cypher recipe, or agent
instruction changes until cutover.

### At cutover

- `oas_schema_graph.py` is deleted.
- `--projection=legacy` and `MCP_KNOWLEDGE_PROJECTION=legacy` are removed.
- `_KNOWLEDGE_SCHEMA_VERSION` is bumped.

### Architecture

- Separation of concerns is explicit: the compiler frontend handles
  parsing and representation; rule packs handle vendor semantics;
  projections handle agent ergonomics.
- The build fails loudly on unknown OAS keywords rather than silently
  producing a partial graph.
- L1 is the single source of truth for the raw spec; multiple L3
  projections can be derived from it without re-parsing.
- L1 size is expected to be 10–100× the current L3 node count.  This
  is acceptable because L1 lives in a separate database and agents never
  query it.

### Risks

| Risk | Mitigation |
|---|---|
| Build time grows significantly | Dual-build is bounded; legacy pass is deleted at cutover |
| OAS cycles cause infinite expansion | `prance` lazy mode + explicit cycle-marker edges in the frontend |
| Rule packs accumulate implicit ordering dependencies | Explicit registration manifest; each pack is a pure function |
| Parity tests give false confidence | Corpus seeded from real agent session gaps and existing tool docstring examples |
| `_KNOWLEDGE_SCHEMA_VERSION` churn | One bump at cutover; none during the dual-build migration window |

## Alternatives considered

- **Patch the current populator incrementally.**  Rejected: the silent-
  lossiness problem does not go away.  Each new gap still requires a
  four-file change and a full agent-session regression cycle.  The
  structural problem is the lack of a lossless base layer, not the
  specific missing keywords.

- **Use NetworkX as the L1 in-memory graph.**  Rejected: LadybugDB is
  already the persistence layer; an in-memory graph would evaporate
  between runs, forcing a full spec re-parse on every L2/L3 iteration.
  Persisting L1 makes iterating on rule packs and projections cheap.

- **Expose L1 directly to agents.**  Rejected: L1 is optimised for
  completeness, not for query ergonomics.  Agents would face hundreds of
  generic Extension nodes with no semantic labels.  L3 remains the agent
  surface.

- **Auto-discover rule packs via entry points.**  Rejected: entry-point
  discovery makes ordering non-deterministic and makes the build log
  harder to interpret.  Explicit registration costs nothing and is
  unambiguously debuggable.
