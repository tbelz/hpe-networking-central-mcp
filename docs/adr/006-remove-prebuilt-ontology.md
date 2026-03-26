# ADR-006: Remove Pre-built Ontology Layer

## Status
Accepted

## Context

The MCP server had a two-layer schema approach:

1. **Bootstrap DDL** — static node/relationship tables for the core domain model
   and knowledge layer (applied at startup).
2. **Generated DDL** — dynamically inferred from OpenAPI response schemas by a
   `schema_generator` module, producing a `generated_ddl.json` artifact at build
   time via GitHub Actions.

On top of this, an **entity mapping** subsystem (~1,850 lines across 7 modules)
maintained 190+ hardcoded rules that linked API parameters to entity types via
`EntityType` nodes and `OPERATES_ON` relationships.  A `search_related_apis`
tool exposed this mapping at runtime.

### Problems

- **Fragility**: The entity mapping rules were tightly coupled to API path
  conventions and parameter names.  Any API change could silently break
  mappings, producing stale or incorrect results with no runtime signal.
- **Complexity vs. value**: The schema generator inferred node tables from
  OpenAPI response schemas using heuristics (singularisation, PK guessing).
  The results were often wrong or incomplete, and agents never used the
  generated tables directly — they relied on the curated bootstrap schema.
- **Build pipeline cost**: The entity mapping and DDL generation added two
  expensive steps to the GitHub Actions build, plus a `generated_ddl.json`
  artifact that had to be shipped, extracted, and applied at startup.
- **Agent capability**: Modern LLM agents can discover API-to-entity
  relationships at runtime by reading endpoint descriptions, parameter names,
  and response schemas directly from the API catalog — making a pre-computed
  ontology redundant.

## Decision

1. **Delete** the `entity_mapping/` module (7 files), `schema_generator.py`,
   and `run_entity_mapping.py`.
2. **Remove** `EntityType` node table and `OPERATES_ON` relationship table from
   the bootstrap schema.
3. **Remove** generated DDL handling from `GraphManager.initialize()`,
   `server.py` startup, `api_catalog.py` knowledge DB refresh, and
   `build_knowledge_db.py`.
4. **Remove** the `search_related_apis` tool entirely.
5. **Simplify** `get_data_provenance` to return only instance-level provenance
   (`POPULATED_BY` edges, `source_api`, `fetched_at`) — no type-level
   `OPERATES_ON` queries.
6. **Add** a `write_graph` tool that lets agents enrich the graph directly with
   CREATE, MERGE, SET, and DELETE operations (schema-altering DDL is blocked).
7. **Set** `enrich_topology` and `populate_config_policy` seeds to
   `auto_run: false` so agents trigger them on demand rather than at startup.
8. **Simplify** the build pipeline from 7 steps to 5 (scrape specs → apply
   schema → populate endpoints → populate seeds → create FTS indexes).

## Consequences

### Positive
- ~2,200 lines of code removed (entity mapping + schema generator + tests).
- Build pipeline is faster and simpler — no entity mapping or DDL inference.
- The knowledge DB archive no longer includes `generated_ddl.json`.
- Agents can enrich the graph incrementally via `write_graph` rather than
  relying on a static pre-computed ontology.
- Fewer moving parts means fewer silent failures from API changes.

### Negative
- Agents must now discover API-to-entity relationships themselves, using
  `unified_search`, `search_api_catalog`, and `get_api_endpoint_detail`.
  This trades build-time computation for runtime reasoning.
- The `search_related_apis` tool is gone — agents that relied on it must
  use the API catalog search tools instead.

### Neutral
- Instance-level provenance (`POPULATED_BY`, `fetched_at`, `source_api`) is
  unchanged — ADR-004's runtime provenance model remains intact.
- The bootstrap schema (Org, Site, Device, ConfigProfile, etc.) is unchanged.
- Seed scripts continue to work as before; only their auto-run config changed.

## Supersedes
- Partially supersedes ADR-004 (type-level provenance via `OPERATES_ON` is
  removed; instance-level provenance is retained).
