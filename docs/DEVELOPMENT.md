# Graph Schema & Build Pipeline

## Overview

The graph schema uses a single bootstrap DDL layer:

1. **Bootstrap DDL** ([src/hpe_networking_central_mcp/graph/schema.py](src/hpe_networking_central_mcp/graph/schema.py)) — Static node/relationship tables for the core domain model (Org, Site, Device, ConfigProfile, etc.) and knowledge layer (ApiEndpoint, ApiCategory, DocSection, Script).

## Key Modules

| Module | Purpose |
|--------|---------|
| `graph/manager.py` | Loads bootstrap DDL, provides `get_schema_description()` via live catalog introspection |
| `graph/schema.py` | Bootstrap DDL constants (`NODE_TABLES`, `REL_TABLES`, etc.) and helper functions |
| `seeds/populate_monitoring.py` | On-demand seed: ports, radios, clients (auto_run=false) |

## Build Pipeline Flow

`scripts/build_knowledge_db.py` runs on GitHub Actions:

1. Scrape OpenAPI specs from Aruba Central docs
2. Apply bootstrap DDL to knowledge DB
3. Index and populate API endpoints
4. Populate seed scripts
5. Create FTS indexes

## LadybugDB Quirks

- `CALL show_tables() RETURN * WHERE type = 'NODE'` is **invalid** — WHERE doesn't work with CALL. Filter in Python after `rows_as_dict()`.
- Always use `IF NOT EXISTS` in DDL for idempotent schema application.
- Use `python3.12` for tests (python3/3.10 lacks `real_ladybug`).

## Testing

```bash
# Run search + graph TDD tests
python3.12 -m pytest test_search.py test_graph.py test_monitoring_seed.py -v
```
