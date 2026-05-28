# Graph Schema & Build Pipeline

## Overview

The graph schema uses a single bootstrap DDL layer:

1. **Bootstrap DDL** ([src/hpe_networking_central_mcp/graph/schema.py](src/hpe_networking_central_mcp/graph/schema.py)) — Static node/relationship tables for the core domain model (Org, Site, Device, ConfigProfile, etc.) and knowledge layer (ApiEndpoint, ApiCategory, DocSection, Script).

## Runtime modes

The server has two startup modes:

- **Connected** — `CENTRAL_BASE_URL` / `CENTRAL_CLIENT_ID` /
  `CENTRAL_CLIENT_SECRET` are set (via env vars or the equivalent
  `--central-url` / `--client-id` / `--client-secret` CLI args). The
  OAuth2 token is validated at startup, the auto-run seeds populate the
  domain graph, and the live-API tools (`call_central_api`,
  `call_greenlake_api`, `execute_script`) are registered.
- **Discovery-only** — no Central credentials configured. The server
  boots without contacting Central, skips the auto-run seeds, and only
  registers `query_graph`, `write_graph`, and the script-CRUD tools.
  Useful for local UI work and for review sessions where the agent
  drafts API calls / scripts that the user runs later in a connected
  workspace. Locally: `uv run hpe-networking-central-mcp` with an empty
  env.

## Key Modules

| Module | Purpose |
|--------|---------|
| `graph/manager.py` | Loads bootstrap DDL, provides `get_schema_description()` via live catalog introspection |
| `graph/schema.py` | Bootstrap DDL constants (`NODE_TABLES`, `REL_TABLES`, etc.) and helper functions |
| `seeds/populate_monitoring.py` | On-demand seed: ports, radios, clients (auto_run=false) |

## Build Pipeline Flow

`scripts/build_knowledge_db.py` runs on GitHub Actions:

1. Sync OpenAPI references from Aruba Central docs
2. Apply bootstrap DDL to knowledge DB
3. Index and populate API endpoints
4. Populate seed scripts
5. Create FTS indexes

## LadybugDB Quirks

- `CALL show_tables() RETURN * WHERE type = 'NODE'` is **invalid** — WHERE doesn't work with CALL. Filter in Python after `rows_as_dict()`.
- Always use `IF NOT EXISTS` in DDL for idempotent schema application.
- Use `python3.12` for tests (python3/3.10 lacks `real_ladybug`).

## Testing

The pytest suite is organised by **markers** (declared in
[pyproject.toml](../pyproject.toml)):

| Marker       | What it covers                                          | Credentials needed |
|--------------|---------------------------------------------------------|--------------------|
| `unit`       | Pure-Python logic, no network, no graph DB              | No                 |
| `integration`| Local subprocess / IPC / seed startup against a temp DB | No (some)          |
| `live_api`   | Real Central / GreenLake API calls                      | **Yes**            |
| `slow`       | Anything that takes more than a few seconds             | n/a                |

Tests marked `integration` or `live_api` are **auto-skipped** when
`CENTRAL_BASE_URL`, `CENTRAL_CLIENT_ID`, and `CENTRAL_CLIENT_SECRET`
are not set in the environment (see `pytest_collection_modifyitems` in
[tests/conftest.py](../tests/conftest.py)). The shell-script aliases
`BASE_URL` / `CLIENT_ID` / `CLIENT_SECRET` from older docs are also
accepted and back-filled to the canonical names.

```bash
# Install test extras
uv sync --extra test

# Fast feedback loop (no creds required)
uv run pytest -m "unit and not slow"

# Full suite (skips live_api without .env)
uv run pytest

# Live API contract / seed tests (requires .env with creds)
uv run pytest -m live_api

# With coverage
uv run pytest --cov --cov-report=term-missing
```

### Real-spec ingestion fixtures (ADR-011)

The OAS → graph ingestion code is regression-tested against real
HPE Aruba Central / GreenLake spec excerpts at
`tests/fixtures/oas/real_excerpts/` rather than handwritten
synthetic OpenAPI. To refresh the corpus:

```bash
# After a fresh build with the current upstream specs:
cp build/spec_cache/central/Config/<file>.json tests/fixtures/oas/real_excerpts/central_config_<slug>.json
cp build/spec_cache/glp/<file>.json            tests/fixtures/oas/real_excerpts/glp_<slug>.json
```

Do NOT hand-edit these fixtures — they are verbatim by policy.
`tests/test_real_spec_ingest_smoke.py` runs every fixture through the
full pipeline and asserts the post-flush invariants
(`src/hpe_networking_central_mcp/graph/invariants.py`) hold.

### Build-time invariants gate

`scripts/build_knowledge_db.py` now runs the four invariants from
`graph/invariants.py` after the schema subgraph is populated. Two new
flags:

- `--strict` — turn violations into a non-zero exit (CI default).
- `--no-invariants` — skip the audit entirely (rarely useful).

Locally:

```bash
uv run python scripts/build_knowledge_db.py            # warn only
uv run python scripts/build_knowledge_db.py --strict   # fail on violation
```

See [docs/adr/011-real-spec-ingestion-invariants.md](adr/011-real-spec-ingestion-invariants.md)
for the rationale.

### Docker E2E layer

The shell scripts at the repo root (`test_all.sh`, `test_mcp.sh`,
`test_inventory.sh`, etc.) exercise the built Docker image end-to-end
and are **not** invoked by `pytest`. Build the image first with
`docker build -t hpe-networking-central-mcp:test .` and then run them
manually.

### Standalone smoke scripts

Two former tests that were really utility runners now live in
`scripts/`:

- `scripts/smoke_oas_e2e.py` — scrapes the Central OAS docs end-to-end.
- `scripts/smoke_graph_live.py` — drives the live populate-graph path.

They are not collected by pytest.

