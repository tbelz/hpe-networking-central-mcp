# HPE Networking Central MCP Server

[![Build](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/build-and-push.yml/badge.svg)](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/build-and-push.yml)
[![Knowledge DB](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/update-knowledge-db.yml/badge.svg)](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/update-knowledge-db.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://python.org)

MCP Server for **HPE Aruba Networking Central** and the **HPE GreenLake Platform**.

The agent manages network devices through a combination of direct API calls and
reusable Python scripts, with full access to both the Central API and the
GreenLake Platform API. The OpenAPI surface of both platforms is decomposed
into a LadybugDB graph at build time, so `query_graph` (Cypher) is the primary
tool for endpoint and schema discovery — the agent walks `ApiEndpoint`,
`Parameter`, `RequestBody`, `Response`, `SchemaComponent`, and `Property`
nodes instead of paging through raw OpenAPI blobs.

## Architecture

```
┌─────────────────────────┐
│      MCP Client         │
│  (VS Code / Claude)     │
└──────────┬──────────────┘
           │ stdio (JSON-RPC)
┌──────────▼─────────────────────────────────────────────────────────────┐
│   MCP Server (FastMCP)                                                 │
│                                                                        │
│  Tools                                                                 │
│  ├─ query_graph                    Cypher reads against LadybugDB      │
│  ├─ write_graph                    Cypher writes to enrich the graph   │
│  ├─ call_central_api               Central REST API                    │
│  ├─ call_greenlake_api             GreenLake Platform API              │
│  ├─ list_scripts / get_script_content / save_script                    │
│  └─ execute_script                 Run scripts with central_helpers    │
│                                                                        │
│  Resources                                                             │
│  ├─ api://endpoint-catalog         Full METHOD /path catalog           │
│  ├─ docs://endpoint-catalog        Alias for clients filtering api://  │
│  ├─ graph://schema                 Live LadybugDB schema + Cypher      │
│  ├─ graph://seed-status            Startup seed execution results      │
│  ├─ docs://central/overview        Central + GreenLake API overview    │
│  ├─ docs://script-writing-guide    Script template + helpers ref       │
│  ├─ docs://config-workflows        Hierarchy / scope / effective cfg   │
│  ├─ docs://vsg/list, docs://vsg/{id}  Validated Solution Guide pages   │
│  └─ script://seeds                 Pre-built seed scripts metadata     │
│                                                                        │
│  Prompts                                                               │
│  └─ analyze_inventory · analyze_config · troubleshoot_device · write_script │
└────────────────────────────────────────────────────────────────────────┘
```

## Knowledge Graph (LadybugDB)

The server ships with a pre-built LadybugDB graph database updated nightly and
downloaded on first launch. It has two layers:

- **Knowledge layer** — the entire OpenAPI surface of Central and GreenLake
  modelled as a graph (`ApiEndpoint`, `Parameter`, `RequestBody`,
  `SchemaComponent`, and related nodes). Populated at build time from the
  upstream specs. Use `query_graph` for endpoint discovery and schema
  navigation; canned Cypher patterns are embedded in `graph://schema`.
- **Domain layer** — live network state (`Org`, `SiteCollection`, `Site`,
  `Device`, `DeviceGroup`) populated at runtime by seed scripts that call
  the Central APIs.

## Prerequisites

- Docker (supports both **amd64** and **arm64** — Apple Silicon Macs pull the native image automatically)
- HPE Aruba Networking Central API credentials (client_id + client_secret)
- Optionally: HPE GreenLake Platform credentials (may share the same credentials)

## Quick Start

### VS Code MCP Configuration

Add to `.vscode/mcp.json`:

```json
{
  "servers": {
    "hpe-networking-central-mcp": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "--pull", "always",
        "--env-file", "${workspaceFolder}/.env",
        "-v", "central-scripts:/scripts/library",
        "ghcr.io/tbelz/hpe-networking-central-mcp:main"
      ]
    }
  }
}
```

> **Tip — interactive credentials:** If you prefer entering credentials on each
> server start instead of storing them in a `.env` file, use VS Code input
> variables:
>
> ```json
> {
>   "inputs": [
>     { "id": "centralBaseUrl", "type": "promptString", "description": "Central API base URL" },
>     { "id": "centralClientId", "type": "promptString", "description": "Central Client ID" },
>     { "id": "centralClientSecret", "type": "promptString", "description": "Central Client Secret", "password": true }
>   ],
>   "servers": {
>     "hpe-networking-central-mcp": {
>       "command": "docker",
>       "args": [
>         "run", "-i", "--rm", "--pull", "always",
>         "-v", "central-scripts:/scripts/library",
>         "-e", "CENTRAL_BASE_URL=${input:centralBaseUrl}",
>         "-e", "CENTRAL_CLIENT_ID=${input:centralClientId}",
>         "-e", "CENTRAL_CLIENT_SECRET=${input:centralClientSecret}",
>         "ghcr.io/tbelz/hpe-networking-central-mcp:main"
>       ]
>     }
>   }
> }
> ```
>
> VS Code will prompt you for each credential when the server starts.
> GreenLake credentials can be added the same way if needed.

### Environment Variables (.env file)

```
CENTRAL_BASE_URL=https://apigw-YOUR_CLUSTER.central.arubanetworks.com
CENTRAL_CLIENT_ID=your_client_id
CENTRAL_CLIENT_SECRET=your_client_secret
GREENLAKE_CLIENT_ID=your_glp_client_id
GREENLAKE_CLIENT_SECRET=your_glp_client_secret
```

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CENTRAL_BASE_URL` | Yes | — | Central API base URL ([find yours](https://developer.arubanetworks.com/aruba-central/docs/api-gateway-url)) |
| `CENTRAL_CLIENT_ID` | Yes | — | OAuth2 client ID for Central |
| `CENTRAL_CLIENT_SECRET` | Yes | — | OAuth2 client secret for Central |
| `GREENLAKE_CLIENT_ID` | No | Central client ID | GreenLake Platform client ID |
| `GREENLAKE_CLIENT_SECRET` | No | Central client secret | GreenLake Platform client secret |
| `GLP_BASE_URL` | No | `https://global.api.greenlake.hpe.com` | GreenLake API base URL |
| `GLP_INCLUDED_SLUGS` | No | — | Comma-separated service slugs to include (or empty for default set) |
| `READ_ONLY` | No | `false` | When set to `true` / `1` / `yes` / `on`, the server refuses any non-GET Central / GreenLake API call (both via tools and from inside scripts) and hides mutating endpoints from the `api://endpoint-catalog` resource. Local operations (`write_graph`, `save_script`, `execute_script`) remain available. |

### Startup behaviour

On first launch the server downloads the latest pre-built knowledge DB
tarball published by the
[`update-knowledge-db`](.github/workflows/update-knowledge-db.yml) workflow.
The on-disk manifest records the GitHub release tag, so subsequent launches
short-circuit the download when the local DB is already current — typical
warm-start latency is well under a second. If GitHub is unreachable but a
local DB exists, the server keeps using it (logged as
`knowledge_db_offline_using_local`) instead of falling back to an empty DB.

### Read-Only Mode

Start the container with `READ_ONLY=true` to lock the server into a
**network-side read-only** posture:

- `call_central_api` / `call_greenlake_api` reject `POST`, `PUT`, `PATCH`,
  and `DELETE` with a `READ_ONLY` error.
- The same restriction is enforced inside scripts — `api.post(...)` and
  friends fail with `CentralAPIError(403, "READ_ONLY", ...)`.
- Mutating endpoints are filtered out of the `api://endpoint-catalog` resource so the model never sees them.
- A banner is prepended to the MCP system prompt so the assistant knows it
  must not attempt configuration changes.
- Local-only operations (graph writes, saving / editing scripts, executing
  scripts that only read) continue to work — useful for auditing and
  reporting workflows.

> **Scope of enforcement.** READ_ONLY is an *agent behavioural guardrail*,
> not a hard sandbox. Enforcement happens at the HTTP-client layer inside
> `central_helpers` and via a `sitecustomize` hook injected into script
> subprocesses. Do not expose READ_ONLY mode to untrusted script authors.

## Claude Desktop / Claude Code Configuration

Claude Desktop reads its MCP servers from
`%APPDATA%\Claude\claude_desktop_config.json` (Windows) or
`~/Library/Application Support/Claude/claude_desktop_config.json` (macOS).
Claude Code reads `~/.config/claude-code/config.json` and uses the same
schema.

Paste the snippet below into the `mcpServers` block, replace the
placeholders, and restart the client. No `.env` file is needed — the
credentials are passed as plain CLI arguments to the server binary
running inside the container, so there is no separate `env` block to
keep in sync. Three profiles are shown:

- **`hpe-networking-central-mcp`** — full access; can read **and** write
  via the Central / GreenLake APIs.
- **`hpe-networking-central-mcp-readonly`** — same image with
  `--read-only`; mutating endpoints are refused at the HTTP client layer.
- **`hpe-networking-central-mcp-discovery-only`** — **no credentials**.
  The server boots in *discovery-only* mode: `query_graph`,
  `write_graph`, and the script-CRUD tools are available so the agent
  can design API calls and draft scripts, but no live API tools
  (`call_central_api`, `call_greenlake_api`, `execute_script`) are
  registered. Use this for review / authoring sessions before granting
  network access.

> **Security note**: CLI arguments to a process are visible to anyone
> who can run `docker inspect` or `ps` on the host. On a shared
> workstation, swap the inline `--client-secret` / `--glp-client-secret`
> for `--env-file /path/to/.env` (Docker) or your platform's secret
> store; the server still accepts the same values via the
> `CENTRAL_*` / `GREENLAKE_*` environment variables.

```json
{
  "mcpServers": {
    "hpe-networking-central-mcp": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm", "--pull", "always",
        "-v", "central-scripts:/scripts/library",
        "ghcr.io/tbelz/hpe-networking-central-mcp:main",
        "--central-url", "https://apigw-YOUR_CLUSTER.central.arubanetworks.com",
        "--client-id", "REPLACE_WITH_YOUR_CENTRAL_CLIENT_ID",
        "--client-secret", "REPLACE_WITH_YOUR_CENTRAL_CLIENT_SECRET",
        "--glp-client-id", "REPLACE_WITH_YOUR_GLP_CLIENT_ID",
        "--glp-client-secret", "REPLACE_WITH_YOUR_GLP_CLIENT_SECRET"
      ]
    },
    "hpe-networking-central-mcp-readonly": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm", "--pull", "always",
        "-v", "central-scripts:/scripts/library",
        "ghcr.io/tbelz/hpe-networking-central-mcp:main",
        "--central-url", "https://apigw-YOUR_CLUSTER.central.arubanetworks.com",
        "--client-id", "REPLACE_WITH_YOUR_CENTRAL_CLIENT_ID",
        "--client-secret", "REPLACE_WITH_YOUR_CENTRAL_CLIENT_SECRET",
        "--glp-client-id", "REPLACE_WITH_YOUR_GLP_CLIENT_ID",
        "--glp-client-secret", "REPLACE_WITH_YOUR_GLP_CLIENT_SECRET",
        "--read-only"
      ]
    },
    "hpe-networking-central-mcp-discovery-only": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm", "--pull", "always",
        "-v", "central-scripts:/scripts/library",
        "ghcr.io/tbelz/hpe-networking-central-mcp:main"
      ]
    }
  }
}
```

The same flat-`args` pattern works for any other MCP client that
supports the standard `command` + `args` schema. Drop profiles you do
not need, omit the `--glp-*` flags if you're only using Central APIs,
or fall back to the equivalent `-e CENTRAL_BASE_URL=...` Docker flags
if you prefer environment variables.

### Compiler v2 smoke-test profile

Use this profile after a knowledge DB release has been published from
`main`. It keeps the session discovery-only, loads the compiler/v2 graph
as the runtime graph, and keeps API discovery on the normal graph query
tools. `MCP_COMPILER_TOOLS=true` only adds compiler sidecar provenance and
health diagnostics; it does not replace `query_api_schema`, `query_fts`,
or `query_yang`.

```json
{
  "mcpServers": {
    "hpe-networking-central-mcp-v2-smoke": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm", "--pull", "always",
        "-v", "central-mcp-v2-data:/data",
        "-v", "central-scripts:/scripts/library",
        "-e", "KNOWLEDGE_RELEASE_REPO=tbelz/hpe-networking-central-mcp",
        "-e", "MCP_KNOWLEDGE_PROJECTION=v2",
        "-e", "MCP_COMPILER_TOOLS=true",
        "ghcr.io/tbelz/hpe-networking-central-mcp:main"
      ]
    }
  }
}
```

For live read-only API testing, add the Central/GreenLake credentials and
`--read-only` after the image name:

```json
"--central-url", "https://apigw-YOUR_CLUSTER.central.arubanetworks.com",
"--client-id", "REPLACE_WITH_YOUR_CENTRAL_CLIENT_ID",
"--client-secret", "REPLACE_WITH_YOUR_CENTRAL_CLIENT_SECRET",
"--glp-client-id", "REPLACE_WITH_YOUR_GLP_CLIENT_ID",
"--glp-client-secret", "REPLACE_WITH_YOUR_GLP_CLIENT_SECRET",
"--read-only"
```

## Tools

| Tool | Description |
|------|-------------|
| `query_graph` | Read-only Cypher against the LadybugDB graph. Primary tool for endpoint discovery, hierarchy navigation, and schema traversal. Soft cap 200 rows / hard cap 2000. Accepts a `parameters` JSON-string for parameterised queries. |
| `write_graph` | Cypher writes (`CREATE`, `MERGE`, `SET`, `DELETE`) to enrich the domain layer of the graph from runtime discoveries. |
| `call_central_api` | Make authenticated requests to any Central API endpoint. Runs stateless pre-flight validation against the graph on every call; schema context is included in any validation error so the agent can self-correct. |
| `call_greenlake_api` | Same as `call_central_api`, against the GreenLake Platform API. Only registered when GreenLake credentials are configured. |
| `list_scripts` | List all scripts in the automation library, optionally filtered by tag. |
| `get_script_content` | Read the source code of a script. |
| `save_script` | Save a Python script to the library for reuse. |
| `execute_script` | Execute a script with Central / GreenLake credentials and the `central_helpers` SDK injected. |

## Development

```bash
# Install uv
pip install uv

# Create venv and install dependencies
uv sync

# Run locally (without Docker)
uv run hpe-networking-central-mcp

# Run the test suite (no creds needed for unit tests; live_api is auto-skipped)
uv run pytest -m "unit and not slow"
uv run pytest                       # full suite (skips live_api without creds)
uv run pytest -m live_api           # requires .env with Central / GLP creds
```

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) for build pipeline details
and the test-marker reference, and the [docs/adr/](docs/adr/) directory
for architectural decisions.

### Building Locally

```bash
docker build -t hpe-networking-central-mcp .
```

## License

MIT
