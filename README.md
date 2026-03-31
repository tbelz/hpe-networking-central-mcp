# HPE Networking Central MCP Server

[![Build](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/build-and-push.yml/badge.svg)](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/build-and-push.yml)
[![Knowledge DB](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/update-knowledge-db.yml/badge.svg)](https://github.com/tbelz/hpe-networking-central-mcp/actions/workflows/update-knowledge-db.yml)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)
[![Python 3.12+](https://img.shields.io/badge/python-3.12%2B-blue.svg)](https://python.org)

MCP Server for **HPE Aruba Networking Central** and the **HPE GreenLake Platform**.

The agent manages network devices through a combination of direct API calls and reusable Python scripts, with full access to both the Central API and GreenLake Platform API.

## Architecture

```
┌─────────────────────────┐
│     MCP Client          │
│  (VS Code / Claude)     │
└──────────┬──────────────┘
           │ stdio (JSON-RPC)
┌──────────▼──────────────┐
│   MCP Server (FastMCP)  │
│                         │
│  Tools:                 │
│  ├─ call_central_api    │──► Central REST API (monitoring, config, etc.)
│  ├─ call_greenlake_api  │──► GreenLake Platform API (devices, subscriptions)
│  ├─ unified_search      │──► Search APIs, docs, and data by keyword (BM25/FTS)
│  ├─ list_api_categories │──► Browse all API categories with counts
│  ├─ get_api_endpoint_detail ──► Full parameter/schema detail for any endpoint
│  ├─ query_graph         │──► Cypher queries against the configuration graph
│  ├─ write_graph         │──► Write Cypher to enrich the graph (CREATE, MERGE, SET)
│  ├─ list_scripts        │──► Browse automation script library
│  ├─ save_script         │──► Save Python scripts for reuse
│  ├─ get_script_content  │──► Read script source code
│  └─ execute_script      │──► Run scripts (central_helpers SDK injected)
│                         │
│  Resources:             │
│  ├─ graph://schema      │──► Live schema introspection
│  ├─ graph://seed-status │──► Startup seed execution results
│  ├─ docs://central/overview  │
│  ├─ docs://script-writing-guide │
│  ├─ docs://config-workflows │
│  └─ script://seeds      │
│                         │
│  Prompts:               │
│  ├─ analyze_inventory   │
│  ├─ analyze_config      │
│  ├─ troubleshoot_device │
│  └─ write_script        │
└─────────────────────────┘
```

## Knowledge Graph

The server maintains a LadybugDB (Kùzu) graph database with two layers:

1. **Knowledge layer** — API endpoints, categories, documentation, and scripts (populated at build time from OpenAPI specs)
2. **Domain layer** — live network state (devices, sites, config profiles) populated at runtime by seed scripts calling Central APIs

### Graph Schema

```mermaid
graph LR
    subgraph Knowledge Layer
        ApiEndpoint
        ApiCategory
        DocSection
        Script
    end

    subgraph Domain Layer
        Org
        SiteCollection
        Site
        Device
        DeviceGroup
        UnmanagedDevice
    end

    ApiEndpoint -->|BELONGS_TO_CATEGORY| ApiCategory

    Org -->|HAS_COLLECTION| SiteCollection
    Org -->|HAS_SITE| Site
    SiteCollection -->|CONTAINS_SITE| Site
    Site -->|HAS_DEVICE| Device
    DeviceGroup -->|HAS_MEMBER| Device
    Site -->|HAS_UNMANAGED| UnmanagedDevice
    Device -->|CONNECTED_TO| Device
    Device -->|LINKED_TO| UnmanagedDevice
```

### Search Architecture

```mermaid
graph TD
    Q[unified_search query, scope, limit] --> FTS{FTS available?}
    FTS -->|Yes| BM25[BM25 ranked search]
    FTS -->|No| CONTAINS[CONTAINS fallback]
    BM25 --> Merge[Merge & rank results]
    CONTAINS --> Merge
    Merge --> Results[JSON results]

    subgraph "FTS Indexes"
        api_fts["api_fts (ApiEndpoint)"]
        doc_fts["doc_fts (DocSection)"]
        device_fts["device_fts (Device)"]
        site_fts["site_fts (Site)"]
        script_fts["script_fts (Script)"]
    end
```

Scopes filter which indexes/tables are searched: `all`, `api`, `docs`, `data`.

### Documentation Pipeline

The `DocSection` node table and `doc_fts` FTS index are defined in the schema
and ready for use, but **no doc scraping pipeline actively populates them yet**.
The existing scrapers (`oas_scraper.py`, `glp_spec_provider.py`) fetch OpenAPI
specs and populate `ApiEndpoint` nodes — they do not extract prose documentation.
A future iteration will add a doc chunking pipeline to populate `DocSection`
nodes from ReadMe.io or other documentation sources.

## Prerequisites

- Docker
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

## Tools

| Tool | Description |
|------|-------------|
| `call_central_api` | Make authenticated requests to any Central API endpoint |
| `call_greenlake_api` | Make authenticated requests to any GreenLake Platform API endpoint (only available when GreenLake credentials are configured) |
| `unified_search` | Search APIs, docs, and graph data by keyword (BM25/FTS with scope filtering) |
| `list_api_categories` | List all API categories with endpoint counts |
| `get_api_endpoint_detail` | Get full parameter and schema details for a specific endpoint |
| `query_graph` | Execute read-only Cypher queries against the configuration graph |
| `write_graph` | Execute write Cypher to enrich the graph (CREATE, MERGE, SET, DELETE) |
| `list_scripts` | List all scripts in the automation library |
| `get_script_content` | Read the source code of a script |
| `save_script` | Save a Python script to the library for reuse |
| `execute_script` | Execute a script with Central/GreenLake credentials injected |

## Development

```bash
# Install uv
pip install uv

# Create venv and install dependencies
uv sync

# Run locally (without Docker)
uv run hpe-networking-central-mcp
```

### Building Locally

```bash
docker build -t hpe-networking-central-mcp .
```

## License

MIT
