# HPE Networking Central MCP Server

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
│  ├─ search_api_catalog  │──► Search endpoints by keyword in the knowledge graph
│  ├─ get_api_endpoint_detail ──► Full parameter/schema detail for any endpoint
│  ├─ list_api_categories │──► Browse all API categories with counts
│  ├─ refresh_knowledge_db│──► Download latest knowledge DB from GitHub releases
│  ├─ refresh_inventory   │──► Network device inventory via Central API
│  ├─ get_device_details  │──► Device lookup by serial/name/IP/MAC
│  ├─ query_graph         │──► Run Cypher queries on the configuration graph
│  ├─ refresh_graph       │──► Reset and re-populate the graph from live APIs
│  ├─ list_scripts        │──► Browse automation script library
│  ├─ save_script         │──► Save Python scripts for reuse
│  ├─ get_script_content  │──► Read script source code
│  └─ execute_script      │──► Run scripts with credentials injected
│                         │
│  Resources:             │
│  ├─ graph://schema      │──► Graph data model and example queries
│  ├─ docs://central/overview  │──► API surface overview
│  ├─ docs://script-writing-guide │──► Script template and patterns
│  ├─ docs://config-workflows │──► Config hierarchy and workflow patterns
│  └─ script://seeds      │──► Pre-built seed script catalog
│                         │
│  Prompts:               │
│  ├─ analyze_inventory   │──► Network hierarchy and device analysis
│  ├─ analyze_config      │──► Config inheritance and effective config analysis
│  ├─ troubleshoot_device │──► Device troubleshooting workflow
│  └─ write_script        │──► Script writing with API discovery
└─────────────────────────┘
```

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

### Environment Variables (.env file)

```
CENTRAL_BASE_URL=https://internal.api.central.arubanetworks.com
CENTRAL_CLIENT_ID=your_client_id
CENTRAL_CLIENT_SECRET=your_client_secret
GREENLAKE_CLIENT_ID=your_glp_client_id
GREENLAKE_CLIENT_SECRET=your_glp_client_secret
```

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CENTRAL_BASE_URL` | Yes | — | Central API base URL |
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
| `call_greenlake_api` | Make authenticated requests to any GreenLake Platform API endpoint |
| `search_api_catalog` | Search the API catalog for endpoints by keyword, with optional category filter |
| `get_api_endpoint_detail` | Get full parameter and schema details for a specific endpoint |
| `list_api_categories` | List all API categories with endpoint counts |
| `refresh_knowledge_db` | Download the latest knowledge database from GitHub releases |
| `refresh_inventory` | Discover all devices, sites, and status from Central |
| `get_device_details` | Look up a device by serial, name, IP, or MAC (partial match) |
| `query_graph` | Run Cypher queries against the configuration graph |
| `refresh_graph` | Reset and re-populate the graph from live APIs |
| `list_scripts` | List all scripts in the automation library |
| `save_script` | Save a Python script to the library for reuse |
| `get_script_content` | Read the source code of a script |
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
