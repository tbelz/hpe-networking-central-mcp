# HPE Networking Central MCP Server

MCP Server for **HPE Aruba Networking Central** implementing the **Code Interpreter Pattern**.

The agent doesn't call Central APIs directly. Instead, it writes, saves, and re-executes Python scripts using the [pycentral v2](https://github.com/aruba/pycentral) SDK, while using the [Ansible dynamic inventory plugin](https://github.com/aruba/aruba-central-ansible-collection) for network state discovery.

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
│  ├─ refresh_inventory   │──► ansible-inventory (dynamic inventory plugin)
│  ├─ get_device_details  │
│  ├─ list_scripts        │
│  ├─ save_script         │──► /scripts/library/*.py
│  └─ execute_script      │──► python3 (pycentral v2 SDK)
│                         │
│  Resources:             │
│  ├─ pycentral docs      │
│  ├─ Ansible module docs │
│  ├─ Example playbooks   │
│  └─ Script writing guide│
│                         │
│  Prompts:               │
│  ├─ onboard_device      │
│  ├─ analyze_inventory   │
│  └─ troubleshoot_device │
└─────────────────────────┘
```

## Prerequisites

- Docker
- HPE Aruba Networking Central API credentials (client_id + client_secret)
- GitHub PAT with `read:packages` scope (for pulling the private container image)

## Quick Start

### Pull the image

```bash
echo $GITHUB_PAT | docker login ghcr.io -u USERNAME --password-stdin
docker pull ghcr.io/tbelz/hpe-networking-central-mcp:latest
```

### VS Code MCP Configuration

Add to your VS Code `settings.json` or `.vscode/mcp.json`:

```json
{
  "mcpServers": {
    "hpe-networking-central-mcp": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-e", "CENTRAL_BASE_URL",
        "-e", "CENTRAL_CLIENT_ID",
        "-e", "CENTRAL_CLIENT_SECRET",
        "-v", "central-scripts:/scripts/library",
        "ghcr.io/tbelz/hpe-networking-central-mcp:latest"
      ],
      "env": {
        "CENTRAL_BASE_URL": "https://internal.api.central.arubanetworks.com",
        "CENTRAL_CLIENT_ID": "your_client_id",
        "CENTRAL_CLIENT_SECRET": "your_client_secret"
      }
    }
  }
}
```

### Claude Desktop Configuration

Add to `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "hpe-networking-central-mcp": {
      "command": "docker",
      "args": [
        "run", "-i", "--rm",
        "-e", "CENTRAL_BASE_URL=https://internal.api.central.arubanetworks.com",
        "-e", "CENTRAL_CLIENT_ID=your_client_id",
        "-e", "CENTRAL_CLIENT_SECRET=your_client_secret",
        "-v", "central-scripts:/scripts/library",
        "ghcr.io/tbelz/hpe-networking-central-mcp:latest"
      ]
    }
  }
}
```

## Environment Variables

| Variable | Required | Default | Description |
|----------|----------|---------|-------------|
| `CENTRAL_BASE_URL` | Yes | — | Central API base URL (e.g., `https://internal.api.central.arubanetworks.com`) |
| `CENTRAL_CLIENT_ID` | Yes | — | OAuth2 client ID for Central |
| `CENTRAL_CLIENT_SECRET` | Yes | — | OAuth2 client secret for Central |
| `GLP_CLIENT_ID` | No | Central client ID | GreenLake Platform client ID (if different) |
| `GLP_CLIENT_SECRET` | No | Central client secret | GreenLake Platform client secret (if different) |
| `SCRIPT_LIBRARY_PATH` | No | `/scripts/library` | Path to the script library directory |
| `INVENTORY_CACHE_TTL` | No | `300` | Inventory cache TTL in seconds |

## Tools

| Tool | Description |
|------|-------------|
| `refresh_inventory` | Run the Ansible inventory plugin to discover all devices, sites, and status. Returns summary or full data. |
| `get_device_details` | Look up a specific device by serial number, name, IP, or MAC address. |
| `list_scripts` | List all scripts in the automation library with metadata. |
| `save_script` | Save a new Python script to the library for reuse. |
| `execute_script` | Execute a script from the library with parameters. |

## Prompts

| Prompt | Description |
|--------|-------------|
| `onboard_device` | Guided workflow: onboard a device to a site with persona assignment. |
| `analyze_inventory` | Guided workflow: analyze inventory health, find issues. |
| `troubleshoot_device` | Guided workflow: troubleshoot a specific device. |

## Pre-installed Seed Scripts

The container ships with these scripts in the library:

| Script | Description |
|--------|-------------|
| `get_device_summary.py` | Get monitoring summary grouped by site/type/status/model |
| `get_device_inventory.py` | Get full device inventory including unassigned devices |
| `onboard_device.py` | Onboard a device to Central (inventory check, site verify, persona assign) |

## Development

### Local Setup

```bash
# Install uv
pip install uv

# Create venv and install dependencies
uv sync

# Run locally (without Docker)
uv run hpe-networking-central-mcp
```

### Building the Docker Image Locally

```bash
docker build -t hpe-networking-central-mcp .
```

### Testing

```bash
# Test MCP handshake
echo '{"jsonrpc":"2.0","method":"initialize","id":1,"params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"test","version":"0.1"}}}' | \
  docker run -i --rm -e CENTRAL_BASE_URL=test -e CENTRAL_CLIENT_ID=test -e CENTRAL_CLIENT_SECRET=test hpe-networking-central-mcp
```

## License

MIT
