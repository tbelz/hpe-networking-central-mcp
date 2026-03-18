"""MCP Resources - documentation for agent context."""

from __future__ import annotations

from pathlib import Path

import structlog

from ..config import Settings

logger = structlog.get_logger("resources.docs")


def register_resources(mcp, settings: Settings):
    """Register documentation resources with the MCP server."""

    @mcp.resource("docs://central/overview")
    def central_overview() -> str:
        """Overview of the Central and GreenLake API surface - what's available and how to use it."""
        return _read_doc(settings.docs_path / "central" / "overview.md",
                         fallback=CENTRAL_API_OVERVIEW)

    @mcp.resource("docs://script-writing-guide")
    def script_writing_guide() -> str:
        """Guide for writing automation scripts that the MCP server can execute."""
        return SCRIPT_WRITING_GUIDE


def _read_doc(path: Path, fallback: str = "Documentation not available.") -> str:
    """Read a documentation file, returning fallback if not found."""
    if path.exists():
        return path.read_text(encoding="utf-8")
    return fallback


# --- Embedded fallback documentation ---

CENTRAL_API_OVERVIEW = """\
# HPE Aruba Networking Central & GreenLake — API Overview

This MCP server provides authenticated access to two API platforms and an in-memory
configuration graph for structural navigation.

## Configuration Graph (via `query_graph`)

An in-memory Kùzu graph models the Central configuration hierarchy:
Org → SiteCollection → Site → Device, DeviceGroup → Device, Org → ConfigProfile.

Read the **graph://schema** resource for the full schema, relationships, and example
Cypher queries. Use `query_graph(cypher)` for structural questions (hierarchy navigation,
blast-radius analysis, cross-site comparison, device lookup).

Use `refresh_graph()` after making changes to keep the graph in sync.

## 1. Aruba Central APIs (via `call_central_api`)

Base URL: configured via `CENTRAL_BASE_URL` (e.g. `https://internal.api.central.arubanetworks.com`).

### Monitoring (network-monitoring/v1alpha1/)
- **devices** — list, filter, inspect monitored devices (switches, APs, gateways)
- **aps** — AP-specific monitoring, CPU/memory/PoE stats
- **gateways** — gateway monitoring, interfaces, tunnels
- **sites** — site health and per-site device health
- **clients** — wireless/wired client monitoring, trends

### Configuration (network-config/v1alpha1/)
- **Profiles** — 100+ config profile types: VLANs, WLANs, DHCP, routing (OSPF, BGP),
  ACLs, AAA, NTP, DNS, SNMP, and more
- CRUD pattern: GET/POST/PATCH/DELETE on `/network-config/v1alpha1/{type}[/{name}]`

## 2. GreenLake Platform APIs (via `call_greenlake_api`)

Base URL: `https://global.api.greenlake.hpe.com`

- **Device Management** (`/devices/v1/`) — add, view, manage devices in your workspace
- **Subscriptions** — license/subscription management and assignment
- **Service Catalog** — provision service managers (e.g. assign devices to Central)
- **Locations** — site/location management at the GreenLake platform level
- **Authorization, Tags, Workspaces, Audit Logs** — and many more

## API Discovery

All endpoints from both platforms are indexed in a single unified catalog.

1. `search_api_catalog("keyword")` — find endpoints by keyword across Central and GreenLake
2. `get_api_endpoint_detail(method, path)` — full parameter schemas, request/response bodies
3. `list_api_categories()` — browse all categories (Central categories and GreenLake categories)

GreenLake categories appear as "HPE GreenLake APIs for ...".

## Authentication

Both platforms use OAuth2 client-credentials via `https://sso.common.cloud.hpe.com/as/token.oauth2`.
Token management is fully automatic — in tools and in scripts.
"""

SCRIPT_WRITING_GUIDE = """\
# Script Writing Guide

## When to Write a Script vs Use call_central_api / call_greenlake_api

**Use direct API tools** for:
- Single API calls (GET, POST, PATCH, DELETE)
- Quick lookups: device status, site health, config profiles
- One-off writes: create a VLAN, delete a profile

**Write a script** for:
- Multi-step workflows (create site → assign devices → set persona)
- Complex logic with conditionals, loops, or error handling
- Operations that need rollback on failure
- Batch operations across many devices or sites
- Workflows that span both Central and GreenLake APIs

## How Scripts Work

Scripts are Python files executed by the MCP server. The server injects
credentials as environment variables and provides `central_helpers.py` with
pre-authenticated API clients. No OAuth2 boilerplate needed.

## Template

```python
#!/usr/bin/env python3
\"\"\"Description of what this script does.\"\"\"

import argparse
import json
import sys

from central_helpers import api, glp


def main():
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--param1", required=True, help="Description")
    args = parser.parse_args()

    # Central API calls
    devices = api.get("network-monitoring/v1alpha1/devices", params={"limit": "10"})

    # GreenLake API calls
    glp_devices = glp.get("devices/v1/devices", params={"limit": "10"})

    # Paginated fetch (auto-detects cursor vs offset pagination)
    all_devices = api.paginate("network-monitoring/v1alpha1/devices")

    print(json.dumps({"status": "success", "count": len(all_devices)}))


if __name__ == "__main__":
    main()
```

## API Helper Reference

### Central: `from central_helpers import api`

- `api.get(path, params=None)` → dict
- `api.post(path, json_body=None, params=None)` → dict
- `api.patch(path, json_body=None, params=None)` → dict
- `api.put(path, json_body=None, params=None)` → dict
- `api.delete(path, params=None)` → dict
- `api.paginate(path, params=None, max_pages=50, page_size=100)` → list[dict]

### GreenLake: `from central_helpers import glp`

Same methods as `api` above, but targeting `https://global.api.greenlake.hpe.com`.

### Error Handling

```python
from central_helpers import api, CentralAPIError, NotFoundError, AuthenticationError

try:
    device = api.get("network-monitoring/v1alpha1/devices/SERIAL123")
except NotFoundError:
    print("Device not found", file=sys.stderr)
except CentralAPIError as e:
    print(f"Error [{e.status_code}]: {e.message}", file=sys.stderr)
```

Error classes: `CentralAPIError` (base), `AuthenticationError` (401/403),
`RateLimitError` (429), `NotFoundError` (404), `PaginationError`.

All methods handle token refresh and 401 retry automatically.
Rate-limited requests (429) are retried once after the server-specified wait.

## Environment Variables Available in Scripts

- `CENTRAL_BASE_URL` — Central API base URL
- `CENTRAL_CLIENT_ID` / `CENTRAL_CLIENT_SECRET` — Central OAuth2 credentials
- `GREENLAKE_CLIENT_ID` / `GREENLAKE_CLIENT_SECRET` — GreenLake OAuth2 credentials
- `GLP_BASE_URL` — GreenLake API base URL (default: https://global.api.greenlake.hpe.com)

**Scripts should NEVER:**
- Manage OAuth2 tokens directly
- Import httpx or requests for API calls
- Hardcode credentials or base URLs
"""
