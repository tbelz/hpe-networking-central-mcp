"""MCP Resources - documentation for agent context."""

from __future__ import annotations

from pathlib import Path

import structlog

from ..config import Settings

logger = structlog.get_logger("resources.docs")


def register_resources(mcp, settings: Settings):
    """Register documentation resources with the MCP server."""

    @mcp.resource("docs://pycentral/overview")
    def pycentral_overview() -> str:
        """Overview of the Central API surface - modules, authentication, and usage patterns."""
        return _read_doc(settings.docs_path / "pycentral" / "index.md",
                         fallback=CENTRAL_API_OVERVIEW)

    @mcp.resource("docs://pycentral/authentication")
    def pycentral_auth() -> str:
        """How to authenticate with Central - OAuth2 client credentials flow."""
        return _read_doc(settings.docs_path / "pycentral" / "getting-started" / "authentication.md",
                         fallback=AUTH_GUIDE)

    @mcp.resource("docs://pycentral/quickstart")
    def pycentral_quickstart() -> str:
        """Quickstart guide - basic API calls using httpx and the Central OAuth2 flow."""
        return _read_doc(settings.docs_path / "pycentral" / "getting-started" / "quickstart.md",
                         fallback=QUICKSTART)

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

CENTRAL_API_OVERVIEW = """# HPE Aruba Networking Central - API Overview

Central exposes a comprehensive REST API for managing network infrastructure.

## API Categories

### Monitoring (network-monitoring/v1alpha1/)
- **devices** - List, filter, and inspect monitored devices (switches, APs, gateways)
- **aps** - AP-specific monitoring, stats (CPU, memory, PoE)
- **gateways** - Gateway monitoring, interfaces, tunnels, stats
- **sites** - Site health and device health per site
- **clients** - Wireless/wired client monitoring, trends, top-N

### Configuration (network-config/v1alpha1/)
- **Profiles** - 100+ configuration profile types including:
  - VLANs (layer2-vlan), WLANs (wlan-ssids), DHCP (dhcp-pool, dhcp-server, dhcp-relay)
  - Routing (static-route, ospf, bgp, vrfs), ACLs (acl-rules)
  - AAA (aaa-policy, radius-servers), NTP, DNS, SNMP
- CRUD pattern: GET/POST/PATCH/DELETE on /network-config/v1alpha1/{type}[/{name}]

### Troubleshooting
- AAA/RADIUS testing, device diagnostics

### GLP / GreenLake Platform (devices/v1/)
- Device onboarding, subscription management, application assignment

## Authentication
OAuth2 client credentials flow via https://sso.common.cloud.hpe.com/as/token.oauth2
- POST with grant_type=client_credentials, HTTP Basic Auth (client_id:client_secret)
- Response: {"access_token": "...", "expires_in": 7200}
- Use: Authorization: Bearer {access_token}

## API Discovery
Read the api://central/catalog resource for a complete list of all available endpoints.
Use search_api_catalog("keyword") to find specific endpoints, then
get_api_endpoint_detail(method, path) for full parameter and schema details.

## Environment Variables
Scripts have these env vars available:
- CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET
- GLP_CLIENT_ID, GLP_CLIENT_SECRET
"""

AUTH_GUIDE = """# Authentication Guide

## OAuth2 Client Credentials Flow

Central uses standard OAuth2 with client credentials. Token management is handled
automatically by `central_helpers` — you do NOT need to manage tokens in scripts.

### In scripts (recommended)
```python
from central_helpers import api

# Just call the API — auth is handled for you
devices = api.get("network-monitoring/v1alpha1/devices", params={"limit": "100"})
```

### Token Lifecycle
- Tokens expire after ~7200 seconds (2 hours)
- `central_helpers` handles token refresh and 401 retry transparently
- One token per client_id is sufficient for all API calls
"""

QUICKSTART = """# Quickstart

## List All Devices
```python
from central_helpers import api
import json

devices = api.get("network-monitoring/v1alpha1/devices", params={"limit": "100"})
for d in devices.get("items", []):
    print(f"{d['serialNumber']} - {d['deviceType']} - {d['status']}")
```

## List All Devices (Paginated)
```python
from central_helpers import api

# Fetches ALL devices across all pages automatically
all_devices = api.paginate("network-monitoring/v1alpha1/devices")
print(f"Total: {len(all_devices)} devices")
for d in all_devices:
    print(f"{d['serialNumber']} - {d['deviceType']} - {d['status']}")
```

## Configuration Profiles (CRUD)
```python
from central_helpers import api

# List DHCP pools
pools = api.get("network-config/v1alpha1/dhcp-pool")

# Create a DHCP pool
api.post("network-config/v1alpha1/dhcp-pool",
    json_body={"name": "office-pool", "network": "10.0.1.0/24",
               "range_start": "10.0.1.100", "range_end": "10.0.1.200"})

# Get a specific pool
pool = api.get("network-config/v1alpha1/dhcp-pool/office-pool")
```

## Error Handling
```python
from central_helpers import api, NotFoundError, CentralAPIError
import sys

try:
    device = api.get("network-monitoring/v1alpha1/devices/SERIAL123")
except NotFoundError:
    print("Device not found", file=sys.stderr)
except CentralAPIError as e:
    print(f"Error [{e.status_code}]: {e.message}", file=sys.stderr)
```

## Site Management
```python
from central_helpers import api

# List sites with health info
sites = api.get("network-monitoring/v1alpha1/sites")
```
"""

SCRIPT_WRITING_GUIDE = """# Script Writing Guide for HPE Central MCP

## When to Write a Script vs Use call_central_api()

**Use call_central_api()** for:
- Single API calls: reads OR writes (GET, POST, PATCH, DELETE)
- Quick lookups: device status, site health, config profiles
- One-off writes: create a VLAN, delete a profile

**Write a script** for:
- Multi-step workflows (create site → assign devices → set persona)
- Complex logic with conditionals, loops, or error handling
- Operations that need rollback on failure
- Batch operations across many devices or sites

## How Scripts Work

Scripts are Python files executed by the MCP server. The server injects
credentials as environment variables and copies `central_helpers.py` into the
script library. Scripts import the pre-authenticated API helper and make calls
without any OAuth2 boilerplate.

**Scripts should NEVER:**
- Manage OAuth2 tokens directly
- Import httpx or requests for Central API calls
- Hardcode credentials or base URLs

## Template
```python
#!/usr/bin/env python3
\"\"\"Description of what this script does.\"\"\"

import argparse
import json
import sys

from central_helpers import api


def main():
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--param1", required=True, help="Description")
    args = parser.parse_args()

    # Make API calls — auth is handled automatically
    result = api.get("network-monitoring/v1alpha1/devices", params={"limit": "10"})

    # Write operations
    # api.post("network-config/v1alpha1/dhcp-pool", json_body={...})
    # api.patch("network-config/v1alpha1/dhcp-pool/pool1", json_body={...})
    # api.delete("network-config/v1alpha1/dhcp-pool/pool1")

    print(json.dumps({"status": "success", "result": result}))


if __name__ == "__main__":
    main()
```

## API Helper Reference

`from central_helpers import api` gives you a pre-authenticated client with:

- `api.get(path, params=None)` → dict
- `api.post(path, json_body=None, params=None)` → dict
- `api.patch(path, json_body=None, params=None)` → dict
- `api.put(path, json_body=None, params=None)` → dict
- `api.delete(path, params=None)` → dict

All methods return the parsed JSON response as a dict. They raise
`httpx.HTTPStatusError` on non-2xx responses (with automatic 401 retry).

## API Discovery
Before writing a script, use these tools to find the right endpoints:
1. Read api://central/catalog for a complete endpoint overview
2. Use search_api_catalog("keyword") to find specific endpoints
3. Use get_api_endpoint_detail(method, path) for full parameter and schema details
4. Use call_central_api() to test individual calls before scripting

## Common API Patterns
- Monitoring: GET network-monitoring/v1alpha1/devices (list), /devices?filter=... (filtered)
- Config profiles: CRUD on network-config/v1alpha1/{profile_type}[/{name}]
- GLP devices: devices/v1/networking (list all GLP devices)

## Environment Variables Available
- CENTRAL_BASE_URL  e.g., https://internal.api.central.arubanetworks.com
- CENTRAL_CLIENT_ID  OAuth2 client ID
- CENTRAL_CLIENT_SECRET  OAuth2 client secret
- GLP_CLIENT_ID, GLP_CLIENT_SECRET  GreenLake Platform credentials
"""
