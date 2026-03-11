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
Use get_api_details("keyword") to search for specific endpoints.

## Environment Variables
Scripts have these env vars available:
- CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET
- GLP_CLIENT_ID, GLP_CLIENT_SECRET
"""

AUTH_GUIDE = """# Authentication Guide

## OAuth2 Client Credentials Flow

Central uses standard OAuth2 with client credentials.

### Using httpx (recommended for MCP scripts)
```python
import os
import httpx

TOKEN_URL = "https://sso.common.cloud.hpe.com/as/token.oauth2"

def get_token():
    resp = httpx.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(os.environ["CENTRAL_CLIENT_ID"], os.environ["CENTRAL_CLIENT_SECRET"]),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]

def api_get(path, params=None):
    token = get_token()
    base = os.environ["CENTRAL_BASE_URL"]
    resp = httpx.get(
        f"{base}/{path}",
        params=params,
        headers={"Authorization": f"Bearer {token}"},
    )
    resp.raise_for_status()
    return resp.json()
```

### Token Lifecycle
- Tokens expire after ~7200 seconds (2 hours)
- Re-authenticate on 401 responses
- One token per client_id is sufficient for all API calls
"""

QUICKSTART = """# Quickstart

## List All Devices
```python
import json, os, httpx

TOKEN_URL = "https://sso.common.cloud.hpe.com/as/token.oauth2"
BASE = os.environ["CENTRAL_BASE_URL"]

# Get token
token_resp = httpx.post(TOKEN_URL, data={"grant_type": "client_credentials"},
    auth=(os.environ["CENTRAL_CLIENT_ID"], os.environ["CENTRAL_CLIENT_SECRET"]))
token = token_resp.json()["access_token"]
headers = {"Authorization": f"Bearer {token}"}

# List devices
devices = httpx.get(f"{BASE}/network-monitoring/v1alpha1/devices",
    params={"limit": "100"}, headers=headers).json()
for d in devices.get("items", []):
    print(f"{d['serialNumber']} - {d['deviceType']} - {d['status']}")
```

## Configuration Profiles (CRUD)
```python
# List DHCP pools
pools = httpx.get(f"{BASE}/network-config/v1alpha1/dhcp-pool",
    headers=headers).json()

# Create a DHCP pool
httpx.post(f"{BASE}/network-config/v1alpha1/dhcp-pool",
    headers=headers,
    json={"name": "office-pool", "network": "10.0.1.0/24",
          "range_start": "10.0.1.100", "range_end": "10.0.1.200"})

# Get a specific pool
pool = httpx.get(f"{BASE}/network-config/v1alpha1/dhcp-pool/office-pool",
    headers=headers).json()
```

## Site Management
```python
# List sites with health info
sites = httpx.get(f"{BASE}/network-monitoring/v1alpha1/sites",
    headers=headers).json()
```
"""

SCRIPT_WRITING_GUIDE = """# Script Writing Guide for HPE Central MCP

## When to Write a Script vs Use call_central_api()

**Use call_central_api()** for:
- Simple read operations (GET requests)
- Quick lookups: device status, site health, config profiles
- Monitoring queries: list devices, check AP stats

**Write a script** for:
- Any write operation (POST, PATCH, DELETE)
- Multi-step workflows (create site + assign devices + set persona)
- Complex logic with conditionals or loops
- Operations that need error handling and rollback

## Script Structure
Scripts are Python files. They should:
1. Use `argparse` for CLI parameters
2. Read credentials from environment variables
3. Use `httpx` for HTTP requests with OAuth2 Bearer tokens
4. Print results as JSON to stdout
5. Use proper exit codes (0=success, 1=error)

## Template
```python
#!/usr/bin/env python3
\"\"\"Description of what this script does.\"\"\"

import argparse
import json
import os
import sys

import httpx

TOKEN_URL = "https://sso.common.cloud.hpe.com/as/token.oauth2"


def get_token():
    resp = httpx.post(
        TOKEN_URL,
        data={"grant_type": "client_credentials"},
        auth=(os.environ["CENTRAL_CLIENT_ID"], os.environ["CENTRAL_CLIENT_SECRET"]),
    )
    resp.raise_for_status()
    return resp.json()["access_token"]


def api_request(method, path, params=None, json_body=None):
    token = get_token()
    base = os.environ["CENTRAL_BASE_URL"]
    resp = httpx.request(
        method,
        f"{base}/{path}",
        params=params,
        json=json_body,
        headers={"Authorization": f"Bearer {token}", "Accept": "application/json"},
    )
    resp.raise_for_status()
    return resp.json()


def main():
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--param1", required=True, help="Description")
    args = parser.parse_args()

    # Use api_request() for all API calls
    result = api_request("GET", "network-monitoring/v1alpha1/devices", params={"limit": "10"})
    print(json.dumps({"status": "success", "result": result}))


if __name__ == "__main__":
    main()
```

## API Discovery
Before writing a script, use these tools to find the right endpoints:
1. Read api://central/catalog for a complete endpoint overview
2. Use get_api_details("keyword") to find specific endpoints and their parameters
3. Use call_central_api() to test GET requests before scripting

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
