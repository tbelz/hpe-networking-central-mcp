"""MCP Resources — documentation and examples for agent context."""

from __future__ import annotations

from pathlib import Path

import structlog

from ..config import Settings

logger = structlog.get_logger("resources.docs")


def register_resources(mcp, settings: Settings):
    """Register documentation and example resources with the MCP server."""

    @mcp.resource("docs://pycentral/overview")
    def pycentral_overview() -> str:
        """Overview of the pycentral v2 SDK — modules, authentication, and usage patterns."""
        return _read_doc(settings.docs_path / "pycentral" / "index.md",
                         fallback=PYCENTRAL_OVERVIEW)

    @mcp.resource("docs://pycentral/authentication")
    def pycentral_auth() -> str:
        """How to authenticate with pycentral v2 — NewCentralBase, token_info, OAuth2."""
        return _read_doc(settings.docs_path / "pycentral" / "getting-started" / "authentication.md",
                         fallback=PYCENTRAL_AUTH_GUIDE)

    @mcp.resource("docs://pycentral/quickstart")
    def pycentral_quickstart() -> str:
        """Quickstart guide for pycentral v2 — basic API calls, modules."""
        return _read_doc(settings.docs_path / "pycentral" / "getting-started" / "quickstart.md",
                         fallback=PYCENTRAL_QUICKSTART)

    @mcp.resource("docs://ansible/inventory-plugin")
    def ansible_inventory_plugin() -> str:
        """Documentation for the Ansible dynamic inventory plugin for Central."""
        return _read_doc(settings.docs_path / "ansible" / "central_inventory_plugin.md",
                         fallback=ANSIBLE_INVENTORY_DOC)

    @mcp.resource("docs://ansible/onboarding")
    def ansible_onboarding() -> str:
        """Example Ansible playbook for device onboarding (GLP + Central)."""
        return _read_doc(settings.examples_path / "onboarding_example.yml",
                         fallback=ONBOARDING_EXAMPLE)

    @mcp.resource("docs://ansible/onboarding-advanced")
    def ansible_onboarding_advanced() -> str:
        """Advanced Ansible onboarding playbook with device filtering and error handling."""
        return _read_doc(settings.examples_path / "onboarding_advanced_example.yml",
                         fallback="No advanced onboarding example available.")

    @mcp.resource("docs://ansible/profiles")
    def ansible_profiles() -> str:
        """Documentation for the central_profiles Ansible module."""
        return _read_doc(settings.docs_path / "ansible" / "central_profiles.md",
                         fallback="No profiles documentation available.")

    @mcp.resource("docs://ansible/sites")
    def ansible_sites() -> str:
        """Documentation for the central_sites Ansible module."""
        return _read_doc(settings.docs_path / "ansible" / "central_sites.md",
                         fallback="No sites documentation available.")

    @mcp.resource("docs://ansible/glp-devices")
    def ansible_glp_devices() -> str:
        """Documentation for the glp_devices Ansible module — assign/unassign to GLP."""
        return _read_doc(settings.docs_path / "ansible" / "glp_devices.md",
                         fallback="No GLP devices documentation available.")

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

PYCENTRAL_OVERVIEW = """# pycentral v2 SDK Overview

pycentral v2 is the Python SDK for HPE Aruba Networking Central (New Central) and GreenLake Platform.

## Modules
- **base.NewCentralBase** — Connection management, OAuth2 authentication, API routing
- **glp.Devices** — GLP device management (add, query, subscribe)
- **glp.Subscriptions** — License/subscription management
- **scopes.Scopes** — Hierarchical scope management (Sites, Device Groups, Devices)
- **scopes.Site** — Site CRUD operations
- **scopes.Device** — Device management within scopes
- **profiles.Profiles** — Configuration profiles (VLANs, WLANs, routes, etc.)
- **new_monitoring.MonitoringDevices** — Device monitoring and inventory
- **new_monitoring.MonitoringAPs** — AP-specific monitoring and statistics
- **troubleshooting.Troubleshooting** — Device diagnostics (AAA, RADIUS tests)
- **streaming.Streaming** — WebSocket event streaming

## Authentication Pattern
```python
from pycentral import NewCentralBase

token_info = {
    "new_central": {
        "base_url": "https://internal.api.central.arubanetworks.com",
        "client_id": "your_client_id",
        "client_secret": "your_client_secret"
    }
}
central = NewCentralBase(token_info=token_info)
```

## Environment Variables
Scripts executed by this MCP server have these env vars available:
- CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET
- GLP_CLIENT_ID, GLP_CLIENT_SECRET
"""

PYCENTRAL_AUTH_GUIDE = """# Authentication Guide

## NewCentralBase

```python
from pycentral import NewCentralBase

# Option 1: Client credentials (auto-generates token)
token_info = {
    "new_central": {
        "base_url": "https://internal.api.central.arubanetworks.com",
        "client_id": "...",
        "client_secret": "..."
    }
}

# Option 2: With GLP credentials too
token_info = {
    "new_central": {
        "base_url": "https://internal.api.central.arubanetworks.com",
        "client_id": "...",
        "client_secret": "..."
    },
    "glp": {
        "base_url": "https://global.api.greenlake.hpe.com",
        "client_id": "...",
        "client_secret": "..."
    }
}

central = NewCentralBase(token_info=token_info)
```

## Using Environment Variables (recommended for MCP scripts)
```python
import os
from pycentral import NewCentralBase

token_info = {
    "new_central": {
        "base_url": os.environ["CENTRAL_BASE_URL"],
        "client_id": os.environ["CENTRAL_CLIENT_ID"],
        "client_secret": os.environ["CENTRAL_CLIENT_SECRET"],
    }
}
central = NewCentralBase(token_info=token_info)
```
"""

PYCENTRAL_QUICKSTART = """# Quickstart

## Monitoring Devices
```python
from pycentral import NewCentralBase
from pycentral.new_monitoring.devices import MonitoringDevices

central = NewCentralBase(token_info=token_info)
devices = MonitoringDevices.get_all_devices(central)
for d in devices:
    print(f"{d['serialNumber']} - {d['deviceType']} - {d['status']}")
```

## GLP Device Operations
```python
from pycentral.glp.devices import Devices

dev_mgr = Devices()
all_devices = dev_mgr.get_all_devices(central)

# Add devices to GLP
dev_mgr.add_devices(central, network=[{"serialNumber": "...", "macAddress": "..."}])

# Add subscription
dev_mgr.add_sub(central, devices=[device_id], sub=subscription_id)
```

## Scope Management
```python
central = NewCentralBase(token_info=token_info, enable_scope=True)
sites = central.scopes.get_all_sites()
devices = central.scopes.get_all_devices()
```

## Site Management
```python
from pycentral.scopes.site import Site

site = Site({
    "name": "Berlin-HQ",
    "address": "123 Main St",
    "city": "Berlin",
    "state": "Berlin",
    "country": "Germany",
    "zipcode": "10115",
    "timezone": "Europe/Berlin"
}, central_conn=central)
site.create()
```
"""

ANSIBLE_INVENTORY_DOC = """# Ansible Dynamic Inventory Plugin

Plugin: `arubanetworks.hpeanw_central.central_inventory`

## Auto-generated Groups
- `site_<sitename>` — devices by site
- `type_<devicetype>` — SWITCH, GATEWAY, ACCESS_POINT
- `model_<model>` — by hardware model
- `status_<status>` — ONLINE, OFFLINE
- `function_<persona>` — CAMPUS_AP, ACCESS_SWITCH, etc.

## Device Variables
Each device gets: serialNumber, model, deviceType, status, siteName,
deviceGroupName, ipv4, macAddress, softwareVersion, isProvisioned, etc.
"""

ONBOARDING_EXAMPLE = """# See the Ansible collection examples/onboarding_example.yml for the full workflow.
# Key steps: GLP token → assign devices to GLP → Central token → create site → assign to site → assign persona
"""

SCRIPT_WRITING_GUIDE = """# Script Writing Guide for HPE Central MCP

## Script Structure
Scripts are Python files executed by the MCP server. They should:
1. Use `argparse` for CLI parameters
2. Read credentials from environment variables
3. Use `pycentral.NewCentralBase` for API connections
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

from pycentral import NewCentralBase


def get_connection():
    token_info = {
        "new_central": {
            "base_url": os.environ["CENTRAL_BASE_URL"],
            "client_id": os.environ["CENTRAL_CLIENT_ID"],
            "client_secret": os.environ["CENTRAL_CLIENT_SECRET"],
        }
    }
    return NewCentralBase(token_info=token_info)


def main():
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--param1", required=True, help="Description")
    args = parser.parse_args()

    central = get_connection()
    # ... do work ...
    print(json.dumps({"status": "success", "result": result}))


if __name__ == "__main__":
    main()
```

## Available Modules
- `pycentral.new_monitoring.devices.MonitoringDevices` — device monitoring
- `pycentral.glp.devices.Devices` — GLP device management
- `pycentral.glp.subscriptions.Subscriptions` — subscription management
- `pycentral.scopes.Scopes` — scope hierarchy
- `pycentral.scopes.site.Site` — site CRUD
- `pycentral.profiles.Profiles` — configuration profiles
- `pycentral.troubleshooting.Troubleshooting` — diagnostics

## Environment Variables Available
- CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET
- GLP_CLIENT_ID, GLP_CLIENT_SECRET
"""
