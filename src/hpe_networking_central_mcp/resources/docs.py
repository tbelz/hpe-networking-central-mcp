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

    @mcp.resource("docs://config-workflows")
    def config_workflows() -> str:
        """Central hierarchy, scope IDs, and configuration workflow patterns."""
        return CONFIG_WORKFLOWS

    @mcp.resource("script://seeds")
    def seed_scripts() -> str:
        """Pre-built seed scripts available in the automation library."""
        import json as _json
        seeds_dir = Path(__file__).parent.parent / "seeds"
        entries = []
        for meta_file in sorted(seeds_dir.glob("*.meta.json")):
            meta = _json.loads(meta_file.read_text(encoding="utf-8"))
            script_name = meta_file.stem + ".py"  # foo.meta → foo.py
            entries.append(
                f"### {script_name}\n"
                f"{meta.get('description', 'No description')}\n\n"
                f"**Tags:** {', '.join(meta.get('tags', []))}\n\n"
                f"**Parameters:**\n"
                + "\n".join(
                    f"- `--{p['name']}` ({p.get('type','str')})"
                    f"{' [required]' if p.get('required') else ''}"
                    f" — {p.get('description','')}"
                    for p in meta.get("parameters", [])
                )
            )
        if not entries:
            return "No seed scripts available."
        return "# Seed Scripts\n\nPre-built reusable scripts. Use `list_scripts()` to see them, `execute_script()` to run.\n\n" + "\n\n".join(entries)


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

A file-backed LadybugDB graph models the Central configuration hierarchy:
Org → SiteCollection → Site → Device, DeviceGroup → Device, Org → ConfigProfile.

Read the **graph://schema** resource for the full schema, relationships, and example
Cypher queries. Use `query_graph(cypher)` for structural questions (hierarchy navigation,
blast-radius analysis, cross-site comparison, device lookup).

The graph is populated and enriched by seed scripts at startup. Use `refresh_graph()`
after making changes to reset and re-populate from live APIs. Scripts can also write
directly to the graph using `from central_helpers import graph`.

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

All endpoints from both platforms are indexed in the knowledge graph.

1. `list_api_categories()` — see all available API areas and endpoint counts.
2. `search_api_catalog(query)` — find endpoints by keyword. Optionally filter by category.
3. `get_api_endpoint_detail(method, path)` — full parameter schemas, request/response bodies.

Always discover endpoints from the catalog before writing scripts or making API calls.

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

## IMPORTANT: Discover endpoints first

Before writing any script, you MUST:
1. Call `search_api_catalog(query)` to find relevant endpoints by keyword
2. Call `get_api_endpoint_detail(method, path)` to get exact parameter schemas

Never guess or hardcode API paths — always discover them from the catalog.

## Template

```python
#!/usr/bin/env python3
\"\"\"Description of what this script does.\"\"\"

import argparse
import json
import sys

from central_helpers import api, glp, graph


def main():
    parser = argparse.ArgumentParser(description="Script description")
    parser.add_argument("--param1", required=True, help="Description")
    args = parser.parse_args()

    # Use api.paginate() for collection endpoints (auto-handles cursor/offset pagination)
    all_items = api.paginate("<path-from-api-catalog>")

    # Use api.get() only for single-item lookups
    single_item = api.get("<path-from-api-catalog>/ITEM_ID")

    print(json.dumps({"status": "success", "count": len(all_items)}))


if __name__ == "__main__":
    main()
```

## API Helper Reference

### Central: `from central_helpers import api`

- `api.get(path, params=None)` → dict — **Single-item lookups only** (e.g., get one device by serial). Never use for fetching collections.
- `api.post(path, json_body=None, params=None)` → dict
- `api.patch(path, json_body=None, params=None)` → dict
- `api.put(path, json_body=None, params=None)` → dict
- `api.delete(path, params=None)` → dict
- `api.paginate(path, params=None, max_pages=50, page_size=100)` → list[dict] — **The ONLY safe way to fetch collections.** Auto-detects cursor vs offset pagination. Never use `api.get()` with a `limit` parameter for fetching multiple items.

### GreenLake: `from central_helpers import glp`

Same methods as `api` above, but targeting `https://global.api.greenlake.hpe.com`.

### Graph Database: `from central_helpers import graph`

Scripts can read from and write to the shared LadybugDB graph database.

- `graph.query(cypher, params=None)` → list[dict] — Read-only Cypher query.
- `graph.execute(cypher, params=None)` → list[dict] — Read-write Cypher query (CREATE, MERGE, SET, DELETE).

```python
from central_helpers import api, graph

# Read existing graph data
sites = graph.query("MATCH (s:Site) RETURN s.scopeId, s.name")

# Enrich the graph with data from APIs
for site in sites:
    health = api.get(f"network-monitoring/v1alpha1/sites/{site['s.scopeId']}/health")
    graph.execute(
        "MATCH (s:Site {scopeId: $sid}) SET s.health = $h",
        {"sid": site["s.scopeId"], "h": health.get("overall", "unknown")},
    )
```

The graph database is file-backed and shared between the MCP server and scripts.
Changes made by scripts are immediately visible to `query_graph()`.

### Error Handling

```python
from central_helpers import api, CentralAPIError, NotFoundError, AuthenticationError

try:
    device = api.get("<discovered-path>/SERIAL123")
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
- `GRAPH_DB_PATH` — Path to the shared file-backed LadybugDB graph database

**Scripts should NEVER:**
- Manage OAuth2 tokens directly
- Import httpx or requests for API calls
- Hardcode credentials or base URLs

## NetworkX for Topology Analysis

The `networkx` library is available for graph/topology analysis in scripts.
Import it directly — it is pre-installed in the MCP server environment.

```python
import networkx as nx
from central_helpers import api

# Build a NetworkX graph from LadybugDB topology data or from the topology API
G = nx.Graph()

# Example: fetch topology for a site and build a graph
topo = api.get(f"network-monitoring/v1/topology/{site_id}")
for device in topo.get("devices", []):
    G.add_node(device["serial"], name=device.get("name", ""), type=device.get("type", ""))
for link in topo.get("links", []):
    G.add_edge(link["from"], link["to"], speed=link.get("speed", 0),
               health=link.get("health", ""))

# Standard NetworkX analysis
print(f"Nodes: {G.number_of_nodes()}, Edges: {G.number_of_edges()}")
print(f"Connected: {nx.is_connected(G)}")
if nx.is_connected(G):
    print(f"Diameter: {nx.diameter(G)}")
    bridges = list(nx.bridges(G))
    print(f"Single points of failure (bridges): {bridges}")
```
"""

CONFIG_WORKFLOWS = """\
# Central Hierarchy & Configuration Workflows

## Hierarchy Scopes (top → bottom)

Central uses hierarchical scopes for configuration. Higher-scope config is
inherited by all child scopes; lower-scope config takes precedence.

| Level           | Description                                    | Precedence |
|-----------------|------------------------------------------------|------------|
| Library         | Template profiles assignable to any scope      | Lowest     |
| Global (Org)    | Encapsulates all collections, sites, devices   | Low        |
| Site Collection | Optional grouping of sites                     | Medium     |
| Site            | Network site with devices                      | High       |
| Device          | Individual device — overrides all above         | Highest    |

**Device Groups** cut across the hierarchy — they group devices from any site
for shared configuration. A device can belong to only one group.  Device Groups
sit between Site and Device for precedence purposes.

**Precedence order**: Device > Device Group > Site > Site Collection > Global

**Two propagation paths**:
- Global → Site Collections → Site → Device  (hierarchy-based)
- Device Groups → Device  (cross-cutting)

**Additive vs atomic profiles**: Some profile types (WLANs, VLANs, auth servers)
are additive — multiple instances coexist. Most profiles are atomic — only the
highest-precedence instance wins.

## Scope IDs

Every config API operation requires a `scopeId` identifying the target scope.
Scope IDs are available in the configuration graph:

- **Site**: `MATCH (s:Site) RETURN s.scopeId, s.name`
- **SiteCollection**: `MATCH (sc:SiteCollection) RETURN sc.scopeId, sc.name`
- **DeviceGroup**: `MATCH (dg:DeviceGroup) RETURN dg.scopeId, dg.name`
- **Device**: `MATCH (d:Device) RETURN d.serial, d.name` (serial = device scope ID)
- **Org/Global**: `MATCH (o:Org) RETURN o.scopeId`

## Configuration API Pattern

Config endpoints are under `network-config/v1alpha1/`. They require `scopeId`
and `scopeType` query parameters.

### Read config at a scope
```
GET network-config/v1alpha1/{category}
    ?scope-id=<id>&scope-type=<site|device|collection|org|device-group>
```

Add `effective=true` for merged inherited config. Add `detailed=true` for
source annotations showing which scope each setting comes from.

The `detailed=true` parameter returns `@` annotation blocks containing
`aruba-annotation:scope_device_function` — a list of scope assignment bindings
that reveals WHERE each profile is assigned.

### Write config
```
POST/PATCH network-config/v1alpha1/{category}
    ?scope-id=<id>&scope-type=<site|device|collection|org|device-group>
    body: { ... config payload ... }
```

## Config Policy Layer (Graph)

The `populate_config_policy` seed populates the graph with config assignment
and effective config data.  Use these queries to answer config questions.

### What profiles are assigned at a scope?
```cypher
// Profiles assigned at a specific site
MATCH (s:Site {name: 'Curry-Zentrale'})-[a:SITE_ASSIGNS_CONFIG]->(cp:ConfigProfile)
RETURN cp.name, cp.category, a.deviceFunctions, a.isDefault

// Profiles assigned at global scope
MATCH (o:Org)-[a:ORG_ASSIGNS_CONFIG]->(cp:ConfigProfile)
RETURN cp.category, cp.name, a.deviceFunctions
ORDER BY cp.category, cp.name

// Profiles assigned to a device group
MATCH (dg:DeviceGroup {name: 'Verkaufstelle'})-[a:GROUP_ASSIGNS_CONFIG]->(cp:ConfigProfile)
RETURN cp.category, cp.name, a.deviceFunctions
```

### What is the effective config on a device?
```cypher
MATCH (d:Device {name: '6300-Zentrale'})-[e:EFFECTIVE_CONFIG]->(cp:ConfigProfile)
RETURN cp.name, cp.category, e.sourceScope, e.sourceScopeName
ORDER BY cp.category, cp.name
```

### Why does a device have a specific config? (config lineage)
```cypher
MATCH (d:Device {name: '6300-Zentrale'})-[e:EFFECTIVE_CONFIG]->(cp:ConfigProfile {name: 'sys_central_nac'})
RETURN e.sourceScope AS assignedAt, e.sourceScopeId, e.sourceScopeName
```

### Blast radius: which devices are affected by changing a profile?
```cypher
MATCH (cp:ConfigProfile {name: 'Client Access'})<-[:EFFECTIVE_CONFIG]-(d:Device)
RETURN d.name AS device, d.serial, d.siteName
```

## Blast Radius Check (Before Mutations)

Before applying config at a scope, check what devices will be affected:

```cypher
// Site-level blast radius
MATCH (s:Site {name: 'MySite'})-[:HAS_DEVICE]->(d:Device)
RETURN d.serial, d.name, d.deviceType, d.configStatus

// Collection-level blast radius
MATCH (sc:SiteCollection {name: 'MyCollection'})-[:CONTAINS_SITE]->(s:Site)-[:HAS_DEVICE]->(d:Device)
RETURN s.name AS site, d.serial, d.name, d.deviceType
```

## Config Sync Verification (After Mutations)

After applying config, verify sync status:

1. Check `configStatus` on affected devices:
   ```cypher
   MATCH (s:Site {name: 'MySite'})-[:HAS_DEVICE]->(d:Device)
   RETURN d.name, d.configStatus
   ```

2. Call `refresh_graph()` to pull latest state from APIs.

3. Values: `synced` = config applied, `not_synced` = pending push, `failed` = error.

## Device Function & Persona

Devices have a `deviceFunction` (e.g., access, core, distribution) and
`persona` that determine which config categories apply. Pushing incompatible
config to a device will fail. Check a device's function before applying config:

```cypher
MATCH (d:Device {serial: 'SERIAL'})
RETURN d.persona, d.deviceFunction, d.deviceType
```
"""
