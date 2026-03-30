"""MCP Prompts - guided workflow templates."""

from __future__ import annotations

from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("prompts.workflows")


def register_prompts(mcp, graph_manager: GraphManager):
    """Register workflow prompts with the MCP server."""

    @mcp.prompt()
    def analyze_inventory() -> str:
        """Guide: analyze the current network hierarchy, devices, and identify issues."""
        return """You are analyzing the HPE Aruba Networking Central network using the configuration graph.

Follow this workflow:

1. **Read Graph Schema**: Read the graph://schema resource to understand the data model.

2. **Explore Hierarchy**: Use query_graph() to understand the structure:
   ```cypher
   MATCH (o:Org)-[:HAS_COLLECTION]->(sc:SiteCollection)-[:CONTAINS_SITE]->(s:Site)
   RETURN o.name AS org, sc.name AS collection, s.name AS site
   ```
   Also check standalone sites:
   ```cypher
   MATCH (o:Org)-[:HAS_SITE]->(s:Site)
   RETURN s.name AS site, s.city AS city
   ```

3. **Device Overview**: Query devices per site and their status:
   ```cypher
   MATCH (s:Site)-[:HAS_DEVICE]->(d:Device)
   RETURN s.name AS site, d.deviceType AS type, d.status AS status, count(d) AS count
   ORDER BY s.name
   ```

4. **Find Issues**: Check for offline devices:
   ```cypher
   MATCH (s:Site)-[:HAS_DEVICE]->(d:Device {status: 'OFFLINE'})
   RETURN s.name AS site, d.serial, d.name, d.model
   ```

5. **Deep Dive**: Use call_central_api() for live monitoring data:
   - Site health endpoints
   - AP/switch/gateway specific stats
   - Client connection counts

6. **Present Report**: Summarize findings:
   - Hierarchy overview (collections → sites → devices)
   - Overall health (% online)
   - Action items (offline devices, firmware inconsistencies)
   - Recommendations

Present data in tables where appropriate for readability.
"""

    @mcp.prompt()
    def troubleshoot_device(identifier: str) -> str:
        """Guide: troubleshoot a specific network device."""
        return f"""You are troubleshooting device "{identifier}" in HPE Aruba Networking Central.

Follow this workflow:

1. **Find Device in Graph**: Use query_graph() to locate the device and its context:
   ```cypher
   MATCH (s:Site)-[:HAS_DEVICE]->(d:Device)
   WHERE d.serial CONTAINS '{identifier}' OR d.name CONTAINS '{identifier}'
   RETURN d.serial, d.name, d.model, d.status, d.ipv4, d.firmware, s.name AS site
   ```

2. **Understand Context**: Check what else is at the same site:
   ```cypher
   MATCH (s:Site)-[:HAS_DEVICE]->(d:Device)
   WHERE s.name = '<site_from_step_1>'
   RETURN d.serial, d.name, d.deviceType, d.status
   ```

3. **Check Blast Radius**: If the device is in a collection, understand the hierarchy:
   ```cypher
   MATCH (sc:SiteCollection)-[:CONTAINS_SITE]->(s:Site)-[:HAS_DEVICE]->(d:Device {{serial: '{identifier}'}})
   RETURN sc.name AS collection, s.name AS site
   ```

4. **Live Diagnostics**: Use call_central_api() for real-time monitoring data.
   Use unified_search(query) to find relevant monitoring endpoints,
   then get_api_endpoint_detail(method, path) for parameter details.

5. **Check Script Library**: Call list_scripts(tag="troubleshooting") for existing diagnostic scripts.

6. **Present Findings**:
   - Device status and key attributes
   - Hierarchy context (collection → site → device)
   - Comparison with other devices at the same site
   - Recommended actions
"""

    @mcp.prompt()
    def analyze_config(scope: str = "") -> str:
        """Guide: analyze configuration profiles and policy assignments across scopes."""
        scope_clause = f' Focus on scope: "{scope}".' if scope else ""
        return f"""You are analyzing configuration policy in HPE Aruba Networking Central.{scope_clause}

Follow this workflow:

1. **Discover Config Categories**: Query the graph for available categories:
   ```cypher
   MATCH (cp:ConfigProfile)
   RETURN DISTINCT cp.category AS category, count(cp) AS profiles,
          collect(DISTINCT cp.mergeStrategy)[0] AS mergeStrategy
   ORDER BY category
   ```

2. **Check Library Profiles**: See what config profiles exist at the org level:
   ```cypher
   MATCH (o:Org)-[:HAS_CONFIG]->(cp:ConfigProfile)
   RETURN cp.category, cp.name, cp.mergeStrategy, cp.isDefault, cp.deviceFunction
   ORDER BY cp.category, cp.name
   ```

3. **Scope Assignments**: Check what's assigned at each hierarchy level:
   ```cypher
   // Site-level assignments
   MATCH (s:Site)-[:SITE_ASSIGNS_CONFIG]->(cp:ConfigProfile)
   RETURN s.name AS site, cp.category, cp.name AS profile
   ORDER BY s.name, cp.category
   ```
   Also check collection, group, and device-level assignments using the
   COLLECTION_ASSIGNS_CONFIG, GROUP_ASSIGNS_CONFIG, and DEVICE_ASSIGNS_CONFIG
   relationships.

4. **Effective Config per Device** (graph-computed, from scope hierarchy walk):
   ```cypher
   MATCH (d:Device {{serial: '<serial>'}})-[r:EFFECTIVE_CONFIG]->(cp:ConfigProfile)
   RETURN cp.category, cp.name, cp.mergeStrategy, r.sourceScope, r.sourceScopeId
   ORDER BY cp.category
   ```
   This shows the graph's pre-computed view.  For **atomic** categories the
   closest scope wins (Device > Group > Site > Collection > Org).  For
   **additive** categories all contributing scopes appear.

   **On-demand API verification** — if you need the authoritative effective
   config for a specific device (e.g., to verify overrides or see live state),
   call the Central API directly:
   ```
   call_central_api(
       "network-config/v1alpha1/{{category}}",
       query_params={{"scopeId": "<serial>", "scopeType": "device",
                     "effective": "true", "detailed": "true"}}
   )
   ```
   The `detailed=true` response includes `sourceScope` and `sourceScopeId`
   annotations from Central's own resolution engine.  Use this when the
   graph-computed result needs validation or when device-level overrides
   are suspected.

5. **Blast Radius for Config Change**: Before modifying config at a scope:
   ```cypher
   MATCH (s:Site {{name: 'MySite'}})-[:HAS_DEVICE]->(d:Device)
   RETURN d.serial, d.name, d.deviceType, d.configStatus
   ```

6. **Merge Strategy Analysis**: Understand how profiles combine:
   - **additive** (e.g., wlan-ssids): profiles from parent scopes are combined
   - **atomic** (e.g., ntp): child scope overrides parent completely

7. **Present Report**:
   - Config categories with profile counts and merge strategies
   - Hierarchy overview showing where config is assigned
   - Per-device effective config (with inheritance source)
   - Anomalies: unassigned profiles, conflicting overrides
   - Recommendations
"""

    @mcp.prompt()
    def write_script(task_description: str) -> str:
        """Guide: write a Python automation script for a given task.

        Provides the script-writing template and instructs the agent to discover
        API endpoints via unified_search() instead of embedding the full catalog.

        Args:
            task_description: What the script should accomplish.
        """
        return f"""You are writing a Python automation script for HPE Aruba Networking Central.

## Task
{task_description}

## Step 1 — Discover Endpoints

Before writing ANY code you MUST:
1. Call `unified_search(query)` with keywords relevant to the task to find candidate endpoints.
2. Call `get_api_endpoint_detail(method, path)` for each endpoint you plan to use — get exact
   parameter names, types, and request/response schemas.
3. Call `list_api_categories()` if you need to explore what API areas exist.

NEVER guess or hardcode API paths — always discover them first.

## Script Template

```python
#!/usr/bin/env python3
\"\"\"<one-line description of what this script does>\"\"\"

import argparse, json, sys
from central_helpers import api, glp, graph
from central_helpers import CentralAPIError, NotFoundError

def main():
    parser = argparse.ArgumentParser()
    # parser.add_argument("--site", required=True, help="Site name")
    args = parser.parse_args()

    # --- your logic here ---

if __name__ == "__main__":
    main()
```

## Authentication

Scripts run as subprocesses with pre-authenticated helpers injected:
- `api` — CentralAPI instance. Use `api.get(path)`, `api.post(path, body)`, `api.put(path, body)`, `api.delete(path)`, `api.patch(path, body)`.
- `api.paginate(path)` — Auto-paginating GET that returns a flat list. Use for any list endpoint.
- `glp` — GreenLakeAPI instance. Same interface, hits `https://global.api.greenlake.hpe.com`.
- `graph` — GraphHelper for the LadybugDB graph DB. Use `graph.execute(cypher, params)` for writes, `graph.query(cypher)` for reads.
- `CentralAPIError`, `NotFoundError` — Exception classes for error handling.

**Do NOT** handle OAuth2, tokens, or base URLs — the helpers do that.

## Pagination

NEVER pass `limit` to individual API calls for collecting data.
Use `api.paginate(path)` which auto-detects cursor vs offset pagination:
```python
all_aps = api.paginate("/monitoring/v2/aps")
```

## Error Handling

```python
from central_helpers import CentralAPIError, NotFoundError
try:
    result = api.get(f"/monitoring/v1/aps/{{serial}}")
except NotFoundError:
    print(f"AP {{serial}} not found", file=sys.stderr)
    sys.exit(1)
except CentralAPIError as e:
    print(f"API error: {{e}}", file=sys.stderr)
    sys.exit(1)
```

## Output

- Print JSON to stdout for structured results: `json.dump(result, sys.stdout, indent=2)`
- Print diagnostics/progress to stderr: `print("Processing...", file=sys.stderr)`
- Exit 0 on success, non-zero on failure.

## Rules

1. Use ONLY endpoints discovered via unified_search — NEVER guess API paths.
2. Use `api.paginate()` for any list/collection endpoint.
3. Always handle errors with try/except CentralAPIError.
4. Print results as JSON to stdout.
5. Keep scripts focused — one task per script.
"""
