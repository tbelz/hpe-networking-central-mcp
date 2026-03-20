"""MCP Prompts - guided workflow templates."""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..graph.manager import GraphManager


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
   - Config policy summary (profiles per scope level)
   - Action items (offline devices, firmware inconsistencies)
   - Recommendations

Present data in tables where appropriate for readability.
"""

    @mcp.prompt()
    def analyze_config(device_or_site: str = "") -> str:
        """Guide: analyze configuration inheritance and effective config for a device or site."""
        target_hint = f'"{device_or_site}"' if device_or_site else "the network"
        return f"""You are analyzing configuration inheritance for {target_hint} in HPE Aruba Networking Central.

Follow this workflow:

1. **Read Graph Schema**: Read the graph://schema resource to understand config policy relationships.
   Also read docs://config-workflows for the config model reference.

2. **Effective Config**: What config is effective on the target?
   ```cypher
   MATCH (d:Device)-[e:EFFECTIVE_CONFIG]->(cp:ConfigProfile)
   WHERE d.name CONTAINS '{device_or_site}' OR d.serial CONTAINS '{device_or_site}'
   RETURN cp.name, cp.category, e.sourceScope, e.sourceScopeName
   ORDER BY cp.category, cp.name
   ```

3. **Config Lineage**: Where does each profile come from?
   ```cypher
   MATCH (d:Device)-[e:EFFECTIVE_CONFIG]->(cp:ConfigProfile)
   WHERE d.name CONTAINS '{device_or_site}'
   RETURN cp.name, cp.category, e.sourceScope AS assignedAt, e.sourceScopeName AS scopeName
   ORDER BY e.sourceScope, cp.category
   ```

4. **Scope Assignments**: What's assigned at each level?
   ```cypher
   MATCH (o:Org)-[a:ORG_ASSIGNS_CONFIG]->(cp:ConfigProfile)
   RETURN 'Global' AS scope, cp.category, cp.name, a.deviceFunctions
   UNION ALL
   MATCH (sc:SiteCollection)-[a:COLLECTION_ASSIGNS_CONFIG]->(cp:ConfigProfile)
   RETURN sc.name AS scope, cp.category, cp.name, a.deviceFunctions
   UNION ALL
   MATCH (s:Site)-[a:SITE_ASSIGNS_CONFIG]->(cp:ConfigProfile)
   RETURN s.name AS scope, cp.category, cp.name, a.deviceFunctions
   ```

5. **Blast Radius**: How many devices are affected by a profile?
   ```cypher
   MATCH (cp:ConfigProfile)<-[:EFFECTIVE_CONFIG]-(d:Device)
   RETURN cp.name, cp.category, count(d) AS deviceCount
   ORDER BY deviceCount DESC
   ```

6. **Present Report**: Summarize:
   - Effective config on the target (grouped by category)
   - Config lineage showing inheritance chain
   - Potential conflicts or overrides
   - Blast radius for key profiles
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
   Use search_api_catalog(query) to find relevant monitoring endpoints,
   then get_api_endpoint_detail(method, path) for parameter details.

5. **Check Script Library**: Call list_scripts(tag="troubleshooting") for existing diagnostic scripts.

6. **Present Findings**:
   - Device status and key attributes
   - Hierarchy context (collection → site → device)
   - Comparison with other devices at the same site
   - Recommended actions
"""

    @mcp.prompt()
    def write_script(task_description: str) -> str:
        """Guide: write a Python automation script for a given task.

        Provides the script-writing guide and instructs the agent to search the
        API catalog dynamically rather than embedding the full endpoint list.

        Args:
            task_description: What the script should accomplish.
        """
        return f"""You are writing a Python automation script for HPE Aruba Networking Central.

## Task
{task_description}

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

## IMPORTANT: Discover endpoints first

Before writing the script, you MUST:
1. Call `list_api_categories()` to see available API areas.
2. Call `search_api_catalog(query)` with keywords relevant to the task to find endpoints.
3. Call `get_api_endpoint_detail(method, path)` for exact parameter schemas of endpoints you plan to use.
4. NEVER guess or hardcode API paths — always discover them from the catalog.

## Rules

1. Use ONLY endpoints discovered from the catalog — NEVER guess API paths.
2. Use `api.paginate()` for any list/collection endpoint.
3. Always handle errors with try/except CentralAPIError.
4. Print results as JSON to stdout.
5. Keep scripts focused — one task per script.
"""
