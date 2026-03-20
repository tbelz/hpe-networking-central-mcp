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
   Read the api://central/catalog resource to find relevant monitoring endpoints,
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

        Assembles the script-writing guide and a compact map of every API endpoint
        from the knowledge graph so the agent can write correct code without guessing.

        Args:
            task_description: What the script should accomplish.
        """
        api_map = _build_api_map(graph_manager)

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

## Available API Endpoints

{api_map}

Use the exact paths shown above. For endpoint details (parameters, request body),
call `get_api_endpoint_detail(method, path)`.

You can also read the `api://central/catalog` resource for the same information.

## Rules

1. Use ONLY endpoints from the catalog above — NEVER guess API paths.
2. Use `api.paginate()` for any list/collection endpoint.
3. Always handle errors with try/except CentralAPIError.
4. Print results as JSON to stdout.
5. Keep scripts focused — one task per script.
"""


def _build_api_map(graph_manager: GraphManager) -> str:
    """Query ApiEndpoint nodes and format a compact API map grouped by category."""
    if not graph_manager.is_available:
        return "(API catalog not available — knowledge database not loaded.)"

    try:
        rows = graph_manager.query(
            "MATCH (e:ApiEndpoint) "
            "RETURN e.category, e.method, e.path, e.summary "
            "ORDER BY e.category, e.path",
            read_only=True,
        )
    except Exception as exc:
        logger.warning("write_script_api_map_failed", error=str(exc))
        return "(Failed to load API map from graph.)"

    if not rows:
        return "(No API endpoints in knowledge database. Run refresh_knowledge_db() or check GH releases.)"

    # Group by category
    categories: dict[str, list[str]] = {}
    for r in rows:
        cat = r.get("e.category", "Uncategorized")
        line = f"  {r.get('e.method', '?'):6s} {r.get('e.path', '?')}  — {r.get('e.summary', '')}"
        categories.setdefault(cat, []).append(line)

    lines: list[str] = []
    for cat in sorted(categories):
        lines.append(f"### {cat} ({len(categories[cat])} endpoints)")
        lines.extend(categories[cat])
        lines.append("")

    return "\n".join(lines)
