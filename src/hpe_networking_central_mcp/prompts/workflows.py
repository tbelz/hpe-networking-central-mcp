"""MCP Prompts - guided workflow templates."""

from __future__ import annotations

import structlog

logger = structlog.get_logger("prompts.workflows")


def register_prompts(mcp):
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
   Use search_api_catalog("troubleshoot") or search_api_catalog("diagnostics")
   to find relevant endpoints.

5. **Check Script Library**: Call list_scripts(tag="troubleshooting") for existing diagnostic scripts.

6. **Present Findings**:
   - Device status and key attributes
   - Hierarchy context (collection → site → device)
   - Comparison with other devices at the same site
   - Recommended actions
"""
