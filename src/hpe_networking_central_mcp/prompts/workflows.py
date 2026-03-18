"""MCP Prompts - guided workflow templates."""

from __future__ import annotations

import structlog

logger = structlog.get_logger("prompts.workflows")


def register_prompts(mcp):
    """Register workflow prompts with the MCP server."""

    @mcp.prompt()
    def onboard_device(serial_number: str, site_name: str, persona: str = "ACCESS_SWITCH") -> str:
        """Guide: onboard a network device to a site in HPE Aruba Networking Central.

        Walks through: inventory check -> verify device -> discover APIs -> create/reuse script -> execute -> verify.
        """
        return f"""You are onboarding device {serial_number} to site "{site_name}" with persona "{persona}" in HPE Aruba Networking Central.

Follow this workflow:

1. **Refresh Inventory**: Call refresh_inventory(detail_level="summary") to get the current network state.
   - Verify the site "{site_name}" exists. If not, the script will create it.
   - Locate device {serial_number}. Check if it is already assigned to the target site.
   - If the device is already at the target site, report that and stop.

2. **Check Existing Scripts**: Call list_scripts(tag="onboarding") to see if an onboarding script exists.

3. **Execute the Script**:
   - If an onboarding script exists (e.g., onboard_device.py), use it directly.
   - If not, write a new script using `from central_helpers import api` for all API calls.
     Save it via save_script() with tag "onboarding".
   - Call execute_script() with the appropriate parameters:
     - serial: {serial_number}
     - site: {site_name}
     - persona: {persona}

4. **Verify**: Call refresh_inventory(force_refresh=true) and confirm the device is now assigned to "{site_name}".

Read docs://script-writing-guide for the script template.
"""

    @mcp.prompt()
    def analyze_inventory() -> str:
        """Guide: analyze the current network inventory and identify issues."""
        return """You are analyzing the HPE Aruba Networking Central inventory.

Follow this workflow:

1. **Refresh Inventory**: Call refresh_inventory(detail_level="summary") to get current counts.

2. **Analyze Summary**: Review the data and identify:
   - Total device count and breakdown by type (switches, APs, gateways)
   - Devices per site
   - Online vs offline devices - flag any offline devices
   - Unassigned devices that need onboarding
   - Device function/persona distribution

3. **Deep Dive (if needed)**: For any anomalies, call refresh_inventory(detail_level="full") with filters:
   - filter_status="OFFLINE" to see offline devices
   - filter_site="<site>" to drill into a specific site
   - filter_type="SWITCH" (or ACCESS_POINT, GATEWAY) for type-specific views

4. **Get Specifics**: Use get_device_details(identifier) for any device that needs attention.

5. **Explore Further**: Use call_central_api() for deeper monitoring queries:
   - Site health: get_api_details("site health") to find the right endpoint
   - AP stats: get_api_details("ap stats") for AP-specific metrics
   - Client counts: get_api_details("clients") for connected client data

6. **Present Report**: Summarize findings in a structured format:
   - Overall health score (% online)
   - Site-by-site breakdown
   - Action items (unassigned devices, offline devices, firmware inconsistencies)
   - Recommendations

Present data in tables where appropriate for readability.
"""

    @mcp.prompt()
    def troubleshoot_device(identifier: str) -> str:
        """Guide: troubleshoot a specific network device."""
        return f"""You are troubleshooting device "{identifier}" in HPE Aruba Networking Central.

Follow this workflow:

1. **Get Device Details**: Call get_device_details("{identifier}") to retrieve full device information.
   - Note: serial number, device type, model, status, site, IP address, firmware version.
   - If device is not found, suggest refresh_inventory(force_refresh=true).

2. **Assess Status**:
   - If ONLINE: device is reachable - check for configuration issues.
   - If OFFLINE: device is unreachable - this is the primary issue to investigate.

3. **Check Context**: Call refresh_inventory(detail_level="full", filter_site="<device_site>") to see:
   - Are other devices at the same site also affected?
   - Is this an isolated issue or a site-wide problem?

4. **Explore Diagnostics**: Use get_api_details("troubleshoot") to find diagnostic API endpoints.
   - Use call_central_api() for read-only diagnostic queries.
   - For active tests (AAA, RADIUS), write and execute a script.

5. **Check Script Library**: Call list_scripts(tag="troubleshooting") for existing diagnostic scripts.
   - If none exist, consider writing one that runs device health checks.

6. **Present Findings**:
   - Device status and key attributes
   - Comparison with other devices at the same site
   - Recommended actions
   - If a script was run, present the diagnostic results.

Read api://central/catalog for available diagnostic and monitoring endpoints.
"""
