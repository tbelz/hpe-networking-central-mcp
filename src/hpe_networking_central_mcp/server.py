"""HPE Networking Central MCP Server — Code Interpreter Pattern."""

from __future__ import annotations

from mcp.server.fastmcp import FastMCP

from .config import load_settings
from .logging import setup_logging
from .prompts.workflows import register_prompts
from .resources.docs import register_resources
from .tools.execution import register_execution_tools
from .tools.inventory import register_inventory_tools, _generate_inventory_config
from .tools.scripts import register_script_tools

logger = setup_logging()

mcp = FastMCP(
    "hpe-networking-central-mcp",
    instructions="""You are an automation engineer for HPE Aruba Networking Central.
You manage network devices (switches, access points, gateways) through the Code Interpreter Pattern:
- Use refresh_inventory() to understand the current network state
- Use list_scripts() to find existing automation scripts
- Write new Python scripts using pycentral v2 SDK when needed, save with save_script()
- Execute scripts with execute_script() to make changes
- Always verify changes by refreshing inventory after execution

You do NOT call APIs directly. Instead, you write, save, and execute reusable Python scripts.
Scripts use the pycentral v2 SDK and read credentials from environment variables.
Read the docs://script-writing-guide resource for the script template.""",
)

settings = load_settings()

# Generate inventory config on startup if creds available
if settings.has_credentials:
    try:
        _generate_inventory_config(settings)
        logger.info("startup_inventory_config_ready")
    except Exception as e:
        logger.warning("startup_inventory_config_failed", error=str(e))

# Ensure script library exists
settings.script_library_path.mkdir(parents=True, exist_ok=True)

# Register all components
register_inventory_tools(mcp, settings)
register_script_tools(mcp, settings)
register_execution_tools(mcp, settings)
register_resources(mcp, settings)
register_prompts(mcp)

logger.info(
    "server_ready",
    credentials_configured=settings.has_credentials,
    script_library=str(settings.script_library_path),
)


def main():
    """Entry point for the MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
