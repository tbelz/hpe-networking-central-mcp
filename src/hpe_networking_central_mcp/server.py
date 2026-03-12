"""HPE Networking Central MCP Server - API Discovery + Code Interpreter Pattern."""

from __future__ import annotations

import sys
import threading

from mcp.server.fastmcp import FastMCP

from .central_client import CentralClient
from .config import load_settings
from .logging import setup_logging
from .prompts.workflows import register_prompts
from .resources.docs import register_resources
from .tools.api_call import register_api_call_tools
from .tools.api_catalog import initialize_catalog, register_catalog_tools
from .tools.execution import register_execution_tools
from .tools.inventory import register_inventory_tools
from .tools.scripts import register_script_tools

logger = setup_logging()

mcp = FastMCP(
    "hpe-networking-central-mcp",
    instructions="""You are an automation engineer for HPE Aruba Networking Central.
You manage network devices (switches, access points, gateways) through a combination of
direct API reads and reusable Python scripts.

## How to work

1. **Discover APIs**: Read the api://central/catalog resource for a complete overview of
   available endpoints. Use get_api_details("keyword") to find specific endpoint details.

2. **Quick reads**: Use call_central_api(path, params) for GET requests - monitoring queries,
   config lookups, health checks. This is the fastest way to read data.

3. **Inventory**: Use refresh_inventory() to get a structured overview of all devices, sites,
   and their status. Use get_device_details() for individual device lookup.

4. **Writes and complex operations**: Write Python scripts using httpx + OAuth2, save with
   save_script(), and execute with execute_script(). Scripts handle POST/PATCH/DELETE operations
   and multi-step workflows.

5. **Reuse**: Always check list_scripts() before writing a new script.

Read docs://script-writing-guide for the script template and authentication pattern.
You decide freely whether to use call_central_api() or write a script based on the task.""",
)

settings = load_settings()

# ── Validate Central credentials before accepting connections ──────────
if not settings.has_credentials:
    logger.error(
        "startup_failed",
        reason="Missing credentials. Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, "
        "CENTRAL_CLIENT_SECRET in your .env file. See .env.example.",
    )
    sys.exit(1)

try:
    _client = CentralClient(
        settings.central_base_url,
        settings.central_client_id,
        settings.central_client_secret,
    )
    _client.validate()
    _client.close()
    logger.info("credentials_validated")
except Exception as exc:
    logger.error(
        "startup_failed",
        reason="Credential validation failed — could not obtain OAuth2 token.",
        error=str(exc),
    )
    sys.exit(1)

# Initialize API catalog in background to avoid blocking MCP handshake
if settings.has_postman_key:
    def _bg_catalog_init():
        try:
            initialize_catalog(settings)
        except Exception as e:
            logger.warning("startup_catalog_init_failed", error=str(e))

    threading.Thread(target=_bg_catalog_init, daemon=True).start()
else:
    logger.info("startup_catalog_skip", reason="no_postman_api_key")

# Ensure script library exists
settings.script_library_path.mkdir(parents=True, exist_ok=True)

# Register all components
register_inventory_tools(mcp, settings)
register_script_tools(mcp, settings)
register_execution_tools(mcp, settings)
register_catalog_tools(mcp, settings)
register_api_call_tools(mcp, settings)
register_resources(mcp, settings)
register_prompts(mcp)

logger.info(
    "server_ready",
    credentials_configured=settings.has_credentials,
    catalog_available=settings.has_postman_key,
    script_library=str(settings.script_library_path),
)


def main():
    """Entry point for the MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
