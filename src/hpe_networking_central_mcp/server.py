"""HPE Networking Central MCP Server - API Discovery + Code Interpreter Pattern."""

from __future__ import annotations

import shutil
import sys
import threading
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .central_client import CentralClient, GreenLakeClient
from .config import load_settings
from .graph import GraphManager
from .logging import setup_logging
from .prompts.workflows import register_prompts
from .resources.docs import register_resources
from .resources.graph import register_graph_resources
from .tools.api_call import register_api_call_tools, register_greenlake_api_call_tools
from .tools.api_catalog import initialize_catalog, register_catalog_tools
from .tools.execution import register_execution_tools
from .tools.graph import register_graph_tools
from .tools.scripts import register_script_tools

logger = setup_logging()

mcp = FastMCP(
    "hpe-networking-central-mcp",
    instructions="""You are an automation engineer for HPE Aruba Networking Central.
You manage network devices (switches, access points, gateways) through a combination of
direct API reads and reusable Python scripts.

## How to work

1. **Understand the network**: Read the graph://schema resource to learn the graph model,
   then use query_graph(cypher) to explore the hierarchy: Org → SiteCollections → Sites →
   Devices. The graph is your structural map — use it for navigation, blast-radius analysis,
   cross-site comparison, config provenance, and dependency tracking.

2. **Discover APIs**: Read the api://central/catalog resource for a complete overview of
   available endpoints. Use search_api_catalog("keyword") to find specific endpoints,
   then get_api_endpoint_detail(method, path) for full parameter and schema details.

3. **Quick reads**: Use call_central_api(path, params) for GET requests - monitoring queries,
   config lookups, health checks. This is the fastest way to read live data.
   Tip: Add `effective=true` to config endpoints for hierarchically merged config,
   and `detailed=true` for source annotations.

4. **Single writes**: Use call_central_api(path, method="POST", body={...}) for simple
   write operations (create a VLAN, delete a profile, update a setting).

5. **Multi-step workflows**: For operations that involve multiple API calls (e.g., onboard
   a device: check inventory → create site → assign device → set persona), ALWAYS use a
   script. Check list_scripts() first for an existing script, then write a new one with
   save_script() if needed. Execute with execute_script(). NEVER chain multiple
   call_central_api() calls for multi-step workflows.

6. **Paginated lists**: When scripts need ALL items from a list endpoint, use
   `api.paginate(path)` instead of manual pagination loops. It auto-detects cursor vs
   offset pagination and returns a flat list.

7. **Error handling**: Scripts should catch `CentralAPIError` (or subclasses like
   `NotFoundError`) for graceful error handling. Import them from `central_helpers`.

8. **GreenLake Platform**: Use call_greenlake_api(path, params) for HPE GreenLake APIs
   (device onboarding, subscriptions, licenses, locations, service catalog). These hit
   https://global.api.greenlake.hpe.com. In scripts, use `from central_helpers import glp`.

9. **After changes**: Call refresh_graph() after making config changes via API or scripts
   to keep the graph in sync with Central.

10. **Reuse**: Always check list_scripts() before writing a new script.

Read docs://script-writing-guide for the script template and authentication pattern.
Scripts use `from central_helpers import api, glp` — no OAuth2 boilerplate needed.""",
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
    client = CentralClient(
        settings.central_base_url,
        settings.central_client_id,
        settings.central_client_secret,
    )
    client.validate()
    logger.info("credentials_validated")
except Exception as exc:
    logger.error(
        "startup_failed",
        reason="Credential validation failed — could not obtain OAuth2 token.",
        error=str(exc),
    )
    sys.exit(1)

# Initialize API catalog in background to avoid blocking MCP handshake
def _bg_catalog_init():
    try:
        initialize_catalog(settings)
    except Exception as e:
        logger.warning("startup_catalog_init_failed", error=str(e))

threading.Thread(target=_bg_catalog_init, daemon=True).start()

# Initialize graph database and populate in background
graph_manager = GraphManager()
graph_manager.initialize()

def _bg_graph_populate():
    try:
        graph_manager.populate(client)
    except Exception as e:
        logger.warning("startup_graph_populate_failed", error=str(e))

threading.Thread(target=_bg_graph_populate, daemon=True).start()

# ── Optionally initialize GreenLake client ────────────────────────────
glp_client: GreenLakeClient | None = None
if settings.has_glp_credentials:
    try:
        glp_client = GreenLakeClient(
            settings.glp_base_url,
            settings.effective_glp_client_id,
            settings.effective_glp_client_secret,
        )
        glp_client.validate()
        logger.info("glp_credentials_validated")
    except Exception as exc:
        logger.warning(
            "glp_validation_failed",
            error=str(exc),
            hint="GreenLake features will be unavailable. Central features still work.",
        )
        glp_client = None
else:
    logger.info("glp_credentials_not_configured", hint="GreenLake features disabled")

# Ensure script library exists and central_helpers.py is available
settings.script_library_path.mkdir(parents=True, exist_ok=True)

_helpers_src = Path(__file__).parent / "central_helpers.py"
_helpers_dst = settings.script_library_path / "central_helpers.py"
if _helpers_src.exists():
    shutil.copy2(_helpers_src, _helpers_dst)
    logger.info("central_helpers_copied", dest=str(_helpers_dst))

# Register all components
register_graph_tools(mcp, settings, client, graph_manager)
register_script_tools(mcp, settings)
register_execution_tools(mcp, settings)
register_catalog_tools(mcp, settings)
register_api_call_tools(mcp, settings, client)
register_greenlake_api_call_tools(mcp, settings, glp_client)
register_resources(mcp, settings)
register_graph_resources(mcp, graph_manager)
register_prompts(mcp)

logger.info(
    "server_ready",
    credentials_configured=settings.has_credentials,
    glp_configured=glp_client is not None,
    script_library=str(settings.script_library_path),
)


def main():
    """Entry point for the MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
