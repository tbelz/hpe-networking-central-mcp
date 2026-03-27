"""HPE Networking Central MCP Server - API Discovery + Code Interpreter Pattern."""

from __future__ import annotations

import atexit
import graphlib
import json
import shutil
import sys
import tarfile
import tempfile
import threading
from pathlib import Path

import httpx
from mcp.server.fastmcp import FastMCP

from .central_client import CentralClient, GreenLakeClient
from .config import load_settings
from .graph import GraphManager
from .graph.ipc_server import GraphIPCServer
from .logging import setup_logging
from .prompts.workflows import register_prompts
from .resources.docs import register_resources
from .resources.graph import register_graph_resources
from .tools.api_call import register_api_call_tools, register_greenlake_api_call_tools
from .tools.api_catalog import register_catalog_tools
from .tools.execution import register_execution_tools, _run_script
from .tools.graph import register_graph_tools
from .tools.scripts import register_script_tools, sync_seeds_to_graph
from .tools.search import register_search_tools

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

2. **Discover APIs**: Use search_api_catalog(query) to find endpoints by keyword (e.g.,
   "vlan", "switch", "dhcp"). Use list_api_categories() to see all API areas. Then use
   get_api_endpoint_detail(method, path) for full parameter and schema details.

3. **Quick reads**: Use call_central_api(path, query_params) for GET requests - monitoring queries,
   config lookups, health checks. This is the fastest way to read live data.
   Tip: Add `effective=true` to config endpoints for hierarchically merged config,
   and `detailed=true` for source annotations.
   For bulk effective-config analysis, prefer the graph: the `EFFECTIVE_CONFIG` relationships
   are pre-computed per device during seed population.  Use the API with `effective=true` and
   `detailed=true` only when you need authoritative per-device verification or suspect
   device-level overrides not yet captured in the graph.

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

8. **GreenLake Platform**: Use call_greenlake_api(path, query_params) for HPE GreenLake APIs
   (device onboarding, subscriptions, licenses, locations, service catalog). These hit
   https://global.api.greenlake.hpe.com. In scripts, use `from central_helpers import glp`.

9. **Graph enrichment**: The graph is populated by seed scripts at startup and can be
   enriched at any time using `write_graph(cypher, parameters)` to add nodes,
   relationships, or properties you discover during investigation.
   Use `list_scripts(tag="graph")` to find enrichment scripts.
   To refresh the graph from scratch, execute `refresh_graph()` — this resets and re-runs
   all auto-run seed scripts.  For custom enrichments, either use `write_graph()` directly
   or write scripts that use `from central_helpers import graph`.

10. **Reuse**: Always check list_scripts() before writing a new script.
    Use get_script_content() to inspect existing scripts and learn patterns.

Read docs://script-writing-guide for the script template and authentication pattern.
Scripts use `from central_helpers import api, glp, graph` — no OAuth2 boilerplate needed.

## MANDATORY: Research before scripting

Before writing ANY script you MUST complete these steps IN ORDER:

1. `list_scripts()` — check if a seed or saved script already solves the task.
2. `search_api_catalog(query)` — find relevant endpoints. NEVER guess API paths.
3. `get_api_endpoint_detail(method, path)` — get exact parameter schemas and response shapes.
4. Only THEN write the script using the discovered endpoints and schemas.

Skipping these steps leads to wrong endpoints, wrong parameter names, and wasted iterations.

## Pagination Rule

NEVER pass a `limit` parameter to `call_central_api()` for fetching collections.
For any operation that lists multiple items, write a script using `api.paginate(path)`.
The paginate helper auto-detects cursor vs offset pagination with safe page_size=100.
`call_central_api()` is for single-item lookups and one-off mutations only.""",
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

# ── Download knowledge DB from GitHub release (if configured) ─────────
def _download_knowledge_db(repo: str, db_path: Path) -> bool:
    """Download the latest knowledge DB tar.gz from a GitHub release.

    Returns True if a DB was downloaded and extracted, False otherwise.
    """
    if not repo:
        logger.info("knowledge_db_skip", reason="KNOWLEDGE_RELEASE_REPO not set")
        return False

    api_url = f"https://api.github.com/repos/{repo}/releases/latest"
    try:
        resp = httpx.get(api_url, timeout=30, follow_redirects=True)
        resp.raise_for_status()
        release = resp.json()
    except Exception as exc:
        logger.warning("knowledge_db_fetch_failed", error=str(exc))
        return False

    # Find the knowledge_db.tar.gz asset
    asset_url = None
    for asset in release.get("assets", []):
        if asset["name"] == "knowledge_db.tar.gz":
            asset_url = asset["browser_download_url"]
            break

    if not asset_url:
        logger.warning("knowledge_db_no_asset", release=release.get("tag_name"))
        return False

    logger.info("knowledge_db_downloading", url=asset_url)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tar_path = Path(tmp) / "knowledge_db.tar.gz"
            with httpx.stream("GET", asset_url, timeout=120, follow_redirects=True) as r:
                r.raise_for_status()
                with open(tar_path, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=65536):
                        f.write(chunk)

            with tarfile.open(tar_path, "r:gz") as tf:
                # Security: validate member paths to prevent path traversal
                for member in tf.getmembers():
                    if member.name.startswith("/") or ".." in member.name:
                        raise ValueError(f"Unsafe tar member: {member.name}")
                tf.extractall(tmp)

            extracted_db = Path(tmp) / "knowledge_db"
            if not extracted_db.exists():
                logger.warning("knowledge_db_extract_failed", reason="knowledge_db not found in archive")
                return False

            # Replace current DB (may be file or directory depending on LadybugDB version)
            if db_path.exists():
                if db_path.is_dir():
                    shutil.rmtree(db_path)
                else:
                    db_path.unlink()
            if extracted_db.is_dir():
                shutil.copytree(extracted_db, db_path)
            else:
                shutil.copy2(extracted_db, db_path)

            # Copy manifest.json if present in archive
            extracted_manifest = Path(tmp) / "manifest.json"
            if extracted_manifest.exists():
                shutil.copy2(extracted_manifest, db_path.parent / "manifest.json")

            logger.info("knowledge_db_installed", tag=release.get("tag_name"))
            return True
    except Exception as exc:
        logger.warning("knowledge_db_download_failed", error=str(exc))
        return False


# Try to download knowledge DB before initializing graph
knowledge_downloaded = _download_knowledge_db(
    settings.knowledge_release_repo, settings.graph_db_path
)

# Initialize file-backed graph database
graph_manager = GraphManager(settings.graph_db_path)
graph_manager.initialize()
graph_manager.create_fts_indexes()

# Start IPC server for script subprocesses
ipc_server = GraphIPCServer(settings.graph_ipc_socket, graph_manager)
ipc_server.start()
atexit.register(ipc_server.stop)

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

# Sync seed scripts into graph DB and disk library
_seeds_dir = Path(__file__).parent / "seeds"
if _seeds_dir.is_dir():
    sync_seeds_to_graph(graph_manager, _seeds_dir, settings.script_library_path)


# Run auto-run seed scripts in background to populate graph on startup
def _get_auto_run_seeds() -> list[str]:
    """Return seed script filenames in dependency order (topological sort)."""
    lib = settings.script_library_path
    auto_seeds: dict[str, list[str]] = {}  # script_name -> depends_on
    for meta_file in sorted(lib.glob("*.meta.json")):
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            if meta.get("auto_run"):
                script_name = meta_file.name.replace(".meta.json", ".py")
                if (lib / script_name).exists():
                    deps = meta.get("depends_on", [])
                    auto_seeds[script_name] = deps
        except Exception:
            continue

    # Topological sort: only include dependencies that are in the auto_run set
    graph: dict[str, set[str]] = {}
    for name, deps in auto_seeds.items():
        valid_deps = {d for d in deps if d in auto_seeds}
        if len(valid_deps) < len(deps):
            missing = set(deps) - valid_deps
            logger.warning("seed_dep_not_auto_run", seed=name, missing=list(missing))
        graph[name] = valid_deps

    try:
        sorter = graphlib.TopologicalSorter(graph)
        ordered = list(sorter.static_order())
    except graphlib.CycleError as e:
        logger.error("seed_dependency_cycle", detail=str(e))
        ordered = sorted(auto_seeds.keys())  # fallback to alphabetical

    logger.info("auto_run_seed_order", order=ordered)
    return ordered


def _bg_auto_run_seeds():
    """Execute all auto_run seed scripts sequentially in a background thread."""
    for script_name in _get_auto_run_seeds():
        logger.info("auto_run_seed_start", filename=script_name)
        try:
            result_json = _run_script(settings, script_name)
            result = json.loads(result_json)
            exit_code = result.get("exit_code", -1)
            if exit_code == 0:
                logger.info("auto_run_seed_done", filename=script_name)
            else:
                logger.warning(
                    "auto_run_seed_failed",
                    filename=script_name,
                    exit_code=exit_code,
                    stderr=result.get("stderr", "")[:500],
                )
        except Exception as e:
            logger.warning("auto_run_seed_error", filename=script_name, error=str(e))


# Register all components
register_execution_tools(mcp, settings)
register_graph_tools(mcp, settings, graph_manager)
register_script_tools(mcp, settings, graph_manager)
register_catalog_tools(mcp, settings, graph_manager)
register_api_call_tools(mcp, settings, client)
register_greenlake_api_call_tools(mcp, settings, glp_client)
register_search_tools(mcp, settings, graph_manager)
register_resources(mcp, settings)
register_graph_resources(mcp, graph_manager)
register_prompts(mcp, graph_manager)

# Start auto-run seeds in background AFTER tools are registered
threading.Thread(target=_bg_auto_run_seeds, daemon=True).start()

logger.info(
    "server_ready",
    credentials_configured=settings.has_credentials,
    glp_configured=glp_client is not None,
    knowledge_db_loaded=knowledge_downloaded,
    script_library=str(settings.script_library_path),
)


def main():
    """Entry point for the MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
