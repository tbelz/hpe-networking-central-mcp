"""HPE Networking Central MCP Server - API Discovery + Code Interpreter Pattern."""

from __future__ import annotations

import atexit
import graphlib
import json
import shutil
import sys
import threading
from pathlib import Path

from mcp.server.fastmcp import FastMCP

from .api_tree import render_path_tree
from .central_client import CentralClient, GreenLakeClient
from .config import load_settings
from .graph import GraphManager
from .graph.ipc_server import GraphIPCServer
from .instructions import build_instructions
from .knowledge_db import download_knowledge_db
from .logging import setup_logging
from .prompts.workflows import register_prompts
from .resources.docs import register_api_catalog_resource, register_resources
from .resources.graph import register_graph_resources
from .tools.api_call import register_api_call_tools, register_greenlake_api_call_tools
from .tools.api_catalog import register_catalog_tools
from .tools.execution import register_execution_tools, _run_script
from .tools.graph import register_graph_tools
from .tools.scripts import register_script_tools, sync_seeds_to_graph

logger = setup_logging()

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
# Try to download knowledge DB before initializing graph
knowledge_downloaded = download_knowledge_db(
    settings.knowledge_release_repo, settings.graph_db_path, logger=logger
)

# Initialize file-backed graph database
graph_manager = GraphManager(settings.graph_db_path)
graph_manager.initialize()
graph_manager.create_fts_indexes()


# ── Knowledge DB schema-version check ────────────────────────────────
# Version 3 introduces normalized OAS specs and the bodySkeletonJson /
# bodyGlossaryJson columns required by get_api_endpoint_detail and
# get_api_endpoint_glossary.  An older DB will lack those columns and
# the per-endpoint detail tools will fail at query time — refuse to
# start so the operator notices immediately rather than seeing
# silent fallbacks for weeks.
_KNOWLEDGE_SCHEMA_VERSION = 3


def _check_knowledge_schema_version() -> None:
    manifest_path = settings.graph_db_path.parent / "manifest.json"
    if not manifest_path.exists():
        logger.info("knowledge_manifest_missing", path=str(manifest_path))
        return
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("knowledge_manifest_unreadable", error=str(exc))
        return
    found = manifest.get("schema_version")
    if found != _KNOWLEDGE_SCHEMA_VERSION:
        msg = (
            f"Knowledge DB schema_version={found!r} does not match "
            f"server-required version {_KNOWLEDGE_SCHEMA_VERSION}. "
            "Re-run scripts/build_knowledge_db.py or wait for the next "
            "knowledge-db release. Refusing to start to avoid serving "
            "broken get_api_endpoint_detail / get_api_endpoint_glossary "
            "responses."
        )
        logger.error("knowledge_schema_version_mismatch", expected=_KNOWLEDGE_SCHEMA_VERSION, found=found)
        raise SystemExit(msg)


_check_knowledge_schema_version()


# ── Render API endpoint catalog as a path-tree for the system instructions ──
def _load_api_tree() -> str:
    """Query all ApiEndpoint rows and render them as a category-grouped path-tree.

    The result is embedded into the MCP server's system instructions so the
    agent always sees the full set of available endpoints without needing
    to call a search tool first.
    """
    try:
        rows = graph_manager.query(
            "MATCH (e:ApiEndpoint) "
            "RETURN e.method AS method, e.path AS path, "
            "e.category AS category, e.deprecated AS deprecated",
            read_only=True,
        )
    except Exception as exc:
        logger.warning("api_tree_query_failed", error=str(exc))
        return render_path_tree([], read_only=settings.read_only)

    text = render_path_tree(rows, read_only=settings.read_only)
    logger.info(
        "api_tree_rendered",
        endpoint_count=len(rows),
        chars=len(text),
        approx_tokens=len(text) // 4,
    )
    return text


_api_tree_text = _load_api_tree()

mcp = FastMCP(
    "hpe-networking-central-mcp",
    instructions=build_instructions(
        read_only=settings.read_only,
        api_tree=_api_tree_text,
    ),
)

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

# Ensure script library exists and central_helpers.py + _http_core.py are available
settings.script_library_path.mkdir(parents=True, exist_ok=True)

_pkg_dir = Path(__file__).parent
for _helper_name in ("_http_core.py", "central_helpers.py"):
    _helpers_src = _pkg_dir / _helper_name
    _helpers_dst = settings.script_library_path / _helper_name
    if _helpers_src.exists():
        shutil.copy2(_helpers_src, _helpers_dst)
        logger.info("helper_copied", file=_helper_name, dest=str(_helpers_dst))

# Sync seed scripts into graph DB and disk library
_seeds_dir = Path(__file__).parent / "seeds"
if _seeds_dir.is_dir():
    sync_seeds_to_graph(graph_manager, _seeds_dir, settings.script_library_path)


# Run auto-run seed scripts in background to populate graph on startup
_seed_status: dict[str, dict] = {}  # filename -> {status, exit_code, error, started_at, finished_at}

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


def _update_script_node(script_name: str, finished: str, exit_code: int):
    """Update the Script graph node's last_run/last_exit_code after seed execution."""
    try:
        # `query()` defaults to read_only=True and rejects SET; use execute()
        # for the write path.
        graph_manager.execute(
            "MATCH (s:Script {filename: $fn}) SET s.last_run = $lr, s.last_exit_code = $ec",
            {"fn": script_name, "lr": finished, "ec": exit_code},
        )
    except Exception as exc:
        logger.debug("script_node_update_failed", filename=script_name, error=str(exc))


def _bg_auto_run_seeds():
    """Execute all auto_run seed scripts sequentially in a background thread."""
    import time as _time

    for script_name in _get_auto_run_seeds():
        logger.info("auto_run_seed_start", filename=script_name)
        started = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
        _seed_status[script_name] = {"status": "running", "started_at": started}
        try:
            result_json = _run_script(settings, script_name)
            result = json.loads(result_json)
            exit_code = result.get("exit_code", -1)
            finished = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
            _update_script_node(script_name, finished, exit_code)
            if exit_code == 0:
                logger.info("auto_run_seed_done", filename=script_name)
                _seed_status[script_name] = {
                    "status": "success",
                    "exit_code": 0,
                    "started_at": started,
                    "finished_at": finished,
                }
            else:
                stderr = result.get("stderr", "")[:500]
                logger.warning(
                    "auto_run_seed_failed",
                    filename=script_name,
                    exit_code=exit_code,
                    stderr=stderr,
                )
                _seed_status[script_name] = {
                    "status": "failed",
                    "exit_code": exit_code,
                    "error": stderr or result.get("stdout", "")[:500],
                    "started_at": started,
                    "finished_at": finished,
                }
        except Exception as e:
            finished = _time.strftime("%Y-%m-%dT%H:%M:%SZ", _time.gmtime())
            logger.warning("auto_run_seed_error", filename=script_name, error=str(e))
            _seed_status[script_name] = {
                "status": "error",
                "error": str(e)[:500],
                "started_at": started,
                "finished_at": finished,
            }

    # Log summary at startup
    succeeded = sum(1 for s in _seed_status.values() if s["status"] == "success")
    failed = sum(1 for s in _seed_status.values() if s["status"] != "success")
    if failed:
        logger.error(
            "seed_startup_summary",
            succeeded=succeeded,
            failed=failed,
            failures={k: v.get("error", "") for k, v in _seed_status.items() if v["status"] != "success"},
        )
    else:
        logger.info("seed_startup_summary", succeeded=succeeded, failed=0)


# Register all components
register_execution_tools(mcp, settings)
register_graph_tools(mcp, settings, graph_manager)
register_script_tools(mcp, settings, graph_manager)
register_catalog_tools(mcp, settings, graph_manager)
register_api_call_tools(mcp, settings, client)
if glp_client is not None:
    register_greenlake_api_call_tools(mcp, settings, glp_client)
else:
    logger.info("greenlake_tools_disabled", reason="GreenLake credentials not configured")
register_resources(mcp, settings, graph_manager)
register_api_catalog_resource(mcp, settings, graph_manager)
register_graph_resources(mcp, graph_manager, lambda: _seed_status)
register_prompts(mcp, graph_manager)

# Start auto-run seeds in background AFTER tools are registered
threading.Thread(target=_bg_auto_run_seeds, daemon=True).start()

logger.info(
    "server_ready",
    credentials_configured=settings.has_credentials,
    glp_configured=glp_client is not None,
    knowledge_db_loaded=knowledge_downloaded,
    script_library=str(settings.script_library_path),
    read_only=settings.read_only,
)


def main():
    """Entry point for the MCP server."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
