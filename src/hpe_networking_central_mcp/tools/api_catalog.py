"""API catalog tools — queries ApiEndpoint/ApiCategory nodes in the graph database.

All API endpoint data is populated by the GitHub Actions knowledge DB builder.
At runtime, the MCP server reads from the graph; no scraping occurs.
"""

from __future__ import annotations

import json
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import TYPE_CHECKING, Any

import httpx
import structlog
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ..config import Settings

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.api_catalog")

_graph_manager: GraphManager | None = None


def register_catalog_tools(mcp: FastMCP, settings: Settings, graph_manager: GraphManager):
    """Register API discovery tools with the MCP server."""
    global _graph_manager
    _graph_manager = graph_manager

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def search_api_catalog(
        query: str,
        limit: int = 25,
        category: str = "",
    ) -> str:
        """Search the API catalog for endpoints matching a keyword query.

        Searches across endpoint paths, summaries, operation IDs, and categories.
        Use this to discover relevant endpoints before making API calls or writing
        scripts. Then call get_api_endpoint_detail(method, path) for full schemas.

        Args:
            query: Search keywords (e.g. "vlan", "wireless client", "firmware").
            limit: Max results to return (default 25).
            category: Optional category filter (e.g. "Monitoring", "Security").

        Returns:
            JSON with matching endpoints and total count.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available."})

        q = query.strip()
        if not q:
            return json.dumps({"error": "Query must not be empty."})

        # Build Cypher with CONTAINS filters on multiple fields
        cat_filter = "AND e.category CONTAINS $cat " if category.strip() else ""
        cypher = (
            "MATCH (e:ApiEndpoint) "
            "WHERE (e.path CONTAINS $q OR e.summary CONTAINS $q "
            "OR e.operationId CONTAINS $q OR e.category CONTAINS $q) "
            f"{cat_filter}"
            "RETURN e.method, e.path, e.summary, e.category, e.operationId "
            "ORDER BY e.category, e.path "
            f"LIMIT {min(max(limit, 1), 100)}"
        )
        params: dict[str, str] = {"q": q}
        if category.strip():
            params["cat"] = category.strip()

        rows = gm.query(cypher, params, read_only=True)
        if not rows:
            return json.dumps({
                "match_count": 0,
                "endpoints": [],
                "hint": "Try broader keywords or call list_api_categories() to see available categories.",
            })

        endpoints = [
            {
                "method": r.get("e.method", ""),
                "path": r.get("e.path", ""),
                "summary": r.get("e.summary", ""),
                "category": r.get("e.category", ""),
            }
            for r in rows
        ]
        return json.dumps({
            "match_count": len(endpoints),
            "endpoints": endpoints,
        })

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def list_api_categories() -> str:
        """List all API categories with endpoint counts.

        Use this to understand what API areas are available, then
        search_api_catalog(query, category=...) to drill down.

        Returns:
            JSON with category names, counts, and total endpoints.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available."})

        rows = gm.query(
            "MATCH (e:ApiEndpoint) "
            "RETURN e.category AS category, count(e) AS cnt "
            "ORDER BY category",
            read_only=True,
        )
        if not rows:
            return json.dumps({
                "error": "API catalog is empty. Run refresh_knowledge_db() to download it.",
            })

        categories = [
            {"name": r["category"], "endpoint_count": r["cnt"]}
            for r in rows
        ]
        total = sum(c["endpoint_count"] for c in categories)
        return json.dumps({
            "total_endpoints": total,
            "categories": categories,
        })

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_api_endpoint_detail(
        method: str,
        path: str,
    ) -> str:
        """Get full details for a specific API endpoint.

        Returns parameters, summary, description, and tags.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE).
            path: Full API path (e.g. "/monitoring/v2/aps").

        Returns:
            JSON with full endpoint detail.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available."})

        rows = gm.query(
            "MATCH (e:ApiEndpoint {endpoint_id: $eid}) "
            "RETURN e.method, e.path, e.summary, e.description, e.operationId, "
            "e.category, e.deprecated, e.tags, e.parameterNames, e.hasRequestBody",
            {"eid": f"{method.upper()}:{path}"},
            read_only=True,
        )
        if not rows:
            return json.dumps({
                "error": f"No endpoint found for {method.upper()} {path}.",
                "hint": "Use search_api_catalog(query) to find the correct path.",
            })

        r = rows[0]
        d: dict[str, Any] = {
            "method": r.get("e.method", ""),
            "path": r.get("e.path", ""),
            "summary": r.get("e.summary", ""),
            "category": r.get("e.category", ""),
            "operation_id": r.get("e.operationId", ""),
        }
        if r.get("e.description"):
            d["description"] = r["e.description"]
        if r.get("e.tags"):
            d["tags"] = r["e.tags"]
        if r.get("e.deprecated"):
            d["deprecated"] = True
        if r.get("e.parameterNames"):
            d["parameter_names"] = r["e.parameterNames"]
        if r.get("e.hasRequestBody"):
            d["has_request_body"] = True

        return json.dumps(d, indent=2)

    @mcp.tool(annotations=ToolAnnotations(destructiveHint=True))
    def refresh_knowledge_db() -> str:
        """Download the latest knowledge database from GitHub releases.

        Checks for a newer release, downloads the knowledge DB tar.gz,
        swaps the graph database, and re-syncs seed scripts.  Use this
        after a new knowledge DB has been published by the CI pipeline.

        Returns:
            JSON with version info, endpoint count, and what changed.
        """
        gm = _graph_manager
        if gm is None:
            return json.dumps({"error": "Graph manager not available."})

        repo = settings.knowledge_release_repo
        if not repo:
            return json.dumps({
                "error": "KNOWLEDGE_RELEASE_REPO not configured.",
                "hint": "Set the KNOWLEDGE_RELEASE_REPO environment variable to owner/repo.",
            })

        # Read current manifest (if any)
        manifest_path = settings.graph_db_path.parent / "manifest.json"
        old_manifest: dict[str, Any] = {}
        if manifest_path.exists():
            try:
                old_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        # Fetch latest release info
        api_url = f"https://api.github.com/repos/{repo}/releases/latest"
        try:
            resp = httpx.get(api_url, timeout=30, follow_redirects=True)
            resp.raise_for_status()
            release = resp.json()
        except Exception as exc:
            return json.dumps({"error": f"Failed to fetch release info: {exc}"})

        release_tag = release.get("tag_name", "unknown")

        # Check if already up to date
        if old_manifest.get("version") == release_tag:
            return json.dumps({
                "status": "up_to_date",
                "version": release_tag,
                "endpoint_count": old_manifest.get("endpoint_count", 0),
            })

        # Find download URL
        asset_url = None
        for asset in release.get("assets", []):
            if asset["name"] == "knowledge_db.tar.gz":
                asset_url = asset["browser_download_url"]
                break

        if not asset_url:
            return json.dumps({"error": f"No knowledge_db.tar.gz in release {release_tag}."})

        # Download and extract
        logger.info("knowledge_db_refresh_start", tag=release_tag)
        try:
            with tempfile.TemporaryDirectory() as tmp:
                tar_path = Path(tmp) / "knowledge_db.tar.gz"
                with httpx.stream("GET", asset_url, timeout=120, follow_redirects=True) as r:
                    r.raise_for_status()
                    with open(tar_path, "wb") as f:
                        for chunk in r.iter_bytes(chunk_size=65536):
                            f.write(chunk)

                with tarfile.open(tar_path, "r:gz") as tf:
                    for member in tf.getmembers():
                        if member.name.startswith("/") or ".." in member.name:
                            raise ValueError(f"Unsafe tar member: {member.name}")
                    tf.extractall(tmp)

                extracted_db = Path(tmp) / "knowledge_db"
                if not extracted_db.exists():
                    return json.dumps({"error": "knowledge_db not found in archive."})

                # Swap the database
                gm.replace_db(extracted_db)

                # Copy manifest if present
                extracted_manifest = Path(tmp) / "manifest.json"
                if extracted_manifest.exists():
                    shutil.copy2(extracted_manifest, settings.graph_db_path.parent / "manifest.json")
        except Exception as exc:
            logger.error("knowledge_db_refresh_failed", error=str(exc))
            return json.dumps({"error": f"Download/extract failed: {exc}"})

        # Re-sync seeds
        from .scripts import sync_seeds_to_graph
        seeds_dir = Path(__file__).resolve().parent.parent / "seeds"
        if seeds_dir.is_dir():
            sync_seeds_to_graph(gm, seeds_dir, settings.script_library_path)

        # Read new manifest
        new_manifest: dict[str, Any] = {}
        new_manifest_path = settings.graph_db_path.parent / "manifest.json"
        if new_manifest_path.exists():
            try:
                new_manifest = json.loads(new_manifest_path.read_text(encoding="utf-8"))
            except Exception:
                pass

        logger.info("knowledge_db_refresh_done", tag=release_tag)

        result: dict[str, Any] = {
            "status": "updated",
            "version": release_tag,
            "endpoint_count": new_manifest.get("endpoint_count", 0),
            "category_count": new_manifest.get("category_count", 0),
            "built_at": new_manifest.get("built_at", ""),
        }
        if old_manifest:
            old_count = old_manifest.get("endpoint_count", 0)
            new_count = new_manifest.get("endpoint_count", 0)
            if old_count != new_count:
                result["endpoint_delta"] = new_count - old_count
            result["previous_version"] = old_manifest.get("version", "unknown")

        return json.dumps(result, indent=2)
