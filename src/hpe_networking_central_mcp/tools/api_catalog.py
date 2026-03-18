"""Postman-backed API catalog for dynamic endpoint discovery.

Fetches Postman collections on startup, parses them into a searchable
catalog, and exposes tools for the agent to explore available APIs.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from typing import Any

import httpx
import structlog
from mcp.types import ToolAnnotations

from ..config import POSTMAN_CONFIG_COLLECTION_ID, POSTMAN_MRT_COLLECTION_ID, Settings

logger = structlog.get_logger("tools.api_catalog")

POSTMAN_API_BASE = "https://api.getpostman.com/collections"


@dataclass
class CatalogEntry:
    """A single API endpoint extracted from a Postman collection."""

    method: str
    path: str
    name: str
    description: str
    folder: str  # top-level folder breadcrumb
    query_params: list[dict[str, str]] = field(default_factory=list)
    body_summary: str = ""


# Module-level catalog singleton
_catalog_entries: list[CatalogEntry] = []
_catalog_by_folder: dict[str, list[CatalogEntry]] = {}


def _fetch_collection(api_key: str, collection_id: str) -> dict:
    """Fetch a Postman collection via the Postman API."""
    resp = httpx.get(
        f"{POSTMAN_API_BASE}/{collection_id}",
        headers={"X-Api-Key": api_key},
        timeout=30.0,
    )
    resp.raise_for_status()
    return resp.json()["collection"]


def _extract_path(url_obj: Any) -> str:
    """Extract a clean API path from a Postman URL object."""
    if isinstance(url_obj, str):
        # Strip protocol and host/baseUrl template
        path = url_obj
        for prefix in ("{{baseUrl}}/", "{{baseUrl}}"):
            if path.startswith(prefix):
                path = path[len(prefix):]
        return path.lstrip("/")

    if isinstance(url_obj, dict):
        # Prefer the 'path' array for structured URLs
        path_parts = url_obj.get("path", [])
        if path_parts:
            segments = []
            for p in path_parts:
                # Postman uses :param for path variables — convert to {param}
                if isinstance(p, str) and p.startswith(":"):
                    segments.append("{" + p[1:] + "}")
                else:
                    segments.append(str(p))
            return "/".join(segments)
        # Fallback to raw URL string
        raw = url_obj.get("raw", "")
        return _extract_path(raw)

    return ""


def _extract_query_params(url_obj: Any) -> list[dict[str, str]]:
    """Extract query parameters from a Postman URL object."""
    if not isinstance(url_obj, dict):
        return []
    params = url_obj.get("query", [])
    return [
        {"key": p.get("key", ""), "description": p.get("description", "")}
        for p in params
        if isinstance(p, dict) and p.get("key")
    ]


def _summarize_body(request: dict) -> str:
    """Extract a short summary of the request body if present."""
    body = request.get("body", {})
    if not body or not isinstance(body, dict):
        return ""
    mode = body.get("mode", "")
    if mode == "raw":
        raw = body.get("raw", "")
        if len(raw) > 500:
            return raw[:500] + "..."
        return raw
    return f"[{mode}]" if mode else ""


def _parse_collection(collection: dict) -> list[CatalogEntry]:
    """Parse a Postman collection into CatalogEntry list."""
    entries: list[CatalogEntry] = []

    def walk(items: list, folder_path: str = "") -> None:
        for item in items:
            if "item" in item:
                # This is a folder — recurse
                subfolder = item.get("name", "")
                next_path = f"{folder_path} > {subfolder}" if folder_path else subfolder
                walk(item["item"], next_path)
            elif "request" in item:
                # This is a request
                req = item["request"]
                method = req.get("method", "GET")
                url_obj = req.get("url", "")
                path = _extract_path(url_obj)

                desc = req.get("description", "") or ""
                if isinstance(desc, dict):
                    desc = desc.get("content", "")
                # Truncate long descriptions
                if len(desc) > 200:
                    desc = desc[:200] + "..."

                entries.append(CatalogEntry(
                    method=method.upper(),
                    path=path,
                    name=item.get("name", ""),
                    description=desc,
                    folder=folder_path or "Uncategorized",
                    query_params=_extract_query_params(url_obj),
                    body_summary=_summarize_body(req),
                ))

    walk(collection.get("item", []))
    return entries


def _build_folder_index(entries: list[CatalogEntry]) -> dict[str, list[CatalogEntry]]:
    """Group entries by their top-level folder."""
    by_folder: dict[str, list[CatalogEntry]] = {}
    for e in entries:
        top = e.folder.split(" > ")[0] if e.folder else "Uncategorized"
        by_folder.setdefault(top, []).append(e)
    return dict(sorted(by_folder.items()))


def initialize_catalog(settings: Settings) -> None:
    """Fetch Postman collections and build the catalog. Called at startup."""
    global _catalog_entries, _catalog_by_folder

    if not settings.has_postman_key:
        logger.warning("catalog_skip_no_api_key")
        return

    try:
        logger.info("catalog_fetch_start")
        entries: list[CatalogEntry] = []

        for name, cid in [("MRT", POSTMAN_MRT_COLLECTION_ID), ("Config", POSTMAN_CONFIG_COLLECTION_ID)]:
            try:
                collection = _fetch_collection(settings.postman_api_key, cid)
                parsed = _parse_collection(collection)
                entries.extend(parsed)
                logger.info("catalog_collection_parsed", collection=name, endpoints=len(parsed))
            except Exception as e:
                logger.warning("catalog_collection_failed", collection=name, error=str(e))

        _catalog_entries = entries
        _catalog_by_folder = _build_folder_index(entries)
        logger.info("catalog_ready", total_endpoints=len(entries), folders=len(_catalog_by_folder))

    except Exception as e:
        logger.error("catalog_init_failed", error=str(e))


def get_catalog_overview() -> str:
    """Return a compact catalog overview for the agent, grouped by folder."""
    if not _catalog_entries:
        return "API catalog is empty. Use refresh_api_catalog() to fetch it, or check POSTMAN_API_KEY."

    lines = [f"# Central API Catalog ({len(_catalog_entries)} endpoints)\n"]
    for folder, entries in _catalog_by_folder.items():
        lines.append(f"\n## {folder}")
        for e in entries:
            desc_short = e.description.split("\n")[0][:80] if e.description else ""
            suffix = f" — {desc_short}" if desc_short else ""
            lines.append(f"  {e.method} /{e.path}{suffix}")

    return "\n".join(lines)


def register_catalog_tools(mcp, settings: Settings):
    """Register API discovery tools with the MCP server."""

    @mcp.resource("api://central/catalog")
    def api_catalog_resource() -> str:
        """Compact overview of all available Central API endpoints.

        Grouped by category (monitoring, configuration, troubleshooting, etc.).
        Read this first to understand what APIs are available, then use
        get_api_details() for specifics on any endpoint.
        """
        return get_catalog_overview()

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )
    def get_api_details(search: str) -> str:
        """Search the API catalog for endpoint details.

        Searches by path substring or keyword in endpoint name/description.
        Returns full details including query parameters and request body schema.

        Args:
            search: Search term — path fragment (e.g., "dhcp", "vlan") or keyword.

        Returns:
            JSON with matching endpoint details, or suggestions if no exact match.
        """
        if not _catalog_entries:
            return json.dumps({
                "error": "API catalog is empty. "
                "Use refresh_api_catalog() to populate it."
            })

        term = search.strip().lower()
        matches: list[dict] = []

        for e in _catalog_entries:
            searchable = f"{e.path} {e.name} {e.description} {e.folder}".lower()
            if term in searchable:
                detail: dict[str, Any] = {
                    "method": e.method,
                    "path": f"/{e.path}",
                    "name": e.name,
                    "folder": e.folder,
                    "description": e.description,
                }
                if e.query_params:
                    detail["query_params"] = e.query_params
                if e.body_summary:
                    detail["request_body"] = e.body_summary
                matches.append(detail)

        if not matches:
            # Suggest closest folder names
            folders = list(_catalog_by_folder.keys())
            return json.dumps({
                "message": f"No endpoints match '{search}'.",
                "hint": "Try a broader term or browse these categories.",
                "categories": folders,
            }, indent=2)

        if len(matches) > 20:
            # Too many — return compact list
            compact = [{"method": m["method"], "path": m["path"], "name": m["name"]} for m in matches]
            return json.dumps({
                "message": f"{len(matches)} endpoints match '{search}'. Showing compact list.",
                "hint": "Narrow your search for full details.",
                "endpoints": compact,
            }, indent=2)

        return json.dumps({"match_count": len(matches), "endpoints": matches}, indent=2)

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=True),
    )
    def refresh_api_catalog() -> str:
        """Re-fetch Postman collections and rebuild the API catalog.

        Use this if endpoints seem outdated or the catalog is empty.

        Returns:
            JSON with refresh status and endpoint count.
        """
        if not settings.has_postman_key:
            return json.dumps({
                "error": "POSTMAN_API_KEY not configured. Cannot fetch API catalog."
            })

        try:
            initialize_catalog(settings)
            return json.dumps({
                "status": "success",
                "total_endpoints": len(_catalog_entries),
                "categories": list(_catalog_by_folder.keys()),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Catalog refresh failed: {e}"})
