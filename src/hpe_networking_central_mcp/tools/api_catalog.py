"""OpenAPI-backed API catalog for dynamic endpoint discovery.

Scrapes OpenAPI specs from developer.arubanetworks.com at startup, builds
a searchable index, and exposes tools for the agent to explore available APIs.
"""

from __future__ import annotations

import json
from dataclasses import asdict
from typing import Any

import structlog
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ..config import Settings
from ..oas_index import CompactEntry, EndpointEntry, OASIndex
from ..oas_scraper import discover_and_scrape

logger = structlog.get_logger("tools.api_catalog")

# Module-level index singleton
_index = OASIndex()


def initialize_catalog(settings: Settings) -> None:
    """Scrape OpenAPI specs and build the searchable index. Called at startup."""
    try:
        logger.info("catalog_fetch_start")
        specs = discover_and_scrape(
            cache_dir=settings.spec_cache_dir,
            ttl=settings.spec_cache_ttl,
        )
        _index.build(specs)
        logger.info(
            "catalog_ready",
            total_endpoints=_index.total_endpoints,
            categories=list(_index.categories.keys()),
        )
    except Exception as e:
        logger.error("catalog_init_failed", error=str(e))


def _compact_to_dict(entry: CompactEntry) -> dict[str, Any]:
    d: dict[str, Any] = {
        "method": entry.method,
        "path": entry.path,
        "summary": entry.summary,
        "category": entry.category,
    }
    if entry.deprecated:
        d["deprecated"] = True
    return d


def _detail_to_dict(entry: EndpointEntry) -> dict[str, Any]:
    d: dict[str, Any] = {
        "method": entry.method,
        "path": entry.path,
        "summary": entry.summary,
        "category": entry.category,
        "operation_id": entry.operation_id,
    }
    if entry.description:
        d["description"] = entry.description
    if entry.tags:
        d["tags"] = entry.tags
    if entry.deprecated:
        d["deprecated"] = True
    if entry.parameters:
        d["parameters"] = [
            {k: v for k, v in asdict(p).items() if v} for p in entry.parameters
        ]
    if entry.request_body:
        d["request_body_schema"] = entry.request_body
    if entry.responses:
        d["responses"] = [
            {k: v for k, v in asdict(r).items() if v} for r in entry.responses
        ]
    return d


def register_catalog_tools(mcp: FastMCP, settings: Settings):
    """Register API discovery tools with the MCP server."""

    @mcp.resource("api://central/catalog")
    def api_catalog_resource() -> str:
        """Compact overview of all available Central API categories.

        Shows category names and endpoint counts. Use search_api_catalog()
        to find specific endpoints, and get_api_endpoint_detail() for full schemas.
        """
        if not _index.total_endpoints:
            return "API catalog is empty. Use refresh_api_catalog() to fetch it."

        lines = [f"# Central API Catalog ({_index.total_endpoints} endpoints)\n"]
        for cat, count in sorted(_index.categories.items()):
            lines.append(f"- **{cat}**: {count} endpoints")
        lines.append("\nUse search_api_catalog(query) to search, "
                      "get_api_endpoint_detail(method, path) for full schemas.")
        return "\n".join(lines)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def search_api_catalog(
        query: str,
        include_deprecated: bool = False,
    ) -> str:
        """Search the API catalog for endpoints matching a keyword.

        Returns a compact list of matching endpoints (method, path, summary).
        Use get_api_endpoint_detail() to get full parameter and schema details
        for a specific endpoint.

        Args:
            query: Search term — path fragment (e.g. "dhcp", "vlan") or keyword.
            include_deprecated: Include deprecated endpoints in results.

        Returns:
            JSON with matching endpoints or suggestions.
        """
        if not _index.total_endpoints:
            return json.dumps({
                "error": "API catalog is empty. Use refresh_api_catalog() to populate it.",
            })

        results = _index.search(query, include_deprecated=include_deprecated)
        if not results:
            return json.dumps({
                "message": f"No endpoints match '{query}'.",
                "hint": "Try a broader term or use list_api_categories() to browse.",
                "categories": list(_index.categories.keys()),
            }, indent=2)

        return json.dumps({
            "match_count": len(results),
            "endpoints": [_compact_to_dict(r) for r in results],
        }, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_api_endpoint_detail(
        method: str,
        path: str,
    ) -> str:
        """Get full details for a specific API endpoint.

        Returns parameters, request body schema, and response schemas with all
        $ref references fully resolved inline.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE).
            path: Full API path (e.g. "/monitoring/v2/aps").

        Returns:
            JSON with full endpoint detail including resolved schemas.
        """
        entry = _index.get_detail(method, path)
        if not entry:
            return json.dumps({
                "error": f"No endpoint found for {method.upper()} {path}.",
                "hint": "Use search_api_catalog() to find the correct path.",
            })

        return json.dumps(_detail_to_dict(entry), indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def list_api_categories() -> str:
        """List all API categories with endpoint counts.

        Returns:
            JSON with category names and their endpoint counts.
        """
        if not _index.total_endpoints:
            return json.dumps({
                "error": "API catalog is empty. Use refresh_api_catalog() to populate it.",
            })

        return json.dumps({
            "total_endpoints": _index.total_endpoints,
            "categories": _index.categories,
        }, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=True))
    def refresh_api_catalog() -> str:
        """Re-scrape OpenAPI specs and rebuild the API catalog.

        Use this if the catalog is empty or you want fresh data from the
        developer documentation site.

        Returns:
            JSON with refresh status, endpoint count, and categories.
        """
        try:
            initialize_catalog(settings)
            return json.dumps({
                "status": "success",
                "total_endpoints": _index.total_endpoints,
                "categories": list(_index.categories.keys()),
            }, indent=2)
        except Exception as e:
            return json.dumps({"error": f"Catalog refresh failed: {e}"})

