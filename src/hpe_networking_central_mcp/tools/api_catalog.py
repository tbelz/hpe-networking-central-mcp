"""API catalog tools — queries ApiEndpoint/ApiCategory nodes in the graph database.

All API endpoint data is populated by the GitHub Actions knowledge DB builder.
At runtime, the MCP server reads from the graph; no scraping occurs.
The unified_search tool also delegates to FTS helpers in search.py for
non-API scopes (docs, data).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ..config import Settings
from .search import fts_search, contains_search

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.api_catalog")

_graph_manager: GraphManager | None = None


def register_catalog_tools(mcp: FastMCP, settings: Settings, graph_manager: GraphManager):
    """Register API discovery tools with the MCP server."""
    global _graph_manager
    _graph_manager = graph_manager

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def unified_search(
        query: str,
        scope: str = "api",
        limit: int = 20,
        category: str | None = None,
    ) -> str:
        """Search the API catalog, documentation, or graph data by keyword.

        Default scope is "api" — searches API endpoints by path, summary,
        operationId, or category.  Use this BEFORE writing scripts or making
        API calls to discover relevant endpoints.  Then call
        get_api_endpoint_detail(method, path) for full parameter schemas.

        Other scopes use BM25 full-text search:
        - "docs": documentation sections
        - "data": devices, sites, config profiles, scripts in the graph
        - "all": everything

        Args:
            query: Search term (e.g., "vlan", "switch", "dhcp", "devices").
            scope: What to search — "api" (default), "docs", "data", or "all".
            limit: Maximum results to return (default 20, max 50).
            category: Optional category name to restrict API results (scope="api" only).

        Returns:
            JSON with matching results.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({
                "error": "Search not available — graph database not initialized.",
                "hint": "The graph database may still be loading. Try again shortly.",
            })

        if not query or not query.strip():
            return json.dumps({
                "error": "Query must not be empty.",
                "hint": "Provide a search term, e.g. unified_search(query='vlan').",
            })

        valid_scopes = {"api", "docs", "data", "all"}
        if scope not in valid_scopes:
            return json.dumps({
                "error": f"Invalid scope '{scope}'. Must be one of: {', '.join(sorted(valid_scopes))}.",
                "hint": "Use scope='api' (default) for endpoint discovery, 'data' for graph nodes, 'docs' for documentation.",
            })

        limit = max(1, min(limit, 50))

        # Non-API scopes delegate to FTS/CONTAINS helpers
        if scope != "api":
            if gm.fts_available:
                results = fts_search(gm, query, scope=scope, limit=limit)
                search_method = "fts"
                if not results:
                    results = contains_search(gm, query, scope=scope, limit=limit)
                    search_method = "contains_fallback"
            else:
                results = contains_search(gm, query, scope=scope, limit=limit)
                search_method = "contains"
            return json.dumps({
                "query": query,
                "scope": scope,
                "search_method": search_method,
                "total": len(results),
                "results": results,
            }, indent=2)

        # API scope: structured Cypher query
        cypher = (
            "MATCH (e:ApiEndpoint) "
            "WHERE (lower(e.path) CONTAINS lower($q) "
            "   OR lower(e.summary) CONTAINS lower($q) "
            "   OR lower(e.operationId) CONTAINS lower($q) "
            "   OR lower(e.category) CONTAINS lower($q)) "
        )
        params: dict[str, Any] = {"q": query, "lim": limit}

        if category:
            cypher += "AND e.category = $cat "
            params["cat"] = category

        cypher += "RETURN e.method, e.path, e.summary, e.category ORDER BY e.category, e.path LIMIT $lim"

        rows = gm.query(cypher, params, read_only=True)

        endpoints = [
            {
                "method": r.get("e.method", ""),
                "path": r.get("e.path", ""),
                "summary": r.get("e.summary", ""),
                "category": r.get("e.category", ""),
            }
            for r in rows
        ]

        if not endpoints:
            return json.dumps({
                "query": query,
                "returned_count": 0,
                "endpoints": [],
                "hint": "No matches. Try broader terms or use list_api_categories() to see available categories.",
            }, indent=2)

        return json.dumps({
            "query": query,
            "returned_count": len(endpoints),
            "endpoints": endpoints,
        }, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def list_api_categories() -> str:
        """List all API categories with their endpoint counts.

        Returns every category name and how many endpoints it contains.
        Use this to discover what API areas are available, then
        unified_search(query, category=...) to drill in.

        Returns:
            JSON with categories and total endpoint count.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "API catalog not available — graph database not initialized.",
                               "hint": "The graph database may still be loading. Try again shortly."})

        rows = gm.query(
            "MATCH (e:ApiEndpoint) "
            "RETURN e.category AS category, count(e) AS cnt "
            "ORDER BY category",
            read_only=True,
        )

        categories = {r["category"]: r["cnt"] for r in rows}
        total = sum(categories.values())
        return json.dumps({
            "categories": categories,
            "total_endpoints": total,
        }, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_api_endpoint_detail(
        method: str,
        path: str,
    ) -> str:
        """Get full details for a specific API endpoint.

        Returns the complete API specification including parameters (with types,
        location, required flags), request body schema, and response status
        codes with their schemas.

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE).
            path: Full API path (e.g. "/monitoring/v2/aps").

        Returns:
            JSON with full endpoint specification.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available.",
                               "hint": "The graph database may still be loading. Try again shortly."})

        rows = gm.query(
            "MATCH (e:ApiEndpoint {endpoint_id: $eid}) "
            "RETURN e.method, e.path, e.summary, e.description, e.operationId, "
            "e.category, e.deprecated, e.tags, e.parameters, e.requestBody, "
            "e.responses",
            {"eid": f"{method.upper()}:{path}"},
            read_only=True,
        )
        if not rows:
            return json.dumps({
                "error": f"No endpoint found for {method.upper()} {path}.",
                "hint": "Use unified_search(query) to find the correct path.",
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

        # Deserialize full schema JSON fields
        params_raw = r.get("e.parameters", "")
        if params_raw:
            try:
                d["parameters"] = json.loads(params_raw)
            except (json.JSONDecodeError, TypeError):
                d["parameters"] = []

        body_raw = r.get("e.requestBody", "")
        if body_raw:
            try:
                d["request_body"] = json.loads(body_raw)
            except (json.JSONDecodeError, TypeError):
                pass

        responses_raw = r.get("e.responses", "")
        if responses_raw:
            try:
                d["responses"] = json.loads(responses_raw)
            except (json.JSONDecodeError, TypeError):
                d["responses"] = []

        return json.dumps(d, indent=2)
