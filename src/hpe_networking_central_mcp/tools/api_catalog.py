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

_API_SCOPE_DEPRECATION_WARNING = (
    "unified_search(scope='api') is deprecated. The full API endpoint catalog "
    "is now embedded in the system instructions as a category-grouped path-tree. "
    "Scan it directly to find the right METHOD /path, then call "
    "get_api_endpoint_detail(...) for the full schema."
)


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
        method_filter = " AND e.method = 'GET' " if settings.read_only else " "
        cypher = (
            "MATCH (e:ApiEndpoint) "
            "WHERE (lower(e.path) CONTAINS lower($q) "
            "   OR lower(e.summary) CONTAINS lower($q) "
            "   OR lower(e.operationId) CONTAINS lower($q) "
            "   OR lower(e.category) CONTAINS lower($q)) "
            f"{method_filter}"
        )
        params: dict[str, Any] = {"q": query, "lim": limit}

        if category:
            cypher += "AND e.category = $cat "
            params["cat"] = category

        cypher += "RETURN e.method, e.path, e.summary, e.category ORDER BY e.category, e.path LIMIT $lim"

        rows = gm.query(cypher, params, read_only=True)

        if not rows:
            return json.dumps({
                "query": query,
                "returned_count": 0,
                "endpoints": [],
                "hint": "No matches. Try broader terms or use list_api_categories() to see available categories.",
                "deprecation_warning": _API_SCOPE_DEPRECATION_WARNING,
            }, indent=2)

        # Group multiple methods on the same path into one entry
        from collections import OrderedDict
        grouped: OrderedDict[str, dict] = OrderedDict()
        for r in rows:
            path = r.get("e.path", "")
            method = r.get("e.method", "")
            if path not in grouped:
                grouped[path] = {
                    "path": path,
                    "methods": [method],
                    "summary": r.get("e.summary", ""),
                    "category": r.get("e.category", ""),
                }
            else:
                grouped[path]["methods"].append(method)

        endpoints = list(grouped.values())

        return json.dumps({
            "query": query,
            "returned_count": len(endpoints),
            "endpoints": endpoints,
            "deprecation_warning": _API_SCOPE_DEPRECATION_WARNING,
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
            + ("WHERE e.method = 'GET' " if settings.read_only else "")
            + "RETURN e.category AS category, count(e) AS cnt "
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
        method: str | None = None,
        path: str | None = None,
        endpoints: list[dict] | None = None,
        view: str = "compact",
    ) -> str:
        """Get full details for one or more API endpoints.

        Returns the API specification: parameters (with types, location,
        required flags), request body schema, and response status codes
        with their schemas.

        Two call forms are supported:

        1. **Single**: pass ``method`` and ``path``.
           Returns one JSON object with the endpoint detail.
        2. **Bulk**: pass ``endpoints`` as a list of
           ``{"method": "GET", "path": "/foo"}`` objects.
           Returns ``{"endpoints": [...], "missing": [...]}`` with one detail
           object per matched endpoint and a ``missing`` list naming any
           endpoints that were not found in the catalog.

        ``view`` selects how much detail to return:

        - ``"compact"`` *(default)* — Repeated error responses and nested
          object schemas are kept as ``$ref`` and bundled in a
          ``$components`` side-table.  ~5–15 KB per endpoint, suitable for
          most workflows.
        - ``"request-only"`` — Just the request-body schema plus a flat
          ``required_paths`` list.  Use when you only need to construct a
          POST/PUT/PATCH payload.
        - ``"full"`` — Every ``$ref`` is fully resolved inline.  May exceed
          100 KB on heavy endpoints; use only when you need a single
          self-contained schema with no indirection.
        - ``"raw"`` — The untouched OpenAPI operation object plus the raw
          ``components`` table.  Diagnostic.

        DELETE endpoints are catalogued and callable via ``central_api_call``
        when ``READ_ONLY=false``.  In ``READ_ONLY=true`` mode, non-GET
        endpoints are silently skipped (single form returns an error; bulk
        form lists them under ``skipped_read_only``).

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE). Single form.
            path: Full API path (e.g. "/monitoring/v2/aps"). Single form.
            endpoints: List of {"method", "path"} dicts. Bulk form.
            view: One of "compact" (default), "request-only", "full", "raw".

        Returns:
            JSON with endpoint specification(s) under the chosen view.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available.",
                               "hint": "The graph database may still be loading. Try again shortly."})

        valid_views = {"compact", "request-only", "full", "raw"}
        if view not in valid_views:
            return json.dumps({
                "error": f"Invalid view '{view}'. Must be one of: {', '.join(sorted(valid_views))}.",
            })

        # ── Resolve call form ─────────────────────────────────────────
        if endpoints is not None:
            if not isinstance(endpoints, list) or not endpoints:
                return json.dumps({
                    "error": "`endpoints` must be a non-empty list of {method, path} objects.",
                })
            requested: list[tuple[str, str]] = []
            for item in endpoints:
                if not isinstance(item, dict) or "method" not in item or "path" not in item:
                    return json.dumps({
                        "error": "Each entry in `endpoints` must be an object with 'method' and 'path' keys.",
                    })
                requested.append((str(item["method"]).upper(), str(item["path"])))
        else:
            if not method or not path:
                return json.dumps({
                    "error": "Provide either (method, path) or `endpoints=[...]`.",
                })
            requested = [(method.upper(), path)]

        # ── READ_ONLY filter ──────────────────────────────────────────
        skipped_read_only: list[dict] = []
        if settings.read_only:
            allowed = []
            for m, p in requested:
                if m == "GET":
                    allowed.append((m, p))
                else:
                    skipped_read_only.append({"method": m, "path": p})
            requested = allowed
            if not requested:
                # All requested endpoints are non-GET in read-only mode
                if endpoints is not None:
                    return json.dumps({
                        "endpoints": [],
                        "missing": [],
                        "skipped_read_only": skipped_read_only,
                    }, indent=2)
                return json.dumps({
                    "error": (
                        f"Endpoint {skipped_read_only[0]['method']} "
                        f"{skipped_read_only[0]['path']} is hidden because the server "
                        "is in READ_ONLY mode. Only GET endpoints are exposed."
                    ),
                })

        # ── Bulk Cypher fetch ─────────────────────────────────────────
        eids = [f"{m}:{p}" for m, p in requested]

        # Try to read the projection columns introduced in schema_version 2;
        # fall back gracefully when running against an older DB.
        select_extra = ""
        if view in {"compact", "request-only"}:
            select_extra = ", e.bodyCompactJson, e.bodyRequestOnlyJson"

        try:
            rows = gm.query(
                "MATCH (e:ApiEndpoint) WHERE e.endpoint_id IN $eids "
                "RETURN e.method, e.path, e.summary, e.description, e.operationId, "
                "e.category, e.deprecated, e.tags, e.parameters, e.requestBody, "
                f"e.responses{select_extra}",
                {"eids": eids},
                read_only=True,
            )
        except Exception as exc:
            # Older DBs lack the projection columns — degrade silently to "full".
            logger.warning(
                "endpoint_detail_view_fallback",
                view=view,
                error=str(exc),
                hint="Projection columns missing — knowledge DB predates schema_version 2.",
            )
            rows = gm.query(
                "MATCH (e:ApiEndpoint) WHERE e.endpoint_id IN $eids "
                "RETURN e.method, e.path, e.summary, e.description, e.operationId, "
                "e.category, e.deprecated, e.tags, e.parameters, e.requestBody, "
                "e.responses",
                {"eids": eids},
                read_only=True,
            )
            view = "full"  # serve the legacy resolved view

        details_by_eid: dict[str, dict] = {}
        for r in rows:
            method_val = r.get("e.method", "")
            path_val = r.get("e.path", "")

            # ── view='compact' / 'request-only': prefer precomputed JSON ──
            if view == "compact":
                blob = r.get("e.bodyCompactJson") or ""
                if blob:
                    try:
                        d = json.loads(blob)
                        d.setdefault("category", r.get("e.category", ""))
                        details_by_eid[f"{method_val}:{path_val}"] = d
                        continue
                    except (json.JSONDecodeError, TypeError):
                        pass  # fall through to legacy assembly
            elif view == "request-only":
                blob = r.get("e.bodyRequestOnlyJson") or ""
                if blob:
                    try:
                        d = json.loads(blob)
                        d.setdefault("category", r.get("e.category", ""))
                        details_by_eid[f"{method_val}:{path_val}"] = d
                        continue
                    except (json.JSONDecodeError, TypeError):
                        pass  # fall through to legacy assembly

            # ── view='full' (or fallback): assemble from legacy columns ──
            d: dict[str, Any] = {
                "method": method_val,
                "path": path_val,
                "summary": r.get("e.summary", ""),
                "category": r.get("e.category", ""),
                "operation_id": r.get("e.operationId", ""),
                "view": "full",
            }
            if r.get("e.description"):
                d["description"] = r["e.description"]
            if r.get("e.tags"):
                d["tags"] = r["e.tags"]
            if r.get("e.deprecated"):
                d["deprecated"] = True

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
                    pass

            # view='raw' is not supported via the precomputed columns;
            # only the resolved 'full' shape is available from the cached DB.
            if view == "raw":
                d["view"] = "raw"
                d["note"] = (
                    "raw view is only available at build time; serving 'full' instead."
                )

            details_by_eid[f"{method_val}:{path_val}"] = d

        # ── Single-form response (preserve legacy shape) ─────────────
        if endpoints is None:
            eid = eids[0]
            if eid not in details_by_eid:
                return json.dumps({
                    "error": f"No endpoint found for {requested[0][0]} {requested[0][1]}.",
                    "hint": "Scan the API Endpoint Catalog in the system instructions for the correct method/path.",
                })
            return json.dumps(details_by_eid[eid], indent=2)

        # ── Bulk-form response ───────────────────────────────────────
        ordered_details = []
        missing = []
        for m, p in requested:
            eid = f"{m}:{p}"
            if eid in details_by_eid:
                ordered_details.append(details_by_eid[eid])
            else:
                missing.append({"method": m, "path": p})

        response: dict[str, Any] = {
            "endpoints": ordered_details,
            "missing": missing,
        }
        if skipped_read_only:
            response["skipped_read_only"] = skipped_read_only
        return json.dumps(response, indent=2)
