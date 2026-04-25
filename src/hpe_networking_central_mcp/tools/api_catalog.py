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
        scope: str = "data",
        limit: int = 20,
    ) -> str:
        """Full-text search the documentation or graph data by keyword.

        Use this to find sections of the VSG documentation, or to look up
        devices / sites / config profiles / scripts in the graph by name.
        For API endpoints, scan the **API Endpoint Catalog** embedded in
        the system instructions instead — it lists every reachable
        ``METHOD /path``.

        Args:
            query: Search term (e.g., "vlan", "switch", "Site-NYC").
            scope: "data" (default), "docs", or "all".
            limit: Maximum results to return (default 20, max 50).

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

        valid_scopes = {"docs", "data", "all"}
        if scope not in valid_scopes:
            return json.dumps({
                "error": (
                    f"Invalid scope '{scope}'. Must be one of: "
                    f"{', '.join(sorted(valid_scopes))}. "
                    "For API endpoints, scan the API Endpoint Catalog in the "
                    "system instructions and call get_api_endpoint_detail()."
                ),
            })

        limit = max(1, min(limit, 50))

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

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def list_api_categories() -> str:
        """List all API categories with their endpoint counts.

        Returns every category name and how many endpoints it contains.
        Use this to discover what API areas are available; then scan the
        API Endpoint Catalog in the system instructions for the exact
        ``METHOD /path``.

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

    # ── Shared resolver for the (method, path) / endpoints argument shape ──

    def _resolve_request(
        method: str | None,
        path: str | None,
        endpoints: list[dict] | None,
    ) -> tuple[list[tuple[str, str]] | None, str | None, bool]:
        """Return (requested_pairs, error_json, is_bulk) for a tool call."""
        if endpoints is not None:
            if not isinstance(endpoints, list) or not endpoints:
                return None, json.dumps({
                    "error": "`endpoints` must be a non-empty list of {method, path} objects.",
                }), True
            requested: list[tuple[str, str]] = []
            for item in endpoints:
                if not isinstance(item, dict) or "method" not in item or "path" not in item:
                    return None, json.dumps({
                        "error": "Each entry in `endpoints` must be an object with 'method' and 'path' keys.",
                    }), True
                requested.append((str(item["method"]).upper(), str(item["path"])))
            return requested, None, True
        if not method or not path:
            return None, json.dumps({
                "error": "Provide either (method, path) or `endpoints=[...]`.",
            }), False
        return [(method.upper(), path)], None, False

    def _filter_read_only(
        requested: list[tuple[str, str]],
    ) -> tuple[list[tuple[str, str]], list[dict]]:
        if not settings.read_only:
            return requested, []
        allowed: list[tuple[str, str]] = []
        skipped: list[dict] = []
        for m, p in requested:
            if m == "GET":
                allowed.append((m, p))
            else:
                skipped.append({"method": m, "path": p})
        return allowed, skipped

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_api_endpoint_detail(
        method: str | None = None,
        path: str | None = None,
        endpoints: list[dict] | None = None,
    ) -> str:
        """Get the structural skeleton of one or more API endpoints.

        Returns parameters, request body schema, success and first-error
        response shapes, and a transitive ``$components`` side-table —
        with all human-readable prose (descriptions, titles, examples)
        stripped at every nested level.  The operation-level ``summary``
        is intentionally preserved as a one-line label so an agent can
        recognise the endpoint without a second tool call.  An agent can
        map configuration values onto field names directly from
        names + types + enums alone, which keeps the payload small enough
        to fit several endpoints into one prompt.

        For ambiguous field names, follow up with
        ``get_api_endpoint_glossary(method, path)`` to fetch the
        descriptions on demand.  Most workflows do not need it.

        Two call forms are supported:

        1. **Single**: pass ``method`` and ``path``.
           Returns one JSON object with the endpoint skeleton.
        2. **Bulk**: pass ``endpoints`` as a list of
           ``{"method": "GET", "path": "/foo"}`` objects.
           Returns ``{"endpoints": [...], "missing": [...]}``.

        DELETE / POST / PUT / PATCH endpoints are catalogued and callable
        via ``call_central_api`` when ``READ_ONLY=false``.  In
        ``READ_ONLY=true`` mode, non-GET endpoints are skipped (single form
        returns an error; bulk form lists them under ``skipped_read_only``).

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE). Single form.
            path: Full API path (e.g. "/monitoring/v2/aps"). Single form.
            endpoints: List of {"method", "path"} dicts. Bulk form.

        Returns:
            JSON with endpoint skeleton(s).
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available.",
                               "hint": "The graph database may still be loading. Try again shortly."})

        requested, err, is_bulk = _resolve_request(method, path, endpoints)
        if err is not None:
            return err
        assert requested is not None

        requested, skipped_read_only = _filter_read_only(requested)
        if not requested:
            if is_bulk:
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

        eids = [f"{m}:{p}" for m, p in requested]
        rows = gm.query(
            "MATCH (e:ApiEndpoint) WHERE e.endpoint_id IN $eids "
            "RETURN e.method, e.path, e.category, e.bodySkeletonJson",
            {"eids": eids},
            read_only=True,
        )

        details_by_eid: dict[str, dict] = {}
        # Track every eid the DB returned a row for (even if the blob was
        # broken) so we can distinguish "endpoint unknown" from "blob corrupt".
        found_eids: set[str] = set()
        for r in rows:
            method_val = r.get("e.method", "")
            path_val = r.get("e.path", "")
            eid_key = f"{method_val}:{path_val}"
            found_eids.add(eid_key)
            blob = r.get("e.bodySkeletonJson") or ""
            if not blob:
                logger.warning(
                    "endpoint_skeleton_blob_missing",
                    method=method_val,
                    path=path_val,
                )
                continue
            try:
                d = json.loads(blob)
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "endpoint_skeleton_blob_invalid",
                    method=method_val,
                    path=path_val,
                    error=str(exc),
                )
                continue
            d.setdefault("category", r.get("e.category", ""))
            details_by_eid[eid_key] = d

        if not is_bulk:
            eid = eids[0]
            if eid not in details_by_eid:
                if eid in found_eids:
                    return json.dumps({
                        "error": (
                            f"Endpoint {requested[0][0]} {requested[0][1]} exists "
                            "but its skeleton blob is missing or corrupt."
                        ),
                        "hint": "Rebuild the knowledge DB to regenerate the skeleton data.",
                    })
                return json.dumps({
                    "error": f"No endpoint found for {requested[0][0]} {requested[0][1]}.",
                    "hint": "Scan the API Endpoint Catalog in the system instructions for the correct method/path.",
                })
            return json.dumps(details_by_eid[eid], indent=2)

        ordered_details = []
        missing = []
        broken = []
        for m, p in requested:
            eid = f"{m}:{p}"
            if eid in details_by_eid:
                ordered_details.append(details_by_eid[eid])
            elif eid in found_eids:
                broken.append({"method": m, "path": p})
            else:
                missing.append({"method": m, "path": p})

        response: dict[str, Any] = {
            "endpoints": ordered_details,
            "missing": missing,
        }
        if broken:
            response["blob_corrupt"] = broken
            response["hint"] = "Rebuild the knowledge DB to regenerate skeleton data for blob_corrupt entries."
        if skipped_read_only:
            response["skipped_read_only"] = skipped_read_only
        return json.dumps(response, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_api_endpoint_glossary(
        method: str | None = None,
        path: str | None = None,
        endpoints: list[dict] | None = None,
        components: list[str] | None = None,
    ) -> str:
        """Get human-readable descriptions for the components of one or more endpoints.

        Returns descriptions, enum value lists, and ``x-mutually-exclusive``
        annotations for the schemas reachable from the endpoint(s) — the
        prose that ``get_api_endpoint_detail`` strips.  Use this only when
        a field name in the skeleton is ambiguous.  Most workflows do not
        need to call it.

        Two call forms (matching ``get_api_endpoint_detail``):

        1. **Single**: pass ``method`` and ``path``.
        2. **Bulk**: pass ``endpoints`` as a list of
           ``{"method": "GET", "path": "/foo"}`` objects.

        Args:
            method: HTTP method. Single form.
            path: Full API path. Single form.
            endpoints: List of {"method", "path"} dicts. Bulk form.
            components: Optional list of component names to restrict the
                glossary to.  Names not present in the endpoint's reachable
                components are silently ignored.

        Returns:
            JSON with per-component descriptions.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available.",
                               "hint": "The graph database may still be loading. Try again shortly."})

        requested, err, is_bulk = _resolve_request(method, path, endpoints)
        if err is not None:
            return err
        assert requested is not None

        requested, skipped_read_only = _filter_read_only(requested)
        if not requested:
            if is_bulk:
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

        eids = [f"{m}:{p}" for m, p in requested]
        rows = gm.query(
            "MATCH (e:ApiEndpoint) WHERE e.endpoint_id IN $eids "
            "RETURN e.method, e.path, e.bodyGlossaryJson",
            {"eids": eids},
            read_only=True,
        )

        wanted_components = set(components) if components is not None else None

        details_by_eid: dict[str, dict] = {}
        # Track every eid the DB returned a row for (even if the blob was
        # broken) so we can distinguish "endpoint unknown" from "blob corrupt".
        found_eids: set[str] = set()
        for r in rows:
            method_val = r.get("e.method", "")
            path_val = r.get("e.path", "")
            eid_key = f"{method_val}:{path_val}"
            found_eids.add(eid_key)
            blob = r.get("e.bodyGlossaryJson") or ""
            if not blob:
                logger.warning(
                    "endpoint_glossary_blob_missing",
                    method=method_val,
                    path=path_val,
                )
                continue
            try:
                d = json.loads(blob)
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "endpoint_glossary_blob_invalid",
                    method=method_val,
                    path=path_val,
                    error=str(exc),
                )
                continue
            if wanted_components is not None and isinstance(d.get("components"), dict):
                d["components"] = {
                    name: entry
                    for name, entry in d["components"].items()
                    if name in wanted_components
                }
            details_by_eid[eid_key] = d

        if not is_bulk:
            eid = eids[0]
            if eid not in details_by_eid:
                if eid in found_eids:
                    return json.dumps({
                        "error": (
                            f"Endpoint {requested[0][0]} {requested[0][1]} exists "
                            "but its glossary blob is missing or corrupt."
                        ),
                        "hint": "Rebuild the knowledge DB to regenerate the glossary data.",
                    })
                return json.dumps({
                    "error": f"No endpoint found for {requested[0][0]} {requested[0][1]}.",
                    "hint": "Scan the API Endpoint Catalog in the system instructions for the correct method/path.",
                })
            return json.dumps(details_by_eid[eid], indent=2)

        ordered_details = []
        missing = []
        broken = []
        for m, p in requested:
            eid = f"{m}:{p}"
            if eid in details_by_eid:
                ordered_details.append(details_by_eid[eid])
            elif eid in found_eids:
                broken.append({"method": m, "path": p})
            else:
                missing.append({"method": m, "path": p})

        response: dict[str, Any] = {
            "endpoints": ordered_details,
            "missing": missing,
        }
        if broken:
            response["blob_corrupt"] = broken
            response["hint"] = "Rebuild the knowledge DB to regenerate glossary data for blob_corrupt entries."
        if skipped_read_only:
            response["skipped_read_only"] = skipped_read_only
        return json.dumps(response, indent=2)
