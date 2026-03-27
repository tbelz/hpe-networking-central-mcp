"""Unified search tools — FTS/BM25-ranked search across APIs, docs, and data."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ..config import Settings

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.search")

_graph_manager: "GraphManager | None" = None

# Primary key fields for domain node types
_PK_MAP: dict[str, str] = {
    "Device": "serial",
    "Site": "scopeId",
    "SiteCollection": "scopeId",
    "DeviceGroup": "scopeId",
    "ConfigProfile": "id",
    "UnmanagedDevice": "mac",
}

# Scope → (table, search_fields, display_fields)
_SCOPE_CONFIG: dict[str, list[tuple[str, list[str], list[str]]]] = {
    "api": [
        ("ApiEndpoint", ["path", "summary", "operationId", "description"], ["endpoint_id", "method", "path", "summary", "category"]),
    ],
    "docs": [
        ("DocSection", ["title", "content"], ["section_id", "title", "source", "url"]),
    ],
    "data": [
        ("Device", ["name", "serial", "model", "deviceType"], ["serial", "name", "model", "deviceType", "status"]),
        ("Site", ["name", "address", "city", "country"], ["scopeId", "name", "city", "country"]),
        ("ConfigProfile", ["name", "category"], ["id", "name", "category"]),
        ("Script", ["filename", "description"], ["filename", "description"]),
    ],
}

# FTS index → (table, display_fields)
_FTS_INDEX_MAP: dict[str, tuple[str, list[str]]] = {
    "api_fts": ("ApiEndpoint", ["endpoint_id", "method", "path", "summary", "category"]),
    "doc_fts": ("DocSection", ["section_id", "title", "source", "url"]),
    "device_fts": ("Device", ["serial", "name", "model", "deviceType", "status"]),
    "site_fts": ("Site", ["scopeId", "name", "city", "country"]),
    "config_fts": ("ConfigProfile", ["id", "name", "category"]),
    "script_fts": ("Script", ["filename", "description"]),
}

# Scope → FTS indexes to query
_SCOPE_FTS: dict[str, list[str]] = {
    "api": ["api_fts"],
    "docs": ["doc_fts"],
    "data": ["device_fts", "site_fts", "config_fts", "script_fts"],
    "all": ["api_fts", "doc_fts", "device_fts", "site_fts", "config_fts", "script_fts"],
}


def _fts_search(
    gm: GraphManager,
    query: str,
    *,
    scope: str = "all",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Search using FTS/BM25 indexes. Returns ranked results."""
    indexes = _SCOPE_FTS.get(scope, _SCOPE_FTS["all"])
    results: list[dict[str, Any]] = []

    per_index_limit = max(5, limit)
    for idx_name in indexes:
        table, display_fields = _FTS_INDEX_MAP[idx_name]
        field_returns = ", ".join(f"n.{f}" for f in display_fields)
        cypher = (
            f"CALL fts.query_fts('{idx_name}', $q, $k) "
            f"WITH node AS n, score "
            f"RETURN {field_returns}, score "
            f"ORDER BY score DESC LIMIT $k"
        )
        try:
            rows = gm.query(cypher, {"q": query, "k": per_index_limit}, read_only=True)
            for row in rows:
                entry: dict[str, Any] = {"type": table, "score": row.get("score", 0)}
                for f in display_fields:
                    entry[f] = row.get(f"n.{f}", "")
                results.append(entry)
        except Exception as exc:
            logger.debug("fts_search_failed", index=idx_name, error=str(exc))
            continue

    # Sort by score descending and limit
    results.sort(key=lambda r: r.get("score", 0), reverse=True)
    return results[:limit]


def _contains_search(
    gm: GraphManager,
    query: str,
    *,
    scope: str = "all",
    limit: int = 20,
) -> list[dict[str, Any]]:
    """Fallback CONTAINS-based search when FTS is unavailable."""
    if scope == "all":
        tables = _SCOPE_CONFIG["api"] + _SCOPE_CONFIG["docs"] + _SCOPE_CONFIG["data"]
    else:
        tables = _SCOPE_CONFIG.get(scope, [])

    results: list[dict[str, Any]] = []
    remaining = limit

    for table, search_fields, display_fields in tables:
        if remaining <= 0:
            break
        where_clauses = " OR ".join(
            f"lower(n.{f}) CONTAINS lower($q)" for f in search_fields
        )
        field_returns = ", ".join(f"n.{f}" for f in display_fields)
        cypher = (
            f"MATCH (n:{table}) WHERE {where_clauses} "
            f"RETURN {field_returns} LIMIT $lim"
        )
        try:
            rows = gm.query(cypher, {"q": query, "lim": remaining}, read_only=True)
            for row in rows:
                entry: dict[str, Any] = {"type": table}
                for f in display_fields:
                    entry[f] = row.get(f"n.{f}", "")
                results.append(entry)
            remaining -= len(rows)
        except Exception as exc:
            logger.debug("contains_search_failed", table=table, error=str(exc))
            continue

    return results[:limit]


def _unified_search_impl(
    gm: GraphManager,
    query: str,
    *,
    scope: str = "all",
    limit: int = 20,
) -> str:
    """Core implementation of unified search. Returns JSON string."""
    if not query or not query.strip():
        return json.dumps({"error": "Query must not be empty."})

    valid_scopes = {"all", "api", "docs", "data"}
    if scope not in valid_scopes:
        return json.dumps({"error": f"Invalid scope '{scope}'. Must be one of: {', '.join(sorted(valid_scopes))}"})

    limit = max(1, min(limit, 50))

    # Try FTS first, fall back to CONTAINS
    if gm.fts_available:
        results = _fts_search(gm, query, scope=scope, limit=limit)
        search_method = "fts"
        if not results:
            results = _contains_search(gm, query, scope=scope, limit=limit)
            search_method = "contains_fallback"
    else:
        results = _contains_search(gm, query, scope=scope, limit=limit)
        search_method = "contains"

    return json.dumps({
        "query": query,
        "scope": scope,
        "search_method": search_method,
        "total": len(results),
        "results": results,
    }, indent=2)


def _get_data_provenance_impl(
    gm: GraphManager,
    node_label: str,
    identifier: str,
) -> str:
    """Get provenance information for a specific data node."""
    pk_field = _PK_MAP.get(node_label, "")
    if not pk_field:
        return json.dumps({
            "error": f"Unknown node type '{node_label}'. Supported: {', '.join(sorted(_PK_MAP.keys()))}"
        })

    result: dict[str, Any] = {
        "node_label": node_label,
        "identifier": identifier,
        "source_api": None,
        "fetched_at": None,
        "populated_by": [],
    }

    # Instance-level provenance from node properties
    try:
        rows = gm.query(
            f"MATCH (n:{node_label} {{{pk_field}: $pk}}) "
            f"RETURN n.source_api, n.fetched_at",
            {"pk": identifier},
            read_only=True,
        )
        if rows:
            result["source_api"] = rows[0].get("n.source_api")
            result["fetched_at"] = rows[0].get("n.fetched_at")
    except Exception as exc:
        logger.debug("provenance_node_query_failed", node_label=node_label, error=str(exc))

    # POPULATED_BY edges
    try:
        rows = gm.query(
            f"MATCH (n:{node_label} {{{pk_field}: $pk}})-[r:POPULATED_BY]->(api:ApiEndpoint) "
            f"RETURN api.endpoint_id, r.fetched_at, r.seed, r.run_id",
            {"pk": identifier},
            read_only=True,
        )
        for row in rows:
            result["populated_by"].append({
                "endpoint_id": row.get("api.endpoint_id", ""),
                "fetched_at": row.get("r.fetched_at", ""),
                "seed": row.get("r.seed", ""),
                "run_id": row.get("r.run_id", ""),
            })
    except Exception as exc:
        logger.debug("provenance_populated_by_failed", node_label=node_label, error=str(exc))

    return json.dumps(result, indent=2)


def register_search_tools(mcp: FastMCP, settings: Settings, graph_manager: GraphManager):
    """Register unified search tools with the MCP server."""
    global _graph_manager
    _graph_manager = graph_manager

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def unified_search(
        query: str,
        scope: str = "all",
        limit: int = 20,
    ) -> str:
        """Search across APIs, documentation, and data nodes in the knowledge graph.

        Uses BM25 full-text search when available, falls back to substring matching.
        Results are ranked by relevance.

        Args:
            query: Search terms (e.g., "vlan config", "device monitoring", "switch ports").
            scope: What to search — "all" (default), "api" (endpoints only),
                   "docs" (documentation), or "data" (devices, sites, configs, scripts).
            limit: Maximum results (default 20, max 50).

        Returns:
            JSON with ranked search results including type, fields, and relevance score.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Search not available — graph database not initialized."})
        return _unified_search_impl(gm, query, scope=scope, limit=limit)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_data_provenance(
        node_type: str,
        identifier: str,
    ) -> str:
        """Get the data provenance for a specific node in the graph.

        Shows where the data came from: which API endpoint populated it,
        when it was fetched, and which seed script ran.

        Args:
            node_type: Node type (e.g., "Device", "Site", "ConfigProfile").
            identifier: Primary key value (e.g., serial number "SN001", site ID "site-123").

        Returns:
            JSON with source API, fetch timestamp, and POPULATED_BY edges.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Search not available — graph database not initialized."})
        return _get_data_provenance_impl(gm, node_type, identifier)
