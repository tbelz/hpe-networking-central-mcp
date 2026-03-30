"""FTS/BM25 search helpers for the unified_search tool in api_catalog.py.

This module provides the FTS and CONTAINS search implementations used by the
unified_search tool.  It does NOT register any MCP tools itself.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.search")

# Scope → (table, search_fields, display_fields)
_SCOPE_CONFIG: dict[str, list[tuple[str, list[str], list[str]]]] = {
    "api": [
        ("ApiEndpoint", ["path", "summary", "operationId", "description", "category"], ["endpoint_id", "method", "path", "summary", "category"]),
    ],
    "docs": [
        ("DocSection", ["title", "content"], ["section_id", "title", "source", "url"]),
    ],
    "data": [
        ("Device", ["name", "serial", "model", "deviceType"], ["serial", "name", "model", "deviceType", "status"]),
        ("Site", ["name", "address", "city", "country"], ["scopeId", "name", "city", "country"]),
        ("Script", ["filename", "description"], ["filename", "description"]),
    ],
}

# FTS index → (table, display_fields)
_FTS_INDEX_MAP: dict[str, tuple[str, list[str]]] = {
    "api_fts": ("ApiEndpoint", ["endpoint_id", "method", "path", "summary", "category"]),
    "doc_fts": ("DocSection", ["section_id", "title", "source", "url"]),
    "device_fts": ("Device", ["serial", "name", "model", "deviceType", "status"]),
    "site_fts": ("Site", ["scopeId", "name", "city", "country"]),
    "script_fts": ("Script", ["filename", "description"]),
}

# Scope → FTS indexes to query
_SCOPE_FTS: dict[str, list[str]] = {
    "api": ["api_fts"],
    "docs": ["doc_fts"],
    "data": ["device_fts", "site_fts", "script_fts"],
    "all": ["api_fts", "doc_fts", "device_fts", "site_fts", "script_fts"],
}


def fts_search(
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
        field_returns = ", ".join(f"node.{f}" for f in display_fields)
        cypher = (
            f"CALL QUERY_FTS_INDEX('{table}', '{idx_name}', $q, top := $k) "
            f"RETURN {field_returns}, score "
            f"ORDER BY score DESC LIMIT $k"
        )
        try:
            rows = gm.query(cypher, {"q": query, "k": per_index_limit}, read_only=True)
            for row in rows:
                entry: dict[str, Any] = {"type": table, "score": row.get("score", 0)}
                for f in display_fields:
                    entry[f] = row.get(f"node.{f}", "")
                results.append(entry)
        except Exception as exc:
            logger.debug("fts_search_failed", index=idx_name, error=str(exc))
            continue

    # Sort by score descending and limit
    results.sort(key=lambda r: r.get("score", 0), reverse=True)
    return results[:limit]


def contains_search(
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
