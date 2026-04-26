"""API catalog tools — queries ApiEndpoint/ApiCategory nodes in the graph database.

All API endpoint data is populated by the GitHub Actions knowledge DB builder.
At runtime, the MCP server reads from the graph; no scraping occurs.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING

import structlog
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ..config import Settings
from .api_call_policy import (
    register_endpoints as _register_endpoint_templates,
    set_property_summary_fetcher,
)
from .describe import describe_endpoint

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.api_catalog")

_graph_manager: GraphManager | None = None


def register_catalog_tools(mcp: FastMCP, settings: Settings, graph_manager: GraphManager):
    """Register API discovery tools with the MCP server."""
    global _graph_manager
    _graph_manager = graph_manager

    # ── Wire the api_call_policy gate to the catalog graph. ──────────
    # The gate needs two things from the catalog:
    #   1. The full set of endpoint templates so it can resolve a
    #      concrete path (e.g. .../DL0006948/...) back to its catalog
    #      template (e.g. .../{serial-number}/...) before checking
    #      whether the agent inspected it.
    #   2. A way to fetch a single endpoint's skeleton on demand so the
    #      gate can inline it into a block response (one round-trip
    #      instead of two).
    if graph_manager is not None and graph_manager.is_available:
        try:
            rows = graph_manager.query(
                "MATCH (e:ApiEndpoint) RETURN e.method, e.path",
                read_only=True,
            )
            method_to_paths: dict[str, list[str]] = {}
            for r in rows:
                m = r.get("e.method", "")
                p = r.get("e.path", "")
                if m and p:
                    method_to_paths.setdefault(m, []).append(p)
            _register_endpoint_templates(method_to_paths)
        except Exception as exc:  # noqa: BLE001 — degrade gracefully
            logger.warning("endpoint_registry_population_failed", error=str(exc))

    def _fetch_property_summary(method: str, template_path: str) -> str | None:
        """Look up a property summary for the gate to inline on block.

        Delegates to ``describe_endpoint`` so the gate's inlined payload
        matches what the agent would get from
        ``describe_endpoint_for_device``.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return None
        try:
            result = describe_endpoint(gm, method, template_path)
        except Exception:  # noqa: BLE001
            return None
        # Empty property list still gets inlined so the agent sees that the
        # endpoint has no body schema (e.g. parameter-only GETs); the gate
        # auto-records and the next call goes through.
        try:
            return json.dumps(result, indent=2)
        except (TypeError, ValueError):
            return None

    set_property_summary_fetcher(_fetch_property_summary)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def list_api() -> str:
        """Return the full API Endpoint Catalog as a nested path tree.

        **Fallback for agents that cannot see the API Endpoint Catalog
        embedded in the server instructions or via the
        ``api://endpoint-catalog`` / ``docs://endpoint-catalog`` resource.**
        Most clients receive the catalog automatically through one of those
        channels and do not need to call this tool.

        The returned text is grouped by API category with nested path
        indentation. A trailing ``!`` after the method list marks
        deprecated endpoints. When ``READ_ONLY`` is enabled, mutating
        methods (POST/PUT/PATCH/DELETE) are filtered out.

        Returns:
            Plain-text catalog rendered by ``render_path_tree``.
        """
        from ..api_tree import render_path_tree

        gm = _graph_manager
        if gm is None or not gm.is_available:
            return (
                "API endpoint catalog is currently unavailable — "
                "graph database not initialized. Try again shortly."
            )
        try:
            rows = gm.query(
                "MATCH (e:ApiEndpoint) "
                "RETURN e.method AS method, e.path AS path, "
                "e.category AS category, e.deprecated AS deprecated",
                read_only=True,
            )
        except Exception:
            logger.exception("api_endpoint_catalog_query_failed")
            return (
                "API endpoint catalog is currently unavailable. "
                "Try again shortly; see server logs for details."
            )
        return render_path_tree(rows, read_only=settings.read_only)

