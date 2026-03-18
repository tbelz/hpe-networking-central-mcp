"""MCP tools for querying and refreshing the Kùzu graph database."""

from __future__ import annotations

import json
from typing import Any

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..central_client import CentralClient
from ..config import Settings
from ..graph.manager import GraphManager
from ..graph.schema import compact_schema_hint, get_node_properties

logger = structlog.get_logger("tools.graph")


def _build_error_hint(error_msg: str) -> str:
    """Build a context-aware hint from a Cypher error message."""
    msg_lower = error_msg.lower()

    # Property not found → show valid properties for that table
    if "cannot find property" in msg_lower or "property" in msg_lower and "does not exist" in msg_lower:
        for table, props in get_node_properties().items():
            if table.lower() in msg_lower:
                return f"\n\nValid {table} properties: {', '.join(props)}"
        # Couldn't match a specific table — show all
        return f"\n\nAvailable node properties:\n{compact_schema_hint()}"

    # Table not found → show valid table names
    if "does not exist" in msg_lower or "cannot find" in msg_lower:
        return f"\n\nAvailable node properties:\n{compact_schema_hint()}"

    return ""


def register_graph_tools(mcp, settings: Settings, client: CentralClient, graph: GraphManager):
    """Register graph query and refresh tools with the MCP server."""

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_graph(cypher: str) -> str:
        """Execute a read-only Cypher query against the Central configuration graph.

        The graph models the Aruba Central hierarchy:
        Org → SiteCollection → Site → Device, DeviceGroup → Device, Org → ConfigProfile.

        Node tables and their key properties:
        - Org: scopeId, name
        - SiteCollection: scopeId, name, siteCount, deviceCount
        - Site: scopeId, name, address, city, country, deviceCount, collectionName
        - DeviceGroup: scopeId, name, deviceCount
        - Device: serial, name, mac, model, deviceType, status, ipv4, firmware,
          persona, deviceFunction, siteId, siteName, configStatus, deviceGroupId
        - ConfigProfile: id, name, category, scopeId, deviceFunction, objectType

        Relationships: HAS_COLLECTION, HAS_SITE, CONTAINS_SITE, HAS_DEVICE, HAS_MEMBER, HAS_CONFIG

        Read graph://schema for full property lists and example queries.
        Write operations are blocked — use call_central_api() for mutations.

        Args:
            cypher: A read-only Cypher query string.
                    Example: MATCH (s:Site)-[:HAS_DEVICE]->(d:Device) RETURN s.name, d.name

        Returns:
            JSON array of result rows.
        """
        if not cypher or not cypher.strip():
            raise ToolError("Cypher query cannot be empty.")

        try:
            rows = graph.query(cypher, read_only=True)
        except ValueError as exc:
            raise ToolError(str(exc))
        except RuntimeError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            msg = str(exc)
            hint = _build_error_hint(msg)
            raise ToolError(f"Cypher query failed: {msg}{hint}")

        logger.info("query_graph_done", rows=len(rows))
        return json.dumps(rows, indent=2, default=str)

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=True, openWorldHint=True),
    )
    def refresh_graph() -> str:
        """Refresh the Central configuration graph from live APIs.

        Drops all existing graph data and re-populates from the Central APIs.
        Use this after making configuration changes via call_central_api() or
        execute_script() to ensure the graph reflects the current state.

        Returns:
            JSON summary of refreshed entity counts.
        """
        if not settings.has_credentials:
            raise ToolError("Central credentials not configured.")

        try:
            summary = graph.refresh(client)
        except Exception as exc:
            raise ToolError(f"Graph refresh failed: {exc}")

        logger.info("refresh_graph_done", **{k: v for k, v in summary.items() if k != "errors"})
        return json.dumps(summary, indent=2)
