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

logger = structlog.get_logger("tools.graph")


def register_graph_tools(mcp, settings: Settings, client: CentralClient, graph: GraphManager):
    """Register graph query and refresh tools with the MCP server."""

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_graph(cypher: str) -> str:
        """Execute a read-only Cypher query against the Central configuration graph.

        The graph models the Aruba Central hierarchy:
        Org → SiteCollection → Site → Device, plus DeviceGroup → Device
        and Org → ConfigProfile for library-level config metadata.

        Read the graph://schema resource first to see all node types,
        relationships, properties, and example queries.

        Write operations (CREATE, DELETE, SET, MERGE, DROP, etc.) are blocked.
        Use call_central_api() for live configuration reads/writes.

        Args:
            cypher: A Cypher query string. Must be read-only.
                    Example: MATCH (s:Site)-[:HAS_DEVICE]->(d:Device) RETURN s.name, d.name

        Returns:
            JSON array of result rows. Each row is a dict of column→value.
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
            hint = (
                "\n\nHint: Read graph://schema for the correct node/relationship names and properties."
                if "does not exist" in msg.lower() or "cannot" in msg.lower()
                else ""
            )
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
