"""MCP Resource — graph schema description for agent context."""

from __future__ import annotations

import structlog

from ..graph.manager import GraphManager

logger = structlog.get_logger("resources.graph")


def register_graph_resources(mcp, graph: GraphManager):
    """Register graph-related resources with the MCP server."""

    @mcp.resource("graph://schema")
    def graph_schema() -> str:
        """Schema of the Central configuration graph — node types, relationships, and example Cypher queries."""
        return graph.get_schema_description()
