"""MCP tools for querying and writing to the LadybugDB graph database."""

from __future__ import annotations

import json

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..config import Settings
from ..graph.manager import GraphManager
from ..graph.schema import compact_schema_hint, get_node_properties

logger = structlog.get_logger("tools.graph")


def _build_error_hint(error_msg: str) -> str:
    """Build a context-aware hint from a Cypher error message."""
    msg_lower = error_msg.lower()

    if "cannot find property" in msg_lower or "property" in msg_lower and "does not exist" in msg_lower:
        for table, props in get_node_properties().items():
            if table.lower() in msg_lower:
                return f"\n\nValid {table} properties: {', '.join(props)}\nRead graph://schema for the full schema."
        return f"\n\nRead graph://schema for the full schema with node types, properties, and relationships.\n\n{compact_schema_hint()}"

    if "does not exist" in msg_lower or "cannot find" in msg_lower:
        return f"\n\nRead graph://schema for the full schema with node types, properties, and relationships.\n\n{compact_schema_hint()}"

    return ""


def register_graph_tools(mcp, settings: Settings, graph: GraphManager):
    """Register graph query and write tools with the MCP server."""

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_graph(cypher: str) -> str:
        """Execute a read-only Cypher query against the Central configuration & topology graph.

        The graph models the Aruba Central hierarchy and physical L2 topology.
        Read graph://schema for the full schema with node types, properties,
        relationships, row counts, and example queries.

        For write operations (CREATE, MERGE, SET, DELETE), use write_graph instead.

        Args:
            cypher: A read-only Cypher query string.
                    Example: MATCH (d:Device)-[c:CONNECTED_TO]->(d2:Device) RETURN d.name, d2.name, c.speed

        Returns:
            JSON array of result rows.
        """
        if not cypher or not cypher.strip():
            raise ToolError("Cypher query cannot be empty. Read graph://schema for the schema.")

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
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=False, openWorldHint=False),
    )
    def write_graph(cypher: str, parameters: str = "{}") -> str:
        """Execute a write Cypher statement to enrich the graph with new nodes, relationships, or properties.

        Use this to add discovered information back to the graph — for example,
        creating relationships between devices and sites after investigating
        API responses, or annotating nodes with computed properties.

        Allowed operations: CREATE, MERGE, SET, DELETE, REMOVE.
        Schema-altering statements (DROP, ALTER, CREATE NODE TABLE) are blocked.

        Args:
            cypher: A write Cypher statement.
                    Example: MATCH (d:Device {serial: 'SN001'}) SET d.customLabel = 'core-switch'
            parameters: JSON-encoded parameter dict for parameterized queries (default: "{}").
                    Example: {"serial": "SN001", "label": "core-switch"}

        Returns:
            JSON object with status and the number of rows affected.
        """
        import re

        if not cypher or not cypher.strip():
            raise ToolError("Cypher statement cannot be empty. Read graph://schema for the schema.")

        # Block schema-altering DDL
        _DDL_PATTERN = re.compile(
            r"\b(DROP|ALTER|CREATE\s+(NODE|REL)\s+TABLE|CREATE\s+INDEX|CREATE\s+CONSTRAINT)\b",
            re.IGNORECASE,
        )
        if _DDL_PATTERN.search(cypher):
            raise ToolError(
                "Schema-altering statements (DROP, ALTER, CREATE NODE/REL TABLE) are not allowed. "
                "Only data manipulation (CREATE, MERGE, SET, DELETE, REMOVE) is permitted."
            )

        # Block other side-effecting or privileged operations
        _DENY_PATTERN = re.compile(
            r"\b(LOAD|COPY|INSTALL|CALL|CREATE\s+DATABASE|DROP\s+DATABASE)\b",
            re.IGNORECASE,
        )
        if _DENY_PATTERN.search(cypher):
            raise ToolError(
                "Only data manipulation statements using CREATE, MERGE, SET, DELETE, or REMOVE are "
                "allowed. Statements using LOAD, COPY, INSTALL, CALL, or database-level DDL are "
                "not permitted in this tool."
            )

        # Require at least one allowed write keyword
        cypher_lower = cypher.lower()
        _ALLOWED_WRITE_KEYWORDS = ("create", "merge", "set", "delete", "remove")
        if not any(re.search(rf"\b{kw}\b", cypher_lower) for kw in _ALLOWED_WRITE_KEYWORDS):
            raise ToolError(
                "write_graph only permits data-manipulation statements that use CREATE, MERGE, SET, "
                "DELETE, or REMOVE (optionally after MATCH/WITH/WHERE). Other operations are not "
                "allowed."
            )

        try:
            params = json.loads(parameters)
        except json.JSONDecodeError as exc:
            raise ToolError(f"Invalid JSON in parameters: {exc}. Example: {{\"serial\": \"SN001\", \"label\": \"core-switch\"}}")

        try:
            rows = graph.execute(cypher, params=params)
        except Exception as exc:
            msg = str(exc)
            hint = _build_error_hint(msg)
            raise ToolError(f"Cypher write failed: {msg}{hint}")

        logger.info("write_graph_done", rows_returned=len(rows))
        result: dict = {"status": "ok"}
        if rows:
            result["rows"] = rows
        return json.dumps(result, indent=2)
