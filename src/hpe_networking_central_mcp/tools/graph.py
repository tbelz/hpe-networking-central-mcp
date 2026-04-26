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
    def query_graph(cypher: str, parameters: str = "{}") -> str:
        """Execute a read-only Cypher query against the Central graph (incl. the API schema subgraph).

        The graph models both the Aruba Central hierarchy / physical L2 topology
        AND the OpenAPI surface (ApiEndpoint, Parameter, RequestBody, Response,
        SchemaComponent, plus REFERENCES/HAS_SKELETON edges). This is the
        primary tool for API discovery and structural exploration. Read
        ``graph://schema`` for the full schema, row counts, and canned API
        discovery patterns.

        For write operations (CREATE, MERGE, SET, DELETE), use ``write_graph``.

        Row caps (to keep responses agent-friendly):
        - Soft cap: 200 rows. Larger result sets come back as
          ``{"truncated": true, "cap": 200, "rows": [...], "warning": "..."}``.
        - Hard cap: 2000 rows. Queries returning more than that are rejected;
          add ``LIMIT``/``WHERE`` filters or aggregate.

        Args:
            cypher: A read-only Cypher query string.
                    Example: MATCH (d:Device)-[c:CONNECTED_TO]->(d2:Device)
                             RETURN d.name, d2.name, c.speed
            parameters: JSON-encoded parameter dict for parameterised queries
                    (default: ``"{}"``). Example: ``{"site": "hq-1"}``.

        Returns:
            JSON array of result rows under the soft cap, otherwise a JSON
            object with ``truncated``/``rows``/``cap``/``warning`` keys.
        """
        if not cypher or not cypher.strip():
            raise ToolError("Cypher query cannot be empty. Read graph://schema for the schema.")

        try:
            params = json.loads(parameters) if parameters else {}
        except json.JSONDecodeError as exc:
            raise ToolError(
                f"Invalid JSON in parameters: {exc}. "
                "Example: {\"serial\": \"SN001\"}"
            )
        if not isinstance(params, dict):
            raise ToolError("parameters must decode to a JSON object (dict).")

        try:
            rows = graph.query(cypher, params=params, read_only=True)
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            msg = str(exc)
            hint = _build_error_hint(msg)
            raise ToolError(f"Cypher query failed: {msg}{hint}")

        soft_cap = 200
        hard_cap = 2000
        n = len(rows)
        if n > hard_cap:
            raise ToolError(
                f"Query returned {n} rows which exceeds the hard cap of {hard_cap}. "
                "Add LIMIT or WHERE filters, or aggregate with COUNT/COLLECT."
            )

        logger.info("query_graph_done", rows=n)
        if n > soft_cap:
            envelope = {
                "truncated": True,
                "cap": soft_cap,
                "total_returned": n,
                "warning": (
                    f"Result truncated: query returned {n} rows; only the first "
                    f"{soft_cap} are shown. Add LIMIT/WHERE or aggregate to see "
                    "the rest."
                ),
                "rows": rows[:soft_cap],
            }
            return json.dumps(envelope, indent=2, default=str)
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
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            msg = str(exc)
            hint = _build_error_hint(msg)
            raise ToolError(f"Cypher write failed: {msg}{hint}")

        logger.info("write_graph_done", rows_returned=len(rows))
        result: dict = {"status": "ok"}
        if rows:
            result["rows"] = rows
        return json.dumps(result, indent=2)
