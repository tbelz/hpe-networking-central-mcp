"""MCP tools for querying and refreshing the LadybugDB graph database."""

from __future__ import annotations

import json

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..config import Settings
from ..graph.manager import GraphManager
from ..graph.schema import compact_schema_hint, get_node_properties
from .execution import _run_script

logger = structlog.get_logger("tools.graph")


def _build_error_hint(error_msg: str) -> str:
    """Build a context-aware hint from a Cypher error message."""
    msg_lower = error_msg.lower()

    if "cannot find property" in msg_lower or "property" in msg_lower and "does not exist" in msg_lower:
        for table, props in get_node_properties().items():
            if table.lower() in msg_lower:
                return f"\n\nValid {table} properties: {', '.join(props)}"
        return f"\n\nAvailable node properties:\n{compact_schema_hint()}"

    if "does not exist" in msg_lower or "cannot find" in msg_lower:
        return f"\n\nAvailable node properties:\n{compact_schema_hint()}"

    return ""


def _get_auto_run_seeds(settings: Settings) -> list[str]:
    """Return seed script filenames that have auto_run: true in their metadata."""
    import json as _json
    seeds: list[str] = []
    lib = settings.script_library_path
    for meta_file in sorted(lib.glob("*.meta.json")):
        try:
            meta = _json.loads(meta_file.read_text(encoding="utf-8"))
            if meta.get("auto_run"):
                script_name = meta_file.name.replace(".meta.json", ".py")
                if (lib / script_name).exists():
                    seeds.append(script_name)
        except Exception:
            continue
    return seeds


def register_graph_tools(mcp, settings: Settings, graph: GraphManager):
    """Register graph query and refresh tools with the MCP server."""

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_graph(cypher: str) -> str:
        """Execute a read-only Cypher query against the Central configuration & topology graph.

        The graph models the Aruba Central hierarchy and physical L2 topology:
        Org → SiteCollection → Site → Device, DeviceGroup → Device, Org → ConfigProfile.
        Device -CONNECTED_TO→ Device, Device -LINKED_TO→ UnmanagedDevice (L2 links from LLDP).

        Node tables and their key properties:
        - Org: scopeId, name
        - SiteCollection: scopeId, name, siteCount, deviceCount
        - Site: scopeId, name, address, city, country, deviceCount, collectionName
        - DeviceGroup: scopeId, name, deviceCount
        - Device: serial, name, mac, model, deviceType, status, ipv4, firmware,
          persona, deviceFunction, siteId, siteName, configStatus, deviceGroupId
        - ConfigProfile: id, name, category, scopeId, deviceFunction, objectType
        - UnmanagedDevice: mac, name, model, deviceType, health, status, ipv4, siteId

        Relationships: HAS_COLLECTION, HAS_SITE, CONTAINS_SITE, HAS_DEVICE, HAS_MEMBER,
        HAS_CONFIG, HAS_UNMANAGED, CONNECTED_TO (Device→Device), LINKED_TO (Device→UnmanagedDevice)

        Topology edges have: fromPorts, toPorts, speed, edgeType, health, lag, stpState, isSibling.

        Read graph://schema for full property lists, dynamic row counts, and enrichment status.
        For write operations (CREATE, MERGE, SET, DELETE), use write_graph instead.

        Args:
            cypher: A read-only Cypher query string.
                    Example: MATCH (d:Device)-[c:CONNECTED_TO]->(d2:Device) RETURN d.name, d2.name, c.speed

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
            raise ToolError("Cypher statement cannot be empty.")

        # Block schema-altering DDL
        _DDL_PATTERN = re.compile(
            r"\b(DROP|ALTER|CREATE\s+(NODE|REL)\s+TABLE)\b", re.IGNORECASE
        )
        if _DDL_PATTERN.search(cypher):
            raise ToolError(
                "Schema-altering statements (DROP, ALTER, CREATE NODE/REL TABLE) are not allowed. "
                "Only data manipulation (CREATE, MERGE, SET, DELETE, REMOVE) is permitted."
            )

        try:
            params = json.loads(parameters)
        except json.JSONDecodeError as exc:
            raise ToolError(f"Invalid JSON in parameters: {exc}")

        try:
            rows = graph.execute(cypher, params=params)
        except Exception as exc:
            msg = str(exc)
            hint = _build_error_hint(msg)
            raise ToolError(f"Cypher write failed: {msg}{hint}")

        logger.info("write_graph_done", rows=len(rows))
        return json.dumps({"status": "ok", "rows_affected": len(rows)}, indent=2)

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=True, openWorldHint=True),
    )
    def refresh_graph() -> str:
        """Reset and re-populate the configuration & topology graph from live APIs.

        Deletes all graph data, re-creates the schema, then runs all auto-run
        seed scripts (populate_base_graph, enrich_topology, etc.) to rebuild
        the graph from scratch.

        Use this after making configuration changes to ensure the graph
        reflects the current state of Central.

        Returns:
            JSON summary of the refresh operation with per-script results.
        """
        if not settings.has_credentials:
            raise ToolError("Central credentials not configured.")

        try:
            graph.reset()
        except Exception as exc:
            raise ToolError(f"Graph reset failed: {exc}")

        results = {}
        for script_name in _get_auto_run_seeds(settings):
            logger.info("refresh_seed_start", filename=script_name)
            try:
                result_json = _run_script(settings, script_name)
                result = json.loads(result_json)
                results[script_name] = {
                    "exit_code": result.get("exit_code", -1),
                    "stdout": result.get("stdout", "")[:2000],
                }
            except Exception as e:
                results[script_name] = {"error": str(e)}

        logger.info("refresh_graph_done", scripts=len(results))
        return json.dumps({"status": "refreshed", "scripts": results}, indent=2)
