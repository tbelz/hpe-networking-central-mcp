"""MCP tools for querying and writing to the LadybugDB graph database."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..config import Settings
from ..graph.manager import GraphManager
from ..graph.schema import VOLATILE_FIELDS, compact_schema_hint, get_node_properties

logger = structlog.get_logger("tools.graph")

# Column-name shape Kuzu uses for property projections, e.g. "d.status".
_PROJ_RE = re.compile(r"^(?P<alias>[A-Za-z_][\w]*)\.(?P<prop>[A-Za-z_][\w]*)$")


def _default_stale_threshold_seconds() -> int:
    raw = os.environ.get("MCP_GRAPH_STALE_THRESHOLD_SECONDS", "900")
    try:
        return max(0, int(raw))
    except ValueError:
        return 900


def _coerce_datetime(value: object) -> datetime | None:
    """Best-effort conversion of a graph value to a timezone-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _label_from_node(node: object) -> str | None:
    """Pull the node label out of whatever shape Kuzu hands us."""
    if not isinstance(node, dict):
        return None
    for key in ("_label", "label", "_labels", "labels"):
        v = node.get(key)
        if isinstance(v, str):
            return v
        if isinstance(v, list) and v:
            return str(v[0])
    return None


def _scan_freshness(
    rows: list[dict], threshold_seconds: int
) -> list[dict]:
    """Return freshness_warnings for any volatile fields present in rows.

    A warning is emitted when:
      - a projected column matches a known volatile field (e.g. "d.status"
        where the Device node carries lastSyncedAt), OR
      - a row column is a whole node carrying lastSyncedAt + a volatile
        property,
    AND the lastSyncedAt is older than threshold_seconds (or missing).

    Each warning aggregates by (label, age bucket) and reports the maximum
    age observed plus the volatile fields that triggered it.
    """
    if not rows:
        return []

    now = datetime.now(timezone.utc)
    # Map label -> {"volatile_in_result": set[str], "max_age": int|None, "stamped": bool, "count": int}
    findings: dict[str, dict] = {}

    # Pre-compute label hints from projection aliases: row columns like
    # "d.status" tell us *which property* is volatile, but not the label.
    # We infer the label from any companion full-node column or fall back
    # to scanning all VOLATILE_FIELDS labels.
    volatile_props_by_label: dict[str, set[str]] = {
        lbl: set(fields) for lbl, fields in VOLATILE_FIELDS.items()
    }

    # Per-row pass.
    for row in rows:
        if not isinstance(row, dict):
            continue

        # 1. Whole-node columns (dict with lastSyncedAt + some volatile prop).
        node_label_in_row: str | None = None
        for col, value in row.items():
            label = _label_from_node(value)
            if label and label in volatile_props_by_label:
                node_label_in_row = label
                vfields = volatile_props_by_label[label]
                triggered = {p for p in vfields if p in value}
                if not triggered:
                    continue
                stamped = "lastSyncedAt" in value
                ts = _coerce_datetime(value.get("lastSyncedAt"))
                age = int((now - ts).total_seconds()) if ts else None
                if age is None or age >= threshold_seconds:
                    bucket = findings.setdefault(
                        label,
                        {"volatile_in_result": set(), "max_age_seconds": None,
                         "stamped": True, "count": 0},
                    )
                    bucket["volatile_in_result"].update(triggered)
                    if not stamped or age is None:
                        bucket["stamped"] = False
                    if age is not None and (
                        bucket["max_age_seconds"] is None
                        or age > bucket["max_age_seconds"]
                    ):
                        bucket["max_age_seconds"] = age
                    bucket["count"] += 1

        # 2. Projected scalar columns (alias.property).
        for col in row:
            m = _PROJ_RE.match(col)
            if not m:
                continue
            prop = m.group("prop")
            # Find which label this prop is volatile for.
            for label, vfields in volatile_props_by_label.items():
                if prop not in vfields:
                    continue
                # Only warn if we have not already accounted for this label
                # via a whole-node match in the same row, or if the query
                # did not also project lastSyncedAt for the same alias.
                alias = m.group("alias")
                lsa_col = f"{alias}.lastSyncedAt"
                ts = _coerce_datetime(row.get(lsa_col))
                age = int((now - ts).total_seconds()) if ts else None
                if age is None or age >= threshold_seconds:
                    bucket = findings.setdefault(
                        label,
                        {"volatile_in_result": set(), "max_age_seconds": None,
                         "stamped": ts is not None, "count": 0},
                    )
                    bucket["volatile_in_result"].add(prop)
                    if ts is None:
                        bucket["stamped"] = False
                    elif age is not None and (
                        bucket["max_age_seconds"] is None
                        or age > bucket["max_age_seconds"]
                    ):
                        bucket["max_age_seconds"] = age
                    bucket["count"] += 1
                # Stop after first label match to avoid duplicate warnings;
                # the same property name is unlikely across multiple labels.
                break

    if not findings:
        return []

    warnings: list[dict] = []
    for label, data in findings.items():
        warnings.append({
            "node_label": label,
            "volatile_fields_in_result": sorted(data["volatile_in_result"]),
            "max_age_seconds": data["max_age_seconds"],
            "lastSyncedAt_present": data["stamped"],
            "rows_affected": data["count"],
            "threshold_seconds": threshold_seconds,
            "recommendation": (
                f"Values for {label} fields {sorted(data['volatile_in_result'])} "
                "may be stale. For live state call the Central API directly "
                "(e.g. call_central_api on the matching monitoring endpoint); "
                f"to refresh the graph run execute_script('populate_base_graph.py'"
                + (f", parameters={{'site-id': '<scopeId>'}}" if label in ('Device', 'Site') else "")
                + ")."
            ),
        })
    return warnings


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
        SchemaComponent, plus REFERENCES edges). This is the
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
        threshold = _default_stale_threshold_seconds()
        freshness_warnings = (
            _scan_freshness(rows, threshold) if threshold > 0 else []
        )

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
            if freshness_warnings:
                envelope["freshness_warnings"] = freshness_warnings
            return json.dumps(envelope, indent=2, default=str)
        if freshness_warnings:
            return json.dumps(
                {"rows": rows, "freshness_warnings": freshness_warnings},
                indent=2,
                default=str,
            )
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
