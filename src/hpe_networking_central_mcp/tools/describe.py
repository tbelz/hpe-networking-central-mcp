"""Builder helper: describe an endpoint's request body field-by-field.

Returns one structured row per leaf property (already flattened across
``allOf`` branches in Phase 2C). Lets the agent assemble a request body
without ever reading the full skeleton blob.

When ``deviceType`` is supplied, only properties whose
``supportedDeviceTypes`` list contains that value (or is empty —
meaning "applies everywhere") are returned.
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ..config import Settings
from .api_call_policy import get_tracker

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.describe")


# Cypher: walk endpoint → request body → root component → property,
# include all the typed columns plus extensionsJson for the agent.
_REQUEST_BODY_QUERY = (
    "MATCH (e:ApiEndpoint {endpoint_id: $eid})"
    "-[:HAS_REQUEST_BODY]->(:RequestBody)-[:BODY_REFERENCES]->(c:SchemaComponent)"
    "-[:HAS_PROPERTY]->(p:Property) "
    "RETURN c.name AS component, p.name AS name, p.type AS type, "
    "p.format AS format, p.required AS required, p.readOnly AS readOnly, "
    "p.enumValues AS enumValues, p.description AS description, "
    "p.supportedDeviceTypes AS supportedDeviceTypes, "
    "p.yangPath AS yangPath, p.inheritedFrom AS inheritedFrom, "
    "p.extensionsJson AS extensionsJson "
    "ORDER BY p.name"
)

_RESPONSE_BODY_QUERY = (
    "MATCH (e:ApiEndpoint {endpoint_id: $eid})"
    "-[:HAS_RESPONSE]->(r:Response)-[:RESPONSE_REFERENCES]->(c:SchemaComponent)"
    "-[:HAS_PROPERTY]->(p:Property) "
    "WHERE r.status = '200' "
    "RETURN c.name AS component, p.name AS name, p.type AS type, "
    "p.format AS format, p.required AS required, p.readOnly AS readOnly, "
    "p.enumValues AS enumValues, p.description AS description, "
    "p.supportedDeviceTypes AS supportedDeviceTypes, "
    "p.yangPath AS yangPath, p.inheritedFrom AS inheritedFrom, "
    "p.extensionsJson AS extensionsJson "
    "ORDER BY p.name"
)


def _normalise_path(p: str) -> str:
    p = p.strip()
    return p if p.startswith("/") else f"/{p}"


def _row_to_property(row: dict) -> dict:
    """Shape one Cypher row into the public property record."""
    ext_raw = row.get("extensionsJson") or ""
    extensions: dict[str, Any] = {}
    if ext_raw:
        try:
            extensions = json.loads(ext_raw)
        except (ValueError, TypeError):
            extensions = {}
    return {
        "component": row.get("component", ""),
        "name": row.get("name", ""),
        "type": row.get("type", ""),
        "format": row.get("format", ""),
        "required": bool(row.get("required") or False),
        "readOnly": bool(row.get("readOnly") or False),
        "enumValues": list(row.get("enumValues") or []),
        "description": row.get("description", ""),
        "supportedDeviceTypes": list(row.get("supportedDeviceTypes") or []),
        "yangPath": row.get("yangPath", ""),
        "inheritedFrom": row.get("inheritedFrom", ""),
        "extensions": extensions,
    }


def _device_type_matches(prop: dict, device_type: str | None) -> bool:
    if not device_type:
        return True
    sdt = prop.get("supportedDeviceTypes") or []
    # Empty list = applies everywhere (no x-supportedDeviceType set).
    if not sdt:
        return True
    return device_type in sdt


def describe_endpoint(
    graph_manager: "GraphManager",
    method: str,
    path: str,
    device_type: str | None = None,
) -> dict:
    """Programmatic entrypoint (also used by tests).

    Returns a dict with keys ``method``, ``path``, ``source``
    ("requestBody" or "response:200"), ``properties`` (list), and
    ``deviceType`` (echoed). When the endpoint has neither a request
    body nor a 200 response with properties, returns an empty
    ``properties`` list.
    """
    eid = f"{method.upper()}:{_normalise_path(path)}"
    rows = graph_manager.query(
        _REQUEST_BODY_QUERY, {"eid": eid}, read_only=True
    )
    source = "requestBody"
    if not rows:
        rows = graph_manager.query(
            _RESPONSE_BODY_QUERY, {"eid": eid}, read_only=True
        )
        source = "response:200"
    if not rows:
        return {
            "method": method.upper(),
            "path": _normalise_path(path),
            "deviceType": device_type or "",
            "source": "",
            "properties": [],
        }
    props = [_row_to_property(r) for r in rows]
    if device_type:
        props = [p for p in props if _device_type_matches(p, device_type)]
    return {
        "method": method.upper(),
        "path": _normalise_path(path),
        "deviceType": device_type or "",
        "source": source,
        "properties": props,
    }


def register_describe_tools(
    mcp: FastMCP,
    settings: Settings,
    graph_manager: "GraphManager",
) -> None:
    """Register the ``describe_endpoint_for_device`` tool."""

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def describe_endpoint_for_device(
        method: str,
        path: str,
        deviceType: str | None = None,
    ) -> str:
        """Field-by-field guide for assembling an endpoint's body.

        Returns one record per leaf property of the endpoint's request
        body (or 200 response if the endpoint has no request body),
        already flattened across ``allOf`` branches. Use this instead of
        reading ``get_api_endpoint_detail`` when you intend to construct
        a call body.

        Each record contains:

          - ``component`` — the owning SchemaComponent name
          - ``name`` — the field name as it appears in JSON
          - ``type`` / ``format`` — JSON Schema type info
          - ``required`` — whether the field is required on its parent
          - ``readOnly`` — true for response-only fields; **omit these
            from POST/PUT/PATCH bodies**
          - ``enumValues`` — allowed string values when constrained
          - ``description`` — human-readable description if present
          - ``supportedDeviceTypes`` — the value of
            ``x-supportedDeviceType`` for that field (empty list means
            "applies to all device types")
          - ``yangPath`` — the value of ``x-path`` (YANG mapping) when
            present, otherwise ``""``
          - ``inheritedFrom`` — name of the ``allOf`` branch that
            contributed the property, or ``""`` for direct properties
          - ``extensions`` — the full ``x-*`` vendor-extension dict
            (includes the keys promoted to typed columns above)

        When ``deviceType`` is supplied, only fields whose
        ``supportedDeviceTypes`` list contains it (or is empty) are
        returned. Pair with ``query_graph`` for ad-hoc filtering needs.

        Args:
            method: HTTP method, e.g. ``"POST"``.
            path: Endpoint path template, e.g.
                ``"/network-config/v1alpha1/ntp/{name}"``.
            deviceType: Optional device-type filter
                (``"Switch CX"``, ``"Gateway"``, …).

        Returns:
            JSON string with ``method``, ``path``, ``deviceType``,
            ``source`` (``requestBody`` / ``response:200``), and
            ``properties`` (list of records as above).
        """
        gm = graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({
                "error": "Graph database not available.",
            })
        try:
            result = describe_endpoint(gm, method, path, deviceType)
        except Exception as exc:  # noqa: BLE001
            logger.exception("describe_endpoint_failed")
            return json.dumps({"error": str(exc)})
        # Record inspection so the policy gate allows the follow-up
        # call_central_api / call_greenlake_api in the same session.
        get_tracker().record(method, _normalise_path(path))
        return json.dumps(result, indent=2)
