"""Materialize compiler semantic graphs into the legacy L3 knowledge shape."""

from __future__ import annotations

import hashlib
import json
from pathlib import Path
from typing import Any

import pyarrow as pa
import real_ladybug as lb

from hpe_networking_central_mcp.graph.schema import (
    KNOWLEDGE_NODE_TABLES,
    KNOWLEDGE_REL_TABLES,
)

from .ast_builder import AstGraph
from .semantic_builder import SemanticEdge, SemanticGraph, SemanticNode

_ENDPOINT_SCHEMA = pa.schema([
    ("endpoint_id", pa.string()),
    ("method", pa.string()),
    ("path", pa.string()),
    ("summary", pa.string()),
    ("description", pa.string()),
    ("operationId", pa.string()),
    ("category", pa.string()),
    ("deprecated", pa.bool_()),
    ("tags", pa.list_(pa.string())),
    ("parameters", pa.string()),
    ("requestBody", pa.string()),
    ("responses", pa.string()),
])

_PARAM_SCHEMA = pa.schema([
    ("parameter_id", pa.string()),
    ("endpoint_id", pa.string()),
    ("name", pa.string()),
    ("location", pa.string()),
    ("required", pa.bool_()),
    ("type", pa.string()),
    ("format", pa.string()),
    ("enumValues", pa.list_(pa.string())),
    ("pattern", pa.string()),
    ("inferredHint", pa.string()),
    ("description", pa.string()),
])

_REQUEST_BODY_SCHEMA = pa.schema([
    ("request_body_id", pa.string()),
    ("endpoint_id", pa.string()),
    ("content_type", pa.string()),
    ("required", pa.bool_()),
    ("root_component_ref", pa.string()),
])

_RESPONSE_SCHEMA = pa.schema([
    ("response_id", pa.string()),
    ("endpoint_id", pa.string()),
    ("status", pa.string()),
    ("content_type", pa.string()),
    ("root_component_ref", pa.string()),
])

_COMPONENT_SCHEMA = pa.schema([
    ("component_id", pa.string()),
    ("spec_source", pa.string()),
    ("section", pa.string()),
    ("name", pa.string()),
    ("type", pa.string()),
    ("kind", pa.string()),
    ("bodyShape", pa.string()),
    ("required", pa.list_(pa.string())),
    ("enumValues", pa.list_(pa.string())),
    ("supportedDeviceTypes", pa.list_(pa.string())),
    ("bodyJson", pa.string()),
])

_PROPERTY_SCHEMA = pa.schema([
    ("property_id", pa.string()),
    ("parent_component_id", pa.string()),
    ("name", pa.string()),
    ("type", pa.string()),
    ("format", pa.string()),
    ("required", pa.bool_()),
    ("enumValues", pa.list_(pa.string())),
    ("description", pa.string()),
    ("supportedDeviceTypes", pa.list_(pa.string())),
    ("yangPath", pa.string()),
    ("extensionsJson", pa.string()),
    ("readOnly", pa.bool_()),
])

_YANG_PATH_SCHEMA = pa.schema([
    ("yangPath", pa.string()),
    ("module", pa.string()),
])

_YANG_MODULE_SCHEMA = pa.schema([
    ("module", pa.string()),
])

_CLI_COMMAND_SCHEMA = pa.schema([
    ("command_id", pa.string()),
    ("commandName", pa.string()),
    ("commandUse", pa.string()),
    ("parentCommand", pa.string()),
    ("pathToPrint", pa.string()),
    ("paramKeys", pa.list_(pa.string())),
])

_REL_AB_SCHEMA = pa.schema([
    ("a", pa.string()),
    ("b", pa.string()),
])

_REL_COMPOSED_OF_SCHEMA = pa.schema([
    ("a", pa.string()),
    ("b", pa.string()),
    ("kind", pa.string()),
])

_REL_REFERENCES_SCHEMA = pa.schema([
    ("a", pa.string()),
    ("b", pa.string()),
    ("via", pa.string()),
])


def build_compiler_projection_database(
    db_path: Path,
    ast_graphs: list[AstGraph],
    semantic_graphs: list[SemanticGraph],
    *,
    buffer_pool_size: int | None = None,
) -> dict[str, Any]:
    """Create a typed L3 compatibility DB from compiler graph outputs."""
    if len(ast_graphs) != len(semantic_graphs):
        raise ValueError("ast_graphs and semantic_graphs must have the same length")
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db_kwargs = {}
    if buffer_pool_size is not None:
        db_kwargs["buffer_pool_size"] = buffer_pool_size
    db = lb.Database(str(db_path), **db_kwargs)
    try:
        conn = lb.Connection(db)
        _apply_projection_schema(conn)
        stats = write_compiler_projection(conn, ast_graphs, semantic_graphs)
    finally:
        db.close()
    stats["db_path"] = db_path.name
    return stats


def write_compiler_projection(
    conn,
    ast_graphs: list[AstGraph],
    semantic_graphs: list[SemanticGraph],
) -> dict[str, Any]:
    """Write compiler-produced L3 rows into an open LadybugDB connection."""
    rows: dict[str, dict[str, dict[str, Any]]] = {
        "ApiEndpoint": {},
        "Parameter": {},
        "RequestBody": {},
        "Response": {},
        "SchemaComponent": {},
        "Property": {},
        "YangPath": {},
        "YangModule": {},
        "CliCommand": {},
    }
    rels: dict[str, dict[tuple[Any, ...], dict[str, Any]]] = {
        "HAS_PARAMETER": {},
        "HAS_REQUEST_BODY": {},
        "HAS_RESPONSE": {},
        "BODY_REFERENCES": {},
        "RESPONSE_REFERENCES": {},
        "REFERENCES": {},
        "HAS_PROPERTY": {},
        "PROPERTY_OF_TYPE": {},
        "COMPOSED_OF": {},
        "HAS_VALUE_SCHEMA": {},
        "PROPERTY_AT_YANG": {},
        "CONFIGURES_YANG": {},
        "HAS_CLI_COMMAND": {},
        "IN_MODULE": {},
    }
    ast_by_spec = {graph.spec_id: graph for graph in ast_graphs}
    for semantic in semantic_graphs:
        ast = ast_by_spec.get(semantic.spec_id)
        if ast is None:
            continue
        _collect_graph_rows(ast, semantic, rows, rels)

    _copy(conn, "ApiEndpoint", list(rows["ApiEndpoint"].values()), _ENDPOINT_SCHEMA)
    _copy(conn, "Parameter", list(rows["Parameter"].values()), _PARAM_SCHEMA)
    _copy(conn, "RequestBody", list(rows["RequestBody"].values()), _REQUEST_BODY_SCHEMA)
    _copy(conn, "Response", list(rows["Response"].values()), _RESPONSE_SCHEMA)
    _copy(conn, "SchemaComponent", list(rows["SchemaComponent"].values()), _COMPONENT_SCHEMA)
    _copy(conn, "Property", list(rows["Property"].values()), _PROPERTY_SCHEMA)
    _copy(conn, "YangPath", list(rows["YangPath"].values()), _YANG_PATH_SCHEMA)
    _copy(conn, "YangModule", list(rows["YangModule"].values()), _YANG_MODULE_SCHEMA)
    _copy(conn, "CliCommand", list(rows["CliCommand"].values()), _CLI_COMMAND_SCHEMA)

    _copy(conn, "HAS_PARAMETER", list(rels["HAS_PARAMETER"].values()), _REL_AB_SCHEMA)
    _copy(conn, "HAS_REQUEST_BODY", list(rels["HAS_REQUEST_BODY"].values()), _REL_AB_SCHEMA)
    _copy(conn, "HAS_RESPONSE", list(rels["HAS_RESPONSE"].values()), _REL_AB_SCHEMA)
    _copy(conn, "BODY_REFERENCES", list(rels["BODY_REFERENCES"].values()), _REL_AB_SCHEMA)
    _copy(conn, "RESPONSE_REFERENCES", list(rels["RESPONSE_REFERENCES"].values()), _REL_AB_SCHEMA)
    _copy(conn, "REFERENCES", list(rels["REFERENCES"].values()), _REL_REFERENCES_SCHEMA)
    _copy(conn, "HAS_PROPERTY", list(rels["HAS_PROPERTY"].values()), _REL_AB_SCHEMA)
    _copy(conn, "PROPERTY_OF_TYPE", list(rels["PROPERTY_OF_TYPE"].values()), _REL_AB_SCHEMA)
    _copy(conn, "COMPOSED_OF", list(rels["COMPOSED_OF"].values()), _REL_COMPOSED_OF_SCHEMA)
    _copy(conn, "HAS_VALUE_SCHEMA", list(rels["HAS_VALUE_SCHEMA"].values()), _REL_AB_SCHEMA)
    _copy(conn, "PROPERTY_AT_YANG", list(rels["PROPERTY_AT_YANG"].values()), _REL_AB_SCHEMA)
    _copy(conn, "CONFIGURES_YANG", list(rels["CONFIGURES_YANG"].values()), _REL_AB_SCHEMA)
    _copy(conn, "HAS_CLI_COMMAND", list(rels["HAS_CLI_COMMAND"].values()), _REL_AB_SCHEMA)
    _copy(conn, "IN_MODULE", list(rels["IN_MODULE"].values()), _REL_AB_SCHEMA)

    return {
        "enabled": True,
        "node_count": sum(len(v) for v in rows.values()),
        "edge_count": sum(len(v) for v in rels.values()),
        "node_kind_counts": {
            table: len(table_rows)
            for table, table_rows in sorted(rows.items())
            if table_rows
        },
        "edge_kind_counts": {
            table: len(table_rows)
            for table, table_rows in sorted(rels.items())
            if table_rows
        },
    }


def _collect_graph_rows(
    ast: AstGraph,
    semantic: SemanticGraph,
    rows: dict[str, dict[str, dict[str, Any]]],
    rels: dict[str, dict[tuple[Any, ...], dict[str, Any]]],
) -> None:
    nodes = {node.semantic_id: node for node in semantic.nodes}
    summaries = {node.semantic_id: _load_summary(node) for node in semantic.nodes}
    raw_by_pointer = {node.json_pointer: node.raw_json for node in ast.nodes}
    typed_ids: dict[str, str] = {}
    endpoint_parent: dict[str, str] = {}
    schema_parent: dict[str, str] = {}
    schema_refs: dict[str, str] = {}

    for edge in semantic.edges:
        if edge.kind in {"HAS_PARAMETER", "HAS_REQUEST_BODY", "HAS_RESPONSE", "HAS_CLI_COMMAND"}:
            endpoint_parent[edge.target_id] = edge.source_id
        elif edge.kind == "HAS_PROPERTY":
            schema_parent[edge.target_id] = edge.source_id
        elif edge.kind in {"BODY_REFERENCES", "RESPONSE_REFERENCES", "PROPERTY_OF_TYPE"}:
            schema_refs[edge.source_id] = edge.target_id

    for node in semantic.nodes:
        typed_id = _typed_node_id(ast, node, summaries[node.semantic_id], endpoint_parent, schema_parent, typed_ids, nodes, summaries)
        if typed_id:
            typed_ids[node.semantic_id] = typed_id

    for node in semantic.nodes:
        summary = summaries[node.semantic_id]
        typed_id = typed_ids.get(node.semantic_id)
        if not typed_id:
            continue
        if node.kind == "ApiEndpoint":
            rows["ApiEndpoint"][typed_id] = {
                "endpoint_id": typed_id,
                "method": _str(summary.get("method")),
                "path": _str(summary.get("path")),
                "summary": _str(summary.get("summary")),
                "description": _str(summary.get("description")),
                "operationId": _str(summary.get("operationId")),
                "category": "",
                "deprecated": False,
                "tags": _string_list(summary.get("tags")),
                "parameters": "",
                "requestBody": "",
                "responses": "",
            }
        elif node.kind == "Parameter":
            endpoint_id = typed_ids.get(endpoint_parent.get(node.semantic_id, ""))
            if endpoint_id:
                rows["Parameter"][typed_id] = {
                    "parameter_id": typed_id,
                    "endpoint_id": endpoint_id,
                    "name": node.name,
                    "location": _str(summary.get("in")),
                    "required": bool(summary.get("required")),
                    "type": _str(summary.get("type")),
                    "format": _str(summary.get("format")),
                    "enumValues": _string_list(summary.get("enumValues")),
                    "pattern": _str(summary.get("pattern")),
                    "inferredHint": _str(summary.get("inferredHint")),
                    "description": _str(summary.get("description")),
                }
        elif node.kind == "RequestBody":
            endpoint_id = typed_ids.get(endpoint_parent.get(node.semantic_id, ""))
            if endpoint_id:
                target_id = typed_ids.get(schema_refs.get(node.semantic_id, ""))
                rows["RequestBody"][typed_id] = {
                    "request_body_id": typed_id,
                    "endpoint_id": endpoint_id,
                    "content_type": _str(summary.get("contentType")),
                    "required": bool(summary.get("required")),
                    "root_component_ref": target_id or "",
                }
        elif node.kind == "Response":
            endpoint_id = typed_ids.get(endpoint_parent.get(node.semantic_id, ""))
            if endpoint_id:
                target_id = typed_ids.get(schema_refs.get(node.semantic_id, ""))
                rows["Response"][typed_id] = {
                    "response_id": typed_id,
                    "endpoint_id": endpoint_id,
                    "status": _str(summary.get("status")),
                    "content_type": _str(summary.get("contentType")),
                    "root_component_ref": target_id or "",
                }
        elif node.kind == "SchemaComponent":
            row = _schema_row(ast, node, summary, typed_id, raw_by_pointer)
            _put_richest(rows["SchemaComponent"], typed_id, row)
        elif node.kind == "Property":
            parent_id = typed_ids.get(schema_parent.get(node.semantic_id, ""))
            if parent_id:
                rows["Property"][typed_id] = {
                    "property_id": typed_id,
                    "parent_component_id": parent_id,
                    "name": node.name,
                    "type": _str(summary.get("type")),
                    "format": _str(summary.get("format")),
                    "required": bool(summary.get("required")),
                    "enumValues": _string_list(summary.get("enumValues")),
                    "description": _str(summary.get("description")),
                    "supportedDeviceTypes": _string_list(summary.get("x-supportedDeviceType")),
                    "yangPath": _str(summary.get("x-path")),
                    "extensionsJson": _json(summary.get("xExtensions", {})),
                    "readOnly": bool(summary.get("readOnly")),
                }
        elif node.kind == "YangPath":
            module = _str(summary.get("module"))
            rows["YangPath"][typed_id] = {"yangPath": typed_id, "module": module}
            if module:
                rows["YangModule"][module] = {"module": module}
                rels["IN_MODULE"][(typed_id, module)] = {"a": typed_id, "b": module}
        elif node.kind == "CliCommand":
            endpoint_id = typed_ids.get(endpoint_parent.get(node.semantic_id, ""))
            if endpoint_id:
                rows["CliCommand"][typed_id] = {
                    "command_id": typed_id,
                    "commandName": _str(summary.get("commandName")) or node.name,
                    "commandUse": _str(summary.get("commandUse")),
                    "parentCommand": _str(summary.get("parentCommand")),
                    "pathToPrint": _str(summary.get("pathToPrint")),
                    "paramKeys": _string_list(summary.get("paramKeys")),
                }

    for edge in semantic.edges:
        row = _edge_row(edge, nodes, typed_ids)
        if not row:
            continue
        table, payload = row
        key = tuple(payload.values())
        rels[table][key] = payload


def _typed_node_id(
    ast: AstGraph,
    node: SemanticNode,
    summary: dict[str, Any],
    endpoint_parent: dict[str, str],
    schema_parent: dict[str, str],
    typed_ids: dict[str, str],
    nodes: dict[str, SemanticNode],
    summaries: dict[str, dict[str, Any]],
) -> str:
    if node.kind == "ApiEndpoint":
        return f"{summary.get('method')}:{summary.get('path')}"
    if node.kind == "SchemaComponent":
        return _component_id(ast, node)
    if node.kind == "Property":
        parent = nodes.get(schema_parent.get(node.semantic_id, ""))
        parent_id = _component_id(ast, parent) if parent else ""
        return f"{parent_id}#prop:{node.name}" if parent_id else _fallback_id(ast, "property", node)
    if node.kind == "Parameter":
        endpoint_id = _parent_endpoint_id(node, endpoint_parent, nodes, summaries)
        return f"{endpoint_id}#param:{summary.get('in')}:{node.name}" if endpoint_id else ""
    if node.kind == "RequestBody":
        endpoint_id = _parent_endpoint_id(node, endpoint_parent, nodes, summaries)
        suffix = _safe_id_part(_str(summary.get("contentType")))
        return f"{endpoint_id}#requestBody:{suffix}" if endpoint_id else ""
    if node.kind == "Response":
        endpoint_id = _parent_endpoint_id(node, endpoint_parent, nodes, summaries)
        status = _safe_id_part(_str(summary.get("status")))
        media = _safe_id_part(_str(summary.get("contentType")))
        return f"{endpoint_id}#response:{status}:{media}" if endpoint_id else ""
    if node.kind == "YangPath":
        return _str(summary.get("yangPath")) or node.name
    if node.kind == "CliCommand":
        endpoint_id = _parent_endpoint_id(node, endpoint_parent, nodes, summaries)
        command = _str(summary.get("commandName")) or node.name
        return f"{endpoint_id}::{command}" if endpoint_id else ""
    return ""


def _parent_endpoint_id(
    node: SemanticNode,
    endpoint_parent: dict[str, str],
    nodes: dict[str, SemanticNode],
    summaries: dict[str, dict[str, Any]],
) -> str:
    parent = nodes.get(endpoint_parent.get(node.semantic_id, ""))
    if parent is None:
        return ""
    summary = summaries.get(parent.semantic_id, {})
    return f"{summary.get('method')}:{summary.get('path')}"


def _component_id(ast: AstGraph, node: SemanticNode | None) -> str:
    if node is None:
        return ""
    provider = _provider(ast)
    parts = _pointer_parts(node.json_pointer)
    if len(parts) >= 3 and parts[0] == "components":
        section = parts[1]
        name = parts[2]
        if len(parts) == 3:
            return f"{provider}:{section}:{name}"
    return _fallback_id(ast, "inline", node)


def _schema_row(
    ast: AstGraph,
    node: SemanticNode,
    summary: dict[str, Any],
    component_id: str,
    raw_by_pointer: dict[str, str],
) -> dict[str, Any]:
    provider = _provider(ast)
    parts = _pointer_parts(node.json_pointer)
    section = parts[1] if len(parts) >= 3 and parts[0] == "components" else "inline"
    body_json = raw_by_pointer.get(node.json_pointer, "")
    return {
        "component_id": component_id,
        "spec_source": provider,
        "section": section,
        "name": node.name,
        "type": _str(summary.get("type")),
        "kind": _str(summary.get("kind")),
        "bodyShape": _str(summary.get("bodyShape")),
        "required": _string_list(summary.get("required")),
        "enumValues": _string_list(summary.get("enumValues")),
        "supportedDeviceTypes": _string_list(summary.get("x-supportedDeviceType")),
        "bodyJson": body_json,
    }


def _edge_row(
    edge: SemanticEdge,
    nodes: dict[str, SemanticNode],
    typed_ids: dict[str, str],
) -> tuple[str, dict[str, Any]] | None:
    source = nodes.get(edge.source_id)
    target = nodes.get(edge.target_id)
    a = typed_ids.get(edge.source_id)
    b = typed_ids.get(edge.target_id)
    if source is None or target is None or not a or not b:
        return None
    if edge.kind in {
        "HAS_PARAMETER",
        "HAS_REQUEST_BODY",
        "HAS_RESPONSE",
        "BODY_REFERENCES",
        "RESPONSE_REFERENCES",
        "HAS_PROPERTY",
        "PROPERTY_OF_TYPE",
        "HAS_VALUE_SCHEMA",
        "PROPERTY_AT_YANG",
        "CONFIGURES_YANG",
        "HAS_CLI_COMMAND",
    }:
        return edge.kind, {"a": a, "b": b}
    evidence = _load_json(edge.evidence_json)
    if edge.kind == "COMPOSED_OF":
        return "COMPOSED_OF", {"a": a, "b": b, "kind": _str(evidence.get("composition"))}
    if edge.kind == "REFERENCES":
        via = _str(evidence.get("ref")) or _str(evidence.get("role")) or "ref"
        return "REFERENCES", {"a": a, "b": b, "via": via}
    return None


def _put_richest(rows: dict[str, dict[str, Any]], key: str, row: dict[str, Any]) -> None:
    current = rows.get(key)
    if current is None or len(row.get("bodyJson") or "") > len(current.get("bodyJson") or ""):
        rows[key] = row


def _apply_projection_schema(conn) -> None:
    for ddl in KNOWLEDGE_NODE_TABLES + KNOWLEDGE_REL_TABLES:
        conn.execute(ddl.strip())


def _copy(conn, table: str, rows: list[dict[str, Any]], schema: pa.Schema) -> None:
    if not rows:
        return
    columns: dict[str, list] = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            columns[field.name].append(row.get(field.name))
    conn.execute(f"COPY {table} FROM $df", parameters={"df": pa.table(columns, schema=schema)})


def _fallback_id(ast: AstGraph, prefix: str, node: SemanticNode) -> str:
    digest = hashlib.sha1(f"{ast.spec_id}\0{node.json_pointer}".encode("utf-8")).hexdigest()[:16]
    return f"{_provider(ast)}:{prefix}:{digest}"


def _provider(ast: AstGraph) -> str:
    source = ast.spec_row.get("source", "")
    return source.split("/", 1)[0] if "/" in source else source or "unknown"


def _safe_id_part(value: str) -> str:
    return value.replace("/", "~1").replace(":", "~3") or "_"


def _pointer_parts(pointer: str) -> list[str]:
    if not pointer:
        return []
    return [part.replace("~1", "/").replace("~0", "~") for part in pointer.strip("/").split("/")]


def _load_summary(node: SemanticNode) -> dict[str, Any]:
    return _load_json(node.summary_json)


def _load_json(raw: str) -> dict[str, Any]:
    try:
        value = json.loads(raw)
    except json.JSONDecodeError:
        return {}
    return value if isinstance(value, dict) else {}


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _string_list(value: Any) -> list[str]:
    if not isinstance(value, list):
        return []
    return [str(item) for item in value if isinstance(item, (str, int, float, bool))]


def _str(value: Any) -> str:
    return value if isinstance(value, str) else ""
