"""Task 3A semantic overlay builder for the OpenAPI AST graph.

The L2 overlay is intentionally compact.  It creates deterministic
"highway" nodes and edges for agent traversal while keeping the lossless
L1 AST as the source of truth and provenance for every semantic row.
"""

from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass, field
from typing import Any

from .ast_builder import AstGraph, AstNode

STRUCTURAL_RULE_PACK_ID = "semantic.structural.v1"

_HTTP_METHODS = {
    "get",
    "put",
    "post",
    "delete",
    "options",
    "head",
    "patch",
    "trace",
}


@dataclass(frozen=True)
class SemanticNode:
    semantic_id: str
    spec_id: str
    kind: str
    name: str
    ast_node_id: str
    json_pointer: str
    stable_key: str
    summary_json: str


@dataclass(frozen=True)
class SemanticEdge:
    source_id: str
    target_id: str
    kind: str
    rule_id: str
    evidence_json: str


@dataclass(frozen=True)
class SemanticDerivedFromEdge:
    semantic_id: str
    ast_node_id: str
    role: str


@dataclass
class SemanticGraph:
    spec_id: str
    rule_packs: tuple[str, ...] = (STRUCTURAL_RULE_PACK_ID,)
    nodes: list[SemanticNode] = field(default_factory=list)
    edges: list[SemanticEdge] = field(default_factory=list)
    derived_edges: list[SemanticDerivedFromEdge] = field(default_factory=list)


class _SemanticState:
    def __init__(self, ast_graph: AstGraph) -> None:
        self.ast_graph = ast_graph
        self.semantic_graph = SemanticGraph(spec_id=ast_graph.spec_id)
        self.ast_by_pointer = {n.json_pointer: n for n in ast_graph.nodes}
        self.semantic_by_key: dict[tuple[str, str], SemanticNode] = {}
        self.schema_by_pointer: dict[str, SemanticNode] = {}
        self.property_by_pointer: dict[str, SemanticNode] = {}
        self.yang_by_path: dict[str, SemanticNode] = {}
        self._seen_edges: set[tuple[str, str, str, str, str]] = set()

    def add_node(
        self,
        *,
        kind: str,
        stable_key: str,
        name: str,
        ast_pointer: str,
        summary: dict[str, Any],
    ) -> SemanticNode:
        key = (kind, stable_key)
        existing = self.semantic_by_key.get(key)
        if existing is not None:
            return existing
        ast_node = self.ast_by_pointer.get(ast_pointer)
        ast_node_id = ast_node.node_id if ast_node else ""
        node = SemanticNode(
            semantic_id=_semantic_id(self.ast_graph.spec_id, kind, stable_key),
            spec_id=self.ast_graph.spec_id,
            kind=kind,
            name=name,
            ast_node_id=ast_node_id,
            json_pointer=ast_pointer,
            stable_key=stable_key,
            summary_json=_json(summary),
        )
        self.semantic_by_key[key] = node
        self.semantic_graph.nodes.append(node)
        if ast_node_id:
            self.semantic_graph.derived_edges.append(
                SemanticDerivedFromEdge(
                    semantic_id=node.semantic_id,
                    ast_node_id=ast_node_id,
                    role="primary",
                )
            )
        return node

    def add_edge(
        self,
        source: SemanticNode | None,
        target: SemanticNode | None,
        *,
        kind: str,
        rule_id: str,
        evidence: dict[str, Any],
    ) -> None:
        if source is None or target is None:
            return
        evidence_json = _json(evidence)
        key = (source.semantic_id, target.semantic_id, kind, rule_id, evidence_json)
        if key in self._seen_edges:
            return
        self._seen_edges.add(key)
        self.semantic_graph.edges.append(
            SemanticEdge(
                source_id=source.semantic_id,
                target_id=target.semantic_id,
                kind=kind,
                rule_id=rule_id,
                evidence_json=evidence_json,
            )
        )


def build_semantic_overlay(ast_graph: AstGraph) -> SemanticGraph:
    """Build a compact semantic overlay from one lossless L1 AST graph."""
    state = _SemanticState(ast_graph)
    _build_schema_nodes(state)
    _build_property_nodes(state)
    _build_schema_edges(state)
    _build_endpoint_nodes_and_edges(state)
    return state.semantic_graph


def _build_schema_nodes(state: _SemanticState) -> None:
    for ast_node in state.ast_graph.nodes:
        if not _is_schema_ast_node(ast_node):
            continue
        body = _load_object(ast_node.raw_json)
        if body is None:
            continue
        pointer = ast_node.json_pointer
        node = state.add_node(
            kind="SchemaComponent",
            stable_key=f"schema:{pointer or '/'}",
            name=_schema_name(ast_node),
            ast_pointer=pointer,
            summary={
                "bodyShape": _schema_shape(body),
                "description": _as_str(body.get("description")),
                "format": _as_str(body.get("format")),
                "isNamed": _is_named_component_schema(pointer),
                "type": _schema_type(body),
                "x-supportedDeviceType": _supported_device_types(body),
            },
        )
        state.schema_by_pointer[pointer] = node


def _build_property_nodes(state: _SemanticState) -> None:
    parent_by_child = _parent_by_child(state.ast_graph)
    for ast_node in state.ast_graph.nodes:
        if not _is_property_ast_node(ast_node):
            continue
        body = _load_object(ast_node.raw_json)
        if body is None:
            continue
        property_name = ast_node.key or ast_node.name or _last_pointer_part(ast_node.json_pointer)
        parent_schema = _parent_schema_node(state, ast_node, parent_by_child)
        node = state.add_node(
            kind="Property",
            stable_key=f"property:{ast_node.json_pointer}",
            name=property_name,
            ast_pointer=ast_node.json_pointer,
            summary={
                "description": _as_str(body.get("description")),
                "format": _as_str(body.get("format")),
                "required": _is_required_property(parent_schema, property_name),
                "type": _schema_type(body),
                "x-supportedDeviceType": _supported_device_types(body),
                "x-path": body.get("x-path") if isinstance(body.get("x-path"), str) else "",
            },
        )
        state.property_by_pointer[ast_node.json_pointer] = node

        parent_semantic = (
            state.schema_by_pointer.get(parent_schema.json_pointer)
            if parent_schema is not None
            else None
        )
        state.add_edge(
            parent_semantic,
            node,
            kind="HAS_PROPERTY",
            rule_id=f"{STRUCTURAL_RULE_PACK_ID}.schema.properties",
            evidence={"propertyPointer": ast_node.json_pointer},
        )

        type_pointer = _schema_type_pointer(body, ast_node.json_pointer)
        type_node = state.schema_by_pointer.get(type_pointer)
        state.add_edge(
            node,
            type_node,
            kind="PROPERTY_OF_TYPE",
            rule_id=f"{STRUCTURAL_RULE_PACK_ID}.property.type",
            evidence={"schemaPointer": type_pointer},
        )

        yang_path = body.get("x-path")
        if isinstance(yang_path, str) and yang_path:
            yang_node = _ensure_yang_node(state, yang_path, ast_node.json_pointer)
            state.add_edge(
                node,
                yang_node,
                kind="PROPERTY_AT_YANG",
                rule_id=f"{STRUCTURAL_RULE_PACK_ID}.property.x-path",
                evidence={"propertyPointer": ast_node.json_pointer, "x-path": yang_path},
            )


def _build_schema_edges(state: _SemanticState) -> None:
    for pointer, node in state.schema_by_pointer.items():
        body = _get_pointer(state.ast_graph.spec, pointer)
        if not isinstance(body, dict):
            continue

        ref_pointer = _internal_ref_pointer(body.get("$ref"))
        if ref_pointer:
            state.add_edge(
                node,
                state.schema_by_pointer.get(ref_pointer),
                kind="REFERENCES",
                rule_id=f"{STRUCTURAL_RULE_PACK_ID}.schema.ref",
                evidence={"ref": body.get("$ref"), "schemaPointer": pointer},
            )

        for composition in ("allOf", "anyOf", "oneOf"):
            entries = body.get(composition)
            if not isinstance(entries, list):
                continue
            for index, child in enumerate(entries):
                if not isinstance(child, dict):
                    continue
                child_pointer = _join_pointer(pointer, composition, str(index))
                state.add_edge(
                    node,
                    state.schema_by_pointer.get(child_pointer),
                    kind="COMPOSED_OF",
                    rule_id=f"{STRUCTURAL_RULE_PACK_ID}.schema.composition",
                    evidence={
                        "composition": composition,
                        "index": index,
                        "schemaPointer": pointer,
                    },
                )

        for map_key in ("items", "additionalProperties"):
            child = body.get(map_key)
            if isinstance(child, dict):
                child_pointer = _join_pointer(pointer, map_key)
                state.add_edge(
                    node,
                    state.schema_by_pointer.get(child_pointer),
                    kind="HAS_VALUE_SCHEMA",
                    rule_id=f"{STRUCTURAL_RULE_PACK_ID}.schema.value",
                    evidence={"role": map_key, "schemaPointer": pointer},
                )


def _build_endpoint_nodes_and_edges(state: _SemanticState) -> None:
    paths = state.ast_graph.spec.get("paths")
    if not isinstance(paths, dict):
        return
    for path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method not in _HTTP_METHODS or not isinstance(operation, dict):
                continue
            op_pointer = _join_pointer("", "paths", path, method)
            endpoint = state.add_node(
                kind="ApiEndpoint",
                stable_key=f"endpoint:{method.upper()}:{path}",
                name=f"{method.upper()} {path}",
                ast_pointer=op_pointer,
                summary={
                    "description": _as_str(operation.get("description")),
                    "method": method.upper(),
                    "operationId": _as_str(operation.get("operationId")),
                    "path": path,
                    "summary": _as_str(operation.get("summary")),
                    "tags": [v for v in operation.get("tags", []) if isinstance(v, str)],
                },
            )
            _add_cli_command(state, endpoint, operation, op_pointer)
            _add_request_body_edges(state, endpoint, operation, op_pointer)
            _add_response_edges(state, endpoint, operation, op_pointer)


def _add_cli_command(
    state: _SemanticState,
    endpoint: SemanticNode,
    operation: dict[str, Any],
    op_pointer: str,
) -> None:
    cli = operation.get("x-cliParam")
    if not isinstance(cli, dict):
        return
    command_name = _as_str(cli.get("commandName")).strip()
    if not command_name:
        return
    param_keys: list[str] = []
    raw_param_keys = cli.get("paramKeys")
    if isinstance(raw_param_keys, list):
        for entry in raw_param_keys:
            if isinstance(entry, str):
                param_keys.append(entry)
            elif isinstance(entry, dict) and isinstance(entry.get("key"), str):
                param_keys.append(entry["key"])
    node = state.add_node(
        kind="CliCommand",
        stable_key=f"cli:{endpoint.stable_key}:{command_name}",
        name=command_name,
        ast_pointer=_join_pointer(op_pointer, "x-cliParam"),
        summary={
            "commandName": command_name,
            "commandUse": _as_str(cli.get("commandUse")),
            "parentCommand": _as_str(cli.get("parentCommand")),
            "paramKeys": param_keys,
            "pathToPrint": _as_str(cli.get("pathToPrint")),
        },
    )
    state.add_edge(
        endpoint,
        node,
        kind="HAS_CLI_COMMAND",
        rule_id=f"{STRUCTURAL_RULE_PACK_ID}.operation.x-cliParam",
        evidence={"operationPointer": op_pointer},
    )


def _add_request_body_edges(
    state: _SemanticState,
    endpoint: SemanticNode,
    operation: dict[str, Any],
    op_pointer: str,
) -> None:
    request_body = operation.get("requestBody")
    if not isinstance(request_body, dict):
        return
    request_pointer = _join_pointer(op_pointer, "requestBody")
    body, body_pointer = _resolve_reference_object(
        state.ast_graph.spec, request_body, request_pointer
    )
    if not isinstance(body, dict):
        return
    media, schema, schema_pointer = _pick_media_schema(body.get("content"), body_pointer)
    if not isinstance(schema, dict) or schema_pointer is None:
        return
    target_pointer = _schema_type_pointer(schema, schema_pointer)
    target = state.schema_by_pointer.get(target_pointer)
    state.add_edge(
        endpoint,
        target,
        kind="ACCEPTS_SCHEMA",
        rule_id=f"{STRUCTURAL_RULE_PACK_ID}.operation.requestBody",
        evidence={
            "contentType": media,
            "requestBodyPointer": body_pointer,
            "schemaPointer": schema_pointer,
            "targetPointer": target_pointer,
        },
    )
    for yang_path in _collect_yang_paths(state.ast_graph.spec, schema, schema_pointer):
        state.add_edge(
            endpoint,
            _ensure_yang_node(state, yang_path, schema_pointer),
            kind="CONFIGURES_YANG",
            rule_id=f"{STRUCTURAL_RULE_PACK_ID}.operation.requestBody.x-path",
            evidence={"schemaPointer": schema_pointer, "x-path": yang_path},
        )


def _add_response_edges(
    state: _SemanticState,
    endpoint: SemanticNode,
    operation: dict[str, Any],
    op_pointer: str,
) -> None:
    responses = operation.get("responses")
    if not isinstance(responses, dict):
        return
    for status, response in responses.items():
        if not isinstance(response, dict):
            continue
        response_pointer = _join_pointer(op_pointer, "responses", str(status))
        body, body_pointer = _resolve_reference_object(
            state.ast_graph.spec, response, response_pointer
        )
        if not isinstance(body, dict):
            continue
        media, schema, schema_pointer = _pick_media_schema(body.get("content"), body_pointer)
        if not isinstance(schema, dict) or schema_pointer is None:
            continue
        target_pointer = _schema_type_pointer(schema, schema_pointer)
        state.add_edge(
            endpoint,
            state.schema_by_pointer.get(target_pointer),
            kind="RETURNS_SCHEMA",
            rule_id=f"{STRUCTURAL_RULE_PACK_ID}.operation.response",
            evidence={
                "contentType": media,
                "responsePointer": body_pointer,
                "schemaPointer": schema_pointer,
                "status": str(status),
                "targetPointer": target_pointer,
            },
        )


def _ensure_yang_node(
    state: _SemanticState,
    yang_path: str,
    evidence_pointer: str,
) -> SemanticNode:
    existing = state.yang_by_path.get(yang_path)
    if existing is not None:
        return existing
    node = state.add_node(
        kind="YangPath",
        stable_key=f"yang:{yang_path}",
        name=yang_path,
        ast_pointer=evidence_pointer,
        summary={"module": _yang_module_for(yang_path), "yangPath": yang_path},
    )
    state.yang_by_path[yang_path] = node
    return node


def _parent_by_child(ast_graph: AstGraph) -> dict[str, AstNode]:
    nodes = {n.node_id: n for n in ast_graph.nodes}
    result: dict[str, AstNode] = {}
    for edge in ast_graph.child_edges:
        parent = nodes.get(edge.parent_id)
        if parent is not None:
            result[edge.child_id] = parent
    return result


def _parent_schema_node(
    state: _SemanticState,
    property_node: AstNode,
    parent_by_child: dict[str, AstNode],
) -> AstNode | None:
    map_node = parent_by_child.get(property_node.node_id)
    if map_node is None:
        return None
    parent = parent_by_child.get(map_node.node_id)
    while parent is not None and parent.json_pointer not in state.schema_by_pointer:
        parent = parent_by_child.get(parent.node_id)
    return parent


def _is_required_property(parent_schema: AstNode | None, property_name: str) -> bool:
    if parent_schema is None:
        return False
    body = _load_object(parent_schema.raw_json)
    if not body:
        return False
    required = body.get("required")
    return isinstance(required, list) and property_name in required


def _is_schema_ast_node(ast_node: AstNode) -> bool:
    if ast_node.value_type != "object":
        return False
    if ast_node.kind in {"Schema", "Property", "Items"}:
        return True
    if _is_property_ast_node(ast_node):
        return True
    parts = _pointer_parts(ast_node.json_pointer)
    return len(parts) >= 2 and parts[-2] in {"allOf", "anyOf", "oneOf", "prefixItems"}


def _is_property_ast_node(ast_node: AstNode) -> bool:
    if ast_node.value_type != "object":
        return False
    if ast_node.kind == "Property":
        return True
    parts = _pointer_parts(ast_node.json_pointer)
    return len(parts) >= 2 and parts[-2] in {"properties", "patternProperties"}


def _schema_name(ast_node: AstNode) -> str:
    if _is_named_component_schema(ast_node.json_pointer):
        return _last_pointer_part(ast_node.json_pointer)
    if ast_node.kind == "Items":
        parent_name = _last_pointer_part(_parent_pointer(ast_node.json_pointer))
        return f"{parent_name}.items" if parent_name else "items"
    return ast_node.name or ast_node.key or _last_pointer_part(ast_node.json_pointer) or "schema"


def _is_named_component_schema(pointer: str) -> bool:
    parts = _pointer_parts(pointer)
    return len(parts) == 3 and parts[0] == "components" and parts[1] == "schemas"


def _schema_type_pointer(schema: dict[str, Any], pointer: str) -> str:
    ref_pointer = _internal_ref_pointer(schema.get("$ref"))
    if ref_pointer:
        return ref_pointer
    items = schema.get("items")
    if isinstance(items, dict):
        return _join_pointer(pointer, "items")
    return pointer


def _pick_media_schema(
    content: Any,
    content_owner_pointer: str,
) -> tuple[str, dict[str, Any] | None, str | None]:
    if not isinstance(content, dict) or not content:
        return "", None, None
    preferred = None
    for media in ("application/json", "application/*+json"):
        if media in content:
            preferred = media
            break
    if preferred is None:
        preferred = next(iter(content))
    media_type = content.get(preferred)
    if not isinstance(media_type, dict):
        return str(preferred), None, None
    schema = media_type.get("schema")
    if not isinstance(schema, dict):
        return str(preferred), None, None
    return str(preferred), schema, _join_pointer(content_owner_pointer, "content", str(preferred), "schema")


def _resolve_reference_object(
    spec: dict[str, Any],
    obj: dict[str, Any],
    pointer: str,
) -> tuple[dict[str, Any] | None, str]:
    ref_pointer = _internal_ref_pointer(obj.get("$ref"))
    if not ref_pointer:
        return obj, pointer
    target = _get_pointer(spec, ref_pointer)
    return (target, ref_pointer) if isinstance(target, dict) else (None, ref_pointer)


def _collect_yang_paths(
    spec: dict[str, Any],
    schema: dict[str, Any],
    pointer: str,
    seen_refs: set[str] | None = None,
) -> set[str]:
    seen_refs = seen_refs or set()
    result: set[str] = set()
    yang_path = schema.get("x-path")
    if isinstance(yang_path, str) and yang_path:
        result.add(yang_path)

    ref_pointer = _internal_ref_pointer(schema.get("$ref"))
    if ref_pointer and ref_pointer not in seen_refs:
        seen_refs.add(ref_pointer)
        target = _get_pointer(spec, ref_pointer)
        if isinstance(target, dict):
            result.update(_collect_yang_paths(spec, target, ref_pointer, seen_refs))

    for key, value in schema.items():
        if key == "$ref":
            continue
        child_pointer = _join_pointer(pointer, str(key))
        if isinstance(value, dict):
            result.update(_collect_yang_paths(spec, value, child_pointer, seen_refs))
        elif isinstance(value, list):
            for index, entry in enumerate(value):
                if isinstance(entry, dict):
                    result.update(
                        _collect_yang_paths(
                            spec,
                            entry,
                            _join_pointer(child_pointer, str(index)),
                            seen_refs,
                        )
                    )
    return result


def _internal_ref_pointer(ref: Any) -> str:
    if not isinstance(ref, str) or not ref.startswith("#/"):
        return ""
    return "/" + "/".join(_unescape_pointer(part) for part in ref[2:].split("/"))


def _get_pointer(obj: Any, pointer: str) -> Any:
    if pointer in ("", "/"):
        return obj
    current = obj
    for part in _pointer_parts(pointer):
        if isinstance(current, dict):
            current = current.get(part)
        elif isinstance(current, list):
            try:
                current = current[int(part)]
            except (ValueError, IndexError):
                return None
        else:
            return None
    return current


def _pointer_parts(pointer: str) -> list[str]:
    if not pointer:
        return []
    return [_unescape_pointer(part) for part in pointer.strip("/").split("/")]


def _join_pointer(base: str, *parts: str) -> str:
    pointer = base.rstrip("/")
    for part in parts:
        pointer += "/" + _escape_pointer(part)
    return pointer


def _parent_pointer(pointer: str) -> str:
    parts = _pointer_parts(pointer)
    if not parts:
        return ""
    result = ""
    for part in parts[:-1]:
        result = _join_pointer(result, part)
    return result


def _last_pointer_part(pointer: str) -> str:
    parts = _pointer_parts(pointer)
    return parts[-1] if parts else ""


def _escape_pointer(value: str) -> str:
    return value.replace("~", "~0").replace("/", "~1")


def _unescape_pointer(value: str) -> str:
    return value.replace("~1", "/").replace("~0", "~")


def _schema_shape(body: dict[str, Any]) -> str:
    if isinstance(body.get("enum"), list) and body["enum"]:
        return "primitive"
    if isinstance(body.get("oneOf"), list) and body["oneOf"]:
        return "union-oneOf"
    if isinstance(body.get("anyOf"), list) and body["anyOf"]:
        return "union-anyOf"
    if isinstance(body.get("allOf"), list) and body["allOf"]:
        return "allOf-composite"
    if isinstance(body.get("properties"), dict) and body["properties"]:
        return "object"
    additional_properties = body.get("additionalProperties")
    if additional_properties is True or isinstance(additional_properties, dict):
        return "map"
    if body.get("type") == "object":
        return "object"
    if body.get("type") == "array":
        return "array"
    return "primitive"


def _schema_type(body: dict[str, Any]) -> str:
    value = body.get("type")
    if isinstance(value, str):
        return value
    if isinstance(value, list):
        return "|".join(str(v) for v in value)
    if "$ref" in body:
        return "$ref"
    return ""


def _supported_device_types(body: dict[str, Any]) -> list[str] | None:
    raw = body.get("x-supportedDeviceType")
    if raw is None:
        return None
    if isinstance(raw, str):
        return [raw]
    if isinstance(raw, list):
        return [v for v in raw if isinstance(v, str)]
    return None


def _yang_module_for(yang_path: str) -> str:
    for part in yang_path.split("/"):
        if ":" in part:
            return part.split(":", 1)[0]
    return ""


def _load_object(raw_json: str) -> dict[str, Any] | None:
    try:
        value = json.loads(raw_json)
    except json.JSONDecodeError:
        return None
    return value if isinstance(value, dict) else None


def _json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"))


def _as_str(value: Any) -> str:
    return value if isinstance(value, str) else ""


def _semantic_id(spec_id: str, kind: str, stable_key: str) -> str:
    digest = hashlib.sha1(f"{spec_id}\0{kind}\0{stable_key}".encode("utf-8")).hexdigest()
    return f"sem:{digest[:24]}"
