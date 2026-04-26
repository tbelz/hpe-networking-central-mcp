"""Build-time helper that decomposes a normalised OAS spec into the
schema subgraph defined by ADR 009.

Given a normalised spec and a list of ``(method, path)`` endpoints, this
populates the following node + relationship tables (created by
``graph/schema.py``):

  Parameter, RequestBody, Response, SchemaComponent
  HAS_PARAMETER, HAS_REQUEST_BODY, HAS_RESPONSE,
  BODY_REFERENCES, RESPONSE_REFERENCES, REFERENCES

The helper is idempotent: it issues ``MERGE`` statements keyed on
deterministic IDs, so re-running on the same spec does not double-insert.

It does **not** create the underlying ``ApiEndpoint`` rows — those are
populated separately by ``scripts/build_knowledge_db.py``.  The helper
silently skips any endpoint whose ``ApiEndpoint`` row is missing so
build ordering does not become a hard constraint.
"""

from __future__ import annotations

import hashlib
import json
from typing import Any, Iterable

from .oas_normalize import (
    _extract_referenced_components,
    _follow_ref,
    _strip_skeleton_keys,
)


# ── Cypher literal escaping (real_ladybug parameter binding bug) ────


def _esc(value: str) -> str:
    """Escape a Python string for inline embedding as a Cypher string literal."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _str_list_literal(values: Iterable[str]) -> str:
    """Emit a Cypher list literal of strings (real_ladybug rejects STRING[] params).

    Empty lists need an explicit CAST so Kuzu can infer the element type.
    """
    items = [f"'{_esc(v)}'" for v in values if isinstance(v, str)]
    if not items:
        return "CAST([] AS STRING[])"
    return "[" + ", ".join(items) + "]"


# ── Component IDs ────────────────────────────────────────────────────


def _named_component_id(spec_source: str, section: str, name: str) -> str:
    return f"{spec_source}:{section}:{name}"


def _anon_component_id(spec_source: str, body: Any) -> str:
    digest = hashlib.sha1(
        json.dumps(body, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()[:12]
    return f"{spec_source}:anon:{digest}"


# ── Schema kind/type inference ───────────────────────────────────────


def _component_kind(body: dict) -> str:
    if not isinstance(body, dict):
        return "primitive"
    for kw in ("oneOf", "anyOf", "allOf"):
        if isinstance(body.get(kw), list) and body[kw]:
            return "union"
    t = body.get("type")
    if t == "object" or "properties" in body:
        return "object"
    if t == "array":
        return "array"
    return "primitive"


# ── Parameter hint inference ─────────────────────────────────────────


def _infer_param_hint(param: dict) -> str:
    """Return a short tag describing the semantic shape of a parameter.

    Heuristic only — falls back to '' when nothing matches.
    """
    schema = param.get("schema") or {}
    name = (param.get("name") or "").lower()
    fmt = schema.get("format") or ""
    if fmt in ("date-time", "date"):
        return f"rfc3339-{fmt}"
    if name in ("filter", "$filter") or "odata" in name:
        return "odata-filter"
    if name in ("orderby", "$orderby", "order_by"):
        return "odata-orderby"
    if "csv" in name or name.endswith("_list") or name.endswith("Ids"):
        if schema.get("type") == "string":
            return "comma-list"
    if name in ("limit", "offset", "page", "page_size"):
        return "pagination"
    return ""


# ── Pick media schema (mirror oas_normalize._pick_media_schema) ─────


def _pick_media_schema(content: dict | None) -> tuple[str, dict | None]:
    if not isinstance(content, dict) or not content:
        return "", None
    if "application/json" in content:
        return "application/json", (content["application/json"] or {}).get("schema")
    media, body = next(iter(content.items()))
    return media, (body or {}).get("schema")


# ── REFERENCES walker ───────────────────────────────────────────────


_VIA_KEYS = ("properties", "items", "allOf", "oneOf", "anyOf", "additionalProperties")


def _walk_refs_with_site(node: Any, current_via: str = "") -> list[tuple[str, str]]:
    """Yield every ``$ref`` reachable from ``node`` plus the structural
    site (``properties`` | ``items`` | ``allOf`` | ``oneOf`` | ``anyOf``
    | ``additionalProperties``) it appeared under.

    The first ref encountered above any of the via keys takes
    ``current_via`` ("" for the root payload).
    """
    out: list[tuple[str, str]] = []
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            out.append((ref, current_via))
            # Do not descend through a $ref — the resolved component is
            # walked separately by the caller.
            return out
        for key, value in node.items():
            next_via = key if key in _VIA_KEYS else current_via
            out.extend(_walk_refs_with_site(value, next_via))
    elif isinstance(node, list):
        for item in node:
            out.extend(_walk_refs_with_site(item, current_via))
    return out


def _root_ref(schema: Any) -> str | None:
    """Return the top-level ``$ref`` of a schema fragment, if any."""
    if isinstance(schema, dict) and isinstance(schema.get("$ref"), str):
        return schema["$ref"]
    return None


# ── Find operation + components ─────────────────────────────────────


def _find_operation(spec: dict, method: str, path: str) -> dict | None:
    paths = spec.get("paths") or {}
    path_item = paths.get(path)
    if not isinstance(path_item, dict):
        return None
    op = path_item.get(method.lower())
    return op if isinstance(op, dict) else None


def _section_for_ref(ref: str) -> str:
    parts = ref.removeprefix("#/components/").split("/")
    return parts[0] if parts else "schemas"


def _name_for_ref(ref: str) -> str:
    parts = ref.removeprefix("#/components/").split("/")
    return parts[1] if len(parts) >= 2 else ""


# ── Public API ──────────────────────────────────────────────────────


def populate_schema_graph(
    conn,
    *,
    spec_source: str,
    spec: dict,
    endpoints: list[tuple[str, str]],
) -> dict:
    """Decompose ``spec`` into schema-subgraph rows for ``endpoints``.

    Returns a small stats dict ``{"parameters": N, "request_bodies": N,
    "responses": N, "components": N}`` for the build
    log.  The helper assumes the underlying ``ApiEndpoint`` row exists;
    endpoints without a row are silently skipped.
    """
    components = spec.get("components") or {}
    stats = {
        "endpoints": 0,
        "parameters": 0,
        "request_bodies": 0,
        "responses": 0,
        "components": 0,
        "properties": 0,
        "references": 0,
    }

    # Track which (component_id, target_id, via) edges have been written
    # so we do not double-insert REFERENCES even within a single call.
    written_refs: set[tuple[str, str, str]] = set()
    # Track which component_ids have already been MERGE-d so we only
    # MERGE each once (cheaper + more deterministic logs).
    written_components: set[str] = set()

    for method, path in endpoints:
        method_u = method.upper()
        op = _find_operation(spec, method_u, path)
        if op is None:
            continue
        eid = f"{method_u}:{path}"

        # ── Verify the ApiEndpoint row exists; skip silently otherwise.
        rows = list(conn.execute(
            "MATCH (e:ApiEndpoint {endpoint_id: $eid}) RETURN COUNT(e) AS c",
            parameters={"eid": eid},
        ).rows_as_dict())
        if not rows or rows[0]["c"] == 0:
            continue
        stats["endpoints"] += 1

        # ── Parameters ──
        for idx, raw_p in enumerate(op.get("parameters") or []):
            if not isinstance(raw_p, dict):
                continue
            resolved = raw_p
            if "$ref" in raw_p:
                target = _follow_ref(raw_p["$ref"], components)
                if isinstance(target, dict):
                    resolved = target
            schema = resolved.get("schema") or {}
            pname = resolved.get("name") or ""
            ploc = resolved.get("in") or ""
            param_id = f"{eid}#param:{ploc}:{pname or idx}"
            enum_lit = _str_list_literal(
                v for v in (schema.get("enum") or []) if isinstance(v, str)
            )
            conn.execute(
                "MERGE (p:Parameter {parameter_id: $pid}) SET "
                "p.endpoint_id = $eid, p.name = $name, p.location = $loc, "
                "p.required = $req, p.type = $ty, p.format = $fmt, "
                f"p.enumValues = {enum_lit}, "
                "p.pattern = $pat, p.inferredHint = $ph, "
                "p.description = $pdesc",
                parameters={
                    "pid": param_id,
                    "eid": eid,
                    "name": pname,
                    "loc": ploc,
                    "req": bool(resolved.get("required", False)),
                    "ty": str(schema.get("type") or ""),
                    "fmt": str(schema.get("format") or ""),
                    "pat": str(schema.get("pattern") or ""),
                    "ph": _infer_param_hint(resolved),
                    "pdesc": str(resolved.get("description") or ""),
                },
            )
            conn.execute(
                "MATCH (e:ApiEndpoint {endpoint_id: $eid}), "
                "(p:Parameter {parameter_id: $pid}) "
                "MERGE (e)-[:HAS_PARAMETER]->(p)",
                parameters={"eid": eid, "pid": param_id},
            )
            stats["parameters"] += 1

        # ── Request body ──
        rb = op.get("requestBody")
        if isinstance(rb, dict):
            rb_resolved = rb
            if "$ref" in rb:
                target = _follow_ref(rb["$ref"], components)
                if isinstance(target, dict):
                    rb_resolved = target
            content = rb_resolved.get("content") or {}
            media, schema = _pick_media_schema(content)
            rb_id = f"{eid}#requestBody"
            root_ref = _root_ref(schema) or ""
            conn.execute(
                "MERGE (rb:RequestBody {request_body_id: $rid}) SET "
                "rb.endpoint_id = $eid, rb.content_type = $ct, "
                "rb.required = $req, rb.root_component_ref = $rr",
                parameters={
                    "rid": rb_id,
                    "eid": eid,
                    "ct": media,
                    "req": bool(rb_resolved.get("required", False)),
                    "rr": root_ref,
                },
            )
            conn.execute(
                "MATCH (e:ApiEndpoint {endpoint_id: $eid}), "
                "(rb:RequestBody {request_body_id: $rid}) "
                "MERGE (e)-[:HAS_REQUEST_BODY]->(rb)",
                parameters={"eid": eid, "rid": rb_id},
            )
            stats["request_bodies"] += 1
            # Link to root component if the body is a single $ref.
            if root_ref:
                comp_id = _ensure_component_node(
                    conn, spec_source, root_ref, components, written_components, stats
                )
                if comp_id:
                    conn.execute(
                        "MATCH (rb:RequestBody {request_body_id: $rid}), "
                        "(c:SchemaComponent {component_id: $cid}) "
                        "MERGE (rb)-[:BODY_REFERENCES]->(c)",
                        parameters={"rid": rb_id, "cid": comp_id},
                    )

        # ── Responses ──
        for status, resp in (op.get("responses") or {}).items():
            if not isinstance(resp, dict):
                continue
            resp_resolved = resp
            if "$ref" in resp:
                target = _follow_ref(resp["$ref"], components)
                if isinstance(target, dict):
                    resp_resolved = target
            content = resp_resolved.get("content") or {}
            media, schema = _pick_media_schema(content)
            resp_id = f"{eid}#response:{status}"
            root_ref = _root_ref(schema) or ""
            conn.execute(
                "MERGE (r:Response {response_id: $rid}) SET "
                "r.endpoint_id = $eid, r.status = $st, "
                "r.content_type = $ct, r.root_component_ref = $rr",
                parameters={
                    "rid": resp_id,
                    "eid": eid,
                    "st": str(status),
                    "ct": media,
                    "rr": root_ref,
                },
            )
            conn.execute(
                "MATCH (e:ApiEndpoint {endpoint_id: $eid}), "
                "(r:Response {response_id: $rid}) "
                "MERGE (e)-[:HAS_RESPONSE]->(r)",
                parameters={"eid": eid, "rid": resp_id},
            )
            stats["responses"] += 1
            if root_ref:
                comp_id = _ensure_component_node(
                    conn, spec_source, root_ref, components, written_components, stats
                )
                if comp_id:
                    conn.execute(
                        "MATCH (r:Response {response_id: $rid}), "
                        "(c:SchemaComponent {component_id: $cid}) "
                        "MERGE (r)-[:RESPONSE_REFERENCES]->(c)",
                        parameters={"rid": resp_id, "cid": comp_id},
                    )

        # ── Walk transitively-referenced components + REFERENCES edges ──
        # Use the canonical ref walker to discover every reachable
        # component, then for each one, MERGE its node and its outgoing
        # REFERENCES edges with their `via` site recorded.
        refs_root: dict[str, Any] = {
            "parameters": op.get("parameters") or [],
            "requestBody": op.get("requestBody"),
            "responses": op.get("responses") or {},
        }
        side = _extract_referenced_components(refs_root, components)
        for section, entries in side.items():
            if not isinstance(entries, dict):
                continue
            for name, body in entries.items():
                ref = f"#/components/{section}/{name}"
                comp_id = _ensure_component_node(
                    conn, spec_source, ref, components, written_components, stats
                )
                if not comp_id:
                    continue
                # Outgoing REFERENCES edges from this component.
                for child_ref, via in _walk_refs_with_site(body):
                    if not child_ref.startswith("#/components/"):
                        continue
                    child_id = _ensure_component_node(
                        conn, spec_source, child_ref, components, written_components, stats
                    )
                    if not child_id or child_id == comp_id:
                        continue
                    edge_key = (comp_id, child_id, via)
                    if edge_key in written_refs:
                        continue
                    written_refs.add(edge_key)
                    conn.execute(
                        "MATCH (a:SchemaComponent {component_id: $aid}), "
                        "(b:SchemaComponent {component_id: $bid}) "
                        "MERGE (a)-[:REFERENCES {via: $via}]->(b)",
                        parameters={"aid": comp_id, "bid": child_id, "via": via},
                    )
                    stats["references"] += 1

    return stats


# ── Component MERGE ──────────────────────────────────────────────────


def _ensure_component_node(
    conn,
    spec_source: str,
    ref: str,
    full_components: dict,
    written: set[str],
    stats: dict,
) -> str:
    """MERGE the SchemaComponent node for ``ref`` and return its component_id.

    Also emits the property-level subgraph (HAS_PROPERTY edges to
    ``Property`` nodes, COMPOSED_OF edges for allOf/oneOf/anyOf
    branches) so the agent can query properties + vendor extensions
    directly without reading ``bodyJson``.

    Returns "" if the ref cannot be resolved.
    """
    if not ref.startswith("#/components/"):
        return ""
    section = _section_for_ref(ref)
    name = _name_for_ref(ref)
    if not section or not name:
        return ""
    comp_id = _named_component_id(spec_source, section, name)
    if comp_id in written:
        return comp_id

    body = _follow_ref(ref, full_components)
    if not isinstance(body, dict):
        return ""
    stripped = _strip_skeleton_keys(body)
    body_json = json.dumps(stripped)
    enum_lit = _str_list_literal(
        v for v in (body.get("enum") or []) if isinstance(v, str)
    )
    _req = body.get("required")
    required_lit = _str_list_literal(
        v for v in (_req if isinstance(_req, list) else []) if isinstance(v, str)
    )
    conn.execute(
        "MERGE (c:SchemaComponent {component_id: $cid}) SET "
        "c.spec_source = $src, c.section = $sec, c.name = $name, "
        "c.type = $ty, c.kind = $kind, "
        f"c.required = {required_lit}, c.enumValues = {enum_lit}, "
        f"c.bodyJson = '{_esc(body_json)}'",
        parameters={
            "cid": comp_id,
            "src": spec_source,
            "sec": section,
            "name": name,
            "ty": str(body.get("type") or ""),
            "kind": _component_kind(body),
        },
    )
    written.add(comp_id)
    stats["components"] += 1

    # ── Property-level extraction + allOf flattening (Phase 2C) ──
    _emit_property_subgraph(
        conn,
        spec_source=spec_source,
        parent_component_id=comp_id,
        parent_component_name=name,
        body=body,
        full_components=full_components,
        written=written,
        stats=stats,
    )

    return comp_id


# ── Property-level extraction (ADR 009 Phase 2C) ────────────────────


# Vendor extensions that get promoted to typed columns on Property
# nodes for first-class Cypher filtering. Every other ``x-*`` key is
# preserved verbatim under ``extensionsJson``.
_TYPED_EXT_DEVICE_TYPE = "x-supportedDeviceType"
_TYPED_EXT_YANG_PATH = "x-path"


def _collect_x_extensions(prop_body: dict) -> dict:
    """Return every ``x-*`` key from ``prop_body`` as a plain dict."""
    return {k: v for k, v in prop_body.items() if isinstance(k, str) and k.startswith("x-")}


def _resolve_property_schema(prop_body: Any, full_components: dict) -> dict:
    """Resolve a property's schema body, following one level of $ref.

    Returns the resolved dict (the original property body if no $ref).
    """
    if not isinstance(prop_body, dict):
        return {}
    ref = prop_body.get("$ref")
    if isinstance(ref, str):
        target = _follow_ref(ref, full_components)
        if isinstance(target, dict):
            return target
    return prop_body


def _emit_property_subgraph(
    conn,
    *,
    spec_source: str,
    parent_component_id: str,
    parent_component_name: str,
    body: dict,
    full_components: dict,
    written: set[str],
    stats: dict,
) -> None:
    """Walk ``body`` and emit ``HAS_PROPERTY`` / ``COMPOSED_OF`` /
    ``PROPERTY_OF_TYPE`` edges for ``parent_component_id``.

    For ``allOf`` branches:
      - emits ``COMPOSED_OF {kind: 'allOf'}`` to every $ref branch,
      - flattens leaf properties (both inline and from $ref branches)
        onto the parent with ``inheritedFrom`` set to the originating
        branch component (or "" for inline).

    For ``oneOf`` / ``anyOf``: only emits ``COMPOSED_OF`` (no
    flattening, since those are alternatives).
    """
    # Direct properties (no allOf composition).
    _req = body.get("required")
    own_required = set(
        v for v in (_req if isinstance(_req, list) else []) if isinstance(v, str)
    )
    own_props = body.get("properties") or {}
    if isinstance(own_props, dict):
        for prop_name, prop_body in own_props.items():
            _emit_one_property(
                conn,
                spec_source=spec_source,
                parent_component_id=parent_component_id,
                prop_name=prop_name,
                prop_body=prop_body if isinstance(prop_body, dict) else {},
                required=prop_name in own_required,
                inherited_from="",
                full_components=full_components,
                written=written,
                stats=stats,
            )

    # Composition keywords.
    for kind in ("allOf", "oneOf", "anyOf"):
        branches = body.get(kind)
        if not isinstance(branches, list):
            continue
        for branch in branches:
            if not isinstance(branch, dict):
                continue
            ref = branch.get("$ref")
            if isinstance(ref, str):
                # Materialise the target component (recurses into its
                # own property subgraph) and record COMPOSED_OF.
                target_id = _ensure_component_node(
                    conn, spec_source, ref, full_components, written, stats
                )
                if target_id:
                    conn.execute(
                        "MATCH (a:SchemaComponent {component_id: $aid}), "
                        "(b:SchemaComponent {component_id: $bid}) "
                        "MERGE (a)-[:COMPOSED_OF {kind: $kind}]->(b)",
                        parameters={
                            "aid": parent_component_id,
                            "bid": target_id,
                            "kind": kind,
                        },
                    )
                # Flatten allOf so the parent exposes every reachable
                # leaf in one HAS_PROPERTY hop.
                if kind == "allOf":
                    target_body = _follow_ref(ref, full_components)
                    if isinstance(target_body, dict):
                        branch_name = _name_for_ref(ref)
                        _flatten_allof_properties(
                            conn,
                            spec_source=spec_source,
                            parent_component_id=parent_component_id,
                            branch_body=target_body,
                            inherited_from=branch_name,
                            full_components=full_components,
                            written=written,
                            stats=stats,
                        )
            else:
                # Inline branch — for allOf, walk its own properties
                # (and any further nested allOf) onto the parent with
                # inheritedFrom="" since they are defined directly here.
                if kind == "allOf":
                    _flatten_allof_properties(
                        conn,
                        spec_source=spec_source,
                        parent_component_id=parent_component_id,
                        branch_body=branch,
                        inherited_from="",
                        full_components=full_components,
                        written=written,
                        stats=stats,
                    )


def _flatten_allof_properties(
    conn,
    *,
    spec_source: str,
    parent_component_id: str,
    branch_body: dict,
    inherited_from: str,
    full_components: dict,
    written: set[str],
    stats: dict,
) -> None:
    """Emit HAS_PROPERTY edges from the parent for every leaf property
    contributed by an ``allOf`` branch (recurses through nested allOf)."""
    _req = branch_body.get("required")
    branch_required = set(
        v for v in (_req if isinstance(_req, list) else []) if isinstance(v, str)
    )
    props = branch_body.get("properties") or {}
    if isinstance(props, dict):
        for prop_name, prop_body in props.items():
            _emit_one_property(
                conn,
                spec_source=spec_source,
                parent_component_id=parent_component_id,
                prop_name=prop_name,
                prop_body=prop_body if isinstance(prop_body, dict) else {},
                required=prop_name in branch_required,
                inherited_from=inherited_from,
                full_components=full_components,
                written=written,
                stats=stats,
            )

    nested = branch_body.get("allOf")
    if isinstance(nested, list):
        for sub in nested:
            if not isinstance(sub, dict):
                continue
            ref = sub.get("$ref")
            if isinstance(ref, str):
                target_body = _follow_ref(ref, full_components)
                if isinstance(target_body, dict):
                    nested_name = _name_for_ref(ref) or inherited_from
                    _flatten_allof_properties(
                        conn,
                        spec_source=spec_source,
                        parent_component_id=parent_component_id,
                        branch_body=target_body,
                        inherited_from=nested_name,
                        full_components=full_components,
                        written=written,
                        stats=stats,
                    )
            else:
                _flatten_allof_properties(
                    conn,
                    spec_source=spec_source,
                    parent_component_id=parent_component_id,
                    branch_body=sub,
                    inherited_from=inherited_from,
                    full_components=full_components,
                    written=written,
                    stats=stats,
                )


def _emit_one_property(
    conn,
    *,
    spec_source: str,
    parent_component_id: str,
    prop_name: str,
    prop_body: dict,
    required: bool,
    inherited_from: str,
    full_components: dict,
    written: set[str],
    stats: dict,
) -> None:
    """MERGE one Property node + HAS_PROPERTY edge.

    Also emits PROPERTY_OF_TYPE when the property's value is itself a
    component reference (top-level $ref or array of $ref).
    """
    resolved = _resolve_property_schema(prop_body, full_components)
    extensions = _collect_x_extensions(prop_body)
    # If the resolved target also carries x-* (rare), merge into the
    # property's extensions but do NOT clobber the call-site values.
    for k, v in _collect_x_extensions(resolved).items():
        extensions.setdefault(k, v)

    sdt_raw = extensions.get(_TYPED_EXT_DEVICE_TYPE) or []
    if isinstance(sdt_raw, list):
        sdt = [v for v in sdt_raw if isinstance(v, str)]
    elif isinstance(sdt_raw, str):
        sdt = [sdt_raw]
    else:
        sdt = []

    yang_path_raw = extensions.get(_TYPED_EXT_YANG_PATH) or ""
    yang_path = yang_path_raw if isinstance(yang_path_raw, str) else ""

    enum_raw = resolved.get("enum") or []
    enum_vals = [str(v) for v in enum_raw if isinstance(v, (str, int, float, bool))]

    prop_type = str(resolved.get("type") or "")
    prop_format = str(resolved.get("format") or "")
    description = str(resolved.get("description") or prop_body.get("description") or "")

    # readOnly: prefer call-site over resolved target; default False.
    read_only_raw = prop_body.get("readOnly")
    if read_only_raw is None:
        read_only_raw = resolved.get("readOnly")
    read_only = bool(read_only_raw) if read_only_raw is not None else False

    extensions_json = json.dumps(extensions, sort_keys=True) if extensions else ""

    property_id = f"{parent_component_id}#prop:{prop_name}"
    if inherited_from:
        property_id = f"{property_id}@{inherited_from}"

    sdt_lit = _str_list_literal(sdt)
    enum_lit = _str_list_literal(enum_vals)

    conn.execute(
        "MERGE (p:Property {property_id: $pid}) SET "
        "p.parent_component_id = $parent, p.name = $pname, "
        "p.type = $pty, p.format = $pfmt, p.required = $preq, "
        f"p.enumValues = {enum_lit}, "
        "p.description = $pdesc, "
        f"p.supportedDeviceTypes = {sdt_lit}, "
        "p.yangPath = $yp, "
        f"p.extensionsJson = '{_esc(extensions_json)}', "
        "p.inheritedFrom = $inh, "
        "p.readOnly = $pro",
        parameters={
            "pid": property_id,
            "parent": parent_component_id,
            "pname": prop_name,
            "pty": prop_type,
            "pfmt": prop_format,
            "preq": bool(required),
            "pdesc": description,
            "yp": yang_path,
            "inh": inherited_from,
            "pro": read_only,
        },
    )
    conn.execute(
        "MATCH (c:SchemaComponent {component_id: $cid}), "
        "(p:Property {property_id: $pid}) "
        "MERGE (c)-[:HAS_PROPERTY]->(p)",
        parameters={"cid": parent_component_id, "pid": property_id},
    )
    stats["properties"] = stats.get("properties", 0) + 1

    # Optional PROPERTY_OF_TYPE edge when the property points at a
    # named component (top-level $ref or array of $ref).
    target_ref: str | None = None
    if isinstance(prop_body.get("$ref"), str):
        target_ref = prop_body["$ref"]
    else:
        items = prop_body.get("items")
        if isinstance(items, dict) and isinstance(items.get("$ref"), str):
            target_ref = items["$ref"]
    if target_ref and target_ref.startswith("#/components/"):
        target_id = _ensure_component_node(
            conn, spec_source, target_ref, full_components, written, stats
        )
        if target_id:
            conn.execute(
                "MATCH (p:Property {property_id: $pid}), "
                "(c:SchemaComponent {component_id: $cid}) "
                "MERGE (p)-[:PROPERTY_OF_TYPE]->(c)",
                parameters={"pid": property_id, "cid": target_id},
            )
