"""Build-time helper that decomposes a normalised OAS spec into the
schema subgraph defined by ADR 009.

Given a normalised spec and a list of ``(method, path)`` endpoints, this
populates the following node + relationship tables (created by
``graph/schema.py``):

  Parameter, RequestBody, Response, SchemaComponent, ApiEndpointSkeleton
  HAS_PARAMETER, HAS_REQUEST_BODY, HAS_RESPONSE,
  BODY_REFERENCES, RESPONSE_REFERENCES, REFERENCES, HAS_SKELETON

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
    project_glossary,
    project_skeleton,
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
    "responses": N, "components": N, "skeletons": N}`` for the build
    log.  The helper assumes the underlying ``ApiEndpoint`` row exists;
    endpoints without a row are silently skipped.
    """
    components = spec.get("components") or {}
    stats = {
        "parameters": 0,
        "request_bodies": 0,
        "responses": 0,
        "components": 0,
        "skeletons": 0,
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
                "p.pattern = $pat, p.inferredHint = $ph",
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

        # ── Skeleton + glossary blob node ──
        try:
            skel = project_skeleton(spec, method_u, path) or {}
        except Exception:
            skel = {}
        try:
            gloss = project_glossary(spec, method_u, path) or {}
        except Exception:
            gloss = {}
        skeleton_json = json.dumps(skel) if skel else ""
        glossary_json = json.dumps(gloss) if gloss else ""
        conn.execute(
            "MERGE (s:ApiEndpointSkeleton {endpoint_id: $eid}) SET "
            f"s.bodySkeletonJson = '{_esc(skeleton_json)}', "
            f"s.bodyGlossaryJson = '{_esc(glossary_json)}'",
            parameters={"eid": eid},
        )
        conn.execute(
            "MATCH (e:ApiEndpoint {endpoint_id: $eid}), "
            "(s:ApiEndpointSkeleton {endpoint_id: $eid}) "
            "MERGE (e)-[:HAS_SKELETON]->(s)",
            parameters={"eid": eid},
        )
        stats["skeletons"] += 1

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
    required_lit = _str_list_literal(
        v for v in (body.get("required") or []) if isinstance(v, str)
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
    return comp_id
