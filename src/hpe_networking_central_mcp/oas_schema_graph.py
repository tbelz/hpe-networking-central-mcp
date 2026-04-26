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

import base64
import hashlib
import json
from typing import Any

import pyarrow as pa

from .oas_normalize import (
    _extract_referenced_components,
    _follow_ref,
    _strip_skeleton_keys,
)


# ── Legacy JSON-blob decoder ────────────────────────────────────────
#
# Earlier builds tagged JSON-string columns with a ``b64:`` prefix to
# work around real_ladybug 0.15.x prepared-statement type-inference
# bugs. The current writer uses literal-embedded UNWIND (no
# parameters), so plain JSON is now stored directly. The decoder is
# kept tolerant so DBs built with the old encoder still read cleanly.
_JSON_BLOB_PREFIX = "b64:"


def decode_json_blob(stored: str) -> str:
    """Return the JSON text for ``stored``; tolerates legacy b64 blobs."""
    if not stored:
        return ""
    if stored.startswith(_JSON_BLOB_PREFIX):
        return base64.b64decode(stored[len(_JSON_BLOB_PREFIX):]).decode("utf-8")
    return stored


# ── Cypher literal escaping (real_ladybug parameter-binding bug) ────
#
# Earlier builds rendered every value as a Cypher literal to bypass
# real_ladybug 0.15.x prepared-statement bugs (auto-inferred MAP from
# JSON strings, ANY[] vs STRING[] inference variance across rows, and
# an ``unordered_map::at`` crash on
# ``UNWIND $rows AS r MATCH ... MERGE rel``). The current writer uses
# ``COPY ... FROM $df`` with PyArrow tables (the documented bulk-load
# path), which avoids prepared statements AND MERGE entirely — Ladybug
# upstream issue #285 is "WONTFIX" for bulk MERGE.
#
# Nothing in this module emits MERGE statements anymore; primary-key
# deduplication happens in Python before COPY runs.


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
    if "csv" in name or name.endswith("_list") or name.endswith("ids"):
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


# ── Vendor-extension columns promoted on Property nodes ────────────


_TYPED_EXT_DEVICE_TYPE = "x-supportedDeviceType"
_TYPED_EXT_YANG_PATH = "x-path"


def _collect_x_extensions(prop_body: dict) -> dict:
    """Return every ``x-*`` key from ``prop_body`` as a plain dict."""
    return {k: v for k, v in prop_body.items() if isinstance(k, str) and k.startswith("x-")}


def _resolve_property_schema(prop_body: Any, full_components: dict) -> dict:
    """Resolve a property's schema body, following one level of $ref."""
    if not isinstance(prop_body, dict):
        return {}
    ref = prop_body.get("$ref")
    if isinstance(ref, str):
        target = _follow_ref(ref, full_components)
        if isinstance(target, dict):
            return target
    return prop_body


# ── Per-spec write batch (UNWIND-flushed) ───────────────────────────


class _Batch:
    """Buffers schema-subgraph writes for a single spec.

    All node MERGEs and edge MERGEs are accumulated in plain Python
    lists and flushed in O(few-dozen) ``UNWIND $rows`` statements at
    ``flush()`` time. This replaces the previous ~5 statements per
    endpoint × ~2k endpoints × ~10 properties each pattern (which
    auto-committed tens of thousands of single-row transactions).

    Edge pair lists are deduped on insert via small ``set`` indexes so
    we never MATCH/MERGE the same edge twice.
    """

    __slots__ = (
        "params",
        "request_bodies",
        "responses",
        "components",
        "properties",
        "has_param",
        "has_request_body",
        "body_refs",
        "has_response",
        "response_refs",
        "has_property",
        "property_of_type",
        "composed_of",
        "references",
        "_seen_params",
        "_seen_request_bodies",
        "_seen_responses",
        "_seen_components",
        "_seen_properties",
        "_seen_has_param",
        "_seen_has_request_body",
        "_seen_body_refs",
        "_seen_has_response",
        "_seen_response_refs",
        "_seen_has_property",
        "_seen_property_of_type",
        "_seen_composed_of",
        "_seen_references",
    )

    def __init__(self) -> None:
        self.params: list[dict] = []
        self.request_bodies: list[dict] = []
        self.responses: list[dict] = []
        self.components: list[dict] = []
        self.properties: list[dict] = []
        self.has_param: list[dict] = []
        self.has_request_body: list[dict] = []
        self.body_refs: list[dict] = []
        self.has_response: list[dict] = []
        self.response_refs: list[dict] = []
        self.has_property: list[dict] = []
        self.property_of_type: list[dict] = []
        self.composed_of: list[dict] = []
        self.references: list[dict] = []
        self._seen_params: set[str] = set()
        self._seen_request_bodies: set[str] = set()
        self._seen_responses: set[str] = set()
        self._seen_components: set[str] = set()
        self._seen_properties: set[str] = set()
        self._seen_has_param: set[tuple[str, str]] = set()
        self._seen_has_request_body: set[tuple[str, str]] = set()
        self._seen_body_refs: set[tuple[str, str]] = set()
        self._seen_has_response: set[tuple[str, str]] = set()
        self._seen_response_refs: set[tuple[str, str]] = set()
        self._seen_has_property: set[tuple[str, str]] = set()
        self._seen_property_of_type: set[tuple[str, str]] = set()
        self._seen_composed_of: set[tuple[str, str, str]] = set()
        self._seen_references: set[tuple[str, str, str]] = set()

    # ── Node MERGE collectors ──────────────────────────────────

    def add_parameter(self, row: dict) -> bool:
        pid = row["parameter_id"]
        if pid in self._seen_params:
            return False
        self._seen_params.add(pid)
        self.params.append(row)
        return True

    def add_request_body(self, row: dict) -> bool:
        rid = row["request_body_id"]
        if rid in self._seen_request_bodies:
            return False
        self._seen_request_bodies.add(rid)
        self.request_bodies.append(row)
        return True

    def add_response(self, row: dict) -> bool:
        rid = row["response_id"]
        if rid in self._seen_responses:
            return False
        self._seen_responses.add(rid)
        self.responses.append(row)
        return True

    def add_component(self, row: dict) -> bool:
        """Returns True if newly added, False if already buffered."""
        cid = row["component_id"]
        if cid in self._seen_components:
            return False
        self._seen_components.add(cid)
        self.components.append(row)
        return True

    def add_property(self, row: dict) -> bool:
        pid = row["property_id"]
        if pid in self._seen_properties:
            return False
        self._seen_properties.add(pid)
        self.properties.append(row)
        return True

    # ── Edge MERGE collectors ──────────────────────────────────

    def add_has_param(self, eid: str, pid: str) -> bool:
        key = (eid, pid)
        if key in self._seen_has_param:
            return False
        self._seen_has_param.add(key)
        self.has_param.append({"a": eid, "b": pid})
        return True

    def add_has_request_body(self, eid: str, rid: str) -> bool:
        key = (eid, rid)
        if key in self._seen_has_request_body:
            return False
        self._seen_has_request_body.add(key)
        self.has_request_body.append({"a": eid, "b": rid})
        return True

    def add_body_ref(self, rid: str, cid: str) -> bool:
        key = (rid, cid)
        if key in self._seen_body_refs:
            return False
        self._seen_body_refs.add(key)
        self.body_refs.append({"a": rid, "b": cid})
        return True

    def add_has_response(self, eid: str, rid: str) -> bool:
        key = (eid, rid)
        if key in self._seen_has_response:
            return False
        self._seen_has_response.add(key)
        self.has_response.append({"a": eid, "b": rid})
        return True

    def add_response_ref(self, rid: str, cid: str) -> bool:
        key = (rid, cid)
        if key in self._seen_response_refs:
            return False
        self._seen_response_refs.add(key)
        self.response_refs.append({"a": rid, "b": cid})
        return True

    def add_has_property(self, cid: str, pid: str) -> bool:
        key = (cid, pid)
        if key in self._seen_has_property:
            return False
        self._seen_has_property.add(key)
        self.has_property.append({"a": cid, "b": pid})
        return True

    def add_property_of_type(self, pid: str, cid: str) -> bool:
        key = (pid, cid)
        if key in self._seen_property_of_type:
            return False
        self._seen_property_of_type.add(key)
        self.property_of_type.append({"a": pid, "b": cid})
        return True

    def add_composed_of(self, parent_cid: str, child_cid: str, kind: str) -> bool:
        key = (parent_cid, child_cid, kind)
        if key in self._seen_composed_of:
            return False
        self._seen_composed_of.add(key)
        self.composed_of.append({"a": parent_cid, "b": child_cid, "kind": kind})
        return True

    def add_reference(self, parent_cid: str, child_cid: str, via: str) -> bool:
        key = (parent_cid, child_cid, via)
        if key in self._seen_references:
            return False
        self._seen_references.add(key)
        self.references.append({"a": parent_cid, "b": child_cid, "via": via})
        return True

    # ── Flush ──────────────────────────────────────────────────

    def flush(self, conn) -> None:
        """Bulk-load every collector via ``COPY ... FROM $df`` with
        in-memory PyArrow tables.

        Avoids both the real_ladybug 0.15.x prepared-statement bugs and
        the documented MERGE-via-UNWIND anti-pattern (LadybugDB issue
        #285, WONTFIX). Primary-key deduplication has already been
        done in Python by the per-batch ``_seen_*`` sets, so each
        ``COPY`` runs with ``ignore_errors=true`` only as belt-and-
        braces against re-running the helper twice on the same DB
        (idempotent for tests).

        ``COPY`` requires nodes to exist before any rel rows that
        reference them, so node tables are loaded first in dependency
        order.
        """
        # ── Node tables (in dependency order) ──
        _copy_node_table(
            conn,
            "Parameter",
            self.params,
            schema=_PARAM_SCHEMA,
        )
        _copy_node_table(
            conn,
            "RequestBody",
            self.request_bodies,
            schema=_REQUEST_BODY_SCHEMA,
        )
        _copy_node_table(
            conn,
            "Response",
            self.responses,
            schema=_RESPONSE_SCHEMA,
        )
        _copy_node_table(
            conn,
            "SchemaComponent",
            self.components,
            schema=_COMPONENT_SCHEMA,
        )
        _copy_node_table(
            conn,
            "Property",
            self.properties,
            schema=_PROPERTY_SCHEMA,
        )

        # ── Rel tables (require both endpoints to be loaded) ──
        _copy_rel_table(conn, "HAS_PARAMETER", self.has_param, _REL_AB_SCHEMA)
        _copy_rel_table(conn, "HAS_REQUEST_BODY", self.has_request_body, _REL_AB_SCHEMA)
        _copy_rel_table(conn, "BODY_REFERENCES", self.body_refs, _REL_AB_SCHEMA)
        _copy_rel_table(conn, "HAS_RESPONSE", self.has_response, _REL_AB_SCHEMA)
        _copy_rel_table(conn, "RESPONSE_REFERENCES", self.response_refs, _REL_AB_SCHEMA)
        _copy_rel_table(conn, "HAS_PROPERTY", self.has_property, _REL_AB_SCHEMA)
        _copy_rel_table(conn, "PROPERTY_OF_TYPE", self.property_of_type, _REL_AB_SCHEMA)
        _copy_rel_table(
            conn, "COMPOSED_OF", self.composed_of, _REL_COMPOSED_OF_SCHEMA
        )
        _copy_rel_table(
            conn, "REFERENCES", self.references, _REL_REFERENCES_SCHEMA
        )


# ── PyArrow schemas (column order MUST match graph/schema.py DDL) ───

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
    ("required", pa.list_(pa.string())),
    ("enumValues", pa.list_(pa.string())),
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
    ("inheritedFrom", pa.string()),
    ("readOnly", pa.bool_()),
])

# Rel schemas: first two columns are FROM/TO PKs; extra cols follow.
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


def _rows_to_pa(rows: list[dict], schema: pa.Schema) -> pa.Table:
    """Materialise a list[dict] as a PyArrow table conforming to ``schema``.

    Missing keys default to ``None`` (or ``""`` / ``[]`` for non-null
    string/list columns). Column order follows ``schema``.
    """
    cols: dict[str, list] = {f.name: [] for f in schema}
    for r in rows:
        for f in schema:
            v = r.get(f.name)
            if v is None:
                # Sensible defaults so Ladybug doesn't reject NULLs on
                # non-nullable columns.
                if pa.types.is_string(f.type):
                    v = ""
                elif pa.types.is_list(f.type):
                    v = []
                elif pa.types.is_boolean(f.type):
                    v = False
            cols[f.name].append(v)
    return pa.table(cols, schema=schema)


def _copy_node_table(
    conn,
    table: str,
    rows: list[dict],
    *,
    schema: pa.Schema,
) -> None:
    """Bulk-load a node table from buffered rows."""
    if not rows:
        return
    pa_table = _rows_to_pa(rows, schema)
    conn.execute(
        f"COPY {table} FROM $df (ignore_errors=true)",
        parameters={"df": pa_table},
    )


def _copy_rel_table(
    conn,
    table: str,
    rows: list[dict],
    schema: pa.Schema,
) -> None:
    """Bulk-load a rel table from buffered rows.

    Schema must list FROM column first, TO column second, then any
    edge properties.
    """
    if not rows:
        return
    pa_table = _rows_to_pa(rows, schema)
    conn.execute(
        f"COPY {table} FROM $df (ignore_errors=true)",
        parameters={"df": pa_table},
    )


def _empty_stats() -> dict:
    return {
        "endpoints": 0,
        "parameters": 0,
        "request_bodies": 0,
        "responses": 0,
        "components": 0,
        "properties": 0,
        "references": 0,
    }


def _query_existing_eids(conn, requested_eids: list[str]) -> set[str]:
    """One-shot lookup of which ApiEndpoint IDs already exist."""
    if not requested_eids:
        return set()
    rows = conn.execute(
        "UNWIND $eids AS eid MATCH (e:ApiEndpoint {endpoint_id: eid}) "
        "RETURN e.endpoint_id AS eid",
        parameters={"eids": requested_eids},
    ).rows_as_dict()
    return {r["eid"] for r in rows}


def _collect_spec_into_batch(
    batch: "_Batch",
    *,
    spec_source: str,
    spec: dict,
    endpoints: list[tuple[str, str]],
    existing_eids: set[str],
    stats: dict,
    emit_property_subgraph: bool = True,
) -> None:
    """Pure-Python collection: walk one spec, append rows to ``batch``.

    No DB I/O. The caller is responsible for flushing the batch via
    ``batch.flush(conn)`` (or ``flush_batch`` below) after all
    interesting specs have been collected.
    """
    components = spec.get("components") or {}

    for method, path in endpoints:
        method_u = method.upper()
        op = _find_operation(spec, method_u, path)
        if op is None:
            continue
        eid = f"{method_u}:{path}"
        if eid not in existing_eids:
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
            enum_vals = [
                v for v in (schema.get("enum") or []) if isinstance(v, str)
            ]
            batch.add_parameter({
                "parameter_id": param_id,
                "endpoint_id": eid,
                "name": pname,
                "location": ploc,
                "required": bool(resolved.get("required", False)),
                "type": str(schema.get("type") or ""),
                "format": str(schema.get("format") or ""),
                "enumValues": enum_vals,
                "pattern": str(schema.get("pattern") or ""),
                "inferredHint": _infer_param_hint(resolved),
                "description": str(resolved.get("description") or ""),
            })
            batch.add_has_param(eid, param_id)
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
            batch.add_request_body({
                "request_body_id": rb_id,
                "endpoint_id": eid,
                "content_type": media,
                "required": bool(rb_resolved.get("required", False)),
                "root_component_ref": root_ref,
            })
            batch.add_has_request_body(eid, rb_id)
            stats["request_bodies"] += 1
            if root_ref:
                comp_id = _ensure_component_node(
                    batch, spec_source, root_ref, components, stats,
                    emit_property_subgraph=emit_property_subgraph,
                )
                if comp_id:
                    batch.add_body_ref(rb_id, comp_id)

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
            batch.add_response({
                "response_id": resp_id,
                "endpoint_id": eid,
                "status": str(status),
                "content_type": media,
                "root_component_ref": root_ref,
            })
            batch.add_has_response(eid, resp_id)
            stats["responses"] += 1
            if root_ref:
                comp_id = _ensure_component_node(
                    batch, spec_source, root_ref, components, stats,
                    emit_property_subgraph=emit_property_subgraph,
                )
                if comp_id:
                    batch.add_response_ref(resp_id, comp_id)

        # ── REFERENCES walker ──
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
                    batch, spec_source, ref, components, stats,
                    emit_property_subgraph=emit_property_subgraph,
                )
                if not comp_id:
                    continue
                for child_ref, via in _walk_refs_with_site(body):
                    if not child_ref.startswith("#/components/"):
                        continue
                    child_id = _ensure_component_node(
                        batch, spec_source, child_ref, components, stats,
                        emit_property_subgraph=emit_property_subgraph,
                    )
                    if not child_id or child_id == comp_id:
                        continue
                    if batch.add_reference(comp_id, child_id, via):
                        stats["references"] += 1


# ── Public API ──────────────────────────────────────────────────────


# Public alias so callers (build script, tests) can import a typed Batch.
SchemaGraphBatch = _Batch


def new_batch() -> "_Batch":
    """Return a fresh schema-graph batch for use with ``collect_into_batch``."""
    return _Batch()


def collect_into_batch(
    batch: "_Batch",
    *,
    spec_source: str,
    spec: dict,
    endpoints: list[tuple[str, str]],
    existing_eids: set[str],
    emit_property_subgraph: bool = True,
) -> dict:
    """Collect schema-subgraph rows for ONE spec into ``batch`` (no DB I/O).

    ``existing_eids`` must be a pre-computed set of ApiEndpoint IDs
    that already exist in the DB. The caller filters with
    ``query_existing_eids(conn, [...])`` before the loop so the build
    script doesn't issue one MATCH query per spec.

    Returns per-spec stats; the build script logs these to provide ETA.
    """
    stats = _empty_stats()
    _collect_spec_into_batch(
        batch,
        spec_source=spec_source,
        spec=spec,
        endpoints=endpoints,
        existing_eids=existing_eids,
        stats=stats,
        emit_property_subgraph=emit_property_subgraph,
    )
    return stats


def query_existing_eids(conn, eids: list[str]) -> set[str]:
    """Public helper: which of ``eids`` exist in the ApiEndpoint table."""
    return _query_existing_eids(conn, eids)


def _preseed_batch_from_db(conn, batch: "_Batch") -> None:
    """Populate ``batch._seen_*`` sets from existing DB rows so a
    re-run of ``populate_schema_graph`` becomes idempotent.

    Only used by the per-spec wrapper. The build path runs against an
    empty DB and skips this entirely.
    """
    # Nodes
    for r in conn.execute("MATCH (p:Parameter) RETURN p.parameter_id AS k").rows_as_dict():
        batch._seen_params.add(r["k"])
    for r in conn.execute("MATCH (rb:RequestBody) RETURN rb.request_body_id AS k").rows_as_dict():
        batch._seen_request_bodies.add(r["k"])
    for r in conn.execute("MATCH (resp:Response) RETURN resp.response_id AS k").rows_as_dict():
        batch._seen_responses.add(r["k"])
    for r in conn.execute("MATCH (c:SchemaComponent) RETURN c.component_id AS k").rows_as_dict():
        batch._seen_components.add(r["k"])
    for r in conn.execute("MATCH (p:Property) RETURN p.property_id AS k").rows_as_dict():
        batch._seen_properties.add(r["k"])
    # Rels
    for r in conn.execute(
        "MATCH (e:ApiEndpoint)-[:HAS_PARAMETER]->(p:Parameter) "
        "RETURN e.endpoint_id AS a, p.parameter_id AS b"
    ).rows_as_dict():
        batch._seen_has_param.add((r["a"], r["b"]))
    for r in conn.execute(
        "MATCH (e:ApiEndpoint)-[:HAS_REQUEST_BODY]->(rb:RequestBody) "
        "RETURN e.endpoint_id AS a, rb.request_body_id AS b"
    ).rows_as_dict():
        batch._seen_has_request_body.add((r["a"], r["b"]))
    for r in conn.execute(
        "MATCH (rb:RequestBody)-[:BODY_REFERENCES]->(c:SchemaComponent) "
        "RETURN rb.request_body_id AS a, c.component_id AS b"
    ).rows_as_dict():
        batch._seen_body_refs.add((r["a"], r["b"]))
    for r in conn.execute(
        "MATCH (e:ApiEndpoint)-[:HAS_RESPONSE]->(resp:Response) "
        "RETURN e.endpoint_id AS a, resp.response_id AS b"
    ).rows_as_dict():
        batch._seen_has_response.add((r["a"], r["b"]))
    for r in conn.execute(
        "MATCH (resp:Response)-[:RESPONSE_REFERENCES]->(c:SchemaComponent) "
        "RETURN resp.response_id AS a, c.component_id AS b"
    ).rows_as_dict():
        batch._seen_response_refs.add((r["a"], r["b"]))
    for r in conn.execute(
        "MATCH (c:SchemaComponent)-[:HAS_PROPERTY]->(p:Property) "
        "RETURN c.component_id AS a, p.property_id AS b"
    ).rows_as_dict():
        batch._seen_has_property.add((r["a"], r["b"]))
    for r in conn.execute(
        "MATCH (p:Property)-[:PROPERTY_OF_TYPE]->(c:SchemaComponent) "
        "RETURN p.property_id AS a, c.component_id AS b"
    ).rows_as_dict():
        batch._seen_property_of_type.add((r["a"], r["b"]))
    for r in conn.execute(
        "MATCH (a:SchemaComponent)-[r:COMPOSED_OF]->(b:SchemaComponent) "
        "RETURN a.component_id AS a, b.component_id AS b, r.kind AS kind"
    ).rows_as_dict():
        batch._seen_composed_of.add((r["a"], r["b"], r["kind"]))
    for r in conn.execute(
        "MATCH (a:SchemaComponent)-[r:REFERENCES]->(b:SchemaComponent) "
        "RETURN a.component_id AS a, b.component_id AS b, r.via AS via"
    ).rows_as_dict():
        batch._seen_references.add((r["a"], r["b"], r["via"]))


def flush_batch(conn, batch: "_Batch") -> None:
    """Flush an accumulated batch to the DB via COPY FROM PyArrow."""
    batch.flush(conn)


def populate_schema_graph(
    conn,
    *,
    spec_source: str,
    spec: dict,
    endpoints: list[tuple[str, str]],
    emit_property_subgraph: bool = True,
) -> dict:
    """Decompose ``spec`` into schema-subgraph rows for ``endpoints``
    and bulk-load them via ``COPY ... FROM $df``.

    Convenience wrapper that creates a batch, looks up existing
    endpoint IDs, collects rows, and flushes — useful for tests and
    one-off populations. The build script uses ``collect_into_batch`` +
    ``flush_batch`` so all 1000+ specs share a single global batch and
    one COPY per table.

    Returns a stats dict with per-table counts. Endpoints whose
    ``ApiEndpoint`` row is missing are silently skipped.
    """
    stats = _empty_stats()
    requested_eids = [f"{m.upper()}:{p}" for m, p in endpoints]
    if not requested_eids:
        return stats
    existing_eids = _query_existing_eids(conn, requested_eids)
    batch = _Batch()
    _preseed_batch_from_db(conn, batch)
    _collect_spec_into_batch(
        batch,
        spec_source=spec_source,
        spec=spec,
        endpoints=endpoints,
        existing_eids=existing_eids,
        stats=stats,
        emit_property_subgraph=emit_property_subgraph,
    )
    batch.flush(conn)
    return stats



# ── Component MERGE (buffered) ──────────────────────────────────────


def _ensure_component_node(
    batch: _Batch,
    spec_source: str,
    ref: str,
    full_components: dict,
    stats: dict,
    emit_property_subgraph: bool = True,
) -> str:
    """Buffer the SchemaComponent row + its property subgraph.

    Returns the component_id (deterministic) so callers can wire up
    edges. Returns "" if the ref cannot be resolved.
    """
    if not ref.startswith("#/components/"):
        return ""
    section = _section_for_ref(ref)
    name = _name_for_ref(ref)
    if not section or not name:
        return ""
    comp_id = _named_component_id(spec_source, section, name)
    if comp_id in batch._seen_components:
        return comp_id

    body = _follow_ref(ref, full_components)
    if not isinstance(body, dict):
        return ""
    stripped = _strip_skeleton_keys(body)
    body_json = json.dumps(stripped)
    enum_vals = [v for v in (body.get("enum") or []) if isinstance(v, str)]
    _req = body.get("required")
    required = [v for v in (_req if isinstance(_req, list) else []) if isinstance(v, str)]

    batch.add_component({
        "component_id": comp_id,
        "spec_source": spec_source,
        "section": section,
        "name": name,
        "type": str(body.get("type") or ""),
        "kind": _component_kind(body),
        "required": required,
        "enumValues": enum_vals,
        "bodyJson": body_json,
    })
    stats["components"] += 1

    if emit_property_subgraph:
        _emit_property_subgraph(
            batch,
            spec_source=spec_source,
            parent_component_id=comp_id,
            parent_component_name=name,
            body=body,
            full_components=full_components,
            stats=stats,
        )

    return comp_id


# ── Property-level extraction (buffered) ────────────────────────────


def _emit_property_subgraph(
    batch: _Batch,
    *,
    spec_source: str,
    parent_component_id: str,
    parent_component_name: str,
    body: dict,
    full_components: dict,
    stats: dict,
) -> None:
    """Emit HAS_PROPERTY / COMPOSED_OF / PROPERTY_OF_TYPE rows."""
    _req = body.get("required")
    own_required = set(
        v for v in (_req if isinstance(_req, list) else []) if isinstance(v, str)
    )
    own_props = body.get("properties") or {}
    if isinstance(own_props, dict):
        for prop_name, prop_body in own_props.items():
            _emit_one_property(
                batch,
                spec_source=spec_source,
                parent_component_id=parent_component_id,
                prop_name=prop_name,
                prop_body=prop_body if isinstance(prop_body, dict) else {},
                required=prop_name in own_required,
                inherited_from="",
                full_components=full_components,
                stats=stats,
            )

    for kind in ("allOf", "oneOf", "anyOf"):
        branches = body.get(kind)
        if not isinstance(branches, list):
            continue
        for branch in branches:
            if not isinstance(branch, dict):
                continue
            ref = branch.get("$ref")
            if isinstance(ref, str):
                target_id = _ensure_component_node(
                    batch, spec_source, ref, full_components, stats
                )
                if target_id:
                    batch.add_composed_of(parent_component_id, target_id, kind)
                if kind == "allOf":
                    target_body = _follow_ref(ref, full_components)
                    if isinstance(target_body, dict):
                        branch_name = _name_for_ref(ref)
                        _flatten_allof_properties(
                            batch,
                            spec_source=spec_source,
                            parent_component_id=parent_component_id,
                            branch_body=target_body,
                            inherited_from=branch_name,
                            full_components=full_components,
                            stats=stats,
                            _visited=frozenset({ref}),
                        )
            else:
                if kind == "allOf":
                    _flatten_allof_properties(
                        batch,
                        spec_source=spec_source,
                        parent_component_id=parent_component_id,
                        branch_body=branch,
                        inherited_from="",
                        full_components=full_components,
                        stats=stats,
                        _visited=frozenset(),
                    )


def _flatten_allof_properties(
    batch: _Batch,
    *,
    spec_source: str,
    parent_component_id: str,
    branch_body: dict,
    inherited_from: str,
    full_components: dict,
    stats: dict,
    _visited: frozenset[str] = frozenset(),
) -> None:
    _req = branch_body.get("required")
    branch_required = set(
        v for v in (_req if isinstance(_req, list) else []) if isinstance(v, str)
    )
    props = branch_body.get("properties") or {}
    if isinstance(props, dict):
        for prop_name, prop_body in props.items():
            _emit_one_property(
                batch,
                spec_source=spec_source,
                parent_component_id=parent_component_id,
                prop_name=prop_name,
                prop_body=prop_body if isinstance(prop_body, dict) else {},
                required=prop_name in branch_required,
                inherited_from=inherited_from,
                full_components=full_components,
                stats=stats,
            )

    nested = branch_body.get("allOf")
    if isinstance(nested, list):
        for sub in nested:
            if not isinstance(sub, dict):
                continue
            ref = sub.get("$ref")
            if isinstance(ref, str):
                if ref in _visited:
                    continue
                target_body = _follow_ref(ref, full_components)
                if isinstance(target_body, dict):
                    nested_name = _name_for_ref(ref) or inherited_from
                    _flatten_allof_properties(
                        batch,
                        spec_source=spec_source,
                        parent_component_id=parent_component_id,
                        branch_body=target_body,
                        inherited_from=nested_name,
                        full_components=full_components,
                        stats=stats,
                        _visited=_visited | {ref},
                    )
            else:
                _flatten_allof_properties(
                    batch,
                    spec_source=spec_source,
                    parent_component_id=parent_component_id,
                    branch_body=sub,
                    inherited_from=inherited_from,
                    full_components=full_components,
                    stats=stats,
                    _visited=_visited,
                )


def _emit_one_property(
    batch: _Batch,
    *,
    spec_source: str,
    parent_component_id: str,
    prop_name: str,
    prop_body: dict,
    required: bool,
    inherited_from: str,
    full_components: dict,
    stats: dict,
) -> None:
    resolved = _resolve_property_schema(prop_body, full_components)
    extensions = _collect_x_extensions(prop_body)
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

    read_only_raw = prop_body.get("readOnly")
    if read_only_raw is None:
        read_only_raw = resolved.get("readOnly")
    read_only = bool(read_only_raw) if read_only_raw is not None else False

    extensions_json = json.dumps(extensions, sort_keys=True) if extensions else ""

    property_id = f"{parent_component_id}#prop:{prop_name}"
    if inherited_from:
        property_id = f"{property_id}@{inherited_from}"

    if batch.add_property({
        "property_id": property_id,
        "parent_component_id": parent_component_id,
        "name": prop_name,
        "type": prop_type,
        "format": prop_format,
        "required": bool(required),
        "enumValues": enum_vals,
        "description": description,
        "supportedDeviceTypes": sdt,
        "yangPath": yang_path,
        "extensionsJson": extensions_json,
        "inheritedFrom": inherited_from,
        "readOnly": read_only,
    }):
        stats["properties"] = stats.get("properties", 0) + 1
    batch.add_has_property(parent_component_id, property_id)

    target_ref: str | None = None
    if isinstance(prop_body.get("$ref"), str):
        target_ref = prop_body["$ref"]
    else:
        items = prop_body.get("items")
        if isinstance(items, dict) and isinstance(items.get("$ref"), str):
            target_ref = items["$ref"]
    if target_ref and target_ref.startswith("#/components/"):
        target_id = _ensure_component_node(
            batch, spec_source, target_ref, full_components, stats
        )
        if target_id:
            batch.add_property_of_type(property_id, target_id)
