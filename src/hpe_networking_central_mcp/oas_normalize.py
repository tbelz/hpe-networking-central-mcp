"""OpenAPI spec normalization + view projections.

The raw OpenAPI specs published by Aruba Central's documentation portal
contain massive amounts of repetition.  A handful of error responses
(400/401/403/404/500) and a small number of nested object shapes
(``rule``, ``rule_action``, ``trunk_group_member``, …) are inlined into
every operation, blowing up endpoints like ``sw-port-profiles`` to 354 KB
of JSON.

This module provides two responsibilities:

1. **``normalize(spec)``** — pure, idempotent transforms that promote
   repeated inline schemas to ``components/{schemas,responses}`` and
   rewrite their use sites to ``$ref``.  The resulting spec is still a
   valid OpenAPI 3.x document.

2. **Projection functions** — ``project_compact``, ``project_request_only``,
   ``project_full``, and ``project_raw`` — which take a normalized spec
   plus a ``(method, path)`` and return a self-contained dict suitable
   for serving via the MCP ``get_api_endpoint_detail`` tool.  Each
   projection bundles only the components it references in a
   ``$components`` side-table so consumers do not need access to the
   whole spec.
"""

from __future__ import annotations

import copy
import hashlib
import json
import re
from typing import Any

# Size budgets the projections target.  Used by tests; not enforced at runtime.
COMPACT_BUDGET_BYTES = 15 * 1024
REQUEST_ONLY_BUDGET_BYTES = 5 * 1024

# Heuristics
_MIN_DEDUP_OCCURRENCES = 2          # inline shape must appear ≥ this many times
_MIN_DEDUP_PROPERTIES = 3            # only objects with ≥ this many properties
_MAX_DESCRIPTION_REPEAT = 200        # x-typeDescription stripped if ≤ this and == description
_MAX_PATTERN_SOURCES_LEN = 4         # x-patternSources lists longer than this are dropped
_DEFAULT_MAX_LENGTH_NOISE = {9999}   # maxLength values treated as noise defaults

_MUTEX_RE = re.compile(
    r"either\s+`?(?P<a>[A-Za-z0-9_\-]+)`?\s+or\s+`?(?P<b>[A-Za-z0-9_\-]+)`?",
    re.IGNORECASE,
)


# ---------------------------------------------------------------------------
# Public: normalize
# ---------------------------------------------------------------------------


def normalize(spec: dict) -> dict:
    """Return an idempotent, dedup'd copy of ``spec``.

    Order matters: noise-stripping runs first so dedup hashes are stable
    across cosmetically-different copies.  Mutex hint emission is last
    because it inspects the cleaned descriptions.
    """
    if not isinstance(spec, dict):
        return spec

    out = copy.deepcopy(spec)
    out.setdefault("components", {})
    out["components"].setdefault("schemas", {})
    out["components"].setdefault("responses", {})

    _strip_noise(out)
    # Mutex hints are emitted before dedup so they annotate the inline
    # shapes that dedup will later promote into ``components/schemas``.
    # Running it after dedup would miss schemas that have already been
    # replaced by ``$ref`` at their use sites.
    _emit_mutex_hints(out)
    _dedup_error_responses(out)
    _dedup_nested_objects(out)

    return out


# ---------------------------------------------------------------------------
# Noise stripping
# ---------------------------------------------------------------------------


def _strip_noise(spec: dict) -> None:
    """Remove noisy metadata that bloats responses without aiding callers."""

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            # Drop x-typeDescription when it duplicates description verbatim.
            xtd = node.get("x-typeDescription")
            desc = node.get("description")
            if (
                isinstance(xtd, str)
                and isinstance(desc, str)
                and xtd.strip() == desc.strip()
                and len(xtd) <= _MAX_DESCRIPTION_REPEAT
            ):
                node.pop("x-typeDescription", None)

            # Trim oversized x-patternSources arrays (build-time YANG noise).
            xps = node.get("x-patternSources")
            if isinstance(xps, list) and len(xps) > _MAX_PATTERN_SOURCES_LEN:
                node.pop("x-patternSources", None)

            # Drop format == "const" — placeholder noise from upstream.
            if node.get("format") == "const":
                node.pop("format", None)

            # Drop placeholder maxLength defaults that signal "no real limit".
            ml = node.get("maxLength")
            if isinstance(ml, int) and ml in _DEFAULT_MAX_LENGTH_NOISE:
                node.pop("maxLength", None)

            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    _walk(spec)


# ---------------------------------------------------------------------------
# Error response deduplication
# ---------------------------------------------------------------------------

_ERROR_STATUSES = {"400", "401", "403", "404", "405", "409", "422", "429", "500", "502", "503", "504"}


def _dedup_error_responses(spec: dict) -> None:
    """Promote repeated inline error response objects to ``components/responses``."""
    components = spec["components"]["responses"]
    paths = spec.get("paths") or {}

    # First pass: collect all candidate inlined error responses.
    candidates: dict[str, list[tuple[dict, str]]] = {}
    # hash → list of (parent_responses_dict, status_key)
    for _path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete", "head", "options"}:
                continue
            if not isinstance(operation, dict):
                continue
            responses = operation.get("responses") or {}
            for status, resp in responses.items():
                if not isinstance(resp, dict):
                    continue
                if "$ref" in resp:
                    continue
                # Treat any 4xx/5xx (or wildcards) as candidates.
                norm_status = str(status)
                if not (
                    norm_status in _ERROR_STATUSES
                    or norm_status.upper().endswith("XX")
                    and norm_status[0] in {"4", "5"}
                ):
                    continue
                h = _hash_obj(resp)
                candidates.setdefault(h, []).append((responses, str(status)))

    # Second pass: promote each shape that appears ≥ threshold to components.
    for h, occurrences in candidates.items():
        if len(occurrences) < _MIN_DEDUP_OCCURRENCES:
            continue

        # Pick a stable name based on the most common status code.
        status_counts: dict[str, int] = {}
        for _, status in occurrences:
            status_counts[status] = status_counts.get(status, 0) + 1
        majority_status = max(status_counts.items(), key=lambda kv: kv[1])[0]

        component_name = _unique_name(
            f"Error{majority_status}_{h[:8]}",
            components,
        )

        # Snapshot the canonical shape from the first occurrence.
        first_responses_dict, first_status = occurrences[0]
        canonical = copy.deepcopy(first_responses_dict[first_status])
        components[component_name] = canonical

        ref = {"$ref": f"#/components/responses/{component_name}"}
        for responses_dict, status in occurrences:
            responses_dict[status] = dict(ref)


# ---------------------------------------------------------------------------
# Nested object schema deduplication
# ---------------------------------------------------------------------------


def _dedup_nested_objects(spec: dict) -> None:
    """Promote repeated inline object shapes to ``components/schemas``.

    Iterates to a fixed point so shapes that only become "exposed" after a
    parent object is promoted (and therefore appear ≥ threshold inside a
    handful of canonical components) still get deduplicated.
    """
    for _ in range(8):  # fixed-point loop with a hard cap as a safety net.
        if not _dedup_nested_objects_pass(spec):
            return


def _dedup_nested_objects_pass(spec: dict) -> bool:
    """Single dedup pass; returns True if anything was rewritten."""
    components = spec["components"]["schemas"]
    paths = spec.get("paths") or {}

    # Collect candidates: {hash: [(parent_dict, key), ...]}
    candidates: dict[str, list[tuple[Any, Any]]] = {}

    def _scan(node: Any, parent: Any = None, key: Any = None) -> None:
        if isinstance(node, dict):
            # Skip schemas that are already $refs.
            if "$ref" in node:
                return
            if _is_dedup_candidate(node):
                h = _hash_obj(node)
                if parent is not None:
                    candidates.setdefault(h, []).append((parent, key))
            # Recurse into children regardless — nested-in-nested may dedup too.
            for k, v in list(node.items()):
                _scan(v, node, k)
        elif isinstance(node, list):
            for i, item in enumerate(node):
                _scan(item, node, i)

    # Scan operation request/response/parameters AND already-promoted components
    # so shapes uncovered by parent promotion in a previous pass are visible.
    for _path, path_item in paths.items():
        if not isinstance(path_item, dict):
            continue
        for method, operation in path_item.items():
            if method.lower() not in {"get", "post", "put", "patch", "delete", "head", "options"}:
                continue
            if not isinstance(operation, dict):
                continue
            _scan(operation.get("requestBody"))
            _scan(operation.get("responses"))
            _scan(operation.get("parameters"))
    for _name, comp in list(components.items()):
        _scan(comp)

    # Promote each shape that appears ≥ threshold.
    rewrote = False
    for h, occurrences in candidates.items():
        if len(occurrences) < _MIN_DEDUP_OCCURRENCES:
            continue

        # Pick a name from the title/x-typeName if present, else from hash.
        first_parent, first_key = occurrences[0]
        first = first_parent[first_key]
        suggested = (
            first.get("title")
            or first.get("x-typeName")
            or "Shape"
        )
        suggested = re.sub(r"[^A-Za-z0-9_]", "", str(suggested)) or "Shape"
        component_name = _unique_name(f"{suggested}_{h[:8]}", components)

        components[component_name] = copy.deepcopy(first)
        ref = {"$ref": f"#/components/schemas/{component_name}"}
        for parent, key in occurrences:
            parent[key] = dict(ref)
        rewrote = True

    return rewrote


def _is_dedup_candidate(node: dict) -> bool:
    """True iff ``node`` looks like an inline object schema worth promoting."""
    if node.get("type") != "object":
        return False
    props = node.get("properties")
    if not isinstance(props, dict) or len(props) < _MIN_DEDUP_PROPERTIES:
        return False
    return True


# ---------------------------------------------------------------------------
# Mutual-exclusion hint emission
# ---------------------------------------------------------------------------


def _emit_mutex_hints(spec: dict) -> None:
    """Annotate schemas whose description says 'either X or Y' with a hint.

    Walks both ``paths`` (inline operation/schema bodies) and
    ``components`` (already-promoted shapes) so hints survive even when
    a later dedup pass moves the annotated schema into a component.
    """

    def _walk(node: Any) -> None:
        if isinstance(node, dict):
            desc = node.get("description")
            if isinstance(desc, str) and "either" in desc.lower():
                m = _MUTEX_RE.search(desc)
                if m:
                    a, b = m.group("a"), m.group("b")
                    props = node.get("properties")
                    if isinstance(props, dict) and a in props and b in props:
                        node.setdefault("x-mutually-exclusive", [a, b])
            for v in node.values():
                _walk(v)
        elif isinstance(node, list):
            for item in node:
                _walk(item)

    for _path, path_item in (spec.get("paths") or {}).items():
        _walk(path_item)
    components = spec.get("components") or {}
    for bucket_name in ("schemas", "responses"):
        bucket = components.get(bucket_name) or {}
        for _name, comp in bucket.items():
            _walk(comp)


# ---------------------------------------------------------------------------
# Hashing / naming helpers
# ---------------------------------------------------------------------------


def _hash_obj(obj: Any) -> str:
    return hashlib.sha1(
        json.dumps(obj, sort_keys=True, separators=(",", ":"), default=str).encode("utf-8")
    ).hexdigest()


def _unique_name(base: str, registry: dict) -> str:
    if base not in registry:
        return base
    for n in range(2, 1000):
        candidate = f"{base}_{n}"
        if candidate not in registry:
            return candidate
    raise RuntimeError(f"could not find unique name for {base!r}")


# ---------------------------------------------------------------------------
# Projection helpers
# ---------------------------------------------------------------------------


def _find_operation(spec: dict, method: str, path: str) -> dict | None:
    paths = spec.get("paths") or {}
    item = paths.get(path)
    if not isinstance(item, dict):
        return None
    op = item.get(method.lower())
    return op if isinstance(op, dict) else None


def _resolve_one(node: Any, components: dict, *, depth: int = 0, max_depth: int = 1) -> Any:
    """One-level $ref resolution helper for lightweight projections.

    Unlike ``oas_index._resolve_refs`` this does not recursively expand
    every nested ref — it inlines the immediate target only.
    """
    if depth > max_depth:
        return node
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str) and ref.startswith("#/components/"):
            target = _follow_ref(ref, components)
            if target is not None:
                return _resolve_one(target, components, depth=depth + 1, max_depth=max_depth)
            return node
        return {k: _resolve_one(v, components, depth=depth, max_depth=max_depth) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_one(i, components, depth=depth, max_depth=max_depth) for i in node]
    return node


def _follow_ref(ref: str, components: dict) -> Any | None:
    if not ref.startswith("#/components/"):
        return None
    parts = ref.removeprefix("#/components/").split("/")
    node: Any = components
    for part in parts:
        if isinstance(node, dict):
            node = node.get(part)
        else:
            return None
    return node


def _collect_refs(node: Any, refs: set[str]) -> None:
    """Collect every ``$ref`` string reachable from ``node`` (transitively)."""
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            refs.add(ref)
        for v in node.values():
            _collect_refs(v, refs)
    elif isinstance(node, list):
        for item in node:
            _collect_refs(item, refs)


def _extract_referenced_components(payload: Any, full_components: dict) -> dict:
    """Return a side-table containing only the components referenced by ``payload``.

    Walks transitively: a referenced schema may itself contain ``$ref`` to
    another schema; both are included.  Returns an empty dict if nothing is
    referenced.  Output mirrors OAS layout: ``{"schemas": {...}, "responses": {...}}``.
    """
    seen: set[str] = set()
    pending: set[str] = set()
    _collect_refs(payload, pending)

    out: dict[str, dict] = {}
    while pending:
        ref = pending.pop()
        if ref in seen:
            continue
        seen.add(ref)
        if not ref.startswith("#/components/"):
            continue
        parts = ref.removeprefix("#/components/").split("/")
        if len(parts) < 2:
            continue
        section, name = parts[0], parts[1]
        target = _follow_ref(ref, full_components)
        if target is None:
            continue
        out.setdefault(section, {})[name] = target
        # Walk into the resolved target — it may transitively reference more.
        new_refs: set[str] = set()
        _collect_refs(target, new_refs)
        pending |= (new_refs - seen)

    return out


def _operation_meta(operation: dict, method: str, path: str) -> dict:
    return {
        "method": method.upper(),
        "path": path,
        "summary": operation.get("summary", "") or "",
        "description": operation.get("description", "") or "",
        "operation_id": operation.get("operationId", "") or "",
        "tags": operation.get("tags", []) or [],
        "deprecated": bool(operation.get("deprecated", False)),
    }


# ---------------------------------------------------------------------------
# Projections
# ---------------------------------------------------------------------------


def project_compact(spec: dict, method: str, path: str) -> dict | None:
    """Return a compact, ref-preserving view of one endpoint.

    Parameter and response schemas are inlined exactly **one level** so
    obvious primitives are visible without a roundtrip; deeper structure
    stays as ``$ref`` and is included via ``$components``.
    """
    op = _find_operation(spec, method, path)
    if op is None:
        return None
    components = spec.get("components") or {}

    # Parameters — one-level inlined (params themselves rarely chain refs).
    params_out: list[dict] = []
    for p in op.get("parameters", []) or []:
        resolved = _resolve_one(p, components, max_depth=1)
        if not isinstance(resolved, dict):
            continue
        schema = resolved.get("schema") or {}
        params_out.append({
            "name": resolved.get("name", ""),
            "in": resolved.get("in", ""),
            "required": resolved.get("required", False),
            "description": resolved.get("description", ""),
            "schema": _resolve_one(schema, components, max_depth=1),
        })

    # Request body — keep top-level ref structure, inline only one level.
    rb_out = None
    rb = op.get("requestBody")
    if isinstance(rb, dict):
        rb_resolved = _resolve_one(rb, components, max_depth=1)
        content = (rb_resolved or {}).get("content") or {}
        media, schema = _pick_media_schema(content)
        if schema is not None:
            rb_out = {
                "content_type": media,
                "schema": schema,  # may contain $ref
                "required": rb_resolved.get("required", False),
            }

    # Responses — keep only success + first error class as $ref.
    responses_out: dict[str, Any] = {}
    success_status, success_resp = _pick_success_response(op.get("responses") or {})
    if success_resp is not None:
        success_resolved = _resolve_one(success_resp, components, max_depth=1)
        content = (success_resolved or {}).get("content") or {}
        _, schema = _pick_media_schema(content)
        responses_out[success_status] = {
            "description": success_resolved.get("description", ""),
            "schema": schema,
        }

    error_ref = _pick_first_error_ref(op.get("responses") or {})
    if error_ref is not None:
        responses_out["error"] = error_ref

    payload: dict[str, Any] = _operation_meta(op, method, path)
    payload["view"] = "compact"
    payload["parameters"] = params_out
    if rb_out is not None:
        payload["request_body"] = rb_out
    payload["responses"] = responses_out

    side = _extract_referenced_components(payload, components)
    if side:
        payload["$components"] = side

    return payload


def project_request_only(spec: dict, method: str, path: str) -> dict | None:
    """Return just the request body schema + flat ``required_paths`` list."""
    op = _find_operation(spec, method, path)
    if op is None:
        return None
    components = spec.get("components") or {}

    rb_out = None
    required_paths: list[str] = []
    rb = op.get("requestBody")
    if isinstance(rb, dict):
        rb_resolved = _resolve_one(rb, components, max_depth=1)
        content = (rb_resolved or {}).get("content") or {}
        media, schema = _pick_media_schema(content)
        if schema is not None:
            rb_out = {"content_type": media, "schema": schema}
            # Compute required leaf paths from the FULLY resolved schema so
            # nested required fields surface; the projection itself stays
            # ref-shaped to keep size down.
            fully_resolved = _resolve_full(schema, components)
            required_paths = _collect_required_paths(fully_resolved)

    payload: dict[str, Any] = _operation_meta(op, method, path)
    payload["view"] = "request-only"
    if rb_out is not None:
        payload["request_body"] = rb_out
    payload["required_paths"] = required_paths

    # Optional: surface a best-effort example pulled out by the scraper.
    example = op.get("x-example-request")
    if example is not None:
        payload["example_request"] = example

    side = _extract_referenced_components(payload, components)
    if side:
        payload["$components"] = side

    return payload


def project_full(spec: dict, method: str, path: str) -> dict | None:
    """Return the fully-resolved view (parity with the legacy detail tool)."""
    op = _find_operation(spec, method, path)
    if op is None:
        return None
    components = spec.get("components") or {}

    params_out: list[dict] = []
    for p in op.get("parameters", []) or []:
        resolved = _resolve_full(p, components)
        if not isinstance(resolved, dict):
            continue
        params_out.append({
            "name": resolved.get("name", ""),
            "in": resolved.get("in", ""),
            "required": resolved.get("required", False),
            "description": resolved.get("description", ""),
            "schema": resolved.get("schema") or {},
        })

    rb_out = None
    rb = op.get("requestBody")
    if isinstance(rb, dict):
        rb_resolved = _resolve_full(rb, components)
        content = (rb_resolved or {}).get("content") or {}
        media, schema = _pick_media_schema(content)
        if schema is not None:
            rb_out = {
                "content_type": media,
                "schema": schema,
                "required": rb_resolved.get("required", False),
            }

    responses_out: list[dict] = []
    for status, resp in (op.get("responses") or {}).items():
        resp_resolved = _resolve_full(resp, components)
        content = (resp_resolved or {}).get("content") or {}
        _, schema = _pick_media_schema(content)
        responses_out.append({
            "status": str(status),
            "description": resp_resolved.get("description", ""),
            "schema": schema,
        })

    payload: dict[str, Any] = _operation_meta(op, method, path)
    payload["view"] = "full"
    payload["parameters"] = params_out
    if rb_out is not None:
        payload["request_body"] = rb_out
    payload["responses"] = responses_out
    return payload


def project_raw(spec: dict, method: str, path: str) -> dict | None:
    """Return the raw operation object plus untouched ``components``."""
    op = _find_operation(spec, method, path)
    if op is None:
        return None
    return {
        "view": "raw",
        "method": method.upper(),
        "path": path,
        "operation": copy.deepcopy(op),
        "components": copy.deepcopy(spec.get("components") or {}),
    }


# ---------------------------------------------------------------------------
# Resolution helpers shared by projections
# ---------------------------------------------------------------------------


_MAX_FULL_DEPTH = 15


def _resolve_full(node: Any, components: dict, *, depth: int = 0) -> Any:
    """Recursively expand every ``$ref``.  Mirrors ``oas_index._resolve_refs``."""
    if depth > _MAX_FULL_DEPTH:
        return {"$circular_ref": True}
    if isinstance(node, dict):
        ref = node.get("$ref")
        if isinstance(ref, str):
            target = _follow_ref(ref, components)
            if target is not None:
                return _resolve_full(target, components, depth=depth + 1)
            return node
        return {k: _resolve_full(v, components, depth=depth) for k, v in node.items()}
    if isinstance(node, list):
        return [_resolve_full(i, components, depth=depth) for i in node]
    return node


def _pick_media_schema(content: dict) -> tuple[str, Any]:
    """Return (media_type, schema) preferring application/json."""
    if not isinstance(content, dict) or not content:
        return ("", None)
    for preferred in ("application/json", "application/merge-patch+json"):
        if preferred in content:
            entry = content[preferred] or {}
            return (preferred, entry.get("schema"))
    media, entry = next(iter(content.items()))
    return (media, (entry or {}).get("schema"))


def _pick_success_response(responses: dict) -> tuple[str, Any]:
    """Return (status, response_obj) preferring 200/201/204."""
    for status in ("200", "201", "204", "202"):
        if status in responses:
            return (status, responses[status])
    # Fallback: first 2xx.
    for status, resp in responses.items():
        s = str(status)
        if s.startswith("2") or s.upper() == "2XX":
            return (s, resp)
    return ("", None)


def _pick_first_error_ref(responses: dict) -> dict | None:
    """Return the first error-class response that points at a $ref."""
    for status, resp in responses.items():
        s = str(status)
        if not (s.startswith(("4", "5")) or s.upper().endswith("XX") and s[0] in {"4", "5"}):
            continue
        if isinstance(resp, dict) and "$ref" in resp:
            return {"status": s, "$ref": resp["$ref"]}
    return None


def _collect_required_paths(schema: Any, prefix: str = "") -> list[str]:
    """Walk a fully-resolved schema and return dot-paths to required leaves."""
    out: list[str] = []
    if not isinstance(schema, dict):
        return out

    required = schema.get("required") or []
    props = schema.get("properties") or {}
    if isinstance(required, list) and isinstance(props, dict):
        for name in required:
            if not isinstance(name, str):
                continue
            child_path = f"{prefix}.{name}" if prefix else name
            child = props.get(name)
            if isinstance(child, dict) and child.get("type") == "object":
                nested = _collect_required_paths(child, child_path)
                out.extend(nested or [child_path])
            elif isinstance(child, dict) and child.get("type") == "array":
                items = child.get("items")
                if isinstance(items, dict):
                    nested = _collect_required_paths(items, f"{child_path}[]")
                    out.extend(nested or [child_path])
                else:
                    out.append(child_path)
            else:
                out.append(child_path)

    # Also walk into nested properties even if not required, to surface deeply
    # nested required leaves.
    if isinstance(props, dict):
        for name, child in props.items():
            if not isinstance(child, dict):
                continue
            child_path = f"{prefix}.{name}" if prefix else name
            if name in (required or []):
                continue  # already handled above
            if child.get("type") == "object":
                out.extend(_collect_required_paths(child, child_path))
            elif child.get("type") == "array":
                items = child.get("items")
                if isinstance(items, dict):
                    out.extend(_collect_required_paths(items, f"{child_path}[]"))

    return out
