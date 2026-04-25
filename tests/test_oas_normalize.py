"""Tests for the OAS normalization + view projections."""

from __future__ import annotations

import copy
import json

import pytest

from hpe_networking_central_mcp.oas_normalize import (
    COMPACT_BUDGET_BYTES,
    REQUEST_ONLY_BUDGET_BYTES,
    normalize,
    project_compact,
    project_full,
    project_raw,
    project_request_only,
)


# ── Helpers ──────────────────────────────────────────────────────────


def _make_error_response(status_msg: str) -> dict:
    return {
        "description": status_msg,
        "content": {
            "application/json": {
                "schema": {
                    "type": "object",
                    "properties": {
                        "error_code": {"type": "string"},
                        "description": {"type": "string"},
                        "service_name": {"type": "string"},
                    },
                }
            }
        },
    }


def _make_nested_object_schema() -> dict:
    return {
        "type": "object",
        "title": "Rule",
        "properties": {
            "src": {"type": "string"},
            "dst": {"type": "string"},
            "action": {"type": "string"},
            "protocol": {"type": "string"},
        },
    }


def _make_synthetic_spec(num_endpoints: int = 12) -> dict:
    """Build a spec that mimics the duplication patterns of sw-port-profiles."""
    paths: dict = {}
    rule_schema = _make_nested_object_schema()
    for i in range(num_endpoints):
        path = f"/v1/widgets/{i}"
        paths[path] = {
            "post": {
                "summary": f"Create widget {i}",
                "operationId": f"createWidget{i}",
                "tags": ["widgets"],
                "requestBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {
                                "type": "object",
                                "required": ["name", "rule"],
                                "properties": {
                                    "name": {"type": "string"},
                                    "rule": copy.deepcopy(rule_schema),
                                    "fallback_rule": copy.deepcopy(rule_schema),
                                },
                            }
                        }
                    },
                },
                "responses": {
                    "201": {
                        "description": "created",
                        "content": {
                            "application/json": {
                                "schema": {"type": "object", "properties": {"id": {"type": "string"}}}
                            }
                        },
                    },
                    "400": _make_error_response("Bad request"),
                    "404": _make_error_response("Not found"),
                    "500": _make_error_response("Internal server error"),
                },
            }
        }
    return {"openapi": "3.0.0", "info": {"title": "Widgets API"}, "paths": paths}


# ── Normalize ────────────────────────────────────────────────────────


def test_normalize_dedups_error_responses_to_components():
    spec = _make_synthetic_spec(num_endpoints=4)

    out = normalize(spec)

    # The four 400/404/500 inline errors per operation should now point at $refs.
    op = out["paths"]["/v1/widgets/0"]["post"]
    assert "$ref" in op["responses"]["400"]
    assert "$ref" in op["responses"]["500"]
    # And the components section grew.
    assert out["components"]["responses"], "expected promoted error components"


def test_normalize_dedups_nested_object_to_components():
    spec = _make_synthetic_spec(num_endpoints=3)
    out = normalize(spec)

    op = out["paths"]["/v1/widgets/0"]["post"]
    schema = op["requestBody"]["content"]["application/json"]["schema"]
    # The outer body schema may itself have been promoted; resolve one hop.
    if "$ref" in schema:
        ref = schema["$ref"].removeprefix("#/components/schemas/")
        schema = out["components"]["schemas"][ref]
    rule = schema["properties"]["rule"]
    assert "$ref" in rule, f"expected rule to be $ref, got {rule!r}"
    assert rule["$ref"].startswith("#/components/schemas/")


def test_normalize_is_idempotent():
    spec = _make_synthetic_spec(num_endpoints=3)
    once = normalize(spec)
    twice = normalize(once)
    assert json.dumps(once, sort_keys=True) == json.dumps(twice, sort_keys=True)


def test_normalize_lossless_against_full_resolution():
    """resolve(normalize(s)) must equal resolve(s) for the same operation body."""
    spec = _make_synthetic_spec(num_endpoints=3)
    raw_full = project_full(spec, "POST", "/v1/widgets/0")
    norm_full = project_full(normalize(spec), "POST", "/v1/widgets/0")
    # Compare request body and responses (the parts that get rewritten).
    assert raw_full is not None
    assert norm_full is not None
    assert raw_full["request_body"]["schema"] == norm_full["request_body"]["schema"]
    # Response shapes (status, description, schema) should match too.
    raw_resp = {r["status"]: r for r in raw_full["responses"]}
    norm_resp = {r["status"]: r for r in norm_full["responses"]}
    assert set(raw_resp) == set(norm_resp)
    for status, raw_r in raw_resp.items():
        assert raw_r["schema"] == norm_resp[status]["schema"]


def test_normalize_strips_noisy_metadata():
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "T"},
        "paths": {
            "/x": {
                "get": {
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "type": "object",
                                        "format": "const",
                                        "maxLength": 9999,
                                        "description": "a duck",
                                        "x-typeDescription": "a duck",
                                        "x-patternSources": list(range(50)),
                                        "properties": {"q": {"type": "string"}},
                                    }
                                }
                            },
                        }
                    }
                }
            }
        },
    }
    out = normalize(spec)
    schema = out["paths"]["/x"]["get"]["responses"]["200"]["content"]["application/json"]["schema"]
    assert "format" not in schema
    assert "maxLength" not in schema
    assert "x-typeDescription" not in schema
    assert "x-patternSources" not in schema


# ── Projections ──────────────────────────────────────────────────────


def test_project_compact_returns_self_contained_payload():
    spec = normalize(_make_synthetic_spec(num_endpoints=4))
    proj = project_compact(spec, "POST", "/v1/widgets/0")
    assert proj is not None
    assert proj["view"] == "compact"
    assert proj["method"] == "POST"
    # Side-table contains every $ref reachable from the projection.
    body_str = json.dumps(proj)
    refs_in_body = body_str.count('"$ref"')
    if refs_in_body:
        assert "$components" in proj


def test_project_compact_size_budget_on_synthetic_spec():
    """Large spec with repeated error/object schemas should compress under 15 KB."""
    spec = normalize(_make_synthetic_spec(num_endpoints=20))
    proj = project_compact(spec, "POST", "/v1/widgets/0")
    assert proj is not None
    size = len(json.dumps(proj))
    assert size <= COMPACT_BUDGET_BYTES, f"compact view {size} bytes > {COMPACT_BUDGET_BYTES}"


def test_project_request_only_returns_required_paths():
    spec = normalize(_make_synthetic_spec(num_endpoints=3))
    proj = project_request_only(spec, "POST", "/v1/widgets/0")
    assert proj is not None
    assert proj["view"] == "request-only"
    assert "request_body" in proj
    # Required leaves include 'name' and recurse into 'rule'.
    rp = proj["required_paths"]
    assert "name" in rp


def test_project_request_only_size_budget():
    spec = normalize(_make_synthetic_spec(num_endpoints=20))
    proj = project_request_only(spec, "POST", "/v1/widgets/0")
    assert proj is not None
    size = len(json.dumps(proj))
    assert size <= REQUEST_ONLY_BUDGET_BYTES, (
        f"request-only view {size} bytes > {REQUEST_ONLY_BUDGET_BYTES}"
    )


def test_project_full_resolves_all_refs():
    spec = normalize(_make_synthetic_spec(num_endpoints=3))
    proj = project_full(spec, "POST", "/v1/widgets/0")
    assert proj is not None
    assert proj["view"] == "full"
    assert '"$ref"' not in json.dumps(proj)


def test_project_raw_returns_untouched_operation():
    spec = normalize(_make_synthetic_spec(num_endpoints=3))
    proj = project_raw(spec, "POST", "/v1/widgets/0")
    assert proj is not None
    assert proj["view"] == "raw"
    assert "operation" in proj
    assert "components" in proj


def test_projections_return_none_for_unknown_endpoint():
    spec = _make_synthetic_spec(num_endpoints=1)
    assert project_compact(spec, "GET", "/nope") is None
    assert project_full(spec, "GET", "/nope") is None
    assert project_raw(spec, "GET", "/nope") is None
    assert project_request_only(spec, "GET", "/nope") is None


def test_normalize_preserves_existing_components():
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "T"},
        "components": {"schemas": {"Existing": {"type": "object"}}},
        "paths": {},
    }
    out = normalize(spec)
    assert "Existing" in out["components"]["schemas"]
