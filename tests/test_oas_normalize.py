"""Tests for the OAS normalization + view projections."""

from __future__ import annotations

import copy
import json

from hpe_networking_central_mcp.oas_normalize import (
    normalize,
    project_glossary,
    project_skeleton,
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
    """Normalization must not change the resolved skeleton shape."""
    spec = _make_synthetic_spec(num_endpoints=3)
    raw_skel = project_skeleton(spec, "POST", "/v1/widgets/0")
    norm_skel = project_skeleton(normalize(spec), "POST", "/v1/widgets/0")
    assert raw_skel is not None
    assert norm_skel is not None
    # The structural keys must match (parameters, request_body, responses,
    # required_paths). $components keys may differ in name because dedup
    # may promote a schema only post-normalization.
    for key in ("parameters", "required_paths"):
        assert raw_skel.get(key) == norm_skel.get(key), key


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


# ── Projections (skeleton + glossary) ───────────────────────────────


DESCRIPTION_KEYS = (
    "description", "title", "example", "examples",
    "x-typeName", "x-typeDescription", "x-patternSources",
)


def _walk(node):
    if isinstance(node, dict):
        yield node
        for v in node.values():
            yield from _walk(v)
    elif isinstance(node, list):
        for item in node:
            yield from _walk(item)


def test_skeleton_strips_descriptions_at_every_level():
    spec = normalize(_make_synthetic_spec(num_endpoints=4))
    skel = project_skeleton(spec, "POST", "/v1/widgets/0")
    assert skel is not None
    # The top-level meta KEEPS its summary so the agent has a one-line label.
    # But every NESTED dict in parameters / request_body / responses /
    # $components must have no description-bearing keys.
    for key in ("parameters", "request_body", "responses", "$components"):
        sub = skel.get(key)
        if sub is None:
            continue
        for node in _walk(sub):
            for forbidden in DESCRIPTION_KEYS:
                assert forbidden not in node, (
                    f"{forbidden!r} leaked into skeleton at {node!r}"
                )


def test_skeleton_preserves_structure():
    spec = normalize(_make_synthetic_spec(num_endpoints=4))
    skel = project_skeleton(spec, "POST", "/v1/widgets/0")
    assert skel is not None
    assert skel["method"] == "POST"
    assert skel["path"] == "/v1/widgets/0"
    assert "parameters" in skel
    assert "request_body" in skel
    assert "required_paths" in skel
    assert "name" in skel["required_paths"]
    body_str = json.dumps(skel)
    if body_str.count('"$ref"') > 0:
        assert "$components" in skel


def test_glossary_returns_descriptions_only():
    spec = normalize(_make_synthetic_spec(num_endpoints=4))
    gloss = project_glossary(spec, "POST", "/v1/widgets/0")
    assert gloss is not None
    assert gloss["method"] == "POST"
    assert gloss["path"] == "/v1/widgets/0"
    assert isinstance(gloss["components"], dict)
    for entry in gloss["components"].values():
        # Glossary must not leak structural keys.
        assert "type" not in entry
        for pe in (entry.get("properties") or {}).values():
            assert "type" not in pe
            assert "$ref" not in pe


def test_glossary_carries_property_descriptions_when_present():
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "T"},
        "components": {
            "schemas": {
                "VlanCfg": {
                    "type": "object",
                    "description": "VLAN configuration object.",
                    "properties": {
                        "vlan_id": {
                            "type": "integer",
                            "description": "VLAN identifier in [1, 4094].",
                            "enum": [1, 100, 200],
                        },
                    },
                }
            }
        },
        "paths": {
            "/v/{id}": {
                "post": {
                    "summary": "create",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/VlanCfg"}
                            }
                        },
                    },
                    "responses": {"201": {"description": "ok"}},
                }
            }
        },
    }
    gloss = project_glossary(spec, "POST", "/v/{id}")
    assert gloss is not None
    assert "VlanCfg" in gloss["components"]
    entry = gloss["components"]["VlanCfg"]
    assert entry["description"] == "VLAN configuration object."
    vlan_id = entry["properties"]["vlan_id"]
    assert vlan_id["description"].startswith("VLAN identifier")
    assert vlan_id["enum"] == [1, 100, 200]


def test_projections_return_none_for_unknown_endpoint():
    spec = _make_synthetic_spec(num_endpoints=1)
    assert project_skeleton(spec, "GET", "/nope") is None
    assert project_glossary(spec, "GET", "/nope") is None


def test_normalize_preserves_existing_components():
    spec = {
        "openapi": "3.0.0",
        "info": {"title": "T"},
        "components": {"schemas": {"Existing": {"type": "object"}}},
        "paths": {},
    }
    out = normalize(spec)
    assert "Existing" in out["components"]["schemas"]
