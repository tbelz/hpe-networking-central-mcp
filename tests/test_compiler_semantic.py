"""Tests for Task 3A: semantic overlay generation from the L1 AST."""

from __future__ import annotations

import json
from pathlib import Path

import pytest

from hpe_networking_central_mcp.compiler.ast_builder import build_ast_graph
from hpe_networking_central_mcp.compiler.frontend import clean_spec
from hpe_networking_central_mcp.compiler.semantic_builder import (
    STRUCTURAL_RULE_PACK_ID,
    build_semantic_overlay,
    _internal_ref_pointer,
)
from hpe_networking_central_mcp.compiler.semantic_metrics import compute_semantic_metrics

pytestmark = [pytest.mark.compiler, pytest.mark.unit]

_FIXTURE_DIR = Path(__file__).resolve().parent / "fixtures" / "oas" / "real_excerpts"


def _semantic_spec() -> dict:
    return {
        "openapi": "3.0.3",
        "info": {"title": "Semantic", "version": "1.0"},
        "paths": {
            "/ntp": {
                "parameters": [
                    {
                        "name": "device_id",
                        "in": "path",
                        "required": True,
                        "schema": {"type": "string"},
                    }
                ],
                "post": {
                    "operationId": "setNtp",
                    "summary": "Configure NTP",
                    "parameters": [
                        {"$ref": "#/components/parameters/SiteParam"},
                    ],
                    "x-cliParam": {
                        "commandName": "ntp server",
                        "commandUse": "configuration",
                        "paramKeys": [{"key": "server"}],
                    },
                    "requestBody": {"$ref": "#/components/requestBodies/NtpBody"},
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/NtpResponse"}
                                },
                                "application/problem+json": {
                                    "schema": {"$ref": "#/components/schemas/NtpError"}
                                }
                            },
                        },
                        "204": {"description": "No content"},
                    },
                }
            }
        },
        "components": {
            "parameters": {
                "SiteParam": {
                    "name": "site_id",
                    "in": "query",
                    "required": False,
                    "schema": {"$ref": "#/components/schemas/SiteId"},
                }
            },
            "requestBodies": {
                "NtpBody": {
                    "required": True,
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/NtpProfile"}
                        }
                    },
                }
            },
            "schemas": {
                "SiteId": {"type": "string", "format": "uuid"},
                "BaseConfig": {
                    "type": "object",
                    "properties": {
                        "enabled": {
                            "type": "boolean",
                            "x-path": "/ac-ntp:ntp/ac-ntp:enabled",
                        }
                    },
                },
                "NtpProfile": {
                    "allOf": [
                        {"$ref": "#/components/schemas/BaseConfig"},
                        {
                            "type": "object",
                            "required": ["server"],
                            "properties": {
                                "server": {
                                    "type": "string",
                                    "description": "NTP server address",
                                    "x-supportedDeviceType": ["Switch CX"],
                                    "x-path": "/ac-ntp:ntp/ac-ntp:server",
                                }
                            },
                        },
                    ]
                },
                "NtpResponse": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                },
                "NtpError": {
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                },
            }
        },
    }


def _semantic_spec_with_untyped_endpoint() -> dict:
    spec = _semantic_spec()
    spec["paths"]["/health"] = {
        "get": {
            "operationId": "health",
            "responses": {"204": {"description": "No content"}},
        }
    }
    return spec


def test_semantic_overlay_builds_agent_highways() -> None:
    ast_graph = build_ast_graph(_semantic_spec(), source="unit/semantic")
    semantic = build_semantic_overlay(ast_graph)

    assert semantic.rule_packs == (STRUCTURAL_RULE_PACK_ID,)
    nodes_by_kind = {}
    for node in semantic.nodes:
        nodes_by_kind.setdefault(node.kind, []).append(node)
    assert {node.name for node in nodes_by_kind["ApiEndpoint"]} == {"POST /ntp"}
    assert {node.name for node in nodes_by_kind["CliCommand"]} == {"ntp server"}
    assert {node.name for node in nodes_by_kind["Parameter"]} == {
        "device_id",
        "site_id",
    }
    assert len(nodes_by_kind["RequestBody"]) == 1
    assert len(nodes_by_kind["Response"]) == 3
    assert {node.name for node in nodes_by_kind["YangPath"]} == {
        "/ac-ntp:ntp/ac-ntp:enabled",
        "/ac-ntp:ntp/ac-ntp:server",
    }

    edge_tuples = {
        (
            next(n.name for n in semantic.nodes if n.semantic_id == edge.source_id),
            edge.kind,
            next(n.name for n in semantic.nodes if n.semantic_id == edge.target_id),
        )
        for edge in semantic.edges
    }
    assert ("POST /ntp", "HAS_PARAMETER", "device_id") in edge_tuples
    assert ("POST /ntp", "HAS_PARAMETER", "site_id") in edge_tuples
    assert ("site_id", "PARAMETER_REFERENCES", "SiteId") in edge_tuples
    assert ("POST /ntp", "HAS_REQUEST_BODY", "POST /ntp requestBody") in edge_tuples
    assert ("POST /ntp requestBody", "BODY_REFERENCES", "NtpProfile") in edge_tuples
    assert ("POST /ntp", "HAS_RESPONSE", "POST /ntp 200") in edge_tuples
    assert ("POST /ntp", "HAS_RESPONSE", "POST /ntp 204") in edge_tuples
    assert ("POST /ntp 200", "RESPONSE_REFERENCES", "NtpResponse") in edge_tuples
    assert ("POST /ntp 200", "RESPONSE_REFERENCES", "NtpError") in edge_tuples
    assert ("POST /ntp", "ACCEPTS_SCHEMA", "NtpProfile") in edge_tuples
    assert ("POST /ntp", "RETURNS_SCHEMA", "NtpResponse") in edge_tuples
    assert ("POST /ntp", "RETURNS_SCHEMA", "NtpError") in edge_tuples
    assert ("POST /ntp", "HAS_CLI_COMMAND", "ntp server") in edge_tuples
    assert ("POST /ntp", "CONFIGURES_YANG", "/ac-ntp:ntp/ac-ntp:server") in edge_tuples
    # "0" is the index-based name of the first allOf branch, the BaseConfig ref.
    assert ("NtpProfile", "COMPOSED_OF", "0") in edge_tuples
    assert ("server", "PROPERTY_AT_YANG", "/ac-ntp:ntp/ac-ntp:server") in edge_tuples

    server = next(node for node in nodes_by_kind["Property"] if node.name == "server")
    summary = json.loads(server.summary_json)
    assert summary["required"] is True
    assert summary["x-supportedDeviceType"] == ["Switch CX"]
    assert server.ast_node_id
    assert any(edge.semantic_id == server.semantic_id for edge in semantic.derived_edges)


def test_internal_ref_pointer_preserves_escaped_json_pointer_tokens() -> None:
    assert _internal_ref_pointer("#/paths/~1pets/get") == "/paths/~1pets/get"
    assert _internal_ref_pointer("#") == ""


def test_semantic_metrics_report_catalog_coverage_ratios() -> None:
    ast_graph = build_ast_graph(_semantic_spec_with_untyped_endpoint(), source="unit/semantic")
    semantic = build_semantic_overlay(ast_graph)
    metrics = compute_semantic_metrics([semantic])

    assert metrics["node_kind_counts"]["ApiEndpoint"] == 2
    assert metrics["node_kind_counts"]["Parameter"] == 2
    assert metrics["node_kind_counts"]["RequestBody"] == 1
    assert metrics["node_kind_counts"]["Response"] == 4
    assert metrics["edge_kind_counts"]["ACCEPTS_SCHEMA"] == 1
    assert metrics["edge_kind_counts"]["BODY_REFERENCES"] == 1
    assert metrics["edge_kind_counts"]["HAS_PARAMETER"] == 2
    assert metrics["edge_kind_counts"]["HAS_REQUEST_BODY"] == 1
    assert metrics["edge_kind_counts"]["HAS_RESPONSE"] == 4
    assert metrics["edge_kind_counts"]["RESPONSE_REFERENCES"] == 2
    assert metrics["edge_kind_counts"]["RETURNS_SCHEMA"] == 2
    assert metrics["coverage"]["endpoints_with_parameters"] == {
        "count": 1,
        "total": 2,
        "ratio": 0.5,
    }
    assert metrics["coverage"]["endpoints_with_request_bodies"] == {
        "count": 1,
        "total": 2,
        "ratio": 0.5,
    }
    assert metrics["coverage"]["endpoints_with_responses"] == {
        "count": 2,
        "total": 2,
        "ratio": 1.0,
    }
    assert metrics["coverage"]["endpoints_accepting_schema"] == {
        "count": 1,
        "total": 2,
        "ratio": 0.5,
    }
    assert metrics["coverage"]["endpoints_returning_schema"] == {
        "count": 1,
        "total": 2,
        "ratio": 0.5,
    }
    assert metrics["coverage"]["endpoints_with_any_schema_edge"] == {
        "count": 1,
        "total": 2,
        "ratio": 0.5,
    }
    assert metrics["coverage"]["semantic_nodes_with_ast_provenance"]["ratio"] == 1.0


@pytest.mark.parametrize(
    "fixture",
    sorted(_FIXTURE_DIR.glob("*.json")),
    ids=lambda p: p.stem,
)
def test_real_spec_excerpt_builds_semantic_overlay(fixture: Path) -> None:
    spec = clean_spec(json.loads(fixture.read_text(encoding="utf-8")))
    ast_graph = build_ast_graph(spec, source=f"fixture/{fixture.name}")
    semantic = build_semantic_overlay(ast_graph)
    assert semantic.nodes
    assert any(node.kind == "ApiEndpoint" for node in semantic.nodes)
    assert all(edge.rule_id.startswith(STRUCTURAL_RULE_PACK_ID) for edge in semantic.edges)
