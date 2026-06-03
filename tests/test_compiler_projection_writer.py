"""Tests for compiler projection materialization into typed L3 tables."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
import shutil
import tempfile

import pytest
import real_ladybug as lb

from hpe_networking_central_mcp.compiler.ast_builder import build_ast_graph
from hpe_networking_central_mcp.compiler.projection_writer import (
    build_compiler_projection_database,
)
from hpe_networking_central_mcp.compiler.semantic_builder import build_semantic_overlay

pytestmark = [pytest.mark.compiler, pytest.mark.unit]


@pytest.fixture
def repo_tmp_path() -> Iterator[Path]:
    repo_tmp = Path(__file__).resolve().parent.parent / "tmp"
    repo_tmp.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="test_compiler_projection_", dir=repo_tmp))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_projection_materializes_reusable_response_header_and_array_item_ref(
    repo_tmp_path: Path,
) -> None:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Projection", "version": "1.0"},
        "paths": {
            "/move": {
                "post": {
                    "operationId": "moveDevices",
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/DeviceMove"}
                            }
                        }
                    },
                    "responses": {
                        "429": {"$ref": "#/components/responses/TooManyRequests"}
                    },
                }
            }
        },
        "components": {
            "headers": {
                "XRateLimitLimitHeader": {
                    "description": "limit",
                    "schema": {"type": "integer"},
                }
            },
            "responses": {
                "TooManyRequests": {
                    "description": "rate limited",
                    "headers": {
                        "X-RateLimit-Limit": {
                            "$ref": "#/components/headers/XRateLimitLimitHeader"
                        }
                    },
                    "content": {
                        "application/json": {
                            "schema": {"$ref": "#/components/schemas/Error"}
                        }
                    },
                }
            },
            "schemas": {
                "DeviceMove": {
                    "type": "object",
                    "properties": {
                        "items": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/ScopeIds"},
                        }
                    },
                },
                "Error": {
                    "type": "object",
                    "properties": {"message": {"type": "string"}},
                },
                "ScopeIds": {"type": "string"},
            },
        },
    }
    ast = build_ast_graph(spec, source="central/projection")
    semantic = build_semantic_overlay(ast)
    db_path = repo_tmp_path / "knowledge_db_compiler"
    stats = build_compiler_projection_database(db_path, [ast], [semantic])

    assert stats["node_kind_counts"]["SchemaComponent"] >= 5
    db = lb.Database(str(db_path), max_db_size=256 * 1024 * 1024)
    conn = lb.Connection(db)
    try:
        reusable_rows = list(
            conn.execute(
                """
                MATCH (component:SchemaComponent)
                WHERE component.component_id IN [
                  'central:headers:XRateLimitLimitHeader',
                  'central:responses:TooManyRequests'
                ]
                RETURN component.component_id AS component_id,
                       component.section AS section,
                       component.name AS name
                ORDER BY component.component_id
                """
            ).rows_as_dict()
        )
        assert reusable_rows == [
            {
                "component_id": "central:headers:XRateLimitLimitHeader",
                "section": "headers",
                "name": "XRateLimitLimitHeader",
            },
            {
                "component_id": "central:responses:TooManyRequests",
                "section": "responses",
                "name": "TooManyRequests",
            },
        ]

        item_type_rows = list(
            conn.execute(
                """
                MATCH (schema:SchemaComponent {component_id: 'central:schemas:DeviceMove'})
                      -[:HAS_PROPERTY]->(prop:Property {name: 'items'})
                      -[:PROPERTY_OF_TYPE]->(target:SchemaComponent)
                RETURN target.component_id AS target_component_id
                """
            ).rows_as_dict()
        )
        assert item_type_rows == [
            {"target_component_id": "central:schemas:ScopeIds"}
        ]

        provenance_rows = list(
            conn.execute(
                """
                MATCH (provenance:CompilerProjectionMap)
                WHERE provenance.row_id = 'central:headers:XRateLimitLimitHeader'
                   OR provenance.row_id = 'central:schemas:DeviceMove#prop:items'
                RETURN provenance.table_name AS table_name,
                       provenance.row_id AS row_id,
                       provenance.semantic_id AS semantic_id,
                       provenance.ast_node_id AS ast_node_id,
                       provenance.json_pointer AS json_pointer
                ORDER BY provenance.row_id
                """
            ).rows_as_dict()
        )
        assert provenance_rows == [
            {
                "table_name": "SchemaComponent",
                "row_id": "central:headers:XRateLimitLimitHeader",
                "semantic_id": "",
                "ast_node_id": ast.spec_id + "#/components/headers/XRateLimitLimitHeader",
                "json_pointer": "/components/headers/XRateLimitLimitHeader",
            },
            {
                "table_name": "Property",
                "row_id": "central:schemas:DeviceMove#prop:items",
                "semantic_id": next(
                    node.semantic_id
                    for node in semantic.nodes
                    if node.kind == "Property" and node.name == "items"
                ),
                "ast_node_id": ast.spec_id + "#/components/schemas/DeviceMove/properties/items",
                "json_pointer": "/components/schemas/DeviceMove/properties/items",
            },
        ]
    finally:
        db.close()
