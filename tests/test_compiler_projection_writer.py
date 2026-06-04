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
                    "parameters": [
                        {"$ref": "#/components/parameters/DeviceId"}
                    ],
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
            "examples": {
                "RateLimitExample": {
                    "summary": "rate limited",
                    "value": {"error": "too many requests"},
                }
            },
            "parameters": {
                "DeviceId": {
                    "name": "device_id",
                    "in": "query",
                    "schema": {"$ref": "#/components/schemas/ScopeIds"},
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
                            "minItems": 1,
                            "x-key": ["scopeId"],
                            "items": {"$ref": "#/components/schemas/ScopeIds"},
                        }
                    },
                },
                "Error": {
                    "type": "object",
                    "properties": {
                        "message": {
                            "type": "string",
                            "default": "unknown",
                            "pattern": "^[a-z]+$",
                            "minLength": 1,
                            "maxLength": 64,
                            "x-enumDescriptions": {"unknown": "Unknown error"},
                        },
                        "code": {
                            "type": "integer",
                            "minimum": 400,
                            "maximum": 599,
                        },
                    },
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
                  'central:examples:RateLimitExample',
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
                "component_id": "central:examples:RateLimitExample",
                "section": "examples",
                "name": "RateLimitExample",
            },
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

        item_schema_rows = list(
            conn.execute(
                """
                MATCH (schema:SchemaComponent {component_id: 'central:schemas:DeviceMove'})
                      -[:HAS_PROPERTY]->(prop:Property {name: 'items'})
                      -[:HAS_ITEM_SCHEMA]->(target:SchemaComponent)
                RETURN target.component_id AS target_component_id
                """
            ).rows_as_dict()
        )
        assert item_schema_rows == [
            {"target_component_id": "central:schemas:ScopeIds"}
        ]

        constraint_rows = list(
            conn.execute(
                """
                MATCH (:SchemaComponent {component_id: 'central:schemas:Error'})
                      -[:HAS_PROPERTY]->(prop:Property)
                RETURN prop.name AS name,
                       prop.pattern AS pattern,
                       prop.defaultValue AS defaultValue,
                       prop.minimum AS minimum,
                       prop.maximum AS maximum,
                       prop.minLength AS minLength,
                       prop.maxLength AS maxLength,
                       prop.enumDescriptionsJson AS enumDescriptionsJson,
                       prop.constraintsJson AS constraintsJson
                ORDER BY prop.name
                """
            ).rows_as_dict()
        )
        assert constraint_rows == [
            {
                "name": "code",
                "pattern": "",
                "defaultValue": "",
                "minimum": 400.0,
                "maximum": 599.0,
                "minLength": None,
                "maxLength": None,
                "enumDescriptionsJson": "",
                "constraintsJson": '{"maximum":599,"minimum":400}',
            },
            {
                "name": "message",
                "pattern": "^[a-z]+$",
                "defaultValue": '"unknown"',
                "minimum": None,
                "maximum": None,
                "minLength": 1,
                "maxLength": 64,
                "enumDescriptionsJson": '{"unknown":"Unknown error"}',
                "constraintsJson": (
                    '{"default":"unknown","maxLength":64,"minLength":1,'
                    '"pattern":"^[a-z]+$","x-enumDescriptions":'
                    '{"unknown":"Unknown error"}}'
                ),
            },
        ]

        array_key_rows = list(
            conn.execute(
                """
                MATCH (:SchemaComponent {component_id: 'central:schemas:DeviceMove'})
                      -[:HAS_PROPERTY]->(prop:Property {name: 'items'})
                RETURN prop.constraintsJson AS constraintsJson
                """
            ).rows_as_dict()
        )
        assert array_key_rows == [
            {
                "constraintsJson": '{"minItems":1,"x-key":["scopeId"]}',
            }
        ]

        parameter_type_rows = list(
            conn.execute(
                """
                MATCH (:ApiEndpoint {endpoint_id: 'POST:/move'})
                      -[:HAS_PARAMETER]->(param:Parameter {name: 'device_id'})
                      -[:PARAMETER_REFERENCES]->(target:SchemaComponent)
                RETURN target.component_id AS target_component_id
                """
            ).rows_as_dict()
        )
        assert parameter_type_rows == [
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


def test_projection_uses_lineage_ids_for_inline_schema_branches(
    repo_tmp_path: Path,
) -> None:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Inline identities", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Base": {
                    "type": "object",
                    "properties": {"base": {"type": "string"}},
                },
                "Root": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Base"},
                        {
                            "type": "object",
                            "properties": {
                                "branch": {"type": "string"},
                                "branchItems": {
                                    "type": "array",
                                    "items": {
                                        "properties": {
                                            "value": {"type": "string"}
                                        }
                                    },
                                },
                            },
                        },
                    ],
                    "properties": {
                        "nested": {
                            "type": "object",
                            "allOf": [{"$ref": "#/components/schemas/Base"}],
                            "properties": {"name": {"type": "string"}},
                        },
                        "entries": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {"id": {"type": "string"}},
                            },
                        },
                        "choice": {
                            "oneOf": [
                                {"$ref": "#/components/schemas/Base"},
                                {"type": "string"},
                            ]
                        },
                    },
                },
            }
        },
    }
    ast = build_ast_graph(spec, source="central/inline-identities")
    semantic = build_semantic_overlay(ast)
    db_path = repo_tmp_path / "knowledge_db_compiler_inline"
    build_compiler_projection_database(db_path, [ast], [semantic])

    db = lb.Database(str(db_path), max_db_size=256 * 1024 * 1024)
    conn = lb.Connection(db)
    try:
        component_ids = {
            row["component_id"]
            for row in conn.execute(
                "MATCH (component:SchemaComponent) "
                "RETURN component.component_id AS component_id"
            ).rows_as_dict()
        }
        assert {
            "central:schemas:Base",
            "central:schemas:Root",
            "central:schemas:Root#allOf:1",
            "central:schemas:Root#allOf:1#prop:branchItems#items",
            "central:schemas:Root#prop:choice#union",
            "central:schemas:Root#prop:choice#union#oneOf:1",
            "central:schemas:Root#prop:entries#items",
            "central:schemas:Root#prop:nested#object",
        }.issubset(component_ids)
        assert not any(component_id.startswith("central:inline:") for component_id in component_ids)
        assert "central:schemas:Root#prop:nested#union" not in component_ids

        composition = {
            (row["source"], row["target"])
            for row in conn.execute(
                """
                MATCH (source:SchemaComponent)-[:COMPOSED_OF]->(target:SchemaComponent)
                RETURN source.component_id AS source, target.component_id AS target
                """
            ).rows_as_dict()
        }
        assert ("central:schemas:Root", "central:schemas:Base") in composition
        assert (
            "central:schemas:Root",
            "central:schemas:Root#allOf:1",
        ) in composition
        assert (
            "central:schemas:Root#prop:choice#union",
            "central:schemas:Base",
        ) in composition
        assert (
            "central:schemas:Root#prop:nested#object",
            "central:schemas:Base",
        ) in composition
    finally:
        db.close()


def test_projection_richest_schema_keeps_matching_provenance(
    repo_tmp_path: Path,
) -> None:
    rich_spec = {
        "openapi": "3.0.3",
        "info": {"title": "Rich", "version": "1.0"},
        "paths": {},
        "components": {
            "schemas": {
                "Shared": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                }
            }
        },
    }
    stub_spec = {
        "openapi": "3.0.3",
        "info": {"title": "Stub", "version": "1.0"},
        "paths": {},
        "components": {"schemas": {"Shared": {"type": "object"}}},
    }
    rich_ast = build_ast_graph(rich_spec, source="central/rich")
    stub_ast = build_ast_graph(stub_spec, source="central/stub")
    db_path = repo_tmp_path / "knowledge_db_compiler_richest"
    build_compiler_projection_database(
        db_path,
        [rich_ast, stub_ast],
        [build_semantic_overlay(rich_ast), build_semantic_overlay(stub_ast)],
    )

    db = lb.Database(str(db_path), max_db_size=256 * 1024 * 1024)
    conn = lb.Connection(db)
    try:
        rows = list(
            conn.execute(
                """
                MATCH (component:SchemaComponent {component_id: 'central:schemas:Shared'}),
                      (provenance:CompilerProjectionMap {
                        row_id: 'central:schemas:Shared',
                        table_name: 'SchemaComponent'
                      })
                RETURN component.bodyJson AS body_json,
                       provenance.source AS source,
                       provenance.json_pointer AS json_pointer
                """
            ).rows_as_dict()
        )
        assert rows == [
            {
                "body_json": '{"type":"object","properties":{"name":{"type":"string"}}}',
                "source": "central/rich",
                "json_pointer": "/components/schemas/Shared",
            }
        ]
    finally:
        db.close()
