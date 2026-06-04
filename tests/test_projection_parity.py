"""Tests for compiler-vs-legacy projection parity reporting."""

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
from hpe_networking_central_mcp.compiler.projection_parity import (
    compute_projection_parity,
    format_projection_parity_report,
)
from hpe_networking_central_mcp.compiler.semantic_builder import build_semantic_overlay
from hpe_networking_central_mcp.graph.schema import (
    KNOWLEDGE_NODE_TABLES,
    KNOWLEDGE_REL_TABLES,
)
from hpe_networking_central_mcp.oas_normalize import normalize
from hpe_networking_central_mcp.oas_schema_graph import (
    collect_into_batch,
    flush_batch,
    new_batch,
    query_existing_eids,
)

pytestmark = [pytest.mark.compiler, pytest.mark.unit]


@pytest.fixture
def repo_tmp_path() -> Iterator[Path]:
    repo_tmp = Path(__file__).resolve().parent.parent / "tmp"
    repo_tmp.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="test_projection_parity_", dir=repo_tmp))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_projection_parity_passes_when_agent_signatures_match(
    repo_tmp_path: Path,
) -> None:
    legacy_db = _make_db(repo_tmp_path / "legacy")
    compiler_db = _make_db(repo_tmp_path / "compiler")
    try:
        _populate_api_shape(lb.Connection(legacy_db))
        _populate_api_shape(lb.Connection(compiler_db))

        report = compute_projection_parity(
            lb.Connection(legacy_db),
            lb.Connection(compiler_db),
        )

        assert report["all_legacy_covered"] is True
        assert report["total_legacy_missing"] == 0
        assert report["checks"]["parameters"]["legacy_coverage_ratio"] == 1.0
        assert "covers all legacy" in format_projection_parity_report(report)
    finally:
        legacy_db.close()
        compiler_db.close()


def test_projection_parity_reports_missing_legacy_signature(
    repo_tmp_path: Path,
) -> None:
    legacy_db = _make_db(repo_tmp_path / "legacy")
    compiler_db = _make_db(repo_tmp_path / "compiler")
    try:
        _populate_api_shape(lb.Connection(legacy_db))
        _populate_api_shape(lb.Connection(compiler_db), include_parameter=False)

        report = compute_projection_parity(
            lb.Connection(legacy_db),
            lb.Connection(compiler_db),
        )

        parameter_check = report["checks"]["parameters"]
        assert report["all_legacy_covered"] is False
        assert parameter_check["legacy_missing_count"] == 1
        assert parameter_check["legacy_missing_samples"] == [
            {
                "location": "query",
                "method": "GET",
                "name": "limit",
                "path": "/pets",
            }
        ]
        assert "parameters: 1/1 legacy signatures missing" in format_projection_parity_report(report)
    finally:
        legacy_db.close()
        compiler_db.close()


def test_compiler_projection_covers_legacy_structural_schema_identities(
    repo_tmp_path: Path,
) -> None:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Structural parity", "version": "1.0"},
        "paths": {
            "/config": {
                "post": {
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Root"}
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Root"}
                                }
                            },
                        }
                    },
                }
            }
        },
        "components": {
            "schemas": {
                "Leaf": {"type": "string"},
                "Base": {
                    "type": "object",
                    "properties": {"base": {"type": "string"}},
                },
                "Root": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Base"},
                        {
                            "type": "object",
                            "properties": {"branch": {"type": "string"}},
                        },
                    ],
                    "properties": {
                        "direct": {"$ref": "#/components/schemas/Leaf"},
                        "nested": {
                            "type": "object",
                            "properties": {
                                "child": {"$ref": "#/components/schemas/Leaf"}
                            },
                        },
                        "entries": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "leaf": {"$ref": "#/components/schemas/Leaf"}
                                },
                            },
                        },
                    },
                    "additionalProperties": {"$ref": "#/components/schemas/Leaf"},
                },
            }
        },
    }
    legacy_db = _make_db(repo_tmp_path / "legacy_structural")
    compiler_path = repo_tmp_path / "compiler_structural"
    normalized = normalize(spec)
    legacy_conn = lb.Connection(legacy_db)
    _seed_endpoint(legacy_conn, method="POST", path="/config")
    batch = new_batch()
    eids = query_existing_eids(legacy_conn, ["POST:/config"])
    collect_into_batch(
        batch,
        spec_source="central",
        spec=normalized,
        endpoints=[("POST", "/config")],
        existing_eids=eids,
    )
    flush_batch(legacy_conn, batch)

    ast = build_ast_graph(spec, source="central/structural-parity")
    semantic = build_semantic_overlay(ast)
    build_compiler_projection_database(compiler_path, [ast], [semantic])
    compiler_db = lb.Database(str(compiler_path), max_db_size=256 * 1024 * 1024)
    try:
        report = compute_projection_parity(legacy_conn, lb.Connection(compiler_db))
        for check in (
            "body_references",
            "response_references",
            "schema_components",
            "properties",
            "property_types",
            "composition",
            "value_schemas",
        ):
            assert report["checks"][check]["legacy_coverage_ratio"] == 1.0, (
                check,
                report["checks"][check]["legacy_missing_samples"],
            )
    finally:
        compiler_db.close()
        legacy_db.close()


def _make_db(path: Path) -> lb.Database:
    db = lb.Database(str(path), max_db_size=256 * 1024 * 1024)
    conn = lb.Connection(db)
    for ddl in KNOWLEDGE_NODE_TABLES + KNOWLEDGE_REL_TABLES:
        conn.execute(ddl.strip())
    return db


def _seed_endpoint(conn, *, method: str, path: str) -> None:
    conn.execute(
        """
        CREATE (:ApiEndpoint {
          endpoint_id: $endpoint_id,
          method: $method,
          path: $path,
          summary: '',
          description: '',
          operationId: '',
          category: '',
          deprecated: false,
          tags: [],
          parameters: '',
          requestBody: '',
          responses: ''
        })
        """,
        parameters={
            "endpoint_id": f"{method}:{path}",
            "method": method,
            "path": path,
        },
    )


def _populate_api_shape(conn, *, include_parameter: bool = True) -> None:
    conn.execute(
        """
        CREATE (:ApiEndpoint {
          endpoint_id: 'GET:/pets',
          method: 'GET',
          path: '/pets',
          summary: 'List pets',
          description: '',
          operationId: 'listPets',
          category: '',
          deprecated: false,
          tags: ['pets'],
          parameters: '',
          requestBody: '',
          responses: ''
        })
        """
    )
    conn.execute(
        """
        CREATE (:SchemaComponent {
          component_id: 'central:schemas:Pet',
          spec_source: 'central',
          section: 'schemas',
          name: 'Pet',
          type: 'object',
          kind: 'object',
          bodyShape: 'object',
          required: ['id'],
          enumValues: [],
          supportedDeviceTypes: [],
          bodyJson: '{"type":"object","properties":{"id":{"type":"string"}}}'
        })
        """
    )
    conn.execute(
        """
        CREATE (:Property {
          property_id: 'central:schemas:Pet#prop:id',
          parent_component_id: 'central:schemas:Pet',
          name: 'id',
          type: 'string',
          format: '',
          required: true,
          enumValues: [],
          description: '',
          supportedDeviceTypes: [],
          yangPath: '/ac-pet:pets/ac-pet:id',
          extensionsJson: '{"x-path":"/ac-pet:pets/ac-pet:id"}',
          readOnly: false
        })
        """
    )
    conn.execute(
        """
        CREATE (:RequestBody {
          request_body_id: 'GET:/pets#requestBody:application~1json',
          endpoint_id: 'GET:/pets',
          content_type: 'application/json',
          required: false,
          root_component_ref: 'central:schemas:Pet'
        })
        """
    )
    conn.execute(
        """
        CREATE (:Response {
          response_id: 'GET:/pets#response:200:application~1json',
          endpoint_id: 'GET:/pets',
          status: '200',
          content_type: 'application/json',
          root_component_ref: 'central:schemas:Pet'
        })
        """
    )
    conn.execute(
        """
        CREATE (:YangPath {
          yangPath: '/ac-pet:pets/ac-pet:id',
          module: 'ac-pet'
        })
        """
    )
    conn.execute("CREATE (:YangModule {module: 'ac-pet'})")
    conn.execute(
        """
        CREATE (:CliCommand {
          command_id: 'GET:/pets::show pets',
          commandName: 'show pets',
          commandUse: 'show',
          parentCommand: '',
          pathToPrint: '',
          paramKeys: ['id']
        })
        """
    )
    if include_parameter:
        conn.execute(
            """
            CREATE (:Parameter {
              parameter_id: 'GET:/pets#param:query:limit',
              endpoint_id: 'GET:/pets',
              name: 'limit',
              location: 'query',
              required: false,
              type: 'integer',
              format: '',
              enumValues: [],
              pattern: '',
              inferredHint: 'pagination',
              description: ''
            })
            """
        )
        conn.execute(
            """
            MATCH (e:ApiEndpoint {endpoint_id: 'GET:/pets'}),
                  (p:Parameter {parameter_id: 'GET:/pets#param:query:limit'})
            CREATE (e)-[:HAS_PARAMETER]->(p)
            """
        )
    conn.execute(
        """
        MATCH (e:ApiEndpoint {endpoint_id: 'GET:/pets'}),
              (body:RequestBody {request_body_id: 'GET:/pets#requestBody:application~1json'}),
              (response:Response {response_id: 'GET:/pets#response:200:application~1json'}),
              (schema:SchemaComponent {component_id: 'central:schemas:Pet'}),
              (prop:Property {property_id: 'central:schemas:Pet#prop:id'}),
              (yang:YangPath {yangPath: '/ac-pet:pets/ac-pet:id'}),
              (module:YangModule {module: 'ac-pet'}),
              (command:CliCommand {command_id: 'GET:/pets::show pets'})
        CREATE (e)-[:HAS_REQUEST_BODY]->(body),
               (body)-[:BODY_REFERENCES]->(schema),
               (e)-[:HAS_RESPONSE]->(response),
               (response)-[:RESPONSE_REFERENCES]->(schema),
               (schema)-[:HAS_PROPERTY]->(prop),
               (prop)-[:PROPERTY_AT_YANG]->(yang),
               (e)-[:CONFIGURES_YANG]->(yang),
               (yang)-[:IN_MODULE]->(module),
               (e)-[:HAS_CLI_COMMAND]->(command)
        """
    )
