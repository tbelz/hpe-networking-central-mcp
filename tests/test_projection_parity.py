"""Tests for compiler-vs-legacy projection parity reporting."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
import shutil
import tempfile

import pytest
import real_ladybug as lb

from hpe_networking_central_mcp.compiler.projection_parity import (
    compute_projection_parity,
    format_projection_parity_report,
)
from hpe_networking_central_mcp.graph.schema import (
    KNOWLEDGE_NODE_TABLES,
    KNOWLEDGE_REL_TABLES,
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


def _make_db(path: Path) -> lb.Database:
    db = lb.Database(str(path), max_db_size=256 * 1024 * 1024)
    conn = lb.Connection(db)
    for ddl in KNOWLEDGE_NODE_TABLES + KNOWLEDGE_REL_TABLES:
        conn.execute(ddl.strip())
    return db


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
