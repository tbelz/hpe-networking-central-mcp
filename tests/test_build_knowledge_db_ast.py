"""Build-script tests for the L1 OpenAPI AST artifact."""

from __future__ import annotations

from collections.abc import Iterator
import importlib.util
import json
import shutil
import tarfile
import tempfile
from pathlib import Path

import pytest
import real_ladybug as lb

from hpe_networking_central_mcp.compiler.ast_builder import UnknownKeywordError
from hpe_networking_central_mcp.compiler.frontend import (
    ResolutionFailure,
    ResolvedSpec,
    resolve_spec,
)

pytestmark = [pytest.mark.compiler, pytest.mark.unit]


@pytest.fixture
def repo_tmp_path() -> Iterator[Path]:
    repo_tmp = Path(__file__).resolve().parent.parent / "tmp"
    repo_tmp.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="test_build_ast_", dir=repo_tmp))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _load_build_module():
    repo_root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "build_knowledge_db",
        repo_root / "scripts" / "build_knowledge_db.py",
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    return mod


def _oas_spec() -> dict:
    return {
        "openapi": "3.0.3",
        "info": {"title": "Pets", "version": "1.0"},
        "paths": {
            "/pets": {
                "get": {
                    "operationId": "listPets",
                    "parameters": [
                        {
                            "name": "limit",
                            "in": "query",
                            "schema": {"type": "integer"},
                        }
                    ],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {
                                    "$ref": "#/components/schemas/Pet",
                                }
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {
                                        "$ref": "#/components/schemas/Pet",
                                    }
                                }
                            },
                        }
                    },
                }
            }
        },
        "components": {
            "schemas": {
                "Pet": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                }
            }
        },
    }


def _resolved() -> ResolvedSpec:
    outcome = resolve_spec(_oas_spec(), source="unit/pets")
    assert isinstance(outcome, ResolvedSpec), getattr(outcome, "error", None)
    return outcome


@pytest.mark.timeout(90)
def test_build_ast_artifact_writes_queryable_ladybug_db(repo_tmp_path: Path) -> None:
    mod = _load_build_module()
    ast_db_path = repo_tmp_path / "knowledge_db_ast"
    compiler_db_path = repo_tmp_path / "knowledge_db_compiler"
    ast_db_path.mkdir()
    compiler_db_path.mkdir()
    (ast_db_path / "stale.txt").write_text("old", encoding="utf-8")
    (compiler_db_path / "stale.txt").write_text("old", encoding="utf-8")

    stats = mod._build_ast_artifact(
        ast_db_path,
        [_resolved()],
        task1_failures=[
            ResolutionFailure(
                source="unit/bad",
                title="Bad",
                error="strict validation failed",
                error_type="validation",
            ),
            ResolutionFailure(
                source="unit/broken",
                title="Broken",
                error="ref resolution failed",
                error_type="resolution",
            ),
        ],
        compiler_projection_db_path=compiler_db_path,
    )

    assert stats["enabled"] is True
    assert stats["db_path"] == "knowledge_db_ast"
    assert stats["raw_spec_count"] == 3
    assert stats["task1_resolved_count"] == 1
    assert stats["spec_count"] == 1
    assert stats["task1_failed_count"] == 2
    assert stats["task1_failures"] == [
        {
            "source": "unit/bad",
            "title": "Bad",
            "error_type": "validation",
            "error": "strict validation failed",
        },
        {
            "source": "unit/broken",
            "title": "Broken",
            "error_type": "resolution",
            "error": "ref resolution failed",
        },
    ]
    assert stats["node_count"] > 0
    assert stats["child_edge_count"] > 0
    assert stats["ref_edge_count"] == 2
    assert stats["semantic"]["graph_count"] == 1
    assert stats["semantic"]["node_count"] > 0
    assert stats["semantic"]["edge_count"] > 0
    assert stats["semantic"]["derived_from_ast_edge_count"] > 0
    assert set(stats["semantic"]["rule_packs"]) == {
        "semantic.identity.v1",
        "semantic.structural.v1",
    }
    assert stats["semantic"]["metrics"]["node_kind_counts"]["ApiEndpoint"] == 1
    assert stats["semantic"]["metrics"]["node_kind_counts"]["ModelEntity"] == 6
    assert stats["semantic"]["metrics"]["node_kind_counts"]["Parameter"] == 1
    assert stats["semantic"]["metrics"]["node_kind_counts"]["RequestBody"] == 1
    assert stats["semantic"]["metrics"]["node_kind_counts"]["Response"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["BODY_REFERENCES"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["ACCEPTS_MODEL"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["HAS_PARAMETER"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["HAS_REQUEST_BODY"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["HAS_RESPONSE"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["RESPONSE_REFERENCES"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["REPRESENTS_MODEL"] == 6
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["RETURNS_MODEL"] == 1
    assert stats["semantic"]["metrics"]["edge_kind_counts"]["RETURNS_SCHEMA"] == 1
    assert stats["semantic"]["metrics"]["carry_through"] == {
        "raw_spec_count": 3,
        "task1_resolved_count": 1,
        "task1_failed_count": 2,
        "ast_graph_count": 1,
        "semantic_graph_count": 1,
        "resolved_to_ast_ratio": 1.0,
        "raw_to_semantic_ratio": 0.3333,
    }
    assert stats["semantic"]["metrics"]["coverage"]["endpoints_with_parameters"] == {
        "count": 1,
        "total": 1,
        "ratio": 1.0,
    }
    assert stats["semantic"]["metrics"]["coverage"]["endpoints_with_request_bodies"] == {
        "count": 1,
        "total": 1,
        "ratio": 1.0,
    }
    assert stats["semantic"]["metrics"]["coverage"]["endpoints_with_responses"] == {
        "count": 1,
        "total": 1,
        "ratio": 1.0,
    }
    assert stats["semantic"]["metrics"]["coverage"]["endpoints_returning_schema"] == {
        "count": 1,
        "total": 1,
        "ratio": 1.0,
    }
    assert stats["semantic"]["metrics"]["coverage"]["endpoints_with_any_model_edge"] == {
        "count": 1,
        "total": 1,
        "ratio": 1.0,
    }
    assert stats["semantic"]["metrics"]["coverage"]["schemas_representing_model"] == {
        "count": 5,
        "total": 5,
        "ratio": 1.0,
    }
    assert stats["semantic"]["metrics"]["coverage"]["semantic_nodes_with_ast_provenance"][
        "ratio"
    ] == 1.0
    assert stats["compiler_projection"]["enabled"] is True
    assert stats["compiler_projection"]["db_path"] == "knowledge_db_compiler"
    assert stats["compiler_projection"]["node_kind_counts"]["ApiEndpoint"] == 1
    assert stats["compiler_projection"]["node_kind_counts"]["SchemaComponent"] == 5
    assert stats["compiler_projection"]["node_kind_counts"]["Property"] == 1
    assert stats["compiler_projection"]["edge_kind_counts"]["HAS_REQUEST_BODY"] == 1
    assert stats["compiler_projection"]["edge_kind_counts"]["BODY_REFERENCES"] == 1
    assert stats["compiler_projection"]["provenance_count"] > 0
    assert set(stats["timings_seconds"]) == {
        "compile",
        "ast_write",
        "semantic_write",
        "projection_collect",
        "projection_write",
    }
    assert all(value >= 0 for value in stats["timings_seconds"].values())
    assert not (ast_db_path / "stale.txt").exists()
    assert not (compiler_db_path / "stale.txt").exists()
    json.dumps({"ast": stats})

    db = lb.Database(str(ast_db_path), max_db_size=256 * 1024 * 1024)
    conn = lb.Connection(db)
    try:
        root_rows = list(
            conn.execute(
                """
                MATCH (s:OasSpec)-[:HAS_AST_ROOT]->(root:OasAstNode)
                RETURN s.source AS source, root.kind AS root_kind
                """
            ).rows_as_dict()
        )
        assert root_rows == [{"source": "unit/pets", "root_kind": "Document"}]

        child_count = list(
            conn.execute("MATCH ()-[r:AST_CHILD]->() RETURN COUNT(r) AS n").rows_as_dict()
        )[0]["n"]
        assert child_count == stats["child_edge_count"]

        ref_rows = list(
            conn.execute(
                """
                MATCH (r:OasAstNode)-[edge:AST_REF_TARGET]->(target:OasAstNode)
                RETURN r.key AS key, edge.ref AS ref, target.kind AS target_kind
                """
            ).rows_as_dict()
        )
        assert ref_rows == [
            {
                "key": "$ref",
                "ref": "#/components/schemas/Pet",
                "target_kind": "Schema",
            },
            {
                "key": "$ref",
                "ref": "#/components/schemas/Pet",
                "target_kind": "Schema",
            },
        ]
        semantic_rows = list(
            conn.execute(
                """
                MATCH (e:SemanticNode {kind: 'ApiEndpoint'})
                      -[edge:SEMANTIC_EDGE]->(schema:SemanticNode {kind: 'SchemaComponent'})
                RETURN e.name AS endpoint, edge.kind AS edge_kind, schema.name AS schema
                """
            ).rows_as_dict()
        )
        assert {
            (row["endpoint"], row["edge_kind"], row["schema"])
            for row in semantic_rows
        } == {
            ("GET /pets", "ACCEPTS_SCHEMA", "Pet"),
            ("GET /pets", "RETURNS_SCHEMA", "Pet"),
        }
        traversal_rows = list(
            conn.execute(
                """
                MATCH (e:SemanticNode {kind: 'ApiEndpoint'})
                      -[:SEMANTIC_EDGE]->(body:SemanticNode {kind: 'RequestBody'})
                      -[edge:SEMANTIC_EDGE]->(schema:SemanticNode {kind: 'SchemaComponent'})
                WHERE edge.kind = 'BODY_REFERENCES'
                RETURN e.name AS endpoint, body.kind AS body_kind, schema.name AS schema
                """
            ).rows_as_dict()
        )
        assert traversal_rows == [
            {
                "endpoint": "GET /pets",
                "body_kind": "RequestBody",
                "schema": "Pet",
            }
        ]
        parameter_rows = list(
            conn.execute(
                """
                MATCH (e:SemanticNode {kind: 'ApiEndpoint'})
                      -[edge:SEMANTIC_EDGE]->(p:SemanticNode {kind: 'Parameter'})
                WHERE edge.kind = 'HAS_PARAMETER'
                RETURN e.name AS endpoint, p.name AS parameter
                """
            ).rows_as_dict()
        )
        assert parameter_rows == [{"endpoint": "GET /pets", "parameter": "limit"}]
        response_rows = list(
            conn.execute(
                """
                MATCH (e:SemanticNode {kind: 'ApiEndpoint'})
                      -[:SEMANTIC_EDGE]->(response:SemanticNode {kind: 'Response'})
                      -[edge:SEMANTIC_EDGE]->(schema:SemanticNode {kind: 'SchemaComponent'})
                WHERE edge.kind = 'RESPONSE_REFERENCES'
                RETURN e.name AS endpoint, response.kind AS response_kind, schema.name AS schema
                """
            ).rows_as_dict()
        )
        assert response_rows == [
            {
                "endpoint": "GET /pets",
                "response_kind": "Response",
                "schema": "Pet",
            }
        ]
        model_rows = list(
            conn.execute(
                """
                MATCH (e:SemanticNode {kind: 'ApiEndpoint'})
                      -[edge:SEMANTIC_EDGE]->(model:SemanticNode {kind: 'ModelEntity'})
                WHERE edge.kind IN ['ACCEPTS_MODEL', 'RETURNS_MODEL']
                RETURN e.name AS endpoint, edge.kind AS edge_kind, model.name AS model
                ORDER BY edge.kind
                """
            ).rows_as_dict()
        )
        assert model_rows == [
            {
                "endpoint": "GET /pets",
                "edge_kind": "ACCEPTS_MODEL",
                "model": "Pet",
            },
            {
                "endpoint": "GET /pets",
                "edge_kind": "RETURNS_MODEL",
                "model": "Pet",
            },
        ]
        semantic_node_count = list(
            conn.execute("MATCH (n:SemanticNode) RETURN COUNT(n) AS n").rows_as_dict()
        )[0]["n"]
        assert semantic_node_count == stats["semantic"]["node_count"]

        semantic_edge_count = list(
            conn.execute("MATCH ()-[r:SEMANTIC_EDGE]->() RETURN COUNT(r) AS n").rows_as_dict()
        )[0]["n"]
        assert semantic_edge_count == stats["semantic"]["edge_count"]

        provenance_rows = list(
            conn.execute(
                """
                MATCH (:SemanticNode)-[edge:SEMANTIC_DERIVED_FROM]->(:OasAstNode)
                RETURN COUNT(edge) AS n
                """
            ).rows_as_dict()
        )
        assert provenance_rows[0]["n"] == stats["semantic"]["derived_from_ast_edge_count"]
    finally:
        db.close()

    compiler_db = lb.Database(str(compiler_db_path), max_db_size=256 * 1024 * 1024)
    compiler_conn = lb.Connection(compiler_db)
    try:
        typed_rows = list(
            compiler_conn.execute(
                """
                MATCH (e:ApiEndpoint {method: 'GET', path: '/pets'})
                      -[:HAS_REQUEST_BODY]->(body:RequestBody)
                      -[:BODY_REFERENCES]->(schema:SchemaComponent)
                      -[:HAS_PROPERTY]->(prop:Property)
                RETURN e.endpoint_id AS endpoint,
                       body.content_type AS content_type,
                       schema.name AS schema,
                       prop.name AS property
                """
            ).rows_as_dict()
        )
        assert typed_rows == [
            {
                "endpoint": "GET:/pets",
                "content_type": "application/json",
                "schema": "Pet",
                "property": "id",
            }
        ]
        response_rows = list(
            compiler_conn.execute(
                """
                MATCH (:ApiEndpoint {method: 'GET', path: '/pets'})
                      -[:HAS_RESPONSE]->(response:Response)
                      -[:RESPONSE_REFERENCES]->(schema:SchemaComponent)
                RETURN response.status AS status,
                       response.content_type AS content_type,
                       schema.component_id AS component
                """
            ).rows_as_dict()
        )
        assert response_rows == [
            {
                "status": "200",
                "content_type": "application/json",
                "component": "unit:schemas:Pet",
            }
        ]
    finally:
        compiler_db.close()


def test_build_ast_artifact_fails_when_resolved_spec_cannot_build(
    repo_tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mod = _load_build_module()

    def fail_build(_resolved_spec):
        raise UnknownKeywordError(
            source="unit/pets",
            pointer="",
            parent_kind="Document",
            key="madeUpKeyword",
        )

    monkeypatch.setattr(mod, "build_ast_from_resolved", fail_build)

    with pytest.raises(UnknownKeywordError):
        mod._build_ast_artifact(
            repo_tmp_path / "knowledge_db_ast",
            [_resolved()],
            task1_failures=[],
        )


def test_release_archives_keep_ast_tar_separate(repo_tmp_path: Path) -> None:
    mod = _load_build_module()
    db_path = repo_tmp_path / "knowledge_db"
    ast_db_path = repo_tmp_path / "knowledge_db_ast"
    compiler_db_path = repo_tmp_path / "knowledge_db_compiler"
    db_path.mkdir()
    ast_db_path.mkdir()
    compiler_db_path.mkdir()
    (db_path / "db.lbd").write_text("runtime", encoding="utf-8")
    (ast_db_path / "db.lbd").write_text("ast", encoding="utf-8")
    (compiler_db_path / "db.lbd").write_text("compiler", encoding="utf-8")
    manifest_path = repo_tmp_path / "manifest.json"
    manifest_path.write_text('{"version": "unit"}', encoding="utf-8")

    archives = mod._create_release_archives(
        repo_tmp_path,
        db_path=db_path,
        ast_db_path=ast_db_path,
        compiler_projection_db_path=compiler_db_path,
        manifest_path=manifest_path,
    )

    assert archives["knowledge_db"].name == "knowledge_db.tar.gz"
    assert archives["knowledge_db_compiler"].name == "knowledge_db_compiler.tar.gz"
    assert archives["knowledge_db_ast"].name == "knowledge_db_ast.tar.gz"

    with tarfile.open(archives["knowledge_db"], "r:gz") as tf:
        names = tf.getnames()
    assert "knowledge_db/db.lbd" in names
    assert "manifest.json" in names
    assert all(not name.startswith("knowledge_db_ast/") for name in names)
    assert all(not name.startswith("knowledge_db_compiler/") for name in names)

    with tarfile.open(archives["knowledge_db_compiler"], "r:gz") as tf:
        compiler_names = tf.getnames()
    assert "knowledge_db_compiler/db.lbd" in compiler_names
    assert "manifest.json" not in compiler_names

    with tarfile.open(archives["knowledge_db_ast"], "r:gz") as tf:
        ast_names = tf.getnames()
    assert "knowledge_db_ast/db.lbd" in ast_names
    assert "manifest.json" not in ast_names


@pytest.mark.real_spec
def test_real_central_stride_builds_ast_artifact(
    repo_tmp_path: Path,
    real_central_specs: list[Path],
) -> None:
    mod = _load_build_module()
    resolved: list[ResolvedSpec] = []
    for path in real_central_specs[::64][:25]:
        outcome = resolve_spec(
            json.loads(path.read_text(encoding="utf-8")),
            source=f"central/{path.name}",
        )
        if isinstance(outcome, ResolvedSpec):
            resolved.append(outcome)
        if len(resolved) >= 3:
            break

    assert resolved, "expected at least one sampled real spec to resolve"
    stats = mod._build_ast_artifact(
        repo_tmp_path / "knowledge_db_ast",
        resolved,
        task1_failures=[],
    )
    assert stats["spec_count"] == len(resolved)
    assert stats["node_count"] > 0
    assert stats["child_edge_count"] > 0
    assert stats["semantic"]["node_count"] > 0
    assert stats["semantic"]["edge_count"] > 0
