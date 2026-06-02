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
from hpe_networking_central_mcp.compiler.frontend import ResolvedSpec, resolve_spec

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


def test_build_ast_artifact_writes_queryable_ladybug_db(repo_tmp_path: Path) -> None:
    mod = _load_build_module()
    ast_db_path = repo_tmp_path / "knowledge_db_ast"
    ast_db_path.mkdir()
    (ast_db_path / "stale.txt").write_text("old", encoding="utf-8")

    stats = mod._build_ast_artifact(
        ast_db_path,
        [_resolved()],
        task1_failed_count=2,
    )

    assert stats["enabled"] is True
    assert stats["db_path"] == "knowledge_db_ast"
    assert stats["spec_count"] == 1
    assert stats["task1_failed_count"] == 2
    assert stats["node_count"] > 0
    assert stats["child_edge_count"] > 0
    assert stats["ref_edge_count"] == 1
    assert not (ast_db_path / "stale.txt").exists()
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
            }
        ]
    finally:
        db.close()


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
            task1_failed_count=0,
        )


def test_release_archives_keep_ast_tar_separate(repo_tmp_path: Path) -> None:
    mod = _load_build_module()
    db_path = repo_tmp_path / "knowledge_db"
    ast_db_path = repo_tmp_path / "knowledge_db_ast"
    db_path.mkdir()
    ast_db_path.mkdir()
    (db_path / "db.lbd").write_text("runtime", encoding="utf-8")
    (ast_db_path / "db.lbd").write_text("ast", encoding="utf-8")
    manifest_path = repo_tmp_path / "manifest.json"
    manifest_path.write_text('{"version": "unit"}', encoding="utf-8")

    archives = mod._create_release_archives(
        repo_tmp_path,
        db_path=db_path,
        ast_db_path=ast_db_path,
        manifest_path=manifest_path,
    )

    assert archives["knowledge_db"].name == "knowledge_db.tar.gz"
    assert archives["knowledge_db_ast"].name == "knowledge_db_ast.tar.gz"

    with tarfile.open(archives["knowledge_db"], "r:gz") as tf:
        names = tf.getnames()
    assert "knowledge_db/db.lbd" in names
    assert "manifest.json" in names
    assert all(not name.startswith("knowledge_db_ast/") for name in names)

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
        task1_failed_count=0,
    )
    assert stats["spec_count"] == len(resolved)
    assert stats["node_count"] > 0
    assert stats["child_edge_count"] > 0
