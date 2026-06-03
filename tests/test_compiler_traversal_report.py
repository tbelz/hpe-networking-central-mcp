"""Tests for compiler traversal corpus reporting."""

from __future__ import annotations

from collections.abc import Iterator
import importlib.util
from pathlib import Path
import shutil
import sys
import tempfile

import pytest

from hpe_networking_central_mcp.compiler.ast_builder import build_ast_graph
from hpe_networking_central_mcp.compiler.ast_writer import build_ast_database
from hpe_networking_central_mcp.compiler.projection_writer import (
    build_compiler_projection_database,
)
from hpe_networking_central_mcp.compiler.semantic_builder import build_semantic_overlay
from hpe_networking_central_mcp.compiler.semantic_writer import write_semantic_database
from hpe_networking_central_mcp.compiler.traversal_report import (
    load_compiler_traversal_report,
)

pytestmark = [pytest.mark.compiler, pytest.mark.unit]


@pytest.fixture
def repo_tmp_path() -> Iterator[Path]:
    repo_tmp = Path(__file__).resolve().parent.parent / "tmp"
    repo_tmp.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="test_compiler_traversal_report_", dir=repo_tmp))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _build_artifacts(repo_tmp_path: Path) -> tuple[Path, Path]:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Traversal Report", "version": "1.0"},
        "paths": {
            "/pets/{id}": {
                "post": {
                    "operationId": "createPet",
                    "parameters": [
                        {
                            "name": "id",
                            "in": "path",
                            "required": True,
                            "schema": {"$ref": "#/components/schemas/PetId"},
                        }
                    ],
                    "requestBody": {
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/Pet"}
                            }
                        }
                    },
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/PetEnvelope"}
                                }
                            },
                        }
                    },
                }
            }
        },
        "components": {
            "schemas": {
                "PetId": {"type": "string"},
                "Pet": {
                    "type": "object",
                    "properties": {
                        "id": {"$ref": "#/components/schemas/PetId"},
                        "tags": {
                            "type": "array",
                            "items": {"$ref": "#/components/schemas/Tag"},
                        },
                    },
                },
                "PetEnvelope": {
                    "allOf": [
                        {"$ref": "#/components/schemas/Pet"},
                        {"type": "object", "properties": {"status": {"type": "string"}}},
                    ]
                },
                "Tag": {"type": "object", "properties": {"name": {"type": "string"}}},
                "TagAlias": {"$ref": "#/components/schemas/Tag"},
                "TagMap": {
                    "type": "object",
                    "additionalProperties": {"$ref": "#/components/schemas/Tag"},
                },
            }
        },
    }
    ast = build_ast_graph(spec, source="central/report")
    semantic = build_semantic_overlay(ast)
    ast_db_path = repo_tmp_path / "knowledge_db_ast"
    compiler_db_path = repo_tmp_path / "knowledge_db_compiler"
    build_ast_database(ast_db_path, [ast], buffer_pool_size=64 * 1024 * 1024)
    write_semantic_database(ast_db_path, [semantic], buffer_pool_size=64 * 1024 * 1024)
    build_compiler_projection_database(
        compiler_db_path,
        [ast],
        [semantic],
        buffer_pool_size=64 * 1024 * 1024,
    )
    return compiler_db_path, ast_db_path


def test_compiler_traversal_report_samples_artifacts(repo_tmp_path: Path) -> None:
    compiler_db_path, ast_db_path = _build_artifacts(repo_tmp_path)

    report = load_compiler_traversal_report(
        compiler_db_path=compiler_db_path,
        ast_db_path=ast_db_path,
        endpoint_limit=10,
        schema_limit=20,
        buffer_pool_size=64 * 1024 * 1024,
    )

    assert report["status"] == "ok"
    assert report["failure_count"] == 0
    assert report["totals"]["endpoints"] == 1
    assert report["totals"]["parameters"] == 1
    assert report["totals"]["request_bodies"] == 1
    assert report["totals"]["responses"] == 1
    assert report["sample"]["endpoint_count"] == 1
    assert report["endpoint_context"]["ok"] == 1
    assert report["endpoint_context"]["with_parameter_schema"] == 1
    assert report["endpoint_context"]["with_request_schema"] == 1
    assert report["endpoint_context"]["with_response_schema"] == 1
    assert report["schema_context"]["with_properties"] > 0
    assert report["schema_context"]["with_property_schema"] > 0
    assert report["schema_context"]["with_composition"] > 0
    assert report["schema_context"]["with_value_schemas"] > 0
    assert report["schema_context"]["with_references"] > 0


def test_report_script_rejects_missing_artifacts(
    repo_tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    repo_root = Path(__file__).resolve().parent.parent
    spec = importlib.util.spec_from_file_location(
        "report_compiler_traversal",
        repo_root / "scripts" / "report_compiler_traversal.py",
    )
    mod = importlib.util.module_from_spec(spec)  # type: ignore[arg-type]
    spec.loader.exec_module(mod)  # type: ignore[union-attr]
    monkeypatch.setattr(
        sys,
        "argv",
        [
            "report_compiler_traversal.py",
            "--output-dir",
            str(repo_tmp_path / "missing_build"),
        ],
    )

    assert mod.main() == 1
    assert "compiler DB not found" in capsys.readouterr().err
