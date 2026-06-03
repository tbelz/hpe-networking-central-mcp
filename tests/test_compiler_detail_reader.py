"""Tests for resolving compiler projection rows back to L2/L1 source detail."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
import shutil
import tempfile

import pytest

from hpe_networking_central_mcp.compiler.ast_builder import build_ast_graph
from hpe_networking_central_mcp.compiler.ast_writer import build_ast_database
from hpe_networking_central_mcp.compiler.detail_reader import (
    ProjectionRowNotFoundError,
    UnknownProjectionTableError,
    load_projection_detail,
)
from hpe_networking_central_mcp.compiler.projection_writer import (
    build_compiler_projection_database,
)
from hpe_networking_central_mcp.compiler.semantic_builder import build_semantic_overlay
from hpe_networking_central_mcp.compiler.semantic_writer import write_semantic_database

pytestmark = [pytest.mark.compiler, pytest.mark.unit]


@pytest.fixture
def repo_tmp_path() -> Iterator[Path]:
    repo_tmp = Path(__file__).resolve().parent.parent / "tmp"
    repo_tmp.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="test_compiler_detail_", dir=repo_tmp))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def test_projection_detail_recovers_raw_openapi_not_in_typed_projection(
    repo_tmp_path: Path,
) -> None:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Detail", "version": "1.0"},
        "paths": {
            "/devices": {
                "post": {
                    "operationId": "createDevice",
                    "x-cliCommand": {
                        "commandName": "device create",
                        "commandUse": "create a device",
                    },
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/DeviceCreate"}
                            }
                        },
                    },
                    "responses": {"200": {"description": "ok"}},
                }
            }
        },
        "components": {
            "headers": {
                "X-Central-Trace": {
                    "description": "trace",
                    "required": False,
                    "schema": {"type": "string"},
                    "x-release-stage": "private-preview",
                }
            },
            "schemas": {
                "DeviceCreate": {
                    "type": "object",
                    "required": ["serial"],
                    "properties": {
                        "serial": {
                            "type": "string",
                            "description": "device serial",
                            "minLength": 12,
                            "x-yang-hint": "/device/serial",
                        }
                    },
                }
            },
        },
    }
    ast = build_ast_graph(spec, source="central/detail")
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

    property_detail = load_projection_detail(
        compiler_db_path=compiler_db_path,
        ast_db_path=ast_db_path,
        table_name="Property",
        row_id="central:schemas:DeviceCreate#prop:serial",
        buffer_pool_size=64 * 1024 * 1024,
    )

    assert property_detail["projection_row"]["name"] == "serial"
    assert property_detail["semantic_node"]["kind"] == "Property"
    assert property_detail["semantic_summary"]["xExtensions"] == {
        "x-yang-hint": "/device/serial"
    }
    assert property_detail["raw_openapi"] == {
        "description": "device serial",
        "minLength": 12,
        "type": "string",
        "x-yang-hint": "/device/serial",
    }
    assert property_detail["provenance"]["json_pointer"] == (
        "/components/schemas/DeviceCreate/properties/serial"
    )

    header_detail = load_projection_detail(
        compiler_db_path=compiler_db_path,
        ast_db_path=ast_db_path,
        table_name="SchemaComponent",
        row_id="central:headers:X-Central-Trace",
        buffer_pool_size=64 * 1024 * 1024,
    )

    assert header_detail["semantic_node"] is None
    assert header_detail["raw_openapi"]["x-release-stage"] == "private-preview"
    assert header_detail["raw_openapi"]["schema"] == {"type": "string"}


def test_projection_detail_rejects_unknown_table(repo_tmp_path: Path) -> None:
    with pytest.raises(UnknownProjectionTableError):
        load_projection_detail(
            compiler_db_path=repo_tmp_path / "missing_compiler",
            ast_db_path=repo_tmp_path / "missing_ast",
            table_name="NotATable",
            row_id="row",
            buffer_pool_size=64 * 1024 * 1024,
        )


def test_projection_detail_raises_when_row_is_missing(repo_tmp_path: Path) -> None:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Missing", "version": "1.0"},
        "paths": {"/ping": {"get": {"responses": {"200": {"description": "ok"}}}}},
    }
    ast = build_ast_graph(spec, source="central/missing")
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

    with pytest.raises(ProjectionRowNotFoundError):
        load_projection_detail(
            compiler_db_path=compiler_db_path,
            ast_db_path=ast_db_path,
            table_name="ApiEndpoint",
            row_id="GET:/missing",
            buffer_pool_size=64 * 1024 * 1024,
        )
