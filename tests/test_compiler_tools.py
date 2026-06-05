"""Tests for opt-in compiler-backed MCP tools."""

from __future__ import annotations

import json
from collections.abc import Iterator
from pathlib import Path
import shutil
import tempfile
from typing import Any

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from hpe_networking_central_mcp.compiler.ast_builder import build_ast_graph
from hpe_networking_central_mcp.compiler.ast_writer import build_ast_database
from hpe_networking_central_mcp.compiler.projection_writer import (
    build_compiler_projection_database,
)
from hpe_networking_central_mcp.compiler.semantic_builder import build_semantic_overlay
from hpe_networking_central_mcp.compiler.semantic_writer import write_semantic_database
from hpe_networking_central_mcp.config import Settings
from hpe_networking_central_mcp.tools.compiler import register_compiler_tools

pytestmark = [pytest.mark.compiler, pytest.mark.unit]

_TEST_DB_BUFFER_POOL_SIZE = 256 * 1024 * 1024


@pytest.fixture
def repo_tmp_path() -> Iterator[Path]:
    repo_tmp = Path(__file__).resolve().parent.parent / "tmp"
    repo_tmp.mkdir(exist_ok=True)
    path = Path(tempfile.mkdtemp(prefix="test_compiler_tools_", dir=repo_tmp))
    try:
        yield path
    finally:
        shutil.rmtree(path, ignore_errors=True)


def _build_tool_artifacts(repo_tmp_path: Path) -> tuple[Path, Path]:
    spec = {
        "openapi": "3.0.3",
        "info": {"title": "Compiler Tools", "version": "1.0"},
        "paths": {
            "/devices/{serial}": {
                "parameters": [
                    {
                        "name": "serial",
                        "in": "path",
                        "required": True,
                        "schema": {
                            "type": "string",
                            "pattern": "^[A-Z0-9]+$",
                            "x-parameter-source": "path-template",
                        },
                    }
                ],
                "post": {
                    "summary": "Create device",
                    "operationId": "createDevice",
                    "x-cliParam": {
                        "commandName": "device create",
                        "commandUse": "create a device",
                        "parentCommand": "device",
                        "paramKeys": [{"key": "serial"}],
                    },
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/DeviceCreate"}
                            }
                        },
                    },
                    "responses": {
                        "200": {
                            "description": "ok",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/Device"}
                                }
                            },
                        }
                    },
                },
            }
        },
        "components": {
            "schemas": {
                "DeviceCreate": {
                    "type": "object",
                    "x-path": "/aruba/device/create",
                    "required": ["serial"],
                    "properties": {
                        "serial": {
                            "type": "string",
                            "description": "device serial",
                            "minLength": 12,
                            "x-yang-hint": "/device/serial",
                        },
                        "tags": {
                            "type": "array",
                            "x-key": ["name"],
                            "items": {"$ref": "#/components/schemas/Tag"},
                        },
                    },
                },
                "Device": {
                    "type": "object",
                    "properties": {"id": {"type": "string"}},
                },
                "DeviceEnvelope": {
                    "allOf": [
                        {"$ref": "#/components/schemas/DeviceCreate"},
                        {
                            "type": "object",
                            "properties": {"createdAt": {"type": "string"}},
                        },
                    ]
                },
                "Tag": {
                    "type": "object",
                    "properties": {"name": {"type": "string"}},
                },
                "TagAlias": {"$ref": "#/components/schemas/Tag"},
                "TagMap": {
                    "type": "object",
                    "additionalProperties": {"$ref": "#/components/schemas/Tag"},
                },
            }
        },
    }
    ast = build_ast_graph(spec, source="central/compiler-tools")
    semantic = build_semantic_overlay(ast)
    ast_db_path = repo_tmp_path / "knowledge_db_ast"
    compiler_db_path = repo_tmp_path / "knowledge_db_compiler"
    build_ast_database(ast_db_path, [ast], buffer_pool_size=_TEST_DB_BUFFER_POOL_SIZE)
    write_semantic_database(ast_db_path, [semantic], buffer_pool_size=_TEST_DB_BUFFER_POOL_SIZE)
    build_compiler_projection_database(
        compiler_db_path,
        [ast],
        [semantic],
        buffer_pool_size=_TEST_DB_BUFFER_POOL_SIZE,
    )
    return compiler_db_path, ast_db_path


def _make_tools(compiler_db_path: Path, ast_db_path: Path) -> dict[str, Any]:
    mcp = FastMCP("test-compiler-tools")
    register_compiler_tools(
        mcp,
        Settings(
            compiler_tools=True,
            compiler_db_path=compiler_db_path,
            compiler_ast_db_path=ast_db_path,
        ),
    )
    return {tool.name: tool.fn for tool in mcp._tool_manager._tools.values()}


def _assert_no_raw_payloads(value: Any) -> None:
    if isinstance(value, list):
        for item in value:
            _assert_no_raw_payloads(item)
    elif isinstance(value, dict):
        assert "raw_openapi" not in value
        assert "rawJson" not in value
        assert "scalarJson" not in value
        for item in value.values():
            _assert_no_raw_payloads(item)


def test_find_api_endpoints_filters_and_limits(repo_tmp_path: Path) -> None:
    compiler_db_path, ast_db_path = _build_tool_artifacts(repo_tmp_path)
    tools = _make_tools(compiler_db_path, ast_db_path)

    parsed = json.loads(
        tools["find_api_endpoints"](
            query="Create",
            method="post",
            path_contains="/devices",
            limit=1,
        )
    )

    assert parsed["total"] == 1
    assert parsed["endpoints"][0]["method"] == "POST"
    assert parsed["endpoints"][0]["path"] == "/devices/{serial}"
    assert parsed["endpoints"][0]["operationId"] == "createDevice"


def test_endpoint_context_excludes_raw_by_default(repo_tmp_path: Path) -> None:
    compiler_db_path, ast_db_path = _build_tool_artifacts(repo_tmp_path)
    tools = _make_tools(compiler_db_path, ast_db_path)

    parsed = json.loads(
        tools["get_api_endpoint_context"](
            method="POST",
            path="/devices/{serial}",
        )
    )

    assert parsed["endpoint"]["projection_row"]["operationId"] == "createDevice"
    assert parsed["parameters"][0]["schema"]["projection_row"]["type"] == "string"
    assert parsed["request_bodies"][0]["schema"]["projection_row"]["component_id"] == (
        "central:schemas:DeviceCreate"
    )
    assert parsed["responses"][0]["schema"]["projection_row"]["component_id"] == (
        "central:schemas:Device"
    )
    _assert_no_raw_payloads(parsed)


def test_schema_context_includes_item_composition_value_and_reference_edges(
    repo_tmp_path: Path,
) -> None:
    compiler_db_path, ast_db_path = _build_tool_artifacts(repo_tmp_path)
    tools = _make_tools(compiler_db_path, ast_db_path)

    create = json.loads(
        tools["get_api_schema_context"](component_id="central:schemas:DeviceCreate")
    )
    props = {prop["projection_row"]["name"]: prop for prop in create["properties"]}
    assert props["tags"]["item_schema"]["projection_row"]["component_id"] == (
        "central:schemas:Tag"
    )

    envelope = json.loads(
        tools["get_api_schema_context"](component_id="central:schemas:DeviceEnvelope")
    )
    assert [entry["kind"] for entry in envelope["composition"]] == ["allOf", "allOf"]

    tag_map = json.loads(
        tools["get_api_schema_context"](component_id="central:schemas:TagMap")
    )
    assert tag_map["value_schemas"][0]["projection_row"]["component_id"] == (
        "central:schemas:Tag"
    )

    tag_alias = json.loads(
        tools["get_api_schema_context"](component_id="central:schemas:TagAlias")
    )
    assert tag_alias["references"][0]["schema"]["projection_row"]["component_id"] == (
        "central:schemas:Tag"
    )


def test_source_detail_recovers_unprojected_vendor_extension(
    repo_tmp_path: Path,
) -> None:
    compiler_db_path, ast_db_path = _build_tool_artifacts(repo_tmp_path)
    tools = _make_tools(compiler_db_path, ast_db_path)

    parsed = json.loads(
        tools["get_openapi_source_detail"](
            table_name="Property",
            row_id="central:schemas:DeviceCreate#prop:serial",
        )
    )

    assert parsed["projection_row"]["name"] == "serial"
    assert parsed["raw_openapi"]["x-yang-hint"] == "/device/serial"
    assert parsed["provenance"]["ingestion_status"] == "strict_valid"


def test_compiler_graph_health_reports_clean_synthetic_corpus(
    repo_tmp_path: Path,
) -> None:
    compiler_db_path, ast_db_path = _build_tool_artifacts(repo_tmp_path)
    tools = _make_tools(compiler_db_path, ast_db_path)

    parsed = json.loads(
        tools["get_compiler_graph_health"](endpoint_limit=10, schema_limit=10)
    )

    assert parsed["status"] == "ok"
    assert parsed["failure_count"] == 0
    assert parsed["totals"]["endpoints"] == 1
    assert parsed["totals"]["item_schema_edges"] >= 1


def test_missing_compiler_artifacts_raise_clear_tool_error(tmp_path: Path) -> None:
    tools = _make_tools(tmp_path / "missing_compiler", tmp_path / "missing_ast")

    with pytest.raises(ToolError, match="Compiler artifacts are unavailable"):
        tools["find_api_endpoints"]()


def test_response_cap_redacts_raw_payloads(repo_tmp_path: Path, monkeypatch) -> None:
    compiler_db_path, ast_db_path = _build_tool_artifacts(repo_tmp_path)
    tools = _make_tools(compiler_db_path, ast_db_path)
    monkeypatch.setenv("MCP_COMPILER_RESPONSE_BYTES", "600")

    parsed = json.loads(
        tools["get_openapi_source_detail"](
            table_name="ApiEndpoint",
            row_id="POST:/devices/{serial}",
        )
    )

    assert parsed["_truncated"] is True
    assert parsed["reason"] == "compiler_response_byte_cap"
