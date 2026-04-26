"""Unit tests for API catalog tools — only ``list_api`` remains after
Phase 2E retirement of get_api_endpoint_detail / glossary / schema_component.

Uses an in-memory LadybugDB graph with minimal fixture data."""

from __future__ import annotations

import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpe_networking_central_mcp.config import Settings
from hpe_networking_central_mcp.graph.manager import GraphManager


@pytest.fixture(scope="module")
def gm(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("catalog_db") / "test.db"
    g = GraphManager(db_path)
    g.initialize()

    def _create(method: str, path: str, category: str) -> None:
        g.execute(
            "CREATE (e:ApiEndpoint {"
            "  endpoint_id: $eid, method: $m, path: $p,"
            "  summary: '', description: '', operationId: '',"
            "  category: $cat, deprecated: false,"
            "  parameters: '', requestBody: '', responses: ''"
            "})",
            {"eid": f"{method}:{path}", "m": method, "p": path, "cat": category},
        )

    _create("GET", "/monitoring/v2/aps", "monitoring")
    _create("GET", "/monitoring/v2/switches", "monitoring")
    _create("POST", "/config/v1/vlans", "config")
    _create("DELETE", "/config/v1/vlans/{id}", "config")
    return g


@pytest.fixture(scope="module")
def settings(tmp_path_factory):
    return Settings(
        central_base_url="https://test.example.com",
        central_client_id="cid",
        central_client_secret="csec",
        script_library_path=tmp_path_factory.mktemp("lib"),
    )


@pytest.fixture(scope="module")
def tools(gm, settings):
    from mcp.server.fastmcp import FastMCP

    from hpe_networking_central_mcp.tools.api_catalog import register_catalog_tools

    mcp = FastMCP("test")
    register_catalog_tools(mcp, settings, gm)
    return {t.name: t.fn for t in mcp._tool_manager._tools.values()}


class TestListApi:

    def test_returns_path_tree_text(self, tools):
        text = tools["list_api"]()
        assert isinstance(text, str)
        assert "API Endpoint Catalog" in text

    def test_lists_methods(self, tools):
        text = tools["list_api"]()
        assert "[GET" in text or "[POST" in text


class TestRetiredToolsAbsent:
    """Phase 2E-4: detail/glossary/component tools must no longer be registered."""

    @pytest.mark.parametrize("tool_name", [
        "get_api_endpoint_detail",
        "get_api_endpoint_glossary",
        "get_schema_component",
        "unified_search",
    ])
    def test_tool_not_registered(self, tools, tool_name):
        assert tool_name not in tools
