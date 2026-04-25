"""Unit tests for API catalog tools — unified_search, list_api_categories,
get_api_endpoint_detail.

Uses an in-memory LadybugDB graph with fixture data (no live API calls).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpe_networking_central_mcp.graph.manager import GraphManager
from hpe_networking_central_mcp.config import Settings


# ── Fixtures ────────────────────────────────────────────────────────


@pytest.fixture(scope="module")
def gm(tmp_path_factory):
    """Create an in-memory GraphManager with API endpoint fixtures."""
    db_path = tmp_path_factory.mktemp("catalog_db") / "test.db"
    gm = GraphManager(db_path)
    gm.initialize()

    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'GET:/monitoring/v2/aps',"
        "  method: 'GET', path: '/monitoring/v2/aps',"
        "  summary: 'List all access points',"
        "  description: 'Paginated list of APs',"
        "  operationId: 'listAPs', category: 'monitoring',"
        "  deprecated: false, tags: ['aps'],"
        "  parameters: '[{\"name\":\"site\",\"in\":\"query\"}]',"
        "  requestBody: '', responses: ''"
        "})"
    )
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'GET:/monitoring/v2/switches',"
        "  method: 'GET', path: '/monitoring/v2/switches',"
        "  summary: 'List all switches',"
        "  description: 'Paginated list of switches',"
        "  operationId: 'listSwitches', category: 'monitoring',"
        "  deprecated: false, tags: ['switches'],"
        "  parameters: '[]', requestBody: '', responses: ''"
        "})"
    )
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'POST:/config/v1/vlans',"
        "  method: 'POST', path: '/config/v1/vlans',"
        "  summary: 'Create a VLAN',"
        "  description: 'Creates a VLAN on a device',"
        "  operationId: 'createVLAN', category: 'config',"
        "  deprecated: false, tags: ['vlans'],"
        "  parameters: '[]',"
        "  requestBody: '{\"type\":\"object\",\"properties\":{\"vlan_id\":{\"type\":\"integer\"}}}',"
        "  responses: '{\"201\":{\"description\":\"Created\"}}'"
        "})"
    )
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'DELETE:/config/v1/vlans/{id}',"
        "  method: 'DELETE', path: '/config/v1/vlans/{id}',"
        "  summary: 'Delete a VLAN',"
        "  description: 'Removes a VLAN',"
        "  operationId: 'deleteVLAN', category: 'config',"
        "  deprecated: false, tags: ['vlans'],"
        "  parameters: '[]', requestBody: '', responses: ''"
        "})"
    )

    gm.create_fts_indexes()
    return gm


@pytest.fixture(scope="module")
def settings(tmp_path_factory):
    """Minimal Settings for tool registration."""
    return Settings(
        central_base_url="https://test.example.com",
        central_client_id="cid",
        central_client_secret="csec",
        script_library_path=tmp_path_factory.mktemp("lib"),
    )


@pytest.fixture(scope="module")
def tools(gm, settings):
    """Register catalog tools and return a dict of tool functions."""
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("test")
    from hpe_networking_central_mcp.tools.api_catalog import register_catalog_tools

    register_catalog_tools(mcp, settings, gm)

    # Extract registered tool functions from FastMCP internals
    tool_map = {}
    for tool in mcp._tool_manager._tools.values():
        tool_map[tool.name] = tool.fn

    return tool_map


# ── unified_search ──────────────────────────────────────────────────


class TestUnifiedSearch:
    """Test the unified_search tool function."""

    def test_search_api_by_path_keyword(self, tools):
        result = json.loads(tools["unified_search"](query="vlans"))
        assert result["returned_count"] >= 1
        paths = [e["path"] for e in result["endpoints"]]
        assert any("vlans" in p for p in paths)

    def test_search_api_by_summary_keyword(self, tools):
        result = json.loads(tools["unified_search"](query="access points"))
        assert result["returned_count"] >= 1

    def test_search_api_by_category(self, tools):
        result = json.loads(tools["unified_search"](query="list", category="monitoring"))
        endpoints = result["endpoints"]
        assert all(e["category"] == "monitoring" for e in endpoints)

    def test_search_api_groups_methods(self, tools):
        result = json.loads(tools["unified_search"](query="vlans"))
        vlan_endpoints = [e for e in result["endpoints"] if "/vlans" in e["path"] and "{id}" not in e["path"]]
        # POST:/config/v1/vlans should appear; if both POST and GET existed they'd be grouped
        assert len(vlan_endpoints) >= 1

    def test_search_api_no_results(self, tools):
        result = json.loads(tools["unified_search"](query="xyznonexistent999"))
        assert result["returned_count"] == 0
        assert "hint" in result

    def test_search_empty_query(self, tools):
        result = json.loads(tools["unified_search"](query=""))
        assert "error" in result

    def test_search_invalid_scope(self, tools):
        result = json.loads(tools["unified_search"](query="test", scope="invalid"))
        assert "error" in result

    def test_search_data_scope(self, gm, tools):
        # Add a device to search for
        gm.execute(
            "CREATE (d:Device {"
            "  serial: 'CAT-SN001', name: 'Cat-Switch-01', model: 'Aruba 6300',"
            "  deviceType: 'SWITCH', status: 'Up'"
            "})"
        )
        gm.create_fts_indexes()
        result = json.loads(tools["unified_search"](query="Cat-Switch", scope="data"))
        assert result["total"] >= 1

    def test_search_limit_clamp(self, tools):
        # Limit > 50 should be clamped to 50
        result = json.loads(tools["unified_search"](query="monitoring", limit=100))
        # No error — limit was clamped silently
        assert "error" not in result


# ── list_api_categories ─────────────────────────────────────────────


class TestListApiCategories:

    def test_returns_categories(self, tools):
        result = json.loads(tools["list_api_categories"]())
        cats = result["categories"]
        assert "monitoring" in cats
        assert "config" in cats
        assert result["total_endpoints"] >= 4

    def test_counts_are_positive(self, tools):
        result = json.loads(tools["list_api_categories"]())
        for cat, count in result["categories"].items():
            assert count > 0


# ── get_api_endpoint_detail ──────────────────────────────────────────


class TestGetApiEndpointDetail:

    def test_returns_full_detail(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](method="POST", path="/config/v1/vlans"))
        assert result["method"] == "POST"
        assert result["path"] == "/config/v1/vlans"
        assert result["operation_id"] == "createVLAN"
        assert "request_body" in result
        assert result["request_body"]["type"] == "object"

    def test_parameters_parsed(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](method="GET", path="/monitoring/v2/aps"))
        assert isinstance(result["parameters"], list)
        assert len(result["parameters"]) >= 1

    def test_not_found(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](method="GET", path="/nonexistent"))
        assert "error" in result
        assert "hint" in result

    def test_case_insensitive_method(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](method="get", path="/monitoring/v2/aps"))
        assert result["method"] == "GET"


# ── Graph unavailable ────────────────────────────────────────────────


class TestGraphUnavailable:
    """Test graceful degradation when graph is not available."""

    def test_unified_search_no_graph(self, settings):
        from mcp.server.fastmcp import FastMCP
        from hpe_networking_central_mcp.tools import api_catalog

        # Temporarily set graph manager to None
        original = api_catalog._graph_manager
        api_catalog._graph_manager = None

        mcp = FastMCP("test2")
        api_catalog.register_catalog_tools(mcp, settings, MagicMock(is_available=False))

        # Restore and test
        api_catalog._graph_manager = None
        try:
            tool_fn = None
            for t in mcp._tool_manager._tools.values():
                if t.name == "unified_search":
                    tool_fn = t.fn
                    break
            result = json.loads(tool_fn(query="test"))
            assert "error" in result
        finally:
            api_catalog._graph_manager = original


# ── Deprecation warning on unified_search(scope="api") ──────────────


class TestUnifiedSearchDeprecation:

    def test_api_scope_returns_deprecation_warning(self, tools):
        result = json.loads(tools["unified_search"](query="vlans"))
        assert "deprecation_warning" in result
        assert "deprecated" in result["deprecation_warning"].lower()
        assert "get_api_endpoint_detail" in result["deprecation_warning"]

    def test_api_scope_zero_results_still_includes_deprecation_warning(self, tools):
        result = json.loads(tools["unified_search"](query="xyznonexistent999"))
        assert result["returned_count"] == 0
        assert "deprecation_warning" in result
        assert "get_api_endpoint_detail" in result["deprecation_warning"]

    def test_data_scope_has_no_deprecation_warning(self, gm, tools):
        gm.execute(
            "CREATE (d:Device {"
            "  serial: 'DEPR-001', name: 'Depr-Switch', model: 'X',"
            "  deviceType: 'SWITCH', status: 'Up'"
            "})"
        )
        gm.create_fts_indexes()
        result = json.loads(tools["unified_search"](query="Depr-Switch", scope="data"))
        assert "deprecation_warning" not in result


# ── Bulk get_api_endpoint_detail ────────────────────────────────────


class TestBulkGetApiEndpointDetail:

    def test_bulk_returns_multiple_endpoints(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            endpoints=[
                {"method": "GET", "path": "/monitoring/v2/aps"},
                {"method": "POST", "path": "/config/v1/vlans"},
            ]
        ))
        assert "endpoints" in result
        assert len(result["endpoints"]) == 2
        paths = {ep["path"] for ep in result["endpoints"]}
        assert paths == {"/monitoring/v2/aps", "/config/v1/vlans"}
        assert result["missing"] == []

    def test_bulk_reports_missing_endpoints(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            endpoints=[
                {"method": "GET", "path": "/monitoring/v2/aps"},
                {"method": "GET", "path": "/does/not/exist"},
            ]
        ))
        assert len(result["endpoints"]) == 1
        assert result["endpoints"][0]["path"] == "/monitoring/v2/aps"
        assert result["missing"] == [{"method": "GET", "path": "/does/not/exist"}]

    def test_bulk_preserves_request_order(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            endpoints=[
                {"method": "POST", "path": "/config/v1/vlans"},
                {"method": "GET", "path": "/monitoring/v2/aps"},
                {"method": "GET", "path": "/monitoring/v2/switches"},
            ]
        ))
        ordered_paths = [ep["path"] for ep in result["endpoints"]]
        assert ordered_paths == [
            "/config/v1/vlans",
            "/monitoring/v2/aps",
            "/monitoring/v2/switches",
        ]

    def test_bulk_empty_list_errors(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](endpoints=[]))
        assert "error" in result

    def test_bulk_invalid_entry_errors(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            endpoints=[{"method": "GET"}]  # missing path
        ))
        assert "error" in result

    def test_no_args_errors(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"]())
        assert "error" in result

    def test_single_form_still_works(self, tools):
        # Phase 5 must preserve legacy single-call behavior
        result = json.loads(tools["get_api_endpoint_detail"](
            method="GET", path="/monitoring/v2/aps"
        ))
        assert result["method"] == "GET"
        assert result["path"] == "/monitoring/v2/aps"
        # Legacy shape — no top-level "endpoints" wrapper
        assert "endpoints" not in result


# ── Bulk + READ_ONLY interaction ────────────────────────────────────


class TestBulkReadOnly:

    def _make_tools_ro(self, gm, tmp_path_factory):
        from mcp.server.fastmcp import FastMCP
        from hpe_networking_central_mcp.tools.api_catalog import register_catalog_tools

        s = Settings(
            central_base_url="https://x",
            central_client_id="cid",
            central_client_secret="csec",
            script_library_path=tmp_path_factory.mktemp("lib_ro"),
            read_only=True,
        )
        mcp_ro = FastMCP("test-ro-bulk")
        register_catalog_tools(mcp_ro, s, gm)
        return {t.name: t.fn for t in mcp_ro._tool_manager._tools.values()}

    def test_bulk_skips_non_get_in_readonly(self, gm, tmp_path_factory):
        tools_ro = self._make_tools_ro(gm, tmp_path_factory)
        result = json.loads(tools_ro["get_api_endpoint_detail"](
            endpoints=[
                {"method": "GET", "path": "/monitoring/v2/aps"},
                {"method": "POST", "path": "/config/v1/vlans"},
                {"method": "DELETE", "path": "/config/v1/vlans/{id}"},
            ]
        ))
        # GET endpoint returned, others reported as skipped
        assert len(result["endpoints"]) == 1
        assert result["endpoints"][0]["method"] == "GET"
        skipped = result.get("skipped_read_only", [])
        assert {(s["method"], s["path"]) for s in skipped} == {
            ("POST", "/config/v1/vlans"),
            ("DELETE", "/config/v1/vlans/{id}"),
        }

    def test_bulk_all_blocked_returns_empty_with_skipped(self, gm, tmp_path_factory):
        tools_ro = self._make_tools_ro(gm, tmp_path_factory)
        result = json.loads(tools_ro["get_api_endpoint_detail"](
            endpoints=[{"method": "POST", "path": "/config/v1/vlans"}]
        ))
        assert result["endpoints"] == []
        assert len(result["skipped_read_only"]) == 1
