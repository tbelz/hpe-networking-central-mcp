"""TDD tests for search tools — unified_search, search_related_apis, get_data_provenance.

Uses an in-memory LadybugDB database with a small fixture graph.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path

import pytest

# Ensure the src directory is importable
sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpe_networking_central_mcp.graph.manager import GraphManager
from hpe_networking_central_mcp.tools.search import (
    _fts_search,
    _contains_search,
    _unified_search_impl,
    _search_related_apis_impl,
    _get_data_provenance_impl,
)


# ── Fixtures ────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def gm(tmp_path_factory):
    """Create an in-memory GraphManager with a small fixture graph."""
    db_path = tmp_path_factory.mktemp("search_db") / "test.db"
    gm = GraphManager(db_path)
    gm.initialize()

    # ApiEndpoint nodes
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'GET:/monitoring/v1/devices',"
        "  method: 'GET', path: '/monitoring/v1/devices',"
        "  summary: 'List all devices in the network',"
        "  description: 'Returns a paginated list of all devices',"
        "  operationId: 'listDevices', category: 'monitoring',"
        "  deprecated: false, tags: ['devices'],"
        "  parameters: '[]', requestBody: '', responses: ''"
        "})"
    )
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'GET:/monitoring/v1/devices/{serial}',"
        "  method: 'GET', path: '/monitoring/v1/devices/{serial}',"
        "  summary: 'Get a specific device by serial number',"
        "  description: 'Retrieve detailed device information',"
        "  operationId: 'getDevice', category: 'monitoring',"
        "  deprecated: false, tags: ['devices'],"
        "  parameters: '[]', requestBody: '', responses: ''"
        "})"
    )
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'POST:/config/v1/sites',"
        "  method: 'POST', path: '/config/v1/sites',"
        "  summary: 'Create a new site',"
        "  description: 'Creates a site in the organization',"
        "  operationId: 'createSite', category: 'config',"
        "  deprecated: false, tags: ['sites'],"
        "  parameters: '[]', requestBody: '', responses: ''"
        "})"
    )
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'DELETE:/config/v1/sites/{id}',"
        "  method: 'DELETE', path: '/config/v1/sites/{id}',"
        "  summary: 'Delete a site',"
        "  description: 'Removes a site permanently',"
        "  operationId: 'deleteSite', category: 'config',"
        "  deprecated: false, tags: ['sites'],"
        "  parameters: '[]', requestBody: '', responses: ''"
        "})"
    )

    # EntityType nodes
    gm.execute(
        "CREATE (et:EntityType {"
        "  name: 'Device', graphNode: 'Device',"
        "  description: 'Network device', fields: '{}'"
        "})"
    )
    gm.execute(
        "CREATE (et:EntityType {"
        "  name: 'Site', graphNode: 'Site',"
        "  description: 'Physical site', fields: '{}'"
        "})"
    )

    # OPERATES_ON edges with operation
    gm.execute(
        "MATCH (e:ApiEndpoint {endpoint_id: 'GET:/monitoring/v1/devices'}), "
        "(et:EntityType {name: 'Device'}) "
        "CREATE (e)-[:OPERATES_ON {paramName: 'serial', fieldName: 'serial', "
        "confidence: 'exact', mapper: 'static', reason: 'test', operation: 'list'}]->(et)"
    )
    gm.execute(
        "MATCH (e:ApiEndpoint {endpoint_id: 'GET:/monitoring/v1/devices/{serial}'}), "
        "(et:EntityType {name: 'Device'}) "
        "CREATE (e)-[:OPERATES_ON {paramName: 'serial', fieldName: 'serial', "
        "confidence: 'exact', mapper: 'static', reason: 'test', operation: 'read'}]->(et)"
    )
    gm.execute(
        "MATCH (e:ApiEndpoint {endpoint_id: 'POST:/config/v1/sites'}), "
        "(et:EntityType {name: 'Site'}) "
        "CREATE (e)-[:OPERATES_ON {paramName: 'id', fieldName: 'scopeId', "
        "confidence: 'high', mapper: 'static', reason: 'test', operation: 'create'}]->(et)"
    )
    gm.execute(
        "MATCH (e:ApiEndpoint {endpoint_id: 'DELETE:/config/v1/sites/{id}'}), "
        "(et:EntityType {name: 'Site'}) "
        "CREATE (e)-[:OPERATES_ON {paramName: 'id', fieldName: 'scopeId', "
        "confidence: 'high', mapper: 'static', reason: 'test', operation: 'delete'}]->(et)"
    )

    # Domain nodes with provenance
    gm.execute(
        "CREATE (d:Device {"
        "  serial: 'SN001', name: 'Switch-01', model: 'Aruba 6300',"
        "  deviceType: 'SWITCH', status: 'Up', fetched_at: '2024-01-01T00:00:00Z',"
        "  source_api: 'GET:/monitoring/v1/devices'"
        "})"
    )
    gm.execute(
        "CREATE (s:Site {"
        "  scopeId: 'site-001', name: 'NYC Office', address: '123 Main St',"
        "  city: 'New York', country: 'US', fetched_at: '2024-01-01T00:00:00Z',"
        "  source_api: 'GET:/config/v1/sites'"
        "})"
    )

    # POPULATED_BY edges
    gm.execute(
        "MATCH (d:Device {serial: 'SN001'}), (e:ApiEndpoint {endpoint_id: 'GET:/monitoring/v1/devices'}) "
        "CREATE (d)-[:POPULATED_BY {fetched_at: '2024-01-01T00:00:00Z', seed: 'populate_base_graph', run_id: 'run-001'}]->(e)"
    )

    # DocSection node
    gm.execute(
        "CREATE (ds:DocSection {"
        "  section_id: 'doc-001', title: 'Getting Started with Devices',"
        "  content: 'This guide explains how to manage network devices in Aruba Central.',"
        "  source: 'user-guide', url: 'https://docs.example.com/devices'"
        "})"
    )

    # Script node
    gm.execute(
        "CREATE (s:Script {"
        "  filename: 'onboard_device.py', description: 'Onboard a new device to Aruba Central',"
        "  tags: ['onboarding', 'devices'], content: 'print(\"hello\")',"
        "  parameters: '[]'"
        "})"
    )

    # Rebuild FTS indexes after data insert
    gm.create_fts_indexes()

    return gm


# ── C1: unified_search ──────────────────────────────────────────────

class TestContainsSearch:
    """Test fallback CONTAINS-based search."""

    def test_search_api_by_keyword(self, gm):
        results = _contains_search(gm, "devices", scope="api", limit=10)
        assert len(results) >= 2
        assert all(r["type"] == "ApiEndpoint" for r in results)

    def test_search_data_nodes(self, gm):
        results = _contains_search(gm, "Switch", scope="data", limit=10)
        assert len(results) >= 1
        assert any(r["type"] == "Device" for r in results)

    def test_search_docs(self, gm):
        results = _contains_search(gm, "devices", scope="docs", limit=10)
        assert len(results) >= 1
        assert any(r["type"] == "DocSection" for r in results)

    def test_search_all_scopes(self, gm):
        results = _contains_search(gm, "devices", scope="all", limit=20)
        types = {r["type"] for r in results}
        # Should find across multiple node types
        assert len(results) >= 2

    def test_limit_respected(self, gm):
        results = _contains_search(gm, "devices", scope="all", limit=1)
        assert len(results) <= 1


class TestFtsSearch:
    """Test FTS/BM25-based search."""

    def test_fts_api_search(self, gm):
        if not gm.fts_available:
            pytest.skip("FTS extension not available")
        results = _fts_search(gm, "devices network", scope="api", limit=10)
        assert len(results) >= 1
        assert all(r["type"] == "ApiEndpoint" for r in results)

    def test_fts_returns_empty_for_no_match(self, gm):
        if not gm.fts_available:
            pytest.skip("FTS extension not available")
        results = _fts_search(gm, "xyznonexistent123", scope="api", limit=10)
        assert results == []


class TestUnifiedSearchImpl:
    """Test the unified search implementation."""

    def test_search_returns_json_with_results(self, gm):
        result = json.loads(_unified_search_impl(gm, "devices", scope="all", limit=10))
        assert "results" in result
        assert "total" in result
        assert result["total"] >= 1

    def test_scope_api_only(self, gm):
        result = json.loads(_unified_search_impl(gm, "devices", scope="api", limit=10))
        for r in result["results"]:
            assert r["type"] == "ApiEndpoint"

    def test_scope_data_only(self, gm):
        result = json.loads(_unified_search_impl(gm, "Switch", scope="data", limit=10))
        for r in result["results"]:
            assert r["type"] in ("Device", "Site", "ConfigProfile", "Script")

    def test_scope_docs_only(self, gm):
        result = json.loads(_unified_search_impl(gm, "devices", scope="docs", limit=10))
        for r in result["results"]:
            assert r["type"] == "DocSection"

    def test_empty_query_returns_error(self, gm):
        result = json.loads(_unified_search_impl(gm, "", scope="all", limit=10))
        assert "error" in result

    def test_invalid_scope_returns_error(self, gm):
        result = json.loads(_unified_search_impl(gm, "test", scope="invalid", limit=10))
        assert "error" in result


# ── C3: search_related_apis ────────────────────────────────────────

class TestSearchRelatedApis:
    """Test search_related_apis_impl."""

    def test_find_apis_for_device(self, gm):
        result = json.loads(_search_related_apis_impl(gm, "Device"))
        assert "apis" in result
        assert len(result["apis"]) >= 1
        assert any("devices" in a["path"] for a in result["apis"])

    def test_filter_by_operation(self, gm):
        result = json.loads(_search_related_apis_impl(gm, "Device", operation="read"))
        apis = result["apis"]
        assert len(apis) >= 1
        assert all(a["operation"] == "read" for a in apis)

    def test_filter_by_list_operation(self, gm):
        result = json.loads(_search_related_apis_impl(gm, "Device", operation="list"))
        apis = result["apis"]
        assert len(apis) >= 1

    def test_site_create_delete(self, gm):
        result = json.loads(_search_related_apis_impl(gm, "Site", operation="create"))
        assert len(result["apis"]) >= 1
        assert result["apis"][0]["method"] == "POST"

    def test_unknown_entity(self, gm):
        result = json.loads(_search_related_apis_impl(gm, "FooBar"))
        assert result["apis"] == []


# ── C4: get_data_provenance ─────────────────────────────────────────

class TestGetDataProvenance:
    """Test get_data_provenance_impl."""

    def test_device_provenance(self, gm):
        result = json.loads(_get_data_provenance_impl(gm, "Device", "SN001"))
        assert result["node_label"] == "Device"
        assert result["identifier"] == "SN001"
        assert result["source_api"] == "GET:/monitoring/v1/devices"
        assert result["fetched_at"] == "2024-01-01T00:00:00Z"

    def test_populated_by_edges(self, gm):
        result = json.loads(_get_data_provenance_impl(gm, "Device", "SN001"))
        assert "populated_by" in result
        assert len(result["populated_by"]) >= 1
        edge = result["populated_by"][0]
        assert edge["seed"] == "populate_base_graph"

    def test_type_level_provenance(self, gm):
        result = json.loads(_get_data_provenance_impl(gm, "Device", "SN001"))
        assert "related_apis" in result
        assert len(result["related_apis"]) >= 1

    def test_unknown_node(self, gm):
        result = json.loads(_get_data_provenance_impl(gm, "Device", "NONEXISTENT"))
        assert result["source_api"] is None
        assert result["populated_by"] == []

    def test_site_provenance(self, gm):
        result = json.loads(_get_data_provenance_impl(gm, "Site", "site-001"))
        assert result["source_api"] == "GET:/config/v1/sites"
