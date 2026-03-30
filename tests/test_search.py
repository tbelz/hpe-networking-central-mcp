"""TDD tests for search tools — unified_search.

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
    fts_search,
    contains_search,
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

    # Domain nodes
    gm.execute(
        "CREATE (d:Device {"
        "  serial: 'SN001', name: 'Switch-01', model: 'Aruba 6300',"
        "  deviceType: 'SWITCH', status: 'Up'"
        "})"
    )
    gm.execute(
        "CREATE (s:Site {"
        "  scopeId: 'site-001', name: 'NYC Office', address: '123 Main St',"
        "  city: 'New York', country: 'US'"
        "})"
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
        results = contains_search(gm, "devices", scope="api", limit=10)
        assert len(results) >= 2
        assert all(r["type"] == "ApiEndpoint" for r in results)

    def test_search_data_nodes(self, gm):
        results = contains_search(gm, "Switch", scope="data", limit=10)
        assert len(results) >= 1
        assert any(r["type"] == "Device" for r in results)

    def test_search_docs(self, gm):
        results = contains_search(gm, "devices", scope="docs", limit=10)
        assert len(results) >= 1
        assert any(r["type"] == "DocSection" for r in results)

    def test_search_all_scopes(self, gm):
        results = contains_search(gm, "devices", scope="all", limit=20)
        types = {r["type"] for r in results}
        # Should find across multiple node types
        assert len(results) >= 2

    def test_limit_respected(self, gm):
        results = contains_search(gm, "devices", scope="all", limit=1)
        assert len(results) <= 1


class TestFtsSearch:
    """Test FTS/BM25-based search."""

    def test_fts_api_search(self, gm):
        if not gm.fts_available:
            pytest.skip("FTS extension not available")
        results = fts_search(gm, "devices network", scope="api", limit=10)
        assert len(results) >= 1
        assert all(r["type"] == "ApiEndpoint" for r in results)

    def test_fts_returns_empty_for_no_match(self, gm):
        if not gm.fts_available:
            pytest.skip("FTS extension not available")
        results = fts_search(gm, "xyznonexistent123", scope="api", limit=10)
        assert results == []
