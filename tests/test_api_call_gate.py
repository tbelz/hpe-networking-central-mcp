"""Tests for the API call policy gate (``tools.api_call_policy``).

The gate refuses ``call_central_api`` / ``call_greenlake_api`` invocations
for endpoints whose schema has not been inspected via
``get_api_endpoint_detail`` or ``get_api_endpoint_glossary`` in the same
session.
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from hpe_networking_central_mcp.tools.api_call_policy import (
    InspectionTracker,
    check_call_policy,
    get_tracker,
    normalise_path,
)


@pytest.fixture(autouse=True)
def _reset_tracker():
    """Each test starts with an empty inspection tracker."""
    get_tracker().reset()
    yield
    get_tracker().reset()


# ── InspectionTracker primitives ────────────────────────────────────


class TestInspectionTracker:

    def test_unrecorded_endpoint_is_not_inspected(self):
        t = InspectionTracker()
        assert t.was_inspected("GET", "/foo") is False

    def test_record_skeleton_satisfies_default_check(self):
        t = InspectionTracker()
        t.record("GET", "/foo", "skeleton")
        assert t.was_inspected("GET", "/foo") is True

    def test_record_glossary_satisfies_default_check(self):
        t = InspectionTracker()
        t.record("GET", "/foo", "glossary")
        assert t.was_inspected("GET", "/foo") is True

    def test_skeleton_does_not_satisfy_glossary_only_predicate(self):
        # Future-proofing: a stricter policy may require glossary specifically.
        t = InspectionTracker()
        t.record("GET", "/foo", "skeleton")
        assert t.was_inspected("GET", "/foo", kinds=("glossary",)) is False
        assert t.was_inspected("GET", "/foo", kinds=("skeleton",)) is True

    def test_method_is_case_insensitive(self):
        t = InspectionTracker()
        t.record("get", "/foo", "skeleton")
        assert t.was_inspected("GET", "/foo") is True

    def test_path_normalisation_with_and_without_leading_slash(self):
        t = InspectionTracker()
        t.record("GET", "monitoring/v2/aps", "skeleton")
        assert t.was_inspected("GET", "/monitoring/v2/aps") is True
        assert t.was_inspected("GET", "monitoring/v2/aps") is True

    def test_reset_clears_records(self):
        t = InspectionTracker()
        t.record("GET", "/foo", "skeleton")
        t.reset()
        assert t.was_inspected("GET", "/foo") is False


class TestNormalisePath:

    def test_adds_leading_slash(self):
        assert normalise_path("monitoring/v2/aps") == "/monitoring/v2/aps"

    def test_keeps_leading_slash(self):
        assert normalise_path("/monitoring/v2/aps") == "/monitoring/v2/aps"

    def test_strips_whitespace(self):
        assert normalise_path("  /foo  ") == "/foo"


# ── check_call_policy ────────────────────────────────────────────────


class TestCheckCallPolicy:

    def test_blocked_when_endpoint_not_inspected(self):
        allowed, reason = check_call_policy("GET", "/monitoring/v2/aps")
        assert allowed is False
        assert reason is not None
        assert "get_api_endpoint_detail" in reason
        assert "/monitoring/v2/aps" in reason
        # Hint must mention glossary as the alternative satisfier.
        assert "get_api_endpoint_glossary" in reason

    def test_allowed_after_skeleton_inspection(self):
        get_tracker().record("GET", "/monitoring/v2/aps", "skeleton")
        allowed, reason = check_call_policy("GET", "/monitoring/v2/aps")
        assert allowed is True
        assert reason is None

    def test_allowed_after_glossary_inspection(self):
        # Glossary alone satisfies the gate (per design).
        get_tracker().record("GET", "/monitoring/v2/aps", "glossary")
        allowed, _ = check_call_policy("GET", "/monitoring/v2/aps")
        assert allowed is True

    def test_inspection_is_per_endpoint_not_global(self):
        get_tracker().record("GET", "/monitoring/v2/aps", "skeleton")
        allowed, _ = check_call_policy("GET", "/monitoring/v2/switches")
        assert allowed is False

    def test_inspection_is_per_method(self):
        get_tracker().record("GET", "/config/v1/vlans", "skeleton")
        allowed, _ = check_call_policy("POST", "/config/v1/vlans")
        assert allowed is False


# ── End-to-end: detail/glossary tools record inspections ────────────


@pytest.fixture
def catalog_tools(tmp_path):
    """Build a FastMCP server with the catalog + api_call tools registered.

    Reuses the in-memory graph fixture from ``test_api_catalog.py``.
    Uses pytest's ``tmp_path`` so the temporary DB and script library are
    cleaned up automatically after the test, even on failure.
    """
    from mcp.server.fastmcp import FastMCP

    from hpe_networking_central_mcp.config import Settings
    from hpe_networking_central_mcp.graph.manager import GraphManager
    from hpe_networking_central_mcp.tools.api_catalog import register_catalog_tools
    from hpe_networking_central_mcp.tools.api_call import (
        register_api_call_tools,
        register_greenlake_api_call_tools,
    )

    db_path = tmp_path / "gate.db"
    gm = GraphManager(db_path)
    gm.initialize()

    skeleton = {
        "method": "GET",
        "path": "/monitoring/v2/aps",
        "summary": "List APs",
        "operation_id": "listAPs",
        "tags": [],
        "deprecated": False,
        "parameters": [],
        "request_body": None,
        "required_paths": [],
        "responses": {"200": {"schema": {"type": "object"}}},
    }
    glossary = {
        "method": "GET",
        "path": "/monitoring/v2/aps",
        "components": {},
    }

    def _esc(s: str) -> str:
        return s.replace("\\", "\\\\").replace("'", "\\'")

    skel_json = json.dumps(skeleton)
    gloss_json = json.dumps(glossary)
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: $eid, method: $m, path: $p,"
        "  summary: $sum, description: '',"
        "  operationId: $op, category: 'monitoring',"
        "  deprecated: false, parameters: '[]',"
        "  requestBody: '', responses: '',"
        f"  bodySkeletonJson: '{_esc(skel_json)}',"
        f"  bodyGlossaryJson: '{_esc(gloss_json)}'"
        "})",
        {
            "eid": "GET:/monitoring/v2/aps",
            "m": "GET", "p": "/monitoring/v2/aps",
            "sum": "List APs", "op": "listAPs",
        },
    )
    gm.create_fts_indexes()

    settings = Settings(
        central_base_url="https://test.example.com",
        central_client_id="cid",
        central_client_secret="csec",
        script_library_path=tmp_path / "lib",
    )

    mcp = FastMCP("test")
    register_catalog_tools(mcp, settings, gm)

    # Mock the Central client so call_central_api can be invoked without
    # touching the network. The gate runs before _make_api_call, so the
    # blocked-path tests never reach this mock.
    mock_client = MagicMock()
    mock_client._request.return_value = {"items": []}
    register_api_call_tools(mcp, settings, mock_client)
    register_greenlake_api_call_tools(mcp, settings, mock_client)

    tool_map = {t.name: t.fn for t in mcp._tool_manager._tools.values()}
    return tool_map, mock_client


class TestEndToEndGate:

    def test_call_central_api_blocked_without_inspection(self, catalog_tools):
        tools, client = catalog_tools
        from mcp.server.fastmcp.exceptions import ToolError

        with pytest.raises(ToolError) as exc:
            tools["call_central_api"](path="/monitoring/v2/aps")
        assert "get_api_endpoint_detail" in str(exc.value)
        assert "/monitoring/v2/aps" in str(exc.value)
        client._request.assert_not_called()

    def test_call_central_api_allowed_after_get_api_endpoint_detail(self, catalog_tools):
        tools, client = catalog_tools

        # Inspect the endpoint first.
        result = json.loads(
            tools["get_api_endpoint_detail"](
                method="GET", path="/monitoring/v2/aps"
            )
        )
        assert result["operation_id"] == "listAPs"

        # Now the call must succeed (and reach the mocked client).
        out = tools["call_central_api"](path="/monitoring/v2/aps")
        assert "items" in out
        client._request.assert_called_once()

    def test_call_central_api_allowed_after_get_api_endpoint_glossary(self, catalog_tools):
        tools, client = catalog_tools
        tools["get_api_endpoint_glossary"](
            method="GET", path="/monitoring/v2/aps"
        )
        out = tools["call_central_api"](path="/monitoring/v2/aps")
        assert "items" in out
        client._request.assert_called_once()

    def test_call_greenlake_api_blocked_without_inspection(self, catalog_tools):
        tools, _ = catalog_tools
        from mcp.server.fastmcp.exceptions import ToolError

        with pytest.raises(ToolError) as exc:
            tools["call_greenlake_api"](path="/monitoring/v2/aps")
        assert "get_api_endpoint_detail" in str(exc.value)

    def test_inspection_for_one_endpoint_does_not_unlock_another(self, catalog_tools):
        tools, _ = catalog_tools
        from mcp.server.fastmcp.exceptions import ToolError

        tools["get_api_endpoint_detail"](
            method="GET", path="/monitoring/v2/aps"
        )

        with pytest.raises(ToolError) as exc:
            tools["call_central_api"](path="/monitoring/v2/switches")
        assert "get_api_endpoint_detail" in str(exc.value)
