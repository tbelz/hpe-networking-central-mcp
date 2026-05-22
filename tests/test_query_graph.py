"""Tests for the ``query_graph`` MCP tool ergonomics (Phase 2B of ADR 009).

Cover:
- pass-through of parameterised Cypher via the new ``parameters`` JSON arg,
- soft row cap (default 200) — truncation envelope,
- hard row cap (default 2000) — raised as ToolError,
- empty / invalid input rejection,
- error hint enrichment via the schema.
"""

from __future__ import annotations

import json

import pytest
from mcp.server.fastmcp import FastMCP
from mcp.server.fastmcp.exceptions import ToolError

from hpe_networking_central_mcp.config import Settings
from hpe_networking_central_mcp.graph.manager import GraphManager
from hpe_networking_central_mcp.tools.graph import register_graph_tools


@pytest.fixture
def gm(tmp_path_factory):
    db_path = tmp_path_factory.mktemp("qg") / "test.db"
    gm = GraphManager(db_path)
    gm.initialize()
    return gm


def _make_query_tool(gm):
    settings = Settings(
        central_base_url="https://x",
        central_client_id="cid",
        central_client_secret="csec",
        read_only=True,
    )
    mcp = FastMCP("test-qg")
    register_graph_tools(mcp, settings, gm)
    tools = {t.name: t.fn for t in mcp._tool_manager._tools.values()}
    return tools["query_graph"]


def _make_n_endpoints(gm: GraphManager, n: int) -> None:
    for i in range(n):
        gm.execute(
            "CREATE (e:ApiEndpoint {"
            "  endpoint_id: $eid,"
            "  method: 'GET', path: $path,"
            "  summary: '', description: '',"
            "  operationId: '', category: 'cat',"
            "  deprecated: false,"
            "  parameters: '[]', requestBody: '', responses: ''"
            "})",
            {"eid": f"GET:/p/{i}", "path": f"/p/{i}"},
        )


class TestQueryGraphParameters:
    def test_accepts_json_parameters(self, gm):
        _make_n_endpoints(gm, 3)
        qg = _make_query_tool(gm)
        out = qg(
            cypher="MATCH (e:ApiEndpoint) WHERE e.path = $p RETURN e.path AS path",
            parameters=json.dumps({"p": "/p/1"}),
        )
        rows = json.loads(out)
        # Either bare list or envelope-with-rows; bare list expected for small results.
        assert isinstance(rows, list), f"expected bare list for small result, got {type(rows)}"
        assert rows == [{"path": "/p/1"}]

    def test_default_parameters_is_empty(self, gm):
        _make_n_endpoints(gm, 1)
        qg = _make_query_tool(gm)
        out = qg(cypher="MATCH (e:ApiEndpoint) RETURN count(e) AS n")
        rows = json.loads(out)
        assert rows == [{"n": 1}]

    def test_invalid_parameters_json_raises(self, gm):
        qg = _make_query_tool(gm)
        with pytest.raises(ToolError, match="Invalid JSON"):
            qg(cypher="MATCH (e) RETURN e", parameters="{not-json")


class TestQueryGraphCaps:
    def test_under_soft_cap_returns_bare_list(self, gm):
        qg = _make_query_tool(gm)
        out = qg(cypher="UNWIND range(1, 10) AS i RETURN i")
        rows = json.loads(out)
        assert isinstance(rows, list)
        assert len(rows) == 10

    def test_over_soft_cap_returns_truncation_envelope(self, gm):
        qg = _make_query_tool(gm)
        out = qg(cypher="UNWIND range(1, 250) AS i RETURN i")
        result = json.loads(out)
        assert isinstance(result, dict)
        assert result.get("truncated") is True
        assert result.get("cap") == 200
        assert "rows" in result
        assert len(result["rows"]) == 200
        assert "warning" in result

    def test_over_hard_cap_raises(self, gm):
        qg = _make_query_tool(gm)
        with pytest.raises(ToolError, match="hard cap|exceeds"):
            qg(cypher="UNWIND range(1, 2100) AS i RETURN i")


class TestQueryGraphRejection:
    def test_empty_cypher_raises(self, gm):
        qg = _make_query_tool(gm)
        with pytest.raises(ToolError, match="empty"):
            qg(cypher="   ")


# ── Freshness signaling (volatile-field detection) ──────────────────


def _insert_device_fresh(gm, serial: str) -> None:
    gm.execute(
        "CREATE (d:Device {serial: $s, name: $s, status: 'Up', "
        "lastSyncedAt: current_timestamp()})",
        {"s": serial},
    )


def _insert_device_stale(gm, serial: str) -> None:
    gm.execute(
        "CREATE (d:Device {serial: $s, name: $s, status: 'Up', "
        "lastSyncedAt: timestamp('2020-01-01 00:00:00')})",
        {"s": serial},
    )


def _insert_device_unstamped(gm, serial: str) -> None:
    gm.execute(
        "CREATE (d:Device {serial: $s, name: $s, status: 'Up'})",
        {"s": serial},
    )


class TestQueryGraphFreshness:
    def test_fresh_volatile_projection_returns_bare_list(self, gm):
        _insert_device_fresh(gm, "FRESH1")
        qg = _make_query_tool(gm)
        out = qg(
            cypher="MATCH (d:Device) RETURN d.status AS `d.status`, "
                   "d.lastSyncedAt AS `d.lastSyncedAt`",
        )
        parsed = json.loads(out)
        assert isinstance(parsed, list), f"expected bare list, got {parsed!r}"
        assert len(parsed) == 1

    def test_stale_volatile_projection_wraps_with_warnings(self, gm):
        _insert_device_stale(gm, "STALE1")
        qg = _make_query_tool(gm)
        out = qg(
            cypher="MATCH (d:Device) RETURN d.status AS `d.status`, "
                   "d.lastSyncedAt AS `d.lastSyncedAt`",
        )
        parsed = json.loads(out)
        assert isinstance(parsed, dict)
        assert "rows" in parsed
        assert "freshness_warnings" in parsed
        warns = parsed["freshness_warnings"]
        assert len(warns) == 1
        w = warns[0]
        assert w["node_label"] == "Device"
        assert "status" in w["volatile_fields_in_result"]
        assert w["lastSyncedAt_present"] is True
        assert w["max_age_seconds"] is not None and w["max_age_seconds"] > 0
        assert w["rows_affected"] >= 1

    def test_no_volatile_projection_returns_bare_list(self, gm):
        _insert_device_stale(gm, "STALE2")
        qg = _make_query_tool(gm)
        # Project only stable fields — name + serial. No warning expected.
        out = qg(cypher="MATCH (d:Device) RETURN d.name AS `d.name`")
        parsed = json.loads(out)
        assert isinstance(parsed, list)

    def test_unstamped_node_with_volatile_field_warns(self, gm):
        _insert_device_unstamped(gm, "NOSTAMP")
        qg = _make_query_tool(gm)
        out = qg(
            cypher="MATCH (d:Device) RETURN d.status AS `d.status`",
        )
        parsed = json.loads(out)
        assert isinstance(parsed, dict)
        warns = parsed["freshness_warnings"]
        assert warns and warns[0]["lastSyncedAt_present"] is False

    def test_threshold_zero_disables_warnings(self, gm, monkeypatch):
        _insert_device_stale(gm, "STALE3")
        monkeypatch.setenv("MCP_GRAPH_STALE_THRESHOLD_SECONDS", "0")
        qg = _make_query_tool(gm)
        out = qg(
            cypher="MATCH (d:Device) RETURN d.status AS `d.status`, "
                   "d.lastSyncedAt AS `d.lastSyncedAt`",
        )
        parsed = json.loads(out)
        assert isinstance(parsed, list), (
            f"threshold=0 should disable freshness wrapping, got {type(parsed)}"
        )
