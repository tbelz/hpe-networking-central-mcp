"""Unit tests for API catalog tools — unified_search, list_api_categories,
get_api_endpoint_detail, get_api_endpoint_glossary.

Uses an in-memory LadybugDB graph with fixture data (no live API calls).
"""

from __future__ import annotations

import json
import sys
from pathlib import Path
from unittest.mock import MagicMock

import pytest

sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpe_networking_central_mcp.graph.manager import GraphManager
from hpe_networking_central_mcp.config import Settings


# ── Fixtures ────────────────────────────────────────────────────────


# Fixtures mirror the real project_skeleton output shape:
#   responses → dict keyed by status code + optional "error" sentinel
#   $components_index → sectioned: {"schemas": {...}, "responses": {...}}
#                       with per-entry minimal hints (type / required / refs)
_VLAN_SKELETON = {
    "method": "POST",
    "path": "/config/v1/vlans",
    "summary": "Create a VLAN",
    "operation_id": "createVLAN",
    "tags": [],
    "deprecated": False,
    "parameters": [],
    "request_body": {
        "content_type": "application/json",
        "required": True,
        "schema": {
            "type": "object",
            "properties": {
                "vlan_id": {"type": "integer"},
                "name": {"type": "string"},
            },
            "required": ["vlan_id"],
        },
    },
    "required_paths": ["vlan_id"],
    "responses": {
        "201": {"schema": {"$ref": "#/components/schemas/VlanCfg"}},
        "error": "#/components/responses/BadRequest",
    },
    "$components_index": {
        "schemas": {"VlanCfg": {"type": "object"}},
        "responses": {"BadRequest": {"type": "object"}},
    },
}

_VLAN_COMPONENTS = {
    "schemas": {
        "VlanCfg": {
            "type": "object",
            "properties": {
                "vlan_id": {"type": "integer"},
                "name": {"type": "string"},
            },
            "required": ["vlan_id"],
        },
    },
    "responses": {
        "BadRequest": {"type": "object"},
    },
}

_VLAN_GLOSSARY = {
    "method": "POST",
    "path": "/config/v1/vlans",
    "components": {
        "VlanCfg": {
            "description": "VLAN configuration object.",
            "properties": {
                "vlan_id": {"description": "VLAN identifier in [1,4094]."},
                "name": {"description": "Human-readable label."},
            },
        },
    },
}

_APS_SKELETON = {
    "method": "GET",
    "path": "/monitoring/v2/aps",
    "summary": "List all access points",
    "operation_id": "listAPs",
    "tags": [],
    "deprecated": False,
    "parameters": [{"name": "site", "in": "query"}],
    "request_body": None,
    "required_paths": [],
    "responses": {
        "200": {"schema": {"type": "object"}},
    },
}

_APS_GLOSSARY = {
    "method": "GET",
    "path": "/monitoring/v2/aps",
    "components": {},
}


@pytest.fixture(scope="module")
def gm(tmp_path_factory):
    """Create an in-memory GraphManager with API endpoint fixtures."""
    db_path = tmp_path_factory.mktemp("catalog_db") / "test.db"
    gm = GraphManager(db_path)
    gm.initialize()

    def _esc(s: str) -> str:
        # Mirror scripts/build_knowledge_db.py::_cypher_escape — single quotes
        # and backslashes must be escaped for inline string literals in Cypher.
        return s.replace("\\", "\\\\").replace("'", "\\'")

    def _create(method: str, path: str, summary: str, description: str,
                op_id: str, category: str, parameters: str = "[]",
                skeleton: dict | None = None, glossary: dict | None = None,
                components: dict | None = None):
        skel_json = json.dumps(skeleton) if skeleton else ""
        gloss_json = json.dumps(glossary) if glossary else ""
        comp_json = json.dumps(components) if components else ""
        # JSON blobs and other long strings are inlined (matches the build
        # script). Kuzu's parameter binder cannot infer types for many string
        # parameters in a single statement, which surfaces as a misleading
        # "vector with ANY type" runtime error.
        gm.execute(
            "CREATE (e:ApiEndpoint {"
            "  endpoint_id: $eid, method: $m, path: $p,"
            "  summary: $sum, description: $descr,"
            "  operationId: $op, category: $cat,"
            "  deprecated: false,"
            f"  parameters: '{_esc(parameters)}',"
            "  requestBody: '', responses: '',"
            f"  bodySkeletonJson: '{_esc(skel_json)}',"
            f"  bodyGlossaryJson: '{_esc(gloss_json)}',"
            f"  bodyComponentsJson: '{_esc(comp_json)}'"
            "})",
            {
                "eid": f"{method}:{path}",
                "m": method, "p": path,
                "sum": summary, "descr": description,
                "op": op_id, "cat": category,
            },
        )
        # Mirror the populate_schema_graph wiring: every ApiEndpoint also
        # owns an ApiEndpointSkeleton blob node + HAS_SKELETON edge.
        eid = f"{method}:{path}"
        gm.execute(
            "CREATE (s:ApiEndpointSkeleton {"
            "  endpoint_id: $eid,"
            f"  bodySkeletonJson: '{_esc(skel_json)}',"
            f"  bodyGlossaryJson: '{_esc(gloss_json)}'"
            "})",
            {"eid": eid},
        )
        gm.execute(
            "MATCH (e:ApiEndpoint {endpoint_id: $eid}), "
            "(s:ApiEndpointSkeleton {endpoint_id: $eid}) "
            "CREATE (e)-[:HAS_SKELETON]->(s)",
            {"eid": eid},
        )
        # Mirror the populate_schema_graph SchemaComponent rows so
        # get_schema_component can resolve them.  Spec source is irrelevant
        # for the lookup (we match by section + name) so we use a fixed
        # "test" tag.
        if components:
            for section, section_map in components.items():
                if not isinstance(section_map, dict):
                    continue
                for name, body in section_map.items():
                    cid = f"test:{section}:{name}"
                    body_json = json.dumps(body)
                    gm.execute(
                        "MERGE (c:SchemaComponent {component_id: $cid}) SET "
                        "c.spec_source = 'test', c.section = $sec, c.name = $name, "
                        "c.type = '', c.kind = '', "
                        "c.required = CAST([] AS STRING[]), "
                        "c.enumValues = CAST([] AS STRING[]), "
                        f"c.bodyJson = '{_esc(body_json)}'",
                        {"cid": cid, "sec": section, "name": name},
                    )

    _create("GET", "/monitoring/v2/aps", "List all access points",
            "Paginated list of APs", "listAPs", "monitoring",
            parameters='[{"name":"site","in":"query"}]',
            skeleton=_APS_SKELETON, glossary=_APS_GLOSSARY)
    _create("GET", "/monitoring/v2/switches", "List all switches",
            "Paginated list of switches", "listSwitches", "monitoring",
            skeleton={"method": "GET", "path": "/monitoring/v2/switches",
                      "summary": "List all switches", "operation_id": "listSwitches",
                      "tags": [], "deprecated": False, "parameters": [],
                      "request_body": None, "required_paths": [], "responses": {}},
            glossary={"method": "GET", "path": "/monitoring/v2/switches",
                      "components": {}})
    _create("POST", "/config/v1/vlans", "Create a VLAN",
            "Creates a VLAN on a device", "createVLAN", "config",
            skeleton=_VLAN_SKELETON, glossary=_VLAN_GLOSSARY,
            components=_VLAN_COMPONENTS)
    _create("DELETE", "/config/v1/vlans/{id}", "Delete a VLAN",
            "Removes a VLAN", "deleteVLAN", "config",
            skeleton={"method": "DELETE", "path": "/config/v1/vlans/{id}",
                      "summary": "Delete a VLAN", "operation_id": "deleteVLAN",
                      "tags": [], "deprecated": False, "parameters": [],
                      "request_body": None, "required_paths": [], "responses": {}},
            glossary={"method": "DELETE", "path": "/config/v1/vlans/{id}",
                      "components": {}})

    gm.create_fts_indexes()
    return gm


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
    """Register catalog tools and return a dict of tool functions."""
    from mcp.server.fastmcp import FastMCP

    mcp = FastMCP("test")
    from hpe_networking_central_mcp.tools.api_catalog import register_catalog_tools

    register_catalog_tools(mcp, settings, gm)

    tool_map = {}
    for tool in mcp._tool_manager._tools.values():
        tool_map[tool.name] = tool.fn
    return tool_map


# ── unified_search (hidden tool) ────────────────────────────────────


class TestUnifiedSearchHidden:
    """``unified_search`` is intentionally not registered as an MCP tool.

    The function body is preserved in :mod:`tools.api_catalog` (so the tool
    can be re-enabled by restoring the ``@mcp.tool`` decorator once a real
    docs / VSG corpus is available) but it must not appear in the registered
    tool surface — agents should reach for ``query_graph`` for keyword
    lookups against graph data, and the API endpoint catalog for endpoint
    discovery.
    """

    def test_unified_search_not_registered_as_tool(self, tools):
        assert "unified_search" not in tools


# ── unified_search (implementation retained for future re-enablement) ─
# The original behavioural tests have been removed because the
# implementation is no longer reachable through ``mcp._tool_manager`` and
# is defined inside ``register_catalog_tools`` (not importable as a
# module-level symbol). Restore them when the tool is re-registered.


# ── list_api ─────────────────────────────────────────────────────────


class TestListApi:

    def test_returns_path_tree_text(self, tools):
        text = tools["list_api"]()
        assert isinstance(text, str)
        assert "API Endpoint Catalog" in text
        # Categories from the conftest fixtures appear as headers
        assert "## monitoring" in text.lower() or "## monitoring" in text
        assert "## config" in text.lower() or "## config" in text

    def test_lists_methods(self, tools):
        text = tools["list_api"]()
        # Real path-tree formatting puts methods in brackets
        assert "[GET" in text or "[POST" in text


# ── get_api_endpoint_detail (skeleton) ──────────────────────────────


class TestGetApiEndpointDetail:

    def test_returns_skeleton_shape(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            method="POST", path="/config/v1/vlans"))
        assert result["method"] == "POST"
        assert result["path"] == "/config/v1/vlans"
        assert result["operation_id"] == "createVLAN"
        assert result["request_body"]["schema"]["type"] == "object"
        assert "vlan_id" in result["request_body"]["schema"]["properties"]
        assert isinstance(result["responses"], dict)
        assert "201" in result["responses"]
        assert "$components_index" in result
        assert "schemas" in result["$components_index"]

    def test_no_descriptions_in_skeleton(self, tools):
        # Skeleton fixture deliberately omits descriptions; the wire
        # format must not surface any description-bearing keys either.
        result = json.loads(tools["get_api_endpoint_detail"](
            method="POST", path="/config/v1/vlans"))
        body = json.dumps(result)
        assert '"description"' not in body
        assert '"title"' not in body
        assert '"example"' not in body

    def test_parameters_parsed(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            method="GET", path="/monitoring/v2/aps"))
        assert isinstance(result["parameters"], list)
        assert result["parameters"][0]["name"] == "site"

    def test_not_found(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            method="GET", path="/nonexistent"))
        assert "error" in result
        assert "hint" in result

    def test_case_insensitive_method(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            method="get", path="/monitoring/v2/aps"))
        assert result["method"] == "GET"

    def test_path_without_leading_slash(self, tools):
        # Agents often omit the leading slash when reading from the catalog tree.
        # Both forms must resolve to the same endpoint.
        with_slash = json.loads(tools["get_api_endpoint_detail"](
            method="GET", path="/monitoring/v2/aps"))
        without_slash = json.loads(tools["get_api_endpoint_detail"](
            method="GET", path="monitoring/v2/aps"))
        assert with_slash["operation_id"] == without_slash["operation_id"]

    def test_parts_filter_restricts_payload(self, tools):
        # parts=["meta", "parameters"] keeps only those sections.
        result = json.loads(tools["get_api_endpoint_detail"](
            method="POST", path="/config/v1/vlans",
            parts=["meta", "parameters"]))
        assert "parameters" in result
        assert "method" in result  # meta key
        assert "request_body" not in result
        assert "responses" not in result
        assert "$components_index" not in result

    def test_parts_filter_components_index_only(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            method="POST", path="/config/v1/vlans",
            parts=["$components_index"]))
        assert "$components_index" in result
        assert "request_body" not in result
        assert "method" not in result  # meta excluded

    def test_parts_filter_meta_includes_operation_id(self, tools):
        # ``operation_id`` (snake_case) must survive the ``meta`` filter.
        result = json.loads(tools["get_api_endpoint_detail"](
            method="POST", path="/config/v1/vlans",
            parts=["meta"]))
        assert "operation_id" in result
        assert "method" in result

    def test_parts_filter_all_unknown_returns_full_skeleton(self, tools):
        # When every requested part is unknown the filter must not apply
        # and the full skeleton is returned instead of an empty dict.
        result = json.loads(tools["get_api_endpoint_detail"](
            method="POST", path="/config/v1/vlans",
            parts=["nope", "also_unknown"]))
        assert "method" in result
        assert "request_body" in result


# ── get_schema_component ─────────────────────────────────────────────


class TestGetSchemaComponent:

    def test_returns_component_body(self, tools):
        result = json.loads(tools["get_schema_component"](
            method="POST", path="/config/v1/vlans", name="VlanCfg"))
        assert result["section"] == "schemas"
        assert result["name"] == "VlanCfg"
        body = result["body"]
        assert body["type"] == "object"
        assert "vlan_id" in body["properties"]
        assert body["required"] == ["vlan_id"]

    def test_unknown_component_lists_available(self, tools):
        result = json.loads(tools["get_schema_component"](
            method="POST", path="/config/v1/vlans", name="DoesNotExist"))
        assert "error" in result
        assert "VlanCfg" in result["available"]

    def test_unknown_endpoint(self, tools):
        result = json.loads(tools["get_schema_component"](
            method="GET", path="/nope", name="Foo"))
        assert "error" in result

    def test_endpoint_without_components(self, tools):
        # /monitoring/v2/aps has no components blob in the fixture.
        result = json.loads(tools["get_schema_component"](
            method="GET", path="/monitoring/v2/aps", name="Foo"))
        assert "error" in result


# ── get_api_endpoint_glossary ───────────────────────────────────────


class TestGetApiEndpointGlossary:

    def test_returns_components(self, tools):
        result = json.loads(tools["get_api_endpoint_glossary"](
            method="POST", path="/config/v1/vlans"))
        assert result["method"] == "POST"
        assert "VlanCfg" in result["components"]
        entry = result["components"]["VlanCfg"]
        assert entry["description"].startswith("VLAN configuration")
        assert "vlan_id" in entry["properties"]

    def test_components_filter(self, tools):
        result = json.loads(tools["get_api_endpoint_glossary"](
            method="POST", path="/config/v1/vlans",
            components=["VlanCfg"]))
        assert set(result["components"].keys()) == {"VlanCfg"}

    def test_components_filter_drops_unknown(self, tools):
        result = json.loads(tools["get_api_endpoint_glossary"](
            method="POST", path="/config/v1/vlans",
            components=["DoesNotExist"]))
        assert result["components"] == {}

    def test_not_found(self, tools):
        result = json.loads(tools["get_api_endpoint_glossary"](
            method="GET", path="/nonexistent"))
        assert "error" in result

    def test_no_args_errors(self, tools):
        result = json.loads(tools["get_api_endpoint_glossary"]())
        assert "error" in result

    def test_path_without_leading_slash(self, tools):
        # Agents often omit the leading slash when reading from the catalog tree.
        # Both forms must resolve to the same endpoint.
        with_slash = json.loads(tools["get_api_endpoint_glossary"](
            method="POST", path="/config/v1/vlans"))
        without_slash = json.loads(tools["get_api_endpoint_glossary"](
            method="POST", path="config/v1/vlans"))
        assert "VlanCfg" in without_slash["components"]


# ── Graph unavailable ────────────────────────────────────────────────


class TestGraphUnavailable:
    """``get_api_endpoint_detail`` / ``_glossary`` must degrade gracefully
    when the graph database is not yet loaded (e.g. server start-up race)."""

    def test_detail_and_glossary_report_graph_unavailable(self, settings):
        from mcp.server.fastmcp import FastMCP
        from hpe_networking_central_mcp.tools import api_catalog

        # ``register_catalog_tools`` sets the module-level ``_graph_manager``.
        # Save and restore so we don't poison the module-scoped ``tools``
        # fixture used by other test classes.
        original = api_catalog._graph_manager
        try:
            mcp = FastMCP("test_no_graph")
            api_catalog.register_catalog_tools(
                mcp, settings, MagicMock(is_available=False)
            )

            tool_map = {t.name: t.fn for t in mcp._tool_manager._tools.values()}

            for name in ("get_api_endpoint_detail", "get_api_endpoint_glossary"):
                result = json.loads(
                    tool_map[name](method="GET", path="/monitoring/v2/aps")
                )
                assert "error" in result, f"{name} did not return an error"
                assert "graph" in result["error"].lower(), (
                    f"{name} error did not mention the graph: {result['error']!r}"
                )
        finally:
            api_catalog._graph_manager = original


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

    def test_bulk_reports_missing(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            endpoints=[
                {"method": "GET", "path": "/monitoring/v2/aps"},
                {"method": "GET", "path": "/does/not/exist"},
            ]
        ))
        assert len(result["endpoints"]) == 1
        assert result["missing"] == [{"method": "GET", "path": "/does/not/exist"}]

    def test_bulk_preserves_order(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            endpoints=[
                {"method": "POST", "path": "/config/v1/vlans"},
                {"method": "GET", "path": "/monitoring/v2/aps"},
                {"method": "GET", "path": "/monitoring/v2/switches"},
            ]
        ))
        ordered = [ep["path"] for ep in result["endpoints"]]
        assert ordered == [
            "/config/v1/vlans",
            "/monitoring/v2/aps",
            "/monitoring/v2/switches",
        ]

    def test_bulk_empty_list_errors(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](endpoints=[]))
        assert "error" in result

    def test_bulk_invalid_entry_errors(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            endpoints=[{"method": "GET"}]
        ))
        assert "error" in result

    def test_no_args_errors(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"]())
        assert "error" in result

    def test_single_form_no_endpoints_wrapper(self, tools):
        result = json.loads(tools["get_api_endpoint_detail"](
            method="GET", path="/monitoring/v2/aps"
        ))
        assert "endpoints" not in result
        assert result["method"] == "GET"


# ── Bulk get_api_endpoint_glossary ──────────────────────────────────


class TestBulkGetApiEndpointGlossary:

    def test_bulk_returns_multiple(self, tools):
        result = json.loads(tools["get_api_endpoint_glossary"](
            endpoints=[
                {"method": "POST", "path": "/config/v1/vlans"},
                {"method": "GET", "path": "/monitoring/v2/aps"},
            ]
        ))
        assert len(result["endpoints"]) == 2
        assert result["missing"] == []

    def test_bulk_components_filter(self, tools):
        result = json.loads(tools["get_api_endpoint_glossary"](
            endpoints=[{"method": "POST", "path": "/config/v1/vlans"}],
            components=["VlanCfg"],
        ))
        assert set(result["endpoints"][0]["components"].keys()) == {"VlanCfg"}


# ── Bulk + READ_ONLY ────────────────────────────────────────────────


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
        assert len(result["endpoints"]) == 1
        assert result["endpoints"][0]["method"] == "GET"
        skipped = result.get("skipped_read_only", [])
        assert {(s["method"], s["path"]) for s in skipped} == {
            ("POST", "/config/v1/vlans"),
            ("DELETE", "/config/v1/vlans/{id}"),
        }

    def test_bulk_all_blocked_returns_empty(self, gm, tmp_path_factory):
        tools_ro = self._make_tools_ro(gm, tmp_path_factory)
        result = json.loads(tools_ro["get_api_endpoint_detail"](
            endpoints=[{"method": "POST", "path": "/config/v1/vlans"}]
        ))
        assert result["endpoints"] == []
        assert len(result["skipped_read_only"]) == 1
