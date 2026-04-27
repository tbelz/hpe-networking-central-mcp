"""Tests for the API call policy gate (``tools.api_call_policy``).

The gate refuses ``call_central_api`` / ``call_greenlake_api`` invocations
for endpoints whose schema has not been inspected via
``describe_endpoint_for_device`` in the same session.
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
    EndpointRegistry,
    InspectionTracker,
    check_call_policy,
    get_registry,
    get_tracker,
    normalise_path,
    register_endpoints,
    set_property_summary_fetcher,
)


@pytest.fixture(autouse=True)
def _reset_policy_state():
    get_tracker().reset()
    get_registry().reset()
    set_property_summary_fetcher(None)
    yield
    get_tracker().reset()
    get_registry().reset()
    set_property_summary_fetcher(None)


# ── InspectionTracker primitives ────────────────────────────────────


class TestInspectionTracker:

    def test_unrecorded_endpoint_is_not_inspected(self):
        t = InspectionTracker()
        assert t.was_inspected("GET", "/foo") is False

    def test_record_satisfies_check(self):
        t = InspectionTracker()
        t.record("GET", "/foo")
        assert t.was_inspected("GET", "/foo") is True

    def test_method_is_case_insensitive(self):
        t = InspectionTracker()
        t.record("get", "/foo")
        assert t.was_inspected("GET", "/foo") is True

    def test_path_normalisation_with_and_without_leading_slash(self):
        t = InspectionTracker()
        t.record("GET", "monitoring/v2/aps")
        assert t.was_inspected("GET", "/monitoring/v2/aps") is True
        assert t.was_inspected("GET", "monitoring/v2/aps") is True

    def test_reset_clears_records(self):
        t = InspectionTracker()
        t.record("GET", "/foo")
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
        # Block message must point the agent at describe_endpoint_for_device,
        # the only remaining schema-inspection tool.
        assert "describe_endpoint_for_device" in reason
        assert "/monitoring/v2/aps" in reason

    def test_allowed_after_inspection(self):
        get_tracker().record("GET", "/monitoring/v2/aps")
        allowed, reason = check_call_policy("GET", "/monitoring/v2/aps")
        assert allowed is True
        assert reason is None

    def test_inspection_is_per_endpoint_not_global(self):
        get_tracker().record("GET", "/monitoring/v2/aps")
        allowed, _ = check_call_policy("GET", "/monitoring/v2/switches")
        assert allowed is False

    def test_inspection_is_per_method(self):
        get_tracker().record("GET", "/config/v1/vlans")
        allowed, _ = check_call_policy("POST", "/config/v1/vlans")
        assert allowed is False


# ── End-to-end: describe_endpoint_for_device records inspection ─────


@pytest.fixture
def catalog_tools(tmp_path):
    """Build a FastMCP server with catalog + describe + api_call tools."""
    from mcp.server.fastmcp import FastMCP

    from hpe_networking_central_mcp.config import Settings
    from hpe_networking_central_mcp.graph.manager import GraphManager
    from hpe_networking_central_mcp.tools.api_catalog import register_catalog_tools
    from hpe_networking_central_mcp.tools.describe import register_describe_tools
    from hpe_networking_central_mcp.tools.api_call import (
        register_api_call_tools,
        register_greenlake_api_call_tools,
    )

    db_path = tmp_path / "gate.db"
    gm = GraphManager(db_path)
    gm.initialize()

    # Endpoint with a request body that decomposes into Properties.
    gm.execute(
        "CREATE (e:ApiEndpoint {"
        "  endpoint_id: 'POST:/config/v1/widgets', method: 'POST',"
        "  path: '/config/v1/widgets', summary: 'Create widget',"
        "  description: '', operationId: 'createWidget',"
        "  category: 'config', deprecated: false, tags: [],"
        "  parameters: '', requestBody: '', responses: ''"
        "})",
    )
    # Add a request body with properties so describe_endpoint can find them.
    gm.execute(
        "CREATE (rb:RequestBody {request_body_id: 'rb1', endpoint_id: "
        "'POST:/config/v1/widgets', content_type: 'application/json', "
        "required: true, root_component_ref: 'Widget'})"
    )
    gm.execute(
        "MATCH (e:ApiEndpoint {endpoint_id: 'POST:/config/v1/widgets'}), "
        "(rb:RequestBody {request_body_id: 'rb1'}) "
        "CREATE (e)-[:HAS_REQUEST_BODY]->(rb)"
    )
    gm.execute(
        "CREATE (c:SchemaComponent {component_id: 'c1', name: 'Widget', "
        "section: 'schemas', spec_source: 'central', bodyJson: '{}'})"
    )
    gm.execute(
        "MATCH (rb:RequestBody {request_body_id: 'rb1'}), "
        "(c:SchemaComponent {component_id: 'c1'}) "
        "CREATE (rb)-[:BODY_REFERENCES]->(c)"
    )
    gm.execute(
        "CREATE (p:Property {property_id: 'p1', parent_component_id: 'c1', "
        "name: 'name', type: 'string', format: '', required: true, "
        "readOnly: false, enumValues: [], description: 'Widget name', "
        "supportedDeviceTypes: [], yangPath: '', inheritedFrom: '', "
        "extensionsJson: ''})"
    )
    gm.execute(
        "MATCH (c:SchemaComponent {component_id: 'c1'}), "
        "(p:Property {property_id: 'p1'}) "
        "CREATE (c)-[:HAS_PROPERTY]->(p)"
    )

    settings = Settings(
        central_base_url="https://test.example.com",
        central_client_id="cid",
        central_client_secret="csec",
        script_library_path=tmp_path / "lib",
    )

    mcp = FastMCP("test")
    register_catalog_tools(mcp, settings, gm)
    register_describe_tools(mcp, settings, gm)

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
            tools["call_central_api"](path="/config/v1/widgets", method="POST")
        msg = str(exc.value)
        assert "schema not consulted" in msg.lower()
        assert "/config/v1/widgets" in msg
        # The fetcher should inline the property summary so the agent has
        # the field list in hand without a second tool round-trip.
        assert '"name"' in msg, f"property summary should be inlined: {msg[:500]}"
        client._request.assert_not_called()

    def test_blocked_call_auto_records_inspection_for_retry(self, catalog_tools):
        tools, client = catalog_tools
        from mcp.server.fastmcp.exceptions import ToolError

        with pytest.raises(ToolError):
            tools["call_central_api"](path="/config/v1/widgets", method="POST")
        # Retry must succeed: gate auto-records when it inlines the summary.
        out = tools["call_central_api"](path="/config/v1/widgets", method="POST", body={"name": "x"})
        assert "items" in out
        client._request.assert_called_once()

    def test_call_central_api_allowed_after_describe_endpoint_for_device(self, catalog_tools):
        tools, client = catalog_tools

        result = json.loads(
            tools["describe_endpoint_for_device"](
                method="POST", path="/config/v1/widgets"
            )
        )
        assert result["properties"], "describe_endpoint_for_device must return properties"

        out = tools["call_central_api"](path="/config/v1/widgets", method="POST", body={"name": "x"})
        assert "items" in out
        client._request.assert_called_once()

    def test_call_greenlake_api_blocked_without_inspection(self, catalog_tools):
        tools, _ = catalog_tools
        from mcp.server.fastmcp.exceptions import ToolError

        with pytest.raises(ToolError) as exc:
            tools["call_greenlake_api"](path="/monitoring/v2/aps")
        assert "schema not consulted" in str(exc.value).lower()


# ── Template-aware matching ──────────────────────────────────────────


class TestTemplateMatching:

    def test_concrete_path_allowed_after_template_inspection(self):
        register_endpoints({
            "GET": ["/monitoring/v2/aps/{serial-number}/ports"],
        })
        get_tracker().record(
            "GET", "/monitoring/v2/aps/{serial-number}/ports"
        )

        allowed, reason = check_call_policy(
            "GET", "/monitoring/v2/aps/DL0006948/ports"
        )
        assert allowed is True, reason

    def test_concrete_path_blocked_when_template_not_inspected(self):
        register_endpoints({
            "GET": ["/monitoring/v2/aps/{serial-number}/ports"],
        })
        allowed, reason = check_call_policy(
            "GET", "/monitoring/v2/aps/DL0006948/ports"
        )
        assert allowed is False
        assert "{serial-number}" in (reason or "")

    def test_unknown_concrete_path_falls_through_to_literal_check(self):
        allowed, _ = check_call_policy("GET", "/foo/bar")
        assert allowed is False
        get_tracker().record("GET", "/foo/bar")
        allowed, _ = check_call_policy("GET", "/foo/bar")
        assert allowed is True

    def test_method_isolation_in_template_match(self):
        register_endpoints({
            "GET": ["/devices/{id}"],
            "POST": ["/devices/{id}"],
        })
        get_tracker().record("GET", "/devices/{id}")
        ok_get, _ = check_call_policy("GET", "/devices/abc")
        assert ok_get is True
        ok_post, _ = check_call_policy("POST", "/devices/abc")
        assert ok_post is False


# ── Property-summary inlining seam ───────────────────────────────────


class TestPropertySummaryFetcherSeam:

    def test_fetcher_invoked_with_template_path(self):
        register_endpoints({"GET": ["/x/{id}/y"]})
        seen: list[tuple[str, str]] = []

        def fetcher(method: str, template: str) -> str | None:
            seen.append((method, template))
            return '{"properties": [{"name": "demo"}]}'

        set_property_summary_fetcher(fetcher)
        allowed, reason = check_call_policy("GET", "/x/123/y")
        assert allowed is False
        assert seen == [("GET", "/x/{id}/y")]
        assert "demo" in (reason or "")

    def test_fetcher_returning_none_falls_back_to_legacy_message(self):
        register_endpoints({"GET": ["/x/{id}/y"]})
        set_property_summary_fetcher(lambda m, p: None)
        allowed, reason = check_call_policy("GET", "/x/123/y")
        assert allowed is False
        assert "describe_endpoint_for_device" in (reason or "")

    def test_fetcher_exceptions_are_swallowed(self):
        register_endpoints({"GET": ["/x/{id}/y"]})

        def boom(method: str, template: str) -> str | None:
            raise RuntimeError("graph offline")

        set_property_summary_fetcher(boom)
        allowed, reason = check_call_policy("GET", "/x/123/y")
        assert allowed is False
        assert "describe_endpoint_for_device" in (reason or "")


# ── EndpointRegistry primitives ──────────────────────────────────────


class TestEndpointRegistry:

    def test_register_and_match_concrete_path(self):
        r = EndpointRegistry()
        r.register("GET", ["/a/{x}/b/{y}"])
        assert r.match("GET", "/a/1/b/2") == "/a/{x}/b/{y}"

    def test_register_and_match_template_path(self):
        r = EndpointRegistry()
        r.register("GET", ["/a/{x}"])
        assert r.match("GET", "/a/{x}") == "/a/{x}"

    def test_no_cross_segment_matching(self):
        r = EndpointRegistry()
        r.register("GET", ["/a/{x}"])
        assert r.match("GET", "/a/1/2") is None

    def test_method_lookup_is_isolated(self):
        r = EndpointRegistry()
        r.register("GET", ["/a/{x}"])
        assert r.match("POST", "/a/1") is None

    def test_reset_clears_state(self):
        r = EndpointRegistry()
        r.register("GET", ["/a/{x}"])
        r.reset()
        assert r.match("GET", "/a/1") is None


# ── endpoint_id explicit-bypass parameter ────────────────────────────


class TestEndpointIdBypass:
    """``endpoint_id="METHOD:/path"`` lets the agent attest it has already
    inspected the schema (e.g. via ``query_graph``) so the gate skips the
    redundant property summary."""

    def test_matching_endpoint_id_skips_gate(self, catalog_tools):
        tools, client = catalog_tools

        out = tools["call_central_api"](
            path="/config/v1/widgets",
            method="POST",
            body={"name": "x"},
            endpoint_id="POST:/config/v1/widgets",
        )
        assert "items" in out
        client._request.assert_called_once()

    def test_matching_endpoint_id_records_inspection_for_followups(self, catalog_tools):
        tools, client = catalog_tools

        tools["call_central_api"](
            path="/config/v1/widgets",
            method="POST",
            body={"name": "x"},
            endpoint_id="POST:/config/v1/widgets",
        )
        # Subsequent call without endpoint_id must now succeed too.
        tools["call_central_api"](
            path="/config/v1/widgets",
            method="POST",
            body={"name": "y"},
        )
        assert client._request.call_count == 2

    def test_matching_endpoint_id_normalises_path(self, catalog_tools):
        # Path supplied to the tool has no leading slash; endpoint_id has one.
        tools, client = catalog_tools

        out = tools["call_central_api"](
            path="config/v1/widgets",
            method="POST",
            body={"name": "x"},
            endpoint_id="POST:/config/v1/widgets",
        )
        assert "items" in out
        client._request.assert_called_once()

    def test_mismatched_endpoint_id_falls_through_to_gate(self, catalog_tools):
        from mcp.server.fastmcp.exceptions import ToolError

        tools, client = catalog_tools

        with pytest.raises(ToolError) as exc:
            tools["call_central_api"](
                path="/config/v1/widgets",
                method="POST",
                body={"name": "x"},
                endpoint_id="GET:/config/v1/widgets",  # wrong method
            )
        assert "schema not consulted" in str(exc.value).lower()
        client._request.assert_not_called()

    def test_mismatched_endpoint_id_path_falls_through(self, catalog_tools):
        from mcp.server.fastmcp.exceptions import ToolError

        tools, client = catalog_tools

        with pytest.raises(ToolError):
            tools["call_central_api"](
                path="/config/v1/widgets",
                method="POST",
                body={"name": "x"},
                endpoint_id="POST:/config/v1/other",
            )
        client._request.assert_not_called()

    def test_no_endpoint_id_unchanged_behaviour(self, catalog_tools):
        from mcp.server.fastmcp.exceptions import ToolError

        tools, client = catalog_tools

        with pytest.raises(ToolError):
            tools["call_central_api"](path="/config/v1/widgets", method="POST")
        client._request.assert_not_called()

    def test_greenlake_matching_endpoint_id_skips_gate(self, catalog_tools):
        tools, client = catalog_tools

        out = tools["call_greenlake_api"](
            path="/monitoring/v2/aps",
            endpoint_id="GET:/monitoring/v2/aps",
        )
        assert "items" in out
        client._request.assert_called_once()

    def test_greenlake_mismatched_endpoint_id_blocks(self, catalog_tools):
        from mcp.server.fastmcp.exceptions import ToolError

        tools, client = catalog_tools

        with pytest.raises(ToolError):
            tools["call_greenlake_api"](
                path="/monitoring/v2/aps",
                endpoint_id="POST:/monitoring/v2/aps",
            )
        client._request.assert_not_called()


# ── eid_for helper ───────────────────────────────────────────────────


class TestEidFor:

    def test_canonical_form(self):
        from hpe_networking_central_mcp.tools.api_call_policy import eid_for

        assert eid_for("get", "monitoring/v1/aps") == "GET:/monitoring/v1/aps"

    def test_idempotent_with_leading_slash(self):
        from hpe_networking_central_mcp.tools.api_call_policy import eid_for

        assert eid_for("POST", "/x/y") == eid_for("post", "x/y")
