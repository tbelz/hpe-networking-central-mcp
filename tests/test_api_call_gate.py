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
    EndpointRegistry,
    InspectionTracker,
    check_call_policy,
    get_registry,
    get_tracker,
    normalise_path,
    register_endpoints,
    set_skeleton_fetcher,
)


@pytest.fixture(autouse=True)
def _reset_policy_state():
    """Each test starts with empty tracker, empty registry, no fetcher."""
    get_tracker().reset()
    get_registry().reset()
    set_skeleton_fetcher(None)
    yield
    get_tracker().reset()
    get_registry().reset()
    set_skeleton_fetcher(None)


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
        # The gate should inline the endpoint skeleton (registered by
        # register_catalog_tools via set_skeleton_fetcher) so the agent
        # has the schema in hand without a second round-trip.
        msg = str(exc.value)
        assert "schema not consulted" in msg.lower()
        assert "/monitoring/v2/aps" in msg
        assert "listAPs" in msg, "skeleton should be inlined in block message"
        client._request.assert_not_called()

    def test_blocked_call_auto_records_inspection_for_retry(self, catalog_tools):
        tools, client = catalog_tools
        from mcp.server.fastmcp.exceptions import ToolError

        # First call blocks but inlines the skeleton — agent has the schema.
        with pytest.raises(ToolError):
            tools["call_central_api"](path="/monitoring/v2/aps")
        # Immediate retry must succeed without an explicit
        # get_api_endpoint_detail call: the gate auto-records the
        # inspection when it inlines the skeleton.
        out = tools["call_central_api"](path="/monitoring/v2/aps")
        assert "items" in out
        client._request.assert_called_once()

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
        assert "schema not consulted" in str(exc.value).lower()

    def test_inspection_for_one_endpoint_does_not_unlock_another(self, catalog_tools):
        tools, _ = catalog_tools
        from mcp.server.fastmcp.exceptions import ToolError

        tools["get_api_endpoint_detail"](
            method="GET", path="/monitoring/v2/aps"
        )

        with pytest.raises(ToolError) as exc:
            tools["call_central_api"](path="/monitoring/v2/switches")
        # /switches is not in the test graph, so the fetcher returns None
        # and we fall back to the explicit-instruction message.
        assert "get_api_endpoint_detail" in str(exc.value)


# ── Template-aware matching ──────────────────────────────────────────


class TestTemplateMatching:
    """The gate must resolve concrete paths back to catalog templates.

    Without this, every device-specific path (``.../{serial-number}/...``
    substituted with a real serial) would look like an uninspected
    endpoint to the gate even after the agent inspected the template.
    """

    def test_concrete_path_allowed_after_template_inspection(self):
        register_endpoints({
            "GET": ["/monitoring/v2/aps/{serial-number}/ports"],
        })
        # Agent inspected the template, as catalog tools record it.
        get_tracker().record(
            "GET", "/monitoring/v2/aps/{serial-number}/ports", "skeleton"
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
        # Block message must reference the *template*, not the concrete path.
        assert "{serial-number}" in (reason or "")

    def test_unknown_concrete_path_falls_through_to_literal_check(self):
        # No templates registered; behaviour matches the original literal-only gate.
        allowed, _ = check_call_policy("GET", "/foo/bar")
        assert allowed is False
        get_tracker().record("GET", "/foo/bar", "skeleton")
        allowed, _ = check_call_policy("GET", "/foo/bar")
        assert allowed is True

    def test_method_isolation_in_template_match(self):
        register_endpoints({
            "GET": ["/devices/{id}"],
            "POST": ["/devices/{id}"],
        })
        get_tracker().record("GET", "/devices/{id}", "skeleton")
        # GET on a concrete path resolves to the GET template — allowed.
        ok_get, _ = check_call_policy("GET", "/devices/abc")
        assert ok_get is True
        # POST on the same concrete path resolves to the POST template
        # which has *not* been inspected — blocked.
        ok_post, _ = check_call_policy("POST", "/devices/abc")
        assert ok_post is False


# ── Skeleton inlining seam ───────────────────────────────────────────


class TestSkeletonFetcherSeam:

    def test_fetcher_invoked_with_template_path(self):
        register_endpoints({"GET": ["/x/{id}/y"]})
        seen: list[tuple[str, str]] = []

        def fetcher(method: str, template: str) -> str | None:
            seen.append((method, template))
            return '{"operation_id": "demo"}'

        set_skeleton_fetcher(fetcher)
        allowed, reason = check_call_policy("GET", "/x/123/y")
        assert allowed is False
        assert seen == [("GET", "/x/{id}/y")]
        assert "demo" in (reason or "")

    def test_fetcher_returning_none_falls_back_to_legacy_message(self):
        register_endpoints({"GET": ["/x/{id}/y"]})
        set_skeleton_fetcher(lambda m, p: None)
        allowed, reason = check_call_policy("GET", "/x/123/y")
        assert allowed is False
        assert "get_api_endpoint_detail" in (reason or "")

    def test_fetcher_exceptions_are_swallowed(self):
        register_endpoints({"GET": ["/x/{id}/y"]})

        def boom(method: str, template: str) -> str | None:
            raise RuntimeError("graph offline")

        set_skeleton_fetcher(boom)
        allowed, reason = check_call_policy("GET", "/x/123/y")
        # Fetcher errors must not break the gate — fall back to legacy text.
        assert allowed is False
        assert "get_api_endpoint_detail" in (reason or "")


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
        # ``{x}`` must not span ``/`` — ``/a/1/2`` should NOT match.
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

