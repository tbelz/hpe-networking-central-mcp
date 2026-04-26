"""Tests for ``describe_endpoint_for_device`` (Phase 2D-2 of ADR 009).

The tool returns one structured row per leaf property of an endpoint's
request body (or 200 response if no request body), with optional
device-type filtering. Backed by the Property subgraph from Phase 2C.
"""

from __future__ import annotations

import copy
import json
import sys
from pathlib import Path
from tempfile import TemporaryDirectory

import pytest

_SRC_DIR = Path(__file__).resolve().parent.parent / "src"
if str(_SRC_DIR) not in sys.path:
    sys.path.insert(0, str(_SRC_DIR))

from hpe_networking_central_mcp.graph.manager import GraphManager  # noqa: E402
from hpe_networking_central_mcp.tools.describe import describe_endpoint  # noqa: E402


# ── Synthetic spec ──────────────────────────────────────────────────


def _ntp_spec() -> dict:
    auth = {
        "type": "object",
        "properties": {
            "authenticate": {
                "type": "boolean",
                "x-supportedDeviceType": ["Gateway", "Switch CX"],
            },
            "key-value": {
                "type": "string",
                "x-supportedDeviceType": ["Switch CX"],
                "x-path": "/ac-ntp:ntp/ac-ntp:auth/ac-ntp:key-value",
            },
        },
    }
    server = {
        "type": "object",
        "properties": {
            "server": {
                "type": "string",
                "x-supportedDeviceType": ["Switch PVOS", "Gateway", "Switch CX"],
            },
            "id": {"type": "string", "readOnly": True},
        },
    }
    ntp_profile = {
        "allOf": [
            {"$ref": "#/components/schemas/AuthenticationConfig"},
            {"$ref": "#/components/schemas/ServerConfig"},
            {
                "properties": {
                    "name": {
                        "type": "string",
                        "x-supportedDeviceType": [
                            "Switch PVOS", "Gateway", "Switch CX",
                        ],
                    },
                    "vrf": {
                        "type": "string",
                        "x-supportedDeviceType": ["Switch CX"],
                    },
                }
            },
        ],
    }
    return {
        "openapi": "3.0.0",
        "info": {"title": "NTP API"},
        "components": {
            "schemas": {
                "NtpprofileSchema": copy.deepcopy(ntp_profile),
                "AuthenticationConfig": copy.deepcopy(auth),
                "ServerConfig": copy.deepcopy(server),
            }
        },
        "paths": {
            "/v1/ntp": {
                "post": {
                    "operationId": "createNtp",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/NtpprofileSchema"}
                            }
                        },
                    },
                    "responses": {"200": {"description": "ok"}},
                }
            },
            "/v1/aps": {
                "get": {
                    "operationId": "listAps",
                    "responses": {
                        "200": {
                            "description": "list",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/ServerConfig"}
                                }
                            },
                        }
                    },
                }
            },
        },
    }


@pytest.fixture
def gm(tmp_path):
    """Fresh GraphManager populated with the NTP spec."""
    db_path = tmp_path / "describe.db"
    gm = GraphManager(db_path)
    gm.initialize()

    from hpe_networking_central_mcp.oas_schema_graph import populate_schema_graph
    import real_ladybug as lb

    spec = _ntp_spec()
    conn = lb.Connection(gm._db)
    # Seed both endpoints.
    for method, path, op_id in [
        ("POST", "/v1/ntp", "createNtp"),
        ("GET", "/v1/aps", "listAps"),
    ]:
        eid = f"{method}:{path}"
        conn.execute(
            "CREATE (e:ApiEndpoint {endpoint_id: $eid, method: $m, path: $p, "
            "summary: '', description: '', operationId: $op, category: '', "
            "deprecated: false, parameters: '', requestBody: '', responses: ''})",
            parameters={"eid": eid, "m": method, "p": path, "op": op_id},
        )
    populate_schema_graph(
        conn,
        spec_source="central",
        spec=spec,
        endpoints=[("POST", "/v1/ntp"), ("GET", "/v1/aps")],
    )
    yield gm


# ── Tests ───────────────────────────────────────────────────────────


class TestDescribeEndpoint:
    def test_returns_request_body_properties(self, gm):
        result = describe_endpoint(gm, "POST", "/v1/ntp")
        assert result["source"] == "requestBody"
        names = {p["name"] for p in result["properties"]}
        # Flattened across allOf branches.
        assert {"authenticate", "key-value", "server", "id", "name", "vrf"} <= names

    def test_normalises_path(self, gm):
        # Without the leading slash should still match.
        result = describe_endpoint(gm, "POST", "v1/ntp")
        assert result["path"] == "/v1/ntp"
        assert result["properties"]

    def test_unknown_endpoint_returns_empty(self, gm):
        result = describe_endpoint(gm, "POST", "/does/not/exist")
        assert result["properties"] == []
        assert result["source"] == ""

    def test_falls_back_to_response_when_no_request_body(self, gm):
        result = describe_endpoint(gm, "GET", "/v1/aps")
        assert result["source"] == "response:200"
        names = {p["name"] for p in result["properties"]}
        assert "server" in names

    def test_extensions_json_parsed_into_dict(self, gm):
        result = describe_endpoint(gm, "POST", "/v1/ntp")
        kv = next(p for p in result["properties"] if p["name"] == "key-value")
        assert isinstance(kv["extensions"], dict)
        assert kv["extensions"]["x-supportedDeviceType"] == ["Switch CX"]
        assert kv["extensions"]["x-path"] == "/ac-ntp:ntp/ac-ntp:auth/ac-ntp:key-value"

    def test_inheritedFrom_passes_through(self, gm):
        result = describe_endpoint(gm, "POST", "/v1/ntp")
        by_name = {p["name"]: p for p in result["properties"]}
        # `key-value` was contributed by AuthenticationConfig via allOf.
        assert by_name["key-value"]["inheritedFrom"] == "AuthenticationConfig"
        # `name` came from the inline allOf branch — direct on parent.
        assert by_name["name"]["inheritedFrom"] == ""

    def test_readOnly_passes_through(self, gm):
        result = describe_endpoint(gm, "GET", "/v1/aps")
        by_name = {p["name"]: p for p in result["properties"]}
        assert by_name["id"]["readOnly"] is True
        assert by_name["server"]["readOnly"] is False

    def test_device_type_filter_drops_non_matching(self, gm):
        result = describe_endpoint(gm, "POST", "/v1/ntp", device_type="Switch CX")
        names = {p["name"] for p in result["properties"]}
        # All NTP profile fields apply to Switch CX in this fixture.
        assert {"authenticate", "key-value", "server", "name", "vrf"} <= names

    def test_device_type_filter_excludes_other_only_fields(self, gm):
        result = describe_endpoint(gm, "POST", "/v1/ntp", device_type="Switch PVOS")
        names = {p["name"] for p in result["properties"]}
        # `authenticate`, `key-value`, `vrf` are not in PVOS lists.
        assert "authenticate" not in names
        assert "key-value" not in names
        assert "vrf" not in names
        # `server` and `name` ARE allowed for PVOS.
        assert "server" in names
        assert "name" in names

    def test_empty_supportedDeviceTypes_means_applies_everywhere(self, gm, tmp_path):
        """A property with no x-supportedDeviceType should not be filtered out."""
        # `id` on ServerConfig has no x-supportedDeviceType set.
        result = describe_endpoint(gm, "GET", "/v1/aps", device_type="Switch CX")
        names = {p["name"] for p in result["properties"]}
        assert "id" in names  # would have been filtered if we required exact match


class TestRegisterTool:
    def test_tool_registered_and_returns_json(self, gm, tmp_path):
        from mcp.server.fastmcp import FastMCP
        from hpe_networking_central_mcp.config import Settings
        from hpe_networking_central_mcp.tools.describe import register_describe_tools

        settings = Settings(
            central_base_url="https://x",
            central_client_id="cid",
            central_client_secret="csec",
            script_library_path=tmp_path / "lib",
        )
        mcp = FastMCP("test")
        register_describe_tools(mcp, settings, gm)
        tool = mcp._tool_manager._tools["describe_endpoint_for_device"]
        out = json.loads(tool.fn(method="POST", path="/v1/ntp"))
        assert out["source"] == "requestBody"
        assert any(p["name"] == "vrf" for p in out["properties"])
