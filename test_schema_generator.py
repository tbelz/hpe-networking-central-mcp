#!/usr/bin/env python3
"""TDD tests for schema_generator — DDL generation from OpenAPI response schemas.

Tests cover:
  1. JSON Schema type → LadybugDB type mapping
  2. Node table extraction from response schemas
  3. Primary key detection
  4. Relationship inference from URL path patterns
  5. Full DDL statement generation
  6. Content hash for schema versioning
  7. Round-trip: generated DDL applied to LadybugDB
"""

from __future__ import annotations

import hashlib
import sys
import tempfile
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpe_networking_central_mcp.oas_index import (
    EndpointEntry,
    OASIndex,
    ParamEntry,
    ResponseEntry,
)

# Module under test — will be created in src/hpe_networking_central_mcp/schema_generator.py
from hpe_networking_central_mcp.schema_generator import (
    NodeTableDef,
    PropertyDef,
    RelTableDef,
    content_hash,
    generate_ddl,
    infer_node_tables,
    infer_rel_tables,
    map_json_type,
)


# ── Fixtures ──────────────────────────────────────────────────────────


def _device_list_endpoint() -> EndpointEntry:
    """GET /monitoring/v1/devices → list of device objects."""
    return EndpointEntry(
        method="GET",
        path="/monitoring/v1/devices",
        summary="List devices",
        description="Returns all monitored devices",
        operation_id="getDevices",
        tags=["Monitoring"],
        category="Monitoring",
        deprecated=False,
        parameters=[
            ParamEntry("limit", "query", False, {"type": "integer"}, "Page size"),
            ParamEntry("offset", "query", False, {"type": "integer"}, "Offset"),
        ],
        responses=[
            ResponseEntry(
                status="200",
                description="OK",
                schema={
                    "type": "object",
                    "properties": {
                        "devices": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "serial": {"type": "string"},
                                    "name": {"type": "string"},
                                    "mac_address": {"type": "string"},
                                    "model": {"type": "string"},
                                    "device_type": {"type": "string"},
                                    "status": {"type": "string"},
                                    "ip_address": {"type": "string"},
                                    "firmware_version": {"type": "string"},
                                    "uptime": {"type": "number"},
                                    "is_managed": {"type": "boolean"},
                                    "labels": {
                                        "type": "array",
                                        "items": {"type": "string"},
                                    },
                                },
                                "required": ["serial"],
                            },
                        },
                        "total": {"type": "integer"},
                    },
                },
            )
        ],
    )


def _interface_list_endpoint() -> EndpointEntry:
    """GET /monitoring/v1/devices/{serial}/interfaces → list of interfaces."""
    return EndpointEntry(
        method="GET",
        path="/monitoring/v1/devices/{serial}/interfaces",
        summary="List device interfaces",
        description="",
        operation_id="getDeviceInterfaces",
        tags=["Monitoring"],
        category="Monitoring",
        deprecated=False,
        parameters=[
            ParamEntry("serial", "path", True, {"type": "string"}, "Device serial"),
        ],
        responses=[
            ResponseEntry(
                status="200",
                description="OK",
                schema={
                    "type": "object",
                    "properties": {
                        "interfaces": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "port_number": {"type": "integer"},
                                    "name": {"type": "string"},
                                    "status": {"type": "string"},
                                    "speed": {"type": "number"},
                                    "duplex": {"type": "string"},
                                    "tx_bytes": {"type": "integer"},
                                    "rx_bytes": {"type": "integer"},
                                    "mac_address": {"type": "string"},
                                    "vlan_id": {"type": "integer"},
                                    "type": {"type": "string"},
                                },
                            },
                        },
                    },
                },
            )
        ],
    )


def _client_list_endpoint() -> EndpointEntry:
    """GET /monitoring/v1/clients → list of clients."""
    return EndpointEntry(
        method="GET",
        path="/monitoring/v1/clients",
        summary="List clients",
        description="",
        operation_id="getClients",
        tags=["Monitoring"],
        category="Monitoring",
        deprecated=False,
        parameters=[],
        responses=[
            ResponseEntry(
                status="200",
                description="OK",
                schema={
                    "type": "object",
                    "properties": {
                        "clients": {
                            "type": "array",
                            "items": {
                                "type": "object",
                                "properties": {
                                    "macaddr": {"type": "string"},
                                    "name": {"type": "string"},
                                    "ip_address": {"type": "string"},
                                    "os_type": {"type": "string"},
                                    "connected_device_serial": {"type": "string"},
                                    "connection_type": {"type": "string"},
                                    "signal_db": {"type": "integer"},
                                    "speed_mbps": {"type": "number"},
                                },
                                "required": ["macaddr"],
                            },
                        },
                    },
                },
            )
        ],
    )


def _site_list_endpoint() -> EndpointEntry:
    """GET /central/v2/sites → sites (top-level array response)."""
    return EndpointEntry(
        method="GET",
        path="/central/v2/sites",
        summary="List sites",
        description="",
        operation_id="getSites",
        tags=["Central"],
        category="Central",
        deprecated=False,
        parameters=[],
        responses=[
            ResponseEntry(
                status="200",
                description="OK",
                schema={
                    "type": "array",
                    "items": {
                        "type": "object",
                        "properties": {
                            "site_id": {"type": "string"},
                            "site_name": {"type": "string"},
                            "address": {"type": "string"},
                            "city": {"type": "string"},
                            "latitude": {"type": "number"},
                            "longitude": {"type": "number"},
                            "device_count": {"type": "integer"},
                        },
                        "required": ["site_id"],
                    },
                },
            )
        ],
    )


def _post_only_endpoint() -> EndpointEntry:
    """POST /central/v1/actions/reboot — no list response, should be skipped."""
    return EndpointEntry(
        method="POST",
        path="/central/v1/actions/reboot",
        summary="Reboot device",
        description="",
        operation_id="rebootDevice",
        tags=["Actions"],
        category="Actions",
        deprecated=False,
        parameters=[],
        responses=[
            ResponseEntry(
                status="200",
                description="OK",
                schema={"type": "object", "properties": {"task_id": {"type": "string"}}},
            )
        ],
    )


def _build_test_index(*endpoints: EndpointEntry) -> OASIndex:
    """Build an OASIndex pre-loaded with test endpoints."""
    idx = OASIndex()
    idx._entries = list(endpoints)
    for ep in endpoints:
        idx._categories[ep.category] = idx._categories.get(ep.category, 0) + 1
    return idx


# ══════════════════════════════════════════════════════════════════════
# Test 1: JSON Schema type → LadybugDB type mapping
# ══════════════════════════════════════════════════════════════════════


class TestTypeMapping:
    def test_string(self):
        assert map_json_type({"type": "string"}) == "STRING"

    def test_integer(self):
        assert map_json_type({"type": "integer"}) == "INT64"

    def test_number(self):
        assert map_json_type({"type": "number"}) == "DOUBLE"

    def test_boolean(self):
        assert map_json_type({"type": "boolean"}) == "BOOLEAN"

    def test_string_array(self):
        assert map_json_type({"type": "array", "items": {"type": "string"}}) == "STRING[]"

    def test_int_array(self):
        assert map_json_type({"type": "array", "items": {"type": "integer"}}) == "INT64[]"

    def test_object_becomes_string(self):
        """Nested objects are JSON-serialized as STRING."""
        assert map_json_type({"type": "object"}) == "STRING"

    def test_object_array_becomes_string(self):
        """Arrays of objects are JSON-serialized as STRING."""
        assert (
            map_json_type({"type": "array", "items": {"type": "object"}}) == "STRING"
        )

    def test_missing_type_defaults_string(self):
        assert map_json_type({}) == "STRING"

    def test_nullable_string(self):
        """Nullable types should still map correctly."""
        assert map_json_type({"type": "string", "nullable": True}) == "STRING"

    def test_int64_format(self):
        assert map_json_type({"type": "integer", "format": "int64"}) == "INT64"

    def test_double_format(self):
        assert map_json_type({"type": "number", "format": "double"}) == "DOUBLE"


# ══════════════════════════════════════════════════════════════════════
# Test 2: Node table extraction from response schemas
# ══════════════════════════════════════════════════════════════════════


class TestNodeTableExtraction:
    def test_extracts_device_table(self):
        idx = _build_test_index(_device_list_endpoint())
        tables = infer_node_tables(idx)
        names = {t.name for t in tables}
        assert "Device" in names

    def test_device_properties(self):
        idx = _build_test_index(_device_list_endpoint())
        tables = infer_node_tables(idx)
        device = next(t for t in tables if t.name == "Device")
        prop_names = {p.name for p in device.properties}
        assert "serial" in prop_names
        assert "name" in prop_names
        assert "mac_address" in prop_names
        assert "firmware_version" in prop_names

    def test_extracts_interface_table(self):
        idx = _build_test_index(_interface_list_endpoint())
        tables = infer_node_tables(idx)
        names = {t.name for t in tables}
        assert "Interface" in names

    def test_extracts_client_table(self):
        idx = _build_test_index(_client_list_endpoint())
        tables = infer_node_tables(idx)
        names = {t.name for t in tables}
        assert "Client" in names

    def test_top_level_array_response(self):
        """Array response (not wrapped in object) should also extract a table."""
        idx = _build_test_index(_site_list_endpoint())
        tables = infer_node_tables(idx)
        names = {t.name for t in tables}
        assert "Site" in names

    def test_skips_action_endpoints(self):
        """POST endpoints without list responses should not create tables."""
        idx = _build_test_index(_post_only_endpoint())
        tables = infer_node_tables(idx)
        assert len(tables) == 0

    def test_deduplicates_same_resource(self):
        """Multiple endpoints for same resource merge into one table."""
        ep1 = _device_list_endpoint()
        ep2 = EndpointEntry(
            method="GET",
            path="/monitoring/v1/devices/{serial}",
            summary="Get device",
            description="",
            operation_id="getDevice",
            tags=["Monitoring"],
            category="Monitoring",
            deprecated=False,
            parameters=[
                ParamEntry("serial", "path", True, {"type": "string"}, ""),
            ],
            responses=[
                ResponseEntry(
                    status="200",
                    description="OK",
                    schema={
                        "type": "object",
                        "properties": {
                            "serial": {"type": "string"},
                            "name": {"type": "string"},
                            "extra_field": {"type": "string"},
                        },
                    },
                )
            ],
        )
        idx = _build_test_index(ep1, ep2)
        tables = infer_node_tables(idx)
        device_tables = [t for t in tables if t.name == "Device"]
        assert len(device_tables) == 1
        # Merged table should include properties from both endpoints
        prop_names = {p.name for p in device_tables[0].properties}
        assert "extra_field" in prop_names
        assert "serial" in prop_names

    def test_property_types_correct(self):
        idx = _build_test_index(_device_list_endpoint())
        tables = infer_node_tables(idx)
        device = next(t for t in tables if t.name == "Device")
        type_map = {p.name: p.db_type for p in device.properties}
        assert type_map["serial"] == "STRING"
        assert type_map["uptime"] == "DOUBLE"
        assert type_map["is_managed"] == "BOOLEAN"
        assert type_map["labels"] == "STRING[]"


# ══════════════════════════════════════════════════════════════════════
# Test 3: Primary key detection
# ══════════════════════════════════════════════════════════════════════


class TestPrimaryKeyDetection:
    def test_serial_is_pk_for_device(self):
        idx = _build_test_index(_device_list_endpoint())
        tables = infer_node_tables(idx)
        device = next(t for t in tables if t.name == "Device")
        assert device.primary_key == "serial"

    def test_macaddr_is_pk_for_client(self):
        idx = _build_test_index(_client_list_endpoint())
        tables = infer_node_tables(idx)
        client = next(t for t in tables if t.name == "Client")
        assert client.primary_key == "macaddr"

    def test_site_id_is_pk_for_site(self):
        idx = _build_test_index(_site_list_endpoint())
        tables = infer_node_tables(idx)
        site = next(t for t in tables if t.name == "Site")
        assert site.primary_key == "site_id"

    def test_composite_name_pk_fallback(self):
        """When no clear PK, synthesize one from the resource name."""
        ep = EndpointEntry(
            method="GET",
            path="/monitoring/v1/alerts",
            summary="List alerts",
            description="",
            operation_id="getAlerts",
            tags=[],
            category="Monitoring",
            deprecated=False,
            parameters=[],
            responses=[
                ResponseEntry(
                    status="200",
                    description="OK",
                    schema={
                        "type": "object",
                        "properties": {
                            "alerts": {
                                "type": "array",
                                "items": {
                                    "type": "object",
                                    "properties": {
                                        "message": {"type": "string"},
                                        "severity": {"type": "string"},
                                        "timestamp": {"type": "string"},
                                    },
                                },
                            },
                        },
                    },
                )
            ],
        )
        idx = _build_test_index(ep)
        tables = infer_node_tables(idx)
        alert = next(t for t in tables if t.name == "Alert")
        # Should have a synthetic PK
        assert alert.primary_key is not None
        assert len(alert.primary_key) > 0


# ══════════════════════════════════════════════════════════════════════
# Test 4: Relationship inference from URL path patterns
# ══════════════════════════════════════════════════════════════════════


class TestRelationshipInference:
    def test_device_has_interface(self):
        """Nested URL /devices/{serial}/interfaces → Device HAS_INTERFACE Interface."""
        idx = _build_test_index(_device_list_endpoint(), _interface_list_endpoint())
        node_tables = infer_node_tables(idx)
        rels = infer_rel_tables(idx, node_tables)
        rel_names = {r.name for r in rels}
        assert "HAS_INTERFACE" in rel_names
        rel = next(r for r in rels if r.name == "HAS_INTERFACE")
        assert rel.from_table == "Device"
        assert rel.to_table == "Interface"

    def test_no_spurious_rels_for_flat_urls(self):
        """Top-level resources without nesting should not create rels."""
        idx = _build_test_index(_device_list_endpoint(), _client_list_endpoint())
        node_tables = infer_node_tables(idx)
        rels = infer_rel_tables(idx, node_tables)
        # Device and Client are at the same level, no containment
        rel_pairs = {(r.from_table, r.to_table) for r in rels}
        assert ("Device", "Client") not in rel_pairs
        assert ("Client", "Device") not in rel_pairs

    def test_deep_nesting(self):
        """Three-level nesting: /a/{id}/b/{id}/c → A HAS_B B, B HAS_C C."""
        ep_a = EndpointEntry(
            method="GET", path="/api/v1/orgs", summary="List orgs", description="",
            operation_id="getOrgs", tags=[], category="Test", deprecated=False,
            parameters=[],
            responses=[ResponseEntry("200", "", {
                "type": "object", "properties": {
                    "orgs": {"type": "array", "items": {
                        "type": "object", "properties": {
                            "org_id": {"type": "string"}, "name": {"type": "string"},
                        }, "required": ["org_id"],
                    }},
                },
            })],
        )
        ep_b = EndpointEntry(
            method="GET", path="/api/v1/orgs/{org_id}/teams", summary="List teams",
            description="", operation_id="getTeams", tags=[], category="Test",
            deprecated=False,
            parameters=[ParamEntry("org_id", "path", True, {"type": "string"}, "")],
            responses=[ResponseEntry("200", "", {
                "type": "object", "properties": {
                    "teams": {"type": "array", "items": {
                        "type": "object", "properties": {
                            "team_id": {"type": "string"}, "name": {"type": "string"},
                        }, "required": ["team_id"],
                    }},
                },
            })],
        )
        ep_c = EndpointEntry(
            method="GET", path="/api/v1/orgs/{org_id}/teams/{team_id}/members",
            summary="List members", description="", operation_id="getMembers",
            tags=[], category="Test", deprecated=False,
            parameters=[
                ParamEntry("org_id", "path", True, {"type": "string"}, ""),
                ParamEntry("team_id", "path", True, {"type": "string"}, ""),
            ],
            responses=[ResponseEntry("200", "", {
                "type": "object", "properties": {
                    "members": {"type": "array", "items": {
                        "type": "object", "properties": {
                            "member_id": {"type": "string"}, "email": {"type": "string"},
                        }, "required": ["member_id"],
                    }},
                },
            })],
        )
        idx = _build_test_index(ep_a, ep_b, ep_c)
        node_tables = infer_node_tables(idx)
        rels = infer_rel_tables(idx, node_tables)
        rel_pairs = {(r.from_table, r.to_table) for r in rels}
        assert ("Org", "Team") in rel_pairs
        assert ("Team", "Member") in rel_pairs


# ══════════════════════════════════════════════════════════════════════
# Test 5: DDL statement generation
# ══════════════════════════════════════════════════════════════════════


class TestDDLGeneration:
    def test_node_table_ddl(self):
        node = NodeTableDef(
            name="Device",
            primary_key="serial",
            properties=[
                PropertyDef("serial", "STRING"),
                PropertyDef("name", "STRING"),
                PropertyDef("status", "STRING"),
                PropertyDef("uptime", "DOUBLE"),
            ],
            source_endpoints=["GET:/monitoring/v1/devices"],
        )
        ddl_list = generate_ddl([node], [])
        assert len(ddl_list) == 1
        ddl = ddl_list[0]
        assert "CREATE NODE TABLE IF NOT EXISTS Device" in ddl
        assert "serial STRING" in ddl
        assert "PRIMARY KEY (serial)" in ddl
        assert "uptime DOUBLE" in ddl

    def test_rel_table_ddl(self):
        rel = RelTableDef(
            name="HAS_INTERFACE",
            from_table="Device",
            to_table="Interface",
        )
        ddl_list = generate_ddl([], [rel])
        assert len(ddl_list) == 1
        ddl = ddl_list[0]
        assert "CREATE REL TABLE IF NOT EXISTS HAS_INTERFACE" in ddl
        assert "FROM Device TO Interface" in ddl

    def test_combined_ddl(self):
        nodes = [
            NodeTableDef("A", "id", [PropertyDef("id", "STRING")], []),
            NodeTableDef("B", "id", [PropertyDef("id", "STRING")], []),
        ]
        rels = [RelTableDef("A_TO_B", "A", "B")]
        ddl_list = generate_ddl(nodes, rels)
        # Node tables first, then rels
        assert len(ddl_list) == 3
        assert "A" in ddl_list[0]
        assert "B" in ddl_list[1]
        assert "A_TO_B" in ddl_list[2]

    def test_ddl_applies_to_ladybugdb(self):
        """Generated DDL must actually execute against LadybugDB."""
        import real_ladybug as lb

        node = NodeTableDef(
            name="TestDevice",
            primary_key="serial",
            properties=[
                PropertyDef("serial", "STRING"),
                PropertyDef("name", "STRING"),
                PropertyDef("count", "INT64"),
            ],
            source_endpoints=[],
        )
        ddl_list = generate_ddl([node], [])

        with tempfile.TemporaryDirectory() as tmp:
            db = lb.Database(str(Path(tmp) / "test_db"))
            conn = lb.Connection(db)
            for ddl in ddl_list:
                conn.execute(ddl.strip())
            # Verify table exists
            conn.execute(
                "CREATE (d:TestDevice {serial: $s, name: $n, count: $c})",
                parameters={"s": "SN001", "n": "test", "c": 42},
            )
            rows = list(
                conn.execute(
                    "MATCH (d:TestDevice) RETURN d.serial AS serial, d.count AS cnt"
                ).rows_as_dict()
            )
            assert len(rows) == 1
            assert rows[0]["serial"] == "SN001"
            assert rows[0]["cnt"] == 42
            db.close()


# ══════════════════════════════════════════════════════════════════════
# Test 6: Content hash for schema versioning
# ══════════════════════════════════════════════════════════════════════


class TestContentHash:
    def test_deterministic(self):
        ddl = ["CREATE NODE TABLE T (id STRING, PRIMARY KEY (id))"]
        h1 = content_hash(ddl)
        h2 = content_hash(ddl)
        assert h1 == h2

    def test_changes_on_different_ddl(self):
        ddl_a = ["CREATE NODE TABLE A (id STRING, PRIMARY KEY (id))"]
        ddl_b = ["CREATE NODE TABLE B (id STRING, PRIMARY KEY (id))"]
        assert content_hash(ddl_a) != content_hash(ddl_b)

    def test_returns_hex_string(self):
        ddl = ["CREATE NODE TABLE T (id STRING, PRIMARY KEY (id))"]
        h = content_hash(ddl)
        assert isinstance(h, str)
        assert len(h) == 16  # truncated sha256


# ══════════════════════════════════════════════════════════════════════
# Test 7: Full round-trip with OASIndex
# ══════════════════════════════════════════════════════════════════════


class TestFullRoundTrip:
    def test_full_pipeline(self):
        """End-to-end: OASIndex → infer tables → generate DDL → apply to DB."""
        import real_ladybug as lb

        idx = _build_test_index(
            _device_list_endpoint(),
            _interface_list_endpoint(),
            _client_list_endpoint(),
        )

        node_tables = infer_node_tables(idx)
        rel_tables = infer_rel_tables(idx, node_tables)
        ddl = generate_ddl(node_tables, rel_tables)

        assert len(ddl) > 0

        with tempfile.TemporaryDirectory() as tmp:
            db = lb.Database(str(Path(tmp) / "roundtrip_db"))
            conn = lb.Connection(db)
            for stmt in ddl:
                conn.execute(stmt.strip())

            # Verify we can insert data matching the generated schema
            conn.execute(
                "CREATE (d:Device {serial: $s, name: $n})",
                parameters={"s": "SN123", "n": "test-ap"},
            )
            rows = list(
                conn.execute("MATCH (d:Device) RETURN d.serial AS s").rows_as_dict()
            )
            assert rows[0]["s"] == "SN123"

            # Content hash should be stable
            h = content_hash(ddl)
            assert len(h) == 16

            db.close()


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
