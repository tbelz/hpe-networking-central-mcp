"""Tests for the property-level API schema subgraph (Phase 2C of ADR 009).

These tests pin down the Property node, vendor-extension extraction
(``x-supportedDeviceType``, ``x-path``, all other ``x-*`` preserved as
JSON), allOf flattening into ``HAS_PROPERTY`` edges with provenance,
and ``COMPOSED_OF`` edges between components.

The synthetic spec mimics the actual Aruba Central NTP profile shape:
a top-level ``NtpprofileSchema`` that is purely an ``allOf`` over
several modular config components, each of which carries
``x-supportedDeviceType`` and friends on the leaves.
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

import real_ladybug as lb  # noqa: E402

from hpe_networking_central_mcp.graph.schema import (  # noqa: E402
    KNOWLEDGE_NODE_TABLES,
    KNOWLEDGE_REL_TABLES,
    NODE_TABLES,
    REL_TABLES,
    get_node_tables,
    get_rel_tables,
)


# ── Synthetic NTP-style spec ─────────────────────────────────────────


def _make_ntp_like_spec() -> dict:
    """An NTP-flavoured spec that exercises allOf flattening + x-* extensions.

    Structure::

        NtpprofileSchema
          allOf:
            $ref AuthenticationConfig   (properties: authenticate, key-value)
            $ref ServerConfig           (properties: server)
            inline { properties: name, vrf }
    """
    auth = {
        "type": "object",
        "description": "Authentication config",
        "properties": {
            "authenticate": {
                "type": "boolean",
                "description": "Enable auth",
                "x-supportedDeviceType": ["Gateway", "Switch CX"],
            },
            "key-value": {
                "type": "string",
                "description": "Auth secret",
                "x-supportedDeviceType": ["Switch CX"],
                "x-path": "/ac-ntp:ntp/ac-ntp:auth/ac-ntp:key-value",
                "x-typeDescription": "Secret string",
            },
        },
    }
    server = {
        "type": "object",
        "description": "Server config",
        "properties": {
            "server": {
                "type": "string",
                "description": "NTP server hostname",
                "x-supportedDeviceType": ["Switch PVOS", "Gateway", "Switch CX"],
            }
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
                        "description": "Profile name",
                        "x-supportedDeviceType": [
                            "Switch PVOS",
                            "Gateway",
                            "Switch CX",
                        ],
                    },
                    "vrf": {
                        "type": "string",
                        "description": "Client VRF",
                        "x-supportedDeviceType": ["Switch CX"],
                        "x-path": "/ac-vrf:vrfs/ac-vrf:vrf/ac-vrf:name",
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
                    "summary": "Create NTP profile",
                    "operationId": "createNtp",
                    "requestBody": {
                        "required": True,
                        "content": {
                            "application/json": {
                                "schema": {"$ref": "#/components/schemas/NtpprofileSchema"}
                            }
                        },
                    },
                    "responses": {
                        "201": {
                            "description": "created",
                            "content": {
                                "application/json": {
                                    "schema": {"$ref": "#/components/schemas/NtpprofileSchema"}
                                }
                            },
                        }
                    },
                }
            }
        },
    }


@pytest.fixture
def fresh_db():
    with TemporaryDirectory(prefix="phase2c_") as tmp:
        db_path = Path(tmp) / "graph_db"
        db = lb.Database(str(db_path))
        conn = lb.Connection(db)
        for ddl in NODE_TABLES + KNOWLEDGE_NODE_TABLES + REL_TABLES + KNOWLEDGE_REL_TABLES:
            conn.execute(ddl.strip())
        yield db, conn


def _seed_endpoint(conn, method: str, path: str) -> str:
    eid = f"{method}:{path}"
    conn.execute(
        "CREATE (e:ApiEndpoint {endpoint_id: $eid, method: $m, path: $p, "
        "summary: '', description: '', operationId: '', category: '', "
        "deprecated: false, parameters: '', requestBody: '', responses: '', "
        "bodySkeletonJson: '', bodyGlossaryJson: '', bodyComponentsJson: ''})",
        parameters={"eid": eid, "m": method, "p": path},
    )
    return eid


def _seed(conn) -> None:
    from hpe_networking_central_mcp.oas_schema_graph import populate_schema_graph

    spec = _make_ntp_like_spec()
    _seed_endpoint(conn, "POST", "/v1/ntp")
    populate_schema_graph(
        conn,
        spec_source="central",
        spec=spec,
        endpoints=[("POST", "/v1/ntp")],
    )


# ── DDL surface ──────────────────────────────────────────────────────


class TestPropertyDDL:
    def test_property_node_table_registered(self):
        assert "Property" in set(get_node_tables())

    def test_new_rel_tables_registered(self):
        rels = set(get_rel_tables())
        assert "HAS_PROPERTY" in rels
        assert "PROPERTY_OF_TYPE" in rels
        assert "COMPOSED_OF" in rels


# ── Population ───────────────────────────────────────────────────────


class TestPopulateProperties:
    def test_components_have_their_own_properties(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'AuthenticationConfig'})-[:HAS_PROPERTY]->(p:Property) "
            "RETURN p.name AS name ORDER BY p.name"
        ).rows_as_dict())
        names = [r["name"] for r in rows]
        assert names == ["authenticate", "key-value"]

    def test_property_carries_supported_device_types(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'AuthenticationConfig'})-[:HAS_PROPERTY]->(p:Property {name: 'key-value'}) "
            "RETURN p.supportedDeviceTypes AS sdt"
        ).rows_as_dict())
        assert rows
        assert rows[0]["sdt"] == ["Switch CX"]

    def test_property_carries_yang_path(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'AuthenticationConfig'})-[:HAS_PROPERTY]->(p:Property {name: 'key-value'}) "
            "RETURN p.yangPath AS yp"
        ).rows_as_dict())
        assert rows
        assert rows[0]["yp"] == "/ac-ntp:ntp/ac-ntp:auth/ac-ntp:key-value"

    def test_extensions_json_preserves_other_x_keys(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'AuthenticationConfig'})-[:HAS_PROPERTY]->(p:Property {name: 'key-value'}) "
            "RETURN p.extensionsJson AS ext"
        ).rows_as_dict())
        assert rows
        ext = json.loads(rows[0]["ext"]) if rows[0]["ext"] else {}
        assert ext.get("x-typeDescription") == "Secret string"
        # Also includes the typed-extracted ones for completeness:
        assert ext.get("x-supportedDeviceType") == ["Switch CX"]
        assert ext.get("x-path") == "/ac-ntp:ntp/ac-ntp:auth/ac-ntp:key-value"

    def test_property_description_is_extracted(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'AuthenticationConfig'})-[:HAS_PROPERTY]->(p:Property {name: 'authenticate'}) "
            "RETURN p.description AS d, p.type AS t"
        ).rows_as_dict())
        assert rows
        assert rows[0]["d"] == "Enable auth"
        assert rows[0]["t"] == "boolean"

    def test_inline_properties_use_empty_inheritedFrom(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'AuthenticationConfig'})-[:HAS_PROPERTY]->(p:Property) "
            "RETURN p.inheritedFrom AS i ORDER BY p.name"
        ).rows_as_dict())
        assert all(r["i"] == "" for r in rows), rows


# ── allOf flattening ────────────────────────────────────────────────


class TestAllOfFlattening:
    def test_composed_of_edges_recorded(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (a:SchemaComponent {name: 'NtpprofileSchema'})-[r:COMPOSED_OF]->(b:SchemaComponent) "
            "RETURN b.name AS name, r.kind AS kind ORDER BY b.name"
        ).rows_as_dict())
        names = sorted({(r["name"], r["kind"]) for r in rows})
        assert ("AuthenticationConfig", "allOf") in names
        assert ("ServerConfig", "allOf") in names

    def test_allOf_flattens_into_parent_properties(self, fresh_db):
        """NtpprofileSchema is allOf [Auth, Server, inline{name,vrf}].

        Every leaf property must be reachable via a single HAS_PROPERTY
        hop on the parent so the agent can ask "what fields can I send"
        in one Cypher step."""
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'NtpprofileSchema'})-[:HAS_PROPERTY]->(p:Property) "
            "RETURN p.name AS name ORDER BY p.name"
        ).rows_as_dict())
        names = sorted(r["name"] for r in rows)
        assert names == sorted(["authenticate", "key-value", "server", "name", "vrf"])

    def test_inherited_properties_track_their_branch(self, fresh_db):
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (c:SchemaComponent {name: 'NtpprofileSchema'})-[:HAS_PROPERTY]->(p:Property) "
            "RETURN p.name AS name, p.inheritedFrom AS i"
        ).rows_as_dict())
        by_name = {r["name"]: r["i"] for r in rows}
        # Inline branch:
        assert by_name["name"] == ""
        assert by_name["vrf"] == ""
        # Inherited from allOf $refs:
        assert by_name["authenticate"] == "AuthenticationConfig"
        assert by_name["key-value"] == "AuthenticationConfig"
        assert by_name["server"] == "ServerConfig"

    def test_filter_by_supported_device_type_works_in_cypher(self, fresh_db):
        """The headline use case: 'show me the NTP fields valid for Switch CX'."""
        _, conn = fresh_db
        _seed(conn)
        rows = list(conn.execute(
            "MATCH (e:ApiEndpoint {endpoint_id: 'POST:/v1/ntp'})"
            "-[:HAS_REQUEST_BODY]->(:RequestBody)-[:BODY_REFERENCES]->"
            "(c:SchemaComponent)-[:HAS_PROPERTY]->(p:Property) "
            "WHERE 'Switch CX' IN p.supportedDeviceTypes "
            "RETURN p.name AS name ORDER BY p.name"
        ).rows_as_dict())
        names = sorted(r["name"] for r in rows)
        # Every leaf in our spec was supported on Switch CX.
        assert names == sorted(
            ["authenticate", "key-value", "server", "name", "vrf"]
        )
