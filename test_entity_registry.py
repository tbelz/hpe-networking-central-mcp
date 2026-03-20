"""TDD tests for DDL-derived entity registry.

Tests that entity definitions can be derived from the generated DDL
(schema_generator output), replacing the static build_aruba_central_registry().
"""

from __future__ import annotations

import pytest

from hpe_networking_central_mcp.entity_mapping.entities import (
    Entity,
    EntityField,
    EntityRegistry,
)
from hpe_networking_central_mcp.schema_generator import (
    NodeTableDef,
    PropertyDef,
)


# ── Fixtures ────────────────────────────────────────────────────────

SAMPLE_NODE_TABLES = [
    NodeTableDef(
        name="Device",
        primary_key="serial",
        properties=[
            PropertyDef(name="serial", db_type="STRING"),
            PropertyDef(name="name", db_type="STRING"),
            PropertyDef(name="model", db_type="STRING"),
            PropertyDef(name="deviceType", db_type="STRING"),
            PropertyDef(name="status", db_type="STRING"),
            PropertyDef(name="firmware", db_type="STRING"),
        ],
        source_endpoints=["/monitoring/v1/devices"],
    ),
    NodeTableDef(
        name="Site",
        primary_key="scopeId",
        properties=[
            PropertyDef(name="scopeId", db_type="STRING"),
            PropertyDef(name="name", db_type="STRING"),
            PropertyDef(name="city", db_type="STRING"),
            PropertyDef(name="country", db_type="STRING"),
        ],
        source_endpoints=["/config/v1/sites"],
    ),
    NodeTableDef(
        name="Interface",
        primary_key="interface_id",
        properties=[
            PropertyDef(name="interface_id", db_type="STRING"),
            PropertyDef(name="name", db_type="STRING"),
            PropertyDef(name="speed", db_type="INT64"),
            PropertyDef(name="status", db_type="STRING"),
        ],
        source_endpoints=["/monitoring/v1/devices/{serial}/interfaces"],
    ),
]


# =====================================================================
# Test: entities_from_node_tables
# =====================================================================


class TestEntitiesFromNodeTables:
    """Test converting NodeTableDef objects to Entity objects."""

    def test_returns_list_of_entities(self):
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        assert isinstance(entities, list)
        assert all(isinstance(e, Entity) for e in entities)

    def test_entity_count_matches(self):
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        assert len(entities) == 3

    def test_entity_name_from_table(self):
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        names = {e.name for e in entities}
        assert "Device" in names
        assert "Site" in names
        assert "Interface" in names

    def test_graph_node_matches_table_name(self):
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        device = next(e for e in entities if e.name == "Device")
        assert device.graph_node == "Device"

    def test_fields_from_properties(self):
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        device = next(e for e in entities if e.name == "Device")
        assert "serial" in device.fields
        assert "name" in device.fields
        assert "model" in device.fields

    def test_field_graph_property_matches(self):
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        device = next(e for e in entities if e.name == "Device")
        assert device.fields["serial"].graph_property == "serial"
        assert device.fields["deviceType"].graph_property == "deviceType"

    def test_primary_key_field_marked(self):
        """The primary key field should have a description indicating it."""
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        device = next(e for e in entities if e.name == "Device")
        assert "primary key" in device.fields["serial"].description.lower()

    def test_description_includes_source_endpoint(self):
        from hpe_networking_central_mcp.entity_mapping.entities import entities_from_node_tables
        entities = entities_from_node_tables(SAMPLE_NODE_TABLES)
        device = next(e for e in entities if e.name == "Device")
        assert "/monitoring/v1/devices" in device.description


# =====================================================================
# Test: build_registry_from_node_tables
# =====================================================================


class TestBuildRegistryFromNodeTables:
    """Test building a complete registry from DDL node tables."""

    def test_returns_registry(self):
        from hpe_networking_central_mcp.entity_mapping.entities import build_registry_from_node_tables
        registry = build_registry_from_node_tables(SAMPLE_NODE_TABLES)
        assert isinstance(registry, EntityRegistry)

    def test_registry_has_all_entities(self):
        from hpe_networking_central_mcp.entity_mapping.entities import build_registry_from_node_tables
        registry = build_registry_from_node_tables(SAMPLE_NODE_TABLES)
        assert len(registry) == 3

    def test_registry_get_by_name(self):
        from hpe_networking_central_mcp.entity_mapping.entities import build_registry_from_node_tables
        registry = build_registry_from_node_tables(SAMPLE_NODE_TABLES)
        device = registry.get("Device")
        assert device is not None
        assert device.graph_node == "Device"

    def test_registry_all_fields(self):
        from hpe_networking_central_mcp.entity_mapping.entities import build_registry_from_node_tables
        registry = build_registry_from_node_tables(SAMPLE_NODE_TABLES)
        all_fields = registry.all_fields()
        # Device(6) + Site(4) + Interface(4) = 14 fields
        assert len(all_fields) == 14

    def test_empty_node_tables_returns_empty_registry(self):
        from hpe_networking_central_mcp.entity_mapping.entities import build_registry_from_node_tables
        registry = build_registry_from_node_tables([])
        assert len(registry) == 0


# =====================================================================
# Test: Static registry still works (backward compat)
# =====================================================================


class TestStaticRegistryCompat:
    """Ensure the static build_aruba_central_registry still works."""

    def test_static_registry_loads(self):
        from hpe_networking_central_mcp.entity_mapping.entities import build_aruba_central_registry
        registry = build_aruba_central_registry()
        assert len(registry) > 0

    def test_static_registry_has_device(self):
        from hpe_networking_central_mcp.entity_mapping.entities import build_aruba_central_registry
        registry = build_aruba_central_registry()
        assert registry.get("Device") is not None
