"""TDD tests for data provenance — OPERATES_ON.operation + POPULATED_BY.

Phase A: Tests for CRUD operation derivation and instance-level provenance.
"""

from __future__ import annotations

import pytest

from hpe_networking_central_mcp.entity_mapping.runner import derive_operation
from hpe_networking_central_mcp.entity_mapping.mapper import MappingResult, Confidence
from hpe_networking_central_mcp.seeds._provenance import (
    record_provenance,
    set_source_fields,
    _make_endpoint_id,
)


# ── A2: CRUD operation derivation ─────────────────────────────────

class TestDeriveOperation:
    """Test derive_operation(method, path) → operation string."""

    def test_get_collection_is_list(self):
        assert derive_operation("GET", "/monitoring/v1/devices") == "list"

    def test_get_single_resource_is_read(self):
        assert derive_operation("GET", "/monitoring/v1/devices/{serial}") == "read"

    def test_post_is_create(self):
        assert derive_operation("POST", "/config/v1/sites") == "create"

    def test_put_is_update(self):
        assert derive_operation("PUT", "/config/v1/sites/{id}") == "update"

    def test_patch_is_update(self):
        assert derive_operation("PATCH", "/config/v1/devices/{serial}") == "update"

    def test_delete_is_delete(self):
        assert derive_operation("DELETE", "/config/v1/sites/{id}") == "delete"

    def test_get_nested_collection_is_list(self):
        assert derive_operation("GET", "/monitoring/v1/switches/{serial}/ports") == "list"

    def test_get_nested_single_is_read(self):
        assert derive_operation("GET", "/monitoring/v1/switches/{serial}/ports/{port_id}") == "read"

    def test_method_case_insensitive(self):
        assert derive_operation("get", "/monitoring/v1/devices") == "list"
        assert derive_operation("post", "/config/v1/sites") == "create"

    def test_unknown_method_returns_empty(self):
        assert derive_operation("OPTIONS", "/foo") == ""


class TestMappingResultOperation:
    """Test that MappingResult carries the operation field."""

    def test_operation_default_empty(self):
        result = MappingResult(param_name="id", param_location="path")
        assert result.operation == ""

    def test_operation_set_explicitly(self):
        result = MappingResult(
            param_name="serial",
            param_location="path",
            entity_name="Device",
            field_name="serial",
            confidence=Confidence.EXACT,
            mapper_name="static",
            reason="test",
            endpoint_id="GET:/devices/{serial}",
            operation="read",
        )
        assert result.operation == "read"


# ── A3: Provenance helper ────────────────────────────────────────

class TestMakeEndpointId:
    """Test endpoint_id construction from API path."""

    def test_get_endpoint(self):
        assert _make_endpoint_id("GET", "/monitoring/v1/devices") == "GET:/monitoring/v1/devices"

    def test_preserves_method_case(self):
        assert _make_endpoint_id("POST", "/config/v1/sites") == "POST:/config/v1/sites"


class TestSetSourceFields:
    """Test set_source_fields() generates correct Cypher fragment."""

    def test_returns_cypher_fragment(self):
        cypher, params = set_source_fields("Device", "serial", "SN123", "GET", "/monitoring/v1/devices")
        assert "fetched_at" in cypher
        assert "source_api" in cypher
        assert params["_pk"] == "SN123"
        assert params["_source_api"] == "GET:/monitoring/v1/devices"
        assert "_fetched_at" in params

    def test_cypher_targets_correct_label(self):
        cypher, _ = set_source_fields("Site", "scopeId", "site-1", "GET", "/config/v1/sites")
        assert "Site" in cypher
        assert "scopeId" in cypher


class TestRecordProvenance:
    """Test record_provenance() generates correct Cypher statements."""

    def test_returns_two_statements(self):
        stmts = record_provenance(
            node_label="Device",
            pk_field="serial",
            pk_value="SN123",
            method="GET",
            api_path="/monitoring/v1/devices",
            seed_name="populate_base_graph",
            run_id="run-001",
        )
        assert len(stmts) == 2
        # First: delete old POPULATED_BY
        assert "DELETE" in stmts[0][0] or "DETACH" in stmts[0][0]
        # Second: create new POPULATED_BY
        assert "POPULATED_BY" in stmts[1][0]
        assert stmts[1][1]["_seed"] == "populate_base_graph"
        assert stmts[1][1]["_run_id"] == "run-001"

    def test_endpoint_id_in_params(self):
        stmts = record_provenance(
            node_label="Site",
            pk_field="scopeId",
            pk_value="site-1",
            method="GET",
            api_path="/config/v1/sites",
            seed_name="populate_base_graph",
            run_id="run-002",
        )
        assert stmts[1][1]["_eid"] == "GET:/config/v1/sites"
