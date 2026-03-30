"""Integration tests for seed scripts.

Tests run seeds as real subprocesses against a temp LadybugDB graph,
using live Central API credentials from .env. All tests are skipped
if credentials are not configured.

Run with:  pytest tests/test_seed_integration.py -v
"""

from __future__ import annotations

import json

import pytest

pytestmark = pytest.mark.integration


# ── Helpers ─────────────────────────────────────────────────────────

def _assert_seed_ok(result) -> None:
    """Assert a seed ran successfully, with informative error on failure."""
    assert result.exit_code == 0, (
        f"Seed failed (exit_code={result.exit_code}).\n"
        f"--- stderr ---\n{result.stderr[-2000:]}\n"
        f"--- stdout ---\n{result.stdout[-2000:]}"
    )


def _query_count(infra, cypher: str) -> int:
    """Run a count query and return the integer result."""
    rows = infra.graph_manager.query(cypher)
    return rows[0]["cnt"] if rows else 0


# ── TestSeedStartupSequence ─────────────────────────────────────────

class TestSeedStartupSequence:
    """Verify seed ordering and full startup sequence."""

    def test_seed_order_is_deterministic(self, seed_infra):
        """Topological sort produces consistent, correct ordering."""
        order1 = seed_infra.seed_order()
        order2 = seed_infra.seed_order()
        assert order1 == order2, "Seed order is not deterministic across calls"

        assert "populate_base_graph.py" in order1, "populate_base_graph.py missing from auto_run seeds"
        base_idx = order1.index("populate_base_graph.py")

        for dep in ("enrich_topology.py",):
            if dep in order1:
                assert order1.index(dep) > base_idx, f"{dep} should come after populate_base_graph.py"

    def test_full_startup_sequence(self, seed_infra):
        """Run all auto_run seeds in order and verify cumulative graph state."""
        order = seed_infra.seed_order()
        assert len(order) >= 1, "No auto_run seeds found"

        seed_outputs: dict[str, str] = {}
        for script_name in order:
            result = seed_infra.run_seed(script_name)
            _assert_seed_ok(result)
            seed_outputs[script_name] = result.stdout

        # Verify cumulative graph state
        assert _query_count(seed_infra, "MATCH (o:Org) RETURN count(o) AS cnt") >= 1, \
            "Expected at least 1 Org node"
        assert _query_count(seed_infra, "MATCH (s:Site) RETURN count(s) AS cnt") >= 1, \
            "Expected at least 1 Site node"
        assert _query_count(seed_infra, "MATCH (d:Device) RETURN count(d) AS cnt") >= 1, \
            "Expected at least 1 Device node"


# ── TestIndividualSeeds ─────────────────────────────────────────────

class TestIndividualSeeds:
    """Test each auto_run seed individually, running deps first."""

    def test_populate_base_graph(self, seed_infra):
        """Base graph creates Org, Site, Device, SiteCollection, DeviceGroup nodes."""
        result = seed_infra.run_seed("populate_base_graph.py")
        _assert_seed_ok(result)

        assert _query_count(seed_infra, "MATCH (o:Org) RETURN count(o) AS cnt") >= 1
        assert _query_count(seed_infra, "MATCH (s:Site) RETURN count(s) AS cnt") >= 1
        assert _query_count(seed_infra, "MATCH (d:Device) RETURN count(d) AS cnt") >= 1

        # Relationships
        assert _query_count(seed_infra, "MATCH ()-[r:HAS_SITE]->() RETURN count(r) AS cnt") >= 1
        assert _query_count(seed_infra, "MATCH ()-[r:HAS_DEVICE]->() RETURN count(r) AS cnt") >= 1

    def test_enrich_topology(self, seed_infra):
        """Topology enrichment runs without error (edges depend on live LLDP data)."""
        # Ensure base graph data exists
        base_count = _query_count(seed_infra, "MATCH (o:Org) RETURN count(o) AS cnt")
        if base_count == 0:
            result = seed_infra.run_seed("populate_base_graph.py")
            _assert_seed_ok(result)

        result = seed_infra.run_seed("enrich_topology.py")
        _assert_seed_ok(result)

        # CONNECTED_TO edges may be 0 if devices are offline — just check it ran clean
        data = json.loads(result.stdout)
        assert isinstance(data, dict), "Expected JSON object on stdout"


# ── TestSeedIdempotency ─────────────────────────────────────────────

class TestSeedIdempotency:
    """Verify seeds use MERGE properly — re-running doesn't duplicate data."""

    def test_base_graph_idempotent(self, seed_infra):
        """Running populate_base_graph twice yields the same node counts."""
        result1 = seed_infra.run_seed("populate_base_graph.py")
        _assert_seed_ok(result1)

        count_after_first = (
            _query_count(seed_infra, "MATCH (o:Org) RETURN count(o) AS cnt")
            + _query_count(seed_infra, "MATCH (s:Site) RETURN count(s) AS cnt")
            + _query_count(seed_infra, "MATCH (d:Device) RETURN count(d) AS cnt")
        )
        assert count_after_first > 0, "First run should create nodes"

        result2 = seed_infra.run_seed("populate_base_graph.py")
        _assert_seed_ok(result2)

        count_after_second = (
            _query_count(seed_infra, "MATCH (o:Org) RETURN count(o) AS cnt")
            + _query_count(seed_infra, "MATCH (s:Site) RETURN count(s) AS cnt")
            + _query_count(seed_infra, "MATCH (d:Device) RETURN count(d) AS cnt")
        )
        assert count_after_first == count_after_second, (
            f"MERGE not idempotent: {count_after_first} nodes after 1st run, "
            f"{count_after_second} after 2nd"
        )


# ── TestSeedOutputFormat ────────────────────────────────────────────

class TestSeedOutputFormat:
    """Verify seeds emit valid JSON on stdout."""

    def test_seed_stdout_is_json(self, seed_infra):
        """Every auto_run seed must write valid JSON to stdout."""
        order = seed_infra.seed_order()

        for script_name in order:
            result = seed_infra.run_seed(script_name)
            _assert_seed_ok(result)

            try:
                data = json.loads(result.stdout)
            except json.JSONDecodeError:
                pytest.fail(
                    f"{script_name} did not emit valid JSON on stdout.\n"
                    f"stdout: {result.stdout[:500]}"
                )
            assert isinstance(data, dict), f"{script_name}: expected JSON object, got {type(data).__name__}"
