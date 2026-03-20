#!/usr/bin/env python3
"""TDD tests for dynamic DDL loading in GraphManager.

Tests that GraphManager can:
  1. Load generated DDL from a JSON file in the archive
  2. Apply dynamic DDL alongside bootstrap (knowledge) tables
  3. Detect schema hash changes and trigger rebuild
  4. Fall back to bootstrap-only when no DDL file exists
"""

from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import pytest
import real_ladybug as lb

sys.path.insert(0, str(Path(__file__).parent / "src"))

from hpe_networking_central_mcp.graph.manager import GraphManager
from hpe_networking_central_mcp.graph.schema import KNOWLEDGE_NODE_TABLES, KNOWLEDGE_REL_TABLES


# ── Helpers ───────────────────────────────────────────────────────────


def _make_ddl_file(path: Path, ddl: list[str], schema_hash: str = "abc123") -> None:
    """Write a generated_ddl.json file."""
    path.write_text(
        json.dumps({
            "schema_hash": schema_hash,
            "node_tables": len([d for d in ddl if "NODE TABLE" in d]),
            "rel_tables": len([d for d in ddl if "REL TABLE" in d]),
            "ddl": ddl,
        }, indent=2),
        encoding="utf-8",
    )


SAMPLE_NODE_DDL = [
    """CREATE NODE TABLE IF NOT EXISTS MonDevice (
    serial STRING,
    name STRING,
    status STRING,
    PRIMARY KEY (serial)
)""",
    """CREATE NODE TABLE IF NOT EXISTS MonInterface (
    iface_id STRING,
    name STRING,
    speed DOUBLE,
    PRIMARY KEY (iface_id)
)""",
]

SAMPLE_REL_DDL = [
    "CREATE REL TABLE IF NOT EXISTS HAS_INTERFACE (FROM MonDevice TO MonInterface)",
]


# ══════════════════════════════════════════════════════════════════════
# Test 1: Load DDL from file
# ══════════════════════════════════════════════════════════════════════


class TestDynamicDDLLoading:
    def test_loads_ddl_from_file(self):
        """GraphManager applies DDL from generated_ddl.json at init."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "graph_db"
            ddl_file = tmp_path / "generated_ddl.json"
            _make_ddl_file(ddl_file, SAMPLE_NODE_DDL + SAMPLE_REL_DDL)

            gm = GraphManager(db_path)
            gm.initialize(generated_ddl_path=ddl_file)

            # Verify dynamic tables are created
            rows = gm.query(
                "CALL show_tables() RETURN *",
                read_only=False,
            )
            table_names = {r["name"] for r in rows}
            assert "MonDevice" in table_names
            assert "MonInterface" in table_names

    def test_applies_knowledge_tables_alongside(self):
        """Knowledge tables (ApiEndpoint, etc.) still exist after dynamic DDL."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "graph_db"
            ddl_file = tmp_path / "generated_ddl.json"
            _make_ddl_file(ddl_file, SAMPLE_NODE_DDL)

            gm = GraphManager(db_path)
            gm.initialize(generated_ddl_path=ddl_file)

            rows = gm.query("CALL show_tables() RETURN *", read_only=False)
            table_names = {r["name"] for r in rows}
            assert "ApiEndpoint" in table_names
            assert "ApiCategory" in table_names
            assert "MonDevice" in table_names

    def test_can_insert_into_dynamic_tables(self):
        """Can insert and query data in dynamically-created tables."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "graph_db"
            ddl_file = tmp_path / "generated_ddl.json"
            _make_ddl_file(ddl_file, SAMPLE_NODE_DDL + SAMPLE_REL_DDL)

            gm = GraphManager(db_path)
            gm.initialize(generated_ddl_path=ddl_file)

            gm.execute(
                "CREATE (d:MonDevice {serial: $s, name: $n, status: $st})",
                {"s": "SN001", "n": "test-ap", "st": "ONLINE"},
            )
            gm.execute(
                "CREATE (i:MonInterface {iface_id: $id, name: $n, speed: $sp})",
                {"id": "SN001:eth0", "n": "eth0", "sp": 1.0},
            )
            gm.execute(
                "MATCH (d:MonDevice {serial: $s}), (i:MonInterface {iface_id: $id}) "
                "CREATE (d)-[:HAS_INTERFACE]->(i)",
                {"s": "SN001", "id": "SN001:eth0"},
            )

            rows = gm.query(
                "MATCH (d:MonDevice)-[:HAS_INTERFACE]->(i:MonInterface) "
                "RETURN d.serial AS serial, i.name AS iface"
            )
            assert len(rows) == 1
            assert rows[0]["serial"] == "SN001"
            assert rows[0]["iface"] == "eth0"

    def test_schema_hash_stored(self):
        """The schema hash from the DDL file is accessible."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "graph_db"
            ddl_file = tmp_path / "generated_ddl.json"
            _make_ddl_file(ddl_file, SAMPLE_NODE_DDL, schema_hash="deadbeef12345678")

            gm = GraphManager(db_path)
            gm.initialize(generated_ddl_path=ddl_file)

            assert gm.schema_hash == "deadbeef12345678"


# ══════════════════════════════════════════════════════════════════════
# Test 2: Fallback behavior
# ══════════════════════════════════════════════════════════════════════


class TestFallback:
    def test_no_ddl_file_uses_bootstrap_only(self):
        """Without a DDL file, only knowledge tables are created."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "graph_db"

            gm = GraphManager(db_path)
            gm.initialize()  # no ddl file

            rows = gm.query("CALL show_tables() RETURN *", read_only=False)
            table_names = {r["name"] for r in rows}
            # Knowledge tables should exist
            assert "ApiEndpoint" in table_names
            # Dynamic tables should not
            assert "MonDevice" not in table_names

    def test_nonexistent_ddl_path_ignored(self):
        """Passing a nonexistent path gracefully falls back to bootstrap."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "graph_db"

            gm = GraphManager(db_path)
            gm.initialize(generated_ddl_path=tmp_path / "nonexistent.json")

            assert gm.is_available
            assert gm.schema_hash is None


# ══════════════════════════════════════════════════════════════════════
# Test 3: Schema description includes dynamic tables
# ══════════════════════════════════════════════════════════════════════


class TestSchemaDescription:
    def test_dynamic_tables_in_description(self):
        """get_schema_description() includes dynamically-created tables."""
        with tempfile.TemporaryDirectory() as tmp:
            tmp_path = Path(tmp)
            db_path = tmp_path / "graph_db"
            ddl_file = tmp_path / "generated_ddl.json"
            _make_ddl_file(ddl_file, SAMPLE_NODE_DDL + SAMPLE_REL_DDL)

            gm = GraphManager(db_path)
            gm.initialize(generated_ddl_path=ddl_file)

            desc = gm.get_schema_description()
            assert "MonDevice" in desc
            assert "MonInterface" in desc
            assert "HAS_INTERFACE" in desc


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
