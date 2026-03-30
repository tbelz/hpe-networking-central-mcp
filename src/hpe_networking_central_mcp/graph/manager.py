"""GraphManager — owns the LadybugDB file-backed database and exposes query/populate/refresh."""

from __future__ import annotations

import re
import shutil
import threading
from pathlib import Path
from typing import Any

import real_ladybug as lb
import structlog

from .schema import KNOWLEDGE_NODE_TABLES, KNOWLEDGE_REL_TABLES, NODE_TABLES, POLICY_REL_TABLES, REL_TABLES, TOPOLOGY_REL_TABLES

logger = structlog.get_logger("graph.manager")

# Cypher keywords that mutate the graph — blocked in read-only query tool.
# LOAD FROM is included because LadybugDB can read arbitrary filesystem paths.
_WRITE_KEYWORDS = re.compile(
    r"\b(CREATE|DELETE|DETACH|SET|REMOVE|MERGE|DROP|ALTER|COPY|INSERT|LOAD|INSTALL)\b",
    re.IGNORECASE,
)


class GraphManager:
    """Manages the LadybugDB file-backed graph database lifecycle.

    The database is stored on disk so that script subprocesses can open it
    for direct reads and writes via ``central_helpers.graph``.

    Thread safety: the Database object is thread-safe; Connection is not.
    We use a threading.Lock to serialise connection access.
    """

    def __init__(self, db_path: Path) -> None:
        self._db_path = db_path
        self._db: lb.Database | None = None
        self._lock = threading.Lock()
        self._fts_available: bool = False

    @property
    def db_path(self) -> Path:
        return self._db_path

    @property
    def fts_available(self) -> bool:
        """Return True if the FTS extension was successfully loaded."""
        return self._fts_available

    # ── Lifecycle ─────────────────────────────────────────────────

    def initialize(self) -> None:
        """Create (or open) the file-backed database and apply schema DDL."""
        logger.info("graph_init_start", db_path=str(self._db_path))
        self._db_path.parent.mkdir(parents=True, exist_ok=True)
        self._db = lb.Database(str(self._db_path))
        conn = self._get_conn()

        # Bootstrap: knowledge layer + legacy domain tables
        bootstrap_ddl = (
            NODE_TABLES + KNOWLEDGE_NODE_TABLES
            + REL_TABLES + KNOWLEDGE_REL_TABLES
            + TOPOLOGY_REL_TABLES + POLICY_REL_TABLES
        )
        for ddl in bootstrap_ddl:
            conn.execute(ddl.strip())

        # Load the algo extension
        try:
            conn.execute("INSTALL algo")
            conn.execute("LOAD EXTENSION algo")
        except Exception as exc:
            msg = str(exc)
            lower_msg = msg.lower()
            if "already installed" in lower_msg or "already loaded" in lower_msg:
                logger.debug("algo_extension_already_loaded", error=msg)
            else:
                logger.warning(
                    "algo_extension_load_failed",
                    reason="failed to install or load algo extension",
                    error=msg,
                    exc_info=True,
                )

        # Load the FTS extension (graceful degradation if unavailable)
        try:
            conn.execute("INSTALL fts")
            conn.execute("LOAD EXTENSION fts")
            self._fts_available = True
            logger.info("fts_extension_loaded")
        except Exception as exc:
            msg = str(exc)
            lower_msg = msg.lower()
            if "already installed" in lower_msg or "already loaded" in lower_msg:
                self._fts_available = True
                logger.debug("fts_extension_already_loaded", error=msg)
            else:
                self._fts_available = False
                logger.warning(
                    "fts_extension_load_failed",
                    reason="FTS unavailable — search will fall back to CONTAINS",
                    error=msg,
                )
        logger.info(
            "graph_schema_created",
            node_tables=len(NODE_TABLES),
            rel_tables=len(REL_TABLES) + len(TOPOLOGY_REL_TABLES) + len(POLICY_REL_TABLES),
        )

        self._check_ladybug_compat(conn)

    # ── Compatibility diagnostics ─────────────────────────────────

    @staticmethod
    def _check_ladybug_compat(conn: lb.Connection) -> None:
        """Log diagnostic warnings for known LadybugDB/Kuzu bugs.

        This runs once at startup and reports which workarounds are still
        needed so they can be removed when upstream fixes land.
        """
        issues: list[str] = []

        # 1. STRING parameter binding with JSON-like values (segfault risk)
        try:
            conn.execute(
                "RETURN $v AS v",
                parameters={"v": '[{"name":"test"}]'},
            )
        except Exception:
            issues.append("json_string_param_binding")

        # 2. STRING[] list parameter binding
        try:
            conn.execute(
                "RETURN $v AS v",
                parameters={"v": ["a", "b"]},
            )
        except Exception:
            issues.append("list_param_binding")

        # 3. MERGE on Script nodes (planner crash)
        try:
            conn.execute("MERGE (n:Script {filename: '__compat_probe__'}) SET n.description = 'probe'")
            conn.execute("MATCH (n:Script {filename: '__compat_probe__'}) DELETE n")
        except Exception:
            issues.append("merge_script_node")
            # Clean up probe node if MERGE partially succeeded
            try:
                conn.execute("MATCH (n:Script {filename: '__compat_probe__'}) DELETE n")
            except Exception:
                pass

        if issues:
            logger.warning(
                "ladybug_compat_workarounds_still_needed",
                issues=issues,
                hint="These workarounds in tools/scripts.py can be removed when the upstream bugs are fixed.",
            )
        else:
            logger.info(
                "ladybug_compat_all_clear",
                hint="All known LadybugDB workarounds may be removable — re-test and simplify.",
            )

    @property
    def is_available(self) -> bool:
        """Return True if the database is open and ready."""
        return self._db is not None

    def reset(self) -> None:
        """Delete the database and re-initialize with empty schema."""
        logger.info("graph_reset_start")
        if self._db is not None:
            self._db.close()
            self._db = None
        if self._db_path.exists():
            if self._db_path.is_dir():
                shutil.rmtree(self._db_path)
            else:
                self._db_path.unlink()
        self.initialize()

    def replace_db(self, new_db_path: Path) -> None:
        """Replace the database directory with a pre-built one.

        Closes the current DB, replaces the directory, and reopens.
        Used to swap in a knowledge DB downloaded from a GitHub release.
        """
        logger.info("graph_replace_start", new_path=str(new_db_path))
        with self._lock:
            if self._db is not None:
                self._db.close()
                self._db = None
            if self._db_path.exists():
                if self._db_path.is_dir():
                    shutil.rmtree(self._db_path)
                else:
                    self._db_path.unlink()
            if new_db_path.is_dir():
                shutil.copytree(new_db_path, self._db_path)
            else:
                shutil.copy2(new_db_path, self._db_path)
            self._db = lb.Database(str(self._db_path))
        logger.info("graph_replace_done")

    def create_fts_indexes(self) -> int:
        """Create FTS indexes on searchable node tables.

        Indexes are created for:
          - ApiEndpoint: summary, description, path, operationId
          - DocSection: title, content
          - Device: name, serial, model, deviceType
          - Site: name, address, city, country
          - Script: filename, description

        Returns the number of indexes created, or 0 if FTS is unavailable.
        """
        if not self._fts_available:
            logger.info("fts_indexes_skipped", reason="FTS extension not available")
            return 0

        conn = self._get_conn()
        fts_defs: list[tuple[str, str, list[str]]] = [
            ("api_fts", "ApiEndpoint", ["summary", "description", "path", "operationId"]),
            ("doc_fts", "DocSection", ["title", "content"]),
            ("device_fts", "Device", ["name", "serial", "model", "deviceType"]),
            ("site_fts", "Site", ["name", "address", "city", "country"]),
            ("script_fts", "Script", ["filename", "description"]),
        ]

        created = 0
        for idx_name, table, fields in fts_defs:
            cypher_field_list = ", ".join(f"'{f}'" for f in fields)
            try:
                conn.execute(f"CALL DROP_FTS_INDEX('{table}', '{idx_name}')")
            except Exception:
                pass  # Index may not exist yet
            try:
                conn.execute(
                    f"CALL CREATE_FTS_INDEX('{table}', '{idx_name}', [{cypher_field_list}])"
                )
                created += 1
                logger.debug("fts_index_created", index=idx_name, table=table)
            except Exception as exc:
                logger.warning("fts_index_create_failed", index=idx_name, error=str(exc))

        logger.info("fts_indexes_created", count=created)
        return created

    # ── Query ─────────────────────────────────────────────────────

    def query(self, cypher: str, params: dict | None = None, *, read_only: bool = True) -> list[dict[str, Any]]:
        """Execute a Cypher query and return results as a list of dicts.

        Args:
            cypher: Cypher query string.
            params: Optional parameter dict for parameterized queries.
            read_only: If True, reject queries containing write keywords.

        Returns:
            List of result rows as dicts.

        Raises:
            ValueError: If read_only=True and query contains write keywords.
            RuntimeError: If graph is not initialized.
        """
        if read_only and _WRITE_KEYWORDS.search(cypher):
            raise ValueError(
                "Write operations are not allowed via query_graph. "
                "Only read queries (MATCH, RETURN, WITH, WHERE, ORDER BY, LIMIT, UNION, UNWIND, CALL) are permitted."
            )

        conn = self._get_conn()
        result = conn.execute(cypher, parameters=params or {})
        return list(result.rows_as_dict())

    def execute(self, cypher: str, params: dict | None = None) -> list[dict[str, Any]]:
        """Execute a Cypher statement (including writes) and return results.

        Used by seed scripts via central_helpers.graph and by internal refresh.

        Args:
            cypher: Cypher statement.
            params: Optional parameter dict.

        Returns:
            List of result rows as dicts (empty for write statements).
        """
        conn = self._get_conn()
        result = conn.execute(cypher, parameters=params or {})
        return list(result.rows_as_dict())

    def get_schema_description(self) -> str:
        """Return a dynamic schema description by introspecting the LadybugDB catalog."""
        if self._db is None:
            return "Graph not initialized."

        conn = self._get_conn()
        lines = ["# Graph Schema — Aruba Central Configuration & Topology\n"]

        # Node tables
        lines.append("## Node Tables\n")
        lines.append("| Table | Primary Key | Properties | Row Count |")
        lines.append("|-------|-------------|------------|-----------|")
        try:
            all_tables = list(conn.execute("CALL show_tables() RETURN *").rows_as_dict())
            node_tables = [row["name"] for row in all_tables if row.get("type") == "NODE"]
        except Exception:
            node_tables = []

        for table in sorted(node_tables):
            try:
                prop_rows = list(conn.execute(f"CALL table_info('{table}') RETURN *"))
                props = []
                pk = ""
                for prow in prop_rows:
                    prop_name = prow[1]  # name
                    if prow[3]:  # isPrimaryKey
                        pk = prop_name
                    else:
                        props.append(prop_name)
                # Count rows
                count_rows = list(conn.execute(f"MATCH (n:{table}) RETURN count(n) AS cnt"))
                cnt = count_rows[0][0] if count_rows else 0
                lines.append(f"| {table} | {pk} | {', '.join(props)} | {cnt} |")
            except Exception:
                lines.append(f"| {table} | — | — | — |")

        # Relationship tables
        lines.append("\n## Relationship Tables\n")
        lines.append("| Relationship | From → To | Properties | Edge Count |")
        lines.append("|-------------|-----------|------------|------------|")
        try:
            all_tables = list(conn.execute("CALL show_tables() RETURN *").rows_as_dict())
            rel_tables = [row["name"] for row in all_tables if row.get("type") == "REL"]
        except Exception:
            rel_tables = []

        for rel in sorted(rel_tables):
            try:
                # Get connection info
                conn_rows = list(conn.execute(f"CALL show_connection('{rel}') RETURN *"))
                from_to = f"{conn_rows[0][0]} → {conn_rows[0][1]}" if conn_rows else ""
                # Get properties
                prop_rows = list(conn.execute(f"CALL table_info('{rel}') RETURN *"))
                props = [prow[1] for prow in prop_rows]
                # Count edges
                count_rows = list(conn.execute(
                    f"MATCH ()-[r:{rel}]->() RETURN count(r) AS cnt"
                ))
                cnt = count_rows[0][0] if count_rows else 0
                prop_str = ", ".join(props) if props else "—"
                lines.append(f"| {rel} | {from_to} | {prop_str} | {cnt} |")
            except Exception:
                lines.append(f"| {rel} | — | — | — |")

        # Hierarchy diagram
        lines.append("""
## Hierarchy & Configuration Model

Central uses five **configuration scopes** in a directory-style structure.
Four are hierarchy-based; one (DeviceGroup) is cross-cutting.

```
Org  (= Global scope — config here applies to ALL devices)
├── SiteCollection  (optional — regions, business units)
│   └── Site  (physical location)
│       ├── Device ──CONNECTED_TO──► Device (LLDP topology)
│       │           └──LINKED_TO──► UnmanagedDevice
│       └── UnmanagedDevice
├── Site  (standalone, not in a collection)
│   └── Device / UnmanagedDevice
├── DeviceGroup  (cross-cutting — devices from ANY site)
│   └── Device
```

### Configuration Propagation Paths
1. `Global (Org) → SiteCollection → Site → Device`
2. `DeviceGroup → Device`

### Precedence (highest wins on conflict)
`Device > DeviceGroup > Site > SiteCollection > Global`

### Effective Config (API-only)
There are no config profile nodes in the graph.  Use the Central API directly:
- Discover categories via `unified_search("config", scope="api")`
- Read effective config: `GET network-config/v1alpha1/{category}?effective=true&detailed=true`
- The `detailed=true` response includes provenance annotations showing which scope each setting comes from.

### Useful Queries
- Devices at a site: `MATCH (s:Site {name: $n})-[:HAS_DEVICE]->(d) RETURN d.serial, d.name`
- Blast radius: `MATCH (sc:SiteCollection)-[:CONTAINS_SITE]->(s:Site)-[:HAS_DEVICE]->(d) RETURN s.name, d.serial`

## Tips
- Use `list_scripts()` to find enrichment scripts (e.g., populate_base_graph, enrich_topology).
- Execute enrichment scripts to populate/enrich graph data on demand.
- Read `graph://schema` for up-to-date schema introspection after enrichment.
- Write operations are blocked in `query_graph()` — enrichment happens via scripts only.
""")

        return "\n".join(lines)

    # ── Internal ──────────────────────────────────────────────────

    def _get_conn(self) -> lb.Connection:
        """Get a connection, serialised via lock.

        Raises:
            RuntimeError: If the database is not initialized.
        """
        with self._lock:
            if self._db is None:
                raise RuntimeError("Graph database is not initialized.")
            return lb.Connection(self._db)
