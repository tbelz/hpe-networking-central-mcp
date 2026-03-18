"""GraphManager — owns the Kùzu in-memory database and exposes query/populate/refresh."""

from __future__ import annotations

import re
import threading
from typing import Any

import kuzu
import structlog

from ..central_client import CentralClient
from .population import populate_graph, populate_topology
from .schema import NODE_TABLES, REL_TABLES, TOPOLOGY_REL_TABLES, SCHEMA_DESCRIPTION

logger = structlog.get_logger("graph.manager")

# Cypher keywords that mutate the graph — blocked in read-only query tool
_WRITE_KEYWORDS = re.compile(
    r"\b(CREATE|DELETE|DETACH|SET|REMOVE|MERGE|DROP|ALTER|COPY|INSERT)\b",
    re.IGNORECASE,
)


class GraphManager:
    """Manages the Kùzu in-memory graph database lifecycle.

    Thread safety: the Database object is thread-safe; Connection is not.
    We use a threading.Lock to serialise connection access.
    """

    def __init__(self) -> None:
        self._db: kuzu.Database | None = None
        self._lock = threading.Lock()
        self._ready = False
        self._topology_ready = False
        self._population_summary: dict[str, Any] = {}
        self._topology_summary: dict[str, Any] = {}

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def topology_ready(self) -> bool:
        return self._topology_ready

    @property
    def population_summary(self) -> dict[str, Any]:
        return dict(self._population_summary)

    # ── Lifecycle ─────────────────────────────────────────────────

    def initialize(self) -> None:
        """Create the in-memory database and apply schema DDL."""
        logger.info("graph_init_start")
        self._db = kuzu.Database()
        conn = self._get_conn()
        all_ddl = NODE_TABLES + REL_TABLES + TOPOLOGY_REL_TABLES
        for ddl in all_ddl:
            conn.execute(ddl.strip())
        logger.info(
            "graph_schema_created",
            node_tables=len(NODE_TABLES),
            rel_tables=len(REL_TABLES) + len(TOPOLOGY_REL_TABLES),
        )

    def populate(self, client: CentralClient) -> dict[str, Any]:
        """Populate the graph from live Central APIs.

        Args:
            client: Authenticated CentralClient.

        Returns:
            Summary dict with entity counts.
        """
        if self._db is None:
            raise RuntimeError("Graph not initialized — call initialize() first")

        logger.info("graph_populate_start")
        conn = self._get_conn()
        summary = populate_graph(client, conn)
        self._population_summary = summary
        self._ready = True
        logger.info("graph_populate_done", **{k: v for k, v in summary.items() if k != "errors"})
        return summary

    def refresh(self, client: CentralClient) -> dict[str, Any]:
        """Drop all data and re-populate from APIs.

        Args:
            client: Authenticated CentralClient.

        Returns:
            Summary dict with entity counts.
        """
        if self._db is None:
            raise RuntimeError("Graph not initialized — call initialize() first")

        logger.info("graph_refresh_start")
        self._ready = False
        self._topology_ready = False
        conn = self._get_conn()

        # Clear all relationship data first (including topology), then node data
        all_rels = REL_TABLES + TOPOLOGY_REL_TABLES
        for ddl in all_rels:
            table_name = _extract_table_name(ddl)
            if table_name:
                try:
                    conn.execute(f"MATCH ()-[r:{table_name}]->() DELETE r")
                except Exception:
                    pass  # Table may be empty

        for ddl in NODE_TABLES:
            table_name = _extract_table_name(ddl)
            if table_name:
                try:
                    conn.execute(f"MATCH (n:{table_name}) DELETE n")
                except Exception:
                    pass

        return self.populate(client)

    def load_topology(self, client: CentralClient) -> dict[str, Any]:
        """Lazily populate L2 topology data from per-site topology APIs.

        Fetches all Site scopeIds from the graph, then calls the topology
        API for each site to create CONNECTED_TO / LINKED_TO edges and
        UnmanagedDevice nodes.

        If topology is already loaded, this is a no-op (returns cached summary).
        Use ``refresh_topology()`` to force a re-load.

        Args:
            client: Authenticated CentralClient.

        Returns:
            Summary dict with topology counts.
        """
        if self._topology_ready:
            return dict(self._topology_summary)
        return self._do_load_topology(client)

    def refresh_topology(self, client: CentralClient) -> dict[str, Any]:
        """Clear and re-populate topology data.

        Args:
            client: Authenticated CentralClient.

        Returns:
            Summary dict with topology counts.
        """
        if self._db is None:
            raise RuntimeError("Graph not initialized — call initialize() first")

        logger.info("topology_refresh_start")
        self._topology_ready = False
        conn = self._get_conn()

        # Clear topology relationships and unmanaged device nodes
        for ddl in TOPOLOGY_REL_TABLES:
            table_name = _extract_table_name(ddl)
            if table_name:
                try:
                    conn.execute(f"MATCH ()-[r:{table_name}]->() DELETE r")
                except Exception:
                    pass
        # Clear HAS_UNMANAGED edges, then UnmanagedDevice nodes
        try:
            conn.execute("MATCH ()-[r:HAS_UNMANAGED]->() DELETE r")
        except Exception:
            pass
        try:
            conn.execute("MATCH (u:UnmanagedDevice) DELETE u")
        except Exception:
            pass

        return self._do_load_topology(client)

    def _do_load_topology(self, client: CentralClient) -> dict[str, Any]:
        """Internal: fetch site IDs from graph and populate topology."""
        if self._db is None:
            raise RuntimeError("Graph not initialized")
        if not self._ready:
            raise RuntimeError("Graph must be populated before loading topology")

        conn = self._get_conn()
        # Get all site IDs from the graph
        result = conn.execute("MATCH (s:Site) RETURN s.scopeId AS sid")
        site_ids: list[str] = []
        while result.has_next():
            row = result.get_next()
            if row[0]:
                site_ids.append(str(row[0]))

        logger.info("topology_load_start", sites=len(site_ids))
        summary = populate_topology(client, conn, site_ids)
        self._topology_summary = summary
        self._topology_ready = True
        logger.info("topology_load_done", **{k: v for k, v in summary.items() if k != "errors"})
        return summary

    # ── Query ─────────────────────────────────────────────────────

    def query(self, cypher: str, *, read_only: bool = True) -> list[dict[str, Any]]:
        """Execute a Cypher query and return results as a list of dicts.

        Args:
            cypher: Cypher query string.
            read_only: If True, reject queries containing write keywords.

        Returns:
            List of result rows as dicts.

        Raises:
            ValueError: If read_only=True and query contains write keywords.
            RuntimeError: If graph is not ready.
        """
        if self._db is None:
            raise RuntimeError("Graph not initialized")
        if not self._ready:
            raise RuntimeError(
                "Graph is still loading. Population runs in the background on startup — "
                "please retry in a few seconds."
            )

        if read_only and _WRITE_KEYWORDS.search(cypher):
            raise ValueError(
                "Write operations are not allowed via query_graph. "
                "Only read queries (MATCH, RETURN, WITH, WHERE, ORDER BY, LIMIT, UNION, UNWIND, CALL) are permitted."
            )

        conn = self._get_conn()
        result = conn.execute(cypher)
        rows: list[dict[str, Any]] = []
        while result.has_next():
            row = result.get_next()
            columns = result.get_column_names()
            rows.append(dict(zip(columns, row)))
        return rows

    def get_schema_description(self) -> str:
        """Return the human-readable schema description."""
        status = "populated" if self._ready else "loading..."
        summary_lines = ""
        if self._population_summary:
            s = self._population_summary
            summary_lines = (
                f"\n## Current Population\n"
                f"- Sites: {s.get('sites', 0)}\n"
                f"- Site Collections: {s.get('site_collections', 0)}\n"
                f"- Device Groups: {s.get('device_groups', 0)}\n"
                f"- Devices: {s.get('devices', 0)}\n"
                f"- Config Profiles: {s.get('config_profiles', 0)}\n"
                f"- Status: {status}\n"
            )
            if s.get("errors"):
                summary_lines += f"- Population errors: {'; '.join(s['errors'])}\n"

        # Topology status
        if self._topology_ready and self._topology_summary:
            t = self._topology_summary
            summary_lines += (
                f"\n## Topology Population\n"
                f"- Sites processed: {t.get('sites_processed', 0)}\n"
                f"- CONNECTED_TO edges (Device→Device): {t.get('connected_to', 0)}\n"
                f"- LINKED_TO edges (Device→UnmanagedDevice): {t.get('linked_to', 0)}\n"
                f"- Unmanaged devices discovered: {t.get('unmanaged_devices', 0)}\n"
                f"- Status: populated\n"
            )
        else:
            summary_lines += (
                "\n## Topology Population\n"
                "- Status: not loaded (call load_topology or refresh_graph to populate)\n"
            )

        return SCHEMA_DESCRIPTION + summary_lines

    # ── Internal ──────────────────────────────────────────────────

    def _get_conn(self) -> kuzu.Connection:
        """Get a connection, serialised via lock."""
        with self._lock:
            return kuzu.Connection(self._db)


def _extract_table_name(ddl: str) -> str | None:
    """Extract table name from a CREATE ... TABLE IF NOT EXISTS <name> DDL."""
    m = re.search(r"TABLE\s+IF\s+NOT\s+EXISTS\s+(\w+)", ddl, re.IGNORECASE)
    if m:
        return m.group(1)
    m = re.search(r"TABLE\s+(\w+)", ddl, re.IGNORECASE)
    return m.group(1) if m else None
