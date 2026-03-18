"""GraphManager — owns the Kùzu in-memory database and exposes query/populate/refresh."""

from __future__ import annotations

import re
import threading
from typing import Any

import kuzu
import structlog

from ..central_client import CentralClient
from .population import populate_graph
from .schema import NODE_TABLES, REL_TABLES, SCHEMA_DESCRIPTION

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
        self._population_summary: dict[str, Any] = {}

    @property
    def ready(self) -> bool:
        return self._ready

    @property
    def population_summary(self) -> dict[str, Any]:
        return dict(self._population_summary)

    # ── Lifecycle ─────────────────────────────────────────────────

    def initialize(self) -> None:
        """Create the in-memory database and apply schema DDL."""
        logger.info("graph_init_start")
        self._db = kuzu.Database()
        conn = self._get_conn()
        for ddl in NODE_TABLES + REL_TABLES:
            conn.execute(ddl.strip())
        logger.info("graph_schema_created", node_tables=len(NODE_TABLES), rel_tables=len(REL_TABLES))

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
        conn = self._get_conn()

        # Clear all relationship data first, then node data
        for ddl in REL_TABLES:
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
