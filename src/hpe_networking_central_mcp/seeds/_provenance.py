"""Provenance helpers for seed scripts.

Provides functions for recording data provenance:
- set_source_fields: Update fetched_at and source_api on domain nodes
- record_provenance: Create POPULATED_BY edges from domain nodes to ApiEndpoint
"""

from __future__ import annotations

from datetime import datetime, timezone


def _make_endpoint_id(method: str, api_path: str) -> str:
    """Build the endpoint_id matching ApiEndpoint nodes in the graph."""
    return f"{method}:{api_path}"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def set_source_fields(
    node_label: str,
    pk_field: str,
    pk_value: str,
    method: str,
    api_path: str,
) -> tuple[str, dict]:
    """Return a (cypher, params) tuple that sets fetched_at + source_api on a node.

    The caller should execute this against the graph connection.
    """
    eid = _make_endpoint_id(method, api_path)
    cypher = (
        f"MATCH (n:{node_label} {{{pk_field}: $_pk}}) "
        f"SET n.fetched_at = $_fetched_at, n.source_api = $_source_api"
    )
    params = {
        "_pk": pk_value,
        "_fetched_at": _now_iso(),
        "_source_api": eid,
    }
    return cypher, params


def record_provenance(
    *,
    node_label: str,
    pk_field: str,
    pk_value: str,
    method: str,
    api_path: str,
    seed_name: str,
    run_id: str,
) -> list[tuple[str, dict]]:
    """Return Cypher statements for latest-only POPULATED_BY provenance.

    Returns a list of (cypher, params) tuples:
      1. Delete any existing POPULATED_BY edge from this node
      2. Create a fresh POPULATED_BY edge to the ApiEndpoint

    The caller should execute both statements in order.
    """
    eid = _make_endpoint_id(method, api_path)
    now = _now_iso()

    # 1. Remove old provenance edge (latest-only semantics)
    delete_cypher = (
        f"MATCH (n:{node_label} {{{pk_field}: $_pk}})-[r:POPULATED_BY]->() DELETE r"
    )
    delete_params = {"_pk": pk_value}

    # 2. Create new POPULATED_BY edge
    create_cypher = (
        f"MATCH (n:{node_label} {{{pk_field}: $_pk}}), (api:ApiEndpoint {{endpoint_id: $_eid}}) "
        f"CREATE (n)-[:POPULATED_BY {{fetched_at: $_fetched_at, seed: $_seed, run_id: $_run_id}}]->(api)"
    )
    create_params = {
        "_pk": pk_value,
        "_eid": eid,
        "_fetched_at": now,
        "_seed": seed_name,
        "_run_id": run_id,
    }

    return [(delete_cypher, delete_params), (create_cypher, create_params)]
