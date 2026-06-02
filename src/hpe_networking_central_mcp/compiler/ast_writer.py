"""Persistence helpers for the Task 2 OpenAPI AST graph."""

from __future__ import annotations

from pathlib import Path

import pyarrow as pa
import real_ladybug as lb

from .ast_builder import AstGraph
from .ast_schema import apply_ast_schema

_SPEC_SCHEMA = pa.schema([
    ("spec_id", pa.string()),
    ("source", pa.string()),
    ("title", pa.string()),
    ("openapi_version", pa.string()),
    ("content_hash", pa.string()),
])

_NODE_SCHEMA = pa.schema([
    ("node_id", pa.string()),
    ("spec_id", pa.string()),
    ("kind", pa.string()),
    ("jsonPointer", pa.string()),
    ("name", pa.string()),
    ("key", pa.string()),
    ("index", pa.int64()),
    ("valueType", pa.string()),
    ("rawJson", pa.string()),
    ("scalarJson", pa.string()),
    ("isExtension", pa.bool_()),
])

_HAS_ROOT_SCHEMA = pa.schema([
    ("a", pa.string()),
    ("b", pa.string()),
])

_CHILD_SCHEMA = pa.schema([
    ("a", pa.string()),
    ("b", pa.string()),
    ("role", pa.string()),
    ("key", pa.string()),
    ("index", pa.int64()),
])

_REF_SCHEMA = pa.schema([
    ("a", pa.string()),
    ("b", pa.string()),
    ("ref", pa.string()),
])


def write_ast_graph(conn, graph: AstGraph) -> None:
    """Bulk-load one L1 AST graph into an open LadybugDB connection."""
    _copy(conn, "OasSpec", [graph.spec_row], _SPEC_SCHEMA)
    _copy(
        conn,
        "OasAstNode",
        [
            {
                "node_id": n.node_id,
                "spec_id": n.spec_id,
                "kind": n.kind,
                "jsonPointer": n.json_pointer,
                "name": n.name,
                "key": n.key,
                "index": n.index,
                "valueType": n.value_type,
                "rawJson": n.raw_json,
                "scalarJson": n.scalar_json,
                "isExtension": n.is_extension,
            }
            for n in graph.nodes
        ],
        _NODE_SCHEMA,
    )
    _copy(
        conn,
        "HAS_AST_ROOT",
        [{"a": graph.spec_id, "b": graph.root_node_id}],
        _HAS_ROOT_SCHEMA,
    )
    _copy(
        conn,
        "AST_CHILD",
        [
            {
                "a": e.parent_id,
                "b": e.child_id,
                "role": e.role,
                "key": e.key,
                "index": e.index,
            }
            for e in graph.child_edges
        ],
        _CHILD_SCHEMA,
    )
    _copy(
        conn,
        "AST_REF_TARGET",
        [
            {"a": e.ref_node_id, "b": e.target_node_id, "ref": e.ref}
            for e in graph.ref_edges
        ],
        _REF_SCHEMA,
    )


def build_ast_database(db_path: Path, graphs: list[AstGraph]) -> None:
    """Create an AST-only LadybugDB at ``db_path`` and write ``graphs``."""
    db_path.parent.mkdir(parents=True, exist_ok=True)
    db = lb.Database(str(db_path))
    try:
        conn = lb.Connection(db)
        apply_ast_schema(conn)
        for graph in graphs:
            write_ast_graph(conn, graph)
    finally:
        db.close()


def _copy(conn, table: str, rows: list[dict], schema: pa.Schema) -> None:
    if not rows:
        return
    columns: dict[str, list] = {field.name: [] for field in schema}
    for row in rows:
        for field in schema:
            columns[field.name].append(row.get(field.name))
    conn.execute(f"COPY {table} FROM $df", parameters={"df": pa.table(columns, schema=schema)})
