"""Post-flush invariants for the schema-graph ingestion pipeline.

Runs against a freshly populated knowledge DB to catch shape-level
regressions that hand-written test fixtures cannot reproduce —
specifically the failure mode where a named ``SchemaComponent`` is
persisted with a non-trivial object body but has zero ``HAS_PROPERTY``
/ ``COMPOSED_OF`` decomposition edges (the "stub-wins" / "in-batch
shadow" bug class addressed by ADR-011).

Surfaces violations as :class:`InvariantViolation` (raised in strict
mode) or as a structured list returned for inspection.
"""
from __future__ import annotations

import json
from dataclasses import dataclass, field


@dataclass
class InvariantViolation:
    invariant: str
    detail: str
    sample: list[dict] = field(default_factory=list)

    def __str__(self) -> str:  # pragma: no cover - cosmetic
        if not self.sample:
            return f"[{self.invariant}] {self.detail}"
        head = ", ".join(
            f"{row.get('component_id') or row.get('id') or row}"
            for row in self.sample[:5]
        )
        return f"[{self.invariant}] {self.detail} :: {head}"


class InvariantViolationError(AssertionError):
    """Raised by :func:`assert_graph_invariants` in strict mode."""

    def __init__(self, violations: list[InvariantViolation]):
        self.violations = violations
        body = "\n  ".join(str(v) for v in violations)
        super().__init__(f"{len(violations)} graph invariant(s) violated:\n  {body}")


def _rows(conn, cypher: str, params: dict | None = None) -> list[dict]:
    return list(conn.execute(cypher, parameters=params or {}).rows_as_dict())


# Anonymous inline components (e.g. ``schema#prop:foo[items]``) are
# allowed to have empty bodyJson: they are leaves of inline promotion
# whose decomposition lives on the parent. Named ``SchemaComponent``
# rows (those whose component_id has the canonical ``provider:section:Name``
# shape with no ``#`` separator) are the ones that must decompose.
_NAMED_COMPONENT_FILTER = "NOT c.component_id CONTAINS '#'"


def check_named_components_decompose(conn) -> InvariantViolation | None:
    """INV-1: every reachable, non-primitive named SchemaComponent that
    has a non-empty object/array bodyJson MUST have at least one outgoing
    ``HAS_PROPERTY``, ``COMPOSED_OF`` or ``HAS_VALUE_SCHEMA`` edge.

    Reachability is approximated by ``BODY_REFERENCES`` (from endpoints'
    request/response bodies) plus ``COMPOSED_OF`` (parent components).
    Components only reached via parameter-level bodies are also covered
    because the parameter walk populates the same edges as bodies.
    """
    rows = _rows(
        conn,
        f"""
        MATCH (c:SchemaComponent)
        WHERE c.bodyJson <> ''
          AND c.kind <> 'primitive'
          AND c.kind <> 'unresolved'
          AND c.bodyShape IN ['object', 'union-oneOf', 'union-anyOf', 'union-allOf', 'map']
          AND {_NAMED_COMPONENT_FILTER}
          AND NOT EXISTS {{ MATCH (c)-[:HAS_PROPERTY]->() }}
          AND NOT EXISTS {{ MATCH (c)-[:COMPOSED_OF]->() }}
          AND NOT EXISTS {{ MATCH (c)-[:HAS_VALUE_SCHEMA]->() }}
        RETURN c.component_id AS component_id,
               c.name AS name,
               c.section AS section,
               c.bodyShape AS bodyShape,
               size(c.bodyJson) AS bodyLen
        ORDER BY bodyLen DESC
        LIMIT 25
        """,
    )
    if not rows:
        return None
    return InvariantViolation(
        invariant="INV-1",
        detail=(
            f"{len(rows)}+ named SchemaComponents have non-empty object/union/map "
            "bodyJson but no HAS_PROPERTY / COMPOSED_OF / HAS_VALUE_SCHEMA edges "
            "(stub-wins or eviction-skip regression)"
        ),
        sample=rows,
    )


def check_no_primitive_with_object_body(conn) -> InvariantViolation | None:
    """INV-2: ``kind='primitive'`` rows must not carry an object/union body."""
    rows = _rows(
        conn,
        """
        MATCH (c:SchemaComponent)
        WHERE c.kind = 'primitive'
          AND c.bodyJson <> ''
          AND c.bodyShape IN ['object', 'union-oneOf', 'union-anyOf', 'union-allOf', 'map']
        RETURN c.component_id AS component_id, c.bodyShape AS bodyShape
        LIMIT 25
        """,
    )
    if not rows:
        return None
    return InvariantViolation(
        invariant="INV-2",
        detail=f"{len(rows)} primitive SchemaComponents carry object/union bodies",
        sample=rows,
    )


def check_no_duplicate_component_ids(conn) -> InvariantViolation | None:
    """INV-3: ``component_id`` is the primary key — duplicates indicate
    flush ordering / replacement bugs.
    """
    rows = _rows(
        conn,
        """
        MATCH (c:SchemaComponent)
        WITH c.component_id AS cid, COUNT(*) AS n
        WHERE n > 1
        RETURN cid AS component_id, n
        LIMIT 25
        """,
    )
    if not rows:
        return None
    return InvariantViolation(
        invariant="INV-3",
        detail=f"{len(rows)} component_ids are duplicated",
        sample=rows,
    )


def check_inherited_chain_resolves(conn) -> InvariantViolation | None:
    """INV-4: every name in a Property's ``inheritedFromChain`` must
    correspond to at least one ``SchemaComponent`` row by name. A
    dangling reference would mean we dropped a component without
    cleaning its consumers.
    """
    rows = _rows(
        conn,
        """
        MATCH (p:Property)
        WHERE size(p.inheritedFromChain) > 0
        UNWIND p.inheritedFromChain AS anc
        WITH DISTINCT anc
        WHERE NOT EXISTS {
            MATCH (c:SchemaComponent {name: anc})
        }
        RETURN anc AS missing_name
        LIMIT 25
        """,
    )
    if not rows:
        return None
    return InvariantViolation(
        invariant="INV-4",
        detail=f"{len(rows)} inheritedFromChain entries reference unknown components",
        sample=rows,
    )


_CHECKS = (
    check_named_components_decompose,
    check_no_primitive_with_object_body,
    check_no_duplicate_component_ids,
    check_inherited_chain_resolves,
)


def assert_graph_invariants(conn, *, strict: bool = True) -> list[InvariantViolation]:
    """Run every invariant against ``conn``.

    Returns the list of violations. When ``strict`` is true (the default
    used by ``build_knowledge_db.py --strict`` and the smoke tests),
    raises :class:`InvariantViolationError` if any check fails.
    """
    violations: list[InvariantViolation] = []
    for check in _CHECKS:
        v = check(conn)
        if v is not None:
            violations.append(v)
    if violations and strict:
        raise InvariantViolationError(violations)
    return violations


def format_report(violations: list[InvariantViolation]) -> str:
    """Pretty-print for the build script log."""
    if not violations:
        return "✓ graph invariants OK (4/4 checks passed)"
    lines = [f"⚠ {len(violations)} graph invariant(s) violated:"]
    for v in violations:
        lines.append(f"  • {v.invariant}: {v.detail}")
        for row in v.sample[:3]:
            lines.append(f"      {json.dumps(row, default=str)}")
        if len(v.sample) > 3:
            lines.append(f"      ... (+{len(v.sample) - 3} more)")
    return "\n".join(lines)
