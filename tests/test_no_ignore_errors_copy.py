"""Regression guard: ``COPY ... (ignore_errors=true)`` is banned.

Kuzu 0.15.x silently drops valid rows from rel-table COPY when the
``ignore_errors=true`` flag is set (no diagnostic, no exception). This
produced ~19% orphan Property nodes in real-spec builds until the flag
was removed in Phase A of the data-quality contract.

A textual scan is sufficient because every COPY in the populator goes
through the two helpers defined in ``oas_schema_graph.py``.
"""

from __future__ import annotations

from pathlib import Path

import pytest


_BANNED = "ignore_errors=true"


def _src_files() -> list[Path]:
    repo = Path(__file__).resolve().parent.parent
    return sorted(
        p
        for folder in ("src", "scripts")
        for p in (repo / folder).rglob("*.py")
    )


@pytest.mark.parametrize("path", _src_files(), ids=lambda p: p.name)
def test_no_ignore_errors_in_copy(path: Path) -> None:
    text = path.read_text(encoding="utf-8", errors="ignore")
    # Only flag actual Cypher COPY statements (have COPY + FROM + the
    # flag on one line). Docstrings/comments that *mention* the ban are
    # fine — that's what this very test does in its module docstring.
    for lineno, line in enumerate(text.splitlines(), start=1):
        stripped = line.lstrip()
        if stripped.startswith("#"):
            continue
        if _BANNED in line and "COPY" in line and "FROM" in line:
            pytest.fail(
                f"{path}:{lineno}: COPY uses banned flag "
                f"`{_BANNED}` — silently drops rows in Kuzu 0.15.x"
            )
