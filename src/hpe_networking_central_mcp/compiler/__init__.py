"""Compiler pipeline for the OAS-to-graph translation (ADR 011).

This package implements the four-task compiler that supersedes the
hand-curated graph populator.  Tasks are introduced incrementally:

* Task 1 — :mod:`.frontend` — resolved ingestion via ``prance``.
* Task 2 — lossless AST generator (not yet implemented).
* Task 3 — semantic overlay (not yet implemented).
* Task 4 — agent projection + MCP tool surface (not yet implemented).
"""
