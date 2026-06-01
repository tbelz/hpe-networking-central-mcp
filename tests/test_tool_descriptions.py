"""Tool-description length guard.

Claude's tool catalogue rejects descriptions longer than 2100 characters
(measured after ``inspect.cleandoc``, which is what FastMCP uses to render
docstrings into the ``Tool.description`` field). Exceeding the cap causes
the entire MCP server to be silently dropped from the agent's tool list —
no error surfaces to the user.

This test enforces the cap on every registered tool so that an over-long
docstring fails CI before it ever ships.
"""

from __future__ import annotations

import importlib
import inspect
import sys

import pytest

pytestmark = pytest.mark.unit

DESCRIPTION_CHAR_LIMIT = 2100


def _load_server_module(monkeypatch, tmp_path):
    monkeypatch.setenv("CENTRAL_BASE_URL", "https://example.invalid")
    monkeypatch.setenv("CENTRAL_CLIENT_ID", "test")
    monkeypatch.setenv("CENTRAL_CLIENT_SECRET", "test")
    monkeypatch.setenv("SCRIPT_LIBRARY_PATH", str(tmp_path / "scripts"))
    monkeypatch.setenv("GRAPH_DB_PATH", str(tmp_path / "graph.db"))
    monkeypatch.setenv("KNOWLEDGE_RELEASE_REPO", "")

    from hpe_networking_central_mcp import central_client as cc

    monkeypatch.setattr(cc.CentralClient, "validate", lambda self: None)
    monkeypatch.setattr(cc.GreenLakeClient, "validate", lambda self: None)

    _prev = sys.modules.get("hpe_networking_central_mcp.server")
    if _prev is not None:
        _ipc = getattr(_prev, "ipc_server", None)
        if _ipc is not None:
            _ipc.stop()
    sys.modules.pop("hpe_networking_central_mcp.server", None)
    return importlib.import_module("hpe_networking_central_mcp.server")


def test_all_tool_descriptions_under_limit(monkeypatch, tmp_path):
    """Every registered FastMCP tool must have a description ≤2100 chars."""
    module = _load_server_module(monkeypatch, tmp_path)
    tool_mgr = getattr(module.mcp, "_tool_manager", None)
    assert tool_mgr is not None, "FastMCP changed tool-manager attribute name"

    over_budget: list[tuple[str, int]] = []
    for tool in tool_mgr._tools.values():
        desc = (tool.description or "").strip()
        # FastMCP renders the docstring through inspect.cleandoc when
        # building the Tool object; mirror that here for parity in case a
        # future release stops trimming.
        cleaned = inspect.cleandoc(desc)
        if len(cleaned) > DESCRIPTION_CHAR_LIMIT:
            over_budget.append((tool.name, len(cleaned)))

    assert not over_budget, (
        "The following tool descriptions exceed the "
        f"{DESCRIPTION_CHAR_LIMIT}-char cap (Claude silently drops the "
        "whole server when this is breached):\n"
        + "\n".join(f"  - {name}: {n} chars" for name, n in over_budget)
    )
