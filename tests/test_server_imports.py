"""Regression test: the top-level server module must import cleanly.

The bug this guards against (caught manually 2026-04-25 in production):

    NameError: name 'shutil' is not defined

was introduced when ``shutil`` was removed from ``server.py`` during the
``knowledge_db`` extraction, even though ``server.py`` still calls
``shutil.copy2`` to seed ``central_helpers.py`` and ``_http_core.py``
into the script library at import time. No existing test imported
``server`` end-to-end (tests construct FastMCP locally and register
individual tool packs), so the broken module-level code path was only
discovered when the MCP server was launched by Claude Desktop.

This test imports ``hpe_networking_central_mcp.server`` with credentials
set and the network-touching boundaries stubbed, so any future
``NameError`` / ``ImportError`` introduced at module import time fails
the suite immediately.
"""

from __future__ import annotations

import importlib
import sys

import pytest

pytestmark = pytest.mark.unit


def test_server_module_imports_cleanly(monkeypatch, tmp_path):
    """Importing ``server`` must not raise — exercises every module-level statement."""
    # Force the credential gate open with throwaway values.
    monkeypatch.setenv("CENTRAL_BASE_URL", "https://example.invalid")
    monkeypatch.setenv("CENTRAL_CLIENT_ID", "test")
    monkeypatch.setenv("CENTRAL_CLIENT_SECRET", "test")
    monkeypatch.setenv("SCRIPT_LIBRARY_PATH", str(tmp_path / "scripts"))
    monkeypatch.setenv("GRAPH_DB_PATH", str(tmp_path / "graph.db"))
    monkeypatch.setenv("KNOWLEDGE_RELEASE_REPO", "")  # skip download path

    # Stub out the OAuth2 round-trip in CentralClient.validate so the
    # import does not require network access.
    from hpe_networking_central_mcp import central_client as cc

    monkeypatch.setattr(cc.CentralClient, "validate", lambda self: None)
    monkeypatch.setattr(cc.GreenLakeClient, "validate", lambda self: None)

    # Ensure a fresh import — server.py runs all its setup at module scope.
    sys.modules.pop("hpe_networking_central_mcp.server", None)

    module = importlib.import_module("hpe_networking_central_mcp.server")

    # Sanity: the names server.py is expected to export are bound.
    assert hasattr(module, "mcp")
    assert hasattr(module, "settings")
    assert hasattr(module, "logger")
