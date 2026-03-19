"""Unix domain socket IPC server for graph database access.

Allows script subprocesses to query/execute Cypher against the Kùzu database
held open by the main MCP server process, avoiding file-lock conflicts.

Protocol: newline-delimited JSON over a Unix domain socket.
  Request:  {"id": N, "method": "query"|"execute", "cypher": "...", "params": {}}
  Response: {"id": N, "result": [...]} | {"id": N, "error": "..."}
"""

from __future__ import annotations

import json
import os
import socketserver
import threading
from pathlib import Path
from typing import TYPE_CHECKING

import structlog

if TYPE_CHECKING:
    from .manager import GraphManager

logger = structlog.get_logger("graph.ipc_server")


class _GraphRequestHandler(socketserver.StreamRequestHandler):
    """Handle one IPC connection: read newline-delimited JSON requests."""

    def handle(self) -> None:
        manager: GraphManager = self.server.graph_manager  # type: ignore[attr-defined]
        for raw_line in self.rfile:
            line = raw_line.strip()
            if not line:
                continue
            try:
                req = json.loads(line)
            except json.JSONDecodeError as exc:
                self._send({"error": f"Invalid JSON: {exc}"})
                continue

            req_id = req.get("id")
            method = req.get("method", "")
            cypher = req.get("cypher", "")
            params = req.get("params") or {}

            if method not in ("query", "execute"):
                self._send({"id": req_id, "error": f"Unknown method: {method}"})
                continue

            try:
                if method == "query":
                    rows = manager.query(cypher, read_only=False)
                else:
                    rows = manager.execute(cypher, params)
                self._send({"id": req_id, "result": rows})
            except Exception as exc:
                self._send({"id": req_id, "error": str(exc)})

    def _send(self, obj: dict) -> None:
        data = json.dumps(obj, default=str) + "\n"
        self.wfile.write(data.encode("utf-8"))
        self.wfile.flush()


class _ThreadedUnixServer(socketserver.ThreadingMixIn, socketserver.UnixStreamServer):
    daemon_threads = True
    allow_reuse_address = True


class GraphIPCServer:
    """Manages the lifecycle of the Unix domain socket IPC server."""

    def __init__(self, socket_path: Path, graph_manager: GraphManager) -> None:
        self._socket_path = socket_path
        self._graph_manager = graph_manager
        self._server: _ThreadedUnixServer | None = None
        self._thread: threading.Thread | None = None

    @property
    def socket_path(self) -> Path:
        return self._socket_path

    def start(self) -> None:
        """Start listening on the Unix domain socket in a background thread."""
        sock_str = str(self._socket_path)
        # Clean up stale socket file
        if self._socket_path.exists():
            os.unlink(sock_str)
        self._socket_path.parent.mkdir(parents=True, exist_ok=True)

        self._server = _ThreadedUnixServer(sock_str, _GraphRequestHandler)
        self._server.graph_manager = self._graph_manager  # type: ignore[attr-defined]

        self._thread = threading.Thread(target=self._server.serve_forever, daemon=True)
        self._thread.start()
        logger.info("graph_ipc_started", socket=sock_str)

    def stop(self) -> None:
        """Shut down the server and clean up the socket file."""
        if self._server is not None:
            self._server.shutdown()
            self._server.server_close()
            self._server = None
        if self._socket_path.exists():
            os.unlink(str(self._socket_path))
        logger.info("graph_ipc_stopped")
