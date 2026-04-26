"""Policy gate for API-call tools.

Tracks which API endpoints an agent has inspected (via
``get_api_endpoint_detail`` or ``get_api_endpoint_glossary``) before allowing
``call_central_api`` / ``call_greenlake_api`` to dispatch a request.

Designed to be extensible: future policies (e.g. requiring glossary
inspection before using OData filter parameters, per-endpoint rate limits,
forbidding ``limit=`` query parameters on collection endpoints) can be added
by extending :class:`InspectionTracker` with additional record kinds and
adding new predicates inside :func:`check_call_policy`.

**Process scope.** Each MCP server runs as a single OS process — the stdio
transport spawns one server process per Claude session. Module-level state
is therefore session-scoped automatically; multiple Claude tabs spawn
independent processes with independent trackers. If the transport is ever
changed to SSE or HTTP (one process serving many concurrent clients), this
module must be reworked to key state on a per-connection identifier from
the FastMCP request context.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

InspectionKind = Literal["skeleton", "glossary"]


def normalise_path(path: str) -> str:
    """Ensure a path starts with ``/`` to match the eid format used in the graph."""
    p = (path or "").strip()
    return p if p.startswith("/") else f"/{p}"


def _eid(method: str, path: str) -> str:
    return f"{method.upper()}:{normalise_path(path)}"


@dataclass
class InspectionRecord:
    """Per-endpoint inspection state. Extend with new flags for new policies."""

    skeleton: bool = False
    glossary: bool = False


@dataclass
class InspectionTracker:
    """In-process record of which endpoints have been inspected."""

    _records: dict[str, InspectionRecord] = field(default_factory=dict)

    def record(self, method: str, path: str, kind: InspectionKind) -> None:
        """Mark ``method``/``path`` as inspected with the given kind."""
        eid = _eid(method, path)
        rec = self._records.setdefault(eid, InspectionRecord())
        if kind == "skeleton":
            rec.skeleton = True
        elif kind == "glossary":
            rec.glossary = True

    def was_inspected(
        self,
        method: str,
        path: str,
        *,
        kinds: tuple[InspectionKind, ...] = ("skeleton", "glossary"),
    ) -> bool:
        """Return True if any of ``kinds`` has been recorded for the endpoint."""
        rec = self._records.get(_eid(method, path))
        if rec is None:
            return False
        return any(getattr(rec, k) for k in kinds)

    def reset(self) -> None:
        """Clear all recorded inspections. Test helper."""
        self._records.clear()


# Module-level singleton — see module docstring for scoping rationale.
_tracker = InspectionTracker()


def get_tracker() -> InspectionTracker:
    """Return the process-global :class:`InspectionTracker`."""
    return _tracker


def check_call_policy(method: str, path: str) -> tuple[bool, str | None]:
    """Decide whether ``call_central_api`` / ``call_greenlake_api`` may dispatch.

    Returns ``(True, None)`` if the call is permitted, otherwise
    ``(False, error_message)`` with a prescriptive next-step hint.

    Extend this function with additional predicates (limit checks,
    glossary requirements for filter parameters, etc.) — keep
    ``check_call_policy`` as the single decision point so the surface
    stays small and testable.
    """
    method_u = method.upper()
    norm_path = normalise_path(path)

    if not _tracker.was_inspected(method_u, norm_path):
        msg = (
            "Endpoint schema not consulted. Before calling this endpoint you must "
            "inspect it first with:\n\n"
            f"  get_api_endpoint_detail(method=\"{method_u}\", path=\"{norm_path}\")\n\n"
            "This reveals every parameter, required field, and response shape. "
            "Agents that skip this step routinely miss server-side filter "
            "parameters and pull oversized responses they then have to truncate "
            "locally. For semantic context on parameter meanings (e.g. OData "
            "filter syntax, enum semantics, units), additionally call "
            f"get_api_endpoint_glossary(method=\"{method_u}\", path=\"{norm_path}\"). "
            "Either tool satisfies the gate; both is recommended for endpoints "
            "with non-trivial filter parameters."
        )
        return False, msg

    return True, None
