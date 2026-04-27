"""Policy gate for API-call tools.

Tracks which API endpoints an agent has inspected (via
``describe_endpoint_for_device``) before allowing
``call_central_api`` / ``call_greenlake_api`` to dispatch a request.

The gate is **template-aware**: an inspection of
``/network-monitoring/v1/gateways/{serial-number}/dhcp-pools`` automatically
authorises a call to the concrete instantiation
``/network-monitoring/v1/gateways/DL0006948/dhcp-pools``. The catalog stores
endpoints by template, so this is the only sane key to track inspections
against; without it the gate forces an impossible inspection of every
concrete URL.

When the gate blocks a call it inlines the matched endpoint's property
summary into the error response (RFC 7807 / actionable-error style) so the
agent can correct course in a single turn instead of round-tripping back to
``describe_endpoint_for_device``.

**Process scope.** Each MCP server runs as a single OS process — the stdio
transport spawns one server process per Claude session. Module-level state
is therefore session-scoped automatically; multiple Claude tabs spawn
independent processes with independent trackers. If the transport is ever
changed to SSE or HTTP (one process serving many concurrent clients), this
module must be reworked to key state on a per-connection identifier from
the FastMCP request context.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable


def normalise_path(path: str) -> str:
    """Ensure a path starts with ``/`` to match the eid format used in the graph."""
    p = (path or "").strip()
    return p if p.startswith("/") else f"/{p}"


def eid_for(method: str, path: str) -> str:
    """Canonical endpoint id: ``METHOD:/path``.

    Used by the gate's inspection tracker and by the explicit-bypass
    parameter on ``call_central_api`` / ``call_greenlake_api`` so both
    sides agree on what counts as "the same endpoint".
    """
    return f"{method.upper()}:{normalise_path(path)}"


# Internal alias kept for the existing private call sites in this module.
_eid = eid_for


# ── Template registry ───────────────────────────────────────────────


_PLACEHOLDER_RE = re.compile(r"\{[^/{}]+\}")


def _compile_template(template_path: str) -> re.Pattern[str]:
    """Compile ``/foo/{id}/bar`` into a regex matching one path component
    per ``{name}`` segment."""
    norm = normalise_path(template_path)
    parts = _PLACEHOLDER_RE.split(norm)
    regex = "^" + "[^/]+".join(re.escape(p) for p in parts) + "$"
    return re.compile(regex)


@dataclass
class _CompiledTemplate:
    template: str
    pattern: re.Pattern[str]


@dataclass
class EndpointRegistry:
    """Tracks known endpoint templates per HTTP method."""

    _by_method: dict[str, list[_CompiledTemplate]] = field(default_factory=dict)

    def register(self, method: str, paths: list[str]) -> None:
        compiled = [
            _CompiledTemplate(template=normalise_path(p), pattern=_compile_template(p))
            for p in paths
        ]
        self._by_method[method.upper()] = compiled

    def match(self, method: str, concrete_path: str) -> str | None:
        """Return the template that matches ``concrete_path``, or ``None``."""
        norm = normalise_path(concrete_path)
        for ct in self._by_method.get(method.upper(), ()):
            if ct.pattern.match(norm):
                return ct.template
        return None

    def reset(self) -> None:
        self._by_method.clear()


# ── Inspection tracker ──────────────────────────────────────────────


@dataclass
class InspectionTracker:
    """In-process record of which endpoints have been inspected.

    Inspections are keyed by **template** path (the form stored in the
    catalog); :func:`check_call_policy` resolves concrete paths to their
    template before lookup.
    """

    _records: set[str] = field(default_factory=set)

    def record(self, method: str, path: str) -> None:
        """Mark ``method``/``path`` as inspected."""
        self._records.add(_eid(method, path))

    def was_inspected(self, method: str, path: str) -> bool:
        """Return True if the endpoint has been recorded as inspected."""
        return _eid(method, path) in self._records

    def reset(self) -> None:
        """Clear all recorded inspections. Test helper."""
        self._records.clear()


# ── Property-summary fetcher (for inlining into block messages) ─────


PropertySummaryFetcher = Callable[[str, str], str | None]
"""``(method, template_path) -> property_summary_json`` or ``None``."""

_property_summary_fetcher: PropertySummaryFetcher | None = None


def set_property_summary_fetcher(fn: PropertySummaryFetcher | None) -> None:
    """Register a callback that the gate uses to inline endpoint property
    summaries.

    Decoupled from the catalog tool to keep this module free of graph
    imports. The callback should return a compact JSON string (or ``None``
    if no property summary is available); the gate falls back to a plain
    instruction in that case.
    """
    global _property_summary_fetcher
    _property_summary_fetcher = fn


# ── Module singletons ──────────────────────────────────────────────

_tracker = InspectionTracker()
_registry = EndpointRegistry()


def get_tracker() -> InspectionTracker:
    """Return the process-global :class:`InspectionTracker`."""
    return _tracker


def get_registry() -> EndpointRegistry:
    """Return the process-global :class:`EndpointRegistry`."""
    return _registry


def register_endpoints(method_to_paths: dict[str, list[str]]) -> None:
    """Bulk-register known endpoint templates. Called once at server startup."""
    for method, paths in method_to_paths.items():
        _registry.register(method, paths)


# ── Decision point ─────────────────────────────────────────────────


def check_call_policy(method: str, path: str) -> tuple[bool, str | None]:
    """Decide whether ``call_central_api`` / ``call_greenlake_api`` may dispatch.

    Returns ``(True, None)`` if the call is permitted, otherwise
    ``(False, error_message)`` with an inlined property summary (when
    available) so the agent can correct course in a single turn.

    Resolution order:
      1. Literal eid match (fast path).
      2. Template match.

    On block, if a property summary is available, the inspection is
    auto-recorded against the matched template — the agent has been given
    the field list in the response, so a follow-up
    ``describe_endpoint_for_device`` round-trip is redundant.
    """
    method_u = method.upper()
    norm_path = normalise_path(path)

    if _tracker.was_inspected(method_u, norm_path):
        return True, None

    template = _registry.match(method_u, norm_path)
    if template is not None and _tracker.was_inspected(method_u, template):
        return True, None

    inspect_path = template or norm_path
    summary_text: str | None = None
    if _property_summary_fetcher is not None:
        try:
            summary_text = _property_summary_fetcher(method_u, inspect_path)
        except Exception:  # noqa: BLE001 — best-effort enrichment
            summary_text = None

    lines = ["Endpoint schema not consulted before calling.", ""]
    if template is not None and template != norm_path:
        lines += [
            f"Concrete path {norm_path!r} matches catalog template "
            f"{template!r}. The catalog stores endpoints by template, so "
            "inspections are recorded against the template form.",
            "",
        ]

    if summary_text:
        lines += [
            "Endpoint property summary (consult before retrying):",
            "",
            summary_text,
            "",
            "Re-call this tool with the same arguments — the gate will "
            "allow it on the next attempt now that the property summary "
            "has been shown.",
            "",
            "For ad-hoc structural questions, query the graph directly "
            "with `query_graph` (Property / SchemaComponent / Parameter "
            "node tables) or call "
            f"describe_endpoint_for_device(method={method_u!r}, "
            f"path={inspect_path!r}, deviceType=...) to filter by device "
            "type.",
        ]
        _tracker.record(method_u, inspect_path)
    else:
        lines += [
            "Inspect it first with:",
            "",
            f"  describe_endpoint_for_device(method={method_u!r}, path={inspect_path!r})",
            "",
            "This returns one record per leaf property of the endpoint's "
            "request body (or 200 response): name, type, format, required, "
            "readOnly, enumValues, description, supportedDeviceTypes, "
            "yangPath, inheritedFrom, and the full extensions dict. For "
            "ad-hoc structural questions, use `query_graph` against the "
            "Property / SchemaComponent / Parameter node tables.",
        ]

    return False, "\n".join(lines)
