"""Policy gate for API-call tools.

Tracks which API endpoints an agent has inspected (via
``get_api_endpoint_detail`` or ``get_api_endpoint_glossary``) before allowing
``call_central_api`` / ``call_greenlake_api`` to dispatch a request.

The gate is **template-aware**: an inspection of
``/network-monitoring/v1/gateways/{serial-number}/dhcp-pools`` automatically
authorises a call to the concrete instantiation
``/network-monitoring/v1/gateways/DL0006948/dhcp-pools``. The catalog stores
endpoints by template, so this is the only sane key to track inspections
against; without it the gate forces an impossible inspection of every
concrete URL.

When the gate blocks a call it inlines the matched endpoint's skeleton into
the error response (RFC 7807 / actionable-error style) so the agent can
correct course in a single turn instead of round-tripping back to
``get_api_endpoint_detail``.

**Process scope.** Each MCP server runs as a single OS process — the stdio
transport spawns one server process per Claude session. Module-level state
is therefore session-scoped automatically; multiple Claude tabs spawn
independent processes with independent trackers. If the transport is ever
changed to SSE or HTTP (one process serving many concurrent clients), this
module must be reworked to key state on a per-connection identifier from
the FastMCP request context.

**Extension seam.** :func:`check_call_policy` is the single pre-call
decision point. A future ``check_response_policy`` (post-call) can hook the
same ``_make_api_call`` path to warn on missing filters, oversized
responses, or other post-hoc conditions; the registry below already gives
that future hook access to the matched template, which is the correct key
for per-endpoint heuristics.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field
from typing import Callable, Literal

InspectionKind = Literal["skeleton", "glossary"]


def normalise_path(path: str) -> str:
    """Ensure a path starts with ``/`` to match the eid format used in the graph."""
    p = (path or "").strip()
    return p if p.startswith("/") else f"/{p}"


def _eid(method: str, path: str) -> str:
    return f"{method.upper()}:{normalise_path(path)}"


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
    """Tracks known endpoint templates per HTTP method.

    Concrete paths (without ``{name}`` segments) compile to a regex that
    matches only themselves, so non-parameterised endpoints flow through
    the same lookup without a special case.
    """

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
class InspectionRecord:
    """Per-endpoint inspection state. Extend with new flags for new policies."""

    skeleton: bool = False
    glossary: bool = False


@dataclass
class InspectionTracker:
    """In-process record of which endpoints have been inspected.

    Inspections are keyed by **template** path (the form stored in the
    catalog); :func:`check_call_policy` resolves concrete paths to their
    template before lookup.
    """

    _records: dict[str, InspectionRecord] = field(default_factory=dict)

    def record(self, method: str, path: str, kind: InspectionKind) -> None:
        """Mark ``method``/``path`` as inspected with the given kind.

        ``path`` should normally be the template form; passing a concrete
        path is harmless for non-parameterised endpoints.

        Raises:
            ValueError: If ``kind`` is not a known :data:`InspectionKind`.
        """
        eid = _eid(method, path)
        rec = self._records.setdefault(eid, InspectionRecord())
        if kind == "skeleton":
            rec.skeleton = True
        elif kind == "glossary":
            rec.glossary = True
        else:
            raise ValueError(
                f"Unknown inspection kind: {kind!r}. "
                "Expected one of: 'skeleton', 'glossary'."
            )

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


# ── Skeleton fetcher (for inlining into block messages) ─────────────


SkeletonFetcher = Callable[[str, str], str | None]
"""``(method, template_path) -> skeleton_json_text`` or ``None`` if unavailable."""

_skeleton_fetcher: SkeletonFetcher | None = None


def set_skeleton_fetcher(fn: SkeletonFetcher | None) -> None:
    """Register a callback that the gate uses to inline endpoint schemas.

    Decoupled from the catalog tool to keep this module free of graph
    imports. The callback should return a compact JSON string (or ``None``
    if no skeleton is available); the gate falls back to a plain
    instruction in that case.
    """
    global _skeleton_fetcher
    _skeleton_fetcher = fn


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
    ``(False, error_message)`` with an inlined endpoint skeleton (when
    available) so the agent can correct course in a single turn.

    Resolution order:
      1. Literal eid match (fast path; covers non-parameterised endpoints
         and cases where the agent inspected a concrete URL).
      2. Template match — find the catalog template that the concrete
         path instantiates, then check whether that template was inspected.

    On block, if a skeleton is available, the inspection is auto-recorded
    against the matched template — the agent has been given the schema in
    the response, so a follow-up ``get_api_endpoint_detail`` round-trip is
    redundant.
    """
    method_u = method.upper()
    norm_path = normalise_path(path)

    if _tracker.was_inspected(method_u, norm_path):
        return True, None

    template = _registry.match(method_u, norm_path)
    if template is not None and _tracker.was_inspected(method_u, template):
        return True, None

    inspect_path = template or norm_path
    skeleton_text: str | None = None
    if _skeleton_fetcher is not None:
        try:
            skeleton_text = _skeleton_fetcher(method_u, inspect_path)
        except Exception:  # noqa: BLE001 — best-effort enrichment
            skeleton_text = None

    lines = ["Endpoint schema not consulted before calling.", ""]
    if template is not None and template != norm_path:
        lines += [
            f"Concrete path {norm_path!r} matches catalog template "
            f"{template!r}. The catalog stores endpoints by template, so "
            "inspections are recorded against the template form.",
            "",
        ]

    if skeleton_text:
        lines += [
            "Endpoint skeleton (consult before retrying):",
            "",
            skeleton_text,
            "",
            "Re-call this tool with the same arguments — the gate will "
            "allow it on the next attempt now that the skeleton has been "
            "shown.",
        ]
        # Auto-record now that the skeleton is in the response. Saves a
        # round-trip and keeps the gate at exactly one block per endpoint.
        _tracker.record(method_u, inspect_path, "skeleton")
    else:
        lines += [
            "Inspect it first with:",
            "",
            f"  get_api_endpoint_detail(method={method_u!r}, path={inspect_path!r})",
            "",
            "This reveals every parameter, required field, and response "
            "shape. For semantic context on parameter meanings (e.g. OData "
            "filter syntax, enum semantics), additionally call "
            f"get_api_endpoint_glossary(method={method_u!r}, "
            f"path={inspect_path!r}).",
        ]

    return False, "\n".join(lines)
