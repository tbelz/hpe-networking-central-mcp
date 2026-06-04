"""Task 1 — Resolved ingestion layer (ADR 011).

Wraps ``prance.ResolvingParser`` to turn a raw OpenAPI document into a
fully ``$ref``-resolved Python dict.  Failures are reported per-spec as
structured ``ResolutionFailure`` records; the caller decides whether a
batch with failures should halt the build.

This module performs two mutations on the input before handing it to
prance:

1. Strip keys beginning with ``_`` (e.g. ``_id``): the ReadMe.io export
   pipeline injects internal indexing metadata that strict OAS 3.1
   validation rejects via ``unevaluatedProperties: False``.
2. Coerce stringly-typed primitive defaults (e.g. ``"false"`` → ``false``
   for ``type: boolean``): the Aruba ReadMe.io export pipeline serialises
   Python booleans as their string representations.  OAS 3.1 / JSON
   Schema 2020-12 says ``default`` values SHOULD (not MUST) be valid
   against their declared type; ``openapi-spec-validator`` enforces this
   as a hard error.  We coerce the obvious cases here to avoid false
   failures.

No other normalisation is performed; substantive content flows through
unchanged so the downstream AST generator (Task 2) sees the original
spec.
"""

from __future__ import annotations

import json
import multiprocessing
import os
import threading
from concurrent.futures import ProcessPoolExecutor
from dataclasses import dataclass, field
from typing import Any, Literal

import prance
import prance.util.url


@dataclass
class ResolvedSpec:
    """A spec that successfully resolved all ``$ref``s."""

    spec: dict[str, Any]
    raw_spec: dict[str, Any]
    source: str
    title: str


@dataclass
class ResolutionFailure:
    """A spec that failed validation or ref resolution."""

    source: str
    title: str
    error: str
    error_type: Literal["validation", "resolution", "unexpected"]


@dataclass
class ResolutionResult:
    """Aggregated outcome of resolving a batch of specs."""

    resolved: list[ResolvedSpec] = field(default_factory=list)
    failed: list[ResolutionFailure] = field(default_factory=list)
    workers_used: int = 1

    @property
    def total(self) -> int:
        return len(self.resolved) + len(self.failed)


def _strip_underscore_keys(obj: Any) -> Any:
    """Recursively drop keys starting with ``_``.

    ReadMe.io's spec export injects ``_id`` (and occasionally other
    underscore-prefixed metadata) into every node.  Strict OAS 3.1
    validation rejects them because the meta-schema sets
    ``unevaluatedProperties: False``.
    """
    if isinstance(obj, dict):
        return {k: _strip_underscore_keys(v) for k, v in obj.items() if not k.startswith("_")}
    if isinstance(obj, list):
        return [_strip_underscore_keys(v) for v in obj]
    return obj


def _coerce_defaults(obj: Any) -> Any:
    """Coerce stringly-typed primitive ``default`` values to native JSON types.

    The Aruba ReadMe.io export pipeline serialises Python booleans as
    ``"true"``/``"false"`` strings rather than native JSON ``true``/``false``.
    ``openapi-spec-validator`` enforces the OAS 3.1 / JSON Schema 2020-12
    SHOULD recommendation that default values be valid against their
    declared type, treating it as a hard error.  We normalise the three
    affected primitive types (boolean, integer, number) to prevent false
    failures.

    Only acts when *both* ``type`` (as a scalar string) and ``default``
    are present in the same object.  All other content is returned
    unchanged.
    """
    if isinstance(obj, dict):
        result = {k: _coerce_defaults(v) for k, v in obj.items()}
        t = result.get("type")
        d = result.get("default")
        if isinstance(t, str) and isinstance(d, str):
            if t == "boolean":
                lowered = d.strip().lower()
                if lowered in ("true", "1"):
                    result["default"] = True
                elif lowered in ("false", "0", ""):
                    result["default"] = False
            elif t == "integer":
                try:
                    result["default"] = int(d)
                except ValueError:
                    pass
            elif t == "number":
                try:
                    result["default"] = float(d)
                except ValueError:
                    pass
        return result
    if isinstance(obj, list):
        return [_coerce_defaults(v) for v in obj]
    return obj


def _spec_title(spec: dict[str, Any], source: str) -> str:
    info = spec.get("info") or {}
    return info.get("title") or source


def clean_spec(spec: dict[str, Any]) -> dict[str, Any]:
    """Return the cleaned raw spec used as Task 1's compiler input.

    This removes ReadMe.io underscore metadata and coerces obvious
    stringly-typed primitive defaults, but does not resolve ``$ref``s or
    otherwise change OpenAPI structure. Task 2 consumes this cleaned raw
    form so reference topology remains visible in the L1 AST.
    """
    return _coerce_defaults(_strip_underscore_keys(spec))


def resolve_spec(spec: dict[str, Any], *, source: str) -> ResolvedSpec | ResolutionFailure:
    """Resolve all ``$ref``s in a single spec under strict validation.

    Args:
        spec: Raw OpenAPI document as a Python dict (e.g. ``json.load``
            of a cached spec file).
        source: Caller-supplied origin label, used in error messages and
            on the returned :class:`ResolvedSpec`.

    Returns:
        :class:`ResolvedSpec` on success, otherwise :class:`ResolutionFailure`
        with ``error_type`` set to ``"validation"``, ``"resolution"`` or
        ``"unexpected"``.  This function never raises for spec-level
        errors so callers can aggregate failures across a batch.
    """
    title = _spec_title(spec, source)
    cleaned = clean_spec(spec)
    try:
        parser = prance.ResolvingParser(
            spec_string=json.dumps(cleaned),
            backend="openapi-spec-validator",
            lazy=True,
            strict=True,
        )
        parser.parse()
    except prance.ValidationError as e:
        return ResolutionFailure(
            source=source, title=title, error=str(e), error_type="validation"
        )
    except prance.util.url.ResolutionError as e:
        return ResolutionFailure(
            source=source, title=title, error=str(e), error_type="resolution"
        )
    except Exception as e:  # noqa: BLE001 — prance can raise unwrapped library errors
        return ResolutionFailure(
            source=source,
            title=title,
            error=f"{type(e).__name__}: {e}",
            error_type="unexpected",
        )
    return ResolvedSpec(
        spec=parser.specification,
        raw_spec=cleaned,
        source=source,
        title=title,
    )


def resolve_specs(
    specs: list[dict[str, Any]],
    *,
    max_workers: int | None = None,
    retain_resolved_spec: bool = True,
) -> ResolutionResult:
    """Resolve a batch of specs.

    The ``source`` label for each spec combines the ``_spec_source``
    provider tag (``"central"``/``"glp"``) stamped by the spec providers
    with the spec title so that failure messages are individually
    identifiable.  Without the title, all Central failures would appear
    as the same ambiguous ``"central"`` label.  Specs without a provider
    tag use the title alone; specs without either fall back to
    ``"unknown"``.

    Batches use a bounded process pool because Prance validation/resolution is
    CPU-bound and each spec is independent. Small batches stay sequential to
    avoid process startup overhead. ``executor.map`` preserves input order, so
    the relative order inside the resolved and failed result buckets remains
    deterministic.
    """
    entries = [(raw, _source_label(raw), retain_resolved_spec) for raw in specs]
    workers = _resolve_worker_count(len(entries), max_workers)
    result = ResolutionResult(workers_used=workers)
    if workers == 1:
        outcomes = [_resolve_entry(entry) for entry in entries]
    else:
        executor_kwargs = {}
        if os.environ.get("PYTEST_CURRENT_TEST") or threading.active_count() > 1:
            executor_kwargs["mp_context"] = _safe_process_context()
        with ProcessPoolExecutor(max_workers=workers, **executor_kwargs) as executor:
            outcomes = list(executor.map(_resolve_entry, entries))
    for outcome in outcomes:
        if isinstance(outcome, ResolvedSpec):
            result.resolved.append(outcome)
        else:
            result.failed.append(outcome)
    return result


def _source_label(raw: dict[str, Any]) -> str:
    """Return the stable provider/title label used for one raw spec."""
    provider = raw.get("_spec_source")
    title = _spec_title(raw, "unknown")
    return f"{provider}/{title}" if provider else title


def _resolve_entry(
    entry: tuple[dict[str, Any], str, bool],
) -> ResolvedSpec | ResolutionFailure:
    """Process-pool compatible wrapper around :func:`resolve_spec`."""
    raw, source, retain_resolved_spec = entry
    outcome = resolve_spec(raw, source=source)
    if isinstance(outcome, ResolvedSpec) and not retain_resolved_spec:
        outcome.spec = {}
    return outcome


def _resolve_worker_count(total: int, requested: int | None) -> int:
    """Return a bounded worker count, avoiding pool overhead for small batches."""
    if requested is not None:
        if requested < 1:
            raise ValueError("max_workers must be at least 1")
        return min(total, requested) if total else 1
    if total < 8:
        return 1
    return min(total, 16, os.cpu_count() or 1)


def _safe_process_context() -> multiprocessing.context.BaseContext:
    """Avoid forking multithreaded callers while preferring a lightweight server."""
    methods = multiprocessing.get_all_start_methods()
    return multiprocessing.get_context("forkserver" if "forkserver" in methods else "spawn")
