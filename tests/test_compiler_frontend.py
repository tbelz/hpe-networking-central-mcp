"""Tests for the compiler frontend (Task 1 — resolved ingestion).

ADR 011 commits us to validating the compiler against the **real**
Central OpenAPI corpus.  These tests use the ``real_central_specs``
fixture from ``conftest.py`` — they auto-skip when the cache is not
hydrated, so the fast dev loop without specs still passes.

The few tests that need a deterministically malformed spec construct
one **by mutating a real spec**, not by inventing a synthetic stub, so
the test still exercises the real shape.
"""

from __future__ import annotations

import copy
import json
from pathlib import Path

import pytest

from hpe_networking_central_mcp.compiler.frontend import (
    ResolutionFailure,
    ResolutionResult,
    ResolvedSpec,
    resolve_spec,
    resolve_specs,
)
from typing import Any

# Smoke-test sample stride. ``corpus[::SMOKE_STRIDE]`` distributes the
# sample across category subdirectories instead of biasing toward a
# single alphabetic prefix.  With ~1600 specs and stride 20 the sample
# is ~80 specs which completes in well under a minute.
SMOKE_STRIDE = 32


def _load(path: Path) -> dict:
    return json.loads(path.read_text(encoding="utf-8"))


def _has_unresolved_refs(obj: Any) -> bool:
    """Return True if any dict in obj has a literal ``$ref`` key.

    Checking the JSON dump for the substring ``'"$ref"'`` is a false-fail
    risk: spec descriptions or example strings may contain the literal
    text ``$ref`` without it being an unresolved reference.  This
    function checks the dict key precisely instead.
    """
    if isinstance(obj, dict):
        if "$ref" in obj:
            return True
        return any(_has_unresolved_refs(v) for v in obj.values())
    if isinstance(obj, list):
        return any(_has_unresolved_refs(v) for v in obj)
    return False


def _find_resolvable(paths: list[Path], limit: int = 50) -> tuple[dict, Path]:
    """Return ``(spec_dict, path)`` for the first spec in *paths* that
    resolves cleanly under ``resolve_spec``.

    Using ``real_central_specs[0]`` directly is brittle because corpus
    ordering can change and the first spec is not guaranteed to be
    structurally valid.  This helper searches up to *limit* specs so
    tests remain correct regardless of which spec lands at index 0.
    Skips the test (does not fail it) when no resolvable spec is found.
    """
    for p in paths[:limit]:
        s = _load(p)
        if isinstance(resolve_spec(s, source="probe"), ResolvedSpec):
            return s, p
    pytest.skip(
        f"None of the first {limit} specs in the corpus resolved cleanly; "
        "corpus may be missing or all sampled specs are defective."
    )


# ── Single-spec resolution ─────────────────────────────────────────


@pytest.mark.compiler
@pytest.mark.real_spec
def test_resolves_a_real_central_spec(real_central_specs: list[Path]) -> None:
    """Resolving a real Central spec inlines every $ref."""
    spec, _ = _find_resolvable(real_central_specs)
    result = resolve_spec(spec, source="smoke")
    assert isinstance(result, ResolvedSpec), getattr(result, "error", None)
    # ResolvingParser fully inlines refs; the resolved doc must contain no
    # unresolved $ref keys.  We check dict keys specifically rather than
    # searching the JSON string, since descriptions/examples may legitimately
    # contain the text "$ref" without being an unresolved reference.
    assert not _has_unresolved_refs(result.spec)


@pytest.mark.compiler
@pytest.mark.real_spec
def test_underscore_metadata_is_stripped(real_central_specs: list[Path]) -> None:
    """ReadMe.io ``_id`` keys are removed before validation.

    Without this, every Central spec fails strict OAS 3.1 validation
    because the meta-schema sets ``unevaluatedProperties: False``.
    """
    # Find a spec that both has underscore metadata AND resolves cleanly.
    # Searching instead of using index 0 makes the test robust to corpus
    # reordering and specs that happen to lack _id injection.
    spec = None
    for p in real_central_specs[:50]:
        candidate = _load(p)
        has_underscore = (
            any(k.startswith("_") for k in candidate)
            or "_id" in json.dumps(candidate)[:500]
        )
        if has_underscore and isinstance(resolve_spec(candidate, source="probe"), ResolvedSpec):
            spec = candidate
            break
    if spec is None:
        pytest.skip(
            "No resolvable spec with underscore-prefixed keys found in first 50."
        )
    result = resolve_spec(spec, source="strip-check")
    assert isinstance(result, ResolvedSpec)
    assert "_id" not in result.spec  # top-level
    # Spot-check one nested level
    components = result.spec.get("components") or {}
    for v in components.values():
        if isinstance(v, dict):
            assert "_id" not in v


# ── Failure modes — mutate real specs to provoke them ──────────────


@pytest.mark.compiler
@pytest.mark.real_spec
def test_validation_failure_returns_structured_failure(
    real_central_specs: list[Path],
) -> None:
    """An OAS-invalid mutation produces a ``ResolutionFailure``, not an exception."""
    # Use a confirmed-resolvable spec so the mutation (not a pre-existing
    # defect) is the cause of any validation failure.
    spec, _ = _find_resolvable(real_central_specs)
    spec.pop("info", None)  # info is REQUIRED by the OAS 3.1 schema.
    result = resolve_spec(spec, source="broken-info")
    assert isinstance(result, ResolutionFailure)
    assert result.error_type == "validation"
    assert result.source == "broken-info"
    assert result.error  # non-empty


@pytest.mark.compiler
@pytest.mark.real_spec
def test_resolution_failure_returns_structured_failure(
    real_central_specs: list[Path],
) -> None:
    """A dangling $ref produces a ``ResolutionFailure`` with type ``resolution``."""
    # Find a real spec that uses internal refs, then break one.
    for path in real_central_specs[:50]:
        spec = _load(path)
        text = json.dumps(spec)
        if '"$ref"' in text:
            mutated = json.loads(text.replace('"#/components/', '"#/ghost/', 1))
            result = resolve_spec(mutated, source="broken-ref")
            assert isinstance(result, ResolutionFailure)
            # Could be resolution OR validation depending on which check fires
            # first inside prance, but it must NOT silently succeed.
            assert result.error_type in {"resolution", "validation"}
            assert result.error
            return
    pytest.skip("No real spec in the first 50 contains an internal $ref to break.")


# ── Batch aggregation ──────────────────────────────────────────────


@pytest.mark.compiler
@pytest.mark.real_spec
def test_resolve_specs_aggregates_mixed_batch(
    real_central_specs: list[Path],
) -> None:
    """A batch with one bad spec yields a ResolutionResult with both buckets populated."""
    good, _ = _find_resolvable(real_central_specs)
    good["_spec_source"] = "central"
    bad = copy.deepcopy(good)
    bad.pop("info", None)
    bad["_spec_source"] = "central"

    result = resolve_specs([good, bad])
    assert isinstance(result, ResolutionResult)
    assert result.total == 2
    assert len(result.resolved) == 1
    assert len(result.failed) == 1
    # resolve_specs builds source as "provider/title" so failures are
    # individually identifiable in build logs (Bug 1 fix).
    assert result.resolved[0].source.startswith("central/")
    assert result.failed[0].source.startswith("central/")
    assert result.failed[0].error_type == "validation"


# ── Coverage smoke against a real-spec sample ──────────────────────


@pytest.mark.compiler
@pytest.mark.real_spec
def test_smoke_resolve_real_spec_sample(real_central_specs: list[Path]) -> None:
    """Resolve a deterministic stride sample across the whole corpus.

    The compiler contract (ADR 011) is "lossless or fail loudly" per
    spec, not a corpus-wide coverage percentage.  The invariants this
    test pins are therefore *behavioural*, not statistical:

    * Every spec yields either a ``ResolvedSpec`` or a classified
      ``ResolutionFailure`` — ``resolve_specs`` never raises.
    * No failure is ever bucketed as ``"unexpected"``.  Unexpected
      classification means the resolver leaked an exception class we
      didn't anticipate; that's a resolver bug, not an upstream bug.
    * The corpus pass/fail mix is *recorded* (printed below the assert
      block) so reviewers can spot a drop in the upstream spec quality,
      but it is not pinned.
    """
    sample = real_central_specs[::SMOKE_STRIDE]
    assert len(sample) >= 20, "Stride sample too small — corpus shrank?"
    specs = [_load(p) for p in sample]
    for s, p in zip(specs, sample):
        s["_spec_source"] = f"central:{p.parent.name}/{p.name}"

    result = resolve_specs(specs)
    assert result.total == len(sample)
    # Behavioural invariant: every outcome is classified.
    unexpected = [f for f in result.failed if f.error_type == "unexpected"]
    assert not unexpected, (
        "Resolver leaked unexpected exception classes:\n  "
        + "\n  ".join(f"{f.source}: {f.error}" for f in unexpected)
    )
    # Diagnostic only — not an assertion.  Visible in pytest -s output.
    print(
        f"\nReal-corpus stride sample: {len(result.resolved)}/{len(sample)} "
        f"resolved cleanly; {len(result.failed)} failed strict validation."
    )
