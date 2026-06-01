#!/usr/bin/env bash
# Fast local test loop for tight feedback cycles.
#
# Runs the unit-test subset only — excludes:
#   - slow tests
#   - integration / live_api tests (need credentials)
#   - real_spec tests (need the real Central spec cache hydrated)
#
# Target runtime: < 30 seconds. Use this before every push.
#
# Failures abort early (-x) and the failed-first ordering (--ff) puts
# previously-failing tests first on the next run.

set -euo pipefail

cd "$(dirname "$0")/.."

uv run pytest \
    -m "unit and not slow and not real_spec and not integration and not live_api" \
    -x --ff --timeout=30 -q "$@"
