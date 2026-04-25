# Agent Notes

Operational rules for AI coding agents working in this repo. Read before acting.

## Running long commands (CRITICAL)

**Never re-issue a long-running command (like `pytest`) while a previous instance
of the same command is still running.** Doing so spawns a competing process that
can deadlock the foreground shell, eat the previous process's output, or cause
both runs to fail.

Required workflow for anything that may take >10s (test suites, builds,
`uv sync`, `pip install`, `build_knowledge_db.py`, etc.):

1. Start the command with `mode="async"` (background terminal). Capture the
   returned terminal ID.
2. Poll with `get_terminal_output` using that ID. **Do not** issue another
   command into the same terminal, and **do not** start the same command in a
   new terminal, until the first one prints a shell prompt again or you have
   explicitly killed it with `kill_terminal`.
3. If polling shows no progress for a long time, prefer `kill_terminal`
   followed by a fresh `mode="async"` start over piling on more invocations.
4. Redirect output to a file when you need the result deterministically:
   `... > /tmp/run.log 2>&1` and then `tail` the log.

Symptom that you violated this rule: the terminal shows your second command
typed at the prompt but never produces output, or earlier output disappears.
Stop, kill the terminal, and start over cleanly.

## Python

* Always use `uv run` (e.g. `uv run pytest`, `uv run python scripts/...`).
* The repo's pytest config lives in `pyproject.toml`; do not pass `--rootdir`.
* `tests/test_graph.py` may `sys.exit(1)` at import time when its prerequisites
  aren't met — exclude it with `--ignore=tests/test_graph.py` for quick runs.
* `tests/test_e2e.py` hits the live network; skip for fast iterations.

## Knowledge sync (formerly "scrape") pipeline

* Provider modules live in `src/hpe_networking_central_mcp/`:
  `oas_scraper.py` (ReadMe.io / Central + GreenLake) and `vsg_scraper.py` (VSG).
  File names are intentionally not renamed — backward-compat matters more than
  cosmetic consistency.
* Public terminology in user-facing strings, manifest keys, workflow step names,
  release bodies, and issue labels is **"sync"** (not "scrape"). Keep it that
  way.
* Both providers expose `last_reports` / `last_report` after a run; the build
  script consumes those to populate `manifest.json["sync_health"]`.
* The VSG host (`arubanetworking.hpe.com`) is fronted by an Akamai WAF that
  blocks GitHub Actions runner IPs with HTTP 403. This is **expected**; the
  provider degrades gracefully and emits one aggregated `vsg_access_denied`
  warning instead of per-page failures. Do not treat 403 as a hard error.
  The workflow primes the on-disk VSG cache from the previous release's
  `vsg-cache.tar.gz` asset before each run, so even fully-blocked runs still
  serve last-good content via the provider's stale-cache fallback.
* The ReadMe.io host throttles aggressively. The OAS provider sends a real
  browser User-Agent, paces requests per-host (~4 req/s), and retries 429/5xx
  with exponential backoff + `Retry-After`. Don't raise the parallelism above
  3 workers.

## API endpoint discovery

* The MCP exposes one **structural** detail tool, `get_api_endpoint_detail`,
  and one **prose** tool, `get_api_endpoint_glossary`. There is no `view`
  parameter. The skeleton has every description-bearing key stripped; the
  glossary is the prose-only counterpart and should only be called when a
  field name in the skeleton is ambiguous. See ADR 007.
* The knowledge DB schema version is **3**. The server refuses to start
  against an older snapshot (hard `SystemExit`); rebuild via
  `python scripts/build_knowledge_db.py --output-dir build` or wait for the
  daily workflow to publish a fresh release.

## GitHub Actions workflow

* `.github/workflows/update-knowledge-db.yml` opens a `knowledge-sync-alert`
  issue when the build is unhealthy. The label is created idempotently in an
  earlier step — never remove that step or the alert step will fail with
  `could not add label: 'knowledge-sync-alert' not found`.
* Health policy: provider `status: "error"` is fatal; `status: "degraded"`
  (e.g. WAF-blocked VSG) is allowed and still publishes a release.
