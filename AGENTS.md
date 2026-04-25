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
4. use a background terminal with output redirected to a file! No pipes in WSL!
5. reading from the terminal with cat/tail via a second sync shell results in failure


## Python

* Always use `uv run` (e.g. `uv run pytest`, `uv run python scripts/...`).
* The repo's pytest config lives in `pyproject.toml`; do not pass `--rootdir`.

  Don't add anything to AGENTS.md except when I explicitly tell you!
