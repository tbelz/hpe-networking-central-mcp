"""Script library management tools."""

from __future__ import annotations

import json
import re
import time
from pathlib import Path
from typing import Any

import structlog
from mcp.types import ToolAnnotations

from ..config import Settings
from .execution import _run_script

logger = structlog.get_logger("tools.scripts")


def _validate_filename(filename: str) -> str | None:
    """Validate script filename. Returns error message or None if valid."""
    if not filename:
        return "Filename cannot be empty."
    if not filename.endswith(".py"):
        return "Filename must end with .py"
    if "/" in filename or "\\" in filename or ".." in filename:
        return "Filename must not contain path separators or '..'."
    if not re.match(r"^[a-zA-Z0-9_\-]+\.py$", filename):
        return "Filename must contain only alphanumeric characters, underscores, and hyphens."
    return None


def _read_meta(script_path: Path) -> dict[str, Any]:
    """Read the .meta.json companion file for a script."""
    meta_path = script_path.with_suffix(".meta.json")
    if meta_path.exists():
        return json.loads(meta_path.read_text(encoding="utf-8"))
    return {}


def _write_meta(script_path: Path, meta: dict[str, Any]) -> None:
    """Write the .meta.json companion file for a script."""
    meta_path = script_path.with_suffix(".meta.json")
    meta_path.write_text(json.dumps(meta, indent=2), encoding="utf-8")


def register_script_tools(mcp, settings: Settings):
    """Register script library management tools with the MCP server."""

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True),
    )
    def list_scripts(tag: str | None = None) -> str:
        """List all scripts in the automation library.

        Scans the script library directory for Python scripts and their metadata.
        Use this before writing a new script to check if a reusable one already exists.

        Args:
            tag: Optional tag to filter by (e.g., "onboarding", "monitoring", "site-management").

        Returns:
            JSON list of scripts with name, description, tags, parameters, and last-run info.
        """
        lib = settings.script_library_path
        if not lib.exists():
            return json.dumps({"scripts": [], "message": "Script library is empty."})

        scripts = []
        for py_file in sorted(lib.glob("*.py")):
            meta = _read_meta(py_file)
            entry = {
                "filename": py_file.name,
                "description": meta.get("description", "No description"),
                "tags": meta.get("tags", []),
                "parameters": meta.get("parameters", []),
                "created_at": meta.get("created_at"),
                "last_run": meta.get("last_run"),
                "last_exit_code": meta.get("last_exit_code"),
            }
            if tag and tag.lower() not in [t.lower() for t in entry["tags"]]:
                continue
            scripts.append(entry)

        return json.dumps({"scripts": scripts, "total": len(scripts)}, indent=2)

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=True),
    )
    def save_script(
        filename: str,
        content: str,
        description: str,
        tags: list[str],
        parameters: list[dict[str, str]] | None = None,
        execute: bool = False,
    ) -> str:
        """Save a Python script to the automation library for reuse.

        BEFORE calling this tool you MUST have:
        1. Called list_scripts() to check for existing scripts
        2. Called search_api_catalog() to discover API endpoints
        3. Called get_api_endpoint_detail() for parameter schemas

        Scripts should use ``from central_helpers import api`` for all API calls.
        Use ``api.paginate()`` for collection endpoints, ``api.get()`` for single-item lookups.
        Read docs://script-writing-guide for the full template.

        Args:
            filename: Script filename (e.g., "onboard_device.py"). Must end in .py.
            content: The full Python script content.
            description: Human-readable description of what the script does.
            tags: List of tags for categorization (e.g., ["onboarding", "glp"]).
            parameters: List of parameter definitions, each with keys:
                        name, type, description, required (bool), default (optional).
            execute: If True, execute the script immediately after saving and return
                     combined save + execution results.

        Returns:
            Confirmation message with the saved file path, or error details.
        """
        error = _validate_filename(filename)
        if error:
            return json.dumps({"error": error})

        lib = settings.script_library_path
        lib.mkdir(parents=True, exist_ok=True)
        script_path = lib / filename

        # Write script
        script_path.write_text(content, encoding="utf-8")

        # Write metadata
        meta = {
            "description": description,
            "tags": tags,
            "parameters": parameters or [],
            "created_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
            "last_run": None,
            "last_exit_code": None,
        }
        # Preserve last_run from existing meta if overwriting
        existing_meta = _read_meta(script_path)
        if existing_meta.get("last_run"):
            meta["last_run"] = existing_meta["last_run"]
            meta["last_exit_code"] = existing_meta.get("last_exit_code")

        _write_meta(script_path, meta)

        logger.info("script_saved", filename=filename, tags=tags)

        save_result = {
            "status": "saved",
            "path": str(script_path),
            "filename": filename,
            "description": description,
        }

        if execute:
            exec_result_json = _run_script(settings, filename, None)
            exec_result = json.loads(exec_result_json)
            save_result["execution"] = exec_result

        return json.dumps(save_result, indent=2)
