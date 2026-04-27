"""Knowledge DB downloader.

Extracted from server.py so production startup and tests share one
implementation. Downloads the latest ``knowledge_db.tar.gz`` asset from
a GitHub release and extracts it to the configured database path.
"""

from __future__ import annotations

import json
import logging
import shutil
import tarfile
import tempfile
from pathlib import Path
from typing import Callable

import httpx

_DEFAULT_LOGGER = logging.getLogger("hpe_networking_central_mcp.knowledge_db")

# How long to wait for the GitHub release-info API call. Kept short so a
# spotty network does not eat the MCP client's ``initialize`` budget on
# cold start — the typical Claude Code timeout is ~10s and the rest of
# startup (graph init + seed sync) also runs synchronously.
_RELEASE_INFO_TIMEOUT = 8.0
_DOWNLOAD_TIMEOUT = 120.0


def _stdlib_log(level: int):
    """Return a callable that accepts structlog-style **kwargs and forwards
    them to ``logging`` as an ``extra`` mapping (so stdlib won't reject them).
    """

    def _log(event: str, **kwargs) -> None:
        if kwargs:
            _DEFAULT_LOGGER.log(level, "%s %s", event, kwargs)
        else:
            _DEFAULT_LOGGER.log(level, event)

    return _log


def _read_local_release_tag(manifest_path: Path) -> str | None:
    """Return the GitHub release tag recorded in the on-disk manifest, if any.

    The manifest written by :func:`_write_local_manifest` carries an explicit
    ``release_tag`` field set from the GitHub release ``tag_name``. Older
    manifests (built before the resume-on-restart fix) only carry
    ``version``, which the build script generates from the same timestamp
    template as the release tag, so we fall back to it for backward compat.
    """
    if not manifest_path.exists():
        return None
    try:
        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    except Exception:
        return None
    tag = manifest.get("release_tag") or manifest.get("version")
    return tag if isinstance(tag, str) and tag else None


def _write_local_manifest(
    extracted_manifest: Path, dest_manifest: Path, *, release_tag: str | None
) -> None:
    """Copy the extracted manifest to ``dest_manifest`` and stamp the
    GitHub release ``tag_name`` so subsequent startups can short-circuit
    the download when the tag is already present locally.
    """
    try:
        manifest = json.loads(extracted_manifest.read_text(encoding="utf-8"))
    except Exception:
        # Manifest is malformed — copy verbatim so we don't lose the
        # original, but we won't be able to short-circuit next time.
        shutil.copy2(extracted_manifest, dest_manifest)
        return
    if release_tag:
        manifest["release_tag"] = release_tag
    dest_manifest.write_text(json.dumps(manifest, indent=2), encoding="utf-8")


def download_knowledge_db(
    repo: str,
    db_path: Path,
    *,
    logger: Callable | None = None,
) -> bool:
    """Download the latest knowledge DB tar.gz from a GitHub release.

    Parameters
    ----------
    repo : str
        ``owner/name`` of the GitHub repository hosting the release.
    db_path : Path
        Destination path for the extracted database (file or directory).
    logger : structured logger, optional
        Object with ``info``/``warning`` methods accepting ``**kwargs``.
        Falls back to the standard ``logging`` module.

    Returns
    -------
    bool
        ``True`` when a DB was downloaded and extracted, ``False`` otherwise.

    Security
    --------
    Members whose names start with ``/`` or contain ``..`` are rejected to
    prevent path-traversal extraction (CWE-22).
    """
    log = logger or _DEFAULT_LOGGER
    _info = getattr(log, "info", None) or _stdlib_log(logging.INFO)
    _warn = getattr(log, "warning", None) or _stdlib_log(logging.WARNING)
    # Stdlib loggers (logging.Logger) expose info/warning that do not accept
    # arbitrary kwargs, so wrap them to honour the documented kwargs contract.
    # The module-level default is also a stdlib logger.
    if isinstance(log, logging.Logger):
        _info = _stdlib_log(logging.INFO)
        _warn = _stdlib_log(logging.WARNING)

    if not repo:
        _info("knowledge_db_skip", reason="KNOWLEDGE_RELEASE_REPO not set")
        return False

    manifest_path = db_path.parent / "manifest.json"
    local_tag = _read_local_release_tag(manifest_path)

    api_url = f"https://api.github.com/repos/{repo}/releases/latest"
    try:
        resp = httpx.get(api_url, timeout=_RELEASE_INFO_TIMEOUT, follow_redirects=True)
        resp.raise_for_status()
        release = resp.json()
    except Exception as exc:
        # Network unreachable / rate-limited / GitHub down. If we already
        # have a local DB on disk, keep using it instead of failing the
        # whole startup — far better than spinning up with an empty graph.
        if db_path.exists() and local_tag:
            _info(
                "knowledge_db_offline_using_local",
                tag=local_tag,
                error=str(exc),
            )
            return False
        _warn("knowledge_db_fetch_failed", error=str(exc))
        return False

    remote_tag = release.get("tag_name")
    if (
        remote_tag
        and local_tag == remote_tag
        and db_path.exists()
    ):
        _info("knowledge_db_up_to_date", tag=remote_tag)
        return False

    asset_url = None
    for asset in release.get("assets", []):
        if asset.get("name") == "knowledge_db.tar.gz":
            asset_url = asset.get("browser_download_url")
            break

    if not asset_url:
        _warn("knowledge_db_no_asset", release=remote_tag)
        return False

    _info("knowledge_db_downloading", url=asset_url)
    try:
        with tempfile.TemporaryDirectory() as tmp:
            tar_path = Path(tmp) / "knowledge_db.tar.gz"
            with httpx.stream("GET", asset_url, timeout=_DOWNLOAD_TIMEOUT, follow_redirects=True) as r:
                r.raise_for_status()
                with open(tar_path, "wb") as f:
                    for chunk in r.iter_bytes(chunk_size=65536):
                        f.write(chunk)

            with tarfile.open(tar_path, "r:gz") as tf:
                for member in tf.getmembers():
                    if member.name.startswith("/") or ".." in member.name:
                        raise ValueError(f"Unsafe tar member: {member.name}")
                tf.extractall(tmp, filter="data")

            extracted_db = Path(tmp) / "knowledge_db"
            if not extracted_db.exists():
                _warn(
                    "knowledge_db_extract_failed",
                    reason="knowledge_db not found in archive",
                )
                return False

            if db_path.exists():
                if db_path.is_dir():
                    shutil.rmtree(db_path)
                else:
                    db_path.unlink()
            if extracted_db.is_dir():
                shutil.copytree(extracted_db, db_path)
            else:
                shutil.copy2(extracted_db, db_path)

            extracted_manifest = Path(tmp) / "manifest.json"
            if extracted_manifest.exists():
                _write_local_manifest(
                    extracted_manifest, manifest_path, release_tag=remote_tag
                )

            _info("knowledge_db_installed", tag=remote_tag)
            return True
    except Exception as exc:
        _warn("knowledge_db_download_failed", error=str(exc))
        return False
