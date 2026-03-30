"""Scrape OpenAPI specs from ReadMe.io-hosted developer documentation.

Uses the ReadMe ``.md`` URL protocol: appending ``.md`` to any reference
page URL returns the full OpenAPI 3.x spec embedded in a markdown code block.

Discovery flow:
    1. Fetch the root reference page HTML (SSR sidebar lists all endpoints).
    2. Extract endpoint slugs from sidebar ``<a href>`` links.
    3. For each slug, fetch ``{slug}.md`` to get the per-endpoint OAS spec.
    4. Cache specs to disk with TTL-based freshness.
"""

from __future__ import annotations

import json
import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass, field
from pathlib import Path

import httpx
import structlog

logger = structlog.get_logger("oas_scraper")

DOCS_HOST = "https://developer.arubanetworks.com"

# ----- Spec source definitions ------------------------------------------------


@dataclass
class SpecSource:
    """A ReadMe.io API documentation site to scrape.

    Each source maps to one ``/reference`` site.  Endpoint slugs are
    auto-discovered from the server-side rendered sidebar HTML.
    """

    name: str
    """Human-readable label (e.g. "MRT", "Config")."""

    reference_path: str
    """Path under the docs host, e.g. ``/new-central/reference``."""

    # Populated after auto-discovery
    endpoint_slugs: list[str] = field(default_factory=list)


# Default sources — extend this list to add new API doc sites later.
DEFAULT_SOURCES: list[SpecSource] = [
    SpecSource(name="MRT", reference_path="/new-central/reference"),
    SpecSource(name="Config", reference_path="/new-central-config/reference"),
]


# ----- Slug discovery ---------------------------------------------------------

_SLUG_RE = re.compile(r'href="[^"]*?/reference/([a-zA-Z0-9_-]+)"')


def _extract_endpoint_slugs(html: str) -> list[str]:
    """Extract unique endpoint slugs from sidebar links in SSR HTML.

    ReadMe SuperHub renders the full sidebar server-side so all endpoint
    links are present in the initial HTML response.
    """
    seen: set[str] = set()
    slugs: list[str] = []
    for match in _SLUG_RE.finditer(html):
        slug = match.group(1)
        if slug not in seen:
            seen.add(slug)
            slugs.append(slug)
    return slugs


# ----- Markdown spec extraction -----------------------------------------------

_JSON_BLOCK_RE = re.compile(r"```json\s*\n(.*?)\n```", re.DOTALL)


def _extract_oas_from_markdown(text: str) -> dict | None:
    """Extract an OpenAPI spec from a ReadMe ``.md`` response.

    The ``.md`` endpoint returns markdown containing a JSON code block
    with the full OpenAPI 3.x specification for that endpoint.
    """
    m = _JSON_BLOCK_RE.search(text)
    if not m:
        return None
    try:
        spec = json.loads(m.group(1))
    except json.JSONDecodeError:
        return None
    if isinstance(spec, dict) and "paths" in spec:
        return spec
    return None


# ----- Caching ----------------------------------------------------------------


def _cache_path(cache_dir: Path, source_name: str, slug: str) -> Path:
    return cache_dir / source_name / f"{slug}.json"


def _is_fresh(path: Path, ttl: int) -> bool:
    if not path.exists():
        return False
    age = time.time() - path.stat().st_mtime
    return age < ttl


def _read_cache(path: Path) -> dict | None:
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def _write_cache(path: Path, spec: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(spec), encoding="utf-8")


# ----- Fetching ---------------------------------------------------------------

_HTTP_TIMEOUT = 30.0
_MAX_WORKERS = 5


def _fetch_page(url: str) -> str:
    """Fetch a page and return the response text."""
    resp = httpx.get(url, timeout=_HTTP_TIMEOUT, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _fetch_spec_for_slug(
    source_name: str,
    reference_path: str,
    slug: str,
    cache_dir: Path,
    ttl: int,
) -> tuple[str, dict | None, bool]:
    """Fetch one endpoint's OAS spec via the ``.md`` protocol.

    Returns (slug, spec_or_None, was_cached).
    """
    cp = _cache_path(cache_dir, source_name, slug)

    if _is_fresh(cp, ttl):
        spec = _read_cache(cp)
        if spec:
            return slug, spec, True

    url = f"{DOCS_HOST}{reference_path}/{slug}.md"
    try:
        text = _fetch_page(url)
        spec = _extract_oas_from_markdown(text)
        if spec:
            _write_cache(cp, spec)
            return slug, spec, False
        logger.warning("oas_no_definition", source=source_name, slug=slug)
        return slug, None, False
    except Exception as e:
        logger.warning("oas_fetch_failed", source=source_name, slug=slug, error=str(e))
        # Fall back to stale cache if available
        spec = _read_cache(cp)
        if spec:
            logger.info("oas_using_stale_cache", source=source_name, slug=slug)
            return slug, spec, True
        return slug, None, False


# ----- Public API -------------------------------------------------------------


def discover_and_scrape(
    sources: list[SpecSource] | None = None,
    cache_dir: Path = Path("/data/oas_cache"),
    ttl: int = 86400,
) -> list[dict]:
    """Scrape OpenAPI specs from all configured sources.

    For each source:
    1. Fetch the root reference page to discover endpoint slugs from the sidebar.
    2. For each slug, fetch ``{slug}.md`` to get the per-endpoint OAS spec.
    3. Cache specs to disk; reuse fresh caches (``ttl`` seconds).

    Returns a list of parsed OpenAPI spec dicts.
    """
    if sources is None:
        sources = [SpecSource(s.name, s.reference_path) for s in DEFAULT_SOURCES]

    all_specs: list[dict] = []

    for source in sources:
        logger.info("oas_source_start", source=source.name, path=source.reference_path)

        # Step 1: discover endpoint slugs from the sidebar HTML
        if not source.endpoint_slugs:
            try:
                root_url = f"{DOCS_HOST}{source.reference_path}"
                root_html = _fetch_page(root_url)
                source.endpoint_slugs = _extract_endpoint_slugs(root_html)

                logger.info(
                    "oas_slugs_discovered",
                    source=source.name,
                    count=len(source.endpoint_slugs),
                )
            except Exception as e:
                logger.error("oas_discovery_failed", source=source.name, error=str(e))
                continue

        # Step 2: fetch specs for each endpoint in parallel
        cached_count = 0
        fetched_count = 0
        spec_count = 0

        with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
            futures = {
                pool.submit(
                    _fetch_spec_for_slug,
                    source.name,
                    source.reference_path,
                    slug,
                    cache_dir,
                    ttl,
                ): slug
                for slug in source.endpoint_slugs
            }

            for future in as_completed(futures):
                slug, spec, was_cached = future.result()
                if was_cached:
                    cached_count += 1
                else:
                    fetched_count += 1

                if spec:
                    all_specs.append(spec)
                    spec_count += 1

        logger.info(
            "oas_source_done",
            source=source.name,
            specs=spec_count,
            cached=cached_count,
            fetched=fetched_count,
        )

    logger.info("oas_scrape_complete", total_specs=len(all_specs))
    return all_specs


# ----- SpecProvider wrapper ---------------------------------------------------


class ReadMeSpecProvider:
    """SpecProvider that scrapes OpenAPI specs from ReadMe.io-hosted docs.

    Wraps :func:`discover_and_scrape` to conform to the
    :class:`~hpe_networking_central_mcp.spec_provider.SpecProvider` protocol.
    """

    def __init__(self, sources: list[SpecSource] | None = None) -> None:
        self._sources = sources

    @property
    def name(self) -> str:
        return "Central"

    def fetch_specs(self, cache_dir: Path, ttl: int) -> list[dict]:
        return discover_and_scrape(sources=self._sources, cache_dir=cache_dir, ttl=ttl)
