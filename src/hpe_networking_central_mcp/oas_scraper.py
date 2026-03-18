"""Scrape OpenAPI specs from ReadMe.io-hosted developer documentation.

Fetches HTML pages from developer.arubanetworks.com, extracts the embedded
``oasDefinition`` JSON, and caches specs to disk with TTL-based freshness.
Auto-discovers API categories from the sidebar metadata in each page.
"""

from __future__ import annotations

import json
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

    Each source maps to one ``/reference`` site. Categories are auto-discovered
    from the sidebar metadata embedded in the first page load.
    """

    name: str
    """Human-readable label (e.g. "MRT", "Config")."""

    reference_path: str
    """Path under the docs host, e.g. ``/new-central/reference``."""

    # Populated after auto-discovery
    category_slugs: list[str] = field(default_factory=list)


# Default sources — extend this list to add new API doc sites later.
DEFAULT_SOURCES: list[SpecSource] = [
    SpecSource(name="MRT", reference_path="/new-central/reference"),
    SpecSource(name="Config", reference_path="/new-central-config/reference"),
]


# ----- HTML extraction --------------------------------------------------------


def _extract_page_data(html: str) -> dict:
    """Extract the large JSON blob from a ReadMe.io reference page.

    ReadMe.io server-side renders a ``<script>`` tag whose content starts with
    ``{"sidebars"`` containing full sidebar metadata *and* the ``oasDefinition``.
    """
    marker = '{"sidebars"'
    start = html.find(marker)
    if start == -1:
        raise ValueError("Could not find page data JSON in HTML")

    # Find the closing </script> tag after the JSON blob
    end = html.find("</script>", start)
    if end == -1:
        raise ValueError("Could not find closing </script> for page data")

    return json.loads(html[start:end])


def _discover_categories(page_data: dict) -> list[dict]:
    """Extract reference sidebar categories from the page data.

    Returns a list of dicts with keys: title, slug, first_child_slug.
    Each category in the sidebar has child pages; we only need one slug per
    category to load that category's OAS definition.
    """
    refs = page_data.get("sidebars", {}).get("refs", [])
    categories = []
    for cat in refs:
        pages = cat.get("pages", [])
        if not pages:
            continue
        first_page = pages[0]
        categories.append({
            "title": cat.get("title", ""),
            "slug": cat.get("slug", ""),
            "first_child_slug": first_page.get("slug", ""),
        })
    return categories


def _extract_oas(page_data: dict) -> dict | None:
    """Extract the ``oasDefinition`` from parsed page data."""
    oas = page_data.get("oasDefinition")
    if oas and isinstance(oas, dict) and "paths" in oas:
        return oas
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
    """Fetch a single HTML page."""
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
    """Fetch one category's OAS spec, using cache if fresh.

    Returns (slug, spec_or_None, was_cached).
    """
    cp = _cache_path(cache_dir, source_name, slug)

    if _is_fresh(cp, ttl):
        spec = _read_cache(cp)
        if spec:
            return slug, spec, True

    url = f"{DOCS_HOST}{reference_path}/{slug}"
    try:
        html = _fetch_page(url)
        page_data = _extract_page_data(html)
        spec = _extract_oas(page_data)
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
    1. Fetch the root reference page to auto-discover categories from the sidebar.
    2. For each category, fetch the first child page to extract its ``oasDefinition``.
    3. Cache specs to disk; reuse fresh caches (``ttl`` seconds).

    Returns a list of parsed OpenAPI spec dicts.
    """
    if sources is None:
        sources = [SpecSource(s.name, s.reference_path) for s in DEFAULT_SOURCES]

    all_specs: list[dict] = []

    for source in sources:
        logger.info("oas_source_start", source=source.name, path=source.reference_path)

        # Step 1: auto-discover categories from the root page
        if not source.category_slugs:
            try:
                root_url = f"{DOCS_HOST}{source.reference_path}"
                root_html = _fetch_page(root_url)
                root_data = _extract_page_data(root_html)
                categories = _discover_categories(root_data)
                source.category_slugs = [c["first_child_slug"] for c in categories if c["first_child_slug"]]

                # The root page itself may have redirected to a category page — extract its spec too
                root_spec = _extract_oas(root_data)
                if root_spec:
                    # Cache under the slug that the root page resolved to
                    doc = root_data.get("doc", {})
                    root_slug = doc.get("slug", "") if isinstance(doc, dict) else ""
                    if root_slug:
                        _write_cache(_cache_path(cache_dir, source.name, root_slug), root_spec)

                logger.info(
                    "oas_categories_discovered",
                    source=source.name,
                    count=len(source.category_slugs),
                    categories=[c["title"] for c in categories],
                )
            except Exception as e:
                logger.error("oas_discovery_failed", source=source.name, error=str(e))
                continue

        # Step 2: fetch specs for each category in parallel
        seen_titles: set[str] = set()
        cached_count = 0
        fetched_count = 0

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
                for slug in source.category_slugs
            }

            for future in as_completed(futures):
                slug, spec, was_cached = future.result()
                if was_cached:
                    cached_count += 1
                else:
                    fetched_count += 1

                if spec:
                    title = spec.get("info", {}).get("title", slug)
                    if title not in seen_titles:
                        seen_titles.add(title)
                        all_specs.append(spec)

        logger.info(
            "oas_source_done",
            source=source.name,
            specs=len(seen_titles),
            cached=cached_count,
            fetched=fetched_count,
        )

    logger.info("oas_scrape_complete", total_specs=len(all_specs))
    return all_specs
