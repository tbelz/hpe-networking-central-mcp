"""Scrape documentation pages from the HPE Aruba Networking VSG site.

Fetches the Central section of the Validated Solution Guide and splits
each page into H2-level sections stored as ``DocSection`` graph nodes.

The site is a Jekyll 4.x static site with clean HTML — content lives
inside ``div#main-content``.  No JavaScript rendering required.
"""

from __future__ import annotations

import re
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from html.parser import HTMLParser
from pathlib import Path

import httpx
import structlog

logger = structlog.get_logger("vsg_scraper")

VSG_HOST = "https://arubanetworking.hpe.com"

# ── Pages to scrape ──────────────────────────────────────────────────

# (slug used for section_id generation, full URL path)
VSG_CENTRAL_PAGES: list[tuple[str, str]] = [
    ("overview", "/techdocs/VSG/docs/002-central/central-000/"),
    ("readiness", "/techdocs/VSG/docs/002-central/central-010-readiness/"),
    ("config-model", "/techdocs/VSG/docs/002-central/central-020-config-model/"),
    ("config-example", "/techdocs/VSG/docs/002-central/central-030-central-config-example/"),
    ("config-api", "/techdocs/VSG/docs/002-central/central-020-configuration-api/"),
    ("policy-config", "/techdocs/VSG/docs/002-central/central-040-policy-configuration/"),
    ("on-premises", "/techdocs/VSG/docs/002-central/central-090-central-on-premises/"),
]


# ── Data model ───────────────────────────────────────────────────────


@dataclass
class DocEntry:
    """A single documentation section ready for graph insertion."""

    section_id: str
    title: str
    content: str
    source: str
    url: str


# ── HTML parsing ─────────────────────────────────────────────────────


class _MainContentExtractor(HTMLParser):
    """Extract the inner HTML of ``div#main-content``."""

    def __init__(self) -> None:
        super().__init__()
        self._inside = False
        self._depth = 0
        self._parts: list[str] = []

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        attr_dict = dict(attrs)
        if tag == "div" and attr_dict.get("id") == "main-content":
            self._inside = True
            self._depth = 1
            return
        if self._inside:
            if tag in _VOID_ELEMENTS:
                pass
            else:
                self._depth += 1
            self._parts.append(self._rebuild_tag(tag, attrs))

    def handle_endtag(self, tag: str) -> None:
        if self._inside:
            if tag not in _VOID_ELEMENTS:
                self._depth -= 1
            if self._depth <= 0:
                self._inside = False
                return
            self._parts.append(f"</{tag}>")

    def handle_data(self, data: str) -> None:
        if self._inside:
            self._parts.append(data)

    @staticmethod
    def _rebuild_tag(tag: str, attrs: list[tuple[str, str | None]]) -> str:
        parts = [tag]
        for k, v in attrs:
            if v is None:
                parts.append(k)
            else:
                parts.append(f'{k}="{v}"')
        return "<" + " ".join(parts) + ">"

    def get_html(self) -> str:
        return "".join(self._parts)


_VOID_ELEMENTS = frozenset({
    "area", "base", "br", "col", "embed", "hr", "img",
    "input", "link", "meta", "param", "source", "track", "wbr",
})


class _TextExtractor(HTMLParser):
    """Strip HTML tags and return clean text content."""

    _SKIP_TAGS = frozenset({"script", "style", "details", "noscript"})

    def __init__(self) -> None:
        super().__init__()
        self._parts: list[str] = []
        self._skip_depth = 0

    def handle_starttag(self, tag: str, attrs: list[tuple[str, str | None]]) -> None:
        if tag in self._SKIP_TAGS:
            self._skip_depth += 1
        elif self._skip_depth == 0:
            # Add line break for block-level elements
            if tag in ("p", "br", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6", "div"):
                self._parts.append("\n")

    def handle_endtag(self, tag: str) -> None:
        if tag in self._SKIP_TAGS and self._skip_depth > 0:
            self._skip_depth -= 1
        elif self._skip_depth == 0 and tag in ("p", "li", "tr", "h1", "h2", "h3", "h4", "h5", "h6"):
            self._parts.append("\n")

    def handle_data(self, data: str) -> None:
        if self._skip_depth == 0:
            self._parts.append(data)

    def get_text(self) -> str:
        text = "".join(self._parts)
        # Collapse multiple blank lines
        text = re.sub(r"\n{3,}", "\n\n", text)
        return text.strip()


def _html_to_text(html: str) -> str:
    """Convert an HTML fragment to clean text."""
    parser = _TextExtractor()
    parser.feed(html)
    return parser.get_text()


def _extract_main_content(full_html: str) -> str:
    """Extract the inner HTML of div#main-content."""
    parser = _MainContentExtractor()
    parser.feed(full_html)
    return parser.get_html()


# ── Section splitting ────────────────────────────────────────────────

# Matches <h1 ...>...</h1> or <h2 ...>...</h2> headings
_HEADING_RE = re.compile(
    r"<(h[12])\b([^>]*)>(.*?)</\1>",
    re.IGNORECASE | re.DOTALL,
)
_ID_RE = re.compile(r'id=["\']([^"\']*)["\']', re.IGNORECASE)


def _extract_heading_text(inner_html: str) -> str:
    """Get plain text from heading inner HTML (may contain <a> tags)."""
    return re.sub(r"<[^>]+>", "", inner_html).strip()


def extract_sections(html: str, page_url: str, page_slug: str) -> list[DocEntry]:
    """Split a page's main content into sections by H1/H2 headings.

    Returns a list of :class:`DocEntry` objects — one per section.
    """
    main_html = _extract_main_content(html)
    if not main_html.strip():
        return []

    # Find all H1/H2 heading positions
    headings = list(_HEADING_RE.finditer(main_html))

    if not headings:
        # No headings — treat the entire content as one section
        text = _html_to_text(main_html)
        if not text.strip():
            return []
        return [DocEntry(
            section_id=f"vsg-central-{page_slug}",
            title=page_slug.replace("-", " ").title(),
            content=text,
            source="vsg-central",
            url=page_url,
        )]

    entries: list[DocEntry] = []

    # Content before the first heading (page intro)
    first_pos = headings[0].start()
    if first_pos > 0:
        intro_text = _html_to_text(main_html[:first_pos])
        if intro_text.strip():
            entries.append(DocEntry(
                section_id=f"vsg-central-{page_slug}-intro",
                title=_extract_heading_text(headings[0].group(3)) + " — Introduction",
                content=intro_text,
                source="vsg-central",
                url=page_url,
            ))

    # Each heading starts a section that runs to the next heading
    for i, match in enumerate(headings):
        heading_tag = match.group(1).lower()
        attrs_str = match.group(2) or ""
        id_match = _ID_RE.search(attrs_str)
        heading_id = id_match.group(1) if id_match else ""
        heading_text = _extract_heading_text(match.group(3))

        if not heading_text:
            continue

        # Section content runs from after this heading to the next heading
        section_start = match.end()
        section_end = headings[i + 1].start() if i + 1 < len(headings) else len(main_html)
        section_html = main_html[section_start:section_end]
        section_text = _html_to_text(section_html)

        if not section_text.strip():
            continue

        # Build section_id from heading id or slug
        if heading_id:
            sid = f"vsg-central-{page_slug}-{heading_id}"
        else:
            slug_heading = re.sub(r"[^a-z0-9]+", "-", heading_text.lower()).strip("-")
            sid = f"vsg-central-{page_slug}-{slug_heading}"

        fragment = f"#{heading_id}" if heading_id else ""
        entries.append(DocEntry(
            section_id=sid,
            title=heading_text,
            content=section_text,
            source="vsg-central",
            url=f"{page_url}{fragment}",
        ))

    return entries


# ── Caching ──────────────────────────────────────────────────────────

_HTTP_TIMEOUT = 30.0
_MAX_WORKERS = 3


def _cache_path(cache_dir: Path, slug: str) -> Path:
    return cache_dir / f"{slug}.html"


def _is_fresh(path: Path, ttl: int) -> bool:
    if not path.exists():
        return False
    return (time.time() - path.stat().st_mtime) < ttl


def _read_cached_html(path: Path) -> str | None:
    try:
        return path.read_text(encoding="utf-8")
    except Exception:
        return None


def _write_cached_html(path: Path, html: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(html, encoding="utf-8")


# ── Fetching ─────────────────────────────────────────────────────────


def _fetch_page(url: str) -> str:
    """Fetch a page and return its HTML."""
    resp = httpx.get(url, timeout=_HTTP_TIMEOUT, follow_redirects=True)
    resp.raise_for_status()
    return resp.text


def _fetch_page_cached(
    slug: str, url_path: str, cache_dir: Path, ttl: int
) -> tuple[str, str | None, bool]:
    """Fetch one VSG page with cache.  Returns (slug, html_or_None, was_cached)."""
    cp = _cache_path(cache_dir, slug)

    if _is_fresh(cp, ttl):
        html = _read_cached_html(cp)
        if html:
            return slug, html, True

    full_url = f"{VSG_HOST}{url_path}"
    try:
        html = _fetch_page(full_url)
        _write_cached_html(cp, html)
        return slug, html, False
    except Exception as e:
        logger.warning("vsg_fetch_failed", slug=slug, error=str(e))
        # Fall back to stale cache
        html = _read_cached_html(cp)
        if html:
            logger.info("vsg_using_stale_cache", slug=slug)
            return slug, html, True
        return slug, None, False


# ── Public API ───────────────────────────────────────────────────────


def scrape_vsg_docs(
    pages: list[tuple[str, str]] | None = None,
    cache_dir: Path = Path("/data/vsg_cache"),
    ttl: int = 86400,
) -> list[DocEntry]:
    """Scrape VSG Central documentation pages and split into sections.

    Args:
        pages: List of ``(slug, url_path)`` tuples. Defaults to
            :data:`VSG_CENTRAL_PAGES`.
        cache_dir: Cache directory for raw HTML pages.
        ttl: Cache freshness in seconds.

    Returns:
        List of :class:`DocEntry` objects ready for graph insertion.
    """
    if pages is None:
        pages = list(VSG_CENTRAL_PAGES)

    all_entries: list[DocEntry] = []
    cached_count = 0
    fetched_count = 0

    with ThreadPoolExecutor(max_workers=_MAX_WORKERS) as pool:
        futures = {
            pool.submit(_fetch_page_cached, slug, url_path, cache_dir, ttl): (slug, url_path)
            for slug, url_path in pages
        }

        for future in as_completed(futures):
            slug, url_path = futures[future]
            slug_result, html, was_cached = future.result()

            if was_cached:
                cached_count += 1
            else:
                fetched_count += 1

            if html:
                page_url = f"{VSG_HOST}{url_path}"
                sections = extract_sections(html, page_url, slug)
                all_entries.extend(sections)
                logger.info("vsg_page_done", slug=slug, sections=len(sections))
            else:
                logger.warning("vsg_page_skipped", slug=slug)

    logger.info(
        "vsg_scrape_complete",
        total_sections=len(all_entries),
        pages_cached=cached_count,
        pages_fetched=fetched_count,
    )
    return all_entries


class VsgDocProvider:
    """Provider that scrapes VSG Central documentation pages."""

    @property
    def name(self) -> str:
        return "VSG Central"

    def fetch_docs(
        self, cache_dir: Path, ttl: int = 86400
    ) -> list[DocEntry]:
        return scrape_vsg_docs(cache_dir=cache_dir, ttl=ttl)
