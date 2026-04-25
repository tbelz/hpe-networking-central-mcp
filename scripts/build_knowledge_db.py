#!/usr/bin/env python3
"""Build a LadybugDB knowledge database from public API specs and seed scripts.

Run on a GitHub Actions runner (no Central/GLP credentials required).
Fetches public developer documentation, parses OpenAPI specs, and populates
a LadybugDB graph database with ApiEndpoint, ApiCategory, DocSection, and Script
nodes.  The resulting DB directory is then tar'd for publishing as a GH release.

Usage:
    python scripts/build_knowledge_db.py [--output-dir ./build] [--tar]
"""

from __future__ import annotations

import argparse
import json
import shutil
import sys
import tarfile
from pathlib import Path

# Ensure the package is importable when running from repo root
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

import real_ladybug as lb  # noqa: E402

from hpe_networking_central_mcp.graph.schema import (  # noqa: E402
    KNOWLEDGE_NODE_TABLES,
    KNOWLEDGE_REL_TABLES,
    NODE_TABLES,
    POLICY_REL_TABLES,
    REL_TABLES,
    TOPOLOGY_REL_TABLES,
)
from hpe_networking_central_mcp.oas_index import OASIndex  # noqa: E402
from hpe_networking_central_mcp.oas_normalize import (  # noqa: E402
    normalize as normalize_spec,
    project_compact,
    project_request_only,
)
from hpe_networking_central_mcp.oas_scraper import ReadMeSpecProvider  # noqa: E402
from hpe_networking_central_mcp.vsg_scraper import VsgDocProvider  # noqa: E402

# GLP provider may fail if dependencies vary; import conditionally
try:
    from hpe_networking_central_mcp.glp_spec_provider import GreenLakeSpecProvider

    _HAS_GLP = True
except ImportError:
    _HAS_GLP = False


def _apply_schema(db: lb.Database) -> None:
    """Apply full schema DDL (live + knowledge tables)."""
    conn = lb.Connection(db)
    all_ddl = NODE_TABLES + KNOWLEDGE_NODE_TABLES + REL_TABLES + KNOWLEDGE_REL_TABLES + TOPOLOGY_REL_TABLES + POLICY_REL_TABLES
    for ddl in all_ddl:
        conn.execute(ddl.strip())
    print(f"  Schema applied: {len(all_ddl)} DDL statements")


def _sync_specs(cache_dir: Path) -> tuple[list[dict], dict]:
    """Sync API specs from all public documentation sources.

    Returns ``(all_specs, sync_health)`` where ``sync_health`` is a dict
    with per-provider stats for the manifest.  Each provider entry includes
    a coverage ratio so downstream health checks can detect partial outages
    even when a few specs come back successfully.
    """
    specs: list[dict] = []
    sync_health: dict = {}

    # Central (ReadMe.io)
    print("  Refreshing Central API references (ReadMe.io)...")
    try:
        provider = ReadMeSpecProvider()
        central_specs = provider.fetch_specs(cache_dir=cache_dir / "central", ttl=0)
        specs.extend(central_specs)
        sync_health["central"] = _summarise_oas_reports(
            provider.last_reports, central_specs
        )
        print(f"    → {len(central_specs)} Central specs")
        for r in provider.last_reports:
            print(
                f"      {r.name}: {r.total_specs}/{r.discovered} "
                f"(missing={r.missing_specs}, failures={dict(r.failure_reasons)})"
            )
    except Exception as e:
        sync_health["central"] = {
            "status": "error",
            "spec_count": 0,
            "coverage": 0.0,
            "error": str(e),
        }
        print(f"    ⚠ Central refresh failed: {e}", file=sys.stderr)

    # GreenLake (developer portal)
    if _HAS_GLP:
        print("  Refreshing GreenLake API references (developer portal)...")
        try:
            glp_provider = GreenLakeSpecProvider()
            glp_specs = glp_provider.fetch_specs(cache_dir=cache_dir / "glp", ttl=0)
            specs.extend(glp_specs)
            sync_health["greenlake"] = {
                "status": "ok",
                "spec_count": len(glp_specs),
                "coverage": 1.0,
            }
            print(f"    → {len(glp_specs)} GreenLake specs")
        except Exception as e:
            sync_health["greenlake"] = {
                "status": "error",
                "spec_count": 0,
                "coverage": 0.0,
                "error": str(e),
            }
            print(f"    ⚠ GreenLake refresh failed: {e}", file=sys.stderr)

    return specs, sync_health


def _summarise_oas_reports(reports, specs: list[dict]) -> dict:
    """Build a manifest entry from per-source OAS reports.

    The status reflects coverage:
        * ``ok``        — 100% of discovered slugs have a spec
        * ``degraded``  — at least one spec returned but coverage < 95%
        * ``error``     — zero specs or every source had a discovery error
    """
    discovered = sum(r.discovered for r in reports)
    fetched = sum(r.total_specs for r in reports)
    missing = sum(r.missing_specs for r in reports)
    discovery_errors = [r.name for r in reports if r.discovery_error]
    coverage = (fetched / discovered) if discovered else 0.0

    if fetched == 0:
        status = "error"
    elif coverage < 0.95 or discovery_errors:
        status = "degraded"
    else:
        status = "ok"

    failure_reasons: dict[str, int] = {}
    for r in reports:
        for reason, count in r.failure_reasons.items():
            failure_reasons[reason] = failure_reasons.get(reason, 0) + count

    return {
        "status": status,
        "spec_count": fetched,
        "discovered": discovered,
        "missing": missing,
        "coverage": round(coverage, 4),
        "sources": [r.as_dict() for r in reports],
        "discovery_errors": discovery_errors,
        "failure_reasons": failure_reasons,
    }


def _cypher_string_list(values: list[str]) -> str:
    """Build a Cypher list literal from Python strings, escaping quotes."""
    if not values:
        return "CAST([] AS STRING[])"
    escaped = [v.replace("\\", "\\\\").replace("'", "\\'") for v in values]
    return "[" + ", ".join(f"'{e}'" for e in escaped) + "]"


def _cypher_escape(value: str) -> str:
    """Escape a string for safe Cypher string literal embedding."""
    return value.replace("\\", "\\\\").replace("'", "\\'")


def _populate_endpoints(db: lb.Database, index: OASIndex, specs: list[dict]) -> int:
    """Insert ApiEndpoint and ApiCategory nodes from the OASIndex.

    ``specs`` should be the **normalized** spec list — projection columns
    (``bodyCompactJson``, ``bodyRequestOnlyJson``) are computed from these.
    """
    conn = lb.Connection(db)
    count = 0
    compact_ok = 0
    request_only_ok = 0

    # ReadMe.io serves one operation per .md file, so most providers ship
    # ~1600 individual specs that share only ~50 ``info.title`` values.
    # Keying by category would discard all but the last spec per title and
    # silently produce empty projection blobs.  Index by (method, path) so
    # every endpoint can find its source spec.
    specs_by_endpoint: dict[tuple[str, str], dict] = {}
    specs_by_category: dict[str, dict] = {}
    for spec in specs:
        title = (spec.get("info") or {}).get("title", "Unknown")
        # Keep the last spec per title for the (rare) all-in-one provider case.
        specs_by_category[title] = spec
        for path, path_item in (spec.get("paths") or {}).items():
            if not isinstance(path_item, dict):
                continue
            for method in ("get", "post", "put", "patch", "delete", "head", "options"):
                if isinstance(path_item.get(method), dict):
                    specs_by_endpoint[(method.upper(), path)] = spec

    for entry in index._entries:  # noqa: SLF001 — accessing internal for bulk insert
        endpoint_id = f"{entry.method}:{entry.path}"

        # Serialize full parameter objects as JSON
        params_json = json.dumps([
            {
                "name": p.name,
                "in": p.location,
                "required": p.required,
                "schema": p.schema,
                "description": p.description,
            }
            for p in entry.parameters
        ]) if entry.parameters else ""

        # Serialize request body schema as JSON
        body_json = json.dumps(entry.request_body) if entry.request_body else ""

        # Serialize response objects as JSON
        responses_json = json.dumps([
            {
                "status": r.status,
                "description": r.description,
                "schema": r.schema,
            }
            for r in entry.responses
        ]) if entry.responses else ""

        # Inline tags as a Cypher list literal (real_ladybug cannot bind
        # Python lists as STRING[] parameters — triggers ANY-type error).
        tags_literal = _cypher_string_list(entry.tags)
        tags_clause = f"tags: {tags_literal}, " if entry.tags else ""

        # Inline JSON-heavy STRING fields as Cypher literals to work around
        # real_ladybug bug that crashes when STRING params resemble JSON arrays.
        escaped_params = _cypher_escape(params_json)
        escaped_body = _cypher_escape(body_json)
        escaped_resps = _cypher_escape(responses_json)

        # Compute compact + request-only projections from the normalized spec.
        compact_json = ""
        request_only_json = ""
        spec = specs_by_endpoint.get((entry.method, entry.path)) or specs_by_category.get(entry.category)
        if spec is not None:
            try:
                compact_view = project_compact(spec, entry.method, entry.path)
                if compact_view is not None:
                    compact_json = json.dumps(compact_view)
            except Exception as exc:  # pragma: no cover — defensive
                print(
                    f"    ⚠ project_compact failed for {entry.method} {entry.path}: {exc}",
                    file=sys.stderr,
                )
            try:
                req_only_view = project_request_only(spec, entry.method, entry.path)
                if req_only_view is not None:
                    request_only_json = json.dumps(req_only_view)
            except Exception as exc:  # pragma: no cover — defensive
                print(
                    f"    ⚠ project_request_only failed for {entry.method} {entry.path}: {exc}",
                    file=sys.stderr,
                )
        if compact_json:
            compact_ok += 1
        if request_only_json:
            request_only_ok += 1
        escaped_compact = _cypher_escape(compact_json)
        escaped_req_only = _cypher_escape(request_only_json)

        conn.execute(
            "CREATE (e:ApiEndpoint {"
            "  endpoint_id: $eid, method: $method, path: $path,"
            "  summary: $summary, description: $descr, operationId: $opid,"
            f"  category: $cat, deprecated: $dep, {tags_clause}"
            f"  parameters: '{escaped_params}', requestBody: '{escaped_body}',"
            f"  responses: '{escaped_resps}',"
            f"  bodyCompactJson: '{escaped_compact}',"
            f"  bodyRequestOnlyJson: '{escaped_req_only}'"
            "})",
            parameters={
                "eid": endpoint_id,
                "method": entry.method,
                "path": entry.path,
                "summary": entry.summary or "",
                "descr": entry.description or "",
                "opid": entry.operation_id or "",
                "cat": entry.category,
                "dep": entry.deprecated,
            },
        )
        count += 1

    # Insert ApiCategory nodes
    for cat_name, cat_count in index.categories.items():
        conn.execute(
            "CREATE (c:ApiCategory {name: $cname, endpointCount: $cnt, sourceProvider: $src})",
            parameters={"cname": cat_name, "cnt": cat_count, "src": "public-docs"},
        )

    # Create BELONGS_TO_CATEGORY relationships
    conn.execute(
        "MATCH (e:ApiEndpoint), (c:ApiCategory) "
        "WHERE e.category = c.name "
        "CREATE (e)-[:BELONGS_TO_CATEGORY]->(c)"
    )

    print(f"  Inserted {count} endpoints in {len(index.categories)} categories")
    if count:
        compact_pct = 100 * compact_ok // count
        ro_pct = 100 * request_only_ok // count
        print(
            f"  Projection coverage: compact={compact_ok}/{count} ({compact_pct}%), "
            f"request-only={request_only_ok}/{count} ({ro_pct}%)"
        )
        # Hard fail if coverage collapses — silently empty blobs degrade
        # the MCP tool surface back to the un-normalized 'full' shape.
        min_pct = 90
        if compact_pct < min_pct or ro_pct < min_pct:
            print(
                f"  ✖ Projection coverage below {min_pct}% — refusing to ship a "
                "degraded knowledge DB.",
                file=sys.stderr,
            )
            sys.exit(1)
    return count


def _sync_docs(cache_dir: Path) -> tuple[list, dict]:
    """Sync VSG documentation pages.

    Returns ``(doc_entries, sync_health_entry)``.  When the upstream WAF
    blocks the runner the entry is marked ``degraded`` (not ``error``) so
    the daily build can still publish a fresh API catalog.
    """
    print("  Refreshing VSG Central documentation...")
    try:
        provider = VsgDocProvider()
        entries = provider.fetch_docs(cache_dir=cache_dir / "vsg", ttl=0)
        report = provider.last_report
        if report is None:
            health = {"status": "ok", "section_count": len(entries)}
        elif report.access_denied:
            health = {
                "status": "degraded",
                "section_count": len(entries),
                "reason": "access_denied",
                "detail": (
                    "VSG host returned HTTP 403 for every page; the runner's "
                    "egress IP is likely blocked at the upstream WAF."
                ),
                "report": report.as_dict(),
            }
        elif report.pages_failed > 0:
            health = {
                "status": "degraded",
                "section_count": len(entries),
                "reason": "partial_failure",
                "report": report.as_dict(),
            }
        else:
            health = {
                "status": "ok",
                "section_count": len(entries),
                "report": report.as_dict(),
            }
        print(f"    → {len(entries)} documentation sections")
        if report and report.pages_failed:
            print(
                f"    ⚠ VSG: {report.pages_failed}/{report.pages_total} pages "
                f"unavailable (failures={dict(report.failure_reasons)})"
            )
        return entries, health
    except Exception as e:
        health = {"status": "error", "section_count": 0, "error": str(e)}
        print(f"    ⚠ VSG refresh failed: {e}", file=sys.stderr)
        return [], health


def _populate_docs(db: lb.Database, docs: list) -> int:
    """Insert DocSection nodes from synced VSG documentation."""
    conn = lb.Connection(db)
    count = 0

    for entry in docs:
        escaped_content = _cypher_escape(entry.content)
        conn.execute(
            "CREATE (d:DocSection {"
            "  section_id: $sid, title: $title,"
            f"  content: '{escaped_content}',"
            "  source: $source, url: $url"
            "})",
            parameters={
                "sid": entry.section_id,
                "title": entry.title,
                "source": entry.source,
                "url": entry.url,
            },
        )
        count += 1

    print(f"  Inserted {count} documentation sections")
    return count


def _populate_seeds(db: lb.Database, seeds_dir: Path) -> int:
    """Insert seed scripts as Script nodes."""
    conn = lb.Connection(db)
    count = 0

    for meta_file in sorted(seeds_dir.glob("*.meta.json")):
        script_file = meta_file.with_suffix("").with_suffix(".py")
        if not script_file.exists():
            continue

        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        content = script_file.read_text(encoding="utf-8")
        filename = script_file.name

        seed_tags = meta.get("tags") or []
        seed_tags_lit = _cypher_string_list(seed_tags)

        # Inline content and params as Cypher literals (real_ladybug binding bug)
        escaped_content = _cypher_escape(content)
        escaped_params = _cypher_escape(json.dumps(meta.get("parameters", [])))

        conn.execute(
            "CREATE (s:Script {"
            f"  filename: $fn, description: $descr, tags: {seed_tags_lit},"
            f"  content: '{escaped_content}', parameters: '{escaped_params}'"
            "})",
            parameters={
                "fn": filename,
                "descr": meta.get("description", ""),
            },
        )
        count += 1
        print(f"    Seed: {filename}")

    print(f"  Inserted {count} seed scripts")
    return count


def _create_fts_indexes(db: lb.Database) -> int:
    """Create FTS indexes for BM25-ranked search."""
    conn = lb.Connection(db)

    try:
        conn.execute("INSTALL fts")
        conn.execute("LOAD EXTENSION fts")
    except Exception as exc:
        if "already" not in str(exc).lower():
            print(f"  ⚠ FTS extension unavailable: {exc}", file=sys.stderr)
            return 0

    fts_defs = [
        ("api_fts", "ApiEndpoint", ["summary", "description", "path", "operationId"]),
        ("doc_fts", "DocSection", ["title", "content"]),
        ("script_fts", "Script", ["filename", "description"]),
        # Data-node indexes are only useful at runtime (nodes populated by seeds),
        # but we create them at build time so the schema is ready.
        ("device_fts", "Device", ["name", "serial", "model", "deviceType"]),
        ("site_fts", "Site", ["name", "address", "city", "country"]),
        ("config_fts", "ConfigProfile", ["name", "category"]),
    ]

    created = 0
    for idx_name, table, fields in fts_defs:
        try:
            conn.execute(f"CALL DROP_FTS_INDEX('{table}', '{idx_name}')")
        except Exception:
            pass
        try:
            cypher_field_list = ", ".join(f"'{f}'" for f in fields)
            conn.execute(
                f"CALL CREATE_FTS_INDEX('{table}', '{idx_name}', [{cypher_field_list}])"
            )
            created += 1
            print(f"    Created {idx_name} on {table}")
        except Exception as exc:
            print(f"    ⚠ {idx_name} failed: {exc}", file=sys.stderr)
    return created


def main() -> None:
    parser = argparse.ArgumentParser(description="Build LadybugDB knowledge database")
    parser.add_argument("--output-dir", type=Path, default=Path("build"),
                        help="Output directory for the DB and tar (default: ./build)")
    parser.add_argument("--tar", action="store_true",
                        help="Create a tar.gz archive of the database")
    args = parser.parse_args()

    output_dir: Path = args.output_dir.resolve()
    db_path = output_dir / "knowledge_db"
    cache_dir = output_dir / "spec_cache"

    # Clean previous build
    if db_path.exists():
        shutil.rmtree(db_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    cache_dir.mkdir(parents=True, exist_ok=True)

    print("=== Building Knowledge Database ===\n")

    # 1. Create DB and apply schema
    print("[1/6] Creating database and applying schema...")
    db = lb.Database(str(db_path))
    _apply_schema(db)

    # 2. Sync API specs
    print("\n[2/6] Refreshing API documentation...")
    specs, sync_health = _sync_specs(cache_dir)
    if not specs:
        print("⚠ No specs available — database will have no API endpoints.", file=sys.stderr)

    # 3. Build index and populate
    print("\n[3/6] Populating API endpoints...")
    if specs:
        print(f"  Normalizing {len(specs)} specs (dedup error/object schemas)...")
        specs = [normalize_spec(s) for s in specs]
    index = OASIndex()
    index.build(specs)
    endpoint_count = _populate_endpoints(db, index, specs)

    # 4. Sync and populate VSG documentation
    print("\n[4/6] Refreshing and populating VSG documentation...")
    doc_entries, vsg_health = _sync_docs(cache_dir)
    sync_health["vsg"] = vsg_health
    doc_count = _populate_docs(db, doc_entries) if doc_entries else 0

    # 5. Populate seed scripts
    print("\n[5/6] Populating seed scripts...")
    seeds_dir = Path(__file__).resolve().parent.parent / "src" / "hpe_networking_central_mcp" / "seeds"
    if seeds_dir.is_dir():
        _populate_seeds(db, seeds_dir)
    else:
        print(f"  ⚠ Seeds dir not found: {seeds_dir}")

    # Create FTS indexes for BM25-ranked search
    print("\n[6/6] Creating FTS indexes...")
    fts_count = _create_fts_indexes(db)
    print(f"  FTS indexes: {fts_count}")

    # Close DB before tar
    db.close()

    # Write manifest.json alongside the DB
    import time
    manifest = {
        "version": time.strftime("knowledge-db-%Y%m%d-%H%M%S", time.gmtime()),
        "schema_version": 2,
        "endpoint_count": endpoint_count,
        "category_count": len(index.categories),
        "doc_count": doc_count,
        "categories": sorted(index.categories.keys()),
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "sync_health": sync_health,
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"\n✓ Knowledge DB ready at {db_path}")
    print(f"  Endpoints: {endpoint_count}")
    print(f"  Categories: {len(index.categories)}")
    print(f"  Doc sections: {doc_count}")
    print(f"  Manifest: {manifest_path}")

    # Optionally tar (include both DB and manifest)
    if args.tar:
        tar_path = output_dir / "knowledge_db.tar.gz"
        print(f"\nCreating archive: {tar_path}")
        with tarfile.open(tar_path, "w:gz") as tf:
            tf.add(db_path, arcname="knowledge_db")
            tf.add(manifest_path, arcname="manifest.json")
        print(f"✓ Archive created ({tar_path.stat().st_size / 1024 / 1024:.1f} MB)")


if __name__ == "__main__":
    main()
