#!/usr/bin/env python3
"""Build a Kùzu knowledge database from scraped API specs and seed scripts.

Run on a GitHub Actions runner (no Central/GLP credentials required).
Scrapes public developer documentation, parses OpenAPI specs, and populates
a Kùzu graph database with ApiEndpoint, ApiCategory, DocSection, and Script
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

import kuzu  # noqa: E402

from hpe_networking_central_mcp.graph.schema import (  # noqa: E402
    KNOWLEDGE_NODE_TABLES,
    KNOWLEDGE_REL_TABLES,
    NODE_TABLES,
    REL_TABLES,
    TOPOLOGY_REL_TABLES,
)
from hpe_networking_central_mcp.oas_index import OASIndex  # noqa: E402
from hpe_networking_central_mcp.oas_scraper import ReadMeSpecProvider  # noqa: E402

# GLP provider may fail if dependencies vary; import conditionally
try:
    from hpe_networking_central_mcp.glp_spec_provider import GreenLakeSpecProvider

    _HAS_GLP = True
except ImportError:
    _HAS_GLP = False


def _apply_schema(db: kuzu.Database) -> None:
    """Apply full schema DDL (live + knowledge tables)."""
    conn = kuzu.Connection(db)
    all_ddl = NODE_TABLES + KNOWLEDGE_NODE_TABLES + REL_TABLES + KNOWLEDGE_REL_TABLES + TOPOLOGY_REL_TABLES
    for ddl in all_ddl:
        conn.execute(ddl.strip())
    print(f"  Schema applied: {len(all_ddl)} DDL statements")


def _scrape_specs(cache_dir: Path) -> list[dict]:
    """Scrape API specs from all public documentation sources."""
    specs: list[dict] = []

    # Central (ReadMe.io)
    print("  Scraping Central API docs (ReadMe.io)...")
    try:
        provider = ReadMeSpecProvider()
        central_specs = provider.fetch_specs(cache_dir=cache_dir / "central", ttl=0)
        specs.extend(central_specs)
        print(f"    → {len(central_specs)} Central specs")
    except Exception as e:
        print(f"    ⚠ Central scrape failed: {e}", file=sys.stderr)

    # GreenLake (developer portal)
    if _HAS_GLP:
        print("  Scraping GreenLake API docs (developer portal)...")
        try:
            glp_provider = GreenLakeSpecProvider()
            glp_specs = glp_provider.fetch_specs(cache_dir=cache_dir / "glp", ttl=0)
            specs.extend(glp_specs)
            print(f"    → {len(glp_specs)} GreenLake specs")
        except Exception as e:
            print(f"    ⚠ GreenLake scrape failed: {e}", file=sys.stderr)

    return specs


def _populate_endpoints(db: kuzu.Database, index: OASIndex) -> int:
    """Insert ApiEndpoint and ApiCategory nodes from the OASIndex."""
    conn = kuzu.Connection(db)
    count = 0

    for entry in index._entries:  # noqa: SLF001 — accessing internal for bulk insert
        endpoint_id = f"{entry.method}:{entry.path}"
        conn.execute(
            "CREATE (e:ApiEndpoint {"
            "  endpoint_id: $eid, method: $method, path: $path,"
            "  summary: $summary, description: $descr, operationId: $opid,"
            "  category: $cat, deprecated: $dep, tags: $tags,"
            "  parameterNames: $params, hasRequestBody: $hasBody"
            "})",
            {
                "eid": endpoint_id,
                "method": entry.method,
                "path": entry.path,
                "summary": entry.summary,
                "descr": entry.description,
                "opid": entry.operation_id,
                "cat": entry.category,
                "dep": entry.deprecated,
                "tags": entry.tags,
                "params": [p.name for p in entry.parameters],
                "hasBody": entry.request_body is not None,
            },
        )
        count += 1

    # Insert ApiCategory nodes
    for cat_name, cat_count in index.categories.items():
        conn.execute(
            "CREATE (c:ApiCategory {name: $cname, endpointCount: $cnt, sourceProvider: $src})",
            {"cname": cat_name, "cnt": cat_count, "src": "scraped"},
        )

    # Create BELONGS_TO_CATEGORY relationships
    conn.execute(
        "MATCH (e:ApiEndpoint), (c:ApiCategory) "
        "WHERE e.category = c.name "
        "CREATE (e)-[:BELONGS_TO_CATEGORY]->(c)"
    )

    print(f"  Inserted {count} endpoints in {len(index.categories)} categories")
    return count


def _populate_seeds(db: kuzu.Database, seeds_dir: Path) -> int:
    """Insert seed scripts as Script nodes."""
    conn = kuzu.Connection(db)
    count = 0

    for meta_file in sorted(seeds_dir.glob("*.meta.json")):
        script_file = meta_file.with_suffix("").with_suffix(".py")
        if not script_file.exists():
            continue

        meta = json.loads(meta_file.read_text(encoding="utf-8"))
        content = script_file.read_text(encoding="utf-8")
        filename = script_file.name

        conn.execute(
            "CREATE (s:Script {"
            "  filename: $fn, description: $descr, tags: $tags,"
            "  content: $content, parameters: $params"
            "})",
            {
                "fn": filename,
                "descr": meta.get("description", ""),
                "tags": meta.get("tags", []),
                "content": content,
                "params": json.dumps(meta.get("parameters", [])),
            },
        )
        count += 1
        print(f"    Seed: {filename}")

    print(f"  Inserted {count} seed scripts")
    return count


def main() -> None:
    parser = argparse.ArgumentParser(description="Build Kùzu knowledge database")
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
    print("[1/4] Creating database and applying schema...")
    db = kuzu.Database(str(db_path))
    _apply_schema(db)

    # 2. Scrape API specs
    print("\n[2/4] Scraping API documentation...")
    specs = _scrape_specs(cache_dir)
    if not specs:
        print("⚠ No specs scraped — database will have no API endpoints.", file=sys.stderr)

    # 3. Build index and populate
    print("\n[3/4] Populating API endpoints...")
    index = OASIndex()
    index.build(specs)
    endpoint_count = _populate_endpoints(db, index)

    # 4. Populate seed scripts
    print("\n[4/4] Populating seed scripts...")
    seeds_dir = Path(__file__).resolve().parent.parent / "src" / "hpe_networking_central_mcp" / "seeds"
    if seeds_dir.is_dir():
        _populate_seeds(db, seeds_dir)
    else:
        print(f"  ⚠ Seeds dir not found: {seeds_dir}")

    # Close DB before tar
    db.close()

    # Write manifest.json alongside the DB
    import time
    manifest = {
        "version": time.strftime("knowledge-db-%Y%m%d-%H%M%S", time.gmtime()),
        "endpoint_count": endpoint_count,
        "category_count": len(index.categories),
        "categories": sorted(index.categories.keys()),
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
    }
    manifest_path = output_dir / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2), encoding="utf-8")

    print(f"\n✓ Knowledge DB ready at {db_path}")
    print(f"  Endpoints: {endpoint_count}")
    print(f"  Categories: {len(index.categories)}")
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
