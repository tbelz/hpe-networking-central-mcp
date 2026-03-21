#!/usr/bin/env python3
"""Build a LadybugDB knowledge database from scraped API specs and seed scripts.

Run on a GitHub Actions runner (no Central/GLP credentials required).
Scrapes public developer documentation, parses OpenAPI specs, and populates
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
    PROVENANCE_REL_TABLES,
    REL_TABLES,
    TOPOLOGY_REL_TABLES,
)
from hpe_networking_central_mcp.oas_index import OASIndex  # noqa: E402
from hpe_networking_central_mcp.oas_scraper import ReadMeSpecProvider  # noqa: E402
from hpe_networking_central_mcp.entity_mapping import run_mapping, build_default_pipeline  # noqa: E402
from hpe_networking_central_mcp.entity_mapping.entities import build_registry_from_node_tables  # noqa: E402
from hpe_networking_central_mcp.schema_generator import (  # noqa: E402
    content_hash,
    generate_ddl,
    infer_node_tables,
    infer_rel_tables,
)

# GLP provider may fail if dependencies vary; import conditionally
try:
    from hpe_networking_central_mcp.glp_spec_provider import GreenLakeSpecProvider

    _HAS_GLP = True
except ImportError:
    _HAS_GLP = False


def _apply_schema(db: lb.Database) -> None:
    """Apply full schema DDL (live + knowledge tables)."""
    conn = lb.Connection(db)
    all_ddl = NODE_TABLES + KNOWLEDGE_NODE_TABLES + REL_TABLES + KNOWLEDGE_REL_TABLES + TOPOLOGY_REL_TABLES + POLICY_REL_TABLES + PROVENANCE_REL_TABLES
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


def _populate_endpoints(db: lb.Database, index: OASIndex) -> int:
    """Insert ApiEndpoint and ApiCategory nodes from the OASIndex."""
    conn = lb.Connection(db)
    count = 0

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

        conn.execute(
            "CREATE (e:ApiEndpoint {"
            "  endpoint_id: $eid, method: $method, path: $path,"
            "  summary: $summary, description: $descr, operationId: $opid,"
            "  category: $cat, deprecated: $dep, tags: $tags,"
            "  parameters: $params, requestBody: $body, responses: $resps"
            "})",
            parameters={
                "eid": endpoint_id,
                "method": entry.method,
                "path": entry.path,
                "summary": entry.summary,
                "descr": entry.description,
                "opid": entry.operation_id,
                "cat": entry.category,
                "dep": entry.deprecated,
                "tags": entry.tags,
                "params": params_json,
                "body": body_json,
                "resps": responses_json,
            },
        )
        count += 1

    # Insert ApiCategory nodes
    for cat_name, cat_count in index.categories.items():
        conn.execute(
            "CREATE (c:ApiCategory {name: $cname, endpointCount: $cnt, sourceProvider: $src})",
            parameters={"cname": cat_name, "cnt": cat_count, "src": "scraped"},
        )

    # Create BELONGS_TO_CATEGORY relationships
    conn.execute(
        "MATCH (e:ApiEndpoint), (c:ApiCategory) "
        "WHERE e.category = c.name "
        "CREATE (e)-[:BELONGS_TO_CATEGORY]->(c)"
    )

    print(f"  Inserted {count} endpoints in {len(index.categories)} categories")
    return count


def _populate_entity_mappings(db: lb.Database, index: OASIndex, registry=None) -> dict:
    """Run entity mapping pipeline and populate EntityType + OPERATES_ON edges.

    Args:
        db: LadybugDB database.
        index: The OAS index with parsed API endpoints.
        registry: Optional pre-built EntityRegistry (e.g. from DDL).
            Falls back to the static build_aruba_central_registry().

    Returns the mapping report as a JSON-serializable dict.
    """
    conn = lb.Connection(db)
    pipeline, registry = build_default_pipeline(registry=registry)

    # Insert EntityType nodes
    for entity in registry.all_entities():
        fields_json = json.dumps({
            f.name: {"graph_property": f.graph_property, "description": f.description}
            for f in entity.fields.values()
        })
        conn.execute(
            "CREATE (e:EntityType {"
            "  name: $name, graphNode: $gn, description: $descr, fields: $fields"
            "})",
            parameters={
                "name": entity.name,
                "gn": entity.graph_node,
                "descr": entity.description,
                "fields": fields_json,
            },
        )

    # Run the mapping pipeline
    report = run_mapping(index, pipeline, registry)

    # Insert OPERATES_ON edges for mapped results
    edge_count = 0
    seen_edges: set[tuple[str, str, str]] = set()
    for result in report.results:
        if not result.is_mapped:
            continue
        # Deduplicate: same endpoint → same entity with same param
        edge_key = (result.endpoint_id, result.entity_name, result.param_name)
        if edge_key in seen_edges:
            continue
        seen_edges.add(edge_key)

        conn.execute(
            "MATCH (e:ApiEndpoint {endpoint_id: $eid}), (et:EntityType {name: $ename}) "
            "CREATE (e)-[:OPERATES_ON {"
            "  paramName: $pname, fieldName: $fname,"
            "  confidence: $conf, mapper: $mapper, reason: $reason,"
            "  operation: $op"
            "}]->(et)",
            parameters={
                "eid": result.endpoint_id,
                "ename": result.entity_name,
                "pname": result.param_name,
                "fname": result.field_name,
                "conf": result.confidence.value,
                "mapper": result.mapper_name,
                "reason": result.reason,
                "op": result.operation,
            },
        )
        edge_count += 1

    print(f"  Entity types: {len(registry)}")
    print(f"  OPERATES_ON edges: {edge_count}")
    print(f"  Coverage: {report.coverage_pct():.1f}% of params mapped")
    return report.to_json()


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

        conn.execute(
            "CREATE (s:Script {"
            "  filename: $fn, description: $descr, tags: $tags,"
            "  content: $content, parameters: $params"
            "})",
            parameters={
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
            conn.execute(f"CALL fts.drop_fts_index('{idx_name}')")
        except Exception:
            pass
        try:
            cypher_field_list = ", ".join(f"'{f}'" for f in fields)
            conn.execute(
                f"CALL fts.create_fts_index('{idx_name}', '{table}', [{cypher_field_list}])"
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
    print("[1/7] Creating database and applying schema...")
    db = lb.Database(str(db_path))
    _apply_schema(db)

    # 2. Scrape API specs
    print("\n[2/7] Scraping API documentation...")
    specs = _scrape_specs(cache_dir)
    if not specs:
        print("⚠ No specs scraped — database will have no API endpoints.", file=sys.stderr)

    # 3. Build index and populate
    print("\n[3/7] Populating API endpoints...")
    index = OASIndex()
    index.build(specs)
    endpoint_count = _populate_endpoints(db, index)

    # 4. Generate DDL from API response schemas
    print("\n[4/7] Generating DDL from API response schemas...")
    node_tables = infer_node_tables(index)
    rel_tables = infer_rel_tables(index, node_tables)
    generated_ddl = generate_ddl(node_tables, rel_tables)
    ddl_hash = content_hash(generated_ddl)
    ddl_path = output_dir / "generated_ddl.json"
    ddl_path.write_text(json.dumps({
        "schema_hash": ddl_hash,
        "node_tables": len(node_tables),
        "rel_tables": len(rel_tables),
        "ddl": generated_ddl,
    }, indent=2), encoding="utf-8")
    print(f"  Node tables: {len(node_tables)}")
    print(f"  Rel tables:  {len(rel_tables)}")
    print(f"  Schema hash: {ddl_hash}")
    print(f"  DDL file:    {ddl_path}")

    # 5. Entity mapping (use DDL-derived registry when available)
    print("\n[5/7] Running entity mapping pipeline...")
    ddl_registry = build_registry_from_node_tables(node_tables) if node_tables else None
    mapping_report = _populate_entity_mappings(db, index, registry=ddl_registry)

    # 6. Populate seed scripts
    print("\n[6/7] Populating seed scripts...")
    seeds_dir = Path(__file__).resolve().parent.parent / "src" / "hpe_networking_central_mcp" / "seeds"
    if seeds_dir.is_dir():
        _populate_seeds(db, seeds_dir)
    else:
        print(f"  ⚠ Seeds dir not found: {seeds_dir}")

    # Create FTS indexes for BM25-ranked search
    print("\n[7/7] Creating FTS indexes...")
    fts_count = _create_fts_indexes(db)
    print(f"  FTS indexes: {fts_count}")

    # Close DB before tar
    db.close()

    # Write manifest.json alongside the DB
    import time
    manifest = {
        "version": time.strftime("knowledge-db-%Y%m%d-%H%M%S", time.gmtime()),
        "schema_hash": ddl_hash,
        "endpoint_count": endpoint_count,
        "category_count": len(index.categories),
        "categories": sorted(index.categories.keys()),
        "generated_ddl": {
            "node_tables": len(node_tables),
            "rel_tables": len(rel_tables),
            "hash": ddl_hash,
        },
        "built_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "entity_mapping": mapping_report,
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
            tf.add(ddl_path, arcname="generated_ddl.json")
        print(f"✓ Archive created ({tar_path.stat().st_size / 1024 / 1024:.1f} MB)")


if __name__ == "__main__":
    main()
