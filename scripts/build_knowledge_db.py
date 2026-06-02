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
)
from hpe_networking_central_mcp.oas_schema_graph import (  # noqa: E402
    collect_into_batch,
    flush_batch,
    new_batch,
    query_existing_eids,
)
from hpe_networking_central_mcp.graph.invariants import (  # noqa: E402
    assert_graph_invariants,
    format_report,
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
        for s in central_specs:
            s["_spec_source"] = "central"
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
            for s in glp_specs:
                s["_spec_source"] = "glp"
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


def _load_cached_specs_sample(cache_dir: Path, n: int) -> tuple[list[dict], dict]:
    """Dev shortcut for ``--sample N``: load cached OAS files and keep
    only the first ``n`` operations per provider.

    Returns ``(specs, sync_health)`` shaped exactly like ``_sync_specs``
    so the rest of the pipeline is identical. Skips all network I/O —
    fails loudly if no cache exists yet.
    """
    import yaml  # type: ignore  # noqa: PLC0415
    specs: list[dict] = []
    sync_health: dict = {}
    # Keep sync_health keys aligned with _sync_specs ("central", "greenlake")
    # so manifest.json's schema is identical between full and --sample builds.
    # The on-disk cache layout still uses ``glp/`` and _spec_source stays
    # "glp" to match the full path.
    providers = (("central", "central"), ("glp", "greenlake"))
    for provider, health_key in providers:
        provider_dir = cache_dir / provider
        if not provider_dir.is_dir():
            sync_health[health_key] = {"status": "error", "spec_count": 0, "coverage": 0.0,
                                      "error": f"no cache at {provider_dir}"}
            continue
        files = sorted(provider_dir.rglob("*.json")) + sorted(provider_dir.rglob("*.yaml"))
        # Cap files loaded so a sample doesn't pull thousands of single-endpoint excerpts.
        # Each cached file in the per-endpoint layout already contributes ~1 path,
        # so loading `n` files yields ~`n` endpoints for this provider.
        files = files[:n]
        loaded = 0
        provider_start = len(specs)
        for f in files:
            try:
                if f.suffix == ".json":
                    spec = json.loads(f.read_text(encoding="utf-8"))
                else:
                    spec = yaml.safe_load(f.read_text(encoding="utf-8"))
            except Exception as exc:
                print(f"    ⚠ Skipping {f.name}: {exc}", file=sys.stderr)
                continue
            if not isinstance(spec, dict):
                continue
            spec["_spec_source"] = provider
            specs.append(spec)
            loaded += 1
        # If any spec carries more than one path (multi-path cached form),
        # truncate so the per-provider total stays close to `n`.
        per_spec_cap = max(1, n // max(1, loaded))
        for spec in specs[provider_start:]:
            paths = spec.get("paths")
            if isinstance(paths, dict) and len(paths) > per_spec_cap:
                spec["paths"] = dict(list(paths.items())[:per_spec_cap])
        sync_health[health_key] = {"status": "ok", "spec_count": loaded, "coverage": 1.0,
                                  "note": f"--sample {n} (loaded {loaded} files)"}
        print(f"    → {loaded} cached {provider} specs (sample mode)")
    return specs, sync_health


def _print_build_report(db: lb.Database, schema_stats: dict, violations: list) -> None:
    """Print absolute current-build counters. No history, no trend lines —
    just enough numbers to eyeball whether this build matches expectations.
    """
    conn = lb.Connection(db)
    def _count(cypher: str) -> int:
        try:
            rows = list(conn.execute(cypher).rows_as_dict())
            return int(rows[0]["n"]) if rows else 0
        except Exception:
            return -1

    metrics = {
        "ApiEndpoint": _count("MATCH (n:ApiEndpoint) RETURN COUNT(n) AS n"),
        "SchemaComponent": _count("MATCH (n:SchemaComponent) RETURN COUNT(n) AS n"),
        "  named": _count(
            "MATCH (n:SchemaComponent) WHERE NOT n.component_id CONTAINS '#' RETURN COUNT(n) AS n"
        ),
        "  inline": _count(
            "MATCH (n:SchemaComponent) WHERE n.component_id CONTAINS '#' RETURN COUNT(n) AS n"
        ),
        "  unresolved": _count(
            "MATCH (n:SchemaComponent {kind: 'unresolved'}) RETURN COUNT(n) AS n"
        ),
        "Property": _count("MATCH (n:Property) RETURN COUNT(n) AS n"),
        "YangPath": _count("MATCH (n:YangPath) RETURN COUNT(n) AS n"),
        "HAS_PROPERTY": _count("MATCH ()-[r:HAS_PROPERTY]->() RETURN COUNT(r) AS n"),
        "COMPOSED_OF": _count("MATCH ()-[r:COMPOSED_OF]->() RETURN COUNT(r) AS n"),
        "CONFIGURES_YANG": _count("MATCH ()-[r:CONFIGURES_YANG]->() RETURN COUNT(r) AS n"),
    }

    named = max(1, metrics.get("  named", 1))
    props_per_named = metrics["HAS_PROPERTY"] / named if named > 0 else 0.0
    empty_named = _count(
        """
        MATCH (c:SchemaComponent)
        WHERE NOT c.component_id CONTAINS '#'
          AND c.bodyShape = 'object'
          AND NOT EXISTS { MATCH (c)-[:HAS_PROPERTY]->() }
        RETURN COUNT(c) AS n
        """
    )
    empty_pct = (empty_named / named * 100.0) if named > 0 else 0.0

    print(f"  Nodes:")
    for k, v in metrics.items():
        print(f"    {k:<18} {v}")
    print(f"  Health:")
    print(f"    properties / named component  {props_per_named:>6.2f}")
    print(f"    named objects with no fields  {empty_named} ({empty_pct:.1f}%)")
    from hpe_networking_central_mcp.graph.invariants import _CHECKS
    print(f"    invariant violations          {len(violations)} of {len(_CHECKS)}")
    del conn


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

    ``specs`` is accepted for signature compatibility; the schema-graph
    population pass (Property/SchemaComponent/Parameter) consumes it.
    The blob projection columns (skeleton/glossary/components) were
    retired in ADR 009 Phase 2E.
    """
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

        # Inline tags as a Cypher list literal (real_ladybug cannot bind
        # Python lists as STRING[] parameters — triggers ANY-type error).
        tags_literal = _cypher_string_list(entry.tags)
        tags_clause = f"tags: {tags_literal}, " if entry.tags else ""

        # Inline JSON-heavy STRING fields as Cypher literals to work around
        # real_ladybug bug that crashes when STRING params resemble JSON arrays.
        escaped_params = _cypher_escape(params_json)
        escaped_body = _cypher_escape(body_json)
        escaped_resps = _cypher_escape(responses_json)

        conn.execute(
            "CREATE (e:ApiEndpoint {"
            "  endpoint_id: $eid, method: $method, path: $path,"
            "  summary: $summary, description: $descr, operationId: $opid,"
            f"  category: $cat, deprecated: $dep, {tags_clause}"
            f"  parameters: '{escaped_params}', requestBody: '{escaped_body}',"
            f"  responses: '{escaped_resps}'"
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
    return count


def _sync_docs(cache_dir: Path) -> tuple[list, dict]:
    """Sync VSG documentation pages.

    Returns ``(doc_entries, sync_health_entry)``.  When the upstream WAF
    blocks the runner the entry is marked ``degraded`` (not ``error``) so
    the daily build can still publish a fresh API catalog.
    """
    print("  Refreshing VSG Central documentation...")
    vsg_cache_dir = cache_dir / "vsg"
    cache_primed = vsg_cache_dir.is_dir() and any(vsg_cache_dir.iterdir())
    cache_age_days: float | None = None
    if cache_primed:
        try:
            mtimes = [p.stat().st_mtime for p in vsg_cache_dir.rglob("*") if p.is_file()]
            if mtimes:
                import time as _time
                cache_age_days = round((_time.time() - max(mtimes)) / 86400, 2)
        except Exception:  # pragma: no cover — stat failures non-fatal
            pass
    try:
        provider = VsgDocProvider()
        entries = provider.fetch_docs(cache_dir=vsg_cache_dir, ttl=0)
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
        health["cache_primed"] = cache_primed
        if cache_age_days is not None:
            health["cache_age_days"] = cache_age_days
        return entries, health
    except Exception as e:
        health = {
            "status": "error",
            "section_count": 0,
            "error": str(e),
            "cache_primed": cache_primed,
        }
        if cache_age_days is not None:
            health["cache_age_days"] = cache_age_days
        print(f"    ⚠ VSG refresh failed: {e}", file=sys.stderr)
        return [], health


def _build_provider_component_pool(specs: list[dict]) -> dict[str, dict]:
    """Pass A: build a provider-wide, richest-wins ``components`` dict.

    Multi-spec bundles (especially the Aruba Central network-config
    family) frequently split a single semantic schema across many spec
    files: one file declares the body fully, others reference it via
    ``$ref`` without re-declaring. Without a provider-wide pool, the
    spec that resolves the ref second silently sees an empty stub and
    drops the body decomposition.

    For every ``(spec_source, section, name)`` triple we keep the body
    whose serialised representation is longest — a stable proxy for
    "most-decomposed". The merged dicts are then passed as
    ``resolution_scope`` to ``collect_into_batch`` in Pass B.
    """
    pools: dict[str, dict[str, dict[str, dict]]] = {}
    for spec in specs:
        spec_source = spec.get("_spec_source") or "central"
        components = spec.get("components")
        if not isinstance(components, dict):
            continue
        provider_pool = pools.setdefault(spec_source, {})
        for section, entries in components.items():
            if not isinstance(entries, dict):
                continue
            section_pool = provider_pool.setdefault(section, {})
            for name, body in entries.items():
                if not isinstance(body, dict):
                    continue
                try:
                    body_len = len(json.dumps(body, default=str))
                except (TypeError, ValueError):
                    body_len = 0
                existing = section_pool.get(name)
                if existing is None:
                    section_pool[name] = body
                    continue
                try:
                    existing_len = len(json.dumps(existing, default=str))
                except (TypeError, ValueError):
                    existing_len = 0
                if body_len > existing_len:
                    section_pool[name] = body
    return pools


def _populate_configures_yang(db: lb.Database) -> int:
    """Derive ApiEndpoint→YangPath edges from the property reverse-index.

    Run after the main schema-subgraph flush. We walk
    ``ApiEndpoint -[HAS_REQUEST_BODY]-> RequestBody -[BODY_REFERENCES]->
    SchemaComponent -[COMPOSED_OF*0..6]-> SchemaComponent
    -[HAS_PROPERTY]-> Property -[PROPERTY_AT_YANG]-> YangPath`` in
    Cypher, dedupe ``(endpoint_id, yangPath)`` pairs, and COPY them
    into the ``CONFIGURES_YANG`` rel table. COPY is required (instead
    of inline ``MERGE``) because of LadybugDB issue #285.
    """
    conn = lb.Connection(db)
    try:
        # Traverse both COMPOSED_OF (allOf/oneOf/anyOf branches) and
        # HAS_VALUE_SCHEMA (additionalProperties value shapes) so that
        # YANG-annotated fields living under map value schemas still
        # contribute a CONFIGURES_YANG edge.
        rows = list(
            conn.execute(
                "MATCH (e:ApiEndpoint)-[:HAS_REQUEST_BODY]->(:RequestBody)"
                "-[:BODY_REFERENCES]->(c:SchemaComponent)"
                "-[:COMPOSED_OF|HAS_VALUE_SCHEMA*0..6]->(c2:SchemaComponent)"
                "-[:HAS_PROPERTY]->(p:Property)-[:PROPERTY_AT_YANG]->(y:YangPath) "
                "RETURN DISTINCT e.endpoint_id AS eid, y.yangPath AS yp"
            ).rows_as_dict()
        )
    except Exception as exc:  # pragma: no cover — defensive
        print(f"    ⚠ CONFIGURES_YANG derivation skipped: {exc}", file=sys.stderr)
        return 0
    if not rows:
        return 0
    import pyarrow as _pa
    schema = _pa.schema([("a", _pa.string()), ("b", _pa.string())])
    table = _pa.table(
        {"a": [r["eid"] for r in rows], "b": [r["yp"] for r in rows]},
        schema=schema,
    )
    conn.execute(
        "COPY CONFIGURES_YANG FROM $df",
        parameters={"df": table},
    )
    return len(rows)


def _populate_schema_subgraph(db: lb.Database, specs: list[dict]) -> dict:
    """Walk every spec, collect Parameter/RequestBody/Response/SchemaComponent
    /Property rows + REFERENCES edges into a single global batch, then
    bulk-load via ``COPY ... FROM $df`` (one COPY per table).

    Two-pass: Pass A builds a per-provider ``components`` pool (richest
    body wins per name); Pass B walks every spec with that pool as
    ``resolution_scope`` so cross-spec $refs and stub-then-rich
    ordering both resolve correctly.

    Bulk path is required because LadybugDB issue #285 (UNWIND+MERGE
    bulk insert) is upstream-WONTFIX. Collection is pure Python (no DB
    I/O), so per-spec progress prints reflect parse cost only and the
    final flush is dominated by O(num_rows) COPY work.

    ``specs`` must already be normalized and tagged with ``_spec_source``
    (set in ``_sync_specs``).
    """
    import time as _time

    conn = lb.Connection(db)
    totals = {
        "endpoints": 0,
        "parameters": 0,
        "request_bodies": 0,
        "responses": 0,
        "components": 0,
        "properties": 0,
        "references": 0,
    }
    timings: list[tuple[float, str]] = []
    total_specs = len(specs)

    # ── Pass A: provider-wide component pool ─────────────────────
    t_pool = _time.monotonic()
    provider_pools = _build_provider_component_pool(specs)
    pool_sizes = {
        prov: sum(len(sect) for sect in sections.values())
        for prov, sections in provider_pools.items()
    }
    print(
        f"    [3a/6] provider component pool built in "
        f"{_time.monotonic() - t_pool:.2f}s — {pool_sizes}",
        flush=True,
    )

    # Pre-compute existing endpoint IDs across ALL specs in one query.
    all_eids: list[str] = []
    spec_endpoints: list[tuple[dict, str, str, list[tuple[str, str]]]] = []
    for spec in specs:
        spec_source = spec.get("_spec_source") or "central"
        title = (spec.get("info") or {}).get("title", "?")
        endpoints: list[tuple[str, str]] = []
        for path, path_item in (spec.get("paths") or {}).items():
            if not isinstance(path_item, dict):
                continue
            for method in ("get", "post", "put", "patch", "delete", "head", "options"):
                if isinstance(path_item.get(method), dict):
                    endpoints.append((method.upper(), path))
        if not endpoints:
            continue
        spec_endpoints.append((spec, spec_source, title, endpoints))
        for m, p in endpoints:
            all_eids.append(f"{m.upper()}:{p}")

    print(f"    [3b/6] resolving {len(all_eids)} endpoint IDs across {len(spec_endpoints)} specs…", flush=True)
    t_eids = _time.monotonic()
    existing_eids = query_existing_eids(conn, all_eids)
    print(
        f"    [3b/6] {len(existing_eids)}/{len(all_eids)} endpoints exist "
        f"in DB (lookup {_time.monotonic() - t_eids:.2f}s)",
        flush=True,
    )

    batch = new_batch()
    t_collect = _time.monotonic()
    for idx, (spec, spec_source, title, endpoints) in enumerate(spec_endpoints, 1):
        t0 = _time.monotonic()
        scope = provider_pools.get(spec_source) or None
        try:
            stats = collect_into_batch(
                batch,
                spec_source=spec_source,
                spec=spec,
                endpoints=endpoints,
                existing_eids=existing_eids,
                emit_property_subgraph=True,
                resolution_scope=scope,
            )
        except Exception as exc:  # pragma: no cover — defensive
            print(
                f"    ⚠ collect_into_batch failed for "
                f"{title!r}: {exc}",
                file=sys.stderr,
            )
            continue
        dt = _time.monotonic() - t0
        for k, v in stats.items():
            if k in totals:
                totals[k] += v
        ep = stats.get("endpoints", 0)
        cmp_ = stats.get("components", 0)
        props = stats.get("properties", 0)
        print(
            f"    [3b/6] {idx}/{len(spec_endpoints)} ({spec_source}/{title}): "
            f"{ep} ep • {cmp_} cmp • {props} props parsed in {dt:.2f}s",
            flush=True,
        )
        timings.append((dt, f"{spec_source}/{title}"))

    print(
        f"    [3b/6] collection complete in "
        f"{_time.monotonic() - t_collect:.2f}s; "
        f"buffered {len(batch.params)} params, {len(batch.request_bodies)} bodies, "
        f"{len(batch.responses)} responses, {len(batch.components)} components, "
        f"{len(batch.properties)} properties, {len(batch.yang_paths)} yang paths, "
        f"{len(batch.has_param)} HAS_PARAMETER, "
        f"{len(batch.references)} REFERENCES — starting COPY…",
        flush=True,
    )
    t_flush = _time.monotonic()
    flush_batch(conn, batch)
    print(
        f"    [3b/6] flush complete in {_time.monotonic() - t_flush:.2f}s",
        flush=True,
    )

    # ── Pass C: derive CONFIGURES_YANG edges from the now-loaded graph ──
    t_yang = _time.monotonic()
    cy_edges = _populate_configures_yang(db)
    print(
        f"    [3c/6] derived {cy_edges} CONFIGURES_YANG edges in "
        f"{_time.monotonic() - t_yang:.2f}s",
        flush=True,
    )

    if timings:
        timings.sort(reverse=True)
        print("    Top slow specs (parse only):")
        for dt, label in timings[:10]:
            print(f"      {dt:6.2f}s  {label}")
    return totals


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
        # ``Property.enumValues`` is STRING[]; Kuzu FTS only indexes scalar
        # string columns, so the array is excluded — enum-value lookup goes
        # via Cypher (``$val IN p.enumValues``) instead.
        ("property_fts", "Property", ["name", "description", "yangPath"]),
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
    parser.add_argument("--strict", dest="strict", action="store_true", default=True,
                        help="Fail the build if graph invariants (see graph/invariants.py) "
                             "are violated. ON by default; use --no-strict to opt out "
                             "(local dev only — CI must keep strict on).")
    parser.add_argument("--no-strict", dest="strict", action="store_false",
                        help="Disable strict invariant enforcement; report violations but "
                             "still exit 0. Intended for local debugging only.")
    parser.add_argument("--no-invariants", action="store_true",
                        help="Skip the post-flush invariant audit entirely.")
    parser.add_argument("--sample", type=int, default=0, metavar="N",
                        help="Dev/CI shortcut: skip spec sync, load cached specs from "
                             "<output-dir>/spec_cache (falling back to ./build/spec_cache "
                             "if the per-output cache is empty), truncate each provider to "
                             "the first N endpoints, and run the full pipeline against a "
                             "tiny graph. Use 0 (default) for the real full build.")
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
    # Cap buffer pool at 2 GB so the build doesn't claim ~80% of system
    # memory by default (real_ladybug's autosize on a workstation can
    # easily reserve 12+ GB and starve the parser/scrapers).
    db = lb.Database(str(db_path), buffer_pool_size=2 * 1024 * 1024 * 1024)
    _apply_schema(db)

    # 2. Sync API specs
    print("\n[2/6] Refreshing API documentation...")
    if args.sample and args.sample > 0:
        # Prefer the per-output cache so --output-dir is respected; fall
        # back to the standard ./build/spec_cache populated by a prior
        # real build when the per-output cache hasn't been warmed yet.
        sample_cache = cache_dir
        if not sample_cache.is_dir() or not any(sample_cache.iterdir()):
            fallback = Path("build/spec_cache").resolve()
            if fallback.is_dir():
                sample_cache = fallback
        specs, sync_health = _load_cached_specs_sample(sample_cache, args.sample)
    else:
        specs, sync_health = _sync_specs(cache_dir)
    if not specs:
        print("⚠ No specs available — database will have no API endpoints.", file=sys.stderr)

    # Task 1 (ADR-011): resolved ingestion proof-of-life.  Runs in parallel to
    # the legacy populator; ``specs`` continues unchanged.  Failures are
    # reported but non-fatal — they document upstream Aruba spec bugs.
    from hpe_networking_central_mcp.compiler.frontend import resolve_specs

    task1 = resolve_specs(specs)
    print(
        f"  Task 1 ingestion: {len(task1.resolved)} resolved, "
        f"{len(task1.failed)} failed strict validation/resolution"
    )
    for f in task1.failed:
        print(
            f"    ⚠ {f.source}: {f.error_type}: {f.error[:200]}",
            file=sys.stderr,
        )

    # 3. Build index and populate
    print("\n[3/6] Populating API endpoints...")
    if specs:
        print(f"  Normalizing {len(specs)} specs (dedup error/object schemas)...")
        specs = [normalize_spec(s) for s in specs]
    index = OASIndex()
    index.build(specs)
    endpoint_count = _populate_endpoints(db, index, specs)

    # 3b. Populate schema subgraph (Parameter / RequestBody / Response /
    # SchemaComponent + REFERENCES edges) from each
    # source spec.  Spec source is determined from the ``_spec_source`` tag
    # set in ``_sync_specs``.
    print("\n[3b/6] Populating API schema subgraph...")
    schema_stats = _populate_schema_subgraph(db, specs)
    print(
        f"  Schema subgraph: {schema_stats['endpoints']} endpoints, "
        f"{schema_stats['parameters']} parameters, "
        f"{schema_stats['components']} components, "
        f"{schema_stats['properties']} properties, "
        f"{schema_stats['references']} REFERENCES edges"
    )

    # 3d. Post-flush invariant audit. Always runs (cheap) unless
    # --no-invariants is set; --strict converts violations from a warning
    # into a non-zero exit so CI catches stub-wins / eviction-skip
    # regressions (ADR-011) before the artifact ships.
    if not args.no_invariants:
        print("\n[3d/6] Auditing graph invariants...")
        invariant_conn = lb.Connection(db)
        try:
            violations = assert_graph_invariants(invariant_conn, strict=False)
        finally:
            del invariant_conn
        print("  " + format_report(violations))
        if violations and args.strict:
            print(
                "\n✗ --strict: refusing to ship a knowledge DB that violates invariants.",
                file=sys.stderr,
            )
            db.close()
            sys.exit(2)
    else:
        violations = []

    # 3e. Build-health snapshot: absolute current-build counters so the
    # operator can eyeball whether the graph looks the same shape as
    # last time. Deliberately no historical comparison / trend tracking.
    print("\n[3e/6] Build-health snapshot...")
    _print_build_report(db, schema_stats, violations)

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
        "schema_version": 10,
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
