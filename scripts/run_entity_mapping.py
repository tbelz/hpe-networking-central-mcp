#!/usr/bin/env python3
"""Run entity mapping analysis and produce a detailed report.

Standalone script for iterating on mapping quality without rebuilding
the full knowledge DB.  Outputs metrics, gap analysis, and coverage
visualizations to stdout.

Usage:
    python scripts/run_entity_mapping.py [--json] [--output-dir ./mapping_report]
"""

from __future__ import annotations

import argparse
import json
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Ensure the package is importable
sys.path.insert(0, str(Path(__file__).resolve().parent.parent / "src"))

from hpe_networking_central_mcp.entity_mapping import (  # noqa: E402
    MappingReport,
    build_default_pipeline,
    run_mapping,
)
from hpe_networking_central_mcp.entity_mapping.runner import INFRA_PARAMS  # noqa: E402
from hpe_networking_central_mcp.oas_index import OASIndex  # noqa: E402
from hpe_networking_central_mcp.oas_scraper import ReadMeSpecProvider  # noqa: E402

try:
    from hpe_networking_central_mcp.glp_spec_provider import GreenLakeSpecProvider
    _HAS_GLP = True
except ImportError:
    _HAS_GLP = False


def _scrape_specs(cache_dir: Path) -> list[dict]:
    specs: list[dict] = []
    print("Scraping Central API docs...", file=sys.stderr)
    provider = ReadMeSpecProvider()
    central = provider.fetch_specs(cache_dir=cache_dir / "central", ttl=86400)
    specs.extend(central)
    print(f"  Central: {len(central)} specs", file=sys.stderr)

    if _HAS_GLP:
        print("Scraping GreenLake API docs...", file=sys.stderr)
        glp = GreenLakeSpecProvider()
        glp_specs = glp.fetch_specs(cache_dir=cache_dir / "glp", ttl=86400)
        specs.extend(glp_specs)
        print(f"  GreenLake: {len(glp_specs)} specs", file=sys.stderr)

    return specs


def _print_bar(label: str, count: int, total: int, width: int = 40) -> str:
    """ASCII bar chart helper."""
    if total == 0:
        pct = 0.0
    else:
        pct = count / total
    filled = int(width * pct)
    bar = "█" * filled + "░" * (width - filled)
    return f"  {label:30s} {bar} {count:5d} ({pct*100:5.1f}%)"


def _generate_detailed_report(report: MappingReport) -> str:
    """Generate a detailed text report with visualizations."""
    lines = []
    lines.append(report.summary_text())

    # ── Coverage bar chart by entity ──
    lines.append("")
    lines.append("=" * 70)
    lines.append("ENTITY DISTRIBUTION (mapped parameter usages)")
    lines.append("=" * 70)
    total_mapped = report.mapped_params
    for entity, count in sorted(report.by_entity.items(), key=lambda x: -x[1]):
        lines.append(_print_bar(entity, count, total_mapped))

    # ── Confidence distribution ──
    lines.append("")
    lines.append("=" * 70)
    lines.append("CONFIDENCE DISTRIBUTION")
    lines.append("=" * 70)
    total = report.total_params
    for conf in ["exact", "high", "medium", "low", "unmapped"]:
        count = report.by_confidence.get(conf, 0)
        lines.append(_print_bar(conf, count, total))

    # ── Per-category coverage ──
    lines.append("")
    lines.append("=" * 70)
    lines.append("COVERAGE BY API CATEGORY")
    lines.append("=" * 70)
    cat_total: dict[str, int] = Counter()
    cat_mapped: dict[str, int] = Counter()
    # Build an endpoint -> category map from unmapped_details (fallback source).
    endpoint_category: dict[str, str] = {}
    for detail in report.unmapped_details:
        endpoint_id = detail.get("endpoint")
        if not endpoint_id:
            continue
        endpoint_category[endpoint_id] = detail.get("category", "Unknown")
    for r in report.results:
        cat = endpoint_category.get(r.endpoint_id, "Unknown")
        cat_total[cat] += 1
        if r.is_mapped:
            cat_mapped[cat] += 1

    for cat in sorted(cat_total, key=lambda c: -cat_total[c]):
        if cat == "Unknown":
            continue
        t = cat_total[cat]
        m = cat_mapped[cat]
        pct = (m / t * 100) if t > 0 else 0
        lines.append(f"  {cat:40s} {m:4d}/{t:4d}  ({pct:5.1f}%)")

    # ── Unmapped parameter analysis ──
    lines.append("")
    lines.append("=" * 70)
    lines.append("UNMAPPED PARAMETERS — GAP ANALYSIS")
    lines.append("=" * 70)
    unmapped_by_name = Counter()
    unmapped_examples: dict[str, list[str]] = defaultdict(list)
    for r in report.results:
        if not r.is_mapped:
            unmapped_by_name[r.param_name] += 1
            if len(unmapped_examples[r.param_name]) < 3:
                unmapped_examples[r.param_name].append(r.endpoint_id)

    lines.append(f"\n  Total unmapped param names: {len(unmapped_by_name)}")
    lines.append(f"  Total unmapped usages:      {report.unmapped_params}")
    lines.append("")
    for name, count in unmapped_by_name.most_common():
        is_infra = "  [INFRA]" if name in INFRA_PARAMS else ""
        lines.append(f"  {name:40s}  x{count:4d}{is_infra}")
        for ex in unmapped_examples[name]:
            lines.append(f"    → {ex}")

    # ── Entity-field heatmap ──
    lines.append("")
    lines.append("=" * 70)
    lines.append("ENTITY.FIELD MAPPING HEATMAP")
    lines.append("=" * 70)
    max_ef = max(report.entity_field_counts.values()) if report.entity_field_counts else 1
    for key, count in sorted(report.entity_field_counts.items(), key=lambda x: -x[1]):
        lines.append(_print_bar(key, count, max_ef))

    return "\n".join(lines)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run entity mapping analysis")
    parser.add_argument("--json", action="store_true", help="Also output JSON report")
    parser.add_argument("--output-dir", type=Path, help="Save reports to this directory")
    args = parser.parse_args()

    cache_dir = Path("/tmp/spec_cache_analysis")
    cache_dir.mkdir(exist_ok=True)

    specs = _scrape_specs(cache_dir)
    print(f"\nBuilding index from {len(specs)} specs...", file=sys.stderr)

    index = OASIndex()
    index.build(specs)
    print(f"Total endpoints: {index.total_endpoints}", file=sys.stderr)

    print("Running entity mapping pipeline...", file=sys.stderr)
    pipeline, registry = build_default_pipeline()
    report = run_mapping(index, pipeline, registry, skip_infra=False)

    # Generate detailed report
    detailed = _generate_detailed_report(report)
    print(detailed)

    if args.output_dir:
        args.output_dir.mkdir(parents=True, exist_ok=True)
        (args.output_dir / "mapping_report.txt").write_text(detailed, encoding="utf-8")
        if args.json:
            (args.output_dir / "mapping_report.json").write_text(
                json.dumps(report.to_json(), indent=2), encoding="utf-8"
            )
        print(f"\nReports saved to {args.output_dir}", file=sys.stderr)


if __name__ == "__main__":
    main()
