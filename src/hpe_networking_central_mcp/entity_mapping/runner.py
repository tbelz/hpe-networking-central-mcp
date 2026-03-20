"""Runner — executes the mapping pipeline over an OASIndex and collects metrics.

This is the main entry point for running entity mapping as part of the
build_knowledge_db pipeline.  It processes every parameter of every endpoint,
runs it through the mapping pipeline, and produces a MappingReport with
detailed metrics and diagnostics.
"""

from __future__ import annotations

import json
from collections import Counter, defaultdict
from dataclasses import dataclass, field
from pathlib import Path

from ..oas_index import OASIndex
from .entities import EntityRegistry
from .mapper import Confidence, MappingPipeline, MappingResult, ParamContext
from .pipeline import build_default_pipeline


@dataclass
class MappingReport:
    """Aggregated metrics from a mapping run.

    Captures everything needed to assess quality and find gaps.
    """
    # All individual mapping results
    results: list[MappingResult] = field(default_factory=list)

    # Aggregate counters
    total_params: int = 0
    mapped_params: int = 0
    unmapped_params: int = 0

    # By confidence level
    by_confidence: dict[str, int] = field(default_factory=dict)

    # By mapper
    by_mapper: dict[str, int] = field(default_factory=dict)

    # By entity
    by_entity: dict[str, int] = field(default_factory=dict)

    # Coverage: unique param names that got mapped vs not
    mapped_param_names: set[str] = field(default_factory=set)
    unmapped_param_names: set[str] = field(default_factory=set)

    # Per-entity field coverage
    entity_field_counts: dict[str, int] = field(default_factory=dict)

    # Unmapped details for gap analysis
    unmapped_details: list[dict] = field(default_factory=list)

    # Endpoint coverage
    total_endpoints: int = 0
    endpoints_with_all_mapped: int = 0
    endpoints_with_some_mapped: int = 0
    endpoints_with_none_mapped: int = 0

    def coverage_pct(self) -> float:
        """Percentage of parameter usages that were mapped."""
        if self.total_params == 0:
            return 0.0
        return (self.mapped_params / self.total_params) * 100

    def unique_coverage_pct(self) -> float:
        """Percentage of unique param names that were mapped."""
        total = len(self.mapped_param_names) + len(self.unmapped_param_names)
        if total == 0:
            return 0.0
        return (len(self.mapped_param_names) / total) * 100

    def summary_text(self) -> str:
        """Human-readable summary."""
        lines = [
            "=" * 70,
            "ENTITY MAPPING REPORT",
            "=" * 70,
            "",
            f"Total endpoints processed:   {self.total_endpoints}",
            f"Total parameter usages:      {self.total_params}",
            f"Mapped:                      {self.mapped_params} ({self.coverage_pct():.1f}%)",
            f"Unmapped:                    {self.unmapped_params}",
            "",
            "── Unique Parameter Names ──",
            f"  Mapped:   {len(self.mapped_param_names)}",
            f"  Unmapped: {len(self.unmapped_param_names)}",
            f"  Coverage: {self.unique_coverage_pct():.1f}%",
            "",
            "── By Confidence ──",
        ]
        for conf in ["exact", "high", "medium", "low", "unmapped"]:
            count = self.by_confidence.get(conf, 0)
            lines.append(f"  {conf:10s}: {count}")

        lines.append("")
        lines.append("── By Mapper ──")
        for mapper, count in sorted(self.by_mapper.items(), key=lambda x: -x[1]):
            lines.append(f"  {mapper:20s}: {count}")

        lines.append("")
        lines.append("── By Entity ──")
        for entity, count in sorted(self.by_entity.items(), key=lambda x: -x[1]):
            lines.append(f"  {entity:20s}: {count}")

        lines.append("")
        lines.append("── Endpoint Coverage ──")
        lines.append(f"  All params mapped:   {self.endpoints_with_all_mapped}")
        lines.append(f"  Some params mapped:  {self.endpoints_with_some_mapped}")
        lines.append(f"  No params mapped:    {self.endpoints_with_none_mapped}")

        if self.unmapped_param_names:
            lines.append("")
            lines.append("── Unmapped Parameter Names ──")
            unmapped_counts = Counter()
            for r in self.results:
                if not r.is_mapped:
                    unmapped_counts[r.param_name] += 1
            for name, count in unmapped_counts.most_common(30):
                lines.append(f"  {name:40s}  (x{count})")

        lines.append("")
        lines.append("── Entity.Field Coverage ──")
        for key, count in sorted(self.entity_field_counts.items(), key=lambda x: -x[1]):
            lines.append(f"  {key:30s}: {count}")

        return "\n".join(lines)

    def to_json(self) -> dict:
        """Serializable summary for storage."""
        return {
            "total_endpoints": self.total_endpoints,
            "total_params": self.total_params,
            "mapped_params": self.mapped_params,
            "unmapped_params": self.unmapped_params,
            "coverage_pct": round(self.coverage_pct(), 2),
            "unique_coverage_pct": round(self.unique_coverage_pct(), 2),
            "by_confidence": self.by_confidence,
            "by_mapper": self.by_mapper,
            "by_entity": self.by_entity,
            "endpoint_coverage": {
                "all_mapped": self.endpoints_with_all_mapped,
                "some_mapped": self.endpoints_with_some_mapped,
                "none_mapped": self.endpoints_with_none_mapped,
            },
            "mapped_param_names": sorted(self.mapped_param_names),
            "unmapped_param_names": sorted(self.unmapped_param_names),
            "entity_field_counts": self.entity_field_counts,
        }


# ── Infrastructure params that are intentionally not entity-related ───
INFRA_PARAMS = frozenset({
    # Pagination / cursor
    "limit", "offset", "next",
    # Sorting / filtering
    "sort", "filter", "search", "select", "filter-tags",
    # Config view / rendering
    "deployment",
    # Time range
    "query-time", "start-query-time", "end-query-time",
    "start-at", "end-at", "start-time", "end-time", "time-at", "timestamp",
    # Context / scoping
    "context-type", "context-identifier",
    "suggest-name", "group-by", "link-tag",
    "ip-version-vlan-client-address",
    # HTTP headers
    "Content-Type", "If-Match", "Hpe-workspace-id",
    # Output format / flags
    "dry-run", "force", "all", "export-type", "raster",
    "count", "include-omnipresent", "unredacted",
    "with-location", "history", "latest-location-per-client",
    # Monitoring-specific filter params
    "band-selected", "floor-selected", "signal-cutoff",
    "is-channel-overlap", "uplink",
    # Heatmap / reporting filters
    "report-type", "kpi-widget",
    # Auth / infrastructure
    "upn", "jobtype", "network", "client-id",
    "type",  # generic type filter, not an entity
})


def run_mapping(
    index: OASIndex,
    pipeline: MappingPipeline | None = None,
    registry: EntityRegistry | None = None,
    *,
    skip_infra: bool = True,
) -> MappingReport:
    """Run the entity mapping pipeline over all endpoints in the OAS index.

    Args:
        index: The built OASIndex with all parsed endpoints.
        pipeline: Mapping pipeline to use (built from defaults if None).
        registry: Entity registry (built from defaults if None).
        skip_infra: If True, skip purely infrastructure params (pagination, etc.)
                    from the unmapped count.

    Returns:
        MappingReport with detailed metrics.
    """
    if pipeline is None and registry is None:
        pipeline, registry = build_default_pipeline(None)
    elif pipeline is None:
        pipeline, _ = build_default_pipeline(registry)
    elif registry is None:
        _, registry = build_default_pipeline(None)

    report = MappingReport()
    report.total_endpoints = index.total_endpoints

    # Track per-endpoint mapping status
    endpoint_mapped: dict[str, list[bool]] = defaultdict(list)

    for entry in index._entries:  # noqa: SLF001
        endpoint_id = f"{entry.method}:{entry.path}"

        for param in entry.parameters:
            # Skip infra params if requested
            if skip_infra and param.name in INFRA_PARAMS:
                continue

            ctx = ParamContext(
                param_name=param.name,
                param_location=param.location,
                param_description=param.description,
                param_schema=param.schema if param.schema else {},
                endpoint_method=entry.method,
                endpoint_path=entry.path,
                endpoint_summary=entry.summary,
                endpoint_description=entry.description,
                endpoint_category=entry.category,
                endpoint_tags=entry.tags,
            )

            result = pipeline.map_param(ctx, registry)
            result = MappingResult(
                param_name=result.param_name,
                param_location=result.param_location,
                entity_name=result.entity_name,
                field_name=result.field_name,
                confidence=result.confidence,
                mapper_name=result.mapper_name,
                reason=result.reason,
                endpoint_id=endpoint_id,
            )

            report.results.append(result)
            report.total_params += 1

            if result.is_mapped:
                report.mapped_params += 1
                report.mapped_param_names.add(result.param_name)
                report.by_entity[result.entity_name] = report.by_entity.get(result.entity_name, 0) + 1
                efk = result.entity_field_key
                if efk:
                    report.entity_field_counts[efk] = report.entity_field_counts.get(efk, 0) + 1
                endpoint_mapped[endpoint_id].append(True)
            else:
                report.unmapped_params += 1
                report.unmapped_param_names.add(result.param_name)
                endpoint_mapped[endpoint_id].append(False)
                report.unmapped_details.append({
                    "param": result.param_name,
                    "location": result.param_location,
                    "endpoint": endpoint_id,
                    "category": entry.category,
                    "description": param.description[:120] if param.description else "",
                })

            conf_key = result.confidence.value
            report.by_confidence[conf_key] = report.by_confidence.get(conf_key, 0) + 1

            if result.mapper_name:
                report.by_mapper[result.mapper_name] = report.by_mapper.get(result.mapper_name, 0) + 1

    # Endpoint-level coverage
    for eid, statuses in endpoint_mapped.items():
        if all(statuses):
            report.endpoints_with_all_mapped += 1
        elif any(statuses):
            report.endpoints_with_some_mapped += 1
        else:
            report.endpoints_with_none_mapped += 1

    # Also count endpoints that had zero (non-infra) params
    endpoints_with_params = len(endpoint_mapped)
    endpoints_without_params = report.total_endpoints - endpoints_with_params
    report.endpoints_with_all_mapped += endpoints_without_params

    # Remove params that appear in both mapped and unmapped (can happen
    # if a param maps in one context but not another)
    report.unmapped_param_names -= report.mapped_param_names

    return report
