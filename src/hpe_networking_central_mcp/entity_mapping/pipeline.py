"""Default pipeline assembly.

Constructs the standard mapping pipeline with the built-in mappers
in the correct priority order.  External callers can also build
custom pipelines by using MappingPipeline directly.
"""

from __future__ import annotations

from .entities import EntityRegistry, build_aruba_central_registry
from .mapper import MappingPipeline
from .pattern_rules import PatternRuleMapper
from .static_rules import StaticRuleMapper


def build_default_pipeline(
    registry: EntityRegistry | None = None,
) -> tuple[MappingPipeline, EntityRegistry]:
    """Build the standard mapping pipeline.

    Mapper priority order:
    1. StaticRuleMapper — exact name lookups (highest confidence)
    2. PatternRuleMapper — path/category context disambiguation

    Future mappers (description-based, LLM) slot in after these.

    Returns:
        (pipeline, registry) tuple.
    """
    if registry is None:
        registry = build_aruba_central_registry()

    pipeline = MappingPipeline()
    pipeline.add_mapper(StaticRuleMapper())
    pipeline.add_mapper(PatternRuleMapper())

    return pipeline, registry
