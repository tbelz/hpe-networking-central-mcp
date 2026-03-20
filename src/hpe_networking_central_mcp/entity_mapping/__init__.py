"""Entity disambiguation and mapping for API parameters.

Maps API parameter names and path variables to canonical domain entities,
enabling the knowledge graph to link endpoints that operate on the same
entities (e.g. "serial-number" and "serial" both refer to Device.serial).
"""

from .entities import Entity, EntityField, EntityRegistry
from .mapper import MappingResult, Mapper, MappingPipeline
from .static_rules import StaticRuleMapper
from .pattern_rules import PatternRuleMapper
from .llm_mapper import LLMMapper, LLMMapperConfig, LLMProvider
from .pipeline import build_default_pipeline
from .runner import run_mapping, MappingReport

__all__ = [
    "Entity",
    "EntityField",
    "EntityRegistry",
    "MappingResult",
    "Mapper",
    "MappingPipeline",
    "StaticRuleMapper",
    "PatternRuleMapper",
    "LLMMapper",
    "LLMMapperConfig",
    "LLMProvider",
    "build_default_pipeline",
    "run_mapping",
    "MappingReport",
]
