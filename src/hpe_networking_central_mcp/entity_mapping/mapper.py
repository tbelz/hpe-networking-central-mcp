"""Mapper protocol and pipeline abstractions.

Defines the Mapper interface that all mapping strategies implement,
and MappingPipeline that chains them in priority order.
"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from enum import Enum
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from .entities import EntityRegistry


class Confidence(Enum):
    """Confidence level for a mapping decision."""
    EXACT = "exact"          # Unambiguous, known-correct mapping
    HIGH = "high"            # Strong signal from pattern/description
    MEDIUM = "medium"        # Plausible but could be wrong
    LOW = "low"              # Guess, needs verification
    UNMAPPED = "unmapped"    # No mapping found


@dataclass(frozen=True)
class MappingResult:
    """The result of mapping a single API parameter to a domain entity.

    Attributes:
        param_name: Original API parameter name (e.g. "serial-number").
        param_location: Where the param appears ("path", "query", "header").
        entity_name: Canonical entity name (e.g. "Device"), or "" if unmapped.
        field_name: Entity field name (e.g. "serial"), or "" if unmapped.
        confidence: How confident the mapper is in this mapping.
        mapper_name: Which mapper produced this result (for audit trail).
        reason: Human-readable explanation of why this mapping was chosen.
        endpoint_id: The endpoint this mapping applies to (method:path).
    """
    param_name: str
    param_location: str
    entity_name: str = ""
    field_name: str = ""
    confidence: Confidence = Confidence.UNMAPPED
    mapper_name: str = ""
    reason: str = ""
    endpoint_id: str = ""
    operation: str = ""

    @property
    def is_mapped(self) -> bool:
        return self.confidence != Confidence.UNMAPPED and self.entity_name != ""

    @property
    def entity_field_key(self) -> str:
        """Canonical key like 'Device.serial'."""
        if self.entity_name and self.field_name:
            return f"{self.entity_name}.{self.field_name}"
        return ""


@dataclass
class ParamContext:
    """All available context for mapping a single parameter.

    Provides the mapper with everything it might need to make a decision.
    """
    param_name: str
    param_location: str  # "path", "query", "header"
    param_description: str
    param_schema: dict
    endpoint_method: str
    endpoint_path: str
    endpoint_summary: str
    endpoint_description: str
    endpoint_category: str
    endpoint_tags: list[str] = field(default_factory=list)


class Mapper(ABC):
    """Abstract base for entity mapping strategies.

    Implementations must provide a ``name`` and a ``map_param`` method.
    The pipeline calls mappers in priority order; the first mapper that
    returns a mapped result (confidence != UNMAPPED) wins.
    """

    @property
    @abstractmethod
    def name(self) -> str:
        """Human-readable mapper name for audit trail."""
        ...

    @abstractmethod
    def map_param(self, ctx: ParamContext, registry: EntityRegistry) -> MappingResult:
        """Attempt to map a parameter to a canonical entity field.

        Args:
            ctx: Full context about the parameter and its endpoint.
            registry: The canonical entity registry to map into.

        Returns:
            MappingResult with confidence != UNMAPPED if mapping succeeded,
            or MappingResult with UNMAPPED confidence to pass to the next mapper.
        """
        ...


class MappingPipeline:
    """Chains multiple Mapper instances in priority order.

    The first mapper to return a mapped result wins.  If no mapper
    can map a parameter, an UNMAPPED result is returned.
    """

    def __init__(self, mappers: list[Mapper] | None = None) -> None:
        self._mappers: list[Mapper] = mappers or []

    def add_mapper(self, mapper: Mapper) -> None:
        self._mappers.append(mapper)

    @property
    def mappers(self) -> list[Mapper]:
        return list(self._mappers)

    def map_param(self, ctx: ParamContext, registry: EntityRegistry) -> MappingResult:
        """Run the parameter through the mapper chain.

        Returns the first successful mapping, or UNMAPPED if none match.
        """
        for mapper in self._mappers:
            result = mapper.map_param(ctx, registry)
            if result.is_mapped:
                return result

        return MappingResult(
            param_name=ctx.param_name,
            param_location=ctx.param_location,
            confidence=Confidence.UNMAPPED,
            mapper_name="pipeline",
            reason="No mapper could resolve this parameter",
            endpoint_id=f"{ctx.endpoint_method}:{ctx.endpoint_path}",
        )
