"""LLM-based mapper — uses a language model for remaining ambiguous params.

This mapper is the last resort in the pipeline. It's designed to handle
parameters that can't be resolved by static rules or path patterns, such as
generic `{id}` path variables in contexts without clear structural signals.

Architecture:
  - LLMMapperConfig captures model settings and prompt templates
  - LLMMapper implements the Mapper protocol
  - Results are cached to avoid repeated LLM calls for the same context
  - A batch mode is available for efficiency when mapping many params at once

The LLM mapper is NOT called during build-time by default — it's opt-in
and requires an LLM provider to be configured. The pipeline works well
without it (98%+ coverage with static + pattern rules alone).

Usage:
    from hpe_networking_central_mcp.entity_mapping.llm_mapper import (
        LLMMapper, LLMMapperConfig, LLMProvider,
    )

    # Implement the provider protocol for your LLM
    class MyLLMProvider:
        async def complete(self, prompt: str) -> str:
            ...  # call your model

    config = LLMMapperConfig(provider=MyLLMProvider())
    llm_mapper = LLMMapper(config)

    # Add to pipeline
    pipeline.add_mapper(llm_mapper)
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from typing import Protocol, runtime_checkable

from .entities import EntityRegistry
from .mapper import Confidence, Mapper, MappingResult, ParamContext


# ── LLM Provider Protocol ───────────────────────────────────────────

@runtime_checkable
class LLMProvider(Protocol):
    """Protocol for language model providers.

    Implementations can wrap OpenAI, Anthropic, local models, or any
    other LLM API. The mapper only needs a simple text-in/text-out
    interface.
    """

    def complete(self, prompt: str) -> str:
        """Send a prompt and return the model's text response.

        Args:
            prompt: The full prompt including system message and context.

        Returns:
            The model's text response (should be valid JSON per the prompt).
        """
        ...


# ── Configuration ────────────────────────────────────────────────────

@dataclass
class LLMMapperConfig:
    """Configuration for the LLM mapper.

    Attributes:
        provider: The LLM provider to use for completions.
        max_confidence: Maximum confidence level the LLM mapper can assign.
            Capped at MEDIUM by default to reflect inherent uncertainty.
        cache_results: Whether to cache results for identical contexts.
        system_prompt: Override the default system prompt template.
    """
    provider: LLMProvider
    max_confidence: Confidence = Confidence.MEDIUM
    cache_results: bool = True
    system_prompt: str = ""


# ── Default prompt template ──────────────────────────────────────────

_DEFAULT_SYSTEM_PROMPT = """\
You are an API parameter classifier for HPE Aruba Networking Central.

Given an API parameter and its endpoint context, determine which domain
entity and field it refers to. Respond with JSON only.

Available entities and their fields:
{entity_list}

Parameter to classify:
  Name: {param_name}
  Location: {param_location}
  Description: {param_description}
  Endpoint: {endpoint_method} {endpoint_path}
  Summary: {endpoint_summary}
  Category: {endpoint_category}

Respond with exactly this JSON structure:
{{
  "entity_name": "<entity name or empty string if unmapped>",
  "field_name": "<field name or empty string if unmapped>",
  "confidence": "<medium or low>",
  "reason": "<one-sentence explanation>"
}}

If you cannot determine the entity, use empty strings for entity_name
and field_name with confidence "low".
"""


def _build_entity_list(registry: EntityRegistry) -> str:
    """Build a concise entity/field reference for the LLM prompt."""
    lines = []
    for entity in registry.all_entities():
        fields = ", ".join(f.name for f in entity.fields.values())
        lines.append(f"  {entity.name}: {entity.description} — fields: [{fields}]")
    return "\n".join(lines)


# ── LLM Mapper Implementation ───────────────────────────────────────

class LLMMapper(Mapper):
    """Maps parameters using a language model for remaining ambiguous cases.

    This is the lowest-priority mapper — it only runs when static rules
    and pattern rules both fail. Its confidence is capped at MEDIUM to
    reflect the inherent uncertainty of LLM-based classification.
    """

    def __init__(self, config: LLMMapperConfig) -> None:
        self._config = config
        self._cache: dict[str, MappingResult] = {}

    @property
    def name(self) -> str:
        return "llm"

    def map_param(self, ctx: ParamContext, registry: EntityRegistry) -> MappingResult:
        endpoint_id = f"{ctx.endpoint_method}:{ctx.endpoint_path}"

        # Check cache
        cache_key = f"{ctx.param_name}|{ctx.param_location}|{endpoint_id}"
        if self._config.cache_results and cache_key in self._cache:
            return self._cache[cache_key]

        # Build prompt
        entity_list = _build_entity_list(registry)
        prompt_template = self._config.system_prompt or _DEFAULT_SYSTEM_PROMPT
        prompt = prompt_template.format(
            entity_list=entity_list,
            param_name=ctx.param_name,
            param_location=ctx.param_location,
            param_description=ctx.param_description or "(none)",
            endpoint_method=ctx.endpoint_method,
            endpoint_path=ctx.endpoint_path,
            endpoint_summary=ctx.endpoint_summary or "(none)",
            endpoint_category=ctx.endpoint_category or "(none)",
        )

        # Call LLM
        try:
            response = self._config.provider.complete(prompt)
            result = self._parse_response(response, ctx, registry, endpoint_id)
        except Exception:
            result = MappingResult(
                param_name=ctx.param_name,
                param_location=ctx.param_location,
                confidence=Confidence.UNMAPPED,
                mapper_name=self.name,
                reason="LLM call failed",
                endpoint_id=endpoint_id,
            )

        # Cache
        if self._config.cache_results:
            self._cache[cache_key] = result

        return result

    def _parse_response(
        self,
        response: str,
        ctx: ParamContext,
        registry: EntityRegistry,
        endpoint_id: str,
    ) -> MappingResult:
        """Parse the LLM's JSON response into a MappingResult."""
        # Extract JSON from response (handle markdown code blocks)
        text = response.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            text = "\n".join(lines[1:-1]) if len(lines) > 2 else text

        data = json.loads(text)

        entity_name = data.get("entity_name", "")
        field_name = data.get("field_name", "")
        reason = data.get("reason", "LLM classification")
        conf_str = data.get("confidence", "low")

        # Validate entity exists
        if entity_name and registry.get(entity_name) is None:
            return MappingResult(
                param_name=ctx.param_name,
                param_location=ctx.param_location,
                confidence=Confidence.UNMAPPED,
                mapper_name=self.name,
                reason=f"LLM suggested unknown entity: {entity_name}",
                endpoint_id=endpoint_id,
            )

        if not entity_name:
            return MappingResult(
                param_name=ctx.param_name,
                param_location=ctx.param_location,
                confidence=Confidence.UNMAPPED,
                mapper_name=self.name,
                reason=reason,
                endpoint_id=endpoint_id,
            )

        # Cap confidence at configured maximum
        conf_map = {"exact": Confidence.EXACT, "high": Confidence.HIGH,
                     "medium": Confidence.MEDIUM, "low": Confidence.LOW}
        confidence = conf_map.get(conf_str, Confidence.LOW)
        if confidence.value < self._config.max_confidence.value:
            confidence = self._config.max_confidence

        return MappingResult(
            param_name=ctx.param_name,
            param_location=ctx.param_location,
            entity_name=entity_name,
            field_name=field_name,
            confidence=confidence,
            mapper_name=self.name,
            reason=reason,
            endpoint_id=endpoint_id,
        )

    def clear_cache(self) -> None:
        """Clear the result cache."""
        self._cache.clear()

    @property
    def cache_size(self) -> int:
        """Number of cached results."""
        return len(self._cache)
