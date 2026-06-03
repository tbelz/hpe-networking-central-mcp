"""Compact quality metrics for the Task 3 semantic overlay."""

from __future__ import annotations

from collections import Counter
from typing import Any

from .semantic_builder import SemanticGraph


def compute_semantic_metrics(graphs: list[SemanticGraph]) -> dict[str, Any]:
    """Return catalog-level coverage counters for L2 semantic graphs."""
    node_kind_counts: Counter[str] = Counter()
    edge_kind_counts: Counter[str] = Counter()
    node_ids_with_provenance: set[str] = set()

    endpoint_ids: set[str] = set()
    endpoint_accepts_schema: set[str] = set()
    endpoint_returns_schema: set[str] = set()
    endpoint_configures_yang: set[str] = set()
    endpoint_has_parameter: set[str] = set()
    endpoint_has_request_body: set[str] = set()
    endpoint_has_response: set[str] = set()
    request_body_ids: set[str] = set()
    request_bodies_referencing_schema: set[str] = set()
    response_ids: set[str] = set()
    responses_referencing_schema: set[str] = set()
    schema_ids: set[str] = set()
    schemas_with_properties: set[str] = set()
    property_ids: set[str] = set()
    properties_at_yang: set[str] = set()

    for graph in graphs:
        node_by_id = {node.semantic_id: node for node in graph.nodes}
        for node in graph.nodes:
            node_kind_counts[node.kind] += 1
            if node.kind == "ApiEndpoint":
                endpoint_ids.add(node.semantic_id)
            elif node.kind == "RequestBody":
                request_body_ids.add(node.semantic_id)
            elif node.kind == "Response":
                response_ids.add(node.semantic_id)
            elif node.kind == "SchemaComponent":
                schema_ids.add(node.semantic_id)
            elif node.kind == "Property":
                property_ids.add(node.semantic_id)
        for edge in graph.derived_edges:
            node_ids_with_provenance.add(edge.semantic_id)
        for edge in graph.edges:
            edge_kind_counts[edge.kind] += 1
            source = node_by_id.get(edge.source_id)
            target = node_by_id.get(edge.target_id)
            if source is None or target is None:
                continue
            if source.kind == "ApiEndpoint":
                if edge.kind == "ACCEPTS_SCHEMA":
                    endpoint_accepts_schema.add(source.semantic_id)
                elif edge.kind == "RETURNS_SCHEMA":
                    endpoint_returns_schema.add(source.semantic_id)
                elif edge.kind == "CONFIGURES_YANG":
                    endpoint_configures_yang.add(source.semantic_id)
                elif edge.kind == "HAS_PARAMETER":
                    endpoint_has_parameter.add(source.semantic_id)
                elif edge.kind == "HAS_REQUEST_BODY":
                    endpoint_has_request_body.add(source.semantic_id)
                elif edge.kind == "HAS_RESPONSE":
                    endpoint_has_response.add(source.semantic_id)
            elif source.kind == "RequestBody" and edge.kind == "BODY_REFERENCES":
                request_bodies_referencing_schema.add(source.semantic_id)
            elif source.kind == "Response" and edge.kind == "RESPONSE_REFERENCES":
                responses_referencing_schema.add(source.semantic_id)
            elif source.kind == "SchemaComponent" and edge.kind == "HAS_PROPERTY":
                schemas_with_properties.add(source.semantic_id)
            elif source.kind == "Property" and edge.kind == "PROPERTY_AT_YANG":
                properties_at_yang.add(source.semantic_id)

    total_nodes = sum(node_kind_counts.values())
    total_edges = sum(edge_kind_counts.values())
    endpoint_count = len(endpoint_ids)
    schema_count = len(schema_ids)
    property_count = len(property_ids)

    return {
        "node_kind_counts": dict(sorted(node_kind_counts.items())),
        "edge_kind_counts": dict(sorted(edge_kind_counts.items())),
        "total_nodes": total_nodes,
        "total_edges": total_edges,
        "coverage": {
            "semantic_nodes_with_ast_provenance": {
                "count": len(node_ids_with_provenance),
                "total": total_nodes,
                "ratio": _ratio(len(node_ids_with_provenance), total_nodes),
            },
            "endpoints_with_parameters": {
                "count": len(endpoint_has_parameter),
                "total": endpoint_count,
                "ratio": _ratio(len(endpoint_has_parameter), endpoint_count),
            },
            "endpoints_with_request_bodies": {
                "count": len(endpoint_has_request_body),
                "total": endpoint_count,
                "ratio": _ratio(len(endpoint_has_request_body), endpoint_count),
            },
            "endpoints_with_responses": {
                "count": len(endpoint_has_response),
                "total": endpoint_count,
                "ratio": _ratio(len(endpoint_has_response), endpoint_count),
            },
            "endpoints_accepting_schema": {
                "count": len(endpoint_accepts_schema),
                "total": endpoint_count,
                "ratio": _ratio(len(endpoint_accepts_schema), endpoint_count),
            },
            "endpoints_returning_schema": {
                "count": len(endpoint_returns_schema),
                "total": endpoint_count,
                "ratio": _ratio(len(endpoint_returns_schema), endpoint_count),
            },
            "endpoints_with_any_schema_edge": {
                "count": len(endpoint_accepts_schema | endpoint_returns_schema),
                "total": endpoint_count,
                "ratio": _ratio(len(endpoint_accepts_schema | endpoint_returns_schema), endpoint_count),
            },
            "endpoints_configuring_yang": {
                "count": len(endpoint_configures_yang),
                "total": endpoint_count,
                "ratio": _ratio(len(endpoint_configures_yang), endpoint_count),
            },
            "request_bodies_referencing_schema": {
                "count": len(request_bodies_referencing_schema),
                "total": len(request_body_ids),
                "ratio": _ratio(len(request_bodies_referencing_schema), len(request_body_ids)),
            },
            "responses_referencing_schema": {
                "count": len(responses_referencing_schema),
                "total": len(response_ids),
                "ratio": _ratio(len(responses_referencing_schema), len(response_ids)),
            },
            "schemas_with_properties": {
                "count": len(schemas_with_properties),
                "total": schema_count,
                "ratio": _ratio(len(schemas_with_properties), schema_count),
            },
            "properties_at_yang": {
                "count": len(properties_at_yang),
                "total": property_count,
                "ratio": _ratio(len(properties_at_yang), property_count),
            },
        },
    }


def _ratio(count: int, total: int) -> float:
    if total <= 0:
        return 0.0
    return round(count / total, 4)
