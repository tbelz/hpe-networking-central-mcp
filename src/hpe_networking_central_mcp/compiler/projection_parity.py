"""Parity checks between the legacy and compiler-produced L3 projections."""

from __future__ import annotations

import json
from typing import Any


_PARITY_QUERIES: dict[str, str] = {
    "endpoints": """
        MATCH (e:ApiEndpoint)
        RETURN e.method AS method, e.path AS path
    """,
    "parameters": """
        MATCH (e:ApiEndpoint)-[:HAS_PARAMETER]->(p:Parameter)
        RETURN e.method AS method, e.path AS path,
               p.location AS location, p.name AS name
    """,
    "request_bodies": """
        MATCH (e:ApiEndpoint)-[:HAS_REQUEST_BODY]->(body:RequestBody)
        RETURN e.method AS method, e.path AS path,
               body.content_type AS content_type
    """,
    "body_references": """
        MATCH (e:ApiEndpoint)-[:HAS_REQUEST_BODY]->(body:RequestBody)
              -[:BODY_REFERENCES]->(schema:SchemaComponent)
        RETURN e.method AS method, e.path AS path,
               body.content_type AS content_type,
               schema.component_id AS component_id
    """,
    "responses": """
        MATCH (e:ApiEndpoint)-[:HAS_RESPONSE]->(response:Response)
        RETURN e.method AS method, e.path AS path,
               response.status AS status,
               response.content_type AS content_type
    """,
    "response_references": """
        MATCH (e:ApiEndpoint)-[:HAS_RESPONSE]->(response:Response)
              -[:RESPONSE_REFERENCES]->(schema:SchemaComponent)
        RETURN e.method AS method, e.path AS path,
               response.status AS status,
               response.content_type AS content_type,
               schema.component_id AS component_id
    """,
    "schema_components": """
        MATCH (schema:SchemaComponent)
        RETURN schema.component_id AS component_id
    """,
    "properties": """
        MATCH (schema:SchemaComponent)-[:HAS_PROPERTY]->(prop:Property)
        RETURN schema.component_id AS component_id, prop.name AS property
    """,
    "property_types": """
        MATCH (schema:SchemaComponent)-[:HAS_PROPERTY]->(prop:Property)
              -[:PROPERTY_OF_TYPE]->(target:SchemaComponent)
        RETURN schema.component_id AS component_id,
               prop.name AS property,
               target.component_id AS target_component_id
    """,
    "composition": """
        MATCH (source:SchemaComponent)-[edge:COMPOSED_OF]->(target:SchemaComponent)
        RETURN source.component_id AS component_id,
               edge.kind AS kind,
               target.component_id AS target_component_id
    """,
    "value_schemas": """
        MATCH (source:SchemaComponent)-[:HAS_VALUE_SCHEMA]->(target:SchemaComponent)
        RETURN source.component_id AS component_id,
               target.component_id AS target_component_id
    """,
    "yang_paths": """
        MATCH (yang:YangPath)
        RETURN yang.yangPath AS yang_path
    """,
    "property_yang": """
        MATCH (schema:SchemaComponent)-[:HAS_PROPERTY]->(prop:Property)
              -[:PROPERTY_AT_YANG]->(yang:YangPath)
        RETURN schema.component_id AS component_id,
               prop.name AS property,
               yang.yangPath AS yang_path
    """,
    "configures_yang": """
        MATCH (e:ApiEndpoint)-[:CONFIGURES_YANG]->(yang:YangPath)
        RETURN e.method AS method, e.path AS path,
               yang.yangPath AS yang_path
    """,
    "cli_commands": """
        MATCH (e:ApiEndpoint)-[:HAS_CLI_COMMAND]->(command:CliCommand)
        RETURN e.method AS method, e.path AS path,
               command.commandName AS command_name
    """,
}


def compute_projection_parity(
    legacy_conn,
    compiler_conn,
    *,
    sample_limit: int = 25,
) -> dict[str, Any]:
    """Compare legacy and compiler L3 projections using agent-facing keys."""
    checks: dict[str, Any] = {}
    total_legacy = 0
    total_missing = 0
    total_compiler = 0
    for name, query in _PARITY_QUERIES.items():
        legacy = _row_set(legacy_conn, query)
        compiler = _row_set(compiler_conn, query)
        missing_keys = sorted(set(legacy) - set(compiler))
        extra_keys = sorted(set(compiler) - set(legacy))
        total_legacy += len(legacy)
        total_compiler += len(compiler)
        total_missing += len(missing_keys)
        checks[name] = {
            "legacy_count": len(legacy),
            "compiler_count": len(compiler),
            "legacy_missing_count": len(missing_keys),
            "compiler_extra_count": len(extra_keys),
            "legacy_coverage_ratio": _ratio(len(legacy) - len(missing_keys), len(legacy)),
            "legacy_missing_samples": [
                legacy[key] for key in missing_keys[:sample_limit]
            ],
            "compiler_extra_samples": [
                compiler[key] for key in extra_keys[:sample_limit]
            ],
        }
    return {
        "enabled": True,
        "all_legacy_covered": total_missing == 0,
        "total_legacy_signatures": total_legacy,
        "total_compiler_signatures": total_compiler,
        "total_legacy_missing": total_missing,
        "overall_legacy_coverage_ratio": _ratio(total_legacy - total_missing, total_legacy),
        "checks": checks,
    }


def format_projection_parity_report(report: dict[str, Any]) -> str:
    """Return a compact build-log summary for compiler projection parity."""
    if report.get("all_legacy_covered"):
        return (
            "✓ compiler projection covers all legacy API graph signatures "
            f"({report.get('total_legacy_signatures', 0)} checked)"
        )
    lines = [
        "⚠ compiler projection parity gaps: "
        f"{report.get('total_legacy_missing', 0)} missing legacy signatures "
        f"across {report.get('total_legacy_signatures', 0)} checked"
    ]
    for name, check in report.get("checks", {}).items():
        missing = check.get("legacy_missing_count", 0)
        if missing <= 0:
            continue
        lines.append(
            f"  • {name}: {missing}/{check.get('legacy_count', 0)} legacy signatures missing"
        )
        for row in check.get("legacy_missing_samples", [])[:3]:
            lines.append(f"      {json.dumps(row, default=str)}")
    return "\n".join(lines)


def _row_set(conn, query: str) -> dict[str, dict[str, Any]]:
    rows = list(conn.execute(query).rows_as_dict())
    result: dict[str, dict[str, Any]] = {}
    for row in rows:
        normalized = {key: _normalize(value) for key, value in row.items()}
        result[_key(normalized)] = normalized
    return result


def _key(row: dict[str, Any]) -> str:
    return json.dumps(row, sort_keys=True, separators=(",", ":"))


def _normalize(value: Any) -> Any:
    if isinstance(value, list):
        return [_normalize(item) for item in value]
    if value is None:
        return ""
    return value


def _ratio(count: int, total: int) -> float:
    if total <= 0:
        return 1.0
    return round(count / total, 4)
