"""Opt-in MCP tools for ADR-011 compiler graph artifacts."""

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Any

import real_ladybug as lb
import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..compiler.detail_reader import (
    ProjectionDetailError,
    load_projection_detail,
)
from ..compiler.traversal_reader import load_endpoint_context, load_schema_context
from ..compiler.traversal_report import load_compiler_traversal_report
from ..config import Settings

logger = structlog.get_logger("tools.compiler")

_DB_BUFFER_POOL_SIZE = 256 * 1024 * 1024
_DEFAULT_RESPONSE_BYTES = 200_000


def register_compiler_tools(mcp, settings: Settings) -> None:
    """Register opt-in compiler-backed API discovery tools."""

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def find_api_endpoints(
        query: str = "",
        method: str = "",
        path_contains: str = "",
        operation_id: str = "",
        limit: int = 20,
    ) -> str:
        """Find compiler-projected API endpoints without writing Cypher.

        Filters are optional and combined. Returns bounded endpoint
        candidates with method, path, summary, operationId, tags/category,
        and deprecation status.
        """
        compiler_db = _open_compiler_db(settings)
        try:
            conn = lb.Connection(compiler_db)
            rows = _find_endpoint_rows(
                conn,
                query=query,
                method=method,
                path_contains=path_contains,
                operation_id=operation_id,
                limit=limit,
            )
            return _json_response({"total": len(rows), "endpoints": rows})
        except ToolError:
            raise
        except Exception as exc:
            raise ToolError(f"Compiler endpoint search failed: {exc}") from exc
        finally:
            compiler_db.close()

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def get_api_endpoint_context(
        method: str,
        path: str,
        include_raw: bool = False,
    ) -> str:
        """Return deterministic compiler context for one endpoint.

        Includes endpoint, parameters, request bodies, responses, YANG/CLI
        links, and provenance. Raw OpenAPI payloads are opt-in.
        """
        _require_artifacts(settings, include_ast=True)
        try:
            context = load_endpoint_context(
                compiler_db_path=settings.compiler_db_path,
                ast_db_path=settings.compiler_ast_db_path,
                method=method,
                path=path,
                include_raw=include_raw,
                buffer_pool_size=_DB_BUFFER_POOL_SIZE,
            )
            return _json_response(context)
        except ProjectionDetailError as exc:
            raise ToolError(str(exc)) from exc
        except Exception as exc:
            raise ToolError(f"Compiler endpoint context failed: {exc}") from exc

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def get_api_schema_context(
        component_id: str,
        include_raw: bool = False,
    ) -> str:
        """Return deterministic compiler context for one SchemaComponent.

        Includes properties, target schemas, array item schemas, composition,
        value schemas, references, and provenance. Raw payloads are opt-in.
        """
        _require_artifacts(settings, include_ast=True)
        try:
            context = load_schema_context(
                compiler_db_path=settings.compiler_db_path,
                ast_db_path=settings.compiler_ast_db_path,
                component_id=component_id,
                include_raw=include_raw,
                buffer_pool_size=_DB_BUFFER_POOL_SIZE,
            )
            return _json_response(context)
        except ProjectionDetailError as exc:
            raise ToolError(str(exc)) from exc
        except Exception as exc:
            raise ToolError(f"Compiler schema context failed: {exc}") from exc

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def get_openapi_source_detail(table_name: str, row_id: str) -> str:
        """Resolve a compiler projection row back to source OpenAPI detail.

        Returns the projection row, provenance, semantic summary, AST node
        metadata, and raw OpenAPI object or scalar for that row.
        """
        _require_artifacts(settings, include_ast=True)
        try:
            detail = load_projection_detail(
                compiler_db_path=settings.compiler_db_path,
                ast_db_path=settings.compiler_ast_db_path,
                table_name=table_name,
                row_id=row_id,
                buffer_pool_size=_DB_BUFFER_POOL_SIZE,
            )
            return _json_response(detail)
        except ProjectionDetailError as exc:
            raise ToolError(str(exc)) from exc
        except Exception as exc:
            raise ToolError(f"Compiler source detail failed: {exc}") from exc

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def get_compiler_graph_health(
        endpoint_limit: int = 100,
        schema_limit: int = 100,
    ) -> str:
        """Report compiler graph traversal health from bounded samples."""
        _require_artifacts(settings, include_ast=True)
        try:
            report = load_compiler_traversal_report(
                compiler_db_path=settings.compiler_db_path,
                ast_db_path=settings.compiler_ast_db_path,
                endpoint_limit=endpoint_limit,
                schema_limit=schema_limit,
                buffer_pool_size=_DB_BUFFER_POOL_SIZE,
            )
            return _json_response(report)
        except Exception as exc:
            raise ToolError(f"Compiler graph health failed: {exc}") from exc


def _find_endpoint_rows(
    conn,
    *,
    query: str,
    method: str,
    path_contains: str,
    operation_id: str,
    limit: int,
) -> list[dict[str, Any]]:
    limit = max(1, min(int(limit or 20), 100))
    where: list[str] = []
    params: dict[str, Any] = {"limit": limit}
    if method.strip():
        where.append("endpoint.method = $method")
        params["method"] = method.strip().upper()
    if path_contains.strip():
        where.append("endpoint.path CONTAINS $path_contains")
        params["path_contains"] = path_contains.strip()
    if operation_id.strip():
        where.append("endpoint.operationId = $operation_id")
        params["operation_id"] = operation_id.strip()
    if query.strip():
        where.append(
            "("
            "endpoint.path CONTAINS $query OR "
            "endpoint.summary CONTAINS $query OR "
            "endpoint.description CONTAINS $query OR "
            "endpoint.operationId CONTAINS $query OR "
            "endpoint.category CONTAINS $query"
            ")"
        )
        params["query"] = query.strip()
    where_clause = f"WHERE {' AND '.join(where)}" if where else ""
    rows = list(
        conn.execute(
            f"""
            MATCH (endpoint:ApiEndpoint)
            {where_clause}
            RETURN endpoint.endpoint_id AS endpoint_id,
                   endpoint.method AS method,
                   endpoint.path AS path,
                   endpoint.summary AS summary,
                   endpoint.operationId AS operationId,
                   endpoint.tags AS tags,
                   endpoint.category AS category,
                   endpoint.deprecated AS deprecated
            ORDER BY endpoint.path, endpoint.method, endpoint.endpoint_id
            LIMIT $limit
            """,
            parameters=params,
        ).rows_as_dict()
    )
    return rows


def _open_compiler_db(settings: Settings):
    _require_artifacts(settings, include_ast=False)
    try:
        return lb.Database(
            str(settings.compiler_db_path),
            buffer_pool_size=_DB_BUFFER_POOL_SIZE,
        )
    except Exception as exc:
        raise ToolError(f"Compiler DB unavailable: {exc}") from exc


def _require_artifacts(settings: Settings, *, include_ast: bool) -> None:
    missing = []
    if not _artifact_exists(settings.compiler_db_path):
        missing.append(f"compiler DB at {settings.compiler_db_path}")
    if include_ast and not _artifact_exists(settings.compiler_ast_db_path):
        missing.append(f"AST DB at {settings.compiler_ast_db_path}")
    if missing:
        raise ToolError(
            "Compiler artifacts are unavailable: "
            + ", ".join(missing)
            + ". Set MCP_COMPILER_TOOLS=true with hydrated compiler artifacts."
        )


def _artifact_exists(path: Path) -> bool:
    return path.exists()


def _json_response(payload: Any) -> str:
    cap = _env_int("MCP_COMPILER_RESPONSE_BYTES", _DEFAULT_RESPONSE_BYTES)
    body = json.dumps(payload, indent=2, default=str)
    if len(body.encode("utf-8")) <= cap:
        return body

    redacted = _redact_raw_payloads(payload)
    body = json.dumps(
        {
            "_truncated": True,
            "reason": "compiler_response_byte_cap",
            "cap_bytes": cap,
            "payload": redacted,
        },
        indent=2,
        default=str,
    )
    if len(body.encode("utf-8")) <= cap:
        return body
    return json.dumps(
        {
            "_truncated": True,
            "reason": "compiler_response_byte_cap",
            "cap_bytes": cap,
            "hint": (
                "Narrow the request, lower sample limits, or set "
                "include_raw=false for context tools."
            ),
        },
        indent=2,
    )


def _redact_raw_payloads(value: Any) -> Any:
    if isinstance(value, list):
        return [_redact_raw_payloads(item) for item in value]
    if not isinstance(value, dict):
        return value
    redacted: dict[str, Any] = {}
    for key, item in value.items():
        if key in {"raw_openapi", "rawJson", "scalarJson", "bodyJson"}:
            redacted[key] = _redaction_envelope(item)
        else:
            redacted[key] = _redact_raw_payloads(item)
    return redacted


def _redaction_envelope(value: Any) -> dict[str, Any]:
    size = len(json.dumps(value, default=str).encode("utf-8"))
    return {
        "_truncated": True,
        "size_bytes": size,
        "hint": "Raw payload omitted by MCP_COMPILER_RESPONSE_BYTES cap.",
    }


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        value = int(raw)
    except ValueError:
        return default
    return value if value > 0 else default
