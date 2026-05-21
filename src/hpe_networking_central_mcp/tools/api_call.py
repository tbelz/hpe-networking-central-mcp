"""Direct API call tools — authenticated access to Central and GreenLake APIs."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Literal

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..central_client import BaseAPIClient, CentralAPIError, CentralClient, GreenLakeClient
from ..config import Settings
from .api_call_validation import (
    format_validation_error,
    format_validation_warnings,
    validate_call,
)

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.api_call")

_WRITE_METHODS = {"POST", "PATCH", "PUT", "DELETE"}


def _api_error_hint(exc: CentralAPIError, path: str, method: str) -> str:
    """Build a contextual hint for common API errors."""
    msg = (exc.message or "").lower()

    if "restricted" in msg or "scope" in msg:
        return (
            "\n\nHint: Central config APIs require a valid scopeId and scopeType. "
            "Use `query_graph` to find scopeId values from Site, SiteCollection, "
            "or Device nodes. See `graph://schema` for canned Cypher patterns "
            "covering required parameters."
        )

    if exc.status_code == 404:
        return (
            "\n\nHint: Endpoint not found. Read the `api://endpoint-catalog` "
            "resource for the full list of valid METHOD /path combinations. "
            "Guessing paths without consulting the catalog has a near-zero "
            "chance of success."
        )

    if exc.status_code == 400:
        return (
            "\n\nHint: Inspect required parameters and body fields with "
            "`query_graph` — see `graph://schema` for canned patterns "
            "(`Required parameters for a specific endpoint`, `Required "
            "top-level fields of an endpoint's request body`)."
        )

    return ""


def _make_api_call(
    client: BaseAPIClient,
    platform: str,
    path: str,
    method: str,
    query_params: dict[str, str] | None,
    body: dict | None,
    warning_header: str = "",
) -> str:
    """Shared implementation for authenticated API calls."""
    clean_path = path.strip().lstrip("/")
    if not clean_path:
        raise ToolError("Path cannot be empty.")
    if ".." in clean_path:
        raise ToolError("Path must not contain '..'.")
    if body and method == "GET":
        raise ToolError("Cannot send a request body with GET. Use POST, PATCH, or PUT.")

    try:
        logger.info("api_call_start", platform=platform, method=method, path=clean_path, params=query_params)
        result = client._request(method, clean_path, params=query_params, json_body=body)
        logger.info("api_call_done", platform=platform, method=method, path=clean_path)
        return warning_header + json.dumps(result, indent=2)
    except CentralAPIError as e:
        logger.error(
            "api_call_failed",
            platform=platform,
            method=method,
            path=clean_path,
            status=e.status_code,
            error_code=e.error_code,
        )
        hint = _api_error_hint(e, clean_path, method)
        raise ToolError(
            f"API call failed [{e.status_code}]: {e.message}{hint}"
            + (f" (debugId: {e.debug_id})" if e.debug_id else "")
        )
    except Exception as e:
        logger.error("api_call_failed", platform=platform, method=method, path=clean_path, error=str(e))
        raise ToolError(f"API call failed: {e}")


def register_api_call_tools(
    mcp,
    settings: Settings,
    client: CentralClient,
    graph_manager: "GraphManager | None" = None,
):
    """Register the Central direct API call tool with the MCP server."""

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            idempotentHint=False,
            openWorldHint=True,
        ),
    )
    def call_central_api(
        path: str,
        method: Literal["GET", "POST", "PATCH", "PUT", "DELETE"] = "GET",
        query_params: dict[str, str] | None = None,
        body: dict | None = None,
    ) -> str:
        """Make an authenticated request to any Central API endpoint.

        For single-item lookups and one-off mutations ONLY.
        To fetch all items from a collection endpoint (list devices, list sites,
        etc.), write a script using ``api.paginate()`` instead — never pass
        ``limit`` to this tool.

        **Before calling**, read the ``api://endpoint-catalog`` resource (or
        scan the API Endpoint Catalog in the system instructions) to find the
        correct ``METHOD /path``, then use ``query_graph`` against the schema
        subgraph (``Parameter``, ``RequestBody``, ``SchemaComponent``,
        ``Property``) to learn the parameter and body shape. See
        ``graph://schema`` for canned Cypher patterns.

        A pre-flight validator runs against the graph before dispatching the
        request. Missing required query parameters and missing required body
        fields (POST only) cause the call to be rejected with a structured
        error containing a schema summary. Unknown body keys appear as
        warnings on a successful response. If the graph is unavailable the
        validator fails open.

        For config APIs (network-config/), you need a scopeId — find it via
        ``query_graph`` on the relevant Site, SiteCollection, or Device node.

        For multi-step workflows, write a script instead of chaining API calls.

        Args:
            path: API path (e.g., "network-monitoring/v1/device-inventory").
                  Do not include the base URL.
            method: HTTP method. Defaults to GET.
            query_params: Optional query parameters as key-value pairs.
            body: Optional JSON request body (for POST, PATCH, PUT).

        Returns:
            JSON response from the Central API, optionally prefixed with a
            block of pre-flight warnings.
        """
        if not settings.has_credentials:
            raise ToolError(
                "Central credentials not configured. "
                "Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET."
            )

        if settings.read_only and method.upper() in _WRITE_METHODS:
            raise ToolError(
                f"Server is in READ_ONLY mode; {method.upper()} requests are not permitted. "
                "Network-side configuration changes are disabled."
            )

        result = validate_call(graph_manager, method, path, query_params, body)
        if not result.ok:
            raise ToolError(format_validation_error(result))

        return _make_api_call(
            client,
            "central",
            path,
            method,
            query_params,
            body,
            warning_header=format_validation_warnings(result),
        )


def register_greenlake_api_call_tools(
    mcp,
    settings: Settings,
    glp_client: GreenLakeClient | None,
    graph_manager: "GraphManager | None" = None,
):
    """Register the GreenLake direct API call tool with the MCP server."""

    @mcp.tool(
        annotations=ToolAnnotations(
            readOnlyHint=False,
            idempotentHint=False,
            openWorldHint=True,
        ),
    )
    def call_greenlake_api(
        path: str,
        method: Literal["GET", "POST", "PATCH", "PUT", "DELETE"] = "GET",
        query_params: dict[str, str] | None = None,
        body: dict | None = None,
    ) -> str:
        """Make an authenticated request to any HPE GreenLake Platform API endpoint.

        Use for GreenLake-specific operations: device inventory management,
        subscription/license management, service catalog, locations, etc.
        The base URL is https://global.api.greenlake.hpe.com.

        **Before calling**, read the ``api://endpoint-catalog`` resource to find
        the correct ``METHOD /path`` — GreenLake endpoints appear under
        categories starting with "HPE GreenLake APIs for ...". Use
        ``query_graph`` against the schema subgraph for parameters and body
        fields; see ``graph://schema`` for canned Cypher patterns.

        A pre-flight validator runs against the graph before dispatching the
        request (same semantics as ``call_central_api``).

        Args:
            path: API path (e.g., "devices/v1/devices").
                  Do not include the base URL.
            method: HTTP method. Defaults to GET.
            query_params: Optional query parameters as key-value pairs.
            body: Optional JSON request body (for POST, PATCH, PUT).

        Returns:
            JSON response from the GreenLake API, optionally prefixed with a
            block of pre-flight warnings.
        """
        if glp_client is None:
            raise ToolError(
                "GreenLake credentials not configured. "
                "Set GREENLAKE_CLIENT_ID and GREENLAKE_CLIENT_SECRET in your .env file "
                "(or GLP_CLIENT_ID / GLP_CLIENT_SECRET)."
            )

        if settings.read_only and method.upper() in _WRITE_METHODS:
            raise ToolError(
                f"Server is in READ_ONLY mode; {method.upper()} requests are not permitted. "
                "Network-side configuration changes are disabled."
            )

        result = validate_call(graph_manager, method, path, query_params, body)
        if not result.ok:
            raise ToolError(format_validation_error(result))

        return _make_api_call(
            glp_client,
            "greenlake",
            path,
            method,
            query_params,
            body,
            warning_header=format_validation_warnings(result),
        )
