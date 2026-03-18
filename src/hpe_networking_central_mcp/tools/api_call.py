"""Direct API call tools — authenticated access to Central and GreenLake APIs."""

from __future__ import annotations

import json
from typing import Literal

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..central_client import BaseAPIClient, CentralAPIError, CentralClient, GreenLakeClient
from ..config import Settings

logger = structlog.get_logger("tools.api_call")

_WRITE_METHODS = {"POST", "PATCH", "PUT", "DELETE"}


def _api_error_hint(exc: CentralAPIError, path: str, method: str) -> str:
    """Build a contextual hint for common API errors."""
    msg = (exc.message or "").lower()

    # Scope restriction errors
    if "restricted" in msg or "scope" in msg:
        return (
            "\n\nHint: Central config APIs require a valid scopeId and scopeType. "
            "Use query_graph to find scopeId values from Site, SiteCollection, or Device nodes. "
            "Use get_api_endpoint_detail() to check required parameters."
        )

    # 404 — wrong path
    if exc.status_code == 404:
        return "\n\nHint: Endpoint not found. Use search_api_catalog() to find the correct path."

    # 400 — bad request
    if exc.status_code == 400:
        return "\n\nHint: Check parameters with get_api_endpoint_detail(method, path)."

    return ""


def _make_api_call(
    client: BaseAPIClient,
    platform: str,
    path: str,
    method: str,
    query_params: dict[str, str] | None,
    body: dict | None,
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
        return json.dumps(result, indent=2)
    except CentralAPIError as e:
        logger.error("api_call_failed", platform=platform, method=method, path=clean_path, status=e.status_code, error_code=e.error_code)
        hint = _api_error_hint(e, clean_path, method)
        raise ToolError(f"API call failed [{e.status_code}]: {e.message}{hint}" + (f" (debugId: {e.debug_id})" if e.debug_id else ""))
    except Exception as e:
        logger.error("api_call_failed", platform=platform, method=method, path=clean_path, error=str(e))
        raise ToolError(f"API call failed: {e}")


def register_api_call_tools(mcp, settings: Settings, client: CentralClient):
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

        Always use get_api_endpoint_detail() first to verify path and parameters.
        For config APIs (network-config/), you need a scopeId — find it via
        query_graph on the relevant Site, SiteCollection, or Device node.

        For multi-step workflows, write a script instead of chaining API calls.

        Args:
            path: API path (e.g., "network-monitoring/v1/device-inventory").
                  Do not include the base URL.
            method: HTTP method. Defaults to GET.
            query_params: Optional query parameters as key-value pairs.
            body: Optional JSON request body (for POST, PATCH, PUT).

        Returns:
            JSON response from the Central API.
        """
        if not settings.has_credentials:
            raise ToolError(
                "Central credentials not configured. "
                "Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET."
            )

        return _make_api_call(client, "central", path, method, query_params, body)


def register_greenlake_api_call_tools(mcp, settings: Settings, glp_client: GreenLakeClient | None):
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

        Use search_api_catalog() to discover available GreenLake endpoints — they
        appear in the catalog under categories starting with "HPE GreenLake APIs for ...".

        Args:
            path: API path (e.g., "devices/v1/devices").
                  Do not include the base URL.
            method: HTTP method. Defaults to GET.
            query_params: Optional query parameters as key-value pairs.
            body: Optional JSON request body (for POST, PATCH, PUT).

        Returns:
            JSON response from the GreenLake API.
        """
        if glp_client is None:
            raise ToolError(
                "GreenLake credentials not configured. "
                "Set GREENLAKE_CLIENT_ID and GREENLAKE_CLIENT_SECRET in your .env file "
                "(or GLP_CLIENT_ID / GLP_CLIENT_SECRET)."
            )

        return _make_api_call(glp_client, "greenlake", path, method, query_params, body)
