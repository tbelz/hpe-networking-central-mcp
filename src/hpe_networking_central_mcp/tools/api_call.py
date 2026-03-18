"""Direct API call tool — authenticated access to Central APIs."""

from __future__ import annotations

import json
from typing import Literal

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..central_client import CentralClient, CentralAPIError
from ..config import Settings

logger = structlog.get_logger("tools.api_call")

_WRITE_METHODS = {"POST", "PATCH", "PUT", "DELETE"}


def register_api_call_tools(mcp, settings: Settings, client: CentralClient):
    """Register the direct API call tool with the MCP server."""

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

        Use get_api_details() first to discover the correct path and parameters.
        For multi-step workflows (create site + assign devices + configure), write
        a script instead of chaining multiple calls.

        Args:
            path: API path (e.g., "network-monitoring/v1alpha1/devices").
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

        # Basic path validation
        clean_path = path.strip().lstrip("/")
        if not clean_path:
            raise ToolError("Path cannot be empty.")
        if ".." in clean_path:
            raise ToolError("Path must not contain '..'.")

        if body and method == "GET":
            raise ToolError("Cannot send a request body with GET. Use POST, PATCH, or PUT.")

        try:
            logger.info("api_call_start", method=method, path=clean_path, params=query_params)
            result = client._request(method, clean_path, params=query_params, json_body=body)
            logger.info("api_call_done", method=method, path=clean_path)
            return json.dumps(result, indent=2)
        except CentralAPIError as e:
            logger.error("api_call_failed", method=method, path=clean_path, status=e.status_code, error_code=e.error_code)
            raise ToolError(f"API call failed [{e.status_code}]: {e.message}" + (f" (debugId: {e.debug_id})" if e.debug_id else ""))
        except Exception as e:
            logger.error("api_call_failed", method=method, path=clean_path, error=str(e))
            raise ToolError(f"API call failed: {e}")
