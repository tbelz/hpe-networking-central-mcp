"""Direct API call tool — GET-only access to Central APIs."""

from __future__ import annotations

import json

import structlog

from ..central_client import CentralClient
from ..config import Settings

logger = structlog.get_logger("tools.api_call")


def register_api_call_tools(mcp, settings: Settings):
    """Register the direct API call tool with the MCP server."""

    @mcp.tool()
    def call_central_api(path: str, query_params: dict[str, str] | None = None) -> str:
        """Make a read-only GET request to any Central API endpoint.

        Use get_api_details() first to discover the correct path and parameters.
        This tool only supports GET requests — for writes (POST, PATCH, DELETE),
        write and execute a script instead.

        Args:
            path: API path (e.g., "network-monitoring/v1alpha1/devices").
                  Do not include the base URL.
            query_params: Optional query parameters as key-value pairs.

        Returns:
            JSON response from the Central API, or an error.
        """
        if not settings.has_credentials:
            return json.dumps({
                "error": "Central credentials not configured. "
                "Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET."
            })

        # Basic path validation
        clean_path = path.strip().lstrip("/")
        if not clean_path:
            return json.dumps({"error": "Path cannot be empty."})
        if ".." in clean_path:
            return json.dumps({"error": "Path must not contain '..'."})

        client = CentralClient(
            base_url=settings.central_base_url,
            client_id=settings.central_client_id,
            client_secret=settings.central_client_secret,
        )
        try:
            logger.info("api_call_start", path=clean_path, params=query_params)
            result = client.get(clean_path, params=query_params)
            logger.info("api_call_done", path=clean_path)
            return json.dumps(result, indent=2)
        except Exception as e:
            logger.error("api_call_failed", path=clean_path, error=str(e))
            return json.dumps({"error": f"API call failed: {e}"})
        finally:
            client.close()
