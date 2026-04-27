"""Direct API call tools — authenticated access to Central and GreenLake APIs."""

from __future__ import annotations

import json
from typing import Literal

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..central_client import BaseAPIClient, CentralAPIError, CentralClient, GreenLakeClient
from ..config import Settings
from .api_call_policy import check_call_policy, eid_for, get_registry, get_tracker

logger = structlog.get_logger("tools.api_call")

_WRITE_METHODS = {"POST", "PATCH", "PUT", "DELETE"}


def _try_endpoint_id_bypass(method: str, path: str, endpoint_id: str | None) -> bool:
    """Template-aware bypass for the schema gate.

    Returns ``True`` and records the inspection (against the matched template
    when one exists, otherwise against the normalised concrete path) when
    ``endpoint_id`` matches either the concrete eid or the registered template
    eid for ``method``/``path``. Mirrors the recording semantics of
    :func:`check_call_policy` so a bypass on ``/foo/123/bar`` also unblocks
    follow-up calls to other concrete instantiations of ``/foo/{id}/bar``.
    """
    if not endpoint_id:
        return False
    eid = endpoint_id.strip()
    if not eid:
        return False
    template = get_registry().match(method, path)
    inspect_path = template if template is not None else path
    if eid == eid_for(method, path) or (template is not None and eid == eid_for(method, template)):
        get_tracker().record(method, inspect_path)
        return True
    return False


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
        return (
            "\n\nHint: Endpoint not found. Read the api://endpoint-catalog resource "
            "for the full list of valid METHOD /path combinations, then use "
            "get_api_endpoint_detail() to verify the exact path and parameters. "
            "Guessing paths without consulting the catalog has a near-zero chance of success."
        )

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
        endpoint_id: str | None = None,
    ) -> str:
        """Make an authenticated request to any Central API endpoint.

        For single-item lookups and one-off mutations ONLY.
        To fetch all items from a collection endpoint (list devices, list sites, etc.),
        write a script using ``api.paginate()`` instead — never pass ``limit`` to this tool.

        **Before calling this tool**, read the ``api://endpoint-catalog`` resource (or
        scan the API Endpoint Catalog in the system instructions) to find the correct
        ``METHOD /path``. Then call ``get_api_endpoint_detail()`` to verify parameters.
        Guessing paths without consulting the catalog has a near-zero chance of success.

        For config APIs (network-config/), you need a scopeId — find it via
        query_graph on the relevant Site, SiteCollection, or Device node.

        For multi-step workflows, write a script instead of chaining API calls.

        **Bypassing the schema gate.** If you have already inspected the endpoint —
        either via ``describe_endpoint_for_device`` or by querying ``Parameter`` /
        ``RequestBody`` / ``Property`` nodes with ``query_graph`` — pass
        ``endpoint_id="METHOD:/path"`` (e.g. ``"GET:/network-notifications/v1/alerts"``)
        to skip the gate's redundant property-summary block. The id must match
        ``method`` and ``path`` exactly; mismatches fall through to the standard gate.

        Args:
            path: API path (e.g., "network-monitoring/v1/device-inventory").
                  Do not include the base URL.
            method: HTTP method. Defaults to GET.
            query_params: Optional query parameters as key-value pairs.
            body: Optional JSON request body (for POST, PATCH, PUT).
            endpoint_id: Optional ``"METHOD:/path"`` token attesting that the
                endpoint schema has already been consulted (see above).

        Returns:
            JSON response from the Central API.
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

        # Explicit attestation: agent has already inspected this endpoint via
        # query_graph / describe_endpoint_for_device. Record + skip the gate.
        # Template-aware: passing the template eid for a concrete path is
        # accepted and records the inspection against the template, matching
        # ``check_call_policy``'s auto-record semantics.
        if not _try_endpoint_id_bypass(method, path, endpoint_id):
            allowed, reason = check_call_policy(method, path)
            if not allowed:
                raise ToolError(reason or "API call blocked by policy.")

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
        endpoint_id: str | None = None,
    ) -> str:
        """Make an authenticated request to any HPE GreenLake Platform API endpoint.

        Use for GreenLake-specific operations: device inventory management,
        subscription/license management, service catalog, locations, etc.
        The base URL is https://global.api.greenlake.hpe.com.

        **Before calling this tool**, read the ``api://endpoint-catalog`` resource (or
        scan the API Endpoint Catalog in the system instructions) to find the correct
        ``METHOD /path`` — GreenLake endpoints appear under categories starting with
        "HPE GreenLake APIs for ...". Guessing paths without consulting the catalog
        has a near-zero chance of success.

        **Bypassing the schema gate.** Pass ``endpoint_id="METHOD:/path"`` to attest
        that you have already inspected the endpoint via ``describe_endpoint_for_device``
        or ``query_graph`` on the schema subgraph; mismatches fall through to the
        standard gate.

        Args:
            path: API path (e.g., "devices/v1/devices").
                  Do not include the base URL.
            method: HTTP method. Defaults to GET.
            query_params: Optional query parameters as key-value pairs.
            body: Optional JSON request body (for POST, PATCH, PUT).
            endpoint_id: Optional ``"METHOD:/path"`` token attesting that the
                endpoint schema has already been consulted (see above).

        Returns:
            JSON response from the GreenLake API.
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

        if not _try_endpoint_id_bypass(method, path, endpoint_id):
            allowed, reason = check_call_policy(method, path)
            if not allowed:
                raise ToolError(reason or "API call blocked by policy.")

        return _make_api_call(glp_client, "greenlake", path, method, query_params, body)
