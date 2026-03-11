"""Inventory tools - Central Monitoring API integration."""

from __future__ import annotations

import json
import time
from typing import Any, Literal

import structlog

from ..central_client import CentralClient
from ..config import Settings

logger = structlog.get_logger("tools.inventory")

# Cache for inventory data
_inventory_cache: list[dict[str, Any]] = []
_cache_timestamp: float = 0.0

# Fields that must never be exposed to the agent
_SENSITIVE_KEYS = {"access_token", "client_id", "client_secret"}

# Monitoring API path and pagination
_DEVICES_PATH = "network-monitoring/v1alpha1/devices"
_PAGE_LIMIT = 100


def _get_client(settings: Settings) -> CentralClient:
    """Create a CentralClient from settings."""
    return CentralClient(
        base_url=settings.central_base_url,
        client_id=settings.central_client_id,
        client_secret=settings.central_client_secret,
    )


def _fetch_all_devices(settings: Settings) -> list[dict[str, Any]]:
    """Fetch all monitored devices with automatic pagination."""
    client = _get_client(settings)
    try:
        devices: list[dict[str, Any]] = []
        page = 1
        while True:
            resp = client.get(_DEVICES_PATH, params={"limit": str(_PAGE_LIMIT), "next": str(page)})
            items = resp.get("items", [])
            devices.extend(items)
            total = resp.get("total", 0)
            if len(devices) >= total or not items:
                break
            page += 1
        return devices
    finally:
        client.close()


def _get_cached_inventory(settings: Settings, force_refresh: bool = False) -> list[dict[str, Any]]:
    """Return inventory data, using cache if valid."""
    global _inventory_cache, _cache_timestamp

    now = time.time()
    if not force_refresh and _inventory_cache and (now - _cache_timestamp) < settings.inventory_cache_ttl:
        logger.debug("inventory_cache_hit", age_seconds=int(now - _cache_timestamp))
        return _inventory_cache

    logger.info("inventory_refresh_start")
    data = _fetch_all_devices(settings)
    _inventory_cache = data
    _cache_timestamp = now
    logger.info("inventory_refresh_done", device_count=len(data))
    return data


def _apply_filters(
    devices: list[dict[str, Any]],
    filter_site: str | None,
    filter_type: str | None,
    filter_status: str | None,
) -> list[dict[str, Any]]:
    """Apply case-insensitive filters to device list."""
    result = devices
    if filter_site:
        fs = filter_site.lower()
        result = [d for d in result if str(d.get("siteName") or "").lower() == fs]
    if filter_type:
        ft = filter_type.upper()
        result = [d for d in result if str(d.get("deviceType") or "").upper() == ft]
    if filter_status:
        fst = filter_status.upper()
        result = [d for d in result if str(d.get("status") or "").upper() == fst]
    return result


def _strip_sensitive(device: dict[str, Any]) -> dict[str, Any]:
    """Remove sensitive keys from a device dict."""
    return {k: v for k, v in device.items() if k not in _SENSITIVE_KEYS}


def _summarize_inventory(devices: list[dict[str, Any]]) -> dict[str, Any]:
    """Build a summary from a device list."""
    sites: dict[str, int] = {}
    types: dict[str, int] = {}
    statuses: dict[str, int] = {}
    functions: dict[str, int] = {}
    unassigned: list[dict[str, str]] = []

    for d in devices:
        site = str(d.get("siteName") or "unassigned")
        dtype = str(d.get("deviceType") or "unknown")
        status = str(d.get("status") or "unknown")
        func = str(d.get("deviceFunction") or "unknown")
        serial = str(d.get("serialNumber") or "unknown")

        sites[site] = sites.get(site, 0) + 1
        types[dtype] = types.get(dtype, 0) + 1
        statuses[status] = statuses.get(status, 0) + 1
        functions[func] = functions.get(func, 0) + 1

        if site == "unassigned":
            unassigned.append({"serial": serial, "type": dtype, "status": status})

    return {
        "total_devices": len(devices),
        "by_site": dict(sorted(sites.items())),
        "by_type": dict(sorted(types.items())),
        "by_status": dict(sorted(statuses.items())),
        "by_function": dict(sorted(functions.items())),
        "unassigned_devices": unassigned,
        "cache_age_seconds": int(time.time() - _cache_timestamp) if _cache_timestamp else 0,
    }


def register_inventory_tools(mcp, settings: Settings):
    """Register inventory-related tools with the MCP server."""

    @mcp.tool()
    def refresh_inventory(
        detail_level: Literal["summary", "full"] = "summary",
        force_refresh: bool = False,
        filter_site: str | None = None,
        filter_type: str | None = None,
        filter_status: str | None = None,
    ) -> str:
        """Refresh and read the network inventory from HPE Aruba Networking Central.

        Queries the Central Monitoring API to discover all devices, sites, and
        their current state. Use this as the first step before any network operation.

        Args:
            detail_level: "summary" for counts and key metrics, "full" for complete device data.
            force_refresh: Force a fresh API call, ignoring the 5-minute cache.
            filter_site: Only return devices at this site name (case-insensitive).
            filter_type: Only return devices of this type (SWITCH, ACCESS_POINT, GATEWAY - case-insensitive).
            filter_status: Only return devices with this status (ONLINE, OFFLINE - case-insensitive).

        Returns:
            JSON string with inventory data - either summary metrics or full device list.
        """
        if not settings.has_credentials:
            return json.dumps({
                "error": "Central credentials not configured. "
                "Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET."
            })

        try:
            devices = _get_cached_inventory(settings, force_refresh=force_refresh)
        except Exception as e:
            return json.dumps({"error": str(e)})

        filtered = _apply_filters(devices, filter_site, filter_type, filter_status)

        if detail_level == "full":
            safe = [_strip_sensitive(d) for d in filtered]
            return json.dumps({"total": len(safe), "devices": safe}, indent=2)
        else:
            return json.dumps(_summarize_inventory(filtered), indent=2)

    @mcp.tool()
    def get_device_details(identifier: str) -> str:
        """Get detailed information about a specific network device.

        Looks up a device by serial number, device name, IP address, or MAC address
        from the cached inventory. Supports partial/substring matching: if the
        identifier uniquely matches one device the full details are returned; if
        multiple devices match a compact candidate list is returned instead.

        Args:
            identifier: Full or partial serial number, device name, IPv4 address, or MAC address.

        Returns:
            JSON string with device details, a candidate list, or an error.
        """
        if not settings.has_credentials:
            return json.dumps({"error": "Central credentials not configured."})

        try:
            devices = _get_cached_inventory(settings)
        except Exception as e:
            return json.dumps({"error": str(e)})

        search = identifier.strip().lower()
        matches: list[dict[str, Any]] = []

        for d in devices:
            fields = [
                str(d.get("serialNumber", "")),
                str(d.get("deviceName", "")),
                str(d.get("ipv4", "")),
                str(d.get("macAddress", "")),
            ]
            if any(search in f.lower() for f in fields):
                matches.append(d)

        if len(matches) == 1:
            safe = _strip_sensitive(matches[0])
            return json.dumps({"device": safe}, indent=2)

        if len(matches) > 1:
            candidates = [
                {
                    "serial": str(d.get("serialNumber", "")),
                    "name": str(d.get("deviceName", "")),
                    "site": str(d.get("siteName", "unassigned")),
                    "type": str(d.get("deviceType", "unknown")),
                    "status": str(d.get("status", "unknown")),
                }
                for d in matches
            ]
            return json.dumps({
                "message": f"Multiple devices match '{identifier}'. "
                "Narrow your search or use the full serial number.",
                "match_count": len(matches),
                "candidates": candidates,
            }, indent=2)

        return json.dumps({
            "error": f"Device '{identifier}' not found in inventory. "
            "Try refresh_inventory(force_refresh=true) first."
        })
