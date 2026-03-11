"""Inventory tools — Ansible dynamic inventory integration."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any

import structlog

from ..config import Settings

logger = structlog.get_logger("tools.inventory")

# Cache for inventory data
_inventory_cache: dict[str, Any] = {}
_cache_timestamp: float = 0.0


def _generate_inventory_config(settings: Settings) -> None:
    """Generate the Ansible inventory YAML config from env vars."""
    config = f"""plugin: arubanetworks.hpeanw_central.central_inventory
central_base_url: "{settings.central_base_url}"
central_client_id: "{settings.central_client_id}"
central_client_secret: "{settings.central_client_secret}"

groups:
  - site
  - device_type
  - model
  - status
  - device_function

compose:
  ansible_host: ipv4
  location: siteName
  is_online: status == 'ONLINE'
"""
    config_path = settings.inventory_config_path
    config_path.parent.mkdir(parents=True, exist_ok=True)
    config_path.write_text(config, encoding="utf-8")
    logger.info("inventory_config_generated", path=str(config_path))


def _run_ansible_inventory(settings: Settings) -> dict[str, Any]:
    """Execute ansible-inventory and return parsed JSON."""
    config_path = settings.inventory_config_path
    if not config_path.exists():
        _generate_inventory_config(settings)

    result = subprocess.run(
        ["ansible-inventory", "-i", str(config_path), "--list"],
        capture_output=True,
        text=True,
        timeout=120,
        cwd="/",
    )

    if result.returncode != 0:
        logger.error("ansible_inventory_failed", stderr=result.stderr[:2000])
        raise RuntimeError(f"ansible-inventory failed (exit {result.returncode}): {result.stderr[:500]}")

    return json.loads(result.stdout)


def _get_cached_inventory(settings: Settings, force_refresh: bool = False) -> dict[str, Any]:
    """Return inventory data, using cache if valid."""
    global _inventory_cache, _cache_timestamp

    now = time.time()
    if not force_refresh and _inventory_cache and (now - _cache_timestamp) < settings.inventory_cache_ttl:
        logger.debug("inventory_cache_hit", age_seconds=int(now - _cache_timestamp))
        return _inventory_cache

    logger.info("inventory_refresh_start")
    data = _run_ansible_inventory(settings)
    _inventory_cache = data
    _cache_timestamp = now
    logger.info("inventory_refresh_done")
    return data


def _summarize_inventory(inventory: dict[str, Any]) -> dict[str, Any]:
    """Build a summary from raw Ansible inventory JSON."""
    meta = inventory.get("_meta", {}).get("hostvars", {})
    total_devices = len(meta)

    # Count by groups
    sites: dict[str, int] = {}
    types: dict[str, int] = {}
    statuses: dict[str, int] = {}
    functions: dict[str, int] = {}
    unassigned: list[dict[str, str]] = []

    for hostname, vars_ in meta.items():
        site = vars_.get("siteName", "unassigned")
        dtype = vars_.get("deviceType", "unknown")
        status = vars_.get("status", "unknown")
        func = vars_.get("deviceFunction", "unknown")
        serial = vars_.get("serialNumber", hostname)

        sites[site] = sites.get(site, 0) + 1
        types[dtype] = types.get(dtype, 0) + 1
        statuses[status] = statuses.get(status, 0) + 1
        functions[func] = functions.get(func, 0) + 1

        if site == "unassigned" or not vars_.get("siteName"):
            unassigned.append({"serial": serial, "type": dtype, "status": status})

    return {
        "total_devices": total_devices,
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
        detail_level: str = "summary",
        force_refresh: bool = False,
        filter_site: str | None = None,
        filter_type: str | None = None,
        filter_status: str | None = None,
    ) -> str:
        """Refresh and read the network inventory from HPE Aruba Networking Central.

        Runs the Ansible dynamic inventory plugin to discover all devices, sites,
        and their current state. Use this as the first step before any network operation.

        Args:
            detail_level: "summary" for counts and key metrics, "full" for complete device data.
            force_refresh: Force a fresh API call, ignoring the 5-minute cache.
            filter_site: Only return devices at this site name.
            filter_type: Only return devices of this type (SWITCH, ACCESS_POINT, GATEWAY).
            filter_status: Only return devices with this status (ONLINE, OFFLINE).

        Returns:
            JSON string with inventory data — either summary metrics or full device list.
        """
        if not settings.has_credentials:
            return json.dumps({"error": "Central credentials not configured. Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET."})

        try:
            inventory = _get_cached_inventory(settings, force_refresh=force_refresh)
        except Exception as e:
            return json.dumps({"error": str(e)})

        if detail_level == "full":
            meta = inventory.get("_meta", {}).get("hostvars", {})
            devices = list(meta.values())

            # Apply filters
            if filter_site:
                devices = [d for d in devices if d.get("siteName") == filter_site]
            if filter_type:
                devices = [d for d in devices if d.get("deviceType") == filter_type]
            if filter_status:
                devices = [d for d in devices if d.get("status") == filter_status]

            return json.dumps({"total": len(devices), "devices": devices}, indent=2)
        else:
            summary = _summarize_inventory(inventory)

            # If filters applied, add filtered counts
            if any([filter_site, filter_type, filter_status]):
                meta = inventory.get("_meta", {}).get("hostvars", {})
                filtered = list(meta.values())
                if filter_site:
                    filtered = [d for d in filtered if d.get("siteName") == filter_site]
                if filter_type:
                    filtered = [d for d in filtered if d.get("deviceType") == filter_type]
                if filter_status:
                    filtered = [d for d in filtered if d.get("status") == filter_status]
                summary["filtered_count"] = len(filtered)

            return json.dumps(summary, indent=2)

    @mcp.tool()
    def get_device_details(identifier: str) -> str:
        """Get detailed information about a specific network device.

        Looks up a device by serial number, device name, IP address, or MAC address
        from the cached inventory. Refresh inventory first if data is stale.

        Args:
            identifier: Serial number, device name, IPv4 address, or MAC address.

        Returns:
            JSON string with all known attributes of the device, or an error if not found.
        """
        if not settings.has_credentials:
            return json.dumps({"error": "Central credentials not configured."})

        try:
            inventory = _get_cached_inventory(settings)
        except Exception as e:
            return json.dumps({"error": str(e)})

        meta = inventory.get("_meta", {}).get("hostvars", {})
        search = identifier.strip()

        # Search across multiple fields
        for hostname, vars_ in meta.items():
            if search.lower() in [
                str(vars_.get("serialNumber", "")).lower(),
                str(hostname).lower(),
                str(vars_.get("ipv4", "")).lower(),
                str(vars_.get("macAddress", "")).lower(),
                str(vars_.get("deviceName", "")).lower(),
            ]:
                return json.dumps({"device": vars_, "hostname": hostname}, indent=2)

        return json.dumps({"error": f"Device '{identifier}' not found in inventory. Try refresh_inventory(force_refresh=true) first."})
