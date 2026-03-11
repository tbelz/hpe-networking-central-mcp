"""Inventory tools — Ansible dynamic inventory integration."""

from __future__ import annotations

import json
import subprocess
import time
from pathlib import Path
from typing import Any, Literal

import structlog

from ..config import Settings

logger = structlog.get_logger("tools.inventory")

# Cache for inventory data
_inventory_cache: dict[str, Any] = {}
_cache_timestamp: float = 0.0

# Keys injected by the inventory plugin that must never be exposed
_SENSITIVE_KEYS = {
    "central_access_token", "central_client_id",
    "central_client_secret", "central_base_url",
    "ansible_ssh_pass", "ansible_become_pass",
}


def _unwrap_ansible_unsafe(value: Any) -> Any:
    """Unwrap Ansible's __ansible_unsafe dict wrappers to plain Python values.

    ansible-inventory --list serialises AnsibleUnsafeText as
    {"__ansible_unsafe": "actual_value"}.  This helper recursively converts
    those back to plain strings so downstream code can use them normally.
    """
    if isinstance(value, dict):
        if "__ansible_unsafe" in value and len(value) == 1:
            return value["__ansible_unsafe"]
        return {_unwrap_ansible_unsafe(k): _unwrap_ansible_unsafe(v) for k, v in value.items()}
    if isinstance(value, list):
        return [_unwrap_ansible_unsafe(v) for v in value]
    return value


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

    raw = json.loads(result.stdout)
    return _unwrap_ansible_unsafe(raw)


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


def _summarize_inventory(devices: dict[str, dict[str, Any]]) -> dict[str, Any]:
    """Build a summary from a hostname → hostvars mapping."""
    total_devices = len(devices)

    # Count by groups
    sites: dict[str, int] = {}
    types: dict[str, int] = {}
    statuses: dict[str, int] = {}
    functions: dict[str, int] = {}
    unassigned: list[dict[str, str]] = []

    for hostname, vars_ in devices.items():
        site = str(vars_.get("siteName") or "unassigned")
        dtype = str(vars_.get("deviceType") or "unknown")
        status = str(vars_.get("status") or "unknown")
        func = str(vars_.get("deviceFunction") or "unknown")
        serial = str(vars_.get("serialNumber") or hostname)

        sites[site] = sites.get(site, 0) + 1
        types[dtype] = types.get(dtype, 0) + 1
        statuses[status] = statuses.get(status, 0) + 1
        functions[func] = functions.get(func, 0) + 1

        if site == "unassigned":
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
        detail_level: Literal["summary", "full"] = "summary",
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
            filter_site: Only return devices at this site name (case-insensitive).
            filter_type: Only return devices of this type (SWITCH, ACCESS_POINT, GATEWAY — case-insensitive).
            filter_status: Only return devices with this status (ONLINE, OFFLINE — case-insensitive).

        Returns:
            JSON string with inventory data — either summary metrics or full device list.
        """
        if not settings.has_credentials:
            return json.dumps({"error": "Central credentials not configured. Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET."})

        try:
            inventory = _get_cached_inventory(settings, force_refresh=force_refresh)
        except Exception as e:
            return json.dumps({"error": str(e)})

        # Extract hostvars and apply filters (case-insensitive) before any view
        meta = inventory.get("_meta", {}).get("hostvars", {})
        filtered: dict[str, dict[str, Any]] = {}
        for hostname, vars_ in meta.items():
            if filter_site and str(vars_.get("siteName") or "").lower() != filter_site.lower():
                continue
            if filter_type and str(vars_.get("deviceType") or "").upper() != filter_type.upper():
                continue
            if filter_status and str(vars_.get("status") or "").upper() != filter_status.upper():
                continue
            filtered[hostname] = vars_

        if detail_level == "full":
            devices = [
                {k: v for k, v in d.items() if k not in _SENSITIVE_KEYS}
                for d in filtered.values()
            ]
            return json.dumps({"total": len(devices), "devices": devices}, indent=2)
        else:
            summary = _summarize_inventory(filtered)
            return json.dumps(summary, indent=2)

    @mcp.tool()
    def get_device_details(identifier: str) -> str:
        """Get detailed information about a specific network device.

        Looks up a device by serial number, device name, IP address, or MAC address
        from the cached inventory.  Supports partial/substring matching: if the
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
            inventory = _get_cached_inventory(settings)
        except Exception as e:
            return json.dumps({"error": str(e)})

        meta = inventory.get("_meta", {}).get("hostvars", {})
        search = identifier.strip().lower()

        # Collect all substring matches across key fields
        matches: list[tuple[str, dict[str, Any]]] = []
        for hostname, vars_ in meta.items():
            fields = [
                str(vars_.get("serialNumber", "")),
                str(hostname),
                str(vars_.get("ipv4", "")),
                str(vars_.get("macAddress", "")),
                str(vars_.get("deviceName", "")),
            ]
            if any(search in f.lower() for f in fields):
                matches.append((hostname, vars_))

        if len(matches) == 1:
            hostname, vars_ = matches[0]
            safe = {k: v for k, v in vars_.items() if k not in _SENSITIVE_KEYS}
            return json.dumps({"device": safe, "hostname": hostname}, indent=2)

        if len(matches) > 1:
            candidates = [
                {
                    "serial": str(v.get("serialNumber", "")),
                    "name": str(v.get("deviceName", h)),
                    "site": str(v.get("siteName", "unassigned")),
                    "type": str(v.get("deviceType", "unknown")),
                    "status": str(v.get("status", "unknown")),
                }
                for h, v in matches
            ]
            return json.dumps({
                "message": f"Multiple devices match '{identifier}'. Narrow your search or use the full serial number.",
                "match_count": len(matches),
                "candidates": candidates,
            }, indent=2)

        return json.dumps({"error": f"Device '{identifier}' not found in inventory. Try refresh_inventory(force_refresh=true) first."})
