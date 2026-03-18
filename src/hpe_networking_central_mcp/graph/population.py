"""Populate the Kùzu graph from Aruba Central APIs.

Fetches structural data (sites, site-collections, device-groups, devices)
and library-level config metadata, then inserts nodes and relationships.
"""

from __future__ import annotations

import structlog

from ..central_client import CentralClient, CentralAPIError

logger = structlog.get_logger("graph.population")

# Config categories to index at the library level
_CONFIG_CATEGORIES = [
    "wlan-ssids",
    "sw-port-profiles",
    "gw-port-profiles",
    "roles",
    "server-groups",
]

# Device inventory API (returns ALL devices including un-onboarded, with deviceGroupId)
_INVENTORY_PATH = "network-monitoring/v1/device-inventory"
_PAGE_LIMIT = 100


# ── Data fetching ────────────────────────────────────────────────────


def _fetch_all_devices(client: CentralClient) -> list[dict]:
    """Fetch all devices from the inventory API (includes un-onboarded devices)."""
    devices: list[dict] = []
    page = 1
    while True:
        resp = client.get(_INVENTORY_PATH, params={"limit": str(_PAGE_LIMIT), "next": str(page)})
        items = resp.get("items", [])
        devices.extend(items)
        total = resp.get("total", 0)
        if len(devices) >= total or not items:
            break
        page += 1
    return devices


def _fetch_sites(client: CentralClient) -> list[dict]:
    """Fetch all sites from Scope Management API."""
    items: list[dict] = []
    offset = 0
    while True:
        resp = client.get(
            "network-config/v1/sites",
            params={"limit": str(_PAGE_LIMIT), "offset": str(offset)},
        )
        batch = resp.get("items", [])
        items.extend(batch)
        total = resp.get("total", 0)
        if len(items) >= total or not batch:
            break
        offset += len(batch)
    return items


def _fetch_site_collections(client: CentralClient) -> list[dict]:
    """Fetch all site-collections from Scope Management API."""
    items: list[dict] = []
    offset = 0
    while True:
        resp = client.get(
            "network-config/v1/site-collections",
            params={"limit": str(_PAGE_LIMIT), "offset": str(offset)},
        )
        batch = resp.get("items", [])
        items.extend(batch)
        total = resp.get("total", 0)
        if len(items) >= total or not batch:
            break
        offset += len(batch)
    return items


def _fetch_device_groups(client: CentralClient) -> list[dict]:
    """Fetch all device-groups from Scope Management API."""
    items: list[dict] = []
    offset = 0
    while True:
        resp = client.get(
            "network-config/v1/device-groups",
            params={"limit": str(_PAGE_LIMIT), "offset": str(offset)},
        )
        batch = resp.get("items", [])
        items.extend(batch)
        total = resp.get("total", 0)
        if len(items) >= total or not batch:
            break
        offset += len(batch)
    return items


def _fetch_config_profiles(client: CentralClient, category: str) -> list[dict]:
    """Fetch library-level config profiles for a given category."""
    try:
        resp = client.get(
            f"network-config/v1alpha1/{category}",
            params={"view-type": "LIBRARY"},
        )
        # Response shape varies by category — try common keys
        if isinstance(resp, list):
            return resp
        for key in ("items", "result", category):
            if key in resp and isinstance(resp[key], list):
                return resp[key]
        # If it's a dict with individual profile data, wrap it
        if "name" in resp:
            return [resp]
        return []
    except CentralAPIError as exc:
        logger.warning("config_fetch_failed", category=category, status=exc.status_code, msg=exc.message)
        return []


# ── Graph population ─────────────────────────────────────────────────


def populate_graph(client: CentralClient, conn) -> dict:
    """Populate the graph with data from Central APIs.

    Args:
        client: Authenticated CentralClient instance.
        conn: Kùzu Connection object.

    Returns:
        Summary dict with counts of populated entities.
    """
    summary = {
        "sites": 0,
        "site_collections": 0,
        "device_groups": 0,
        "devices": 0,
        "config_profiles": 0,
        "errors": [],
    }

    # ── Fetch all data ────────────────────────────────────────────
    logger.info("population_fetch_start")

    try:
        sites = _fetch_sites(client)
        logger.info("fetched_sites", count=len(sites))
    except CentralAPIError as exc:
        sites = []
        summary["errors"].append(f"sites: [{exc.status_code}] {exc.message}")
        logger.error("fetch_sites_failed", error=str(exc))

    try:
        collections = _fetch_site_collections(client)
        logger.info("fetched_site_collections", count=len(collections))
    except CentralAPIError as exc:
        collections = []
        summary["errors"].append(f"site_collections: [{exc.status_code}] {exc.message}")
        logger.error("fetch_collections_failed", error=str(exc))

    try:
        groups = _fetch_device_groups(client)
        logger.info("fetched_device_groups", count=len(groups))
    except CentralAPIError as exc:
        groups = []
        summary["errors"].append(f"device_groups: [{exc.status_code}] {exc.message}")
        logger.error("fetch_groups_failed", error=str(exc))

    try:
        devices = _fetch_all_devices(client)
        logger.info("fetched_devices", count=len(devices))
    except CentralAPIError as exc:
        devices = []
        summary["errors"].append(f"devices: [{exc.status_code}] {exc.message}")
        logger.error("fetch_devices_failed", error=str(exc))

    logger.info("population_fetch_done")

    # ── Insert nodes ──────────────────────────────────────────────

    # Org node (synthetic root — use a fixed scopeId)
    conn.execute(
        "MERGE (o:Org {scopeId: $sid}) SET o.name = $name",
        {"sid": "org-root", "name": "Organization"},
    )

    # Site-collections
    collection_ids = set()
    for sc in collections:
        sid = str(sc.get("id", sc.get("scopeId", "")))
        if not sid:
            continue
        collection_ids.add(sid)
        conn.execute(
            "MERGE (sc:SiteCollection {scopeId: $sid}) "
            "SET sc.name = $name, sc.siteCount = $sc, sc.deviceCount = $dc",
            {
                "sid": sid,
                "name": sc.get("scopeName", sc.get("name", "")),
                "sc": sc.get("siteCount", sc.get("site_count", 0)),
                "dc": sc.get("deviceCount", sc.get("device_count", 0)),
            },
        )
        summary["site_collections"] += 1

    # Sites
    site_ids = set()
    site_collection_map: dict[str, str] = {}  # siteId -> collectionId
    for s in sites:
        sid = str(s.get("id", s.get("scopeId", "")))
        if not sid:
            continue
        site_ids.add(sid)
        coll_id = str(s.get("collectionId", s.get("collection_id", "") or ""))
        coll_name = s.get("collectionName", s.get("collection_name", "") or "")
        if coll_id:
            site_collection_map[sid] = coll_id
        # Extract timezone from nested object or flat field
        tz_obj = s.get("timezone", {})
        tz_id = tz_obj.get("timezoneId", "") if isinstance(tz_obj, dict) else str(tz_obj or "")
        conn.execute(
            "MERGE (s:Site {scopeId: $sid}) "
            "SET s.name = $name, s.address = $addr, s.city = $city, "
            "s.country = $country, s.state = $state, s.zipcode = $zip, "
            "s.lat = $lat, s.lon = $lon, s.deviceCount = $dc, "
            "s.collectionId = $cid, s.collectionName = $cn, s.timezoneId = $tz",
            {
                "sid": sid,
                "name": s.get("scopeName", s.get("name", "")),
                "addr": s.get("address", ""),
                "city": s.get("city", ""),
                "country": s.get("country", ""),
                "state": s.get("state", ""),
                "zip": s.get("zipcode", ""),
                "lat": float(s.get("latitude", s.get("lat", 0)) or 0),
                "lon": float(s.get("longitude", s.get("lon", 0)) or 0),
                "dc": s.get("deviceCount", s.get("device_count", 0)),
                "cid": coll_id,
                "cn": coll_name,
                "tz": tz_id,
            },
        )
        summary["sites"] += 1

    # Device-groups
    for dg in groups:
        sid = str(dg.get("id", dg.get("scopeId", "")))
        if not sid:
            continue
        conn.execute(
            "MERGE (dg:DeviceGroup {scopeId: $sid}) "
            "SET dg.name = $name, dg.deviceCount = $dc",
            {
                "sid": sid,
                "name": dg.get("scopeName", dg.get("name", "")),
                "dc": dg.get("deviceCount", dg.get("device_count", 0)),
            },
        )
        summary["device_groups"] += 1

    # Devices
    device_site_map: dict[str, str] = {}  # serial -> siteId
    for d in devices:
        serial = str(d.get("serialNumber", d.get("serial", "")))
        if not serial:
            continue
        site_id = str(d.get("siteId", d.get("site_id", "") or ""))
        if site_id:
            device_site_map[serial] = site_id
        conn.execute(
            "MERGE (d:Device {serial: $serial}) "
            "SET d.name = $name, d.mac = $mac, d.model = $model, "
            "d.deviceType = $dt, d.status = $status, d.ipv4 = $ip, "
            "d.firmware = $fw, d.persona = $persona, d.deviceFunction = $df, "
            "d.siteId = $sid, d.siteName = $sn, d.partNumber = $pn, "
            "d.deployment = $dep, d.configStatus = $cs, "
            "d.deviceGroupId = $dgid, d.deviceGroupName = $dgn",
            {
                "serial": serial,
                "name": d.get("deviceName", d.get("name", "")),
                "mac": d.get("macAddress", d.get("mac", "")),
                "model": d.get("model", ""),
                "dt": d.get("deviceType", d.get("device_type", "")),
                "status": d.get("status", "") or "",
                "ip": d.get("ipv4", d.get("ip_address", "")) or "",
                "fw": d.get("firmwareVersion", d.get("firmware", "")) or "",
                "persona": d.get("persona", "") or "",
                "df": d.get("deviceFunction", d.get("device_function", "")) or "",
                "sid": site_id,
                "sn": d.get("siteName", d.get("site_name", "")) or "",
                "pn": d.get("partNumber", d.get("part_number", "")),
                "dep": d.get("deployment", "") or "",
                "cs": d.get("configStatus", d.get("config_status", "")) or "",
                "dgid": str(d.get("deviceGroupId", "") or ""),
                "dgn": d.get("deviceGroupName", "") or "",
            },
        )
        summary["devices"] += 1

    # ── Insert relationships ──────────────────────────────────────

    # Org -> SiteCollection
    for cid in collection_ids:
        conn.execute(
            "MATCH (o:Org {scopeId: 'org-root'}), (sc:SiteCollection {scopeId: $cid}) "
            "MERGE (o)-[:HAS_COLLECTION]->(sc)",
            {"cid": cid},
        )

    # SiteCollection -> Site  AND  Org -> Site (standalone)
    for sid in site_ids:
        coll_id = site_collection_map.get(sid)
        if coll_id and coll_id in collection_ids:
            conn.execute(
                "MATCH (sc:SiteCollection {scopeId: $cid}), (s:Site {scopeId: $sid}) "
                "MERGE (sc)-[:CONTAINS_SITE]->(s)",
                {"cid": coll_id, "sid": sid},
            )
        else:
            conn.execute(
                "MATCH (o:Org {scopeId: 'org-root'}), (s:Site {scopeId: $sid}) "
                "MERGE (o)-[:HAS_SITE]->(s)",
                {"sid": sid},
            )

    # Site -> Device
    for serial, site_id in device_site_map.items():
        if site_id in site_ids:
            conn.execute(
                "MATCH (s:Site {scopeId: $sid}), (d:Device {serial: $serial}) "
                "MERGE (s)-[:HAS_DEVICE]->(d)",
                {"sid": site_id, "serial": serial},
            )

    # DeviceGroup -> Device (from deviceGroupId on each device)
    _populate_device_group_membership(conn, devices, summary)

    # ── Config profiles (library level) ──────────────────────────
    for category in _CONFIG_CATEGORIES:
        profiles = _fetch_config_profiles(client, category)
        for p in profiles:
            pid = str(p.get("id", p.get("name", "")))
            if not pid:
                continue
            conn.execute(
                "MERGE (cp:ConfigProfile {id: $pid}) "
                "SET cp.name = $name, cp.category = $cat, "
                "cp.scopeId = $sid, cp.deviceFunction = $df, cp.objectType = $ot",
                {
                    "pid": f"{category}:{pid}",
                    "name": p.get("name", pid),
                    "cat": category,
                    "sid": p.get("scopeId", p.get("scope_id", "")),
                    "df": p.get("deviceFunction", p.get("device_function", "")),
                    "ot": p.get("objectType", p.get("object_type", "")),
                },
            )
            # Org -> ConfigProfile (library level)
            conn.execute(
                "MATCH (o:Org {scopeId: 'org-root'}), (cp:ConfigProfile {id: $pid}) "
                "MERGE (o)-[:HAS_CONFIG]->(cp)",
                {"pid": f"{category}:{pid}"},
            )
            summary["config_profiles"] += 1

    logger.info("population_done", **{k: v for k, v in summary.items() if k != "errors"})
    return summary


def _populate_device_group_membership(
    conn,
    devices: list[dict],
    summary: dict,
) -> None:
    """Populate DeviceGroup -> Device relationships from device inventory data.

    The device-inventory API includes deviceGroupId on each device,
    which we match against the DeviceGroup.scopeId already in the graph.
    """
    linked = 0
    for d in devices:
        serial = str(d.get("serialNumber", d.get("serial", "")))
        dg_id = str(d.get("deviceGroupId", "") or "")
        if not serial or not dg_id:
            continue
        conn.execute(
            "MATCH (dg:DeviceGroup {scopeId: $dgid}), (d:Device {serial: $serial}) "
            "MERGE (dg)-[:HAS_MEMBER]->(d)",
            {"dgid": dg_id, "serial": serial},
        )
        linked += 1
    logger.info("device_group_membership", linked=linked)
