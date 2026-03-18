"""Populate the Kùzu graph from Aruba Central APIs.

Fetches structural data (sites, site-collections, device-groups, devices)
and library-level config metadata, then inserts nodes and relationships.

Topology population (CONNECTED_TO / LINKED_TO / UnmanagedDevice) is separate
and lazily loaded via ``populate_topology()``.
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


# ── Topology population (lazy) ───────────────────────────────────────

_TOPOLOGY_PATH = "network-monitoring/v1/topology"


def _fetch_site_topology(client: CentralClient, site_id: str) -> dict:
    """Fetch L2 topology for a single site.

    Returns:
        Dict with ``devices`` and ``links`` lists (may be empty).
    """
    try:
        resp = client.get(f"{_TOPOLOGY_PATH}/{site_id}")
        return resp
    except CentralAPIError as exc:
        logger.warning("topology_fetch_failed", site_id=site_id, status=exc.status_code, msg=exc.message)
        return {"devices": [], "links": []}


def populate_topology(client: CentralClient, conn, site_ids: list[str]) -> dict:
    """Fetch per-site L2 topology and insert edges + unmanaged device nodes.

    For each site, calls ``GET /network-monitoring/v1/topology/{siteId}`` and
    processes the ``links`` array.  Managed devices must already exist as
    ``Device`` nodes.  Third-party LLDP neighbours (serial starting with
    ``tpd_``) are created as ``UnmanagedDevice`` nodes.

    One CONNECTED_TO (Device→Device) or LINKED_TO (Device→UnmanagedDevice)
    edge is created per device *pair*, aggregating multiple port-level links
    into comma-separated ``fromPorts``/``toPorts`` strings.

    Args:
        client: Authenticated CentralClient instance.
        conn: Kùzu Connection object.
        site_ids: List of site scopeId values to process.

    Returns:
        Summary dict with topology-specific counts.
    """
    summary = {
        "sites_processed": 0,
        "connected_to": 0,
        "linked_to": 0,
        "unmanaged_devices": 0,
        "errors": [],
    }

    for site_id in site_ids:
        topo = _fetch_site_topology(client, site_id)
        links = topo.get("links", [])
        if not links:
            summary["sites_processed"] += 1
            continue

        # Collect unmanaged device metadata from the topology ``devices`` list
        # so we can create richer UnmanagedDevice nodes.
        topo_device_info: dict[str, dict] = {}
        for td in topo.get("devices", []):
            serial = str(td.get("serial", ""))
            if serial:
                topo_device_info[serial] = td

        # Aggregate links by (from, to) device pair
        edge_map: dict[tuple[str, str], dict] = {}
        for link in links:
            from_serial = str(link.get("from", ""))
            to_serial = str(link.get("to", ""))
            if not from_serial or not to_serial:
                continue

            key = (from_serial, to_serial)
            if key not in edge_map:
                edge_map[key] = {
                    "fromPorts": [],
                    "toPorts": [],
                    "speed": link.get("speed", 0.0),
                    "edgeType": link.get("edgeType", ""),
                    "health": link.get("health", ""),
                    "lag": "",
                    "stpState": link.get("stpState", ""),
                    "isSibling": bool(link.get("isSibling", False)),
                }

            # Collect port names
            for p in link.get("fromPortList", []):
                name = p.get("name", "")
                if name:
                    edge_map[key]["fromPorts"].append(name)
                lag = p.get("lag", "")
                if lag:
                    edge_map[key]["lag"] = lag
            for p in link.get("toPortList", []):
                name = p.get("name", "")
                if name:
                    edge_map[key]["toPorts"].append(name)

        # Insert edges
        unmanaged_created: set[str] = set()
        for (from_serial, to_serial), props in edge_map.items():
            from_is_unmanaged = from_serial.startswith("tpd_")
            to_is_unmanaged = to_serial.startswith("tpd_")

            params = {
                "fromPorts": ",".join(props["fromPorts"]),
                "toPorts": ",".join(props["toPorts"]),
                "speed": float(props["speed"] or 0) / 1_000_000_000,  # bps → Gbps
                "edgeType": props["edgeType"],
                "health": props["health"],
                "lag": props["lag"],
                "stpState": props["stpState"],
                "isSibling": props["isSibling"],
            }

            if from_is_unmanaged and to_is_unmanaged:
                # Both unmanaged — skip (no rel table for that combo)
                continue
            elif not from_is_unmanaged and not to_is_unmanaged:
                # Device → Device
                conn.execute(
                    "MATCH (a:Device {serial: $a}), (b:Device {serial: $b}) "
                    "MERGE (a)-[c:CONNECTED_TO]->(b) "
                    "SET c.fromPorts = $fromPorts, c.toPorts = $toPorts, "
                    "c.speed = $speed, c.edgeType = $edgeType, c.health = $health, "
                    "c.lag = $lag, c.stpState = $stpState, c.isSibling = $isSibling",
                    {"a": from_serial, "b": to_serial, **params},
                )
                summary["connected_to"] += 1
            else:
                # One side is unmanaged
                managed = from_serial if not from_is_unmanaged else to_serial
                unmanaged = to_serial if not from_is_unmanaged else from_serial

                # Create UnmanagedDevice node if not already done
                if unmanaged not in unmanaged_created:
                    # Extract MAC from tpd_ prefix
                    mac = unmanaged[4:] if unmanaged.startswith("tpd_") else unmanaged
                    info = topo_device_info.get(unmanaged, {})
                    conn.execute(
                        "MERGE (u:UnmanagedDevice {mac: $mac}) "
                        "SET u.name = $name, u.model = $model, u.deviceType = $dt, "
                        "u.health = $health, u.status = $status, u.ipv4 = $ip, "
                        "u.siteId = $sid",
                        {
                            "mac": mac,
                            "name": info.get("name", "") or "",
                            "model": info.get("model", "") or "",
                            "dt": info.get("type", info.get("deviceFunction", "")) or "",
                            "health": str(info.get("health", "")) or "",
                            "status": info.get("status", "") or "",
                            "ip": info.get("ipv4", "") or "",
                            "sid": site_id,
                        },
                    )
                    # Site → UnmanagedDevice
                    conn.execute(
                        "MATCH (s:Site {scopeId: $sid}), (u:UnmanagedDevice {mac: $mac}) "
                        "MERGE (s)-[:HAS_UNMANAGED]->(u)",
                        {"sid": site_id, "mac": mac},
                    )
                    unmanaged_created.add(unmanaged)
                    summary["unmanaged_devices"] += 1

                mac = unmanaged[4:] if unmanaged.startswith("tpd_") else unmanaged
                conn.execute(
                    "MATCH (d:Device {serial: $d}), (u:UnmanagedDevice {mac: $mac}) "
                    "MERGE (d)-[l:LINKED_TO]->(u) "
                    "SET l.fromPorts = $fromPorts, l.toPorts = $toPorts, "
                    "l.speed = $speed, l.edgeType = $edgeType, l.health = $health, "
                    "l.lag = $lag, l.stpState = $stpState, l.isSibling = $isSibling",
                    {"d": managed, "mac": mac, **params},
                )
                summary["linked_to"] += 1

        summary["sites_processed"] += 1

    logger.info("topology_population_done", **{k: v for k, v in summary.items() if k != "errors"})
    return summary
