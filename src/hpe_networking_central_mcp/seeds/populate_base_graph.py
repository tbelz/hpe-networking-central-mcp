#!/usr/bin/env python3
"""Populate the base configuration graph from Aruba Central APIs.

Fetches structural data (sites, site-collections, device-groups, devices)
and library-level config profiles, then inserts them into the Kùzu graph
as nodes and relationships.

This is the foundational graph population — run it before any enrichment scripts.

Hierarchy built:
  Org → SiteCollection → Site → Device
  DeviceGroup → Device (cross-cutting membership)
  Org → ConfigProfile (library-level config metadata)
"""

import json
import sys

from central_helpers import api, graph, CentralAPIError

PAGE_LIMIT = 100

# Config categories to index at the library level
CONFIG_CATEGORIES = [
    "wlan-ssids",
    "sw-port-profiles",
    "gw-port-profiles",
    "roles",
    "server-groups",
]


def fetch_all_devices() -> list[dict]:
    """Fetch all devices from the inventory API."""
    return api.paginate(
        "network-monitoring/v1/device-inventory",
        page_size=PAGE_LIMIT,
    )


def fetch_sites() -> list[dict]:
    """Fetch all sites from Scope Management API."""
    return api.paginate("network-config/v1/sites", page_size=PAGE_LIMIT)


def fetch_site_collections() -> list[dict]:
    """Fetch all site-collections."""
    return api.paginate("network-config/v1/site-collections", page_size=PAGE_LIMIT)


def fetch_device_groups() -> list[dict]:
    """Fetch all device-groups."""
    return api.paginate("network-config/v1/device-groups", page_size=PAGE_LIMIT)


def fetch_config_profiles(category: str) -> list[dict]:
    """Fetch library-level config profiles for a given category."""
    try:
        resp = api.get(
            f"network-config/v1alpha1/{category}",
            params={"view-type": "LIBRARY"},
        )
        if isinstance(resp, list):
            return resp
        for key in ("items", "result", category):
            if key in resp and isinstance(resp[key], list):
                return resp[key]
        if "name" in resp:
            return [resp]
        return []
    except CentralAPIError as exc:
        print(f"Warning: config fetch failed for {category}: [{exc.status_code}] {exc.message}",
              file=sys.stderr)
        return []


def main():
    summary = {
        "sites": 0,
        "site_collections": 0,
        "device_groups": 0,
        "devices": 0,
        "config_profiles": 0,
        "errors": [],
    }

    # ── Fetch all data ────────────────────────────────────────────
    print("Fetching data from Central APIs...", file=sys.stderr)

    try:
        sites = fetch_sites()
        print(f"  Sites: {len(sites)}", file=sys.stderr)
    except CentralAPIError as exc:
        sites = []
        summary["errors"].append(f"sites: [{exc.status_code}] {exc.message}")

    try:
        collections = fetch_site_collections()
        print(f"  Site Collections: {len(collections)}", file=sys.stderr)
    except CentralAPIError as exc:
        collections = []
        summary["errors"].append(f"site_collections: [{exc.status_code}] {exc.message}")

    try:
        groups = fetch_device_groups()
        print(f"  Device Groups: {len(groups)}", file=sys.stderr)
    except CentralAPIError as exc:
        groups = []
        summary["errors"].append(f"device_groups: [{exc.status_code}] {exc.message}")

    try:
        devices = fetch_all_devices()
        print(f"  Devices: {len(devices)}", file=sys.stderr)
    except CentralAPIError as exc:
        devices = []
        summary["errors"].append(f"devices: [{exc.status_code}] {exc.message}")

    # ── Insert nodes ──────────────────────────────────────────────
    print("Populating graph...", file=sys.stderr)

    # Org node (synthetic root)
    graph.execute(
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
        graph.execute(
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
    site_collection_map: dict[str, str] = {}
    for s in sites:
        sid = str(s.get("id", s.get("scopeId", "")))
        if not sid:
            continue
        site_ids.add(sid)
        coll_id = str(s.get("collectionId", s.get("collection_id", "") or ""))
        coll_name = s.get("collectionName", s.get("collection_name", "") or "")
        if coll_id:
            site_collection_map[sid] = coll_id
        tz_obj = s.get("timezone", {})
        tz_id = tz_obj.get("timezoneId", "") if isinstance(tz_obj, dict) else str(tz_obj or "")
        graph.execute(
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
        graph.execute(
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
    device_site_map: dict[str, str] = {}
    for d in devices:
        serial = str(d.get("serialNumber", d.get("serial", "")))
        if not serial:
            continue
        site_id = str(d.get("siteId", d.get("site_id", "") or ""))
        if site_id:
            device_site_map[serial] = site_id
        graph.execute(
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
        graph.execute(
            "MATCH (o:Org {scopeId: 'org-root'}), (sc:SiteCollection {scopeId: $cid}) "
            "MERGE (o)-[:HAS_COLLECTION]->(sc)",
            {"cid": cid},
        )

    # SiteCollection -> Site  AND  Org -> Site (standalone)
    for sid in site_ids:
        coll_id = site_collection_map.get(sid)
        if coll_id and coll_id in collection_ids:
            graph.execute(
                "MATCH (sc:SiteCollection {scopeId: $cid}), (s:Site {scopeId: $sid}) "
                "MERGE (sc)-[:CONTAINS_SITE]->(s)",
                {"cid": coll_id, "sid": sid},
            )
        else:
            graph.execute(
                "MATCH (o:Org {scopeId: 'org-root'}), (s:Site {scopeId: $sid}) "
                "MERGE (o)-[:HAS_SITE]->(s)",
                {"sid": sid},
            )

    # Site -> Device
    for serial, site_id in device_site_map.items():
        if site_id in site_ids:
            graph.execute(
                "MATCH (s:Site {scopeId: $sid}), (d:Device {serial: $serial}) "
                "MERGE (s)-[:HAS_DEVICE]->(d)",
                {"sid": site_id, "serial": serial},
            )

    # DeviceGroup -> Device
    linked = 0
    for d in devices:
        serial = str(d.get("serialNumber", d.get("serial", "")))
        dg_id = str(d.get("deviceGroupId", "") or "")
        if not serial or not dg_id:
            continue
        graph.execute(
            "MATCH (dg:DeviceGroup {scopeId: $dgid}), (d:Device {serial: $serial}) "
            "MERGE (dg)-[:HAS_MEMBER]->(d)",
            {"dgid": dg_id, "serial": serial},
        )
        linked += 1

    # ── Config profiles (library level) ──────────────────────────
    for category in CONFIG_CATEGORIES:
        profiles = fetch_config_profiles(category)
        for p in profiles:
            pid = str(p.get("id", p.get("name", "")))
            if not pid:
                continue
            graph.execute(
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
            graph.execute(
                "MATCH (o:Org {scopeId: 'org-root'}), (cp:ConfigProfile {id: $pid}) "
                "MERGE (o)-[:HAS_CONFIG]->(cp)",
                {"pid": f"{category}:{pid}"},
            )
            summary["config_profiles"] += 1

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
