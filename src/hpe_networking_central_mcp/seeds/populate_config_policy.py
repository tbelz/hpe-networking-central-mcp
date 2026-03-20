#!/usr/bin/env python3
"""Populate config policy layer — discover categories, profiles, and scope assignments.

Dynamically discovers configuration categories from the Central API,
fetches library-level profiles with extended metadata, creates scope
assignment relationships (*_ASSIGNS_CONFIG edges), and computes
EFFECTIVE_CONFIG edges per device by walking the scope hierarchy.

For atomic categories the closest scope wins (Device > Group > Site >
Collection > Org).  For additive categories all scope levels contribute.

Merge strategy is detected by inspecting the API response shape:
  - A list of multiple named profiles → additive (e.g., wlan-ssids)
  - A singleton object / single-element list → atomic (e.g., ntp)
  - Ambiguous or error → unknown
"""

import json
import sys

from central_helpers import api, graph, CentralAPIError


# ── Category discovery ───────────────────────────────────────────────

def discover_categories() -> list[str]:
    """Discover all config categories by listing the config API root."""
    try:
        resp = api.get("network-config/v1alpha1")
        if isinstance(resp, dict):
            # The root endpoint may return available categories
            for key in ("categories", "items", "result"):
                if key in resp and isinstance(resp[key], list):
                    return [c if isinstance(c, str) else c.get("name", c.get("category", ""))
                            for c in resp[key] if c]
        if isinstance(resp, list):
            return [c if isinstance(c, str) else c.get("name", c.get("category", ""))
                    for c in resp if c]
    except CentralAPIError as exc:
        print(f"Warning: category discovery failed: [{exc.status_code}] {exc.message}",
              file=sys.stderr)

    # Fallback: well-known categories
    return [
        "wlan-ssids",
        "sw-port-profiles",
        "gw-port-profiles",
        "roles",
        "server-groups",
        "ntp",
        "dns",
        "dhcp-pools",
        "acls",
        "aaa",
        "snmp",
    ]


# ── Merge strategy detection ────────────────────────────────────────

def _infer_merge_strategy_from_items(items: list[dict] | None) -> str:
    """Infer merge strategy from pre-fetched library-level items.

    - list of 2+ items with distinct names → additive
    - single item or singleton list → atomic
    - empty or ambiguous → unknown
    """
    if items is None or len(items) == 0:
        return "unknown"

    # Multiple items with distinct names → additive (e.g., SSID list)
    names = {i.get("name", "") for i in items if isinstance(i, dict)}
    if len(items) >= 2 and len(names) >= 2:
        return "additive"

    # Singleton → atomic (e.g., NTP settings — only one object per scope)
    if len(items) == 1:
        return "atomic"

    return "unknown"


def _extract_items(resp, category: str) -> list[dict] | None:
    """Extract the list of config items from a config API response."""
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for key in ("items", "result", category):
            if key in resp and isinstance(resp[key], list):
                return resp[key]
        # Singleton object with a name field
        if "name" in resp:
            return [resp]
    return None


# ── Profile fetching ────────────────────────────────────────────────

def fetch_library_profiles(category: str) -> tuple[list[dict], str]:
    """Fetch library-level config profiles and infer merge strategy in one call.

    Returns (profiles, merge_strategy) to avoid a redundant API round-trip.
    """
    try:
        resp = api.get(
            f"network-config/v1alpha1/{category}",
            params={"view-type": "LIBRARY"},
        )
        items = _extract_items(resp, category)
        profiles = items or []
        merge_strategy = _infer_merge_strategy_from_items(items)
        return profiles, merge_strategy
    except CentralAPIError as exc:
        print(f"  Warning: fetch {category} failed: [{exc.status_code}] {exc.message}",
              file=sys.stderr)
        return [], "unknown"


def fetch_scope_profiles(category: str, scope_id: str, scope_type: str) -> list[dict]:
    """Fetch config profiles assigned at a specific scope."""
    try:
        resp = api.get(
            f"network-config/v1alpha1/{category}",
            params={"scopeId": scope_id, "scopeType": scope_type},
        )
        items = _extract_items(resp, category)
        return items or []
    except CentralAPIError:
        return []


# ── Graph insertion ──────────────────────────────────────────────────

def upsert_profile(category: str, profile: dict, merge_strategy: str) -> str:
    """Insert or update a ConfigProfile node with extended metadata.

    Returns the profile ID used as the primary key.
    """
    pid = profile.get("id", profile.get("name", ""))
    if not pid:
        return ""

    full_id = f"{category}:{pid}"

    graph.execute(
        "MERGE (cp:ConfigProfile {id: $pid}) "
        "SET cp.name = $name, cp.category = $cat, "
        "cp.scopeId = $sid, cp.deviceFunction = $df, cp.objectType = $ot, "
        "cp.isDefault = $isDef, cp.isEditable = $isEdit, "
        "cp.deviceScopeOnly = $dso, cp.mergeStrategy = $ms, "
        "cp.assignedScopeIds = $asi, cp.assignedDeviceFunctions = $adf",
        {
            "pid": full_id,
            "name": profile.get("name", pid),
            "cat": category,
            "sid": profile.get("scopeId", profile.get("scope_id", "")),
            "df": profile.get("deviceFunction", profile.get("device_function", "")),
            "ot": profile.get("objectType", profile.get("object_type", "")),
            "isDef": bool(profile.get("isDefault", profile.get("is_default", False))),
            "isEdit": bool(profile.get("isEditable", profile.get("is_editable", True))),
            "dso": bool(profile.get("deviceScopeOnly", profile.get("device_scope_only", False))),
            "ms": merge_strategy,
            "asi": json.dumps(profile.get("assignedScopeIds", profile.get("assigned_scope_ids", []))),
            "adf": json.dumps(profile.get("assignedDeviceFunctions",
                                          profile.get("assigned_device_functions", []))),
        },
    )
    return full_id


def link_profile_to_scope(scope_label: str, scope_key: str, scope_id: str,
                          profile_id: str, rel_type: str) -> None:
    """Create a relationship from a scope node to a ConfigProfile."""
    graph.execute(
        f"MATCH (s:{scope_label} {{{scope_key}: $sid}}), "
        f"(cp:ConfigProfile {{id: $pid}}) "
        f"MERGE (s)-[:{rel_type}]->(cp)",
        {"sid": scope_id, "pid": profile_id},
    )


# ── Scope assignment resolution ─────────────────────────────────────

def resolve_scope_assignments(category: str, merge_strategy: str) -> dict:
    """Resolve config assignments across all known scopes."""
    stats = {"org": 0, "collection": 0, "site": 0, "group": 0, "device": 0}

    # Org-level
    org_profiles = fetch_scope_profiles(category, "org-root", "org")
    for p in org_profiles:
        pid = upsert_profile(category, p, merge_strategy)
        if pid:
            link_profile_to_scope("Org", "scopeId", "org-root", pid, "ORG_ASSIGNS_CONFIG")
            stats["org"] += 1

    # Site-collections
    collections = graph.query("MATCH (sc:SiteCollection) RETURN sc.scopeId")
    for row in collections:
        sc_id = row.get("sc.scopeId", "")
        if not sc_id:
            continue
        profiles = fetch_scope_profiles(category, sc_id, "collection")
        for p in profiles:
            pid = upsert_profile(category, p, merge_strategy)
            if pid:
                link_profile_to_scope("SiteCollection", "scopeId", sc_id, pid,
                                      "COLLECTION_ASSIGNS_CONFIG")
                stats["collection"] += 1

    # Sites
    sites = graph.query("MATCH (s:Site) RETURN s.scopeId")
    for row in sites:
        s_id = row.get("s.scopeId", "")
        if not s_id:
            continue
        profiles = fetch_scope_profiles(category, s_id, "site")
        for p in profiles:
            pid = upsert_profile(category, p, merge_strategy)
            if pid:
                link_profile_to_scope("Site", "scopeId", s_id, pid, "SITE_ASSIGNS_CONFIG")
                stats["site"] += 1

    # Device-groups
    groups = graph.query("MATCH (dg:DeviceGroup) RETURN dg.scopeId")
    for row in groups:
        dg_id = row.get("dg.scopeId", "")
        if not dg_id:
            continue
        profiles = fetch_scope_profiles(category, dg_id, "device-group")
        for p in profiles:
            pid = upsert_profile(category, p, merge_strategy)
            if pid:
                link_profile_to_scope("DeviceGroup", "scopeId", dg_id, pid,
                                      "GROUP_ASSIGNS_CONFIG")
                stats["group"] += 1

    return stats


# ── Effective config computation ─────────────────────────────────────

def compute_effective_config(category: str, merge_strategy: str) -> int:
    """Walk the scope hierarchy and write EFFECTIVE_CONFIG edges for every device.

    For **atomic** categories the closest (most-specific) scope wins:
        Device > DeviceGroup > Site > SiteCollection > Org
    For **additive** categories every scope that assigns a profile contributes.

    Returns the number of EFFECTIVE_CONFIG edges created.
    """
    edges = 0

    # Precedence layers, from highest to lowest.  Each entry:
    #   (relationship_type, path_from_device, scope_label)
    # We walk the graph *from* each Device backward through each scope.
    layers = [
        # Device-level override (direct assignment)
        ("DEVICE_ASSIGNS_CONFIG", "(d:Device)-[:DEVICE_ASSIGNS_CONFIG]->", "device", "d.serial"),
        # DeviceGroup (device ←HAS_MEMBER- group)
        ("GROUP_ASSIGNS_CONFIG",
         "(d:Device)<-[:HAS_MEMBER]-(dg:DeviceGroup)-[:GROUP_ASSIGNS_CONFIG]->",
         "device-group", "dg.scopeId"),
        # Site (device ←HAS_DEVICE- site)
        ("SITE_ASSIGNS_CONFIG",
         "(d:Device)<-[:HAS_DEVICE]-(s:Site)-[:SITE_ASSIGNS_CONFIG]->",
         "site", "s.scopeId"),
        # SiteCollection (device ←HAS_DEVICE- site ←CONTAINS_SITE- collection)
        ("COLLECTION_ASSIGNS_CONFIG",
         "(d:Device)<-[:HAS_DEVICE]-(s:Site)<-[:CONTAINS_SITE]-(sc:SiteCollection)-[:COLLECTION_ASSIGNS_CONFIG]->",
         "collection", "sc.scopeId"),
        # Org
        ("ORG_ASSIGNS_CONFIG",
         "(d:Device)<-[:HAS_DEVICE]-(:Site)<-[:HAS_SITE|CONTAINS_SITE]-(:Org|SiteCollection)-[:ORG_ASSIGNS_CONFIG]->",
         "org", "'org-root'"),
    ]

    # Get all devices
    devices = graph.query("MATCH (d:Device) RETURN d.serial")

    for dev_row in devices:
        serial = dev_row.get("d.serial", "")
        if not serial:
            continue

        # Collect profiles that apply to this device for this category
        seen_profiles: set[str] = set()  # profile ids already covered
        found_atomic = False  # for atomic: stop after first scope with profiles

        for _rel, path, scope_label, scope_id_expr in layers:
            if merge_strategy == "atomic" and found_atomic:
                break  # closest scope already won

            cypher = (
                f"MATCH {path}(cp:ConfigProfile) "
                f"WHERE d.serial = $serial AND cp.category = $cat "
                f"RETURN cp.id AS pid, {scope_id_expr} AS scopeId"
            )
            try:
                rows = graph.query(cypher, {"serial": serial, "cat": category})
            except Exception:
                continue

            scope_has_profiles = False
            for row in rows:
                pid = row.get("pid", "")
                scope_id = str(row.get("scopeId", ""))
                if not pid or pid in seen_profiles:
                    continue
                seen_profiles.add(pid)
                scope_has_profiles = True

                graph.execute(
                    "MATCH (d:Device {serial: $serial}), (cp:ConfigProfile {id: $pid}) "
                    "MERGE (d)-[r:EFFECTIVE_CONFIG]->(cp) "
                    "SET r.sourceScope = $scope, r.sourceScopeId = $scopeId",
                    {"serial": serial, "pid": pid,
                     "scope": scope_label, "scopeId": scope_id},
                )
                edges += 1

            if scope_has_profiles:
                found_atomic = True  # relevant only for atomic

    return edges


# ── Main ─────────────────────────────────────────────────────────────

def main():
    summary = {
        "categories_discovered": 0,
        "categories_with_profiles": 0,
        "total_profiles": 0,
        "merge_strategies": {},
        "scope_assignments": {},
        "errors": [],
    }

    # Step 1: Discover categories
    print("Discovering config categories...", file=sys.stderr)
    categories = discover_categories()
    summary["categories_discovered"] = len(categories)
    print(f"  Found {len(categories)} categories", file=sys.stderr)

    # Step 2: For each category, fetch library profiles and infer merge strategy
    for cat in categories:
        print(f"  Processing {cat}...", file=sys.stderr)

        profiles, merge_strategy = fetch_library_profiles(cat)
        summary["merge_strategies"][cat] = merge_strategy

        if not profiles:
            continue

        summary["categories_with_profiles"] += 1

        # Insert library-level profiles and link to Org
        for p in profiles:
            pid = upsert_profile(cat, p, merge_strategy)
            if pid:
                link_profile_to_scope("Org", "scopeId", "org-root", pid, "ORG_ASSIGNS_CONFIG")
                # Also keep the legacy HAS_CONFIG relationship
                graph.execute(
                    "MATCH (o:Org {scopeId: 'org-root'}), (cp:ConfigProfile {id: $pid}) "
                    "MERGE (o)-[:HAS_CONFIG]->(cp)",
                    {"pid": pid},
                )
                summary["total_profiles"] += 1

        # Step 3: Resolve per-scope assignments
        stats = resolve_scope_assignments(cat, merge_strategy)
        summary["scope_assignments"][cat] = stats

    # Step 4: Compute EFFECTIVE_CONFIG edges per device from the graph hierarchy
    print("Computing effective config per device...", file=sys.stderr)
    effective_edges = 0
    for cat in categories:
        ms = summary["merge_strategies"].get(cat, "unknown")
        effective_edges += compute_effective_config(cat, ms)
    summary["effective_config_edges"] = effective_edges
    print(f"  Created {effective_edges} EFFECTIVE_CONFIG edges", file=sys.stderr)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
