#!/usr/bin/env python3
"""Populate the config policy layer in the graph.

Fetches config profiles with scope assignment metadata from Central's
configuration API and creates:

- Enriched ConfigProfile nodes with annotation metadata (isDefault, isEditable, etc.)
- *_ASSIGNS_CONFIG edges linking scopes to their assigned profiles
- EFFECTIVE_CONFIG edges linking devices to their effective (resolved) profile set

The config API's ``detailed=true`` parameter returns ``@`` annotation blocks
containing ``aruba-annotation:scope_device_function`` — a list of scope
assignment bindings that tells us WHERE each profile is assigned. This is the
primary data source for building assignment edges.

Using ``effective=true`` at a site scope returns the full inherited/merged
profile set, which we use to build EFFECTIVE_CONFIG edges for devices.

Precedence (highest wins): Device > Device Group > Site > Site Collection > Global

Requires: populate_base_graph must have been run first (needs scope nodes).
"""

import ast
import json
import sys

from central_helpers import CentralAPIError, api, graph

# ── Configuration ────────────────────────────────────────────────────
# Categories are discovered dynamically from the graph's ApiEndpoint nodes
# (paths matching network-config/v1alpha1/{category}).  This list is only
# used as a last-resort fallback when the knowledge DB has not been loaded.
_FALLBACK_CATEGORIES = [
    "wlan-ssids",
    "sw-port-profiles",
    "gw-port-profiles",
    "roles",
    "server-groups",
    "dns-servers",
    "ntp-servers",
    "acls",
    "auth-servers",
    "auth-profiles",
]


def _discover_config_categories() -> list[str]:
    """Discover config categories from ApiEndpoint nodes in the graph.

    Queries for all paths matching ``network-config/v1alpha1/{category}`` and
    extracts the unique category slugs.  Falls back to the static list above
    when the knowledge DB hasn't been loaded.
    """
    try:
        rows = graph.query(
            "MATCH (e:ApiEndpoint) "
            "WHERE e.path CONTAINS 'network-config/v1alpha1/' "
            "RETURN DISTINCT e.path AS path"
        )
    except Exception:
        rows = []

    categories: set[str] = set()
    for r in rows:
        path = r.get("path", "")
        # Extract the segment after network-config/v1alpha1/
        parts = path.split("network-config/v1alpha1/")
        if len(parts) < 2:
            continue
        slug = parts[1].split("/")[0].split("?")[0].strip()
        # Skip path parameters like {name}
        if slug and not slug.startswith("{"):
            categories.add(slug)

    if categories:
        return sorted(categories)

    print("No config categories found in graph, using fallback list", file=sys.stderr)
    return list(_FALLBACK_CATEGORIES)


# Heuristic to infer merge strategy from a category slug.  Categories whose
# API allows multiple named instances at the same scope are "additive" (e.g.
# wlan-ssids — you can stack SSIDs).  Categories that have a single unnamed
# or singleton config blob per scope are "atomic" (only one value, overwritten
# by the winning scope).  The API's response shape (list of profiles vs single
# object) and common naming patterns drive this heuristic.  Unknown categories
# default to "unknown" so the graph consumer can handle them safely.
_ADDITIVE_PATTERNS = {"wlan", "ssid", "profile", "acl", "role", "server-group", "auth"}
_ATOMIC_PATTERNS = {"dns", "ntp", "snmp", "syslog", "radius", "tacacs", "nap"}


def _infer_merge_strategy(category: str, profiles: list) -> str:
    """Infer whether a config category is additive or atomic.

    Returns "additive", "atomic", or "unknown".
    """
    slug = category.lower()
    # Check known patterns
    for pat in _ADDITIVE_PATTERNS:
        if pat in slug:
            return "additive"
    for pat in _ATOMIC_PATTERNS:
        if pat in slug:
            return "atomic"
    # Heuristic: if we got multiple distinct named profiles, likely additive
    if len(profiles) > 1:
        return "additive"
    return "unknown"

# Maps scope_type values from API annotations to graph relationship names.
# If Central introduces new scope types, they will appear in the
# ``unrecognized_scope_types`` summary list so you can add them here.
SCOPE_TYPE_TO_REL = {
    "org": "ORG_ASSIGNS_CONFIG",
    "collection": "COLLECTION_ASSIGNS_CONFIG",
    "site": "SITE_ASSIGNS_CONFIG",
    "device-group": "GROUP_ASSIGNS_CONFIG",
    "device": "DEVICE_ASSIGNS_CONFIG",
}

# Graph label used to MATCH the scope node for each scope_type.
SCOPE_TYPE_TO_LABEL = {
    "org": "Org",
    "collection": "SiteCollection",
    "site": "Site",
    "device-group": "DeviceGroup",
    "device": "Device",
}

# Primary key field on each scope node type.
SCOPE_TYPE_TO_PK = {
    "org": "scopeId",
    "collection": "scopeId",
    "site": "scopeId",
    "device-group": "scopeId",
    "device": "serial",
}

# Precedence weight for determining which assignment scope "wins"
# when building EFFECTIVE_CONFIG source annotations.
SCOPE_PRECEDENCE = {
    "org": 1,
    "collection": 2,
    "site": 3,
    "device-group": 4,
    "device": 5,
}


# ── Helpers ──────────────────────────────────────────────────────────


def parse_scope_device_function(raw):
    """Parse the ``scope_device_function`` annotation value.

    The API returns this field inconsistently: sometimes proper JSON with
    double quotes, sometimes Python dict repr with single quotes.  We try
    ``json.loads`` first, then ``ast.literal_eval`` as a fallback.
    """
    if not raw:
        return []
    if isinstance(raw, list):
        return raw
    s = str(raw).strip()
    if not s:
        return []
    # Try JSON first (most common)
    try:
        result = json.loads(s)
        if isinstance(result, list):
            return result
    except (json.JSONDecodeError, ValueError):
        pass
    # Fallback: Python literal (single-quoted dicts)
    try:
        result = ast.literal_eval(s)
        if isinstance(result, list):
            return result
    except (ValueError, SyntaxError):
        pass
    return []


def extract_profiles(resp, category):
    """Extract profile list from an API response.

    Response wrapper keys vary by category (``profile``, ``server-group``,
    ``role``, etc.).  We try several strategies before giving up.
    """
    if isinstance(resp, list):
        return resp
    if not isinstance(resp, dict):
        return []
    # Try well-known keys
    for key in ("items", "result", category):
        if key in resp and isinstance(resp[key], list):
            return resp[key]
    # Try first list-valued key (skip annotation blocks)
    for key, val in resp.items():
        if isinstance(val, list) and key != "@":
            return val
    # Single-profile response
    if "name" in resp:
        return [resp]
    return []


def parse_annotations(profile):
    """Extract annotation metadata from a profile's ``@`` block."""
    ann = profile.get("@", {})
    if not ann:
        return {
            "object_type": "",
            "is_default": False,
            "is_editable": True,
            "device_scope_only": False,
            "scope_device_function": [],
        }
    return {
        "object_type": str(ann.get("aruba-annotation:object_type", "")),
        "is_default": bool(ann.get("aruba-annotation:is_default", False)),
        "is_editable": bool(ann.get("aruba-annotation:is_editable", True)),
        "device_scope_only": bool(ann.get("aruba-annotation:device_scope_only", False)),
        "scope_device_function": parse_scope_device_function(
            ann.get("aruba-annotation:scope_device_function")
        ),
    }


def fetch_config_at_scope(category, scope_id, scope_type, *, effective=False):
    """Fetch config profiles at a specific scope with detailed annotations."""
    params = {
        "scope-id": str(scope_id),
        "scope-type": scope_type,
        "detailed": "true",
    }
    if effective:
        params["effective"] = "true"
    return api.get(f"network-config/v1alpha1/{category}", params=params)


def resolve_effective_source(sdf_entries):
    """Pick the highest-precedence scope assignment from annotation entries.

    Returns ``(scope_type, scope_id, scope_name)`` for the winning scope.
    """
    if not sdf_entries:
        return ("unknown", "", "")
    best = max(
        sdf_entries,
        key=lambda e: SCOPE_PRECEDENCE.get(str(e.get("scope_type", "")), 0),
    )
    return (
        str(best.get("scope_type", "unknown")),
        str(best.get("scope_id", "")),
        str(best.get("scope_name", "")),
    )


# ── Main ─────────────────────────────────────────────────────────────


def main():    # ── Discover config categories dynamically ───────────────
    config_categories = _discover_config_categories()
    # ── Load scope nodes from graph ──────────────────────────────
    sites = graph.query("MATCH (s:Site) RETURN s.scopeId AS id, s.name AS name")
    groups = graph.query(
        "MATCH (dg:DeviceGroup) RETURN dg.scopeId AS id, dg.name AS name"
    )

    if not sites:
        print(
            json.dumps(
                {"error": "No sites in graph. Run populate_base_graph first."}
            )
        )
        sys.exit(1)

    print(
        f"Config policy: {len(sites)} sites, {len(groups)} device groups, "
        f"{len(config_categories)} categories to probe",
        file=sys.stderr,
    )

    # Map site → devices and group → devices (for EFFECTIVE_CONFIG)
    site_device_rows = graph.query(
        "MATCH (s:Site)-[:HAS_DEVICE]->(d:Device) "
        "RETURN s.scopeId AS scopeId, d.serial AS serial"
    )
    site_devices: dict[str, list[str]] = {}
    for r in site_device_rows:
        site_devices.setdefault(r["scopeId"], []).append(r["serial"])

    # ── Summary tracking ─────────────────────────────────────────
    summary: dict = {
        "sites_processed": 0,
        "groups_processed": 0,
        "categories_discovered": len(config_categories),
        "categories_supported": [],
        "categories_failed": [],
        "config_profiles_created": 0,
        "assignment_edges": 0,
        "effective_edges": 0,
        "errors": [],
        "warnings": [],
        "unrecognized_scope_types": [],
    }

    # Dedup sets
    profiles_merged: set[str] = set()
    assignments_created: set[tuple] = set()
    effective_created: set[tuple] = set()

    # ── Phase 1: Site-effective queries ──────────────────────────
    # Fetching effective=true at each site returns all profiles active
    # there (global + collection + site assignments, resolved by
    # precedence).  Annotations reveal the assignment source.
    for category in config_categories:
        cat_ok = False

        for site in sites:
            site_id = site["id"]
            site_name = site.get("name", "")
            try:
                resp = fetch_config_at_scope(
                    category, site_id, "site", effective=True
                )
            except CentralAPIError as exc:
                if exc.status_code == 400:
                    continue  # category not supported at this scope
                summary["errors"].append(
                    f"{category}@site:{site_id}: [{exc.status_code}] {exc.message}"
                )
                continue

            cat_ok = True
            profiles = extract_profiles(resp, category)

            for profile in profiles:
                pid = str(profile.get("id", profile.get("name", "")))
                if not pid:
                    continue
                profile_key = f"{category}:{pid}"
                ann = parse_annotations(profile)

                # ── MERGE ConfigProfile node ─────────────────────
                if profile_key not in profiles_merged:
                    profiles_merged.add(profile_key)
                    merge_strat = _infer_merge_strategy(category, profiles)
                    graph.execute(
                        "MERGE (cp:ConfigProfile {id: $pid}) "
                        "SET cp.name = $name, cp.category = $cat, "
                        "cp.objectType = $ot, "
                        "cp.isDefault = $isDef, cp.isEditable = $isEdit, "
                        "cp.deviceScopeOnly = $dso, cp.mergeStrategy = $ms",
                        {
                            "pid": profile_key,
                            "name": profile.get("name", pid),
                            "cat": category,
                            "ot": ann["object_type"],
                            "isDef": ann["is_default"],
                            "isEdit": ann["is_editable"],
                            "dso": ann["device_scope_only"],
                            "ms": merge_strat,
                        },
                    )
                    summary["config_profiles_created"] += 1

                # ── Build *_ASSIGNS_CONFIG edges from annotations ─
                sdf_entries = ann["scope_device_function"]
                scope_ids_agg: list[str] = []
                device_fns_agg: list[str] = []

                for entry in sdf_entries:
                    e_scope_type = str(entry.get("scope_type", ""))
                    e_scope_id = str(entry.get("scope_id", ""))
                    e_scope_name = str(entry.get("scope_name", ""))
                    e_device_fn = str(entry.get("device_function", ""))

                    if e_scope_id:
                        scope_ids_agg.append(e_scope_id)
                    if e_device_fn:
                        device_fns_agg.append(e_device_fn)

                    rel_name = SCOPE_TYPE_TO_REL.get(e_scope_type)
                    if not rel_name:
                        if e_scope_type and e_scope_type not in summary["unrecognized_scope_types"]:
                            summary["unrecognized_scope_types"].append(e_scope_type)
                            summary["warnings"].append(
                                f"Unrecognized scope_type '{e_scope_type}' "
                                f"in annotation for {profile_key}"
                            )
                        continue

                    label = SCOPE_TYPE_TO_LABEL[e_scope_type]
                    pk_field = SCOPE_TYPE_TO_PK[e_scope_type]

                    # Dedup assignment edges
                    asgn_key = (rel_name, e_scope_id, profile_key, e_device_fn)
                    if asgn_key in assignments_created:
                        continue
                    assignments_created.add(asgn_key)

                    # Build MATCH clause — Org uses 'org-root' as synthetic PK
                    if e_scope_type == "org":
                        match_scope = f"(scope:{label} {{scopeId: 'org-root'}})"
                    else:
                        match_scope = f"(scope:{label} {{{pk_field}: $scopeId}})"

                    try:
                        graph.execute(
                            f"MATCH {match_scope}, "
                            f"(cp:ConfigProfile {{id: $pid}}) "
                            f"MERGE (scope)-[a:{rel_name}]->(cp) "
                            f"SET a.deviceFunctions = $df, "
                            f"a.isDefault = $isDef",
                            {
                                "scopeId": e_scope_id,
                                "pid": profile_key,
                                "df": e_device_fn,
                                "isDef": ann["is_default"],
                            },
                        )
                        summary["assignment_edges"] += 1
                    except Exception as exc:
                        summary["warnings"].append(
                            f"Assignment edge {rel_name} "
                            f"{e_scope_id}→{profile_key}: {exc}"
                        )

                # Update aggregated scope/function metadata on profile
                if scope_ids_agg or device_fns_agg:
                    graph.execute(
                        "MATCH (cp:ConfigProfile {id: $pid}) "
                        "SET cp.assignedScopeIds = $sids, "
                        "cp.assignedDeviceFunctions = $dfs",
                        {
                            "pid": profile_key,
                            "sids": ",".join(sorted(set(scope_ids_agg))),
                            "dfs": ",".join(sorted(set(device_fns_agg))),
                        },
                    )

                # ── EFFECTIVE_CONFIG edges for devices at this site ──
                source_scope, source_id, source_name = resolve_effective_source(
                    sdf_entries
                )
                for serial in site_devices.get(site_id, []):
                    eff_key = (serial, profile_key)
                    if eff_key in effective_created:
                        continue
                    effective_created.add(eff_key)
                    graph.execute(
                        "MATCH (d:Device {serial: $serial}), "
                        "(cp:ConfigProfile {id: $pid}) "
                        "MERGE (d)-[e:EFFECTIVE_CONFIG]->(cp) "
                        "SET e.sourceScope = $ss, "
                        "e.sourceScopeId = $ssid, "
                        "e.sourceScopeName = $ssn",
                        {
                            "serial": serial,
                            "pid": profile_key,
                            "ss": source_scope,
                            "ssid": source_id,
                            "ssn": source_name,
                        },
                    )
                    summary["effective_edges"] += 1

            summary["sites_processed"] += 1

        # ── Phase 2: Device-group queries ────────────────────────
        # Groups are outside the hierarchy — fetch their explicit
        # assignments separately.
        for grp in groups:
            grp_id = grp["id"]
            try:
                resp = fetch_config_at_scope(
                    category, grp_id, "device-group", effective=False
                )
            except CentralAPIError as exc:
                if exc.status_code == 400:
                    continue
                summary["errors"].append(
                    f"{category}@device-group:{grp_id}: "
                    f"[{exc.status_code}] {exc.message}"
                )
                continue

            cat_ok = True
            profiles = extract_profiles(resp, category)

            for profile in profiles:
                pid = str(profile.get("id", profile.get("name", "")))
                if not pid:
                    continue
                profile_key = f"{category}:{pid}"
                ann = parse_annotations(profile)

                # MERGE ConfigProfile node (may already exist from site phase)
                if profile_key not in profiles_merged:
                    profiles_merged.add(profile_key)
                    merge_strat = _infer_merge_strategy(category, profiles)
                    graph.execute(
                        "MERGE (cp:ConfigProfile {id: $pid}) "
                        "SET cp.name = $name, cp.category = $cat, "
                        "cp.objectType = $ot, "
                        "cp.isDefault = $isDef, cp.isEditable = $isEdit, "
                        "cp.deviceScopeOnly = $dso, cp.mergeStrategy = $ms",
                        {
                            "pid": profile_key,
                            "name": profile.get("name", pid),
                            "cat": category,
                            "ot": ann["object_type"],
                            "isDef": ann["is_default"],
                            "isEdit": ann["is_editable"],
                            "dso": ann["device_scope_only"],
                            "ms": merge_strat,
                        },
                    )
                    summary["config_profiles_created"] += 1

                # Assignment edges from annotations
                for entry in ann["scope_device_function"]:
                    e_scope_type = str(entry.get("scope_type", ""))
                    e_scope_id = str(entry.get("scope_id", ""))
                    e_device_fn = str(entry.get("device_function", ""))

                    rel_name = SCOPE_TYPE_TO_REL.get(e_scope_type)
                    if not rel_name:
                        if e_scope_type and e_scope_type not in summary["unrecognized_scope_types"]:
                            summary["unrecognized_scope_types"].append(e_scope_type)
                        continue

                    label = SCOPE_TYPE_TO_LABEL[e_scope_type]
                    pk_field = SCOPE_TYPE_TO_PK[e_scope_type]

                    asgn_key = (rel_name, e_scope_id, profile_key, e_device_fn)
                    if asgn_key in assignments_created:
                        continue
                    assignments_created.add(asgn_key)

                    if e_scope_type == "org":
                        match_scope = f"(scope:{label} {{scopeId: 'org-root'}})"
                    else:
                        match_scope = f"(scope:{label} {{{pk_field}: $scopeId}})"

                    try:
                        graph.execute(
                            f"MATCH {match_scope}, "
                            f"(cp:ConfigProfile {{id: $pid}}) "
                            f"MERGE (scope)-[a:{rel_name}]->(cp) "
                            f"SET a.deviceFunctions = $df, "
                            f"a.isDefault = $isDef",
                            {
                                "scopeId": e_scope_id,
                                "pid": profile_key,
                                "df": e_device_fn,
                                "isDef": ann["is_default"],
                            },
                        )
                        summary["assignment_edges"] += 1
                    except Exception as exc:
                        summary["warnings"].append(
                            f"Assignment edge {rel_name} "
                            f"{e_scope_id}→{profile_key}: {exc}"
                        )

            summary["groups_processed"] += 1

        # Track category support
        if cat_ok:
            if category not in summary["categories_supported"]:
                summary["categories_supported"].append(category)
        else:
            if category not in summary["categories_failed"]:
                summary["categories_failed"].append(category)

    # ── Summary ──────────────────────────────────────────────────
    # Deduplicate sites/groups processed counts (one per category iteration)
    summary["sites_processed"] = len(sites)
    summary["groups_processed"] = len(groups)

    print(json.dumps(summary, indent=2))


if __name__ == "__main__":
    main()
