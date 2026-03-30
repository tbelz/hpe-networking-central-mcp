#!/usr/bin/env python3
"""Validate graph-computed EFFECTIVE_CONFIG vs Central API effective+detailed response.

Compares the pre-computed EFFECTIVE_CONFIG edges in the LadybugDB graph against
the authoritative effective config returned by the Central API with
``effective=true&detailed=true``.

Phases:
  1. Set up local graph + IPC, run base + config seeds
  2. Create safe test fixtures (empty group, library profiles)
  3. Re-run config seed to pick up fixtures
  4. Sample devices, compare graph vs API per category
  5. Clean up fixtures, print report

Requires Central credentials in .env.

Safety:
  - Temp group has zero members → no config pushed to any device.
  - Only reads (GET) are issued at device scope.
  - Cleanup runs in a ``finally`` block.
  - Only deletes resources the script itself created (tracked by ID).
"""

from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
import tempfile
import time
import uuid
from dataclasses import dataclass, field
from pathlib import Path

# Ensure the src directory is importable
sys.path.insert(0, str(Path(__file__).parent / "src"))

PASS = "\033[92mPASS\033[0m"
FAIL = "\033[91mFAIL\033[0m"
WARN = "\033[93mWARN\033[0m"
BOLD = "\033[1m"
RESET = "\033[0m"

TEST_PREFIX = "__mcp_test_"


# ── Env / credentials ────────────────────────────────────────────────

env_path = Path(__file__).parent / ".env"
if env_path.exists():
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip().replace("\r", "")
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        os.environ.setdefault(key.strip(), value.strip())

base_url = os.environ.get("CENTRAL_BASE_URL", "")
client_id = os.environ.get("CENTRAL_CLIENT_ID", "")
client_secret = os.environ.get("CENTRAL_CLIENT_SECRET", "")

if not all([base_url, client_id, client_secret]):
    print(f"[{FAIL}] Central credentials not configured.")
    print("       Set CENTRAL_BASE_URL, CENTRAL_CLIENT_ID, CENTRAL_CLIENT_SECRET in .env")
    sys.exit(2)


# ── Imports (after env is loaded) ────────────────────────────────────

from hpe_networking_central_mcp.central_client import (
    CentralClient,
    CentralAPIError,
    NotFoundError,
)
from hpe_networking_central_mcp.graph.ipc_server import GraphIPCServer
from hpe_networking_central_mcp.graph.manager import GraphManager


# ── Data classes ─────────────────────────────────────────────────────

@dataclass
class GraphProfile:
    """A profile as seen from the graph's EFFECTIVE_CONFIG edge."""
    profile_id: str
    name: str
    merge_strategy: str
    source_scope: str
    source_scope_id: str


@dataclass
class ApiProfile:
    """A profile extracted from the API effective+detailed response."""
    name: str
    source_scope: str
    source_scope_id: str


@dataclass
class ComparisonResult:
    serial: str
    category: str
    status: str  # "match", "mismatch", "graph_only", "api_only", "api_unsupported", "error"
    graph_profiles: list[GraphProfile] = field(default_factory=list)
    api_profiles: list[ApiProfile] = field(default_factory=list)
    details: str = ""


# ── Infrastructure setup ─────────────────────────────────────────────


def setup_graph_and_ipc() -> tuple[GraphManager, GraphIPCServer, Path, Path]:
    """Create a temp graph DB, start the IPC server, return (gm, ipc, db_path, socket_path)."""
    temp_dir = Path(tempfile.mkdtemp(prefix="mcp_config_val_"))
    db_path = temp_dir / "graph_db"
    socket_path = temp_dir / "graph.sock"

    gm = GraphManager(db_path)
    gm.initialize()

    ipc = GraphIPCServer(socket_path, gm)
    ipc.start()
    time.sleep(0.2)  # let the socket bind

    return gm, ipc, temp_dir, socket_path


def run_seed(seed_name: str, seeds_dir: Path, socket_path: Path) -> dict:
    """Execute a seed script as a subprocess (same model as the MCP server).

    Replicates the MCP server's script library setup: copies central_helpers.py
    alongside the seed so imports resolve correctly.
    """
    script_path = seeds_dir / f"{seed_name}.py"
    if not script_path.exists():
        return {"error": f"Seed {seed_name}.py not found in {seeds_dir}"}

    # Build a temp working directory with the seed + helpers co-located
    work_dir = Path(tempfile.mkdtemp(prefix="mcp_seed_run_"))
    src_pkg = Path(__file__).parent / "src" / "hpe_networking_central_mcp"
    helpers_src = src_pkg / "central_helpers.py"

    shutil.copy2(script_path, work_dir / f"{seed_name}.py")
    if helpers_src.exists():
        shutil.copy2(helpers_src, work_dir / "central_helpers.py")

    env = os.environ.copy()
    env["CENTRAL_BASE_URL"] = base_url
    env["CENTRAL_CLIENT_ID"] = client_id
    env["CENTRAL_CLIENT_SECRET"] = client_secret
    env["GRAPH_IPC_SOCKET"] = str(socket_path)

    try:
        result = subprocess.run(
            [sys.executable, str(work_dir / f"{seed_name}.py")],
            capture_output=True,
            text=True,
            timeout=300,
            cwd=str(work_dir),
            env=env,
        )
    except subprocess.TimeoutExpired as e:
        stderr = (e.stderr or "").strip()
        if stderr:
            for line in stderr.splitlines():
                print(f"    [seed] {line}", file=sys.stderr)
        return {
            "error": "Seed execution timed out after 300 seconds",
            "stderr": stderr[-2000:],
        }
    except OSError as e:
        return {
            "error": f"Failed to execute seed script: {e}",
            "stderr": "",
        }
    finally:
        # Always cleanup temp work directory, even on timeout/exception
        shutil.rmtree(work_dir, ignore_errors=True)

    if result.stderr:
        for line in result.stderr.strip().splitlines():
            print(f"    [seed] {line}", file=sys.stderr)

    if result.returncode != 0:
        return {"error": f"Seed exited {result.returncode}", "stderr": result.stderr[-2000:]}

    try:
        return json.loads(result.stdout)
    except (json.JSONDecodeError, ValueError):
        return {"error": "Seed did not emit valid JSON", "stdout": result.stdout[-2000:]}


# ── Category discovery ───────────────────────────────────────────────


def discover_categories(client: CentralClient) -> list[str]:
    """Discover config categories from the live API."""
    try:
        resp = client.get("network-config/v1alpha1")
        if isinstance(resp, dict):
            for key in ("categories", "items", "result"):
                if key in resp and isinstance(resp[key], list):
                    cats = [
                        c if isinstance(c, str) else c.get("name", c.get("category", ""))
                        for c in resp[key] if c
                    ]
                    return [cat for cat in cats if isinstance(cat, str) and cat.strip()]
        if isinstance(resp, list):
            cats = [
                c if isinstance(c, str) else c.get("name", c.get("category", ""))
                for c in resp if c
            ]
            return [cat for cat in cats if isinstance(cat, str) and cat.strip()]
    except CentralAPIError as exc:
        print(f"  [{WARN}] Category discovery failed: [{exc.status_code}] {exc.message}")

    # Fallback
    return [
        "wlan-ssids", "sw-port-profiles", "gw-port-profiles", "roles",
        "server-groups", "ntp", "dns", "dhcp-pools", "acls", "aaa", "snmp",
    ]


# ── Fixture management ───────────────────────────────────────────────


@dataclass
class Fixtures:
    """Tracks created fixtures for cleanup."""
    group_name: str = ""
    group_id: str = ""
    profile_ids: list[tuple[str, str]] = field(default_factory=list)  # (category, profile_name)


def create_fixtures(client: CentralClient) -> Fixtures:
    """Create a temporary empty device group. Returns fixture tracker.

    We intentionally do NOT create test config profiles via API —
    the config API's POST semantics vary by category and risk side effects.
    The empty group alone exercises the scope-resolution code path.
    """
    fixtures = Fixtures()
    group_name = f"{TEST_PREFIX}{uuid.uuid4().hex[:8]}"

    print(f"  Creating temp group '{group_name}'...")
    try:
        resp = client.post(
            "configuration/v2/groups",
            json_body={"group": group_name, "group_properties": {"AllowedDevTypes": ["Switches"]}},
        )
        fixtures.group_name = group_name
        # The API may return the group name or an object
        if isinstance(resp, dict):
            fixtures.group_id = resp.get("id", resp.get("scopeId", group_name))
        else:
            fixtures.group_id = group_name
        print(f"    -> Created group '{group_name}' (id: {fixtures.group_id})")
    except CentralAPIError as exc:
        print(f"  [{WARN}] Failed to create temp group: [{exc.status_code}] {exc.message}")

    return fixtures


def cleanup_fixtures(client: CentralClient, fixtures: Fixtures) -> None:
    """Delete all fixtures created by this script."""
    if fixtures.group_name:
        print(f"  Cleaning up temp group '{fixtures.group_name}'...")
        try:
            client.delete(
                f"configuration/v2/groups/{fixtures.group_name}",
            )
            print(f"    -> Deleted group '{fixtures.group_name}'")
        except CentralAPIError as exc:
            print(f"  [{WARN}] Failed to delete temp group: [{exc.status_code}] {exc.message}")


# ── Device sampling ──────────────────────────────────────────────────


def sample_devices(gm: GraphManager, target: int = 3) -> list[dict]:
    """Pick devices from different hierarchy positions for coverage.

    Ensures diversity by taking at most one device from each hierarchy
    tier first, then filling from any tier with remaining candidates.

    Returns list of dicts: {serial, name, site, group, collection}.
    """
    # Collect candidates per hierarchy tier
    tiers: list[list[dict]] = []

    # Tier 0: Devices in a group
    rows = gm.query(
        "MATCH (dg:DeviceGroup)-[:HAS_MEMBER]->(d:Device)"
        "<-[:HAS_DEVICE]-(s:Site) "
        "RETURN d.serial AS serial, d.name AS name, s.name AS site, "
        "dg.name AS grp LIMIT 5",
        read_only=True,
    )
    tiers.append([
        {"serial": r["serial"], "name": r["name"],
         "site": r.get("site", ""), "group": r.get("grp", ""), "collection": ""}
        for r in rows
    ])

    # Tier 1: Devices in a site-collection but NOT in a group
    rows = gm.query(
        "MATCH (sc:SiteCollection)-[:CONTAINS_SITE]->(s:Site)-[:HAS_DEVICE]->(d:Device) "
        "WHERE NOT EXISTS { MATCH (dg:DeviceGroup)-[:HAS_MEMBER]->(d) } "
        "RETURN d.serial AS serial, d.name AS name, s.name AS site, "
        "sc.name AS coll LIMIT 3",
        read_only=True,
    )
    tiers.append([
        {"serial": r["serial"], "name": r["name"],
         "site": r.get("site", ""), "group": "", "collection": r.get("coll", "")}
        for r in rows
    ])

    # Tier 2: Devices in a standalone site (no collection, no group)
    rows = gm.query(
        "MATCH (o:Org)-[:HAS_SITE]->(s:Site)-[:HAS_DEVICE]->(d:Device) "
        "WHERE NOT EXISTS { MATCH (:DeviceGroup)-[:HAS_MEMBER]->(d) } "
        "AND NOT EXISTS { MATCH (:SiteCollection)-[:CONTAINS_SITE]->(s) } "
        "RETURN d.serial AS serial, d.name AS name, s.name AS site LIMIT 3",
        read_only=True,
    )
    tiers.append([
        {"serial": r["serial"], "name": r["name"],
         "site": r.get("site", ""), "group": "", "collection": ""}
        for r in rows
    ])

    # Round-robin: take 1 from each tier first, then fill from remaining
    seen: set[str] = set()
    result: list[dict] = []

    # First pass: one from each populated tier (diversity)
    for tier in tiers:
        for c in tier:
            if c["serial"] not in seen:
                seen.add(c["serial"])
                result.append(c)
                break
        if len(result) >= target:
            break

    # Second pass: fill from any tier
    if len(result) < target:
        for tier in tiers:
            for c in tier:
                if c["serial"] not in seen:
                    seen.add(c["serial"])
                    result.append(c)
                    if len(result) >= target:
                        break
            if len(result) >= target:
                break

    # Fallback: just grab any devices if we don't have enough
    if len(result) < target:
        rows = gm.query(
            f"MATCH (d:Device) WHERE d.serial IS NOT NULL "
            f"RETURN d.serial AS serial, d.name AS name LIMIT {target * 2}",
            read_only=True,
        )
        for r in rows:
            if r["serial"] not in seen:
                seen.add(r["serial"])
                result.append({
                    "serial": r["serial"], "name": r["name"],
                    "site": "", "group": "", "collection": "",
                })
                if len(result) >= target:
                    break

    if len(result) < target:
        print(f"  [{WARN}] Only found {len(result)} device(s), wanted {target}")

    return result


# ── Graph query ──────────────────────────────────────────────────────


def query_graph_effective_config(
    gm: GraphManager, serial: str, category: str
) -> list[GraphProfile]:
    """Query EFFECTIVE_CONFIG edges for a device+category from the graph."""
    rows = gm.query(
        "MATCH (d:Device {serial: $serial})-[r:EFFECTIVE_CONFIG]->(cp:ConfigProfile) "
        "WHERE cp.category = $cat "
        "RETURN cp.id AS pid, cp.name AS name, cp.mergeStrategy AS ms, "
        "r.sourceScope AS scope, r.sourceScopeId AS scopeId",
        params={"serial": serial, "cat": category},
        read_only=True,
    )
    return [
        GraphProfile(
            profile_id=r.get("pid", ""),
            name=r.get("name", ""),
            merge_strategy=r.get("ms", ""),
            source_scope=r.get("scope", ""),
            source_scope_id=r.get("scopeId", ""),
        )
        for r in rows
    ]


# ── API query ────────────────────────────────────────────────────────


def query_api_effective_config(
    client: CentralClient, serial: str, category: str
) -> list[ApiProfile] | None | str:
    """Call the API with effective=true&detailed=true for a device+category.

    Returns list of ApiProfile, or None if the API returned an error.
    """
    try:
        resp = client.get(
            f"network-config/v1alpha1/{category}",
            params={
                "scopeId": serial,
                "scopeType": "device",
                "effective": "true",
                "detailed": "true",
            },
        )
    except NotFoundError:
        return []  # No config for this category/device
    except CentralAPIError as exc:
        if exc.status_code == 400:
            # 400 = category not supported by API (e.g. dhcp-pools, acls, aaa)
            print(f"    [{WARN}] API unsupported for {serial}/{category}: [{exc.status_code}] {exc.message}")
            return "unsupported"  # sentinel
        print(f"    [{WARN}] API error for {serial}/{category}: [{exc.status_code}] {exc.message}")
        return None

    # Parse response — extract profile list with sourceScope annotations
    profiles: list[ApiProfile] = []
    items = _extract_items(resp, category)

    if items is None:
        # Single object response (atomic category)
        if isinstance(resp, dict) and resp.get("name"):
            profiles.append(ApiProfile(
                name=resp.get("name", ""),
                source_scope=resp.get("sourceScope", resp.get("source_scope", "")),
                source_scope_id=resp.get("sourceScopeId", resp.get("source_scope_id", "")),
            ))
        return profiles

    for item in items:
        if not isinstance(item, dict):
            continue
        profiles.append(ApiProfile(
            name=item.get("name", ""),
            source_scope=item.get("sourceScope", item.get("source_scope", "")),
            source_scope_id=item.get("sourceScopeId", item.get("source_scope_id", "")),
        ))

    return profiles


def _extract_items(resp, category: str) -> list[dict] | None:
    """Extract list of config items from API response (mirrors seed logic)."""
    if isinstance(resp, list):
        return resp
    if isinstance(resp, dict):
        for key in ("items", "result", category):
            if key in resp and isinstance(resp[key], list):
                return resp[key]
        if "name" in resp:
            return [resp]
    return None


# ── Comparison ───────────────────────────────────────────────────────


def compare(
    serial: str,
    category: str,
    graph_profiles: list[GraphProfile],
    api_profiles: list[ApiProfile] | None | str,
) -> ComparisonResult:
    """Compare graph EFFECTIVE_CONFIG with API effective+detailed response."""

    if api_profiles == "unsupported":
        return ComparisonResult(
            serial=serial, category=category, status="api_unsupported",
            graph_profiles=graph_profiles, details="Category not supported by API (400)",
        )

    if api_profiles is None:
        return ComparisonResult(
            serial=serial, category=category, status="error",
            graph_profiles=graph_profiles, details="API call failed",
        )

    # Both empty — match (no config for this category)
    if not graph_profiles and not api_profiles:
        return ComparisonResult(
            serial=serial, category=category, status="match",
            details="Both empty — no config for this category",
        )

    # API empty, graph has profiles
    if graph_profiles and not api_profiles:
        return ComparisonResult(
            serial=serial, category=category, status="graph_only",
            graph_profiles=graph_profiles,
            details=f"Graph has {len(graph_profiles)} profiles, API returned empty",
        )

    # Graph empty, API has profiles
    if not graph_profiles and api_profiles:
        return ComparisonResult(
            serial=serial, category=category, status="api_only",
            api_profiles=api_profiles,
            details=f"API has {len(api_profiles)} profiles, graph has none",
        )

    # Both have profiles — compare by name
    graph_names = {gp.name for gp in graph_profiles}
    api_names = {ap.name for ap in api_profiles}

    only_in_graph = graph_names - api_names
    only_in_api = api_names - graph_names
    common = graph_names & api_names

    # Compare sourceScope and sourceScopeId for common profiles
    scope_mismatches: list[str] = []
    for name in common:
        gp = next(p for p in graph_profiles if p.name == name)
        ap = next(p for p in api_profiles if p.name == name)
        # Normalize scope labels for comparison
        g_scope = _normalize_scope(gp.source_scope)
        a_scope = _normalize_scope(ap.source_scope)
        scope_differs = g_scope and a_scope and g_scope != a_scope
        scope_id_differs = (
            gp.source_scope_id and ap.source_scope_id
            and gp.source_scope_id != ap.source_scope_id
        )
        if scope_differs or scope_id_differs:
            scope_mismatches.append(
                f"  '{name}': graph={gp.source_scope}({gp.source_scope_id}) "
                f"vs api={ap.source_scope}({ap.source_scope_id})"
            )

    if not only_in_graph and not only_in_api and not scope_mismatches:
        return ComparisonResult(
            serial=serial, category=category, status="match",
            graph_profiles=graph_profiles, api_profiles=api_profiles,
            details=f"All {len(common)} profile(s) match",
        )

    # Build mismatch details
    parts: list[str] = []
    if only_in_graph:
        parts.append(f"Only in graph: {only_in_graph}")
    if only_in_api:
        parts.append(f"Only in API: {only_in_api}")
    if scope_mismatches:
        parts.append("Scope mismatches:\n" + "\n".join(scope_mismatches))

    return ComparisonResult(
        serial=serial, category=category, status="mismatch",
        graph_profiles=graph_profiles, api_profiles=api_profiles,
        details="\n".join(parts),
    )


def _normalize_scope(scope: str) -> str:
    """Normalize scope labels for comparison (e.g., 'device-group' == 'group')."""
    s = scope.lower().strip()
    mapping = {
        "device-group": "group",
        "devicegroup": "group",
        "device_group": "group",
        "site-collection": "collection",
        "sitecollection": "collection",
        "site_collection": "collection",
        "organization": "org",
    }
    return mapping.get(s, s)


# ── Report ───────────────────────────────────────────────────────────


def print_report(results: list[ComparisonResult], devices: list[dict], categories: list[str]):
    """Print a structured validation report."""
    print(f"\n{'=' * 70}")
    print(f"{BOLD}Effective Config Validation Report{RESET}")
    print(f"{'=' * 70}")

    # Summary counts
    by_status: dict[str, int] = {}
    for r in results:
        by_status[r.status] = by_status.get(r.status, 0) + 1

    total = len(results)
    matches = by_status.get("match", 0)
    unsupported = by_status.get("api_unsupported", 0)
    valid_total = total - unsupported
    print(f"\n  Total comparisons: {total}")
    print(f"  Valid comparisons: {valid_total}")
    print(f"  Matches:           {matches}/{valid_total} ({matches * 100 // valid_total if valid_total else 0}%)")
    if unsupported:
        print(f"  API unsupported:   {unsupported} (categories returning 400)")
    for status, count in sorted(by_status.items()):
        if status not in ("match", "api_unsupported"):
            print(f"  {status:18s}: {count}")

    # Per-category summary
    print(f"\n{BOLD}Per-Category Summary{RESET}")
    print(f"  {'Category':<25s} {'Match':>6s} {'Mismatch':>10s} {'GraphOnly':>10s} {'ApiOnly':>10s} {'Error':>6s} {'Unsup':>6s}")
    print(f"  {'-' * 81}")
    for cat in categories:
        cat_results = [r for r in results if r.category == cat]
        if not cat_results:
            continue
        counts = {s: 0 for s in ("match", "mismatch", "graph_only", "api_only", "error", "api_unsupported")}
        for r in cat_results:
            counts[r.status] = counts.get(r.status, 0) + 1
        print(
            f"  {cat:<25s} {counts['match']:>6d} {counts.get('mismatch', 0):>10d} "
            f"{counts.get('graph_only', 0):>10d} {counts.get('api_only', 0):>10d} "
            f"{counts.get('error', 0):>6d} {counts.get('api_unsupported', 0):>6d}"
        )

    # Discrepancies detail (exclude api_unsupported from real discrepancies)
    discrepancies = [r for r in results if r.status not in ("match", "api_unsupported")]
    if discrepancies:
        print(f"\n{BOLD}Discrepancies{RESET}")
        for r in discrepancies:
            if r.status == "match":
                continue
            marker = FAIL if r.status == "mismatch" else WARN
            print(f"\n  [{marker}] {r.serial} / {r.category} — {r.status}")
            if r.details:
                for line in r.details.splitlines():
                    print(f"       {line}")
            if r.graph_profiles:
                print(f"       Graph profiles:")
                for gp in r.graph_profiles:
                    print(f"         - {gp.name} (scope={gp.source_scope}, scopeId={gp.source_scope_id})")
            if r.api_profiles:
                print(f"       API profiles:")
                for ap in r.api_profiles:
                    print(f"         - {ap.name} (scope={ap.source_scope}, scopeId={ap.source_scope_id})")
    else:
        print(f"\n  [{PASS}] All comparisons match — graph effective config is consistent with API")

    # JSON output for machine consumption
    json_report = {
        "total": total,
        "valid_total": valid_total,
        "matches": matches,
        "match_rate": f"{matches * 100 / valid_total:.1f}%" if valid_total else "N/A",
        "api_unsupported": unsupported,
        "by_status": by_status,
        "discrepancies": [
            {
                "serial": r.serial,
                "category": r.category,
                "status": r.status,
                "details": r.details,
                "graph_profiles": [
                    {"name": gp.name, "source_scope": gp.source_scope, "source_scope_id": gp.source_scope_id}
                    for gp in r.graph_profiles
                ],
                "api_profiles": [
                    {"name": ap.name, "source_scope": ap.source_scope, "source_scope_id": ap.source_scope_id}
                    for ap in r.api_profiles
                ],
            }
            for r in results if r.status != "match"
        ],
    }
    report_path = Path(__file__).parent / "config_validation_report.json"
    report_path.write_text(json.dumps(json_report, indent=2), encoding="utf-8")
    print(f"\n  Full report written to {report_path.name}")

    return len(discrepancies) == 0


# ── Main ─────────────────────────────────────────────────────────────


def main() -> int:
    print(f"\n{'=' * 70}")
    print(f"Config Validation: Graph EFFECTIVE_CONFIG vs API effective+detailed")
    print(f"{'=' * 70}")

    client = CentralClient(base_url, client_id, client_secret)

    # Validate credentials
    print("\nPhase 0: Credential validation")
    try:
        client.validate()
        print(f"  [{PASS}] OAuth2 token acquired")
    except Exception as exc:
        print(f"  [{FAIL}] Credential validation failed: {exc}")
        return 2

    # Phase 1: Set up graph + IPC, run seeds
    print(f"\nPhase 1: Graph setup + seed population")
    gm, ipc, temp_dir, socket_path = setup_graph_and_ipc()

    seeds_dir = Path(__file__).parent / "src" / "hpe_networking_central_mcp" / "seeds"
    fixtures = Fixtures()  # initialise early for the finally block

    try:
        print("  Running populate_base_graph seed...")
        t0 = time.time()
        summary = run_seed("populate_base_graph", seeds_dir, socket_path)
        elapsed = time.time() - t0
        if "error" in summary:
            print(f"  [{FAIL}] Base graph seed failed: {summary['error']}")
            return 2
        print(f"  [{PASS}] Base graph populated in {elapsed:.1f}s")
        print(f"    -> {json.dumps({k: v for k, v in summary.items() if k != 'errors'}, default=str)}")

        print("  Running populate_config_policy seed...")
        t0 = time.time()
        summary = run_seed("populate_config_policy", seeds_dir, socket_path)
        elapsed = time.time() - t0
        if "error" in summary:
            print(f"  [{FAIL}] Config policy seed failed: {summary['error']}")
            return 2
        print(f"  [{PASS}] Config policy populated in {elapsed:.1f}s")
        print(f"    -> {json.dumps({k: v for k, v in summary.items() if k != 'errors'}, default=str)}")

        # Quick stats
        device_count = gm.query("MATCH (d:Device) RETURN count(d) AS cnt", read_only=True)
        config_count = gm.query(
            "MATCH (d:Device)-[r:EFFECTIVE_CONFIG]->(cp:ConfigProfile) RETURN count(r) AS cnt",
            read_only=True,
        )
        n_devices = device_count[0]["cnt"] if device_count else 0
        n_edges = config_count[0]["cnt"] if config_count else 0
        print(f"    -> {n_devices} devices, {n_edges} EFFECTIVE_CONFIG edges in graph")

        if n_devices == 0:
            print(f"  [{FAIL}] No devices in graph — cannot validate")
            return 2

        # Phase 2: Create test fixtures
        print(f"\nPhase 2: Create test fixtures")
        fixtures = create_fixtures(client)
        # Phase 3: Re-run config seed to pick up fixtures
        if fixtures.group_name:
            print(f"\nPhase 3: Re-run config seed with fixtures")
            t0 = time.time()
            summary = run_seed("populate_config_policy", seeds_dir, socket_path)
            elapsed = time.time() - t0
            if "error" in summary:
                print(f"  [{WARN}] Config re-seed failed: {summary.get('error', '')}")
            else:
                print(f"  [{PASS}] Config policy re-seeded in {elapsed:.1f}s")
        else:
            print(f"\nPhase 3: Skipped (no fixtures created)")

        # Phase 4: Discover categories, sample devices, compare
        print(f"\nPhase 4: Validation — graph vs API comparison")

        categories = discover_categories(client)
        print(f"  Discovered {len(categories)} categories: {', '.join(categories)}")

        devices = sample_devices(gm)
        if not devices:
            print(f"  [{FAIL}] No devices to validate")
            return 2
        print(f"  Sampled {len(devices)} devices:")
        for d in devices:
            parts = [d["serial"], d.get("name", "")]
            if d.get("group"):
                parts.append(f"group={d['group']}")
            if d.get("site"):
                parts.append(f"site={d['site']}")
            if d.get("collection"):
                parts.append(f"coll={d['collection']}")
            print(f"    - {' | '.join(p for p in parts if p)}")

        # Run comparisons
        results: list[ComparisonResult] = []
        total = len(devices) * len(categories)
        done = 0

        for device in devices:
            serial = device["serial"]
            for category in categories:
                done += 1
                sys.stdout.write(f"\r  Comparing {done}/{total}...")
                sys.stdout.flush()

                graph_profiles = query_graph_effective_config(gm, serial, category)
                api_profiles = query_api_effective_config(client, serial, category)
                result = compare(serial, category, graph_profiles, api_profiles)
                results.append(result)

        print(f"\r  Compared {total} device×category pairs")

        # Phase 5: Report
        all_match = print_report(results, devices, categories)

        return 0 if all_match else 1

    finally:
        # Phase 6: Cleanup
        print(f"\nCleanup:")
        cleanup_fixtures(client, fixtures)

        # Shutdown IPC and remove temp dir (always, even if Phase 1 fails)
        ipc.stop()
        try:
            shutil.rmtree(temp_dir)
            print(f"  Temp directory cleaned up")
        except OSError:
            pass

        client.close()


if __name__ == "__main__":
    sys.exit(main())
