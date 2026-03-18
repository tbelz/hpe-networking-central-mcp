"""Kùzu graph schema DDL for the Aruba Central configuration hierarchy.

Defines node and relationship tables that model:
  Org → SiteCollection → Site → Device
  DeviceGroup → Device (cross-cutting membership)
  Scope → ConfigProfile (library-level config metadata)
"""

from __future__ import annotations

import re

SCHEMA_VERSION = 1

# ── Node table DDL ───────────────────────────────────────────────────

NODE_TABLES: list[str] = [
    """
    CREATE NODE TABLE IF NOT EXISTS Org (
        scopeId STRING,
        name    STRING,
        PRIMARY KEY (scopeId)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS SiteCollection (
        scopeId     STRING,
        name        STRING,
        siteCount   INT64,
        deviceCount INT64,
        PRIMARY KEY (scopeId)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS Site (
        scopeId        STRING,
        name           STRING,
        address        STRING,
        city           STRING,
        country        STRING,
        state          STRING,
        zipcode        STRING,
        lat            DOUBLE,
        lon            DOUBLE,
        deviceCount    INT64,
        collectionId   STRING,
        collectionName STRING,
        timezoneId     STRING,
        PRIMARY KEY (scopeId)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS DeviceGroup (
        scopeId     STRING,
        name        STRING,
        deviceCount INT64,
        PRIMARY KEY (scopeId)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS Device (
        serial          STRING,
        name            STRING,
        mac             STRING,
        model           STRING,
        deviceType      STRING,
        status          STRING,
        ipv4            STRING,
        firmware        STRING,
        persona         STRING,
        deviceFunction  STRING,
        siteId          STRING,
        siteName        STRING,
        partNumber      STRING,
        deployment      STRING,
        configStatus    STRING,
        deviceGroupId   STRING,
        deviceGroupName STRING,
        PRIMARY KEY (serial)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS ConfigProfile (
        id             STRING,
        name           STRING,
        category       STRING,
        scopeId        STRING,
        deviceFunction STRING,
        objectType     STRING,
        PRIMARY KEY (id)
    )
    """,
]

# ── Relationship table DDL ───────────────────────────────────────────

REL_TABLES: list[str] = [
    "CREATE REL TABLE IF NOT EXISTS HAS_COLLECTION  (FROM Org TO SiteCollection)",
    "CREATE REL TABLE IF NOT EXISTS HAS_SITE        (FROM Org TO Site)",
    "CREATE REL TABLE IF NOT EXISTS CONTAINS_SITE   (FROM SiteCollection TO Site)",
    "CREATE REL TABLE IF NOT EXISTS HAS_DEVICE      (FROM Site TO Device)",
    "CREATE REL TABLE IF NOT EXISTS HAS_MEMBER      (FROM DeviceGroup TO Device)",
    "CREATE REL TABLE IF NOT EXISTS HAS_CONFIG      (FROM Org TO ConfigProfile)",
]

# Phase 2 — topology (not created yet, placeholder for documentation)
_PHASE2_REL_TABLES: list[str] = [
    """
    CREATE REL TABLE IF NOT EXISTS CONNECTED_TO (
        FROM Device TO Device,
        portFrom  STRING,
        portTo    STRING,
        speed     INT64,
        edgeType  STRING,
        health    STRING,
        lag       STRING
    )
    """,
]

# ── Human-readable schema description (exposed via graph://schema) ───

SCHEMA_DESCRIPTION = """\
# Graph Schema — Aruba Central Configuration Hierarchy

Schema version: {version}

## Node Tables

| Table           | Primary Key | Properties |
|-----------------|-------------|------------|
| Org             | scopeId     | name |
| SiteCollection  | scopeId     | name, siteCount, deviceCount |
| Site            | scopeId     | name, address, city, country, state, zipcode, lat, lon, deviceCount, collectionId, collectionName, timezoneId |
| DeviceGroup     | scopeId     | name, deviceCount |
| Device          | serial      | name, mac, model, deviceType, status, ipv4, firmware, persona, deviceFunction, siteId, siteName, partNumber, deployment, configStatus, deviceGroupId, deviceGroupName |
| ConfigProfile   | id          | name, category, scopeId, deviceFunction, objectType |

## Relationship Tables

| Relationship    | From → To                | Properties |
|-----------------|--------------------------|------------|
| HAS_COLLECTION  | Org → SiteCollection     | — |
| HAS_SITE        | Org → Site               | — (sites NOT in any collection) |
| CONTAINS_SITE   | SiteCollection → Site    | — |
| HAS_DEVICE      | Site → Device            | — |
| HAS_MEMBER      | DeviceGroup → Device     | — |
| HAS_CONFIG      | Org → ConfigProfile      | — (library-level configs) |

## Hierarchy

```
Org (root)
├── SiteCollection
│   └── Site
│       └── Device
├── Site (standalone, not in a collection)
│   └── Device
├── DeviceGroup (cross-cutting, devices from any site)
│   └── Device
└── ConfigProfile (library-level, inherited by all scopes)
```

## Phase 2 (not yet populated)

| Relationship  | From → To       | Properties |
|---------------|-----------------|------------|
| CONNECTED_TO  | Device → Device | portFrom, portTo, speed, edgeType, health, lag |

## Example Cypher Queries

### Hierarchy Navigation
```cypher
// Full hierarchy tree
MATCH (o:Org)-[:HAS_COLLECTION]->(sc:SiteCollection)-[:CONTAINS_SITE]->(s:Site)
RETURN o.name AS org, sc.name AS collection, s.name AS site

// Sites not in any collection (standalone)
MATCH (o:Org)-[:HAS_SITE]->(s:Site)
RETURN s.name AS site, s.city AS city

// All sites (both collection and standalone)
MATCH (s:Site) RETURN s.name, s.city, s.collectionName ORDER BY s.name
```

### Device Lookup
```cypher
// All devices with their site
MATCH (s:Site)-[:HAS_DEVICE]->(d:Device)
RETURN s.name AS site, d.name AS device, d.deviceType AS type, d.status AS status
ORDER BY s.name, d.name

// Devices at a specific site
MATCH (s:Site {{name: 'Curry-Zentrale'}})-[:HAS_DEVICE]->(d:Device)
RETURN d.serial, d.name, d.model, d.status

// Offline devices
MATCH (s:Site)-[:HAS_DEVICE]->(d:Device {{status: 'OFFLINE'}})
RETURN s.name AS site, d.serial, d.name, d.model
```

### Blast Radius Analysis
```cypher
// What devices are under a site-collection? (blast radius for collection-level config change)
MATCH (sc:SiteCollection {{name: 'Wurstfabrik'}})-[:CONTAINS_SITE]->(s:Site)-[:HAS_DEVICE]->(d:Device)
RETURN sc.name AS collection, s.name AS site, d.serial, d.name, d.deviceType

// Count devices per collection
MATCH (sc:SiteCollection)-[:CONTAINS_SITE]->(s:Site)-[:HAS_DEVICE]->(d:Device)
RETURN sc.name AS collection, count(d) AS deviceCount
```

### Device Group Queries
```cypher
// Devices in a device-group
MATCH (dg:DeviceGroup {{name: 'Verkaufstelle'}})-[:HAS_MEMBER]->(d:Device)
RETURN d.serial, d.name, d.siteName, d.deviceType

// Cross-reference: which device-groups have devices at a given site?
MATCH (dg:DeviceGroup)-[:HAS_MEMBER]->(d:Device {{siteName: 'Curry-Zentrale'}})
RETURN DISTINCT dg.name AS deviceGroup, count(d) AS deviceCount
```

### Config Provenance
```cypher
// All library-level config profiles
MATCH (o:Org)-[:HAS_CONFIG]->(cp:ConfigProfile)
RETURN cp.category, cp.name, cp.deviceFunction, cp.objectType
ORDER BY cp.category, cp.name

// Config profiles for a specific category
MATCH (o:Org)-[:HAS_CONFIG]->(cp:ConfigProfile {{category: 'wlan-ssids'}})
RETURN cp.name, cp.deviceFunction
```

### Tips
- Always use aliases for aggregations: `count(d) AS cnt` (un-aliased count may show internal column names).
- The `firmware` property maps to the API's `firmwareVersion` field.
- Devices include `deviceGroupId` and `deviceGroupName` for direct group lookups without traversing HAS_MEMBER.
- Avoid reserved words as aliases: `group`, `order`, `limit`, `match`, `return`, `set`, `delete`. Use e.g. `grp` instead of `group`.

### Cross-Site Comparison
```cypher
// Device count per site
MATCH (s:Site)-[:HAS_DEVICE]->(d:Device)
RETURN s.name AS site, count(d) AS devices, collect(DISTINCT d.deviceType) AS types
ORDER BY devices DESC

// Firmware versions in use
MATCH (d:Device)
RETURN d.firmware AS version, d.deviceType AS type, count(d) AS count
ORDER BY type, count DESC
```
""".format(version=SCHEMA_VERSION)


# ── Helpers for dynamic property lookup (used by error hints) ────────

_PROP_RE = re.compile(r"^\s+(\w+)\s+", re.MULTILINE)
_TABLE_NAME_RE = re.compile(r"CREATE NODE TABLE IF NOT EXISTS (\w+)")


def get_node_properties() -> dict[str, list[str]]:
    """Extract {TableName: [property, ...]} from the DDL, always in sync."""
    result: dict[str, list[str]] = {}
    for ddl in NODE_TABLES:
        m = _TABLE_NAME_RE.search(ddl)
        if not m:
            continue
        table = m.group(1)
        props = [p.group(1) for p in _PROP_RE.finditer(ddl)
                 if p.group(1).upper() not in ("PRIMARY", "CREATE", "FROM", "TO")]
        result[table] = props
    return result


def get_node_tables() -> list[str]:
    """Return all node table names."""
    return [m.group(1) for ddl in NODE_TABLES
            if (m := _TABLE_NAME_RE.search(ddl))]


def get_rel_tables() -> list[str]:
    """Return all relationship table names."""
    _rel_re = re.compile(r"CREATE REL TABLE IF NOT EXISTS (\w+)")
    return [m.group(1) for ddl in REL_TABLES if (m := _rel_re.search(ddl))]


def compact_schema_hint() -> str:
    """One-line-per-table property summary for error messages."""
    lines = []
    for table, props in get_node_properties().items():
        lines.append(f"  {table}: {', '.join(props)}")
    rels = get_rel_tables()
    lines.append(f"  Relationships: {', '.join(rels)}")
    return "\n".join(lines)
