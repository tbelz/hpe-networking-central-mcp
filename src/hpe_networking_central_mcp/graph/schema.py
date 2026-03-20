"""LadybugDB graph schema DDL for the Aruba Central configuration hierarchy.

Defines node and relationship tables that model:
  Org → SiteCollection → Site → Device
  DeviceGroup → Device (cross-cutting membership)
  Scope → ConfigProfile (library-level config metadata)
  Device → Device / UnmanagedDevice (physical L2 topology links)
"""

from __future__ import annotations

import re

SCHEMA_VERSION = 2

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
        id                     STRING,
        name                   STRING,
        category               STRING,
        scopeId                STRING,
        deviceFunction         STRING,
        objectType             STRING,
        isDefault              BOOLEAN,
        isEditable             BOOLEAN,
        deviceScopeOnly        BOOLEAN,
        assignedScopeIds       STRING,
        assignedDeviceFunctions STRING,
        PRIMARY KEY (id)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS UnmanagedDevice (
        mac            STRING,
        name           STRING,
        model          STRING,
        deviceType     STRING,
        health         STRING,
        status         STRING,
        ipv4           STRING,
        siteId         STRING,
        PRIMARY KEY (mac)
    )
    """,
]

# ── Knowledge layer node tables (populated by GH runner or runtime fallback) ─

KNOWLEDGE_NODE_TABLES: list[str] = [
    """
    CREATE NODE TABLE IF NOT EXISTS ApiEndpoint (
        endpoint_id    STRING,
        method         STRING,
        path           STRING,
        summary        STRING,
        description    STRING,
        operationId    STRING,
        category       STRING,
        deprecated     BOOLEAN,
        tags           STRING[],
        parameterNames STRING[],
        hasRequestBody BOOLEAN,
        PRIMARY KEY (endpoint_id)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS ApiCategory (
        name           STRING,
        endpointCount  INT64,
        sourceProvider STRING,
        PRIMARY KEY (name)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS DocSection (
        section_id STRING,
        title      STRING,
        content    STRING,
        source     STRING,
        url        STRING,
        PRIMARY KEY (section_id)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS Script (
        filename    STRING,
        description STRING,
        tags        STRING[],
        content     STRING,
        parameters  STRING,
        created_at  STRING,
        last_run    STRING,
        last_exit_code INT64,
        PRIMARY KEY (filename)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS EntityType (
        name        STRING,
        graphNode   STRING,
        description STRING,
        fields      STRING,
        PRIMARY KEY (name)
    )
    """,
]

KNOWLEDGE_REL_TABLES: list[str] = [
    "CREATE REL TABLE IF NOT EXISTS BELONGS_TO_CATEGORY (FROM ApiEndpoint TO ApiCategory)",
    # Entity mapping relationships — populated by the entity_mapping pipeline
    """
    CREATE REL TABLE IF NOT EXISTS OPERATES_ON (
        FROM ApiEndpoint TO EntityType,
        paramName   STRING,
        fieldName   STRING,
        confidence  STRING,
        mapper      STRING,
        reason      STRING
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
    "CREATE REL TABLE IF NOT EXISTS HAS_UNMANAGED   (FROM Site TO UnmanagedDevice)",
]

# Policy layer relationship tables — scope-to-config assignment edges.
# One table per scope type because LadybugDB requires explicit FROM/TO types.
# Populated by populate_config_policy seed.
POLICY_REL_TABLES: list[str] = [
    """
    CREATE REL TABLE IF NOT EXISTS ORG_ASSIGNS_CONFIG (
        FROM Org TO ConfigProfile,
        deviceFunctions STRING,
        isDefault       BOOLEAN
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS COLLECTION_ASSIGNS_CONFIG (
        FROM SiteCollection TO ConfigProfile,
        deviceFunctions STRING,
        isDefault       BOOLEAN
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS SITE_ASSIGNS_CONFIG (
        FROM Site TO ConfigProfile,
        deviceFunctions STRING,
        isDefault       BOOLEAN
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS GROUP_ASSIGNS_CONFIG (
        FROM DeviceGroup TO ConfigProfile,
        deviceFunctions STRING,
        isDefault       BOOLEAN
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS DEVICE_ASSIGNS_CONFIG (
        FROM Device TO ConfigProfile,
        deviceFunctions STRING,
        isDefault       BOOLEAN
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS EFFECTIVE_CONFIG (
        FROM Device TO ConfigProfile,
        sourceScope     STRING,
        sourceScopeId   STRING,
        sourceScopeName STRING
    )
    """,
]

# Topology relationship tables — created alongside the main schema but
# populated lazily on first topology query or explicit refresh.
TOPOLOGY_REL_TABLES: list[str] = [
    """
    CREATE REL TABLE IF NOT EXISTS CONNECTED_TO (
        FROM Device TO Device,
        fromPorts STRING,
        toPorts   STRING,
        speed     DOUBLE,
        edgeType  STRING,
        health    STRING,
        lag       STRING,
        stpState  STRING,
        isSibling BOOLEAN
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS LINKED_TO (
        FROM Device TO UnmanagedDevice,
        fromPorts STRING,
        toPorts   STRING,
        speed     DOUBLE,
        edgeType  STRING,
        health    STRING,
        lag       STRING,
        stpState  STRING,
        isSibling BOOLEAN
    )
    """,
]

# ── Human-readable schema description (exposed via graph://schema) ───

SCHEMA_DESCRIPTION = """\
# Graph Schema — Aruba Central Configuration & Topology

Schema version: {version}

## Node Tables

| Table            | Primary Key | Properties |
|------------------|-------------|------------|
| Org              | scopeId     | name |
| SiteCollection   | scopeId     | name, siteCount, deviceCount |
| Site             | scopeId     | name, address, city, country, state, zipcode, lat, lon, deviceCount, collectionId, collectionName, timezoneId |
| DeviceGroup      | scopeId     | name, deviceCount |
| Device           | serial      | name, mac, model, deviceType, status, ipv4, firmware, persona, deviceFunction, siteId, siteName, partNumber, deployment, configStatus, deviceGroupId, deviceGroupName |
| ConfigProfile    | id          | name, category, scopeId, deviceFunction, objectType, isDefault, isEditable, deviceScopeOnly, assignedScopeIds, assignedDeviceFunctions |
| UnmanagedDevice  | mac         | name, model, deviceType, health, status, ipv4, siteId |

### Knowledge Layer (populated by GH runner)

| Table          | Primary Key   | Properties |
|----------------|---------------|------------|
| ApiEndpoint    | endpoint_id   | method, path, summary, description, operationId, category, deprecated, tags, parameterNames, hasRequestBody |
| ApiCategory    | name          | endpointCount, sourceProvider |
| EntityType     | name          | graphNode, description, fields |
| DocSection     | section_id    | title, content, source, url |
| Script         | filename      | description, tags, content, parameters, created_at, last_run, last_exit_code |

## Relationship Tables

### Configuration Hierarchy

| Relationship    | From → To                | Properties |
|-----------------|--------------------------|------------|
| HAS_COLLECTION  | Org → SiteCollection     | — |
| HAS_SITE        | Org → Site               | — (sites NOT in any collection) |
| CONTAINS_SITE   | SiteCollection → Site    | — |
| HAS_DEVICE      | Site → Device            | — |
| HAS_MEMBER      | DeviceGroup → Device     | — |
| HAS_CONFIG      | Org → ConfigProfile      | — (library-level configs, legacy) |
| HAS_UNMANAGED   | Site → UnmanagedDevice   | — |

### Configuration Policy Layer (populated by populate_config_policy seed)

| Relationship              | From → To                        | Properties |
|---------------------------|----------------------------------|------------|
| ORG_ASSIGNS_CONFIG        | Org → ConfigProfile              | deviceFunctions, isDefault |
| COLLECTION_ASSIGNS_CONFIG | SiteCollection → ConfigProfile   | deviceFunctions, isDefault |
| SITE_ASSIGNS_CONFIG       | Site → ConfigProfile             | deviceFunctions, isDefault |
| GROUP_ASSIGNS_CONFIG      | DeviceGroup → ConfigProfile      | deviceFunctions, isDefault |
| DEVICE_ASSIGNS_CONFIG     | Device → ConfigProfile           | deviceFunctions, isDefault |
| EFFECTIVE_CONFIG          | Device → ConfigProfile           | sourceScope, sourceScopeId, sourceScopeName |

### Physical L2 Topology (populated lazily via load_topology / refresh_graph)

| Relationship  | From → To                    | Properties |
|---------------|------------------------------|------------|
| CONNECTED_TO  | Device → Device              | fromPorts, toPorts, speed, edgeType, health, lag, stpState, isSibling |
| LINKED_TO     | Device → UnmanagedDevice     | fromPorts, toPorts, speed, edgeType, health, lag, stpState, isSibling |

### Entity Mapping (populated by GH runner — links APIs to domain entities)

| Relationship   | From → To                  | Properties |
|----------------|----------------------------|------------|
| BELONGS_TO_CATEGORY | ApiEndpoint → ApiCategory | — |
| OPERATES_ON    | ApiEndpoint → EntityType   | paramName, fieldName, confidence, mapper, reason |

## Hierarchy

```
Org (root)
├── SiteCollection ──COLLECTION_ASSIGNS_CONFIG──► ConfigProfile
│   └── Site ──SITE_ASSIGNS_CONFIG──► ConfigProfile
│       ├── Device ──CONNECTED_TO──► Device
│       │           ├──LINKED_TO──► UnmanagedDevice
│       │           ├──DEVICE_ASSIGNS_CONFIG──► ConfigProfile
│       │           └──EFFECTIVE_CONFIG──► ConfigProfile
│       └── UnmanagedDevice
├── Site (standalone, not in a collection)
│   └── Device / UnmanagedDevice
├── DeviceGroup ──GROUP_ASSIGNS_CONFIG──► ConfigProfile
│   └── Device
├── ConfigProfile (library-level, inherited by all scopes)
└── Org ──ORG_ASSIGNS_CONFIG──► ConfigProfile (global scope assignments)
```

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

### Config Policy Layer — Scope Assignments & Effective Config
```cypher
// What profiles are assigned at a specific site?
MATCH (s:Site {{name: 'Curry-Zentrale'}})-[a:SITE_ASSIGNS_CONFIG]->(cp:ConfigProfile)
RETURN cp.name, cp.category, a.deviceFunctions, a.isDefault

// What is the effective config on a device?
MATCH (d:Device {{name: '6300-Zentrale'}})-[e:EFFECTIVE_CONFIG]->(cp:ConfigProfile)
RETURN cp.name, cp.category, e.sourceScope, e.sourceScopeName

// Why does this device have a specific config? (config lineage)
MATCH (d:Device {{name: '6300-Zentrale'}})-[e:EFFECTIVE_CONFIG]->(cp:ConfigProfile {{name: 'sys_central_nac'}})
RETURN e.sourceScope AS assignedAt, e.sourceScopeId, e.sourceScopeName

// Blast radius: which devices are affected by changing a profile?
MATCH (cp:ConfigProfile {{name: 'Client Access'}})<-[:EFFECTIVE_CONFIG]-(d:Device)
RETURN d.name AS device, d.serial, d.siteName

// All profiles assigned at global scope
MATCH (o:Org)-[a:ORG_ASSIGNS_CONFIG]->(cp:ConfigProfile)
RETURN cp.category, cp.name, a.deviceFunctions, a.isDefault
ORDER BY cp.category, cp.name

// Profiles assigned at device-group level
MATCH (dg:DeviceGroup)-[a:GROUP_ASSIGNS_CONFIG]->(cp:ConfigProfile)
RETURN dg.name AS deviceGroup, cp.category, cp.name, a.deviceFunctions
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

## Topology Queries

Topology data is populated lazily — call `load_topology()` or `refresh_graph()` first.
Data comes from the per-site LLDP topology API.

### Physical Path Tracing
```cypher
// L2 path between two devices (up to 5 hops)
MATCH p = (src:Device {{name: 'AP-01'}})-[:CONNECTED_TO*1..5]-(dst:Device {{name: 'Core-SW'}})
RETURN nodes(p) AS path, length(p) AS hops
ORDER BY hops LIMIT 5

// All directly connected neighbors of a device
MATCH (d:Device {{serial: 'SG99KN3020'}})-[c:CONNECTED_TO]-(neighbor:Device)
RETURN neighbor.name AS neighbor, neighbor.deviceType AS type,
       c.speed AS speedGbps, c.edgeType AS linkType, c.health AS health
```

### Failure Impact Analysis
```cypher
// If a switch fails, which devices lose their ONLY path?
// Find all devices reachable through a specific switch
MATCH (target:Device {{name: 'Core-SW'}})-[:CONNECTED_TO*1..10]-(affected:Device)
WHERE affected <> target
RETURN DISTINCT affected.name AS device, affected.deviceType AS type,
       affected.siteName AS site

// Devices downstream of a specific device (directed traversal)
MATCH (src:Device {{name: 'Distrib-SW-1'}})-[:CONNECTED_TO*1..5]->(leaf:Device)
RETURN leaf.name AS device, leaf.deviceType AS type
```

### Link Health & STP
```cypher
// All unhealthy links
MATCH (a:Device)-[c:CONNECTED_TO]->(b:Device)
WHERE c.health <> 'Good'
RETURN a.name AS from, b.name AS to, c.health AS health,
       c.speed AS speedGbps, c.stpState AS stp

// Blocking STP links
MATCH (a:Device)-[c:CONNECTED_TO]->(b:Device)
WHERE c.stpState IS NOT NULL AND c.stpState <> 'FORWARDING'
RETURN a.name AS from, b.name AS to, c.stpState AS state, c.speed AS speedGbps
```

### LAG Redundancy Audit
```cypher
// Device pairs with single-link connections (no LAG — potential SPOF)
MATCH (a:Device)-[c:CONNECTED_TO]->(b:Device)
WHERE c.lag = '' OR c.lag IS NULL
RETURN a.name AS from, b.name AS to, c.speed AS speedGbps,
       a.siteName AS site

// Device pairs with LAG (redundant links)
MATCH (a:Device)-[c:CONNECTED_TO]->(b:Device)
WHERE c.lag IS NOT NULL AND c.lag <> ''
RETURN a.name AS from, b.name AS to, c.lag AS lagGroup,
       c.fromPorts AS ports, c.speed AS speedGbps
```

### Unmanaged Devices (third-party discovered via LLDP)
```cypher
// All unmanaged devices and their managed neighbor
MATCH (d:Device)-[l:LINKED_TO]->(u:UnmanagedDevice)
RETURN d.name AS managedDevice, u.name AS unmanagedDevice,
       u.mac AS mac, l.fromPorts AS port

// Unmanaged devices per site
MATCH (s:Site)-[:HAS_UNMANAGED]->(u:UnmanagedDevice)
RETURN s.name AS site, count(u) AS unmanagedCount
```

### Combined Config + Topology
```cypher
// Devices reachable from a switch that uses a specific device group
// (blast radius: config change on group affects these physical paths)
MATCH (dg:DeviceGroup {{name: 'Verkaufstelle'}})-[:HAS_MEMBER]->(d:Device)-[:CONNECTED_TO*1..3]-(neighbor:Device)
RETURN DISTINCT d.name AS groupDevice, neighbor.name AS reachable,
       neighbor.deviceType AS type

// Topology at a specific site with device details
MATCH (s:Site {{name: 'Curry-Zentrale'}})-[:HAS_DEVICE]->(d:Device)-[c:CONNECTED_TO]->(d2:Device)
RETURN d.name AS from, d2.name AS to, c.speed AS speedGbps,
       c.edgeType AS linkType, c.health AS health, c.stpState AS stp
```

## LadybugDB Cypher Engine — Supported & Unsupported Features

### Supported
- MATCH, WHERE, RETURN, ORDER BY, LIMIT, SKIP, DISTINCT
- CREATE, SET, DELETE, MERGE
- Variable-length paths: `[*1..5]`
- Shortest path patterns: `MATCH (a)-[e* SHORTEST 1..N]->(b)`, `ALL SHORTEST`, `WSHORTEST`, `ALL WSHORTEST`
- Aggregations: count(), sum(), avg(), min(), max(), collect()
- String functions: starts with, ends with, contains, toLower(), toUpper()
- nodes(path), length(path), rels(path)
- CASE WHEN ... THEN ... ELSE ... END
- UNWIND, WITH, OPTIONAL MATCH
- IS NULL / IS NOT NULL
- Algo extension (loaded at startup): weakly_connected_components, PageRank, Louvain, SCC, K-Core via `CALL PROJECT_GRAPH()` then `CALL <algorithm>()`

### NOT Supported (will cause errors)
- APOC procedures (apoc.*)
- List comprehensions: `[x IN list | x.prop]`
- REDUCE, FOREACH
- Full-text search indexes

### For Graph Algorithms
Shortest-path queries can be expressed directly in Cypher using the SHORTEST keyword:
```cypher
MATCH (a:Device)-[e* SHORTEST 1..5]->(b:Device) WHERE a.name = 'src' AND b.name = 'dst'
RETURN nodes(e), length(e)
```
For other graph algorithms (WCC, PageRank, etc.), first project a graph and then call the algorithm:
```cypher
CALL PROJECT_GRAPH('Topo', ['Device'], ['CONNECTED_TO']);
CALL weakly_connected_components('Topo') RETURN node.serial AS serial, group_id;
```
The seed script `analyze_topology.py` demonstrates building a NetworkX graph from the
topology API and running bridges, articulation points, diameter, etc.
Find it with `list_scripts()` and run with `execute_script("analyze_topology.py", {{"site-id": "<scopeId>"}})`.
""".format(version=SCHEMA_VERSION)


# ── Helpers for dynamic property lookup (used by error hints) ────────

_PROP_RE = re.compile(r"^\s+(\w+)\s+", re.MULTILINE)
_TABLE_NAME_RE = re.compile(r"CREATE NODE TABLE IF NOT EXISTS (\w+)")


def get_node_properties() -> dict[str, list[str]]:
    """Extract {TableName: [property, ...]} from the DDL, always in sync."""
    result: dict[str, list[str]] = {}
    for ddl in NODE_TABLES + KNOWLEDGE_NODE_TABLES:
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
    return [m.group(1) for ddl in NODE_TABLES + KNOWLEDGE_NODE_TABLES
            if (m := _TABLE_NAME_RE.search(ddl))]


def get_rel_tables() -> list[str]:
    """Return all relationship table names (including topology)."""
    _rel_re = re.compile(r"CREATE REL TABLE IF NOT EXISTS (\w+)")
    all_ddl = REL_TABLES + KNOWLEDGE_REL_TABLES + TOPOLOGY_REL_TABLES + POLICY_REL_TABLES
    return [m.group(1) for ddl in all_ddl if (m := _rel_re.search(ddl))]


def compact_schema_hint() -> str:
    """One-line-per-table property summary for error messages."""
    lines = []
    for table, props in get_node_properties().items():
        lines.append(f"  {table}: {', '.join(props)}")
    rels = get_rel_tables()
    lines.append(f"  Relationships: {', '.join(rels)}")
    return "\n".join(lines)
