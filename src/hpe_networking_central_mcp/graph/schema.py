"""LadybugDB graph schema DDL for the Aruba Central configuration hierarchy.

Defines bootstrap node and relationship tables for:
  - Domain: Org, SiteCollection, Site, Device, DeviceGroup, ConfigProfile, UnmanagedDevice
  - Knowledge: ApiEndpoint, ApiCategory, EntityType, DocSection, Script
  - Topology: CONNECTED_TO, LINKED_TO
  - Policy: *_ASSIGNS_CONFIG, EFFECTIVE_CONFIG

Additional tables may be created dynamically from generated_ddl.json
(produced by the build pipeline's schema_generator).
"""

from __future__ import annotations

import re

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
        fetched_at  STRING,
        source_api  STRING,
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
        fetched_at     STRING,
        source_api     STRING,
        PRIMARY KEY (scopeId)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS DeviceGroup (
        scopeId     STRING,
        name        STRING,
        deviceCount INT64,
        fetched_at  STRING,
        source_api  STRING,
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
        fetched_at      STRING,
        source_api      STRING,
        PRIMARY KEY (serial)
    )
    """,
    """
    CREATE NODE TABLE IF NOT EXISTS ConfigProfile (
        id               STRING,
        name             STRING,
        category         STRING,
        scopeId          STRING,
        deviceFunction   STRING,
        objectType       STRING,
        isDefault        BOOLEAN,
        isEditable       BOOLEAN,
        deviceScopeOnly  BOOLEAN,
        mergeStrategy    STRING,
        assignedScopeIds STRING,
        assignedDeviceFunctions STRING,
        fetched_at       STRING,
        source_api       STRING,
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
        fetched_at     STRING,
        source_api     STRING,
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
        parameters     STRING,
        requestBody    STRING,
        responses      STRING,
        embedding      FLOAT[384],
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
        embedding  FLOAT[384],
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

# Provenance tables — track which API populated domain nodes
PROVENANCE_REL_TABLES: list[str] = [
    """
    CREATE REL TABLE IF NOT EXISTS POPULATED_BY (
        FROM Device TO ApiEndpoint,
        fetched_at STRING,
        seed       STRING,
        run_id     STRING
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS POPULATED_BY (
        FROM Site TO ApiEndpoint,
        fetched_at STRING,
        seed       STRING,
        run_id     STRING
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS POPULATED_BY (
        FROM SiteCollection TO ApiEndpoint,
        fetched_at STRING,
        seed       STRING,
        run_id     STRING
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS POPULATED_BY (
        FROM DeviceGroup TO ApiEndpoint,
        fetched_at STRING,
        seed       STRING,
        run_id     STRING
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS POPULATED_BY (
        FROM ConfigProfile TO ApiEndpoint,
        fetched_at STRING,
        seed       STRING,
        run_id     STRING
    )
    """,
    """
    CREATE REL TABLE IF NOT EXISTS POPULATED_BY (
        FROM UnmanagedDevice TO ApiEndpoint,
        fetched_at STRING,
        seed       STRING,
        run_id     STRING
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
        reason      STRING,
        operation   STRING
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

# ── Config policy relationship tables ────────────────────────────────

POLICY_REL_TABLES: list[str] = [
    "CREATE REL TABLE IF NOT EXISTS ORG_ASSIGNS_CONFIG         (FROM Org            TO ConfigProfile)",
    "CREATE REL TABLE IF NOT EXISTS COLLECTION_ASSIGNS_CONFIG  (FROM SiteCollection TO ConfigProfile)",
    "CREATE REL TABLE IF NOT EXISTS SITE_ASSIGNS_CONFIG        (FROM Site           TO ConfigProfile)",
    "CREATE REL TABLE IF NOT EXISTS GROUP_ASSIGNS_CONFIG       (FROM DeviceGroup    TO ConfigProfile)",
    "CREATE REL TABLE IF NOT EXISTS DEVICE_ASSIGNS_CONFIG      (FROM Device         TO ConfigProfile)",
    """
    CREATE REL TABLE IF NOT EXISTS EFFECTIVE_CONFIG (
        FROM Device TO ConfigProfile,
        sourceScope  STRING,
        sourceScopeId STRING
    )
    """,
]

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
    """Return all relationship table names (including topology and policy)."""
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
