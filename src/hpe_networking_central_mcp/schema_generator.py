"""Generate LadybugDB DDL from OpenAPI response schemas.

Infers node tables, primary keys, and relationship tables from the
scraped API endpoint index.  Runs during the GitHub Actions build step
(no Central credentials needed) and stores the generated DDL alongside
the knowledge database archive.
"""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass, field
from typing import Any

from .oas_index import EndpointEntry, OASIndex


# ── Data classes ──────────────────────────────────────────────────────


@dataclass
class PropertyDef:
    """A single column in a node table."""

    name: str
    db_type: str  # e.g. "STRING", "INT64", "DOUBLE", "BOOLEAN", "STRING[]"


@dataclass
class NodeTableDef:
    """Definition of a LadybugDB node table inferred from API responses."""

    name: str
    primary_key: str
    properties: list[PropertyDef] = field(default_factory=list)
    source_endpoints: list[str] = field(default_factory=list)


@dataclass
class RelTableDef:
    """Definition of a LadybugDB relationship table."""

    name: str
    from_table: str
    to_table: str
    properties: list[PropertyDef] = field(default_factory=list)


# ── Type mapping ──────────────────────────────────────────────────────

_TYPE_MAP = {
    "string": "STRING",
    "integer": "INT64",
    "number": "DOUBLE",
    "boolean": "BOOLEAN",
}


def map_json_type(schema: dict) -> str:
    """Map a JSON Schema type definition to a LadybugDB column type."""
    json_type = schema.get("type", "")
    if json_type == "array":
        items = schema.get("items", {})
        items_type = items.get("type", "")
        if items_type in ("object", ""):
            return "STRING"  # complex arrays serialized as JSON
        mapped = _TYPE_MAP.get(items_type, "STRING")
        return f"{mapped}[]"
    return _TYPE_MAP.get(json_type, "STRING")


# ── Resource name extraction ──────────────────────────────────────────

_IRREGULARS = {
    "devices": "Device",
    "interfaces": "Interface",
    "clients": "Client",
    "sites": "Site",
    "switches": "Switch",
    "gateways": "Gateway",
    "addresses": "Address",
    "policies": "Policy",
    "categories": "Category",
    "entries": "Entry",
    "statuses": "Status",
    "families": "Family",
    "proxies": "Proxy",
    "bodies": "Body",
    "indices": "Index",
    "vertices": "Vertex",
}


def _singularize(word: str) -> str:
    """Simple English singularization for API resource names."""
    if word.lower() in _IRREGULARS:
        return _IRREGULARS[word.lower()]
    if word.endswith("ies") and len(word) > 3:
        return word[:-3] + "y"
    if word.endswith("ses") or word.endswith("xes") or word.endswith("zes"):
        return word[:-2]
    if word.endswith("s") and not word.endswith("ss"):
        return word[:-1]
    return word


def _to_pascal_case(name: str) -> str:
    """Convert a URL segment to PascalCase table name."""
    singular = _singularize(name)
    # Handle snake_case and kebab-case
    parts = re.split(r"[-_]", singular)
    return "".join(p.capitalize() for p in parts if p)


# ── Path parsing ──────────────────────────────────────────────────────

_PATH_PARAM_RE = re.compile(r"\{[^}]+\}")


def _parse_path_segments(path: str) -> list[tuple[str, str | None]]:
    """Parse URL path into (resource_name, param_name_or_None) pairs.

    Example: /monitoring/v1/devices/{serial}/interfaces
    → [("devices", "serial"), ("interfaces", None)]

    Skips version segments like v1, v2, v1alpha1.
    """
    parts = [p for p in path.strip("/").split("/") if p]
    segments: list[tuple[str, str | None]] = []
    i = 0
    while i < len(parts):
        part = parts[i]
        # Skip version segments
        if re.match(r"^v\d+", part):
            i += 1
            continue
        # Skip path parameters on their own
        if _PATH_PARAM_RE.fullmatch(part):
            # Attach as param of previous segment
            if segments:
                prev_name, _ = segments[-1]
                param = part.strip("{}")
                segments[-1] = (prev_name, param)
            i += 1
            continue
        # Resource segment
        segments.append((part, None))
        i += 1
    return segments


# ── Schema extraction ─────────────────────────────────────────────────

# Priority-ordered list of property names that indicate a primary key.
_PK_CANDIDATES = [
    "serial", "serial_number", "serialNumber",
    "id", "macaddr", "mac_address", "mac",
    "site_id", "siteId", "scope_id", "scopeId",
    "org_id", "orgId", "group_id", "groupId",
    "team_id", "teamId", "member_id", "memberId",
    "name",
]


def _extract_list_schema(endpoint: EndpointEntry) -> tuple[str | None, dict | None]:
    """Extract the resource name and item schema from a list endpoint response.

    Handles two patterns:
    1. Wrapped: {"type": "object", "properties": {"<resource>": {"type": "array", ...}}}
    2. Top-level: {"type": "array", "items": {...}}

    Returns (resource_name, item_schema) or (None, None) if not a list endpoint.
    """
    # Only consider GET endpoints
    if endpoint.method != "GET":
        return None, None

    # Find the 200 response
    resp_schema = None
    for resp in endpoint.responses:
        if resp.status in ("200", "201") and resp.schema:
            resp_schema = resp.schema
            break
    if not resp_schema:
        return None, None

    # Pattern 1: Top-level array
    if resp_schema.get("type") == "array":
        items = resp_schema.get("items", {})
        if items.get("type") == "object" and items.get("properties"):
            # Derive name from last path segment
            segments = _parse_path_segments(endpoint.path)
            if segments:
                name = segments[-1][0]
                return name, items
        return None, None

    # Pattern 2: Wrapped object with array property
    if resp_schema.get("type") == "object":
        props = resp_schema.get("properties", {})
        for prop_name, prop_schema in props.items():
            if (
                isinstance(prop_schema, dict)
                and prop_schema.get("type") == "array"
                and isinstance(prop_schema.get("items"), dict)
                and prop_schema["items"].get("type") == "object"
                and prop_schema["items"].get("properties")
            ):
                return prop_name, prop_schema["items"]

    # Pattern 3: Single-object detail endpoint (GET /resource/{id})
    # Detect by: path ends with a path parameter and response is an object with properties
    if resp_schema.get("type") == "object" and resp_schema.get("properties"):
        segments = _parse_path_segments(endpoint.path)
        if segments and segments[-1][1] is not None:
            # Last segment has a param → this is a detail endpoint
            # Check if the object has enough properties to be a real resource
            obj_props = resp_schema.get("properties", {})
            if len(obj_props) >= 2:
                name = segments[-1][0]
                return name, resp_schema

    return None, None


def _detect_primary_key(
    item_schema: dict,
    resource_name: str,
    path_params: list[str],
) -> str:
    """Detect the primary key for a resource from its schema and path params."""
    properties = item_schema.get("properties", {})
    required = set(item_schema.get("required", []))
    prop_names = set(properties.keys())

    # Strategy 1: required field that matches a PK candidate
    for candidate in _PK_CANDIDATES:
        if candidate in required and candidate in prop_names:
            return candidate

    # Strategy 2: path parameter that matches a property
    for pp in path_params:
        if pp in prop_names:
            return pp

    # Strategy 3: any PK candidate present in properties
    for candidate in _PK_CANDIDATES:
        if candidate in prop_names:
            return candidate

    # Strategy 4: property ending in _id or Id
    for pn in prop_names:
        if pn.endswith("_id") or pn.endswith("Id"):
            return pn

    # Strategy 5: synthesize from resource name
    synthetic = f"__{_singularize(resource_name).lower()}_pk"
    return synthetic


def _extract_properties(item_schema: dict) -> list[PropertyDef]:
    """Extract PropertyDef list from an item schema's properties."""
    result = []
    for name, prop_schema in item_schema.get("properties", {}).items():
        if not isinstance(prop_schema, dict):
            continue
        db_type = map_json_type(prop_schema)
        result.append(PropertyDef(name, db_type))
    return result


# ── Main inference functions ──────────────────────────────────────────


def infer_node_tables(index: OASIndex) -> list[NodeTableDef]:
    """Infer node table definitions from API response schemas.

    Scans all GET endpoints for list/detail responses and groups them
    by resource name.  Merges properties from multiple endpoints for
    the same resource.
    """
    # Collect resource_name → {properties, required, path_params, source_endpoints}
    resources: dict[str, dict] = {}

    for entry in index._entries:  # noqa: SLF001
        resource_name, item_schema = _extract_list_schema(entry)
        if not resource_name or not item_schema:
            continue

        table_name = _to_pascal_case(resource_name)
        endpoint_id = f"{entry.method}:{entry.path}"

        # Collect path params for PK detection
        path_params = [p.name for p in entry.parameters if p.location == "path"]

        if table_name not in resources:
            resources[table_name] = {
                "schemas": [],
                "path_params": [],
                "source_endpoints": [],
            }
        resources[table_name]["schemas"].append(item_schema)
        resources[table_name]["path_params"].extend(path_params)
        resources[table_name]["source_endpoints"].append(endpoint_id)

    # Build NodeTableDef for each resource
    result = []
    for table_name, info in resources.items():
        # Merge properties from all schemas
        merged_props: dict[str, PropertyDef] = {}
        merged_required: set[str] = set()
        for schema in info["schemas"]:
            for prop in _extract_properties(schema):
                if prop.name not in merged_props:
                    merged_props[prop.name] = prop
            merged_required.update(schema.get("required", []))

        # Build merged schema for PK detection
        merged_schema = {
            "properties": {p.name: {} for p in merged_props.values()},
            "required": list(merged_required),
        }

        pk = _detect_primary_key(
            merged_schema,
            table_name,
            info["path_params"],
        )

        # Ensure PK property exists
        if pk not in merged_props:
            merged_props[pk] = PropertyDef(pk, "STRING")

        # Sort properties: PK first, then alphabetically
        props = sorted(merged_props.values(), key=lambda p: (p.name != pk, p.name))

        result.append(NodeTableDef(
            name=table_name,
            primary_key=pk,
            properties=props,
            source_endpoints=info["source_endpoints"],
        ))

    return result


def infer_rel_tables(
    index: OASIndex,
    node_tables: list[NodeTableDef],
) -> list[RelTableDef]:
    """Infer relationship tables from URL path nesting patterns.

    For each endpoint whose URL is nested under a parent resource
    (e.g. /devices/{serial}/interfaces), creates a relationship from
    the parent table to the child table.
    """
    table_names = {t.name for t in node_tables}
    # Map resource segment (plural) → table name
    seg_to_table: dict[str, str] = {}
    for entry in index._entries:  # noqa: SLF001
        resource_name, item_schema = _extract_list_schema(entry)
        if resource_name:
            pascal = _to_pascal_case(resource_name)
            if pascal in table_names:
                segments = _parse_path_segments(entry.path)
                for seg_name, _ in segments:
                    if _to_pascal_case(seg_name) == pascal:
                        seg_to_table[seg_name] = pascal

    # Find parent→child nesting
    seen: set[tuple[str, str]] = set()
    rels: list[RelTableDef] = []

    for entry in index._entries:  # noqa: SLF001
        segments = _parse_path_segments(entry.path)
        # Look for consecutive segments where both map to known tables
        # and the first has a path param (indicating parent resource)
        for i in range(len(segments) - 1):
            seg_name, seg_param = segments[i]
            child_name, _ = segments[i + 1]

            parent_table = seg_to_table.get(seg_name)
            child_table = seg_to_table.get(child_name)

            if parent_table and child_table and seg_param:
                pair = (parent_table, child_table)
                if pair not in seen:
                    seen.add(pair)
                    child_singular = _singularize(child_name)
                    rel_name = f"HAS_{child_singular.upper()}"
                    rels.append(RelTableDef(
                        name=rel_name,
                        from_table=parent_table,
                        to_table=child_table,
                    ))

    return rels


# ── DDL generation ────────────────────────────────────────────────────


def generate_ddl(
    node_tables: list[NodeTableDef],
    rel_tables: list[RelTableDef],
) -> list[str]:
    """Generate LadybugDB DDL statements from inferred definitions.

    Returns node table DDL first, then relationship table DDL.
    """
    stmts: list[str] = []

    for node in node_tables:
        lines = [f"CREATE NODE TABLE IF NOT EXISTS {node.name} ("]
        for i, prop in enumerate(node.properties):
            comma = "," if i < len(node.properties) - 1 else ","
            lines.append(f"    {prop.name} {prop.db_type}{comma}")
        lines.append(f"    PRIMARY KEY ({node.primary_key})")
        lines.append(")")
        stmts.append("\n".join(lines))

    for rel in rel_tables:
        if rel.properties:
            prop_lines = ",\n".join(
                f"    {p.name} {p.db_type}" for p in rel.properties
            )
            stmts.append(
                f"CREATE REL TABLE IF NOT EXISTS {rel.name} (\n"
                f"    FROM {rel.from_table} TO {rel.to_table},\n"
                f"{prop_lines}\n"
                f")"
            )
        else:
            stmts.append(
                f"CREATE REL TABLE IF NOT EXISTS {rel.name} "
                f"(FROM {rel.from_table} TO {rel.to_table})"
            )

    return stmts


# ── Content hash ──────────────────────────────────────────────────────


def content_hash(ddl: list[str]) -> str:
    """Compute a truncated SHA-256 hash of the DDL statements.

    Used as a schema version identifier — when DDL changes, the hash
    changes, triggering a DB rebuild.
    """
    h = hashlib.sha256()
    for stmt in sorted(ddl):
        h.update(stmt.encode("utf-8"))
    return h.hexdigest()[:16]
