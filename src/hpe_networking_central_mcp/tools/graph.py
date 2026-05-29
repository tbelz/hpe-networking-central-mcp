"""MCP tools for querying and writing to the LadybugDB graph database."""

from __future__ import annotations

import json
import os
import re
from datetime import datetime, timezone

import structlog
from mcp.server.fastmcp.exceptions import ToolError
from mcp.types import ToolAnnotations

from ..config import Settings
from ..graph.manager import GraphManager
from ..graph.schema import VOLATILE_FIELDS, compact_schema_hint, get_node_properties

logger = structlog.get_logger("tools.graph")

# Column-name shape Kuzu uses for property projections, e.g. "d.status".
_PROJ_RE = re.compile(r"^(?P<alias>[A-Za-z_][\w]*)\.(?P<prop>[A-Za-z_][\w]*)$")

# Matches Cypher node patterns like "(d:Device)" or "(s :Site {scopeId: ...})".
_ALIAS_LABEL_RE = re.compile(r"\(\s*([A-Za-z_]\w*)\s*:\s*([A-Za-z_]\w*)")

# Matches the binder hint "for <alias>" in a 'Cannot find property X for d.' message.
_FOR_ALIAS_RE = re.compile(r"\bfor\s+([A-Za-z_]\w*)\b", re.IGNORECASE)


def _parse_alias_labels(cypher: str) -> dict[str, str]:
    """Best-effort alias->label map from MATCH/CREATE/MERGE node patterns."""
    if not cypher:
        return {}
    return {m.group(1): m.group(2) for m in _ALIAS_LABEL_RE.finditer(cypher)}


def _default_stale_threshold_seconds() -> int:
    raw = os.environ.get("MCP_GRAPH_STALE_THRESHOLD_SECONDS", "900")
    try:
        return max(0, int(raw))
    except ValueError:
        return 900


def _env_int(name: str, default: int) -> int:
    raw = os.environ.get(name)
    if raw is None:
        return default
    try:
        v = int(raw)
        return v if v > 0 else default
    except ValueError:
        return default


def _per_cell_byte_cap() -> int:
    return _env_int("MCP_GRAPH_PER_CELL_BYTES", 4096)


def _per_response_byte_cap() -> int:
    return _env_int("MCP_GRAPH_PER_RESPONSE_BYTES", 50_000)


def _truncate_cell(value: str, cap: int) -> dict:
    """Replace an oversize string cell with a typed truncation envelope.

    Keeps the agent aware the value exists (and how to fetch it) instead of
    silently inlining a 100 KB JSON blob like ``bodyJson``.
    """
    # Use UTF-8 byte length — multi-byte characters would otherwise cause the
    # reported size and cap enforcement to diverge from the actual wire size.
    byte_len = len(value.encode("utf-8"))
    return {
        "_truncated": True,
        "preview": value[:200],
        "size_bytes": byte_len,
        "hint": (
            "Cell exceeded per-cell cap. Use get_raw_schema(component_id) "
            "for raw JSON, or walk COMPOSED_OF/HAS_PROPERTY to enumerate "
            "fields structurally."
        ),
    }


def _apply_per_cell_cap(rows: list[dict], cap: int) -> list[dict]:
    """Walk rows in place; replace string cells longer than ``cap`` with envelopes."""
    if not rows or cap <= 0:
        return rows
    for row in rows:
        if not isinstance(row, dict):
            continue
        for key, val in list(row.items()):
            if isinstance(val, str) and len(val.encode("utf-8")) > cap:
                row[key] = _truncate_cell(val, cap)
    return rows


def _apply_response_byte_cap(
    rows: list[dict], cap: int, base_envelope: dict | None = None
) -> tuple[list[dict], dict | None]:
    """If the JSON-serialised rows exceed ``cap``, drop rows until they fit.

    Returns ``(rows_to_emit, envelope_or_None)``. When trimming occurs, the
    envelope is the wrapper to serialise instead of the raw row array.
    """
    def _measure(r: list[dict]) -> int:
        # Measure UTF-8 bytes of the indented form — matching what the tool
        # actually serialises — so the cap is a true byte budget, not a
        # character count of compact JSON.
        return len(json.dumps(r, default=str, indent=2).encode("utf-8"))

    if cap <= 0 or not rows:
        return rows, None
    total = _measure(rows)
    if total <= cap:
        return rows, None
    # Binary-search the largest prefix that fits under the cap.
    lo, hi = 0, len(rows)
    while lo < hi:
        mid = (lo + hi + 1) // 2
        if _measure(rows[:mid]) <= cap:
            lo = mid
        else:
            hi = mid - 1
    kept = rows[:lo]
    env = {
        "truncated": True,
        "reason": "response_byte_cap",
        "cap_bytes": cap,
        "total_bytes": total,
        "rows_returned": len(kept),
        "rows_dropped": len(rows) - len(kept),
        "warning": (
            "Response exceeded byte cap. Add LIMIT/WHERE, project fewer "
            "columns, or avoid RETURN c.bodyJson; use get_raw_schema for "
            "single-component raw fetches."
        ),
        "rows": kept,
    }
    if base_envelope:
        for k, v in base_envelope.items():
            env.setdefault(k, v)
    return kept, env


def _coerce_datetime(value: object) -> datetime | None:
    """Best-effort conversion of a graph value to a timezone-aware datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, str):
        try:
            dt = datetime.fromisoformat(value.replace("Z", "+00:00"))
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
    return None


def _label_from_node(node: object) -> str | None:
    """Pull the node label out of whatever shape Kuzu hands us."""
    if not isinstance(node, dict):
        return None
    for key in ("_label", "label", "_labels", "labels"):
        v = node.get(key)
        if isinstance(v, str):
            return v
        if isinstance(v, list) and v:
            return str(v[0])
    return None


def _scan_freshness(
    rows: list[dict], threshold_seconds: int, cypher: str = ""
) -> list[dict]:
    """Return freshness_warnings for any volatile fields present in rows.

    A warning is emitted when:
      - a projected column matches a known volatile field (e.g. "d.status"
        where the Device node carries lastSyncedAt), OR
      - a row column is a whole node carrying lastSyncedAt + a volatile
        property,
    AND the lastSyncedAt is older than threshold_seconds (or missing).

    Each warning aggregates by (label, age bucket) and reports the maximum
    age observed plus the volatile fields that triggered it.
    """
    if not rows:
        return []

    now = datetime.now(timezone.utc)
    # Map label -> {"volatile_in_result": set[str], "max_age": int|None, "stamped": bool, "row_ids": set[int]}
    findings: dict[str, dict] = {}

    volatile_props_by_label: dict[str, set[str]] = {
        lbl: set(fields) for lbl, fields in VOLATILE_FIELDS.items()
    }
    alias_to_label = _parse_alias_labels(cypher)

    def _record(label: str, prop: str, ts: datetime | None, row_idx: int) -> None:
        age = int((now - ts).total_seconds()) if ts else None
        if ts is not None and age is not None and age < threshold_seconds:
            return
        bucket = findings.setdefault(
            label,
            {"volatile_in_result": set(), "max_age_seconds": None,
             "stamped": True, "row_ids": set()},
        )
        bucket["volatile_in_result"].add(prop)
        bucket["row_ids"].add(row_idx)
        if ts is None:
            bucket["stamped"] = False
        elif age is not None and (
            bucket["max_age_seconds"] is None
            or age > bucket["max_age_seconds"]
        ):
            bucket["max_age_seconds"] = age

    for row_idx, row in enumerate(rows):
        if not isinstance(row, dict):
            continue

        # 1. Whole-node columns (dict carrying a label + volatile props).
        for value in row.values():
            label = _label_from_node(value)
            if not label or label not in volatile_props_by_label:
                continue
            triggered = {p for p in volatile_props_by_label[label] if p in value}
            if not triggered:
                continue
            ts = _coerce_datetime(value.get("lastSyncedAt"))
            for prop in triggered:
                _record(label, prop, ts, row_idx)

        # 2. Projected scalar columns (alias.property). Disambiguate the
        # node label via the cypher MATCH pattern when possible; otherwise
        # fall back to the unique label that owns the property.
        for col in row:
            m = _PROJ_RE.match(col)
            if not m:
                continue
            alias = m.group("alias")
            prop = m.group("prop")
            label = alias_to_label.get(alias)
            if label is not None:
                if prop not in volatile_props_by_label.get(label, set()):
                    continue
                candidate_labels = [label]
            else:
                candidate_labels = [
                    lbl for lbl, vfields in volatile_props_by_label.items()
                    if prop in vfields
                ]
                # Without an alias->label binding, only record when the
                # property name belongs to exactly one label; otherwise we
                # would have to guess.
                if len(candidate_labels) != 1:
                    continue
            lsa_col = f"{alias}.lastSyncedAt"
            ts = _coerce_datetime(row.get(lsa_col))
            for lbl in candidate_labels:
                _record(lbl, prop, ts, row_idx)

    if not findings:
        return []

    warnings: list[dict] = []
    for label, data in findings.items():
        warnings.append({
            "node_label": label,
            "volatile_fields_in_result": sorted(data["volatile_in_result"]),
            "max_age_seconds": data["max_age_seconds"],
            "lastSyncedAt_present": data["stamped"],
            "rows_affected": len(data["row_ids"]),
            "threshold_seconds": threshold_seconds,
            "recommendation": (
                f"Values for {label} fields {sorted(data['volatile_in_result'])} "
                "may be stale. For live state call the Central API directly "
                "(e.g. call_central_api on the matching monitoring endpoint); "
                f"to refresh the graph run execute_script('populate_base_graph.py'"
                + (", parameters={'site-id': '<siteScopeId>'}" if label in ('Device', 'Site') else "")
                + ")."
                + (
                    " Device rows are refreshed by re-fetching their containing"
                    " site, so pass the site's scopeId (not a device serial)."
                    if label == 'Device' else ""
                )
            ),
        })
    return warnings


def _build_error_hint(error_msg: str, cypher: str = "") -> str:
    """Build a context-aware hint from a Cypher error message.

    For "Cannot find property X for <alias>" errors we resolve <alias> back
    to the node label declared in the user's cypher (e.g. ``(d:Device)``)
    and return *that* label's properties, instead of substring-matching
    table names against the message (which would pick up the OAS-schema
    ``Property`` node table for any error containing the word "property").
    """
    msg_lower = error_msg.lower()
    is_property_err = (
        "cannot find property" in msg_lower
        or ("property" in msg_lower and "does not exist" in msg_lower)
    )

    if is_property_err:
        node_props = get_node_properties()
        alias_to_label = _parse_alias_labels(cypher)
        alias_match = _FOR_ALIAS_RE.search(error_msg)
        if alias_match:
            alias = alias_match.group(1)
            label = alias_to_label.get(alias)
            if label and label in node_props:
                return (
                    f"\n\nValid {label} properties: {', '.join(node_props[label])}\n"
                    "Read graph://schema for the full schema."
                )
        return (
            f"\n\nRead graph://schema for the full schema with node types, "
            f"properties, and relationships.\n\n{compact_schema_hint()}"
        )

    if "does not exist" in msg_lower or "cannot find" in msg_lower:
        return f"\n\nRead graph://schema for the full schema with node types, properties, and relationships.\n\n{compact_schema_hint()}"

    return ""


def register_graph_tools(mcp, settings: Settings, graph: GraphManager):
    """Register graph query and write tools with the MCP server."""

    def _run_query(cypher: str, parameters: str, tool_label: str) -> str:
        """Shared body for ``query_graph`` and its focused aliases.

        All 5 read tools (``query_graph``, ``query_api_schema``, ``query_fts``,
        ``query_topology``, ``query_yang``) delegate here. They differ only
        in docstring focus so each gets its own ~1500-char guidance budget
        without overflowing one giant tool description; the caps, error
        hints, and freshness scanning are identical.
        """
        if not cypher or not cypher.strip():
            raise ToolError(
                "Cypher query cannot be empty. Read graph://schema for the schema."
            )

        try:
            params = json.loads(parameters) if parameters else {}
        except json.JSONDecodeError as exc:
            raise ToolError(
                f"Invalid JSON in parameters: {exc}. "
                "Example: {\"serial\": \"SN001\"}"
            )
        if not isinstance(params, dict):
            raise ToolError("parameters must decode to a JSON object (dict).")

        try:
            rows = graph.query(cypher, params=params, read_only=True)
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            msg = str(exc)
            hint = _build_error_hint(msg, cypher)
            raise ToolError(f"Cypher query failed: {msg}{hint}")

        soft_cap = 200
        hard_cap = 2000
        n = len(rows)
        if n > hard_cap:
            raise ToolError(
                f"Query returned {n} rows which exceeds the hard cap of {hard_cap}. "
                "Add LIMIT or WHERE filters, or aggregate with COUNT/COLLECT."
            )

        logger.info("query_done", tool=tool_label, rows=n)
        threshold = _default_stale_threshold_seconds()
        freshness_warnings = (
            _scan_freshness(rows, threshold, cypher) if threshold > 0 else []
        )

        # Per-cell byte cap: replace oversize string cells (e.g. bodyJson)
        # with a typed truncation envelope BEFORE row-cap envelope so the
        # serialised payload stays small.
        rows = _apply_per_cell_cap(rows, _per_cell_byte_cap())

        if n > soft_cap:
            envelope = {
                "truncated": True,
                "cap": soft_cap,
                "total_returned": n,
                "warning": (
                    f"Result truncated: query returned {n} rows; only the first "
                    f"{soft_cap} are shown. Add LIMIT/WHERE or aggregate to see "
                    "the rest."
                ),
                "rows": rows[:soft_cap],
            }
            if freshness_warnings:
                envelope["freshness_warnings"] = freshness_warnings
            kept, byte_env = _apply_response_byte_cap(
                envelope["rows"], _per_response_byte_cap(),
            )
            if byte_env is not None:
                if freshness_warnings:
                    byte_env["freshness_warnings"] = freshness_warnings
                byte_env["row_cap"] = soft_cap
                byte_env["total_returned"] = n
                return json.dumps(byte_env, indent=2, default=str)
            return json.dumps(envelope, indent=2, default=str)

        kept, byte_env = _apply_response_byte_cap(rows, _per_response_byte_cap())
        if byte_env is not None:
            if freshness_warnings:
                byte_env["freshness_warnings"] = freshness_warnings
            return json.dumps(byte_env, indent=2, default=str)

        if freshness_warnings:
            return json.dumps(
                {"rows": rows, "freshness_warnings": freshness_warnings},
                indent=2,
                default=str,
            )
        return json.dumps(rows, indent=2, default=str)

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_graph(cypher: str, parameters: str = "{}") -> str:
        """Run a read-only Cypher query against the Central graph (general escape hatch).

        **Prefer one of the focused aliases first** — each has a smaller,
        topic-specific docstring with the right canonical query templates:

        - ``query_api_schema`` — walk OpenAPI endpoints, schemas, properties,
          YANG paths; the right tool for "what fields does endpoint X take".
        - ``query_fts`` — keyword search across endpoints / docs / scripts /
          devices / sites / configs via ``CALL QUERY_FTS_INDEX(...)``.
        - ``query_topology`` — Org / SiteCollection / Site / Device /
          DeviceGroup / UnmanagedDevice and their HAS_* / LINKED_TO edges.
        - ``query_yang`` — YangPath nodes, CONFIGURES_YANG /
          PROPERTY_AT_YANG (forward and reverse lookups).

        Reach for ``query_graph`` only when your query spans those topics
        (e.g. joining a topology MATCH to an FTS YIELD) or uses node tables
        none of the aliases describe (``DocSection``, ``Script``,
        ``ApiCategory``, custom-added labels via ``write_graph``).

        For writes, use ``write_graph``. For raw schema JSON of a single
        component, use ``get_raw_schema(component_id)``.

        Schema reference: read ``graph://schema``. Node tables include
        ``ApiEndpoint``, ``Parameter``, ``RequestBody``, ``Response``,
        ``SchemaComponent``, ``Property``, ``YangPath``, ``Org``,
        ``SiteCollection``, ``Site``, ``Device``, ``DeviceGroup``,
        ``UnmanagedDevice``, ``DocSection``, ``Script``, ``ApiCategory``.

        Row caps: soft 200, hard 2000 (rejected).
        Byte caps: per-cell ~4 KB, per-response ~50 KB. Oversize string
        cells (typically ``bodyJson``) come back as
        ``{"_truncated": true, "preview": "...", "size_bytes": N,
        "hint": "use get_raw_schema(...)"}``. All caps overridable via
        ``MCP_GRAPH_PER_CELL_BYTES`` / ``MCP_GRAPH_PER_RESPONSE_BYTES``.

        Args:
            cypher: A read-only Cypher query (or ``CALL QUERY_FTS_INDEX``).
            parameters: JSON-encoded parameter dict (default ``"{}"``).

        Returns:
            JSON array of rows, or a truncation envelope when caps trip.
        """
        return _run_query(cypher, parameters, "query_graph")

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_api_schema(cypher: str, parameters: str = "{}") -> str:
        """Cypher over the OpenAPI subgraph: endpoints, schemas, properties, YANG.

        Node tables: ``ApiEndpoint(method, path, summary, description,
        operationId, category)``, ``Parameter(name, location, required, type,
        inferredHint)``, ``RequestBody``, ``Response(status)``,
        ``SchemaComponent(component_id PK, name, section, kind, bodyShape,
        supportedDeviceTypes, bodyJson)``,
        ``Property(property_id PK, parent_component_id, name, type, required,
        enumValues, supportedDeviceTypes, yangPath, readOnly)``,
        ``YangPath(yangPath PK, module)``.

        ``bodyShape`` ∈ {object, union-oneOf, union-anyOf, allOf-composite,
        map, array, primitive, unresolved}. ``section='inline'`` marks a
        synthetic component promoted from an inline allOf/oneOf/items branch
        (its ``component_id`` contains ``#``).

        Edges: ``HAS_PARAMETER``, ``HAS_REQUEST_BODY``, ``HAS_RESPONSE``,
        ``BODY_REFERENCES``, ``RESPONSE_REFERENCES``,
        ``COMPOSED_OF {kind}`` (allOf/oneOf/anyOf, between SchemaComponents),
        ``HAS_PROPERTY`` (component → its direct fields only),
        ``HAS_VALUE_SCHEMA`` (map value shape),
        ``PROPERTY_OF_TYPE`` (property → referenced nested SchemaComponent),
        ``PROPERTY_AT_YANG``, ``CONFIGURES_YANG``.

        ## CANONICAL — walk all fields of a request body

        Properties live ONLY on the component that declares them. ALWAYS
        ``COMPOSED_OF*0..5`` first to pick up allOf parents / promoted inline
        branches, and ALWAYS use ``DISTINCT`` (multi-path traversals double-
        count when a property is reachable via two COMPOSED_OF edges):

        ```cypher
        MATCH (e:ApiEndpoint {method: $m, path: $p})
              -[:HAS_REQUEST_BODY]->(:RequestBody)
              -[:BODY_REFERENCES]->(root:SchemaComponent)
        MATCH (root)-[:COMPOSED_OF*0..5]->(c:SchemaComponent)
              -[:HAS_PROPERTY]->(p:Property)
        RETURN DISTINCT c.name AS declaredOn, p.name, p.type, p.required,
               p.enumValues, p.supportedDeviceTypes, p.yangPath
        ORDER BY declaredOn, p.required DESC, p.name
        ```

        Device-type filter (covers NULL = "applies to all", empty list, and
        explicit match):

        ```
        WHERE p.supportedDeviceTypes IS NULL
           OR size(p.supportedDeviceTypes) = 0
           OR $deviceType IN p.supportedDeviceTypes
        ```

        ## CANONICAL — descend into nested object properties (``type=""``)

        A property with empty ``type`` is usually a ref to another
        SchemaComponent. Follow ``PROPERTY_OF_TYPE`` and recurse with the
        canonical walk above on the target component:

        ```cypher
        MATCH (p:Property {parent_component_id: $cid, name: $field})
              -[:PROPERTY_OF_TYPE]->(child:SchemaComponent)
        MATCH (child)-[:COMPOSED_OF*0..5]->(c:SchemaComponent)
              -[:HAS_PROPERTY]->(np:Property)
        RETURN DISTINCT c.name, np.name, np.type, np.required
        ```

        ## Other patterns

        ```cypher
        // Endpoints in a category
        MATCH (e:ApiEndpoint {category: $cat, method: 'GET'})
        RETURN e.path, e.summary ORDER BY e.path

        // Required parameters of an endpoint
        MATCH (e:ApiEndpoint {method: $m, path: $p})
              -[:HAS_PARAMETER]->(p:Parameter {required: true})
        RETURN p.name, p.location, p.type

        // Branches of a union component
        MATCH (c:SchemaComponent {name: $name})-[r:COMPOSED_OF]->(b:SchemaComponent)
        WHERE c.bodyShape IN ['union-oneOf','union-anyOf']
        RETURN r.kind, b.name, b.bodyShape
        ```

        Caps and ``get_raw_schema`` escape hatch behave as in ``query_graph``.

        Args:
            cypher: Read-only Cypher.
            parameters: JSON-encoded parameter dict (default ``"{}"``).
        """
        return _run_query(cypher, parameters, "query_api_schema")

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_fts(cypher: str, parameters: str = "{}") -> str:
        """Full-text search over the graph via ``CALL QUERY_FTS_INDEX(...)``.

        FTS is REQUIRED for keyword discovery — path-grep misses cases like
        "vrf config" (which lives only under ``/stacks/``; the path doesn't
        mention vrf, the description does). Each FTS hit yields ``node`` +
        ``score``, which you can chain into a structural follow-up MATCH in
        the same Cypher block.

        Invocation shape:

        ```cypher
        CALL QUERY_FTS_INDEX($table, $index, $query)
        YIELD node, score
        RETURN ... ORDER BY score DESC LIMIT 25
        ```

        Available indexes (table → index → fields):

        - ``ApiEndpoint`` → ``api_fts`` → summary, description, path, operationId
        - ``DocSection`` → ``doc_fts`` → title, body
        - ``Script`` → ``script_fts`` → name, description
        - ``Property`` → ``property_fts`` → name, description, yangPath
        - ``Device`` → ``device_fts`` → name, serial, model (runtime, populated
          by live seed)
        - ``Site`` → ``site_fts`` → name, address (runtime)
        - ``SchemaComponent`` / config nodes → ``config_fts`` (runtime)

        ## Canonical: keyword → endpoint

        ```cypher
        CALL QUERY_FTS_INDEX('ApiEndpoint', 'api_fts', 'vrf')
        YIELD node, score
        RETURN node.method, node.path, node.summary, score
        ORDER BY score DESC LIMIT 25
        ```

        ## Canonical: keyword → endpoint → schema (one query)

        ```cypher
        CALL QUERY_FTS_INDEX('ApiEndpoint', 'api_fts', 'mvrp')
        YIELD node AS e, score
        MATCH (e)-[:HAS_REQUEST_BODY]->(:RequestBody)
              -[:BODY_REFERENCES]->(c:SchemaComponent)
        RETURN e.method, e.path, c.name, score
        ORDER BY score DESC LIMIT 10
        ```

        ## Canonical: keyword → property → owning component → endpoints

        Use this when you know what a field DOES (e.g. "ntp server",
        "vrf binding") but not which schema or endpoint owns it. Free-
        text matches on Property descriptions and YANG paths in one hop.

        ```cypher
        CALL QUERY_FTS_INDEX('Property', 'property_fts', 'ntp server')
        YIELD node AS p, score
        MATCH (c:SchemaComponent)-[:HAS_PROPERTY]->(p)
        OPTIONAL MATCH (e:ApiEndpoint)
                 -[:HAS_REQUEST_BODY|:HAS_RESPONSE]->()
                 -[:BODY_REFERENCES|:RESPONSE_REFERENCES]->(root:SchemaComponent)
                 -[:COMPOSED_OF*0..5]->(c)
        RETURN p.name, p.type, c.name AS declaredOn,
               e.method, e.path, score
        ORDER BY score DESC LIMIT 25
        ```

        ## Canonical: keyword → docs

        ```cypher
        CALL QUERY_FTS_INDEX('DocSection', 'doc_fts', 'firmware compliance')
        YIELD node, score
        RETURN node.title, node.section_id, score
        ORDER BY score DESC LIMIT 10
        ```

        Tips: the third arg accepts Lucene-style terms (``"foo bar"``,
        ``foo*``, ``foo OR bar``). For multi-word phrases prefer quoting.
        If FTS returns 0 rows, fall back to ``CONTAINS`` on a property:
        ``MATCH (e:ApiEndpoint) WHERE e.description CONTAINS $kw RETURN ...``.

        Caps as in ``query_graph``.

        Args:
            cypher: Read-only Cypher beginning with ``CALL QUERY_FTS_INDEX``
                (chaining a MATCH after the YIELD is encouraged).
            parameters: JSON-encoded parameter dict (default ``"{}"``).
        """
        return _run_query(cypher, parameters, "query_fts")

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_topology(cypher: str, parameters: str = "{}") -> str:
        """Cypher over the live Aruba Central network topology subgraph.

        Node tables (all carry ``lastSyncedAt TIMESTAMP``; volatile fields
        flagged in ``freshness_warnings`` when stale):

        - ``Org(scopeId PK, name)``
        - ``SiteCollection(scopeId PK, name, parent_scope_id)``
        - ``Site(scopeId PK, name, address, city, country, latitude, longitude)``
        - ``Device(serial PK, name, model, deviceType, status, ipv4, mac,
          firmware, persona, deviceFunction, siteId, siteName, partNumber,
          deployment, configStatus, deviceGroupId, deviceGroupName)``
        - ``DeviceGroup(scopeId PK, name, deviceCount)``
        - ``UnmanagedDevice(mac PK, name, model, deviceType, health, status,
          ipv4, siteId)``

        Edges (parent → child unless noted):

        - ``(Org)-[:HAS_COLLECTION]->(SiteCollection)``
        - ``(Org)-[:HAS_SITE]->(Site)`` (standalone site, no collection)
        - ``(SiteCollection)-[:CONTAINS_SITE]->(Site)``
        - ``(Site)-[:HAS_DEVICE]->(Device)``
        - ``(Site)-[:HAS_UNMANAGED]->(UnmanagedDevice)``
        - ``(DeviceGroup)-[:HAS_MEMBER]->(Device)`` (cross-site grouping)
        - ``(Device)-[:CONNECTED_TO]->(Device)`` — LLDP/CDP neighbour
        - ``(Device)-[:LINKED_TO]->(UnmanagedDevice)`` — edge-of-managed

        ## Canonical: all devices at a site

        ```cypher
        MATCH (s:Site {name: $siteName})-[:HAS_DEVICE]->(d:Device)
        RETURN d.serial, d.name, d.model, d.deviceType, d.status
        ORDER BY d.deviceType, d.name
        ```

        ## Canonical: blast radius (all devices in a device group across sites)

        ```cypher
        MATCH (g:DeviceGroup {name: $groupName})-[:HAS_MEMBER]->(d:Device)
        MATCH (s:Site)-[:HAS_DEVICE]->(d)
        RETURN s.name AS site, d.serial, d.name, d.model
        ORDER BY site, d.name
        ```

        ## Canonical: L2 neighbours of a switch

        ```cypher
        MATCH (d:Device {serial: $serial})-[:CONNECTED_TO]->(n:Device)
        RETURN n.serial, n.name, n.deviceType, n.model
        ```

        Freshness: when you SELECT a volatile field (``status``, ``ip``,
        ``firmware``, ...) and the node's ``lastSyncedAt`` is older than
        ~15 min, the response wraps as ``{"rows": [...],
        "freshness_warnings": [...]}``. Override the staleness threshold
        via ``MCP_GRAPH_STALE_THRESHOLD_SECONDS``.

        Caps as in ``query_graph``.

        Args:
            cypher: Read-only Cypher.
            parameters: JSON-encoded parameter dict (default ``"{}"``).
        """
        return _run_query(cypher, parameters, "query_topology")

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def query_yang(cypher: str, parameters: str = "{}") -> str:
        """Cypher over the YANG reverse-index subgraph (Phase 3).

        Lets you map a known YANG path (e.g. from a legacy CLI/YANG config
        or telemetry stream) back to the API endpoints that configure it,
        or to the property/schema-component where it surfaces.

        Node table: ``YangPath(yangPath PK, module)``.

        Edges:

        - ``(Property)-[:PROPERTY_AT_YANG]->(YangPath)`` — direct: property
          carries an ``x-path`` extension annotating its YANG location.
        - ``(ApiEndpoint)-[:CONFIGURES_YANG]->(YangPath)`` — derived: rolled
          up from the endpoint's request-body property graph.

        ## Canonical: endpoint → YANG paths it touches

        ```cypher
        MATCH (e:ApiEndpoint {method: $m, path: $p})
              -[:CONFIGURES_YANG]->(y:YangPath)
        RETURN y.yangPath, y.module ORDER BY y.yangPath
        ```

        ## Canonical: YANG path → endpoints (reverse)

        ```cypher
        MATCH (e:ApiEndpoint)-[:CONFIGURES_YANG]->(:YangPath {yangPath: $yp})
        RETURN DISTINCT e.method, e.path ORDER BY e.path
        ```

        ## Canonical: YANG path → properties that surface it

        ```cypher
        MATCH (p:Property)-[:PROPERTY_AT_YANG]->(:YangPath {yangPath: $yp})
        MATCH (c:SchemaComponent {component_id: p.parent_component_id})
        RETURN c.name AS schema, p.name, p.type, p.required
        ORDER BY c.name, p.name
        ```

        ## Canonical: all YANG paths under a module

        ```cypher
        MATCH (y:YangPath {module: $mod}) RETURN y.yangPath ORDER BY y.yangPath
        ```

        Module name appears as the prefix in the YANG path (e.g.
        ``ac-ntp`` for ``/ac-ntp:ntp/...``). Use it to scope queries to a
        single feature area.

        Caps as in ``query_graph``.

        Args:
            cypher: Read-only Cypher.
            parameters: JSON-encoded parameter dict (default ``"{}"``).
        """
        return _run_query(cypher, parameters, "query_yang")

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True, openWorldHint=False),
    )
    def get_raw_schema(component_id: str) -> str:
        """Fetch the raw OpenAPI ``bodyJson`` for one ``SchemaComponent`` by id.

        Escape hatch for the per-cell truncation envelope returned by
        ``query_graph`` when ``RETURN c.bodyJson`` would be too large. Prefer
        walking ``(:SchemaComponent)-[:COMPOSED_OF*0..5]->()-[:HAS_PROPERTY]->(:Property)``
        for structural exploration; reach for this tool only when you need
        the literal JSON (e.g. to read a vendor extension not surfaced as a
        graph property).

        ## Component ID format

        ``component_id`` is the canonical primary key
        ``<provider>:<section>:<Name>``, e.g.::

            central:schemas:VlanInterface
            central:parameters:TenantId
            glp:schemas:Device

        Inline-promoted branches (oneOf/anyOf/allOf items, ``additionalProperties``
        value shapes, array items) carry a ``#`` suffix and live in
        ``section='inline'``::

            central:schemas:NtpprofileSchema#allOf:2
            central:schemas:VlanInterface#prop:vlan_id[items]

        Look up the exact id first if you only know the bare name::

            MATCH (c:SchemaComponent) WHERE c.name = $name
            RETURN c.component_id, c.section LIMIT 5

        Args:
            component_id: The ``SchemaComponent.component_id`` primary key.
                Must be the EXACT id — this tool does not suffix-match.

        Returns:
            JSON object ``{"component_id": "...", "name": "...", "section": "...",
            "bodyShape": "...", "bodyJson": "..."}``. If the body exceeds
            ~200 KB, returns ``{"error": "...", "size_bytes": N, "hint":
            "walk COMPOSED_OF/HAS_PROPERTY instead"}``.
        """
        if not component_id or not component_id.strip():
            raise ToolError("component_id cannot be empty.")

        try:
            rows = graph.query(
                "MATCH (c:SchemaComponent {component_id: $cid}) "
                "RETURN c.component_id AS component_id, c.name AS name, "
                "       c.section AS section, c.bodyShape AS bodyShape, "
                "       c.bodyJson AS bodyJson",
                params={"cid": component_id},
                read_only=True,
            )
        except Exception as exc:
            raise ToolError(f"Lookup failed: {exc}") from exc

        if not rows:
            raise ToolError(
                f"No SchemaComponent with component_id={component_id!r}. "
                "IDs are EXACT — the canonical shape is "
                "'<provider>:<section>:<Name>' (e.g. 'central:schemas:VlanInterface'). "
                "If you only know the bare name, look it up first: "
                "MATCH (c:SchemaComponent) WHERE c.name = $name "
                "RETURN c.component_id, c.section LIMIT 5"
            )

        row = rows[0]
        body = row.get("bodyJson") or ""
        max_blob = _env_int("MCP_GRAPH_RAW_SCHEMA_MAX_BYTES", 200_000)
        if len(body) > max_blob:
            return json.dumps(
                {
                    "error": "bodyJson exceeds raw-schema cap",
                    "component_id": row.get("component_id"),
                    "name": row.get("name"),
                    "size_bytes": len(body),
                    "cap_bytes": max_blob,
                    "hint": (
                        "Walk the property graph instead: "
                        "MATCH (root:SchemaComponent {component_id: $cid})"
                        "-[:COMPOSED_OF*0..5]->(c)-[:HAS_PROPERTY]->(p:Property) "
                        "RETURN c.name, p.name, p.type, p.required"
                    ),
                },
                indent=2,
            )

        return json.dumps(row, indent=2, default=str)

    @mcp.tool(
        annotations=ToolAnnotations(readOnlyHint=False, idempotentHint=False, openWorldHint=False),
    )
    def write_graph(cypher: str, parameters: str = "{}") -> str:
        """Execute a write Cypher statement to enrich the graph with new nodes, relationships, or properties.

        Use this to add discovered information back to the graph — for example,
        creating relationships between devices and sites after investigating
        API responses, or annotating nodes with computed properties.

        Allowed operations: CREATE, MERGE, SET, DELETE, REMOVE.
        Schema-altering statements (DROP, ALTER, CREATE NODE TABLE) are blocked.

        Args:
            cypher: A write Cypher statement.
                    Example: MATCH (d:Device {serial: 'SN001'}) SET d.customLabel = 'core-switch'
            parameters: JSON-encoded parameter dict for parameterized queries (default: "{}").
                    Example: {"serial": "SN001", "label": "core-switch"}

        Returns:
            JSON object with status and the number of rows affected.
        """
        if not cypher or not cypher.strip():
            raise ToolError("Cypher statement cannot be empty. Read graph://schema for the schema.")

        # Block schema-altering DDL
        _DDL_PATTERN = re.compile(
            r"\b(DROP|ALTER|CREATE\s+(NODE|REL)\s+TABLE|CREATE\s+INDEX|CREATE\s+CONSTRAINT)\b",
            re.IGNORECASE,
        )
        if _DDL_PATTERN.search(cypher):
            raise ToolError(
                "Schema-altering statements (DROP, ALTER, CREATE NODE/REL TABLE) are not allowed. "
                "Only data manipulation (CREATE, MERGE, SET, DELETE, REMOVE) is permitted."
            )

        # Block other side-effecting or privileged operations
        _DENY_PATTERN = re.compile(
            r"\b(LOAD|COPY|INSTALL|CALL|CREATE\s+DATABASE|DROP\s+DATABASE)\b",
            re.IGNORECASE,
        )
        if _DENY_PATTERN.search(cypher):
            raise ToolError(
                "Only data manipulation statements using CREATE, MERGE, SET, DELETE, or REMOVE are "
                "allowed. Statements using LOAD, COPY, INSTALL, CALL, or database-level DDL are "
                "not permitted in this tool."
            )

        # Require at least one allowed write keyword
        cypher_lower = cypher.lower()
        _ALLOWED_WRITE_KEYWORDS = ("create", "merge", "set", "delete", "remove")
        if not any(re.search(rf"\b{kw}\b", cypher_lower) for kw in _ALLOWED_WRITE_KEYWORDS):
            raise ToolError(
                "write_graph only permits data-manipulation statements that use CREATE, MERGE, SET, "
                "DELETE, or REMOVE (optionally after MATCH/WITH/WHERE). Other operations are not "
                "allowed."
            )

        try:
            params = json.loads(parameters)
        except json.JSONDecodeError as exc:
            raise ToolError(f"Invalid JSON in parameters: {exc}. Example: {{\"serial\": \"SN001\", \"label\": \"core-switch\"}}")

        try:
            rows = graph.execute(cypher, params=params)
        except ValueError as exc:
            raise ToolError(str(exc))
        except Exception as exc:
            msg = str(exc)
            hint = _build_error_hint(msg)
            raise ToolError(f"Cypher write failed: {msg}{hint}")

        logger.info("write_graph_done", rows_returned=len(rows))
        result: dict = {"status": "ok"}
        if rows:
            result["rows"] = rows
        return json.dumps(result, indent=2)
