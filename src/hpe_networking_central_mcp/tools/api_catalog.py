"""API catalog tools — queries ApiEndpoint/ApiCategory nodes in the graph database.

All API endpoint data is populated by the GitHub Actions knowledge DB builder.
At runtime, the MCP server reads from the graph; no scraping occurs.
The unified_search tool also delegates to FTS helpers in search.py for
non-API scopes (docs, data).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

import structlog
from mcp.server.fastmcp import FastMCP
from mcp.types import ToolAnnotations

from ..config import Settings
from .api_call_policy import (
    get_tracker,
    register_endpoints as _register_endpoint_templates,
    set_skeleton_fetcher,
)
from .search import fts_search, contains_search

if TYPE_CHECKING:
    from ..graph.manager import GraphManager

logger = structlog.get_logger("tools.api_catalog")

_graph_manager: GraphManager | None = None


def register_catalog_tools(mcp: FastMCP, settings: Settings, graph_manager: GraphManager):
    """Register API discovery tools with the MCP server."""
    global _graph_manager
    _graph_manager = graph_manager

    # ── Wire the api_call_policy gate to the catalog graph. ──────────
    # The gate needs two things from the catalog:
    #   1. The full set of endpoint templates so it can resolve a
    #      concrete path (e.g. .../DL0006948/...) back to its catalog
    #      template (e.g. .../{serial-number}/...) before checking
    #      whether the agent inspected it.
    #   2. A way to fetch a single endpoint's skeleton on demand so the
    #      gate can inline it into a block response (one round-trip
    #      instead of two).
    if graph_manager is not None and graph_manager.is_available:
        try:
            rows = graph_manager.query(
                "MATCH (e:ApiEndpoint) RETURN e.method, e.path",
                read_only=True,
            )
            method_to_paths: dict[str, list[str]] = {}
            for r in rows:
                m = r.get("e.method", "")
                p = r.get("e.path", "")
                if m and p:
                    method_to_paths.setdefault(m, []).append(p)
            _register_endpoint_templates(method_to_paths)
        except Exception as exc:  # noqa: BLE001 — degrade gracefully
            logger.warning("endpoint_registry_population_failed", error=str(exc))

    def _fetch_skeleton(method: str, template_path: str) -> str | None:
        """Look up a single endpoint's skeleton blob for the gate to inline."""
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return None
        try:
            rows = gm.query(
                "MATCH (e:ApiEndpoint {endpoint_id: $eid})-[:HAS_SKELETON]->"
                "(s:ApiEndpointSkeleton) RETURN s.bodySkeletonJson",
                {"eid": f"{method.upper()}:{template_path}"},
                read_only=True,
            )
        except Exception:  # noqa: BLE001
            return None
        if not rows:
            return None
        blob = rows[0].get("s.bodySkeletonJson") or ""
        if not blob:
            return None
        # Re-emit as compact-but-readable JSON. If parsing fails, fall back
        # to the raw blob — better to show *something* than nothing.
        try:
            return json.dumps(json.loads(blob), indent=2)
        except (json.JSONDecodeError, TypeError):
            return blob

    set_skeleton_fetcher(_fetch_skeleton)

    # NOTE: ``unified_search`` is intentionally NOT registered as an MCP tool.
    # The implementation below is retained in-source purely so the tool can
    # be re-enabled by restoring the @mcp.tool decorator once the underlying
    # VSG / docs corpus is populated — it is defined inside this closure and
    # is not exposed as an importable symbol. Until then, agents should
    # navigate via the graph (``query_graph``) and the API endpoint catalog
    # (``api://endpoint-catalog`` / ``get_api_endpoint_detail``).
    def unified_search(
        query: str,
        scope: str = "data",
        limit: int = 20,
    ) -> str:
        """Full-text search the documentation or graph data by keyword.

        Use this to find sections of the VSG documentation, or to look up
        devices / sites / config profiles / scripts in the graph by name.
        For API endpoints, read the ``api://endpoint-catalog`` resource (or
        scan the API Endpoint Catalog in the system instructions) — it lists
        every reachable ``METHOD /path``.

        Args:
            query: Search term (e.g., "vlan", "switch", "Site-NYC").
            scope: "data" (default), "docs", or "all".
            limit: Maximum results to return (default 20, max 50).

        Returns:
            JSON with matching results.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({
                "error": "Search not available — graph database not initialized.",
                "hint": "The graph database may still be loading. Try again shortly.",
            })

        if not query or not query.strip():
            return json.dumps({
                "error": "Query must not be empty.",
                "hint": "Provide a search term, e.g. unified_search(query='vlan').",
            })

        valid_scopes = {"docs", "data", "all"}
        if scope not in valid_scopes:
            return json.dumps({
                "error": (
                    f"Invalid scope '{scope}'. Must be one of: "
                    f"{', '.join(sorted(valid_scopes))}. "
                    "For API endpoints, read the api://endpoint-catalog resource "
                    "and call get_api_endpoint_detail()."
                ),
            })

        limit = max(1, min(limit, 50))

        if gm.fts_available:
            results = fts_search(gm, query, scope=scope, limit=limit)
            search_method = "fts"
            if not results:
                results = contains_search(gm, query, scope=scope, limit=limit)
                search_method = "contains_fallback"
        else:
            results = contains_search(gm, query, scope=scope, limit=limit)
            search_method = "contains"
        return json.dumps({
            "query": query,
            "scope": scope,
            "search_method": search_method,
            "total": len(results),
            "results": results,
        }, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def list_api() -> str:
        """Return the full API Endpoint Catalog as a nested path tree.

        **Fallback for agents that cannot see the API Endpoint Catalog
        embedded in the server instructions or via the
        ``api://endpoint-catalog`` / ``docs://endpoint-catalog`` resource.**
        Most clients receive the catalog automatically through one of those
        channels and do not need to call this tool.

        The returned text is grouped by API category with nested path
        indentation. A trailing ``!`` after the method list marks
        deprecated endpoints. When ``READ_ONLY`` is enabled, mutating
        methods (POST/PUT/PATCH/DELETE) are filtered out.

        Returns:
            Plain-text catalog rendered by ``render_path_tree``.
        """
        from ..api_tree import render_path_tree

        gm = _graph_manager
        if gm is None or not gm.is_available:
            return (
                "API endpoint catalog is currently unavailable — "
                "graph database not initialized. Try again shortly."
            )
        try:
            rows = gm.query(
                "MATCH (e:ApiEndpoint) "
                "RETURN e.method AS method, e.path AS path, "
                "e.category AS category, e.deprecated AS deprecated",
                read_only=True,
            )
        except Exception:
            logger.exception("api_endpoint_catalog_query_failed")
            return (
                "API endpoint catalog is currently unavailable. "
                "Try again shortly; see server logs for details."
            )
        return render_path_tree(rows, read_only=settings.read_only)

    # ── Shared resolver for the (method, path) / endpoints argument shape ──

    def _normalise_path(p: str) -> str:
        """Ensure path starts with '/' to match DB storage from OpenAPI spec paths."""
        p = p.strip()
        return p if p.startswith("/") else f"/{p}"

    def _resolve_request(
        method: str | None,
        path: str | None,
        endpoints: list[dict] | None,
    ) -> tuple[list[tuple[str, str]] | None, str | None, bool]:
        """Return (requested_pairs, error_json, is_bulk) for a tool call."""
        if endpoints is not None:
            if not isinstance(endpoints, list) or not endpoints:
                return None, json.dumps({
                    "error": "`endpoints` must be a non-empty list of {method, path} objects.",
                }), True
            requested: list[tuple[str, str]] = []
            for item in endpoints:
                if not isinstance(item, dict) or "method" not in item or "path" not in item:
                    return None, json.dumps({
                        "error": "Each entry in `endpoints` must be an object with 'method' and 'path' keys.",
                    }), True
                requested.append((str(item["method"]).upper(), _normalise_path(str(item["path"]))))
            return requested, None, True
        if not method or not path:
            return None, json.dumps({
                "error": "Provide either (method, path) or `endpoints=[...]`.",
            }), False
        return [(method.upper(), _normalise_path(path))], None, False

    def _filter_read_only(
        requested: list[tuple[str, str]],
    ) -> tuple[list[tuple[str, str]], list[dict]]:
        if not settings.read_only:
            return requested, []
        allowed: list[tuple[str, str]] = []
        skipped: list[dict] = []
        for m, p in requested:
            if m == "GET":
                allowed.append((m, p))
            else:
                skipped.append({"method": m, "path": p})
        return allowed, skipped

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_api_endpoint_detail(
        method: str | None = None,
        path: str | None = None,
        endpoints: list[dict] | None = None,
        parts: list[str] | None = None,
    ) -> str:
        """Get the structural skeleton of one or more API endpoints.

        Returns parameters, request body schema, success and first-error
        response shapes, and a ``$components_index`` listing every
        transitively-referenced component by name with minimal hints
        (``type``, ``enum``, ``required``, ``child_refs``, presence
        flags for ``oneOf`` / ``anyOf`` / ``allOf``).  All
        human-readable prose (descriptions, titles, examples) is
        stripped at every nested level.  The operation-level
        ``summary`` is intentionally preserved as a one-line label so an
        agent can recognise the endpoint without a second tool call.

        Component bodies themselves are NOT inlined — the index is the
        contract.  Once the agent knows it needs the full body of a
        specific schema (e.g. ``Vlan`` or ``ApRadioProfile``) it calls
        ``get_schema_component(method, path, name)`` to fetch just that
        one component.  This keeps detail calls small even for endpoints
        whose transitively expanded components used to dominate >98 %
        of payload bytes (port profiles, ethernet interfaces, …).

        For ambiguous field names — and for parameter semantics the
        skeleton omits because they are documented only in prose (e.g.
        OData filter syntax, the meaning of enum values, or constraints
        described in text rather than encoded as ``format`` / ``pattern``
        / numeric bounds) — follow up with
        ``get_api_endpoint_glossary(method, path)`` to fetch the
        descriptions on demand.  Most workflows do not need it.

        Two call forms are supported:

        1. **Single**: pass ``method`` and ``path``.
           Returns one JSON object with the endpoint skeleton.
        2. **Bulk**: pass ``endpoints`` as a list of
           ``{"method": "GET", "path": "/foo"}`` objects.
           Returns ``{"endpoints": [...], "missing": [...]}``.

        DELETE / POST / PUT / PATCH endpoints are catalogued and callable
        via ``call_central_api`` when ``READ_ONLY=false``.  In
        ``READ_ONLY=true`` mode, non-GET endpoints are skipped (single form
        returns an error; bulk form lists them under ``skipped_read_only``).

        Args:
            method: HTTP method (GET, POST, PUT, PATCH, DELETE). Single form.
            path: Full API path (e.g. "/monitoring/v2/aps"). Single form.
            endpoints: List of {"method", "path"} dicts. Bulk form.
            parts: Optional projection filter — restrict the returned
                payload to a subset of top-level keys.  Valid values:
                ``"meta"`` (operation-level fields like ``method``,
                ``path``, ``summary``, ``category``), ``"parameters"``,
                ``"request_body"``, ``"required_paths"``, ``"responses"``,
                ``"$components_index"``.  Unknown names are ignored.

        Returns:
            JSON with endpoint skeleton(s).
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available.",
                               "hint": "The graph database may still be loading. Try again shortly."})

        requested, err, is_bulk = _resolve_request(method, path, endpoints)
        if err is not None:
            return err
        assert requested is not None

        requested, skipped_read_only = _filter_read_only(requested)
        if not requested:
            if is_bulk:
                return json.dumps({
                    "endpoints": [],
                    "missing": [],
                    "skipped_read_only": skipped_read_only,
                }, indent=2)
            return json.dumps({
                "error": (
                    f"Endpoint {skipped_read_only[0]['method']} "
                    f"{skipped_read_only[0]['path']} is hidden because the server "
                    "is in READ_ONLY mode. Only GET endpoints are exposed."
                ),
            })

        eids = [f"{m}:{p}" for m, p in requested]
        rows = gm.query(
            "MATCH (e:ApiEndpoint)-[:HAS_SKELETON]->(s:ApiEndpointSkeleton) "
            "WHERE e.endpoint_id IN $eids "
            "RETURN e.method, e.path, e.category, s.bodySkeletonJson",
            {"eids": eids},
            read_only=True,
        )

        details_by_eid: dict[str, dict] = {}
        # Track every eid the DB returned a row for (even if the blob was
        # broken) so we can distinguish "endpoint unknown" from "blob corrupt".
        found_eids: set[str] = set()
        for r in rows:
            method_val = r.get("e.method", "")
            path_val = r.get("e.path", "")
            eid_key = f"{method_val}:{path_val}"
            found_eids.add(eid_key)
            blob = r.get("s.bodySkeletonJson") or ""
            if not blob:
                logger.warning(
                    "endpoint_skeleton_blob_missing",
                    method=method_val,
                    path=path_val,
                )
                continue
            try:
                d = json.loads(blob)
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "endpoint_skeleton_blob_invalid",
                    method=method_val,
                    path=path_val,
                    error=str(exc),
                )
                continue
            d.setdefault("category", r.get("e.category", ""))
            details_by_eid[eid_key] = d

        # Apply optional ``parts=`` projection filter.  ``meta`` keeps the
        # operation-level fields (method, path, summary, category, etc.);
        # other names map to top-level keys produced by ``project_skeleton``.
        if parts:
            _META_KEYS = {"method", "path", "summary", "category", "operation_id", "deprecated", "tags"}
            _SECTION_KEYS = {"parameters", "request_body", "required_paths", "responses", "$components_index"}
            _KNOWN_PARTS = {"meta"} | _SECTION_KEYS
            requested_parts = {str(p) for p in parts}
            known_requested_parts = requested_parts & _KNOWN_PARTS
            if known_requested_parts:
                keep_meta = "meta" in known_requested_parts
                keep_sections = known_requested_parts & _SECTION_KEYS
                for eid_key, d in list(details_by_eid.items()):
                    filtered: dict[str, Any] = {}
                    for k, v in d.items():
                        if k in keep_sections:
                            filtered[k] = v
                        elif keep_meta and k in _META_KEYS:
                            filtered[k] = v
                    details_by_eid[eid_key] = filtered

        # Record inspection for the policy gate. Any endpoint the DB
        # actually returned a row for counts as inspected — even if the
        # skeleton blob is corrupt, the agent has demonstrably looked it up.
        tracker = get_tracker()
        for eid_key in found_eids:
            method_val, _, path_val = eid_key.partition(":")
            tracker.record(method_val, path_val, "skeleton")

        if not is_bulk:
            eid = eids[0]
            if eid not in details_by_eid:
                if eid in found_eids:
                    return json.dumps({
                        "error": (
                            f"Endpoint {requested[0][0]} {requested[0][1]} exists "
                            "but its skeleton blob is missing or corrupt."
                        ),
                        "hint": "Rebuild the knowledge DB to regenerate the skeleton data.",
                    })
                return json.dumps({
                    "error": f"No endpoint found for {requested[0][0]} {requested[0][1]}.",
                    "hint": (
                        "Read the api://endpoint-catalog resource for the correct method/path. "
                        "Guessing paths without consulting the catalog has a near-zero chance of success."
                    ),
                })
            return json.dumps(details_by_eid[eid], indent=2)

        ordered_details = []
        missing = []
        broken = []
        for m, p in requested:
            eid = f"{m}:{p}"
            if eid in details_by_eid:
                ordered_details.append(details_by_eid[eid])
            elif eid in found_eids:
                broken.append({"method": m, "path": p})
            else:
                missing.append({"method": m, "path": p})

        response: dict[str, Any] = {
            "endpoints": ordered_details,
            "missing": missing,
        }
        if broken:
            response["blob_corrupt"] = broken
            response["hint"] = "Rebuild the knowledge DB to regenerate skeleton data for blob_corrupt entries."
        if skipped_read_only:
            response["skipped_read_only"] = skipped_read_only
        return json.dumps(response, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_api_endpoint_glossary(
        method: str | None = None,
        path: str | None = None,
        endpoints: list[dict] | None = None,
        components: list[str] | None = None,
    ) -> str:
        """Get human-readable descriptions for the parameters and components of one or more endpoints.

        The glossary is the complement of the **nested** structural
        skeleton: every prose key that ``get_api_endpoint_detail`` strips
        from nested parameter and schema content (``description``,
        ``title``, ``example``, ``examples``, ``x-typeName``,
        ``x-typeDescription``, ``x-patternSources``) is surfaced here at
        every nesting level it appears — and nothing else.  Structural
        fields (``type``, ``enum``, ``format``, ``pattern``, ``default``,
        ``required``, ``x-mutually-exclusive``, length / numeric
        constraints) are NOT repeated; the skeleton already carries them.

        Operation-level ``summary`` is the one intentional exception to
        the strip-keys rule: it stays on the top-level skeleton output as
        a one-line endpoint label and is not duplicated into the glossary.

        Returns:
        - **parameters**: per-parameter prose for query/path/header/cookie
          parameters.  Each entry contains an ``in`` scaffold plus
          whichever prose keys the upstream spec attached at the
          parameter or schema level.  This is where Central encodes
          rich semantics like OData filter syntax, allowed values
          listed only in prose, and format constraints.
        - **components**: prose for the schemas reachable from the
          endpoint(s), in the same shape as the schema itself
          (``properties`` / ``items`` / ``allOf`` / … traversed only as
          scaffolding to locate the prose).

        Use this whenever a parameter or field needs semantic context;
        the skeleton alone is sufficient for purely structural mapping.

        Note: the ``components`` filter argument applies to schema
        components only; the ``parameters`` block is always returned in
        full because it is small and frequently needed.

        Two call forms (matching ``get_api_endpoint_detail``):

        1. **Single**: pass ``method`` and ``path``.
        2. **Bulk**: pass ``endpoints`` as a list of
           ``{"method": "GET", "path": "/foo"}`` objects.

        Args:
            method: HTTP method. Single form.
            path: Full API path. Single form.
            endpoints: List of {"method", "path"} dicts. Bulk form.
            components: Optional list of component names to restrict the
                glossary to.  Names not present in the endpoint's reachable
                components are silently ignored.

        Returns:
            JSON with per-component descriptions.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({"error": "Graph database not available.",
                               "hint": "The graph database may still be loading. Try again shortly."})

        requested, err, is_bulk = _resolve_request(method, path, endpoints)
        if err is not None:
            return err
        assert requested is not None

        requested, skipped_read_only = _filter_read_only(requested)
        if not requested:
            if is_bulk:
                return json.dumps({
                    "endpoints": [],
                    "missing": [],
                    "skipped_read_only": skipped_read_only,
                }, indent=2)
            return json.dumps({
                "error": (
                    f"Endpoint {skipped_read_only[0]['method']} "
                    f"{skipped_read_only[0]['path']} is hidden because the server "
                    "is in READ_ONLY mode. Only GET endpoints are exposed."
                ),
            })

        eids = [f"{m}:{p}" for m, p in requested]
        rows = gm.query(
            "MATCH (e:ApiEndpoint)-[:HAS_SKELETON]->(s:ApiEndpointSkeleton) "
            "WHERE e.endpoint_id IN $eids "
            "RETURN e.method, e.path, s.bodyGlossaryJson",
            {"eids": eids},
            read_only=True,
        )

        wanted_components = set(components) if components is not None else None

        details_by_eid: dict[str, dict] = {}
        # Track every eid the DB returned a row for (even if the blob was
        # broken) so we can distinguish "endpoint unknown" from "blob corrupt".
        found_eids: set[str] = set()
        for r in rows:
            method_val = r.get("e.method", "")
            path_val = r.get("e.path", "")
            eid_key = f"{method_val}:{path_val}"
            found_eids.add(eid_key)
            blob = r.get("s.bodyGlossaryJson") or ""
            if not blob:
                logger.warning(
                    "endpoint_glossary_blob_missing",
                    method=method_val,
                    path=path_val,
                )
                continue
            try:
                d = json.loads(blob)
            except (json.JSONDecodeError, TypeError) as exc:
                logger.warning(
                    "endpoint_glossary_blob_invalid",
                    method=method_val,
                    path=path_val,
                    error=str(exc),
                )
                continue
            if wanted_components is not None and isinstance(d.get("components"), dict):
                d["components"] = {
                    name: entry
                    for name, entry in d["components"].items()
                    if name in wanted_components
                }
            details_by_eid[eid_key] = d

        # Record inspection for the policy gate (see get_api_endpoint_detail).
        tracker = get_tracker()
        for eid_key in found_eids:
            method_val, _, path_val = eid_key.partition(":")
            tracker.record(method_val, path_val, "glossary")

        if not is_bulk:
            eid = eids[0]
            if eid not in details_by_eid:
                if eid in found_eids:
                    return json.dumps({
                        "error": (
                            f"Endpoint {requested[0][0]} {requested[0][1]} exists "
                            "but its glossary blob is missing or corrupt."
                        ),
                        "hint": "Rebuild the knowledge DB to regenerate the glossary data.",
                    })
                return json.dumps({
                    "error": f"No endpoint found for {requested[0][0]} {requested[0][1]}.",
                    "hint": (
                        "Read the api://endpoint-catalog resource for the correct method/path. "
                        "Guessing paths without consulting the catalog has a near-zero chance of success."
                    ),
                })
            return json.dumps(details_by_eid[eid], indent=2)

        ordered_details = []
        missing = []
        broken = []
        for m, p in requested:
            eid = f"{m}:{p}"
            if eid in details_by_eid:
                ordered_details.append(details_by_eid[eid])
            elif eid in found_eids:
                broken.append({"method": m, "path": p})
            else:
                missing.append({"method": m, "path": p})

        response: dict[str, Any] = {
            "endpoints": ordered_details,
            "missing": missing,
        }
        if broken:
            response["blob_corrupt"] = broken
            response["hint"] = "Rebuild the knowledge DB to regenerate glossary data for blob_corrupt entries."
        if skipped_read_only:
            response["skipped_read_only"] = skipped_read_only
        return json.dumps(response, indent=2)

    @mcp.tool(annotations=ToolAnnotations(readOnlyHint=True, idempotentHint=True))
    def get_schema_component(
        method: str,
        path: str,
        name: str,
        section: str = "schemas",
    ) -> str:
        """Fetch the full body of one schema component referenced by an endpoint.

        ``get_api_endpoint_detail`` returns a ``$components_index`` that
        names every component reachable from an endpoint along with
        minimal hints (type, enum, required, child_refs).  Use this tool
        to drill into a specific component when the index alone is not
        enough — typically when the index flags ``oneOf`` / ``anyOf`` /
        ``allOf`` (the agent must see the variants), when a nested
        ``items_ref`` points at an array element schema, or when the
        agent needs the property list of an object referenced via
        ``child_refs``.

        Component bodies are stored prose-stripped (descriptions /
        titles / examples removed) so a tight structural fetch stays
        small.  For human-readable descriptions of the same component,
        use ``get_api_endpoint_glossary(method, path, components=[name])``.

        Args:
            method: HTTP method of the owning endpoint.
            path: Full API path of the owning endpoint.
            name: Component name (as it appears in ``$components_index``,
                e.g. ``"Vlan"``, ``"ApRadioProfile"``).
            section: Component section — usually ``"schemas"`` (default);
                ``"responses"`` for shared response components.

        Returns:
            JSON object with the component body, or an error if the
            endpoint or component is unknown.
        """
        gm = _graph_manager
        if gm is None or not gm.is_available:
            return json.dumps({
                "error": "Graph database not available.",
                "hint": "The graph database may still be loading. Try again shortly.",
            })

        norm_path = _normalise_path(path)
        eid = f"{method.upper()}:{norm_path}"

        # Verify the endpoint exists so we can distinguish "endpoint unknown"
        # from "endpoint exists but does not reference this component".
        try:
            ep_rows = gm.query(
                "MATCH (e:ApiEndpoint {endpoint_id: $eid}) RETURN COUNT(e) AS c",
                {"eid": eid},
                read_only=True,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("get_schema_component_endpoint_check_failed", error=str(exc), eid=eid)
            return json.dumps({
                "error": "Component lookup failed.",
                "hint": "The graph database may be misconfigured. Rebuild the knowledge DB.",
            })
        if not ep_rows or (ep_rows[0].get("c") or 0) == 0:
            return json.dumps({
                "error": f"No endpoint found for {method.upper()} {norm_path}.",
                "hint": (
                    "Read the api://endpoint-catalog resource for the correct method/path. "
                    "Guessing paths without consulting the catalog has a near-zero chance of success."
                ),
            })

        # Match the SchemaComponent by section + name. Spec sources rarely
        # collide on a component name, but if they do we just take the
        # first hit — same behaviour as the previous bodyComponentsJson
        # blob, which was also keyed only by section/name.
        try:
            rows = gm.query(
                "MATCH (c:SchemaComponent) "
                "WHERE c.section = $sec AND c.name = $name "
                "RETURN c.bodyJson, c.spec_source LIMIT 1",
                {"sec": section, "name": name},
                read_only=True,
            )
        except Exception as exc:  # noqa: BLE001
            logger.warning("get_schema_component_query_failed", error=str(exc), eid=eid)
            return json.dumps({
                "error": "Component lookup failed.",
                "hint": "The graph database may be missing the SchemaComponent table. Rebuild the knowledge DB.",
            })

        if not rows:
            # Surface what *is* known about this section so the caller can
            # quickly see what is available without re-fetching the
            # endpoint detail.
            avail_rows = gm.query(
                "MATCH (c:SchemaComponent) WHERE c.section = $sec "
                "RETURN c.name ORDER BY c.name",
                {"sec": section},
                read_only=True,
            )
            available = [r.get("c.name", "") for r in avail_rows if r.get("c.name")]
            if available:
                return json.dumps({
                    "error": f"Component '{name}' not found in section '{section}'.",
                    "available": available[:200],
                    "hint": (
                        "Component names come from the $components_index returned by "
                        "get_api_endpoint_detail."
                    ),
                })
            return json.dumps({
                "error": f"Section '{section}' not present in the schema graph.",
                "available_sections": ["schemas", "responses", "parameters", "requestBodies"],
            })

        blob = rows[0].get("c.bodyJson") or ""
        if not blob:
            return json.dumps({
                "error": (
                    f"Component '{name}' in section '{section}' has an empty body."
                ),
                "hint": "Rebuild the knowledge DB to regenerate component data.",
            })

        try:
            body = json.loads(blob)
        except (json.JSONDecodeError, TypeError) as exc:
            logger.warning(
                "get_schema_component_blob_invalid",
                method=method,
                path=norm_path,
                name=name,
                error=str(exc),
            )
            return json.dumps({
                "error": (
                    f"Component '{name}' body is corrupt."
                ),
                "hint": "Rebuild the knowledge DB to regenerate component data.",
            })

        return json.dumps({
            "method": method.upper(),
            "path": norm_path,
            "section": section,
            "name": name,
            "body": body,
        }, indent=2)
