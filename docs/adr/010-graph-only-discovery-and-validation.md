# ADR 010: Graph-only Discovery and Pre-flight Validator

Status: Accepted
Date: 2025-12-12
Supersedes parts of: [ADR 009: Graph as Primary API Discovery](009-graph-as-primary-api-discovery.md)

## Context

ADR 009 introduced the graph as the primary API discovery surface and
specified a session-scoped "inspection gate": before any `call_central_api`
/ `call_greenlake_api` request, the agent had to call
`describe_endpoint_for_device(method, path, deviceType?)` for the same
endpoint. The gate tracked inspected endpoints in a per-session
`InspectionTracker` and rejected calls that had not been inspected.

Three problems surfaced in practice:

1. **Tool surface duplication.** The catalog tools (`list_api`,
   `list_api_categories`, `get_api_endpoint_detail`) overlap heavily with
   the `api://endpoint-catalog` resource and with `query_graph` against
   the schema subgraph (`Parameter`, `RequestBody`, `SchemaComponent`,
   `Property`). Agents that already had the catalog inlined into the
   system instructions ignored the catalog tools entirely.
2. **`describe_endpoint_for_device` was a stateful crutch.** It was used
   primarily to satisfy the inspection gate, not for real discovery —
   `query_graph` was already strictly more powerful (filterable by
   property, joinable, paginatable). The gate added a mandatory extra
   round-trip per endpoint without catching any class of mistake that a
   schema-aware validator could not catch at call-time.
3. **The `endpoint_id` bypass parameter** on `call_central_api` /
   `call_greenlake_api` (which let the caller skip the gate by passing
   a pre-resolved endpoint identifier) leaked an internal seed-time
   detail through the tool surface and was a footgun for agents.

## Decision

1. **Remove `describe_endpoint_for_device`** as a tool. All structured
   endpoint metadata is reachable from `query_graph` against the v8
   schema graph; canned Cypher patterns are documented in `graph://schema`.
2. **Remove the `list_api`, `list_api_categories`, and
   `get_api_endpoint_detail` tools** (`tools/api_catalog.py`). The
   `api://endpoint-catalog` MCP resource and the inlined catalog in the
   system instructions remain.
3. **Remove the inspection gate** (`tools/api_call_policy.py`,
   `InspectionTracker`, `EndpointRegistry`) and the `endpoint_id`
   parameter from `call_central_api` / `call_greenlake_api`. These were
   only meaningful in the presence of the gate.
4. **Replace them with a stateless pre-flight validator**
   (`tools/api_call_validation.py`, `validate_call(...)`) that runs on
   every `call_central_api` / `call_greenlake_api` invocation. The
   validator queries the graph for `Parameter` and `Property` nodes of
   the target endpoint and:
   - **errors** (blocking) — a required `location='query'` parameter is
     missing; on POST only, a required top-level body field is missing;
   - **warnings** (advisory) — a body key (POST/PATCH/PUT) is not
     declared in the schema. PATCH/PUT skip the required-body check
     because partial updates are expected.
5. **Fail open.** If the graph is unavailable (not yet initialized,
   missing, schema mismatch), the validator attaches a warning and
   allows the call. Failing closed would block every API call on graph
   hiccups.
6. **Schema summary on error.** When the validator returns errors, the
   resulting `ToolError` includes a compact JSON summary of the
   endpoint's parameters and body fields plus a pointer to
   `query_graph` / `graph://schema` for deeper inspection.

## Consequences

### Breaking changes

- The `endpoint_id` parameter has been removed from `call_central_api`
  and `call_greenlake_api`. Existing tool callers that passed it will
  receive a parameter-validation error from FastMCP.
- The `describe_endpoint_for_device`, `list_api`,
  `list_api_categories`, and `get_api_endpoint_detail` tools are gone.
  Callers must read `api://endpoint-catalog` (resource) and use
  `query_graph` for structured field-level lookups.

### Architecture

- `tools/api_call.py` keeps a single check per call (the validator)
  instead of two (registry + tracker). The signature of
  `register_api_call_tools` / `register_greenlake_api_call_tools` gained
  an optional `graph_manager` parameter so the validator can be wired in
  at registration time.
- The validator is stateless and idempotent — there is no per-session
  bookkeeping to invalidate, persist, or test in isolation. Its
  behaviour is fully determined by the graph snapshot and the call
  payload.
- Knowledge DB schema v8 (the property graph from ADR 009 Phase 2E) is
  the validator's contract. `endpoint_id` remains an `ApiEndpoint`
  property — only the tool *parameter* was removed.

### Documentation and UX

- `instructions.py`, `resources/docs.py`, `prompts/workflows.py`,
  `api_tree.py`, and `graph/manager.py` no longer reference the
  removed tools. They steer agents to the catalog resource +
  `query_graph` and document the pre-flight validation behaviour.
- The CI smoke check
  (`.github/workflows/build-and-push.yml`) and
  `scripts/smoke_oas_e2e.py` no longer assert the presence of the
  removed tools.

### Testing

- `tests/test_api_call_gate.py` (the gate suite) and
  `tests/test_api_catalog.py` were removed.
- `tests/test_describe_endpoint.py` was removed.
- `tests/test_readonly.py` no longer covers the deleted catalog tools.
- `tests/test_api_call_validation.py` is new and covers the validator's
  required-param / required-body / unknown-key / fail-open / schema-
  summary behaviours against a fake graph.

## Alternatives considered

- **Keep the gate, drop only the catalog tools.** Rejected: the gate's
  primary purpose was to force a structured inspection step, but
  `describe_endpoint_for_device` and `query_graph` cover the same
  ground and the gate added cost without adding safety the validator
  cannot provide at call time.
- **Move the validator into `_http_core`.** Rejected: the HTTP client
  is shared with scripts (which already opt out of the gate per ADR
  009) and should remain transport-only. Validation lives at the tool
  boundary.
- **Fail closed on graph unavailability.** Rejected: it would make
  startup races and operator misconfiguration look like API errors. A
  warning is enough to make the degraded state visible.
