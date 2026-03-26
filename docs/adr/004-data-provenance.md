# ADR-004: Data Provenance in the Graph

## Status
Partially superseded by [ADR-006](006-remove-prebuilt-ontology.md)

## Context
Domain nodes in the graph (Device, Site, Port, etc.) are populated by seed
scripts that call Central APIs.  Neither the seed scripts nor the graph schema
track **which API** produced a particular node, **when** it was fetched, or
**which seed run** created it.

The existing `OPERATES_ON` relationship links `ApiEndpoint → EntityType` but
carries no CRUD operation type, so an LLM cannot distinguish "this endpoint
READS devices" from "this endpoint CREATES devices."

## Decision

### Type-level provenance (build-time)
Add an `operation` property (`read | list | create | update | delete`) to
`OPERATES_ON` edges, derived from the HTTP method at build time (GET → read/list,
POST → create, PUT/PATCH → update, DELETE → delete).

### Instance-level provenance (runtime, seeds)
1. Add `fetched_at` (STRING, ISO-8601) and `source_api` (STRING, endpoint_id in
   the form `METHOD:/path`) columns to every domain node table.
2. Create a new `POPULATED_BY` relationship table
   `(FROM <DomainNode> TO ApiEndpoint)` with `fetched_at`, `seed`, and `run_id`
   properties.
3. Provide a shared `record_provenance()` helper that all seeds call after
   upserting a domain node.

### Latest-only semantics
`POPULATED_BY` edges are replaced on each seed re-run (not accumulated) to keep
the graph compact.  Historical audit trails are out of scope for this ADR.

## Consequences
- Every domain node becomes traceable to its source API.
- LLMs can ask "what API produced this Device?" and get an answer.
- Seeds must call the provenance helper, adding a small amount of code per
  upsert.  The helper is centralised so the pattern is consistent.
- `OPERATES_ON.operation` enables CRUD-aware API discovery ("what APIs can
  create a VLAN?").
