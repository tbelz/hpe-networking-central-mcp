# ADR-005: Unified Search with FTS/BM25

## Status
Superseded by [ADR 009](009-graph-as-primary-api-discovery.md) (2026-04-26).

The `unified_search` MCP tool, the `tools/search.py` helpers, and the
`embedding FLOAT[384]` columns on `ApiEndpoint` and `DocSection` were
removed in the ADR 009 implementation. Agents now use `query_graph`
(Cypher, optionally accelerated by the FTS indexes that survive on
text-bearing nodes) for keyword discovery, and
`describe_endpoint_for_device` for endpoint-scoped property lookup.

The FTS extension load and the per-node FTS index creation
(`create_fts_indexes`) are retained — they back ad-hoc Cypher queries
and graceful degradation, but no MCP tool is exposed on top of them.

## Context
API discovery currently uses Cypher `CONTAINS` (substring matching) with no
relevance ranking, stemming, or tokenisation.  Each entity type is searched
separately.  There is no cross-type search.

LadybugDB provides a native FTS extension (BM25-based) that can be loaded at
runtime:  `INSTALL fts; LOAD EXTENSION fts;`

## Decision

### FTS/BM25 as the primary ranking strategy
Create FTS indexes on:
- `ApiEndpoint` (path, summary, description, operationId)
- `DocSection`  (title, content)
- `Device`      (name, serial, model, deviceType)
- `Site`        (name, address, city, country)
- `ConfigProfile` (name, category)
- `Script`      (filename, description)

At query time:  `CALL fts.query_fts('idx_name', $q, $k)`.

### Graceful degradation
If the FTS extension is unavailable (older LadybugDB build), fall back to
the existing `CONTAINS` substring search.  A boolean flag on GraphManager
tracks whether FTS is available.

### Unified search tool
A new `unified_search(query, scope, limit)` MCP tool queries one or more FTS
indexes depending on `scope` ("all" | "api" | "docs" | "data") and merges
results into a ranked list.

### Embedding columns reserved
`ApiEndpoint` and `DocSection` gain an `embedding FLOAT[384]` column
(nullable / default empty).  This is a placeholder for a later dense/hybrid
search phase; no embeddings are computed in this iteration.

### Existing tool preserved
`search_api_catalog` is kept for backward compatibility but refactored
internally to use FTS when available.

## Consequences
- Relevance-ranked search out of the box (BM25 > substring).
- Single entry-point (`unified_search`) for cross-type discovery.
- FTS extension becomes a soft runtime dependency; degradation is logged.
- Dense vector search can be layered on later without schema changes.
