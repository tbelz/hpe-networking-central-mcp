# ADR-005: Unified Search with FTS/BM25

## Status
Accepted

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
