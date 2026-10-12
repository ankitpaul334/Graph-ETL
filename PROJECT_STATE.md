# Project State

_Last updated: 2026-10-12_

## Current Stage

Repository simplification is implemented in the working tree. Candidate evaluation now runs through the production resolver, identity normalization has a single shared implementation, persistence is split by pipeline stage, migrations are isolated, and the redundant `observation_fields.value_json` column is removed by migration 5.

## Implemented Pipeline

1. **Ingestion** — bounded RSS/RDF/Atom fetch, raw payload retention, retry outcomes, source URI audit, and document membership.
2. **Observations** — immutable per-entry observations with raw field values and locators.
3. **Extraction** — versioned candidates, deterministic slot identity, classification, raw/normalized values, and per-observation outcomes.
4. **Resolution** — source-ID and normalized-URL evidence before title/date matching; unresolved and review states remain valid data.
5. **Canonical events** — stable IDs, immutable versions, membership links, and append-only resolution decisions.

## Structure

```text
graph_etl/
  identity.py        shared title/URL/source identity normalization
  domain.py          Source and SourceKind only
  cli.py             command wiring and JSON output
  ingestion/         feed parsing, HTTP client, persistence
  extraction/        dates, candidate extraction, persistence
  resolution/        resolver, decision models, persistence
  evaluation/        labeled pair metrics over the real resolver
  persistence/
    clock.py         timestamp helpers
    migrations.py    schema versions 1-5 and migration runner
    database.py      connection lifecycle and store composition
    ingestion_store.py / extraction_store.py / resolution_store.py
tests/
  helpers.py         shared stub client and loopback feed server
```

## Current Schema

SQLite schema version: **5**.

The schema preserves:

- raw documents and source provenance
- observation and document membership
- versioned candidates and fields
- per-observation extraction outcomes
- resolution runs and decision sequences
- canonical events, versions, and candidate membership
- review, split, and restore audit history

Migration 5 drops `observation_fields.value_json`, which duplicated `raw_value` and was never read.

## Verification

Latest executed result: **64 tests passed**.

```powershell
python -m unittest discover -s tests -v
python -m compileall -q graph_etl tests
python -m graph_etl --help
python -m graph_etl evaluate --labels tests/fixtures/resolution_labels.json
```

Current labeled-pair result: false-merge rate `0.0`, precision `1.0`, recall `1.0`, uncertain rate `0.375` over eight synthetic pairs.

## Known Boundaries

- Only feed adapters are implemented.
- Venue, cancellation, and explicit classification resolution paths are tested through stored synthetic fields, not a live structured adapter.
- The labeled evaluation set is small and synthetic.
- `source_uri_history` and `document_observations` are currently write-only; they are retained for replay and audit rather than queried.
- Canonical proposals and review actions are operational but not exposed as a separate UI.
- Search, scheduling, graph projection, and production orchestration remain deferred.
- Lint and static type-check tooling are not configured.

## Next Stage

Validate the resolver with one controlled heterogeneous source, expand labeled pairs from real candidates, and keep false merges at zero before adding production search or scheduling.