# Graph ETL

Graph ETL is a local, source-agnostic pipeline for developer and technology event intelligence. It ingests public feeds, preserves raw observations and provenance, creates versioned event candidates, and resolves them into conservative canonical events with auditable history.

## Current Capabilities

- RSS, RDF, and Atom ingestion with bounded HTTP fetching and retries
- Byte-preserving raw documents, document membership, and field provenance
- Deterministic candidate extraction with raw and normalized values
- Explicit event classification and per-observation extraction outcomes
- Conservative canonical resolution using source identity, normalized URLs, and title/date evidence
- Immutable resolution decisions, canonical versions, and reversible review actions
- Labeled pair evaluation that scores the production resolver directly
- SQLite persistence and a JSON-oriented CLI

No external event API adapter, search index, scheduler, graph database, LLM, or embedding pipeline is implemented yet.

## Requirements

- Python 3.11 or newer
- No third-party runtime dependencies

## Quick Start

```powershell
python -m unittest discover -s tests -v
python -m graph_etl --help
```

Ingest a public feed into a local database:

```powershell
python -m graph_etl ingest-rss `
  --source-key rss:example `
  --name "Example Events" `
  --url "https://example.com/events.xml" `
  --database graph_etl.sqlite3
```

Run and inspect the pipeline:

```powershell
python -m graph_etl extract --database graph_etl.sqlite3
python -m graph_etl candidates --database graph_etl.sqlite3
python -m graph_etl resolve --database graph_etl.sqlite3
python -m graph_etl canonical-events --database graph_etl.sqlite3
python -m graph_etl resolutions --database graph_etl.sqlite3
python -m graph_etl extraction-outcomes --database graph_etl.sqlite3
```

Evaluate conservative matching against labeled pairs:

```powershell
python -m graph_etl evaluate --labels tests/fixtures/resolution_labels.json
```

Review a candidate without deleting prior history:

```powershell
python -m graph_etl review-candidate `
  --database graph_etl.sqlite3 `
  --candidate-id 1 `
  --action reject `
  --reason "Not an event"
```

## Project Documents

- `AGENTS.md` — agent rules and validation requirements
- `PROJECT_STATE.md` — compact current implementation state
- `WORKFLOW.md` — lifecycle and milestone workflow

## Validation

```powershell
python -m unittest discover -s tests -v
python -m compileall -q graph_etl tests
python -m graph_etl --help
git diff --check
```

Do not commit generated SQLite databases, caches, credentials, or API keys.