# Workflow

## Pipeline Lifecycle

```text
Source
  -> raw document and ingestion run
  -> observation and field provenance
  -> versioned event candidate
  -> resolution decision
  -> canonical event and version history
  -> later search or graph projection
```

### 1. Ingest

```powershell
python -m graph_etl ingest-rss --source-key <key> --name <name> --url <https-url> --database <path>
```

Guarantees: raw payload retention, bounded fetch, retry outcomes, idempotent observations, and document membership.

### 2. Extract

```powershell
python -m graph_etl extract --database <path> [--source-key <key>]
python -m graph_etl candidates --database <path>
python -m graph_etl extraction-outcomes --database <path>
```

Guarantees: immutable extraction versions, candidate slot identity independent of ordinal, explicit classification, and per-observation failure isolation.

### 3. Resolve

```powershell
python -m graph_etl resolve --database <path> [--source-key <key>]
python -m graph_etl canonical-events --database <path>
python -m graph_etl resolutions --database <path>
```

Guarantees: strong identity evidence, conservative review states, append-only decisions, and no silent canonical overwrite.

### 4. Review

```powershell
python -m graph_etl review-candidate --database <path> --candidate-id <id> --action reject --reason <text>
python -m graph_etl review-candidate --database <path> --candidate-id <id> --action split --reason <text>
python -m graph_etl review-candidate --database <path> --candidate-id <id> --action restore --canonical-event-id <id> --reason <text>
```

Prior decisions and canonical versions remain auditable.

### 5. Evaluate

```powershell
python -m graph_etl evaluate --labels <path>
```

Evaluation calls the same resolver used in production, so metrics reflect real matching behavior. False-merge rate is the primary safety metric. Expand labels before enabling riskier matching rules.

## Milestone Loop

1. Read `AGENTS.md`, `PROJECT_STATE.md`, and this file.
2. Inspect the current schema, tests, and Git state.
3. Define one small milestone and its acceptance tests.
4. Implement the milestone without unrelated refactors.
5. Run targeted tests, then full validation.
6. Inspect `git diff --check`, secrets, generated files, and migration behavior.
7. Request approval before committing or pushing.
8. Replace `PROJECT_STATE.md` with the new current state after the implementation commit.

## Validation Commands

```powershell
python -m unittest discover -s tests -v
python -m compileall -q graph_etl tests
python -m graph_etl --help
python -m graph_etl evaluate --labels tests/fixtures/resolution_labels.json
git diff --check
```

Add a standard isolated wheel build when packaging behavior changes. Do not introduce lint or type-check tooling without an explicit requirement.