# Agent Instructions

## Startup

1. Read `README.md`, `PROJECT_STATE.md`, `WORKFLOW.md`, and this file.
2. Inspect `git status`, the relevant source modules, schema migrations, and tests.
3. Run the narrowest relevant tests before making changes.
4. Preserve uncommitted work and do not implement unrelated product scope.

## Architecture Principles

- Use Python 3.11+ and SQLite as the relational source of truth.
- Keep ingestion, extraction, resolution, evaluation, and persistence modules separate.
- Keep identity normalization in `graph_etl/identity.py`; do not reimplement it per stage.
- Keep evaluation calling the production resolver so metrics reflect real behavior.
- Keep migrations in `graph_etl/persistence/migrations.py` and stage writes in the matching store module.
- Preserve raw payloads, source values, field locators, processing versions, and run outcomes for replay.
- Keep publication timestamps separate from event dates.
- Make extraction, normalization, and resolver versions immutable; upgrades create new outputs.
- Classify event candidates separately from date completeness and record per-observation extraction outcomes.
- Never infer cancellation or event status from missing data.
- Never silently overwrite canonical values with missing or conflicting data.
- Keep recurring instances distinct and prefer `REVIEW_REQUIRED` over uncertain merges.
- Keep graph projection, external adapters, LLMs, embeddings, scheduling, and distributed infrastructure deferred unless explicitly required.
- Use deterministic identities and content hashes for idempotency.

## Change Rules

- Make the smallest cohesive change that satisfies the current milestone.
- Use migrations for schema changes; never edit an applied migration.
- Preserve raw candidates and prior resolution decisions.
- Add tests for every behavioral change.
- Do not commit generated databases, caches, credentials, or API keys.
- Do not commit, push, or publish without explicit user approval.

## Validation Commands

```powershell
python -m unittest discover -s tests -v
python -m compileall -q graph_etl tests
python -m graph_etl --help
python -m graph_etl evaluate --labels tests/fixtures/resolution_labels.json
git diff --check
```

Run an isolated wheel build when packaging changes. No lint or static type-check command is configured; do not add tooling speculatively.

## Completion Report

Report exact commands and results, files changed, migration behavior, remaining risks, and checks not run. Replace `PROJECT_STATE.md` after each approved implementation commit rather than appending history.