# Project Instructions

## Purpose

Build a source-agnostic developer and technology event intelligence pipeline. Ingest heterogeneous public information, preserve raw observations and provenance, extract and normalize event candidates, resolve duplicates conservatively, and maintain canonical event history for later search and graph projections.

## Architecture

- Use Python 3.11+ and SQLite as the initial relational source of truth.
- Keep `graph_etl/domain`, `graph_etl/ingestion`, `graph_etl/persistence`, and future extraction, resolution, and orchestration modules separate.
- Build around `Source`, `Observation`, `EventCandidate`, `CanonicalEvent`, and `EventVersion` contracts.
- Keep source adapters independent of persistence and orchestration.
- Preserve raw payloads, source values, field locators, run outcomes, and processing versions for replay.
- Keep any future graph projection replaceable; do not add a graph database without demonstrated graph-query requirements.
- Do not add a frontend, cloud deployment, microservices, or scheduling infrastructure unless required by demonstrated workload.

## Conventions

- Use `snake_case` for modules, functions, and variables; use `PascalCase` for classes and `UPPER_SNAKE_CASE` for constants.
- Prefer the Python standard library and small cohesive changes; add dependencies only when a concrete requirement justifies them.
- Use UTC RFC 3339 timestamps and retain source timezone text separately.
- Use deterministic identifiers and content hashes for idempotency.
- Never silently overwrite known canonical values with missing or conflicting source data.
- Keep recurring event instances distinct and leave ambiguous entity matches unresolved.
- Validate untrusted input, bound response sizes, use least privilege, and keep secrets out of source, logs, and chat.

## Commands

- Test: `python -m unittest discover -s tests -v`
- Syntax check: `python -m compileall -q graph_etl tests`
- CLI help: `python -m graph_etl --help`
- Run one feed: `python -m graph_etl ingest-rss --source-key <key> --name <name> --url <https-url> --database <path>`
- No lint or static type-check command is configured yet; do not add tooling speculatively.

## Constraints

- Initial adapters are RSS/Atom, one permitted structured event API, and one permitted community source. Verify current availability and terms before implementation.
- Ticketmaster requires an API key; keep it in local configuration and never request or commit it.
- GitHub is a developer-ecosystem signal, not a direct event API.
- Do not make X or Instagram scraping an MVP dependency.
- Do not commit, push, or publish without explicit user approval.
- Do not commit secrets, generated databases, caches, or local environment files.

## Verification

- First run targeted parser and ingestion tests, then the full test suite and syntax check.
- Cover malformed inputs, duplicate ingestion, retries, raw retention, provenance, and changed feed versions as milestones advance.
- Before resolution milestones, add tests for timezone ambiguity, recurring instances, conflicting sources, ambiguous entities, replay, and auditable date, venue, and cancellation changes.
- Review the final diff for unintended changes, debug code, secrets, and generated artifacts.
- Report exact verification commands, results, changed files, and remaining risks.
