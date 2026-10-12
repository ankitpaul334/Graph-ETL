import json
import sqlite3
from collections.abc import Callable
from datetime import datetime

from graph_etl.identity import normalize_title, normalize_url
from graph_etl.identity import source_identity_key as build_source_identity_key
from graph_etl.identity import title_date_identity_key

Clock = Callable[[], datetime]

SCHEMA_VERSION = 5

MIGRATION_1 = """
CREATE TABLE IF NOT EXISTS sources (
    id INTEGER PRIMARY KEY,
    source_key TEXT NOT NULL UNIQUE,
    name TEXT NOT NULL,
    kind TEXT NOT NULL,
    uri TEXT NOT NULL,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS raw_documents (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    content_sha256 TEXT NOT NULL,
    content_type TEXT,
    etag TEXT,
    last_modified TEXT,
    payload BLOB NOT NULL,
    fetched_at TEXT NOT NULL,
    feed_format TEXT,
    feed_title TEXT,
    UNIQUE(source_id, content_sha256)
);

CREATE TABLE IF NOT EXISTS ingestion_runs (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    raw_document_id INTEGER REFERENCES raw_documents(id),
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    attempt_number INTEGER NOT NULL,
    started_at TEXT NOT NULL,
    completed_at TEXT,
    error TEXT,
    UNIQUE(source_id, attempt_number)
);

CREATE TABLE IF NOT EXISTS observations (
    id INTEGER PRIMARY KEY,
    raw_document_id INTEGER NOT NULL REFERENCES raw_documents(id),
    source_id INTEGER NOT NULL REFERENCES sources(id),
    external_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    parser_version TEXT NOT NULL,
    entry_index INTEGER NOT NULL,
    observed_at TEXT NOT NULL,
    UNIQUE(raw_document_id, entry_index),
    UNIQUE(source_id, external_id, content_sha256)
);

CREATE TABLE IF NOT EXISTS observation_fields (
    observation_id INTEGER NOT NULL REFERENCES observations(id),
    field_name TEXT NOT NULL,
    field_ordinal INTEGER NOT NULL,
    raw_value TEXT,
    is_present INTEGER NOT NULL CHECK (is_present IN (0, 1)),
    locator TEXT NOT NULL,
    value_json TEXT NOT NULL,
    PRIMARY KEY (observation_id, field_name, field_ordinal)
);

CREATE INDEX IF NOT EXISTS ingestion_runs_source_started
    ON ingestion_runs(source_id, started_at);
CREATE INDEX IF NOT EXISTS observations_source
    ON observations(source_id, observed_at);
CREATE INDEX IF NOT EXISTS observations_external
    ON observations(source_id, external_id);
"""

MIGRATION_2 = """
CREATE TABLE IF NOT EXISTS source_uri_history (
    id INTEGER PRIMARY KEY,
    source_id INTEGER NOT NULL REFERENCES sources(id),
    previous_uri TEXT,
    current_uri TEXT NOT NULL,
    changed_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS document_observations (
    id INTEGER PRIMARY KEY,
    raw_document_id INTEGER NOT NULL REFERENCES raw_documents(id),
    observation_id INTEGER NOT NULL REFERENCES observations(id),
    entry_index INTEGER NOT NULL,
    external_id TEXT NOT NULL,
    content_sha256 TEXT NOT NULL,
    recorded_at TEXT NOT NULL,
    UNIQUE(raw_document_id, entry_index)
);

CREATE TABLE IF NOT EXISTS extraction_runs (
    id INTEGER PRIMARY KEY,
    extraction_version TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    observations_processed INTEGER NOT NULL DEFAULT 0,
    candidates_created INTEGER NOT NULL DEFAULT 0,
    candidates_existing INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS event_candidates (
    id INTEGER PRIMARY KEY,
    observation_id INTEGER NOT NULL REFERENCES observations(id),
    extraction_run_id INTEGER NOT NULL REFERENCES extraction_runs(id),
    extraction_version TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    candidate_ordinal INTEGER NOT NULL,
    candidate_key TEXT NOT NULL UNIQUE,
    source_external_id TEXT NOT NULL,
    created_at TEXT NOT NULL,
    UNIQUE(observation_id, extraction_version, normalization_version, candidate_ordinal)
);

CREATE TABLE IF NOT EXISTS candidate_fields (
    id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES event_candidates(id),
    field_name TEXT NOT NULL,
    field_ordinal INTEGER NOT NULL,
    raw_value TEXT,
    normalized_value TEXT,
    normalization_status TEXT NOT NULL,
    source_timezone TEXT,
    normalization_error TEXT,
    source_observation_id INTEGER,
    source_field_name TEXT,
    source_field_ordinal INTEGER,
    locator TEXT NOT NULL,
    FOREIGN KEY (source_observation_id, source_field_name, source_field_ordinal)
        REFERENCES observation_fields(observation_id, field_name, field_ordinal),
    UNIQUE(candidate_id, field_name, field_ordinal)
);

CREATE INDEX IF NOT EXISTS document_observations_document
    ON document_observations(raw_document_id, entry_index);
CREATE INDEX IF NOT EXISTS document_observations_observation
    ON document_observations(observation_id);
CREATE INDEX IF NOT EXISTS extraction_runs_version
    ON extraction_runs(extraction_version, normalization_version, started_at);
CREATE INDEX IF NOT EXISTS event_candidates_observation
    ON event_candidates(observation_id, extraction_version, normalization_version);
CREATE INDEX IF NOT EXISTS candidate_fields_source
    ON candidate_fields(source_observation_id, source_field_name, source_field_ordinal);

INSERT OR IGNORE INTO document_observations(
    raw_document_id,
    observation_id,
    entry_index,
    external_id,
    content_sha256,
    recorded_at
)
SELECT raw_document_id, id, entry_index, external_id, content_sha256, observed_at
FROM observations;
"""

MIGRATION_3 = """
CREATE TABLE IF NOT EXISTS resolution_runs (
    id INTEGER PRIMARY KEY,
    resolver_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('running', 'succeeded', 'failed')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    candidates_processed INTEGER NOT NULL DEFAULT 0,
    decisions_created INTEGER NOT NULL DEFAULT 0,
    decisions_existing INTEGER NOT NULL DEFAULT 0,
    conflicts INTEGER NOT NULL DEFAULT 0,
    unresolved INTEGER NOT NULL DEFAULT 0,
    error TEXT
);

CREATE TABLE IF NOT EXISTS canonical_events (
    id INTEGER PRIMARY KEY,
    identity_key TEXT NOT NULL UNIQUE,
    title_key TEXT NOT NULL,
    event_date_key TEXT NOT NULL,
    current_version INTEGER NOT NULL DEFAULT 1,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS canonical_event_versions (
    id INTEGER PRIMARY KEY,
    canonical_event_id INTEGER NOT NULL REFERENCES canonical_events(id),
    version_number INTEGER NOT NULL,
    snapshot_json TEXT NOT NULL,
    valid_from TEXT NOT NULL,
    valid_to TEXT,
    change_reason TEXT NOT NULL,
    resolution_run_id INTEGER NOT NULL REFERENCES resolution_runs(id),
    candidate_id INTEGER NOT NULL REFERENCES event_candidates(id),
    UNIQUE(canonical_event_id, version_number)
);

CREATE TABLE IF NOT EXISTS candidate_resolutions (
    id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES event_candidates(id),
    resolution_run_id INTEGER NOT NULL REFERENCES resolution_runs(id),
    resolver_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('created', 'matched', 'updated', 'conflict', 'ambiguous', 'unmatched')
    ),
    canonical_event_id INTEGER REFERENCES canonical_events(id),
    identity_key TEXT,
    title_key TEXT,
    reason TEXT NOT NULL,
    conflict_fields_json TEXT NOT NULL,
    resolved_at TEXT NOT NULL,
    UNIQUE(candidate_id, resolver_version)
);

CREATE INDEX IF NOT EXISTS canonical_events_title
    ON canonical_events(title_key, event_date_key);
CREATE INDEX IF NOT EXISTS canonical_event_versions_event
    ON canonical_event_versions(canonical_event_id, version_number);
CREATE INDEX IF NOT EXISTS candidate_resolutions_run
    ON candidate_resolutions(resolution_run_id, status);
"""

MIGRATION_4 = """
ALTER TABLE extraction_runs ADD COLUMN observations_with_errors INTEGER DEFAULT 0;
ALTER TABLE extraction_runs ADD COLUMN observations_skipped INTEGER DEFAULT 0;

ALTER TABLE event_candidates ADD COLUMN classification TEXT DEFAULT 'EVENT_POSSIBLE'
    CHECK (classification IN ('EVENT_CONFIRMED_BY_SOURCE', 'EVENT_POSSIBLE', 'NOT_EVENT', 'EXTRACTION_FAILED'));
ALTER TABLE event_candidates ADD COLUMN classification_reason TEXT;
ALTER TABLE event_candidates ADD COLUMN candidate_slot_key TEXT;
UPDATE event_candidates
SET candidate_slot_key = candidate_key
WHERE candidate_slot_key IS NULL;
CREATE UNIQUE INDEX IF NOT EXISTS event_candidates_slot
    ON event_candidates(observation_id, extraction_version, normalization_version, candidate_slot_key);

ALTER TABLE canonical_events ADD COLUMN status TEXT DEFAULT 'ACTIVE'
    CHECK (status IN ('PROPOSED', 'ACTIVE', 'REVIEW_REQUIRED', 'REJECTED', 'SPLIT'));
ALTER TABLE canonical_events ADD COLUMN source_identity_key TEXT;
ALTER TABLE canonical_events ADD COLUMN url_key TEXT;
CREATE UNIQUE INDEX IF NOT EXISTS canonical_events_source_identity
    ON canonical_events(source_identity_key)
    WHERE source_identity_key IS NOT NULL;
CREATE INDEX IF NOT EXISTS canonical_events_url
    ON canonical_events(url_key);

DROP INDEX candidate_resolutions_run;
ALTER TABLE candidate_resolutions RENAME TO candidate_resolutions_legacy;
CREATE TABLE candidate_resolutions (
    id INTEGER PRIMARY KEY,
    candidate_id INTEGER NOT NULL REFERENCES event_candidates(id),
    resolution_run_id INTEGER NOT NULL REFERENCES resolution_runs(id),
    resolver_version TEXT NOT NULL,
    decision_sequence INTEGER NOT NULL DEFAULT 0,
    status TEXT NOT NULL CHECK (
        status IN ('UNRESOLVED', 'MATCHED', 'NEW_EVENT_PROPOSED', 'REVIEW_REQUIRED', 'REJECTED', 'UPDATED', 'CONFLICT')
    ),
    canonical_event_id INTEGER REFERENCES canonical_events(id),
    identity_key TEXT,
    title_key TEXT,
    reason TEXT NOT NULL,
    conflict_fields_json TEXT NOT NULL,
    resolved_at TEXT NOT NULL,
    superseded_by INTEGER REFERENCES candidate_resolutions(id),
    review_note TEXT,
    reviewed_by TEXT,
    reviewed_at TEXT,
    UNIQUE(candidate_id, resolver_version, decision_sequence)
);
INSERT INTO candidate_resolutions(
    id,
    candidate_id,
    resolution_run_id,
    resolver_version,
    decision_sequence,
    status,
    canonical_event_id,
    identity_key,
    title_key,
    reason,
    conflict_fields_json,
    resolved_at
)
SELECT
    id,
    candidate_id,
    resolution_run_id,
    resolver_version,
    0,
    CASE status
        WHEN 'created' THEN 'NEW_EVENT_PROPOSED'
        WHEN 'matched' THEN 'MATCHED'
        WHEN 'updated' THEN 'MATCHED'
        WHEN 'conflict' THEN 'REVIEW_REQUIRED'
        WHEN 'ambiguous' THEN 'REVIEW_REQUIRED'
        ELSE 'UNRESOLVED'
    END,
    canonical_event_id,
    identity_key,
    title_key,
    reason,
    conflict_fields_json,
    resolved_at
FROM candidate_resolutions_legacy;
DROP TABLE candidate_resolutions_legacy;
CREATE INDEX IF NOT EXISTS candidate_resolutions_run
    ON candidate_resolutions(resolution_run_id, status);

CREATE TABLE canonical_event_candidates (
    id INTEGER PRIMARY KEY,
    canonical_event_id INTEGER NOT NULL REFERENCES canonical_events(id),
    candidate_id INTEGER NOT NULL REFERENCES event_candidates(id),
    resolution_decision_id INTEGER REFERENCES candidate_resolutions(id),
    linked_at TEXT NOT NULL,
    link_reason TEXT NOT NULL,
    link_status TEXT NOT NULL DEFAULT 'ACTIVE'
        CHECK (link_status IN ('ACTIVE', 'REVIEW', 'DETACHED')),
    detached_at TEXT,
    detach_reason TEXT
);
CREATE UNIQUE INDEX IF NOT EXISTS canonical_event_candidates_active
    ON canonical_event_candidates(canonical_event_id, candidate_id)
    WHERE detached_at IS NULL;
CREATE INDEX IF NOT EXISTS canonical_event_candidates_candidate
    ON canonical_event_candidates(candidate_id, detached_at);
INSERT INTO canonical_event_candidates(
    canonical_event_id,
    candidate_id,
    resolution_decision_id,
    linked_at,
    link_reason,
    link_status
)
SELECT
    canonical_event_id,
    candidate_id,
    MIN(id),
    MIN(resolved_at),
    'migrated_resolution_history',
    CASE WHEN MIN(status) = 'REVIEW_REQUIRED' THEN 'REVIEW' ELSE 'ACTIVE' END
FROM candidate_resolutions
WHERE canonical_event_id IS NOT NULL
GROUP BY canonical_event_id, candidate_id;

CREATE TABLE extraction_observation_outcomes (
    id INTEGER PRIMARY KEY,
    observation_id INTEGER NOT NULL REFERENCES observations(id),
    extraction_run_id INTEGER NOT NULL REFERENCES extraction_runs(id),
    extraction_version TEXT NOT NULL,
    normalization_version TEXT NOT NULL,
    status TEXT NOT NULL CHECK (
        status IN ('SUCCEEDED', 'SUCCEEDED_WITH_ERRORS', 'FAILED', 'SKIPPED')
    ),
    event_classification TEXT CHECK (
        event_classification IN ('EVENT_CONFIRMED_BY_SOURCE', 'EVENT_POSSIBLE', 'NOT_EVENT', 'EXTRACTION_FAILED')
    ),
    candidates_created INTEGER NOT NULL DEFAULT 0,
    error TEXT,
    recorded_at TEXT NOT NULL
);
CREATE INDEX IF NOT EXISTS extraction_observation_outcomes_run
    ON extraction_observation_outcomes(extraction_run_id, status);
"""

MIGRATION_5 = """
ALTER TABLE observation_fields DROP COLUMN value_json;
"""

MIGRATIONS = {
    1: MIGRATION_1,
    2: MIGRATION_2,
    3: MIGRATION_3,
    4: MIGRATION_4,
    5: MIGRATION_5,
}


def apply_migrations(
    connection: sqlite3.Connection,
    clock: Clock,
    timestamp: Callable[[datetime], str],
) -> None:
    if max(MIGRATIONS) != SCHEMA_VERSION:
        raise RuntimeError("schema version constant does not match migration table")
    started_transaction = not connection.in_transaction
    if started_transaction:
        connection.execute("BEGIN IMMEDIATE")
    try:
        connection.execute(
            """
            CREATE TABLE IF NOT EXISTS schema_migrations (
                version INTEGER PRIMARY KEY,
                applied_at TEXT NOT NULL
            )
            """
        )
        current_row = connection.execute(
            "SELECT COALESCE(MAX(version), 0) AS version FROM schema_migrations"
        ).fetchone()
        current_version = int(current_row["version"])
        if current_version > SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema version {current_version} is newer than supported {SCHEMA_VERSION}"
            )
        for version in sorted(MIGRATIONS):
            if version <= current_version:
                continue
            for statement in MIGRATIONS[version].split(";"):
                stripped = statement.strip()
                if not stripped or not _statement_needed(connection, version, stripped):
                    continue
                connection.execute(stripped)
            if version == 4:
                _backfill_canonical_identities(connection)
            connection.execute(
                "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                (version, timestamp(clock())),
            )
        if started_transaction:
            connection.commit()
    except Exception:
        if started_transaction:
            connection.rollback()
        raise


def _statement_needed(
    connection: sqlite3.Connection,
    version: int,
    statement: str,
) -> bool:
    normalized = " ".join(statement.split())
    if version == 4:
        return _migration_4_statement_needed(connection, normalized)
    if version == 5:
        if normalized.startswith("ALTER TABLE observation_fields DROP COLUMN "):
            column = normalized.split()[-1]
            return column in table_columns(connection, "observation_fields")
    return True


def _migration_4_statement_needed(
    connection: sqlite3.Connection,
    normalized: str,
) -> bool:
    if normalized.startswith("ALTER TABLE ") and " ADD COLUMN " in normalized:
        parts = normalized.split()
        return parts[5] not in table_columns(connection, parts[2])
    if normalized == "DROP INDEX candidate_resolutions_run":
        return object_exists(connection, "index", "candidate_resolutions_run")
    if normalized == "ALTER TABLE candidate_resolutions RENAME TO candidate_resolutions_legacy":
        return (
            not object_exists(connection, "table", "candidate_resolutions_legacy")
            and "decision_sequence"
            not in table_columns(connection, "candidate_resolutions")
        )
    if normalized.startswith("CREATE TABLE candidate_resolutions ("):
        return "decision_sequence" not in table_columns(
            connection, "candidate_resolutions"
        )
    if normalized.startswith("INSERT INTO candidate_resolutions("):
        return (
            object_exists(connection, "table", "candidate_resolutions_legacy")
            and connection.execute(
                "SELECT COUNT(*) FROM candidate_resolutions"
            ).fetchone()[0]
            == 0
        )
    if normalized == "DROP TABLE candidate_resolutions_legacy":
        return object_exists(connection, "table", "candidate_resolutions_legacy")
    if normalized.startswith("INSERT INTO canonical_event_candidates("):
        return (
            connection.execute(
                "SELECT COUNT(*) FROM canonical_event_candidates"
            ).fetchone()[0]
            == 0
        )
    return True


def table_columns(connection: sqlite3.Connection, table: str) -> set[str]:
    if not object_exists(connection, "table", table):
        return set()
    return {
        row["name"]
        for row in connection.execute(f"PRAGMA table_info({table})")
    }


def object_exists(
    connection: sqlite3.Connection,
    object_type: str,
    name: str,
) -> bool:
    return (
        connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = ? AND name = ?",
            (object_type, name),
        ).fetchone()
        is not None
    )


def _backfill_canonical_identities(connection: sqlite3.Connection) -> None:
    events = connection.execute(
        """
        SELECT ce.id, v.snapshot_json
        FROM canonical_events AS ce
        JOIN canonical_event_versions AS v
          ON v.canonical_event_id = ce.id
         AND v.version_number = ce.current_version
        """
    ).fetchall()
    for event in events:
        snapshot = json.loads(event["snapshot_json"])
        title = snapshot.get("title")
        event_date = snapshot.get("event_date")
        identity_key = title_date_identity_key(title, event_date)
        title_key = normalize_title(title) or ""
        url_key = normalize_url(snapshot.get("url"))
        provenance = connection.execute(
            """
            SELECT s.source_key, ec.source_external_id
            FROM candidate_resolutions AS cr
            JOIN event_candidates AS ec ON ec.id = cr.candidate_id
            JOIN observations AS o ON o.id = ec.observation_id
            JOIN sources AS s ON s.id = o.source_id
            WHERE cr.canonical_event_id = ?
            ORDER BY cr.id
            LIMIT 1
            """,
            (event["id"],),
        ).fetchone()
        source_identity = (
            build_source_identity_key(
                provenance["source_key"],
                provenance["source_external_id"],
            )
            if provenance is not None
            else None
        )
        try:
            connection.execute(
                """
                UPDATE canonical_events
                SET identity_key = ?, title_key = ?, source_identity_key = ?, url_key = ?
                WHERE id = ?
                """,
                (identity_key, title_key, source_identity, url_key, event["id"]),
            )
        except sqlite3.IntegrityError:
            connection.execute(
                "UPDATE canonical_events SET status = 'REVIEW_REQUIRED' WHERE id = ?",
                (event["id"],),
            )