from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable, Iterable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from graph_etl.domain import Source, SourceKind

if TYPE_CHECKING:
    from graph_etl.extraction.models import CandidateDraft
    from graph_etl.ingestion.models import FetchedDocument, ParsedFeed
    from graph_etl.resolution.models import ResolutionDecision

Clock = Callable[[], datetime]

SCHEMA_VERSION = 3

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

MIGRATIONS = {1: MIGRATION_1, 2: MIGRATION_2, 3: MIGRATION_3}


def utc_now() -> datetime:
    return datetime.now(UTC)


def timestamp(value: datetime) -> str:
    if value.tzinfo is None:
        value = value.replace(tzinfo=UTC)
    return value.astimezone(UTC).isoformat().replace("+00:00", "Z")


class Database:
    def __init__(self, path: str | Path, *, clock: Clock = utc_now) -> None:
        self.connection = sqlite3.connect(str(path), timeout=30)
        self.connection.row_factory = sqlite3.Row
        self.connection.execute("PRAGMA foreign_keys = ON")
        self.clock = clock
        try:
            self.initialize()
        except Exception:
            self.connection.close()
            raise

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *_: object) -> None:
        self.connection.rollback()
        self.close()

    def close(self) -> None:
        self.connection.close()

    def initialize(self) -> None:
        if max(MIGRATIONS) != SCHEMA_VERSION:
            raise RuntimeError("schema version constant does not match migration table")
        with self.connection:
            self.connection.execute(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            current_row = self.connection.execute(
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
                self.connection.executescript(MIGRATIONS[version])
                self.connection.execute(
                    "INSERT OR IGNORE INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (version, timestamp(self.clock())),
                )
                current_version = version

    def upsert_source(self, source: Source) -> int:
        now = timestamp(self.clock())
        kind = source.kind.value if isinstance(source.kind, SourceKind) else str(source.kind)
        with self.connection:
            row = self.connection.execute(
                "SELECT id, uri FROM sources WHERE source_key = ?",
                (source.key,),
            ).fetchone()
            if row is None:
                cursor = self.connection.execute(
                    """
                    INSERT INTO sources(
                        source_key, name, kind, uri, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (source.key, source.name, kind, source.uri, now, now),
                )
                return int(cursor.lastrowid)
            source_id = int(row["id"])
            if row["uri"] != source.uri:
                self.connection.execute(
                    """
                    INSERT INTO source_uri_history(
                        source_id, previous_uri, current_uri, changed_at
                    ) VALUES (?, ?, ?, ?)
                    """,
                    (source_id, row["uri"], source.uri, now),
                )
            self.connection.execute(
                """
                UPDATE sources
                SET name = ?, kind = ?, uri = ?, updated_at = ?
                WHERE id = ?
                """,
                (source.name, kind, source.uri, now, source_id),
            )
            return source_id

    def start_run(self, source_id: int) -> int:
        now = timestamp(self.clock())
        with self.connection:
            self.connection.execute("BEGIN IMMEDIATE")
            row = self.connection.execute(
                """
                SELECT COALESCE(MAX(attempt_number), 0) + 1 AS next_attempt
                FROM ingestion_runs
                WHERE source_id = ?
                """,
                (source_id,),
            ).fetchone()
            attempt_number = int(row["next_attempt"])
            cursor = self.connection.execute(
                """
                INSERT INTO ingestion_runs(
                    source_id, status, attempt_number, started_at
                ) VALUES (?, 'running', ?, ?)
                """,
                (source_id, attempt_number, now),
            )
            return int(cursor.lastrowid)

    def save_raw_document(
        self,
        source_id: int,
        fetched: FetchedDocument,
        *,
        fetched_at: str,
    ) -> tuple[int, bool]:
        content_sha256 = hashlib.sha256(fetched.content).hexdigest()
        with self.connection:
            row = self.connection.execute(
                """
                SELECT id
                FROM raw_documents
                WHERE source_id = ? AND content_sha256 = ?
                """,
                (source_id, content_sha256),
            ).fetchone()
            if row is not None:
                return int(row["id"]), False
            cursor = self.connection.execute(
                """
                INSERT INTO raw_documents(
                    source_id,
                    content_sha256,
                    content_type,
                    etag,
                    last_modified,
                    payload,
                    fetched_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    source_id,
                    content_sha256,
                    fetched.content_type,
                    fetched.etag,
                    fetched.last_modified,
                    sqlite3.Binary(fetched.content),
                    fetched_at,
                ),
            )
            return int(cursor.lastrowid), True

    def attach_document(self, run_id: int, document_id: int) -> None:
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE ingestion_runs
                SET raw_document_id = ?
                WHERE id = ? AND status = 'running'
                """,
                (document_id, run_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("ingestion run is not active")

    def persist_feed(
        self,
        run_id: int,
        document_id: int,
        feed: ParsedFeed,
    ) -> tuple[int, int]:
        now = timestamp(self.clock())
        inserted_count = 0
        existing_count = 0
        with self.connection:
            run = self.connection.execute(
                """
                SELECT source_id, raw_document_id, status
                FROM ingestion_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if (
                run is None
                or run["status"] != "running"
                or run["raw_document_id"] != document_id
            ):
                raise RuntimeError("ingestion run does not reference the raw document")
            document = self.connection.execute(
                "SELECT source_id FROM raw_documents WHERE id = ?",
                (document_id,),
            ).fetchone()
            source_id = int(run["source_id"])
            if document is None or int(document["source_id"]) != source_id:
                raise RuntimeError("raw document does not belong to the ingestion source")
            self.connection.execute(
                """
                UPDATE raw_documents
                SET feed_format = ?, feed_title = ?
                WHERE id = ?
                """,
                (feed.feed_format, feed.title, document_id),
            )
            seen_entry_indices: set[int] = set()
            for entry in feed.entries:
                if entry.index in seen_entry_indices:
                    raise ValueError("parsed feed contains a duplicate entry index")
                seen_entry_indices.add(entry.index)
                conflict = self._observation_conflict(
                    source_id,
                    document_id,
                    entry.external_id,
                    entry.content_sha256,
                    entry.index,
                )
                if conflict is not None:
                    observation_id = int(conflict["id"])
                    existing_count += 1
                else:
                    try:
                        cursor = self.connection.execute(
                            """
                            INSERT INTO observations(
                                raw_document_id,
                                source_id,
                                external_id,
                                content_sha256,
                                parser_version,
                                entry_index,
                                observed_at
                            ) VALUES (?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                document_id,
                                source_id,
                                entry.external_id,
                                entry.content_sha256,
                                feed.parser_version,
                                entry.index,
                                now,
                            ),
                        )
                    except sqlite3.IntegrityError:
                        conflict = self._observation_conflict(
                            source_id,
                            document_id,
                            entry.external_id,
                            entry.content_sha256,
                            entry.index,
                        )
                        if conflict is None:
                            raise
                        observation_id = int(conflict["id"])
                        existing_count += 1
                    else:
                        inserted_count += 1
                        observation_id = int(cursor.lastrowid)
                        field_ordinals: dict[str, int] = {}
                        for field in entry.fields:
                            ordinal = field_ordinals.get(field.name, 0)
                            field_ordinals[field.name] = ordinal + 1
                            self.connection.execute(
                                """
                                INSERT INTO observation_fields(
                                    observation_id,
                                    field_name,
                                    field_ordinal,
                                    raw_value,
                                    is_present,
                                    locator,
                                    value_json
                                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    observation_id,
                                    field.name,
                                    ordinal,
                                    field.raw_value,
                                    int(field.is_present),
                                    field.locator,
                                    json.dumps(
                                        field.raw_value,
                                        ensure_ascii=False,
                                        separators=(",", ":"),
                                    ),
                                ),
                            )
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO document_observations(
                        raw_document_id,
                        observation_id,
                        entry_index,
                        external_id,
                        content_sha256,
                        recorded_at
                    ) VALUES (?, ?, ?, ?, ?, ?)
                    """,
                    (
                        document_id,
                        observation_id,
                        entry.index,
                        entry.external_id,
                        entry.content_sha256,
                        now,
                    ),
                )
            cursor = self.connection.execute(
                """
                UPDATE ingestion_runs
                SET status = 'succeeded', completed_at = ?, error = NULL
                WHERE id = ? AND status = 'running'
                """,
                (now, run_id),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("ingestion run is not active")
        return inserted_count, existing_count

    def _observation_conflict(
        self,
        source_id: int,
        document_id: int,
        external_id: str,
        content_sha256: str,
        entry_index: int,
    ) -> sqlite3.Row | None:
        return self.connection.execute(
            """
            SELECT id
            FROM observations
            WHERE (raw_document_id = ? AND entry_index = ?)
               OR (source_id = ? AND external_id = ? AND content_sha256 = ?)
            LIMIT 1
            """,
            (document_id, entry_index, source_id, external_id, content_sha256),
        ).fetchone()

    def create_extraction_run(
        self,
        extraction_version: str,
        normalization_version: str,
    ) -> int:
        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO extraction_runs(
                    extraction_version,
                    normalization_version,
                    status,
                    started_at
                ) VALUES (?, ?, 'running', ?)
                """,
                (
                    extraction_version,
                    normalization_version,
                    timestamp(self.clock()),
                ),
            )
            return int(cursor.lastrowid)

    def load_extraction_observations(
        self,
        source_key: str | None = None,
    ) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    o.id AS observation_id,
                    o.source_id,
                    o.external_id,
                    o.content_sha256,
                    f.field_name,
                    f.field_ordinal,
                    f.raw_value,
                    f.locator,
                    f.is_present
                FROM observations AS o
                JOIN sources AS s ON s.id = o.source_id
                LEFT JOIN observation_fields AS f ON f.observation_id = o.id
                WHERE (? IS NULL OR s.source_key = ?)
                ORDER BY o.id, f.field_ordinal, f.field_name
                """,
                (source_key, source_key),
            )
        )

    def persist_extraction(
        self,
        run_id: int,
        batches: Iterable[tuple[int, tuple[CandidateDraft, ...]]],
    ) -> tuple[int, int]:
        now = timestamp(self.clock())
        created_count = 0
        existing_count = 0
        with self.connection:
            run = self.connection.execute(
                """
                SELECT extraction_version, normalization_version, status
                FROM extraction_runs
                WHERE id = ?
                """,
                (run_id,),
            ).fetchone()
            if run is None or run["status"] != "running":
                raise RuntimeError("extraction run is not active")
            extraction_version = run["extraction_version"]
            normalization_version = run["normalization_version"]
            for observation_id, drafts in batches:
                for draft in drafts:
                    if draft.observation_id != observation_id:
                        raise RuntimeError("candidate observation does not match batch")
                    conflict = self.connection.execute(
                        """
                        SELECT id
                        FROM event_candidates
                        WHERE observation_id = ?
                          AND extraction_version = ?
                          AND normalization_version = ?
                          AND candidate_ordinal = ?
                        """,
                        (
                            observation_id,
                            extraction_version,
                            normalization_version,
                            draft.candidate_ordinal,
                        ),
                    ).fetchone()
                    if conflict is not None:
                        existing_count += 1
                        continue
                    key_conflict = self.connection.execute(
                        "SELECT id FROM event_candidates WHERE candidate_key = ?",
                        (draft.candidate_key,),
                    ).fetchone()
                    if key_conflict is not None:
                        raise RuntimeError("candidate key collides with an existing candidate")
                    cursor = self.connection.execute(
                        """
                        INSERT INTO event_candidates(
                            observation_id,
                            extraction_run_id,
                            extraction_version,
                            normalization_version,
                            candidate_ordinal,
                            candidate_key,
                            source_external_id,
                            created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            observation_id,
                            run_id,
                            extraction_version,
                            normalization_version,
                            draft.candidate_ordinal,
                            draft.candidate_key,
                            draft.external_id,
                            now,
                        ),
                    )
                    created_count += 1
                    candidate_id = int(cursor.lastrowid)
                    for field in draft.fields:
                        source_observation_id = None
                        if field.source_field_name is not None:
                            if field.source_field_ordinal is None:
                                raise RuntimeError(
                                    "candidate field provenance is missing an ordinal"
                                )
                            source_field = self.connection.execute(
                                """
                                SELECT 1
                                FROM observation_fields
                                WHERE observation_id = ?
                                  AND field_name = ?
                                  AND field_ordinal = ?
                                """,
                                (
                                    observation_id,
                                    field.source_field_name,
                                    field.source_field_ordinal,
                                ),
                            ).fetchone()
                            if source_field is None:
                                raise RuntimeError(
                                    "candidate field provenance does not belong to observation"
                                )
                            source_observation_id = observation_id
                        self.connection.execute(
                            """
                            INSERT INTO candidate_fields(
                                candidate_id,
                                field_name,
                                field_ordinal,
                                raw_value,
                                normalized_value,
                                normalization_status,
                                source_timezone,
                                normalization_error,
                                source_observation_id,
                                source_field_name,
                                source_field_ordinal,
                                locator
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                            """,
                            (
                                candidate_id,
                                field.field_name,
                                field.field_ordinal,
                                field.raw_value,
                                field.normalized_value,
                                field.normalization_status,
                                field.source_timezone,
                                field.normalization_error,
                                source_observation_id,
                                field.source_field_name,
                                field.source_field_ordinal,
                                field.locator,
                            ),
                        )
        return created_count, existing_count

    def complete_extraction_run(
        self,
        run_id: int,
        *,
        observations_processed: int,
        candidates_created: int,
        candidates_existing: int,
    ) -> None:
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE extraction_runs
                SET status = 'succeeded',
                    completed_at = ?,
                    observations_processed = ?,
                    candidates_created = ?,
                    candidates_existing = ?
                WHERE id = ? AND status = 'running'
                """,
                (
                    timestamp(self.clock()),
                    observations_processed,
                    candidates_created,
                    candidates_existing,
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("extraction run is not active")

    def fail_extraction_run(self, run_id: int, error: Exception) -> None:
        with self.connection:
            self.connection.execute(
                """
                UPDATE extraction_runs
                SET status = 'failed', completed_at = ?, error = ?
                WHERE id = ? AND status = 'running'
                """,
                (
                    timestamp(self.clock()),
                    f"{type(error).__name__}: {error}",
                    run_id,
                ),
            )

    def get_extraction_run(self, run_id: int) -> dict[str, object] | None:
        row = self.connection.execute(
            "SELECT * FROM extraction_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def query_candidates(
        self,
        *,
        source_key: str | None = None,
        extraction_version: str | None = None,
        normalization_version: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        conditions = ["1 = 1"]
        parameters: list[object] = []
        if source_key is not None:
            conditions.append("s.source_key = ?")
            parameters.append(source_key)
        if extraction_version is not None:
            conditions.append("ec.extraction_version = ?")
            parameters.append(extraction_version)
        if normalization_version is not None:
            conditions.append("ec.normalization_version = ?")
            parameters.append(normalization_version)
        limit_clause = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be positive")
            limit_clause = " LIMIT ?"
            parameters.append(limit)
        rows = self.connection.execute(
            f"""
            SELECT
                ec.id AS candidate_id,
                ec.observation_id,
                ec.extraction_run_id,
                ec.extraction_version,
                ec.normalization_version,
                ec.candidate_ordinal,
                ec.candidate_key,
                ec.source_external_id,
                ec.created_at,
                o.external_id AS observation_external_id,
                s.source_key
            FROM event_candidates AS ec
            JOIN observations AS o ON o.id = ec.observation_id
            JOIN sources AS s ON s.id = o.source_id
            WHERE {' AND '.join(conditions)}
            ORDER BY ec.id
            {limit_clause}
            """,
            parameters,
        ).fetchall()
        candidates: list[dict[str, object]] = []
        for row in rows:
            candidate = dict(row)
            candidate["fields"] = [
                dict(field)
                for field in self.connection.execute(
                    """
                    SELECT
                        field_name,
                        field_ordinal,
                        raw_value,
                        normalized_value,
                        normalization_status,
                        source_timezone,
                        normalization_error,
                        source_observation_id,
                        source_field_name,
                        source_field_ordinal,
                        locator
                    FROM candidate_fields
                    WHERE candidate_id = ?
                    ORDER BY field_ordinal, field_name
                    """,
                    (row["candidate_id"],),
                )
            ]
            candidates.append(candidate)
        return candidates

    def create_resolution_run(self, resolver_version: str) -> int:
        with self.connection:
            cursor = self.connection.execute(
                """
                INSERT INTO resolution_runs(
                    resolver_version, status, started_at
                ) VALUES (?, 'running', ?)
                """,
                (resolver_version, timestamp(self.clock())),
            )
            return int(cursor.lastrowid)

    def load_resolution_candidates(
        self,
        source_key: str | None,
        extraction_version: str,
        normalization_version: str,
    ) -> list[sqlite3.Row]:
        return list(
            self.connection.execute(
                """
                SELECT
                    ec.id AS candidate_id,
                    ec.source_external_id,
                    s.source_key,
                    cf.field_name,
                    cf.raw_value,
                    cf.normalized_value,
                    cf.normalization_status,
                    cf.source_timezone,
                    cf.locator
                FROM event_candidates AS ec
                JOIN observations AS o ON o.id = ec.observation_id
                JOIN sources AS s ON s.id = o.source_id
                JOIN candidate_fields AS cf ON cf.candidate_id = ec.id
                WHERE (? IS NULL OR s.source_key = ?)
                  AND ec.extraction_version = ?
                  AND ec.normalization_version = ?
                ORDER BY ec.id, cf.field_ordinal, cf.field_name
                """,
                (source_key, source_key, extraction_version, normalization_version),
            )
        )

    def find_canonical_by_identity(self, identity_key: str | None) -> dict[str, object] | None:
        if identity_key is None:
            return None
        row = self.connection.execute(
            """
            SELECT ce.id, ce.identity_key, ce.title_key, ce.event_date_key,
                   v.snapshot_json
            FROM canonical_events AS ce
            JOIN canonical_event_versions AS v
              ON v.canonical_event_id = ce.id
             AND v.version_number = ce.current_version
            WHERE ce.identity_key = ?
            """,
            (identity_key,),
        ).fetchone()
        return dict(row) if row is not None else None

    def find_canonical_by_title(self, title_key: str | None) -> list[dict[str, object]]:
        if title_key is None:
            return []
        return [
            dict(row)
            for row in self.connection.execute(
                """
                SELECT ce.id, ce.identity_key, ce.title_key, ce.event_date_key,
                       v.snapshot_json
                FROM canonical_events AS ce
                JOIN canonical_event_versions AS v
                  ON v.canonical_event_id = ce.id
                 AND v.version_number = ce.current_version
                WHERE ce.title_key = ?
                ORDER BY ce.id
                """,
                (title_key,),
            )
        ]

    def persist_resolution(
        self,
        run_id: int,
        decisions: Iterable[ResolutionDecision],
    ) -> tuple[int, int, int, int]:
        now = timestamp(self.clock())
        created = 0
        existing = 0
        conflicts = 0
        unresolved = 0
        with self.connection:
            run = self.connection.execute(
                "SELECT resolver_version, status FROM resolution_runs WHERE id = ?",
                (run_id,),
            ).fetchone()
            if run is None or run["status"] != "running":
                raise RuntimeError("resolution run is not active")
            resolver_version = run["resolver_version"]
            for decision in decisions:
                previous = self.connection.execute(
                    """
                    SELECT id FROM candidate_resolutions
                    WHERE candidate_id = ? AND resolver_version = ?
                    """,
                    (decision.candidate_id, resolver_version),
                ).fetchone()
                if previous is not None:
                    existing += 1
                    if decision.status.value == "conflict":
                        conflicts += 1
                    if decision.status.value in {"ambiguous", "unmatched"}:
                        unresolved += 1
                    continue

                canonical_event_id = decision.canonical_event_id
                if decision.status.value == "created":
                    cursor = self.connection.execute(
                        """
                        INSERT INTO canonical_events(
                            identity_key, title_key, event_date_key,
                            current_version, created_at, updated_at
                        ) VALUES (?, ?, ?, 1, ?, ?)
                        """,
                        (
                            decision.identity_key,
                            decision.title_key,
                            decision.snapshot.get("event_date"),
                            now,
                            now,
                        ),
                    )
                    canonical_event_id = int(cursor.lastrowid)
                    self._insert_canonical_version(
                        canonical_event_id,
                        1,
                        decision.snapshot,
                        decision.reason,
                        run_id,
                        decision.candidate_id,
                        now,
                    )
                elif decision.status.value in {"updated", "conflict"}:
                    if canonical_event_id is None:
                        raise RuntimeError("matched decision has no canonical event")
                    current = self.connection.execute(
                        """
                        SELECT current_version FROM canonical_events
                        WHERE id = ?
                        """,
                        (canonical_event_id,),
                    ).fetchone()
                    if current is None:
                        raise RuntimeError("canonical event does not exist")
                    next_version = int(current["current_version"]) + 1
                    self.connection.execute(
                        """
                        UPDATE canonical_event_versions
                        SET valid_to = ?
                        WHERE canonical_event_id = ?
                          AND version_number = ?
                          AND valid_to IS NULL
                        """,
                        (now, canonical_event_id, int(current["current_version"])),
                    )
                    self._insert_canonical_version(
                        canonical_event_id,
                        next_version,
                        decision.snapshot,
                        decision.reason,
                        run_id,
                        decision.candidate_id,
                        now,
                    )
                    self.connection.execute(
                        """
                        UPDATE canonical_events
                        SET current_version = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (next_version, now, canonical_event_id),
                    )

                self.connection.execute(
                    """
                    INSERT INTO candidate_resolutions(
                        candidate_id,
                        resolution_run_id,
                        resolver_version,
                        status,
                        canonical_event_id,
                        identity_key,
                        title_key,
                        reason,
                        conflict_fields_json,
                        resolved_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        decision.candidate_id,
                        run_id,
                        resolver_version,
                        decision.status.value,
                        canonical_event_id,
                        decision.identity_key,
                        decision.title_key,
                        decision.reason,
                        json.dumps(list(decision.conflict_fields), separators=(",", ":")),
                        now,
                    ),
                )
                created += 1
                if decision.status.value == "conflict":
                    conflicts += 1
                if decision.status.value in {"ambiguous", "unmatched"}:
                    unresolved += 1
        return created, existing, conflicts, unresolved

    def _insert_canonical_version(
        self,
        canonical_event_id: int,
        version_number: int,
        snapshot: object,
        change_reason: str,
        run_id: int,
        candidate_id: int,
        created_at: str,
    ) -> None:
        self.connection.execute(
            """
            INSERT INTO canonical_event_versions(
                canonical_event_id,
                version_number,
                snapshot_json,
                valid_from,
                change_reason,
                resolution_run_id,
                candidate_id
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            """,
            (
                canonical_event_id,
                version_number,
                json.dumps(snapshot, ensure_ascii=False, sort_keys=True, separators=(",", ":")),
                created_at,
                change_reason,
                run_id,
                candidate_id,
            ),
        )

    def complete_resolution_run(
        self,
        run_id: int,
        *,
        candidates_processed: int,
        decisions_created: int,
        decisions_existing: int,
        conflicts: int,
        unresolved: int,
    ) -> None:
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE resolution_runs
                SET status = 'succeeded',
                    completed_at = ?,
                    candidates_processed = ?,
                    decisions_created = ?,
                    decisions_existing = ?,
                    conflicts = ?,
                    unresolved = ?
                WHERE id = ? AND status = 'running'
                """,
                (
                    timestamp(self.clock()),
                    candidates_processed,
                    decisions_created,
                    decisions_existing,
                    conflicts,
                    unresolved,
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("resolution run is not active")

    def fail_resolution_run(self, run_id: int, error: Exception) -> None:
        with self.connection:
            self.connection.execute(
                """
                UPDATE resolution_runs
                SET status = 'failed', completed_at = ?, error = ?
                WHERE id = ? AND status = 'running'
                """,
                (
                    timestamp(self.clock()),
                    f"{type(error).__name__}: {error}",
                    run_id,
                ),
            )

    def query_canonical_events(self, *, limit: int | None = None) -> list[dict[str, object]]:
        limit_clause = ""
        parameters: list[object] = []
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be positive")
            limit_clause = " LIMIT ?"
            parameters.append(limit)
        rows = self.connection.execute(
            f"""
            SELECT ce.id, ce.identity_key, ce.title_key, ce.event_date_key,
                   ce.current_version, ce.created_at, ce.updated_at,
                   v.snapshot_json, v.change_reason
            FROM canonical_events AS ce
            JOIN canonical_event_versions AS v
              ON v.canonical_event_id = ce.id
             AND v.version_number = ce.current_version
            ORDER BY ce.id
            {limit_clause}
            """,
            parameters,
        ).fetchall()
        events: list[dict[str, object]] = []
        for row in rows:
            event = dict(row)
            event["snapshot"] = json.loads(event.pop("snapshot_json"))
            event["versions"] = [
                dict(version)
                for version in self.connection.execute(
                    """
                    SELECT version_number, valid_from, valid_to, change_reason,
                           resolution_run_id, candidate_id
                    FROM canonical_event_versions
                    WHERE canonical_event_id = ?
                    ORDER BY version_number
                    """,
                    (row["id"],),
                )
            ]
            event["resolutions"] = [
                dict(resolution)
                for resolution in self.connection.execute(
                    """
                    SELECT candidate_id, resolver_version, status, reason,
                           conflict_fields_json, resolved_at
                    FROM candidate_resolutions
                    WHERE canonical_event_id = ?
                    ORDER BY id
                    """,
                    (row["id"],),
                )
            ]
            events.append(event)
        return events

    def fail_run(self, run_id: int, error: Exception) -> None:
        completed_at = timestamp(self.clock())
        message = f"{type(error).__name__}: {error}"
        with self.connection:
            self.connection.execute(
                """
                UPDATE ingestion_runs
                SET status = 'failed', completed_at = ?, error = ?
                WHERE id = ? AND status = 'running'
                """,
                (completed_at, message, run_id),
            )
