from __future__ import annotations

import hashlib
import json
import sqlite3
from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING

from graph_etl.domain import Source, SourceKind

if TYPE_CHECKING:
    from graph_etl.ingestion.models import FetchedDocument, ParsedFeed

Clock = Callable[[], datetime]


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
        self.initialize()

    def __enter__(self) -> "Database":
        return self

    def __exit__(self, *_: object) -> None:
        self.connection.rollback()
        self.close()

    def close(self) -> None:
        self.connection.close()

    def initialize(self) -> None:
        with self.connection:
            self.connection.executescript(
                """
                CREATE TABLE IF NOT EXISTS schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );

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
            )
            migration = self.connection.execute(
                "SELECT version FROM schema_migrations WHERE version = 1"
            ).fetchone()
            if migration is None:
                self.connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (1, ?)",
                    (timestamp(self.clock()),),
                )

    def upsert_source(self, source: Source) -> int:
        now = timestamp(self.clock())
        kind = source.kind.value if isinstance(source.kind, SourceKind) else str(source.kind)
        with self.connection:
            row = self.connection.execute(
                "SELECT id FROM sources WHERE source_key = ?",
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
                    existing_count += 1
                    continue
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
                    existing_count += 1
                    continue
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
