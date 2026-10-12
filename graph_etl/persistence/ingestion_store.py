import hashlib
import json
import sqlite3
from typing import TYPE_CHECKING

from graph_etl.domain import Source, SourceKind
from graph_etl.persistence.clock import timestamp

if TYPE_CHECKING:
    from graph_etl.ingestion.models import FetchedDocument, ParsedFeed
    from graph_etl.persistence.database import Database


class IngestionStore:
    def upsert_source(self: "Database", source: Source) -> int:
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

    def start_run(self: "Database", source_id: int) -> int:
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
        self: "Database",
        source_id: int,
        fetched: "FetchedDocument",
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

    def attach_document(self: "Database", run_id: int, document_id: int) -> None:
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
        self: "Database",
        run_id: int,
        document_id: int,
        feed: "ParsedFeed",
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
                                    locator
                                ) VALUES (?, ?, ?, ?, ?, ?)
                                """,
                                (
                                    observation_id,
                                    field.name,
                                    ordinal,
                                    field.raw_value,
                                    int(field.is_present),
                                    field.locator,
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
        self: "Database",
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

    def fail_run(self: "Database", run_id: int, error: Exception) -> None:
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