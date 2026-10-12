import json
import sqlite3
from collections.abc import Iterable
from typing import TYPE_CHECKING

from graph_etl.persistence.clock import timestamp

if TYPE_CHECKING:
    from graph_etl.extraction.models import CandidateDraft
    from graph_etl.persistence.database import Database


class ExtractionStore:
    def create_extraction_run(
        self: "Database",
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
        self: "Database",
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
        self: "Database",
        run_id: int,
        batches: Iterable[tuple[int, tuple["CandidateDraft", ...]]],
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
                          AND candidate_slot_key = ?
                        """,
                        (
                            observation_id,
                            extraction_version,
                            normalization_version,
                            draft.candidate_slot_key,
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
                            candidate_slot_key,
                            candidate_key,
                            source_external_id,
                            classification,
                            classification_reason,
                            created_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                        """,
                        (
                            observation_id,
                            run_id,
                            extraction_version,
                            normalization_version,
                            draft.candidate_ordinal,
                            draft.candidate_slot_key,
                            draft.candidate_key,
                            draft.external_id,
                            draft.classification,
                            draft.classification_reason,
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

    def record_extraction_outcome(
        self: "Database",
        run_id: int,
        observation_id: int,
        *,
        status: str,
        candidates_created: int,
        event_classification: str | None = None,
        error: str | None = None,
    ) -> None:
        run = self.connection.execute(
            """
            SELECT extraction_version, normalization_version
            FROM extraction_runs
            WHERE id = ?
            """,
            (run_id,),
        ).fetchone()
        if run is None:
            raise RuntimeError("extraction run does not exist")
        with self.connection:
            self.connection.execute(
                """
                INSERT INTO extraction_observation_outcomes(
                    observation_id,
                    extraction_run_id,
                    extraction_version,
                    normalization_version,
                    status,
                    event_classification,
                    candidates_created,
                    error,
                    recorded_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    observation_id,
                    run_id,
                    run["extraction_version"],
                    run["normalization_version"],
                    status,
                    event_classification,
                    candidates_created,
                    error,
                    timestamp(self.clock()),
                ),
            )

    def complete_extraction_run(
        self: "Database",
        run_id: int,
        *,
        observations_processed: int,
        candidates_created: int,
        candidates_existing: int,
        observations_with_errors: int = 0,
        observations_skipped: int = 0,
    ) -> None:
        with self.connection:
            cursor = self.connection.execute(
                """
                UPDATE extraction_runs
                SET status = 'succeeded',
                    completed_at = ?,
                    observations_processed = ?,
                    candidates_created = ?,
                    candidates_existing = ?,
                    observations_with_errors = ?,
                    observations_skipped = ?
                WHERE id = ? AND status = 'running'
                """,
                (
                    timestamp(self.clock()),
                    observations_processed,
                    candidates_created,
                    candidates_existing,
                    observations_with_errors,
                    observations_skipped,
                    run_id,
                ),
            )
            if cursor.rowcount != 1:
                raise RuntimeError("extraction run is not active")

    def fail_extraction_run(self: "Database", run_id: int, error: Exception) -> None:
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

    def get_extraction_run(self: "Database", run_id: int) -> dict[str, object] | None:
        row = self.connection.execute(
            "SELECT * FROM extraction_runs WHERE id = ?",
            (run_id,),
        ).fetchone()
        return dict(row) if row is not None else None

    def query_candidates(
        self: "Database",
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
                ec.candidate_slot_key,
                ec.candidate_key,
                ec.classification,
                ec.classification_reason,
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

    def query_extraction_outcomes(
        self: "Database",
        *,
        source_key: str | None = None,
        status: str | None = None,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
        conditions = ["1 = 1"]
        parameters: list[object] = []
        if source_key is not None:
            conditions.append("s.source_key = ?")
            parameters.append(source_key)
        if status is not None:
            conditions.append("eoo.status = ?")
            parameters.append(status)
        limit_clause = ""
        if limit is not None:
            if limit < 1:
                raise ValueError("limit must be positive")
            limit_clause = " LIMIT ?"
            parameters.append(limit)
        return [
            dict(row)
            for row in self.connection.execute(
                f"""
                SELECT eoo.*, s.source_key, o.external_id
                FROM extraction_observation_outcomes AS eoo
                JOIN observations AS o ON o.id = eoo.observation_id
                JOIN sources AS s ON s.id = o.source_id
                WHERE {' AND '.join(conditions)}
                ORDER BY eoo.id
                {limit_clause}
                """,
                parameters,
            )
        ]