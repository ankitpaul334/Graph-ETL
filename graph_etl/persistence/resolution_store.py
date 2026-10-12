import json
import sqlite3
from collections.abc import Iterable
from typing import TYPE_CHECKING

from graph_etl.persistence.clock import timestamp

if TYPE_CHECKING:
    from graph_etl.persistence.database import Database
    from graph_etl.resolution.models import ResolutionDecision


_CANONICAL_SELECT = """
    SELECT ce.id, ce.identity_key, ce.title_key, ce.event_date_key,
           ce.source_identity_key, ce.url_key, v.snapshot_json
    FROM canonical_events AS ce
    JOIN canonical_event_versions AS v
      ON v.canonical_event_id = ce.id
     AND v.version_number = ce.current_version
"""


class ResolutionStore:
    def create_resolution_run(self: "Database", resolver_version: str) -> int:
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
        self: "Database",
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
                    ec.classification,
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

    def find_canonical_by_identity(
        self: "Database",
        identity_key: str | None,
    ) -> dict[str, object] | None:
        if identity_key is None:
            return None
        row = self.connection.execute(
            _CANONICAL_SELECT + " WHERE ce.identity_key = ?",
            (identity_key,),
        ).fetchone()
        return dict(row) if row is not None else None

    def find_canonical_by_source_identity(
        self: "Database",
        source_identity_key: str | None,
    ) -> dict[str, object] | None:
        if source_identity_key is None:
            return None
        row = self.connection.execute(
            _CANONICAL_SELECT + " WHERE ce.source_identity_key = ?",
            (source_identity_key,),
        ).fetchone()
        return dict(row) if row is not None else None

    def find_canonical_by_url(
        self: "Database",
        url_key: str | None,
    ) -> list[dict[str, object]]:
        if url_key is None:
            return []
        return [
            dict(row)
            for row in self.connection.execute(
                _CANONICAL_SELECT + " WHERE ce.url_key = ? ORDER BY ce.id",
                (url_key,),
            )
        ]

    def find_canonical_by_title(
        self: "Database",
        title_key: str | None,
    ) -> list[dict[str, object]]:
        if title_key is None:
            return []
        return [
            dict(row)
            for row in self.connection.execute(
                _CANONICAL_SELECT + " WHERE ce.title_key = ? ORDER BY ce.id",
                (title_key,),
            )
        ]

    def persist_resolution(
        self: "Database",
        run_id: int,
        decisions: Iterable["ResolutionDecision"],
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
                    SELECT id, status
                    FROM candidate_resolutions
                    WHERE candidate_id = ? AND resolver_version = ?
                    ORDER BY decision_sequence DESC
                    LIMIT 1
                    """,
                    (decision.candidate_id, resolver_version),
                ).fetchone()
                if previous is not None:
                    existing += 1
                    if previous["status"] in {"REVIEW_REQUIRED", "CONFLICT"}:
                        conflicts += 1
                    if previous["status"] in {"UNRESOLVED", "REVIEW_REQUIRED"}:
                        unresolved += 1
                    continue
                sequence_row = self.connection.execute(
                    """
                    SELECT COALESCE(MAX(decision_sequence), -1) + 1 AS next_sequence
                    FROM candidate_resolutions
                    WHERE candidate_id = ? AND resolver_version = ?
                    """,
                    (decision.candidate_id, resolver_version),
                ).fetchone()
                decision_sequence = int(sequence_row["next_sequence"])
                canonical_event_id = decision.canonical_event_id
                status = decision.status.value
                if status in {"MATCHED", "UPDATED"} and canonical_event_id is None:
                    raise RuntimeError("matched decision has no canonical event")
                if status == "NEW_EVENT_PROPOSED":
                    cursor = self.connection.execute(
                        """
                        INSERT INTO canonical_events(
                            identity_key, title_key, event_date_key,
                            source_identity_key, url_key, status,
                            current_version, created_at, updated_at
                        ) VALUES (?, ?, ?, ?, ?, 'PROPOSED', 1, ?, ?)
                        """,
                        (
                            decision.identity_key,
                            decision.title_key,
                            decision.snapshot.get("event_date"),
                            decision.source_identity_key,
                            decision.url_key,
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
                elif status in {"UPDATED", "REVIEW_REQUIRED", "CONFLICT"} and canonical_event_id is not None:
                    current = self.connection.execute(
                        "SELECT current_version FROM canonical_events WHERE id = ?",
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
                        SET current_version = ?,
                            updated_at = ?,
                            status = 'ACTIVE',
                            source_identity_key = COALESCE(source_identity_key, ?),
                            url_key = COALESCE(url_key, ?)
                        WHERE id = ?
                        """,
                        (
                            next_version,
                            now,
                            decision.source_identity_key,
                            decision.url_key,
                            canonical_event_id,
                        ),
                    )
                elif status == "MATCHED" and canonical_event_id is not None:
                    self.connection.execute(
                        """
                        UPDATE canonical_events
                        SET status = 'ACTIVE',
                            source_identity_key = COALESCE(source_identity_key, ?),
                            url_key = COALESCE(url_key, ?),
                            updated_at = ?
                        WHERE id = ?
                        """,
                        (
                            decision.source_identity_key,
                            decision.url_key,
                            now,
                            canonical_event_id,
                        ),
                    )

                decision_cursor = self.connection.execute(
                    """
                    INSERT INTO candidate_resolutions(
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
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                    """,
                    (
                        decision.candidate_id,
                        run_id,
                        resolver_version,
                        decision_sequence,
                        status,
                        canonical_event_id,
                        decision.identity_key,
                        decision.title_key,
                        decision.reason,
                        json.dumps(list(decision.conflict_fields), separators=(",", ":")),
                        now,
                    ),
                )
                decision_id = int(decision_cursor.lastrowid)
                if canonical_event_id is not None and status in {
                    "NEW_EVENT_PROPOSED",
                    "MATCHED",
                    "UPDATED",
                    "REVIEW_REQUIRED",
                    "CONFLICT",
                }:
                    active_link = self.connection.execute(
                        """
                        SELECT id
                        FROM canonical_event_candidates
                        WHERE canonical_event_id = ? AND candidate_id = ?
                          AND detached_at IS NULL
                        """,
                        (canonical_event_id, decision.candidate_id),
                    ).fetchone()
                    if active_link is None:
                        self.connection.execute(
                            """
                            INSERT INTO canonical_event_candidates(
                                canonical_event_id,
                                candidate_id,
                                resolution_decision_id,
                                linked_at,
                                link_reason,
                                link_status
                            ) VALUES (?, ?, ?, ?, ?, ?)
                            """,
                            (
                                canonical_event_id,
                                decision.candidate_id,
                                decision_id,
                                now,
                                decision.reason,
                                "REVIEW" if status in {"REVIEW_REQUIRED", "CONFLICT"} else "ACTIVE",
                            ),
                        )
                created += 1
                if status in {"REVIEW_REQUIRED", "CONFLICT"}:
                    conflicts += 1
                if status in {"UNRESOLVED", "REVIEW_REQUIRED"}:
                    unresolved += 1
        return created, existing, conflicts, unresolved

    def _insert_canonical_version(
        self: "Database",
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
        self: "Database",
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

    def fail_resolution_run(self: "Database", run_id: int, error: Exception) -> None:
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

    def restore_candidate_link(
        self: "Database",
        candidate_id: int,
        canonical_event_id: int,
        *,
        resolver_version: str,
        reason: str,
        review_note: str | None = None,
    ) -> dict[str, object]:
        run_id = self.create_resolution_run(resolver_version)
        now = timestamp(self.clock())
        try:
            with self.connection:
                previous = self.connection.execute(
                    """
                    SELECT id, identity_key, title_key
                    FROM candidate_resolutions
                    WHERE candidate_id = ? AND resolver_version = ?
                    ORDER BY decision_sequence DESC
                    LIMIT 1
                    """,
                    (candidate_id, resolver_version),
                ).fetchone()
                sequence = self.connection.execute(
                    """
                    SELECT COALESCE(MAX(decision_sequence), -1) + 1 AS next_sequence
                    FROM candidate_resolutions
                    WHERE candidate_id = ? AND resolver_version = ?
                    """,
                    (candidate_id, resolver_version),
                ).fetchone()["next_sequence"]
                cursor = self.connection.execute(
                    """
                    INSERT INTO candidate_resolutions(
                        candidate_id, resolution_run_id, resolver_version,
                        decision_sequence, status, canonical_event_id,
                        identity_key, title_key, reason, conflict_fields_json,
                        resolved_at, review_note, reviewed_at
                    ) VALUES (?, ?, ?, ?, 'MATCHED', ?, ?, ?, ?, '[]', ?, ?, ?)
                    """,
                    (
                        candidate_id,
                        run_id,
                        resolver_version,
                        sequence,
                        canonical_event_id,
                        previous["identity_key"] if previous is not None else None,
                        previous["title_key"] if previous is not None else None,
                        reason,
                        now,
                        review_note,
                        now,
                    ),
                )
                self.connection.execute(
                    """
                    INSERT OR IGNORE INTO canonical_event_candidates(
                        canonical_event_id, candidate_id, resolution_decision_id,
                        linked_at, link_reason, link_status
                    ) VALUES (?, ?, ?, ?, ?, 'ACTIVE')
                    """,
                    (
                        canonical_event_id,
                        candidate_id,
                        int(cursor.lastrowid),
                        now,
                        reason,
                    ),
                )
                self.connection.execute(
                    "UPDATE canonical_events SET status = 'ACTIVE', updated_at = ? WHERE id = ?",
                    (now, canonical_event_id),
                )
                if previous is not None:
                    self.connection.execute(
                        "UPDATE candidate_resolutions SET superseded_by = ? WHERE id = ?",
                        (int(cursor.lastrowid), previous["id"]),
                    )
            self.complete_resolution_run(
                run_id,
                candidates_processed=1,
                decisions_created=1,
                decisions_existing=0,
                conflicts=0,
                unresolved=0,
            )
        except Exception as error:
            self.fail_resolution_run(run_id, error)
            raise
        row = self.connection.execute(
            "SELECT * FROM candidate_resolutions WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()
        return dict(row)

    def change_candidate_link(
        self: "Database",
        candidate_id: int,
        *,
        resolver_version: str,
        action: str,
        reason: str,
        review_note: str | None = None,
    ) -> dict[str, object]:
        normalized_action = action.strip().lower()
        if normalized_action not in {"reject", "split"}:
            raise ValueError("action must be reject or split")
        status = "REJECTED" if normalized_action == "reject" else "REVIEW_REQUIRED"
        run_id = self.create_resolution_run(resolver_version)
        now = timestamp(self.clock())
        try:
            with self.connection:
                candidate = self.connection.execute(
                    """
                    SELECT source_external_id
                    FROM event_candidates
                    WHERE id = ?
                    """,
                    (candidate_id,),
                ).fetchone()
                if candidate is None:
                    raise RuntimeError("candidate does not exist")
                links = self.connection.execute(
                    """
                    SELECT id, canonical_event_id
                    FROM canonical_event_candidates
                    WHERE candidate_id = ? AND detached_at IS NULL
                    """,
                    (candidate_id,),
                ).fetchall()
                for link in links:
                    self.connection.execute(
                        """
                        UPDATE canonical_event_candidates
                        SET detached_at = ?, detach_reason = ?, link_status = 'DETACHED'
                        WHERE id = ?
                        """,
                        (now, reason, link["id"]),
                    )
                    current = self.connection.execute(
                        """
                        SELECT ce.current_version, ce.status, v.snapshot_json
                        FROM canonical_events AS ce
                        JOIN canonical_event_versions AS v
                          ON v.canonical_event_id = ce.id
                         AND v.version_number = ce.current_version
                        WHERE ce.id = ?
                        """,
                        (link["canonical_event_id"],),
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
                        (now, link["canonical_event_id"], int(current["current_version"])),
                    )
                    self._insert_canonical_version(
                        int(link["canonical_event_id"]),
                        next_version,
                        json.loads(current["snapshot_json"]),
                        f"{normalized_action}:{reason}",
                        run_id,
                        candidate_id,
                        now,
                    )
                    self.connection.execute(
                        """
                        UPDATE canonical_events
                        SET current_version = ?, updated_at = ?
                        WHERE id = ?
                        """,
                        (next_version, now, link["canonical_event_id"]),
                    )
                    remaining = self.connection.execute(
                        """
                        SELECT COUNT(*)
                        FROM canonical_event_candidates
                        WHERE canonical_event_id = ?
                          AND detached_at IS NULL
                        """,
                        (link["canonical_event_id"],),
                    ).fetchone()[0]
                    if remaining == 0:
                        self.connection.execute(
                            "UPDATE canonical_events SET status = ? WHERE id = ?",
                            (
                                "REJECTED" if normalized_action == "reject" else "SPLIT",
                                link["canonical_event_id"],
                            ),
                        )
                previous = self.connection.execute(
                    """
                    SELECT id, identity_key, title_key
                    FROM candidate_resolutions
                    WHERE candidate_id = ? AND resolver_version = ?
                    ORDER BY decision_sequence DESC
                    LIMIT 1
                    """,
                    (candidate_id, resolver_version),
                ).fetchone()
                sequence = self.connection.execute(
                    """
                    SELECT COALESCE(MAX(decision_sequence), -1) + 1 AS next_sequence
                    FROM candidate_resolutions
                    WHERE candidate_id = ? AND resolver_version = ?
                    """,
                    (candidate_id, resolver_version),
                ).fetchone()["next_sequence"]
                cursor = self.connection.execute(
                    """
                    INSERT INTO candidate_resolutions(
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
                        resolved_at,
                        review_note,
                        reviewed_at
                    ) VALUES (?, ?, ?, ?, ?, NULL, ?, ?, ?, '[]', ?, ?, ?)
                    """,
                    (
                        candidate_id,
                        run_id,
                        resolver_version,
                        sequence,
                        status,
                        previous["identity_key"] if previous is not None else None,
                        previous["title_key"] if previous is not None else None,
                        reason,
                        now,
                        review_note,
                        now,
                    ),
                )
                if previous is not None:
                    self.connection.execute(
                        "UPDATE candidate_resolutions SET superseded_by = ? WHERE id = ?",
                        (int(cursor.lastrowid), previous["id"]),
                    )
            self.complete_resolution_run(
                run_id,
                candidates_processed=1,
                decisions_created=1,
                decisions_existing=0,
                conflicts=1 if status == "REVIEW_REQUIRED" else 0,
                unresolved=1 if status == "REVIEW_REQUIRED" else 0,
            )
        except Exception as error:
            self.fail_resolution_run(run_id, error)
            raise
        row = self.connection.execute(
            "SELECT * FROM candidate_resolutions WHERE id = ?",
            (int(cursor.lastrowid),),
        ).fetchone()
        return dict(row)

    def query_resolutions(
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
            conditions.append("cr.status = ?")
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
                SELECT cr.*, s.source_key, ec.source_external_id
                FROM candidate_resolutions AS cr
                JOIN event_candidates AS ec ON ec.id = cr.candidate_id
                JOIN observations AS o ON o.id = ec.observation_id
                JOIN sources AS s ON s.id = o.source_id
                WHERE {' AND '.join(conditions)}
                ORDER BY cr.id
                {limit_clause}
                """,
                parameters,
            )
        ]

    def query_canonical_events(
        self: "Database",
        *,
        limit: int | None = None,
    ) -> list[dict[str, object]]:
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
                   ce.source_identity_key, ce.url_key, ce.status,
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
                    SELECT candidate_id, resolver_version, decision_sequence,
                           status, reason, conflict_fields_json, resolved_at
                    FROM candidate_resolutions
                    WHERE canonical_event_id = ?
                    ORDER BY id
                    """,
                    (row["id"],),
                )
            ]
            event["members"] = [
                dict(member)
                for member in self.connection.execute(
                    """
                    SELECT candidate_id, link_status, link_reason,
                           linked_at, detached_at, detach_reason
                    FROM canonical_event_candidates
                    WHERE canonical_event_id = ?
                    ORDER BY id
                    """,
                    (row["id"],),
                )
            ]
            events.append(event)
        return events