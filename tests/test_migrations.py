import sqlite3
import tempfile
from pathlib import Path
import unittest

from graph_etl.persistence.database import Database
from graph_etl.persistence.migrations import (
    MIGRATION_1,
    MIGRATION_2,
    MIGRATION_3,
    MIGRATIONS,
    SCHEMA_VERSION,
)


class MigrationTests(unittest.TestCase):
    def test_schema_version_matches_migration_table(self) -> None:
        self.assertEqual(SCHEMA_VERSION, max(MIGRATIONS))

    def test_version_one_database_is_upgraded_to_current_schema(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.sqlite3"
            connection = sqlite3.connect(path)
            connection.executescript(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                );
                """
            )
            connection.executescript(MIGRATION_1)
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (1, '2026-01-01T00:00:00Z')"
            )
            source_id = connection.execute(
                """
                INSERT INTO sources(source_key, name, kind, uri, created_at, updated_at)
                VALUES (?, ?, ?, ?, ?, ?)
                """,
                (
                    "feed:legacy",
                    "Legacy Feed",
                    "feed",
                    "https://example.org/legacy.xml",
                    "2026-01-01T00:00:00Z",
                    "2026-01-01T00:00:00Z",
                ),
            ).lastrowid
            document_id = connection.execute(
                """
                INSERT INTO raw_documents(
                    source_id, content_sha256, payload, fetched_at
                ) VALUES (?, ?, ?, ?)
                """,
                (source_id, "a" * 64, b"<rss/>", "2026-01-01T00:00:00Z"),
            ).lastrowid
            observation_id = connection.execute(
                """
                INSERT INTO observations(
                    raw_document_id, source_id, external_id, content_sha256,
                    parser_version, entry_index, observed_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (
                    document_id,
                    source_id,
                    "legacy-1",
                    "b" * 64,
                    "1",
                    0,
                    "2026-01-01T00:00:00Z",
                ),
            ).lastrowid
            connection.execute(
                """
                INSERT INTO observation_fields(
                    observation_id, field_name, field_ordinal, raw_value,
                    is_present, locator, value_json
                ) VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (observation_id, "title", 0, "Legacy", 1, "legacy:title", '"Legacy"'),
            )
            connection.commit()
            connection.close()

            with Database(path) as database:
                versions = [
                    row["version"]
                    for row in database.connection.execute(
                        "SELECT version FROM schema_migrations ORDER BY version"
                    )
                ]
                tables = {
                    row["name"]
                    for row in database.connection.execute(
                        "SELECT name FROM sqlite_master WHERE type = 'table'"
                    )
                }
                membership_count = database.connection.execute(
                    "SELECT COUNT(*) FROM document_observations"
                ).fetchone()[0]
                columns = {
                    row["name"]
                    for row in database.connection.execute(
                        "PRAGMA table_info(event_candidates)"
                    )
                }

            self.assertEqual(list(range(1, SCHEMA_VERSION + 1)), versions)
            self.assertEqual(1, membership_count)
            self.assertIn("document_observations", tables)
            self.assertIn("source_uri_history", tables)
            self.assertIn("extraction_runs", tables)
            self.assertIn("event_candidates", tables)
            self.assertIn("candidate_fields", tables)
            self.assertIn("resolution_runs", tables)
            self.assertIn("canonical_events", tables)
            self.assertIn("canonical_event_versions", tables)
            self.assertIn("candidate_resolutions", tables)
            self.assertIn("canonical_event_candidates", tables)
            self.assertIn("extraction_observation_outcomes", tables)
            self.assertIn("classification", columns)
            self.assertIn("candidate_slot_key", columns)

    def test_redundant_value_json_column_is_dropped(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy-value-json.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            connection.executescript(MIGRATION_1)
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (1, '2026-01-01T00:00:00Z')"
            )
            connection.commit()
            connection.close()

            with Database(path) as database:
                columns = {
                    row["name"]
                    for row in database.connection.execute(
                        "PRAGMA table_info(observation_fields)"
                    )
                }

            self.assertNotIn("value_json", columns)
            self.assertIn("raw_value", columns)


    def test_version_three_identity_columns_are_backfilled(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy-v3.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            for script in (MIGRATION_1, MIGRATION_2, MIGRATION_3):
                connection.executescript(script)
            for version in (1, 2, 3):
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (version, "2026-01-01T00:00:00Z"),
                )
            source_id = connection.execute(
                """
                INSERT INTO sources(source_key, name, kind, uri, created_at, updated_at)
                VALUES ('feed:a', 'Feed A', 'feed', 'https://example.org/feed', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            ).lastrowid
            document_id = connection.execute(
                """
                INSERT INTO raw_documents(source_id, content_sha256, payload, fetched_at)
                VALUES (?, ?, ?, '2026-01-01T00:00:00Z')
                """,
                (source_id, "a" * 64, b"<rss/>"),
            ).lastrowid
            observation_id = connection.execute(
                """
                INSERT INTO observations(
                    raw_document_id, source_id, external_id, content_sha256,
                    parser_version, entry_index, observed_at
                ) VALUES (?, ?, ?, ?, '1', 0, '2026-01-01T00:00:00Z')
                """,
                (document_id, source_id, "event-1", "b" * 64),
            ).lastrowid
            extraction_run_id = connection.execute(
                """
                INSERT INTO extraction_runs(
                    extraction_version, normalization_version, status, started_at, completed_at
                ) VALUES ('1', '1', 'succeeded', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            ).lastrowid
            candidate_id = connection.execute(
                """
                INSERT INTO event_candidates(
                    observation_id, extraction_run_id, extraction_version,
                    normalization_version, candidate_ordinal, candidate_key,
                    source_external_id, created_at
                ) VALUES (?, ?, '1', '1', 0, ?, ?, '2026-01-01T00:00:00Z')
                """,
                (observation_id, extraction_run_id, "c" * 64, "event-1"),
            ).lastrowid
            resolution_run_id = connection.execute(
                """
                INSERT INTO resolution_runs(
                    resolver_version, status, started_at, completed_at
                ) VALUES ('1', 'succeeded', '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            ).lastrowid
            canonical_event_id = connection.execute(
                """
                INSERT INTO canonical_events(
                    identity_key, title_key, event_date_key, current_version,
                    created_at, updated_at
                ) VALUES ('old-identity', 'Raw Title!', '2026-11-05', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z')
                """
            ).lastrowid
            snapshot = (
                '{"title":"PyData London!","event_date":"2026-11-05",'
                '"url":"https://WWW.Example.org/event/?x=1"}'
            )
            connection.execute(
                """
                INSERT INTO canonical_event_versions(
                    canonical_event_id, version_number, snapshot_json, valid_from,
                    change_reason, resolution_run_id, candidate_id
                ) VALUES (?, 1, ?, '2026-01-01T00:00:00Z', 'created', ?, ?)
                """,
                (canonical_event_id, snapshot, resolution_run_id, candidate_id),
            )
            connection.execute(
                """
                INSERT INTO candidate_resolutions(
                    candidate_id, resolution_run_id, resolver_version, status,
                    canonical_event_id, identity_key, title_key, reason,
                    conflict_fields_json, resolved_at
                ) VALUES (?, ?, '1', 'created', ?, 'old-identity', 'Raw Title!', 'created', '[]', '2026-01-01T00:00:00Z')
                """,
                (candidate_id, resolution_run_id, canonical_event_id),
            )
            connection.commit()
            connection.close()

            with Database(path) as database:
                event = database.connection.execute(
                    "SELECT * FROM canonical_events WHERE id = ?",
                    (canonical_event_id,),
                ).fetchone()
                member_count = database.connection.execute(
                    "SELECT COUNT(*) FROM canonical_event_candidates"
                ).fetchone()[0]

            self.assertNotEqual("old-identity", event["identity_key"])
            self.assertEqual("pydata london", event["title_key"])
            self.assertEqual("example.org/event", event["url_key"])
            self.assertIsNotNone(event["source_identity_key"])
            self.assertEqual(1, member_count)


    def test_partially_applied_version_four_migration_resumes(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "partial-v4.sqlite3"
            connection = sqlite3.connect(path)
            connection.execute(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            for script in (MIGRATION_1, MIGRATION_2, MIGRATION_3):
                connection.executescript(script)
            for version in (1, 2, 3):
                connection.execute(
                    "INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",
                    (version, "2026-01-01T00:00:00Z"),
                )
            connection.execute(
                "ALTER TABLE extraction_runs ADD COLUMN observations_with_errors INTEGER DEFAULT 0"
            )
            connection.commit()
            connection.close()

            with Database(path) as database:
                versions = [
                    row["version"]
                    for row in database.connection.execute(
                        "SELECT version FROM schema_migrations ORDER BY version"
                    )
                ]
                columns = {
                    row["name"]
                    for row in database.connection.execute(
                        "PRAGMA table_info(extraction_runs)"
                    )
                }

            self.assertEqual(list(range(1, SCHEMA_VERSION + 1)), versions)
            self.assertIn("observations_with_errors", columns)
            self.assertIn("observations_skipped", columns)


if __name__ == "__main__":
    unittest.main()
