import sqlite3
import tempfile
from pathlib import Path
import unittest

from graph_etl.persistence.database import MIGRATION_1, MIGRATIONS, SCHEMA_VERSION, Database


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


if __name__ == "__main__":
    unittest.main()
