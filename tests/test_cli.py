from contextlib import redirect_stderr, redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
import json
import sqlite3
from pathlib import Path
import tempfile
import threading
import unittest

from graph_etl.cli import main
from graph_etl.persistence.database import Database


FIXTURES = Path(__file__).parent / "fixtures"


class CliIngestionTests(unittest.TestCase):
    def test_ingests_feed_through_cli(self) -> None:
        payload = (FIXTURES / "rss.xml").read_bytes()

        class FeedHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/rss+xml")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        server = ThreadingHTTPServer(("127.0.0.1", 0), FeedHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary_directory:
                database_path = Path(temporary_directory) / "graph_etl.sqlite3"
                output = StringIO()
                with redirect_stdout(output):
                    exit_code = main(
                        [
                            "ingest-rss",
                            "--source-key",
                            "rss:cli-test",
                            "--name",
                            "CLI Test Feed",
                            "--url",
                            f"http://127.0.0.1:{server.server_port}/feed.xml",
                            "--database",
                            str(database_path),
                        ]
                    )

                self.assertEqual(0, exit_code)
                result = json.loads(output.getvalue())
                self.assertEqual(1, result["observations_inserted"])
                with Database(database_path) as database:
                    self.assertEqual(
                        1,
                        database.connection.execute(
                            "SELECT COUNT(*) FROM raw_documents"
                        ).fetchone()[0],
                    )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_extracts_and_queries_candidates_through_cli(self) -> None:
        payload = (FIXTURES / "rss.xml").read_bytes()

        class FeedHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                self.send_response(200)
                self.send_header("Content-Type", "application/rss+xml")
                self.send_header("Content-Length", str(len(payload)))
                self.end_headers()
                self.wfile.write(payload)

            def log_message(self, format: str, *args: object) -> None:
                del format, args

        server = ThreadingHTTPServer(("127.0.0.1", 0), FeedHandler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary_directory:
                database_path = Path(temporary_directory) / "graph_etl.sqlite3"
                ingest_output = StringIO()
                with redirect_stdout(ingest_output):
                    ingest_code = main(
                        [
                            "ingest-rss",
                            "--source-key",
                            "rss:cli-extract-test",
                            "--name",
                            "CLI Extract Test Feed",
                            "--url",
                            f"http://127.0.0.1:{server.server_port}/feed.xml",
                            "--database",
                            str(database_path),
                        ]
                    )
                extract_output = StringIO()
                with redirect_stdout(extract_output):
                    extract_code = main(
                        [
                            "extract",
                            "--database",
                            str(database_path),
                            "--source-key",
                            "rss:cli-extract-test",
                        ]
                    )
                candidates_output = StringIO()
                with redirect_stdout(candidates_output):
                    candidates_code = main(
                        [
                            "candidates",
                            "--database",
                            str(database_path),
                            "--source-key",
                            "rss:cli-extract-test",
                        ]
                    )

                self.assertEqual(0, ingest_code)
                self.assertEqual(0, extract_code)
                self.assertEqual(0, candidates_code)
                extraction = json.loads(extract_output.getvalue())
                self.assertEqual(1, extraction["candidates_created"])
                candidates = json.loads(candidates_output.getvalue())["candidates"]
                self.assertEqual(1, len(candidates))
                fields = {
                    field["field_name"]: field for field in candidates[0]["fields"]
                }
                self.assertEqual("missing", fields["event_date"]["normalization_status"])
                self.assertEqual(
                    "normalized",
                    fields["publication_timestamp"]["normalization_status"],
                )
                resolve_output = StringIO()
                with redirect_stdout(resolve_output):
                    resolve_code = main(
                        [
                            "resolve",
                            "--database",
                            str(database_path),
                            "--source-key",
                            "rss:cli-extract-test",
                        ]
                    )
                canonical_output = StringIO()
                with redirect_stdout(canonical_output):
                    canonical_code = main(
                        [
                            "canonical-events",
                            "--database",
                            str(database_path),
                        ]
                    )
                self.assertEqual(0, resolve_code)
                self.assertEqual(0, canonical_code)
                resolution = json.loads(resolve_output.getvalue())
                self.assertEqual(1, resolution["unresolved"])
                self.assertEqual(
                    0,
                    len(json.loads(canonical_output.getvalue())["events"]),
                )
        finally:
            server.shutdown()
            server.server_close()
            thread.join(timeout=5)

    def test_schema_version_error_is_reported_as_json(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "future.sqlite3"
            connection = sqlite3.connect(database_path)
            connection.execute(
                """
                CREATE TABLE schema_migrations (
                    version INTEGER PRIMARY KEY,
                    applied_at TEXT NOT NULL
                )
                """
            )
            connection.execute(
                "INSERT INTO schema_migrations(version, applied_at) VALUES (99, '2026-01-01T00:00:00Z')"
            )
            connection.commit()
            connection.close()
            error_output = StringIO()

            with redirect_stderr(error_output):
                exit_code = main(
                    [
                        "candidates",
                        "--database",
                        str(database_path),
                    ]
                )

            self.assertEqual(1, exit_code)
            self.assertIn("newer than supported", error_output.getvalue())

    def test_candidates_rejects_missing_database(self) -> None:
        with tempfile.TemporaryDirectory() as temporary_directory:
            database_path = Path(temporary_directory) / "missing.sqlite3"
            error_output = StringIO()
            with redirect_stderr(error_output):
                exit_code = main(
                    [
                        "candidates",
                        "--database",
                        str(database_path),
                    ]
                )

            self.assertEqual(1, exit_code)
            self.assertIn("database does not exist", error_output.getvalue())
            self.assertFalse(database_path.exists())


if __name__ == "__main__":
    unittest.main()
