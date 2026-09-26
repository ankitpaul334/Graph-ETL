from contextlib import redirect_stdout
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from io import StringIO
import json
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


if __name__ == "__main__":
    unittest.main()
