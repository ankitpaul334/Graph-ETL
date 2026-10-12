from collections.abc import Iterator
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import threading

from graph_etl.ingestion.models import FetchedDocument, SourceFetchError


class StubClient:
    """Feed client that replays fixed outcomes without network access."""

    def __init__(
        self,
        *outcomes: bytes | FetchedDocument | SourceFetchError,
        content_type: str | None = "application/rss+xml",
    ) -> None:
        self.outcomes = [
            FetchedDocument(outcome, content_type) if isinstance(outcome, bytes) else outcome
            for outcome in outcomes
        ]

    def fetch(self, uri: str, *, timeout: float) -> FetchedDocument:
        del uri, timeout
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, SourceFetchError):
            raise outcome
        return outcome


@contextmanager
def loopback_feed_server(
    payload: bytes,
    *,
    content_type: str = "application/rss+xml",
) -> Iterator[str]:
    class FeedHandler(BaseHTTPRequestHandler):
        def do_GET(self) -> None:
            self.send_response(200)
            self.send_header("Content-Type", content_type)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, format: str, *args: object) -> None:
            del format, args

    server = ThreadingHTTPServer(("127.0.0.1", 0), FeedHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        yield f"http://127.0.0.1:{server.server_port}/feed.xml"
    finally:
        server.shutdown()
        server.server_close()
        thread.join(timeout=5)