from http.client import IncompleteRead
from unittest.mock import Mock
from urllib.request import Request
import unittest

from graph_etl.domain import Source, SourceKind
from graph_etl.ingestion.http import HttpFeedClient, _SameOriginRedirectHandler
from graph_etl.ingestion.models import SourceFetchError


class FakeResponse:
    def __init__(self, content: bytes) -> None:
        self.content = content
        self.headers = {"Content-Type": "application/rss+xml"}

    def __enter__(self) -> "FakeResponse":
        return self

    def __exit__(self, *_: object) -> None:
        return None

    def read(self, amount: int) -> bytes:
        return self.content[:amount]


class HttpFeedClientTests(unittest.TestCase):
    def test_rejects_credentialed_uri_before_network_access(self) -> None:
        client = HttpFeedClient()
        client.opener.open = Mock()

        with self.assertRaises(SourceFetchError) as context:
            client.fetch("https://user:secret@example.org/feed.xml", timeout=1.0)

        self.assertFalse(context.exception.retryable)
        client.opener.open.assert_not_called()
        with self.assertRaises(ValueError):
            Source(
                key="unsafe",
                name="Unsafe",
                kind=SourceKind.FEED,
                uri="https://user:secret@example.org/feed.xml",
            )

    def test_bounds_response_size(self) -> None:
        client = HttpFeedClient(max_bytes=4)
        client.opener.open = Mock(return_value=FakeResponse(b"12345"))

        with self.assertRaises(SourceFetchError) as context:
            client.fetch("https://example.org/feed.xml", timeout=1.0)

        self.assertFalse(context.exception.retryable)

    def test_truncated_response_is_retryable(self) -> None:
        client = HttpFeedClient()
        client.opener.open = Mock(side_effect=IncompleteRead(b"partial"))

        with self.assertRaises(SourceFetchError) as context:
            client.fetch("https://example.org/feed.xml", timeout=1.0)

        self.assertTrue(context.exception.retryable)

    def test_redirect_remains_on_original_origin(self) -> None:
        handler = _SameOriginRedirectHandler()
        request = Request("https://example.org/feed.xml")

        redirected = handler.redirect_request(
            request,
            None,
            302,
            "Found",
            {},
            "https://example.org:443/moved.xml",
        )
        self.assertEqual("https://example.org:443/moved.xml", redirected.full_url)
        with self.assertRaises(SourceFetchError):
            handler.redirect_request(
                request,
                None,
                302,
                "Found",
                {},
                "https://other.example/feed.xml",
            )


if __name__ == "__main__":
    unittest.main()
