from http.client import HTTPException
from typing import Protocol
from urllib.error import HTTPError, URLError
from urllib.parse import urljoin, urlparse
from urllib.request import HTTPRedirectHandler, Request, build_opener

from graph_etl.domain import validate_source_uri

from .models import FetchedDocument, SourceFetchError


class FeedClient(Protocol):
    def fetch(self, uri: str, *, timeout: float) -> FetchedDocument:
        pass


def _origin(uri: str) -> tuple[str, str, int]:
    parsed = urlparse(uri)
    try:
        port = parsed.port
    except ValueError as error:
        raise SourceFetchError("feed URI has an invalid port", retryable=False) from error
    if port is None:
        port = 443 if parsed.scheme == "https" else 80
    return parsed.scheme, (parsed.hostname or "").lower(), port


class _SameOriginRedirectHandler(HTTPRedirectHandler):
    def redirect_request(
        self,
        request: Request,
        file_pointer: object,
        code: int,
        message: str,
        headers: object,
        new_url: str,
    ) -> Request | None:
        target = urljoin(request.full_url, new_url)
        try:
            validate_source_uri(target)
        except ValueError as error:
            raise SourceFetchError(str(error), retryable=False) from error
        if _origin(request.full_url) != _origin(target):
            raise SourceFetchError(
                "feed redirect must remain on the same origin",
                retryable=False,
            )
        return super().redirect_request(
            request,
            file_pointer,
            code,
            message,
            headers,
            target,
        )


class HttpFeedClient:
    def __init__(self, *, max_bytes: int = 5_000_000) -> None:
        if max_bytes < 1:
            raise ValueError("max_bytes must be positive")
        self.max_bytes = max_bytes
        self.opener = build_opener(_SameOriginRedirectHandler())

    def fetch(self, uri: str, *, timeout: float) -> FetchedDocument:
        try:
            validate_source_uri(uri)
        except ValueError as error:
            raise SourceFetchError(str(error), retryable=False) from error
        if timeout <= 0:
            raise SourceFetchError("timeout must be positive", retryable=False)

        request = Request(
            uri,
            headers={
                "Accept": "application/atom+xml, application/rss+xml, application/xml;q=0.9, */*;q=0.1",
                "User-Agent": "graph-etl/0.1",
            },
        )
        try:
            with self.opener.open(request, timeout=timeout) as response:
                content = response.read(self.max_bytes + 1)
                if len(content) > self.max_bytes:
                    raise SourceFetchError(
                        "feed response exceeds configured size limit",
                        retryable=False,
                    )
                return FetchedDocument(
                    content=content,
                    content_type=response.headers.get("Content-Type"),
                    etag=response.headers.get("ETag"),
                    last_modified=response.headers.get("Last-Modified"),
                )
        except HTTPError as error:
            retryable = error.code == 429 or 500 <= error.code < 600
            raise SourceFetchError(
                f"feed request failed with HTTP {error.code}",
                retryable=retryable,
            ) from error
        except HTTPException as error:
            raise SourceFetchError(
                "feed response was truncated or malformed",
                retryable=True,
            ) from error
        except (URLError, TimeoutError, OSError) as error:
            raise SourceFetchError(
                "feed request failed due to a network error",
                retryable=True,
            ) from error
        except ValueError as error:
            raise SourceFetchError(
                "feed URI is invalid",
                retryable=False,
            ) from error
