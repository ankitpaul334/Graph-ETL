from dataclasses import dataclass


class IngestionError(RuntimeError):
    pass


class SourceFetchError(IngestionError):
    def __init__(self, message: str, *, retryable: bool) -> None:
        super().__init__(message)
        self.retryable = retryable


class FeedParseError(IngestionError):
    pass


@dataclass(frozen=True, slots=True)
class FetchedDocument:
    content: bytes
    content_type: str | None = None
    etag: str | None = None
    last_modified: str | None = None


@dataclass(frozen=True, slots=True)
class FieldValue:
    name: str
    raw_value: str | None
    locator: str

    @property
    def is_present(self) -> bool:
        return self.raw_value is not None


@dataclass(frozen=True, slots=True)
class ParsedEntry:
    external_id: str
    content_sha256: str
    index: int
    fields: tuple[FieldValue, ...]


@dataclass(frozen=True, slots=True)
class ParsedFeed:
    feed_format: str
    title: str | None
    parser_version: str
    entries: tuple[ParsedEntry, ...]


@dataclass(frozen=True, slots=True)
class IngestionResult:
    source_key: str
    run_id: int
    document_id: int
    attempts: int
    raw_document_created: bool
    observations_inserted: int
    observations_existing: int
