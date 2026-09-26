from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Mapping
from urllib.parse import urlparse


def validate_source_uri(uri: str) -> None:
    parsed = urlparse(uri)
    if parsed.scheme not in {"http", "https"}:
        raise ValueError("source URI must use HTTP or HTTPS")
    if not parsed.netloc:
        raise ValueError("source URI must include a host")
    if parsed.username is not None or parsed.password is not None:
        raise ValueError("source URI must not contain credentials")


class SourceKind(StrEnum):
    FEED = "feed"
    STRUCTURED_API = "structured_api"
    COMMUNITY = "community"


@dataclass(frozen=True, slots=True)
class Source:
    key: str
    name: str
    kind: SourceKind
    uri: str
    id: int | None = None

    def __post_init__(self) -> None:
        validate_source_uri(self.uri)


@dataclass(frozen=True, slots=True)
class Observation:
    source_id: int
    document_id: int
    external_id: str
    content_sha256: str
    parser_version: str
    entry_index: int
    observed_at: str
    id: int | None = None


@dataclass(frozen=True, slots=True)
class EventCandidate:
    source_id: int
    observation_id: int
    external_id: str
    extraction_version: str
    fields: Mapping[str, Any]
    id: int | None = None


@dataclass(frozen=True, slots=True)
class CanonicalEvent:
    created_at: str
    updated_at: str
    id: int | None = None


@dataclass(frozen=True, slots=True)
class EventVersion:
    canonical_event_id: int
    version_number: int
    snapshot: Mapping[str, Any]
    valid_from: str
    change_reason: str
    valid_to: str | None = None
    id: int | None = None
