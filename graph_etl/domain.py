from dataclasses import dataclass
from enum import StrEnum
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


@dataclass(frozen=True, slots=True)
class Source:
    key: str
    name: str
    kind: SourceKind
    uri: str
    id: int | None = None

    def __post_init__(self) -> None:
        validate_source_uri(self.uri)