from dataclasses import dataclass
from enum import StrEnum
from typing import Mapping


RESOLVER_VERSION = "1"


class ResolutionStatus(StrEnum):
    CREATED = "created"
    MATCHED = "matched"
    UPDATED = "updated"
    CONFLICT = "conflict"
    AMBIGUOUS = "ambiguous"
    UNMATCHED = "unmatched"


@dataclass(frozen=True, slots=True)
class CandidateFieldValue:
    name: str
    raw_value: str | None
    normalized_value: str | None
    normalization_status: str
    source_timezone: str | None
    locator: str


@dataclass(frozen=True, slots=True)
class ResolutionCandidate:
    id: int
    source_key: str
    external_id: str
    fields: tuple[CandidateFieldValue, ...]


@dataclass(frozen=True, slots=True)
class CanonicalSnapshot:
    id: int
    identity_key: str
    title_key: str
    snapshot: Mapping[str, str | None]


@dataclass(frozen=True, slots=True)
class CandidateAnalysis:
    snapshot: Mapping[str, str | None]
    identity_key: str | None
    title_key: str | None
    event_date_key: str | None
    missing_fields: tuple[str, ...]
    ambiguous_fields: tuple[str, ...]


@dataclass(frozen=True, slots=True)
class ResolutionDecision:
    status: ResolutionStatus
    candidate_id: int
    canonical_event_id: int | None
    identity_key: str | None
    title_key: str | None
    snapshot: Mapping[str, str | None]
    reason: str
    conflict_fields: tuple[str, ...] = ()


@dataclass(frozen=True, slots=True)
class ResolutionRun:
    id: int
    resolver_version: str
    status: str
    started_at: str
    completed_at: str | None
    candidates_processed: int
    decisions_created: int
    decisions_existing: int
    conflicts: int
    unresolved: int
    error: str | None


@dataclass(frozen=True, slots=True)
class ResolutionRunResult:
    run_id: int
    resolver_version: str
    candidates_processed: int
    decisions_created: int
    decisions_existing: int
    conflicts: int
    unresolved: int
