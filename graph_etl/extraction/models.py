from dataclasses import dataclass
from enum import StrEnum


EXTRACTION_VERSION = "1"
NORMALIZATION_VERSION = "1"


class NormalizationStatus(StrEnum):
    MISSING = "missing"
    NORMALIZED = "normalized"
    DATE_ONLY = "date_only"
    TIMEZONE_AMBIGUOUS = "timezone_ambiguous"
    INVALID = "invalid"
    RAW_ONLY = "raw_only"


@dataclass(frozen=True, slots=True)
class NormalizedDate:
    status: NormalizationStatus
    normalized_value: str | None
    source_timezone: str | None
    error: str | None


@dataclass(frozen=True, slots=True)
class StoredObservationField:
    name: str
    field_ordinal: int
    raw_value: str | None
    locator: str
    is_present: bool


@dataclass(frozen=True, slots=True)
class StoredObservation:
    id: int
    source_id: int
    external_id: str
    content_sha256: str
    fields: tuple[StoredObservationField, ...]


@dataclass(frozen=True, slots=True)
class CandidateField:
    field_name: str
    field_ordinal: int
    raw_value: str | None
    normalized_value: str | None
    normalization_status: str
    source_timezone: str | None
    normalization_error: str | None
    source_observation_id: int | None
    source_field_name: str | None
    source_field_ordinal: int | None
    locator: str


@dataclass(frozen=True, slots=True)
class CandidateDraft:
    observation_id: int
    candidate_ordinal: int
    candidate_key: str
    external_id: str
    fields: tuple[CandidateField, ...]


@dataclass(frozen=True, slots=True)
class ExtractionRun:
    id: int
    extraction_version: str
    normalization_version: str
    status: str
    started_at: str
    completed_at: str | None
    observations_processed: int
    candidates_created: int
    candidates_existing: int
    error: str | None


@dataclass(frozen=True, slots=True)
class ExtractionRunResult:
    run_id: int
    extraction_version: str
    normalization_version: str
    observations_processed: int
    candidates_created: int
    candidates_existing: int
