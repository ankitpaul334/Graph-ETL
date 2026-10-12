import hashlib
import json

from .dates import normalize_date
from .models import (
    CandidateDraft,
    CandidateField,
    EventClassification,
    NormalizationStatus,
    StoredObservation,
    StoredObservationField,
    EXTRACTION_VERSION,
    NORMALIZATION_VERSION,
)


_FIELD_ALIASES = {
    "title": ("title",),
    "event_date": ("event_date", "start_date", "event_start", "starts_at", "start_at"),
    "publication_timestamp": ("published_at", "publication_date"),
    "publication_updated_timestamp": ("updated_at", "publication_updated_at"),
    "url": ("link", "url"),
    "venue": ("venue", "location"),
    "cancellation_status": ("cancellation_status", "cancellation"),
    "source_status": ("status",),
    "summary": ("summary",),
    "description": ("content", "description"),
    "classification": (
        "classification",
        "event_classification",
        "event_type",
    ),
    "source_identifier": ("identifier", "guid"),
    "author": ("authors", "author"),
}

_FIELD_ORDER = tuple(_FIELD_ALIASES)
_DATE_FIELDS = {"event_date", "publication_timestamp", "publication_updated_timestamp"}


class EventCandidateExtractor:
    def __init__(
        self,
        *,
        extraction_version: str = EXTRACTION_VERSION,
        normalization_version: str = NORMALIZATION_VERSION,
    ) -> None:
        if not extraction_version or not normalization_version:
            raise ValueError("extraction versions cannot be empty")
        self.extraction_version = extraction_version
        self.normalization_version = normalization_version

    def extract(self, observation: StoredObservation) -> tuple[CandidateDraft, ...]:
        event_slots = self._event_slots(observation.fields)
        classification, classification_reason = self._classify(observation.fields)
        drafts: list[CandidateDraft] = []
        for candidate_ordinal, event_field in enumerate(event_slots):
            fields = self._candidate_fields(observation.fields, event_field)
            slot_key = self._slot_key(event_field)
            drafts.append(
                CandidateDraft(
                    observation_id=observation.id,
                    candidate_ordinal=candidate_ordinal,
                    candidate_slot_key=slot_key,
                    candidate_key=self._candidate_key(
                        observation.id,
                        slot_key,
                    ),
                    external_id=observation.external_id,
                    classification=classification,
                    classification_reason=classification_reason,
                    fields=fields,
                )
            )
        return tuple(drafts)

    def _classify(
        self,
        fields: tuple[StoredObservationField, ...],
    ) -> tuple[str, str]:
        classification_values = {
            (field.raw_value or "").strip().lower()
            for field in fields
            if field.name in _FIELD_ALIASES["classification"]
            and field.is_present
            and (field.raw_value or "").strip()
        }
        if classification_values & {
            "not_event",
            "non-event",
            "article",
            "blog",
            "news",
            "product",
            "documentation",
        }:
            return EventClassification.NOT_EVENT.value, "source marked item as non-event"
        if classification_values & {
            "event",
            "conference",
            "meetup",
            "webinar",
            "workshop",
        }:
            return (
                EventClassification.EVENT_CONFIRMED_BY_SOURCE.value,
                "source marked item as an event",
            )
        event_date_values = [
            field
            for field in fields
            if field.name in _FIELD_ALIASES["event_date"]
            and field.is_present
            and field.raw_value
            and field.raw_value.strip()
            and field.name
        ]
        if event_date_values:
            return (
                EventClassification.EVENT_CONFIRMED_BY_SOURCE.value,
                "source supplied an explicit event date",
            )
        return (
            EventClassification.EVENT_POSSIBLE.value,
            "no explicit non-event or event classification",
        )

    @staticmethod
    def _slot_key(event_field: StoredObservationField | None) -> str:
        if event_field is None:
            return "no-event-date"
        payload = json.dumps(
            {
                "field_name": event_field.name,
                "locator": event_field.locator,
                "raw_value": event_field.raw_value,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()

    def _event_slots(
        self,
        fields: tuple[StoredObservationField, ...],
    ) -> list[StoredObservationField | None]:
        aliases = _FIELD_ALIASES["event_date"]
        event_fields = sorted(
            (field for field in fields if field.name in aliases),
            key=lambda field: (
                field.field_ordinal,
                field.locator,
                field.name,
                field.raw_value or "",
            ),
        )
        slots: list[StoredObservationField] = []
        seen_values: set[str] = set()
        for field in event_fields:
            if not field.is_present:
                continue
            value = (field.raw_value or "").strip()
            if not value or value in seen_values:
                continue
            seen_values.add(value)
            slots.append(field)
        if slots:
            return slots
        if event_fields:
            return [event_fields[0]]
        return [None]

    def _candidate_fields(
        self,
        fields: tuple[StoredObservationField, ...],
        event_field: StoredObservationField | None,
    ) -> tuple[CandidateField, ...]:
        output: list[CandidateField] = []
        for field_name in _FIELD_ORDER:
            aliases = _FIELD_ALIASES[field_name]
            if field_name == "event_date":
                sources = [event_field] if event_field is not None else []
            else:
                sources = [field for field in fields if field.name in aliases]
            if not sources:
                output.append(
                    CandidateField(
                        field_name=field_name,
                        field_ordinal=0,
                        raw_value=None,
                        normalized_value=None,
                        normalization_status=NormalizationStatus.MISSING.value,
                        source_timezone=None,
                        normalization_error=None,
                        source_observation_id=None,
                        source_field_name=None,
                        source_field_ordinal=None,
                        locator="",
                    )
                )
                continue
            for field_ordinal, source in enumerate(sources):
                output.append(self._normalize_field(field_name, field_ordinal, source))
        return tuple(output)

    def _normalize_field(
        self,
        field_name: str,
        field_ordinal: int,
        source: StoredObservationField,
    ) -> CandidateField:
        raw_value = source.raw_value
        if raw_value is None or not raw_value.strip():
            return CandidateField(
                field_name=field_name,
                field_ordinal=field_ordinal,
                raw_value=raw_value,
                normalized_value=None,
                normalization_status=NormalizationStatus.MISSING.value,
                source_timezone=None,
                normalization_error=None,
                source_observation_id=None,
                source_field_name=source.name,
                source_field_ordinal=source.field_ordinal,
                locator=source.locator,
            )
        if field_name in _DATE_FIELDS:
            normalized = normalize_date(raw_value)
            return CandidateField(
                field_name=field_name,
                field_ordinal=field_ordinal,
                raw_value=raw_value,
                normalized_value=normalized.normalized_value,
                normalization_status=normalized.status.value,
                source_timezone=normalized.source_timezone,
                normalization_error=normalized.error,
                source_observation_id=None,
                source_field_name=source.name,
                source_field_ordinal=source.field_ordinal,
                locator=source.locator,
            )
        return CandidateField(
            field_name=field_name,
            field_ordinal=field_ordinal,
            raw_value=raw_value,
            normalized_value=" ".join(raw_value.split()),
            normalization_status=NormalizationStatus.NORMALIZED.value,
            source_timezone=None,
            normalization_error=None,
            source_observation_id=None,
            source_field_name=source.name,
            source_field_ordinal=source.field_ordinal,
            locator=source.locator,
        )

    def _candidate_key(
        self,
        observation_id: int,
        candidate_slot_key: str,
    ) -> str:
        payload = json.dumps(
            {
                "observation_id": observation_id,
                "candidate_slot_key": candidate_slot_key,
                "extraction_version": self.extraction_version,
                "normalization_version": self.normalization_version,
            },
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
