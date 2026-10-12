from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from graph_etl.persistence.database import Database

from .extractor import EventCandidateExtractor
from .models import (
    CandidateDraft,
    EventClassification,
    ExtractionOutcomeStatus,
    ExtractionRunResult,
    StoredObservation,
    StoredObservationField,
)


class ExtractionError(RuntimeError):
    pass


class ExtractionService:
    def __init__(
        self,
        database: Database,
        *,
        extractor: EventCandidateExtractor | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = database
        self.extractor = extractor or EventCandidateExtractor()
        self.clock = clock or (lambda: datetime.now(UTC))

    def run(self, *, source_key: str | None = None) -> ExtractionRunResult:
        run_id = self.database.create_extraction_run(
            self.extractor.extraction_version,
            self.extractor.normalization_version,
        )
        processed = 0
        created = 0
        existing = 0
        with_errors = 0
        skipped = 0
        try:
            rows = self.database.load_extraction_observations(source_key)
            for observation in self._group_observations(rows):
                processed += 1
                try:
                    drafts = self.extractor.extract(observation)
                    if not drafts:
                        skipped += 1
                        self.database.record_extraction_outcome(
                            run_id,
                            observation.id,
                            status=ExtractionOutcomeStatus.SKIPPED.value,
                            candidates_created=0,
                        )
                        continue
                    observation_created, observation_existing = self.database.persist_extraction(
                        run_id,
                        ((observation.id, drafts),),
                    )
                    has_errors = any(
                        field.normalization_error is not None
                        for draft in drafts
                        for field in draft.fields
                    )
                    status = (
                        ExtractionOutcomeStatus.SUCCEEDED_WITH_ERRORS.value
                        if has_errors
                        else ExtractionOutcomeStatus.SUCCEEDED.value
                    )
                    self.database.record_extraction_outcome(
                        run_id,
                        observation.id,
                        status=status,
                        candidates_created=observation_created,
                        event_classification=drafts[0].classification,
                    )
                    created += observation_created
                    existing += observation_existing
                except Exception as error:
                    with_errors += 1
                    try:
                        self.database.record_extraction_outcome(
                            run_id,
                            observation.id,
                            status=ExtractionOutcomeStatus.FAILED.value,
                            candidates_created=0,
                            event_classification=EventClassification.EXTRACTION_FAILED.value,
                            error=f"{type(error).__name__}: {error}",
                        )
                    except Exception:
                        raise
            self.database.complete_extraction_run(
                run_id,
                observations_processed=processed,
                candidates_created=created,
                candidates_existing=existing,
                observations_with_errors=with_errors,
                observations_skipped=skipped,
            )
        except Exception as error:
            self.database.fail_extraction_run(run_id, error)
            raise ExtractionError("event candidate extraction failed") from error

        return ExtractionRunResult(
            run_id=run_id,
            extraction_version=self.extractor.extraction_version,
            normalization_version=self.extractor.normalization_version,
            observations_processed=processed,
            candidates_created=created,
            candidates_existing=existing,
            observations_with_errors=with_errors,
            observations_skipped=skipped,
        )

    def _group_observations(self, rows: list[Any]) -> list[StoredObservation]:
        grouped: dict[int, dict[str, Any]] = {}
        for row in rows:
            observation_id = int(row["observation_id"])
            record = grouped.setdefault(
                observation_id,
                {
                    "source_id": int(row["source_id"]),
                    "external_id": row["external_id"],
                    "content_sha256": row["content_sha256"],
                    "fields": [],
                },
            )
            if row["field_name"] is not None:
                record["fields"].append(
                    StoredObservationField(
                        name=row["field_name"],
                        field_ordinal=int(row["field_ordinal"]),
                        raw_value=row["raw_value"],
                        locator=row["locator"],
                        is_present=bool(row["is_present"]),
                    )
                )
        return [
            StoredObservation(
                id=observation_id,
                source_id=record["source_id"],
                external_id=record["external_id"],
                content_sha256=record["content_sha256"],
                fields=tuple(record["fields"]),
            )
            for observation_id, record in grouped.items()
        ]
