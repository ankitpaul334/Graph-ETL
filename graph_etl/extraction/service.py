from collections.abc import Callable
from datetime import UTC, datetime
from typing import Any

from graph_etl.persistence.database import Database

from .extractor import EventCandidateExtractor
from .models import (
    CandidateDraft,
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
        try:
            rows = self.database.load_extraction_observations(source_key)
            batches: list[tuple[int, tuple[CandidateDraft, ...]]] = []
            for observation in self._group_observations(rows):
                drafts = self.extractor.extract(observation)
                batches.append((observation.id, drafts))
            created, existing = self.database.persist_extraction(run_id, batches)
            self.database.complete_extraction_run(
                run_id,
                observations_processed=len(batches),
                candidates_created=created,
                candidates_existing=existing,
            )
        except Exception as error:
            self.database.fail_extraction_run(run_id, error)
            raise ExtractionError("event candidate extraction failed") from error

        return ExtractionRunResult(
            run_id=run_id,
            extraction_version=self.extractor.extraction_version,
            normalization_version=self.extractor.normalization_version,
            observations_processed=len(batches),
            candidates_created=created,
            candidates_existing=existing,
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
