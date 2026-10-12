from collections.abc import Callable
from dataclasses import replace
from datetime import UTC, datetime
import json
from typing import Any

from graph_etl.extraction.models import EXTRACTION_VERSION, NORMALIZATION_VERSION
from graph_etl.persistence.database import Database

from .models import (
    CandidateFieldValue,
    CanonicalSnapshot,
    ResolutionCandidate,
    ResolutionDecision,
    ResolutionRunResult,
)
from .resolver import CanonicalResolver


class ResolutionError(RuntimeError):
    pass


class ResolutionService:
    def __init__(
        self,
        database: Database,
        *,
        resolver: CanonicalResolver | None = None,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = database
        self.resolver = resolver or CanonicalResolver()
        self.clock = clock or (lambda: datetime.now(UTC))

    def run(
        self,
        *,
        source_key: str | None = None,
        extraction_version: str = EXTRACTION_VERSION,
        normalization_version: str = NORMALIZATION_VERSION,
    ) -> ResolutionRunResult:
        run_id = self.database.create_resolution_run(self.resolver.version)
        try:
            rows = self.database.load_resolution_candidates(
                source_key,
                extraction_version,
                normalization_version,
            )
            processed = 0
            created = 0
            existing = 0
            conflicts = 0
            unresolved = 0
            for candidate in self._group_candidates(rows):
                analysis = self.resolver.analyze(candidate)
                identity_row = self.database.find_canonical_by_identity(
                    analysis.identity_key
                )
                identity_match = self._snapshot(identity_row)
                source_match = self._snapshot(
                    self.database.find_canonical_by_source_identity(
                        analysis.source_identity_key
                    )
                )
                url_rows = self.database.find_canonical_by_url(analysis.url_key)
                url_match = self._snapshot(url_rows[0]) if len(url_rows) == 1 else None
                title_matches = tuple(
                    self._snapshot(row)
                    for row in self.database.find_canonical_by_title(
                        analysis.title_key
                    )
                )
                decision = replace(
                    self.resolver.decide(
                        candidate,
                        analysis,
                        identity_match,
                        source_match,
                        url_match,
                        title_matches,
                    ),
                    source_identity_key=analysis.source_identity_key,
                    url_key=analysis.url_key,
                )
                decision_created, decision_existing, decision_conflicts, decision_unresolved = (
                    self.database.persist_resolution(run_id, (decision,))
                )
                processed += 1
                created += decision_created
                existing += decision_existing
                conflicts += decision_conflicts
                unresolved += decision_unresolved
            self.database.complete_resolution_run(
                run_id,
                candidates_processed=processed,
                decisions_created=created,
                decisions_existing=existing,
                conflicts=conflicts,
                unresolved=unresolved,
            )
        except Exception as error:
            self.database.fail_resolution_run(run_id, error)
            raise ResolutionError("canonical event resolution failed") from error

        return ResolutionRunResult(
            run_id=run_id,
            resolver_version=self.resolver.version,
            candidates_processed=processed,
            decisions_created=created,
            decisions_existing=existing,
            conflicts=conflicts,
            unresolved=unresolved,
        )

    def _snapshot(self, row: dict[str, object] | None) -> CanonicalSnapshot | None:
        if row is None:
            return None
        return CanonicalSnapshot(
            id=int(row["id"]),
            identity_key=str(row["identity_key"]),
            title_key=str(row["title_key"]),
            source_identity_key=(
                str(row["source_identity_key"])
                if row["source_identity_key"] is not None
                else None
            ),
            url_key=(
                str(row["url_key"])
                if row["url_key"] is not None
                else None
            ),
            snapshot=json.loads(str(row["snapshot_json"])),
        )

    def _group_candidates(self, rows: list[Any]) -> list[ResolutionCandidate]:
        grouped: dict[int, dict[str, Any]] = {}
        for row in rows:
            candidate_id = int(row["candidate_id"])
            record = grouped.setdefault(
                candidate_id,
                {
                    "source_key": row["source_key"],
                    "external_id": row["source_external_id"],
                    "classification": row["classification"],
                    "fields": [],
                },
            )
            record["fields"].append(
                CandidateFieldValue(
                    name=row["field_name"],
                    raw_value=row["raw_value"],
                    normalized_value=row["normalized_value"],
                    normalization_status=row["normalization_status"],
                    source_timezone=row["source_timezone"],
                    locator=row["locator"],
                )
            )
        return [
            ResolutionCandidate(
                id=candidate_id,
                source_key=record["source_key"],
                external_id=record["external_id"],
                classification=record["classification"],
                fields=tuple(record["fields"]),
            )
            for candidate_id, record in grouped.items()
        ]
