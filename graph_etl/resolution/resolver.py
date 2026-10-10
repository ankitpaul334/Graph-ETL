import hashlib
import json

from .models import (
    CandidateAnalysis,
    CanonicalSnapshot,
    ResolutionCandidate,
    ResolutionDecision,
    ResolutionStatus,
)


_SNAPSHOT_FIELDS = (
    "title",
    "event_date",
    "url",
    "venue",
    "cancellation_status",
    "description",
)
_ACCEPTED_STATUSES = {"normalized", "date_only"}


class CanonicalResolver:
    VERSION = "1"

    def __init__(self, *, version: str = "1") -> None:
        if not version:
            raise ValueError("resolver version cannot be empty")
        self.version = version

    def analyze(self, candidate: ResolutionCandidate) -> CandidateAnalysis:
        snapshot: dict[str, str | None] = {}
        missing: list[str] = []
        ambiguous: list[str] = []
        for field_name in _SNAPSHOT_FIELDS:
            values = {
                field.normalized_value
                for field in candidate.fields
                if field.name == field_name
                and field.normalization_status in _ACCEPTED_STATUSES
                and field.normalized_value is not None
            }
            if len(values) > 1:
                ambiguous.append(field_name)
                snapshot[field_name] = None
            elif not values:
                missing.append(field_name)
                snapshot[field_name] = None
            else:
                snapshot[field_name] = next(iter(values))
        title = snapshot.get("title")
        event_date = snapshot.get("event_date")
        identity_key = self._identity_key(title, event_date)
        return CandidateAnalysis(
            snapshot=snapshot,
            identity_key=identity_key,
            title_key=title,
            event_date_key=event_date,
            missing_fields=tuple(missing),
            ambiguous_fields=tuple(ambiguous),
        )

    def decide(
        self,
        candidate: ResolutionCandidate,
        analysis: CandidateAnalysis,
        identity_match: CanonicalSnapshot | None,
        title_matches: tuple[CanonicalSnapshot, ...],
    ) -> ResolutionDecision:
        if analysis.ambiguous_fields:
            return ResolutionDecision(
                status=ResolutionStatus.AMBIGUOUS,
                candidate_id=candidate.id,
                canonical_event_id=None,
                identity_key=analysis.identity_key,
                title_key=analysis.title_key,
                snapshot=analysis.snapshot,
                reason="multiple normalized values: " + ",".join(analysis.ambiguous_fields),
                conflict_fields=analysis.ambiguous_fields,
            )
        if "title" in analysis.missing_fields or "event_date" in analysis.missing_fields:
            return ResolutionDecision(
                status=ResolutionStatus.UNMATCHED,
                candidate_id=candidate.id,
                canonical_event_id=None,
                identity_key=None,
                title_key=analysis.title_key,
                snapshot=analysis.snapshot,
                reason="missing required fields: "
                + ",".join(
                    field
                    for field in ("title", "event_date")
                    if field in analysis.missing_fields
                ),
            )
        if identity_match is not None:
            return self._merge_existing(candidate, analysis, identity_match)

        if title_matches:
            candidate_url = analysis.snapshot.get("url")
            known_urls = {
                match.snapshot.get("url")
                for match in title_matches
                if match.snapshot.get("url")
            }
            same_url_matches = [
                match
                for match in title_matches
                if candidate_url and match.snapshot.get("url") == candidate_url
            ]
            if len(same_url_matches) == 1:
                existing = same_url_matches[0]
                return ResolutionDecision(
                    status=ResolutionStatus.CONFLICT,
                    candidate_id=candidate.id,
                    canonical_event_id=existing.id,
                    identity_key=existing.identity_key,
                    title_key=existing.title_key,
                    snapshot=existing.snapshot,
                    reason="conflicting fields: event_date",
                    conflict_fields=("event_date",),
                )
            if candidate_url and known_urls and candidate_url not in known_urls:
                return ResolutionDecision(
                    status=ResolutionStatus.CREATED,
                    candidate_id=candidate.id,
                    canonical_event_id=None,
                    identity_key=analysis.identity_key,
                    title_key=analysis.title_key,
                    snapshot=analysis.snapshot,
                    reason="created_recurring_instance",
                )
            return ResolutionDecision(
                status=ResolutionStatus.AMBIGUOUS,
                candidate_id=candidate.id,
                canonical_event_id=None,
                identity_key=analysis.identity_key,
                title_key=analysis.title_key,
                snapshot=analysis.snapshot,
                reason="title matches multiple or insufficiently distinct events",
            )

        return ResolutionDecision(
            status=ResolutionStatus.CREATED,
            candidate_id=candidate.id,
            canonical_event_id=None,
            identity_key=analysis.identity_key,
            title_key=analysis.title_key,
            snapshot=analysis.snapshot,
            reason="created",
        )

    def _merge_existing(
        self,
        candidate: ResolutionCandidate,
        analysis: CandidateAnalysis,
        existing: CanonicalSnapshot,
    ) -> ResolutionDecision:
        merged = dict(existing.snapshot)
        conflicts: list[str] = []
        changes: list[str] = []
        for field_name in _SNAPSHOT_FIELDS:
            incoming = analysis.snapshot.get(field_name)
            if incoming is None:
                continue
            current = merged.get(field_name)
            if current is None:
                merged[field_name] = incoming
                changes.append(field_name)
            elif current != incoming:
                conflicts.append(field_name)
        if conflicts:
            return ResolutionDecision(
                status=ResolutionStatus.CONFLICT,
                candidate_id=candidate.id,
                canonical_event_id=existing.id,
                identity_key=existing.identity_key,
                title_key=existing.title_key,
                snapshot=existing.snapshot,
                reason="conflicting fields: " + ",".join(conflicts),
                conflict_fields=tuple(conflicts),
            )
        if changes:
            return ResolutionDecision(
                status=ResolutionStatus.UPDATED,
                candidate_id=candidate.id,
                canonical_event_id=existing.id,
                identity_key=existing.identity_key,
                title_key=existing.title_key,
                snapshot=merged,
                reason="updated fields: " + ",".join(changes),
            )
        return ResolutionDecision(
            status=ResolutionStatus.MATCHED,
            candidate_id=candidate.id,
            canonical_event_id=existing.id,
            identity_key=existing.identity_key,
            title_key=existing.title_key,
            snapshot=existing.snapshot,
            reason="matched",
        )

    @staticmethod
    def _identity_key(title: str | None, event_date: str | None) -> str | None:
        if title is None or event_date is None:
            return None
        payload = json.dumps(
            {"title": title, "event_date": event_date},
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        )
        return hashlib.sha256(payload.encode("utf-8")).hexdigest()
