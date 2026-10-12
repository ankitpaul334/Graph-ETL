from graph_etl.identity import (
    normalize_title,
    normalize_url,
    source_identity_key,
    title_date_identity_key,
)

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
    "source_identifier",
    "classification",
)
_ACCEPTED_STATUSES = {"normalized", "date_only"}
_NOT_EVENT = "NOT_EVENT"
_PAIR_MATCH = "MATCH"
_PAIR_NO_MATCH = "NO_MATCH"
_PAIR_UNCERTAIN = "UNCERTAIN"


class CanonicalResolver:
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
        return CandidateAnalysis(
            snapshot=snapshot,
            identity_key=title_date_identity_key(title, event_date),
            source_identity_key=source_identity_key(
                candidate.source_key,
                candidate.external_id,
            ),
            url_key=normalize_url(snapshot.get("url")),
            title_key=normalize_title(title),
            missing_fields=tuple(missing),
            ambiguous_fields=tuple(ambiguous),
        )

    def decide(
        self,
        candidate: ResolutionCandidate,
        analysis: CandidateAnalysis,
        identity_match: CanonicalSnapshot | None,
        source_match: CanonicalSnapshot | None,
        url_match: CanonicalSnapshot | None,
        title_matches: tuple[CanonicalSnapshot, ...],
    ) -> ResolutionDecision:
        if candidate.classification == _NOT_EVENT or (
            analysis.snapshot.get("classification") or ""
        ).casefold() in {
            "not_event",
            "not event",
            "article",
            "blog",
            "blog post",
            "news",
            "product",
            "documentation",
        }:
            return ResolutionDecision(
                status=ResolutionStatus.REJECTED,
                candidate_id=candidate.id,
                canonical_event_id=None,
                identity_key=analysis.identity_key,
                title_key=analysis.title_key,
                snapshot=analysis.snapshot,
                reason="source classification is not an event",
            )
        if analysis.ambiguous_fields:
            return ResolutionDecision(
                status=ResolutionStatus.REVIEW_REQUIRED,
                candidate_id=candidate.id,
                canonical_event_id=None,
                identity_key=analysis.identity_key,
                title_key=analysis.title_key,
                snapshot=analysis.snapshot,
                reason="multiple normalized values: " + ",".join(analysis.ambiguous_fields),
                conflict_fields=analysis.ambiguous_fields,
            )
        if source_match is not None:
            return self._merge_existing(
                candidate,
                analysis,
                source_match,
                reason="matched_source_identity",
            )
        if "title" in analysis.missing_fields or "event_date" in analysis.missing_fields:
            return ResolutionDecision(
                status=ResolutionStatus.UNRESOLVED,
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
        if url_match is not None:
            if (
                analysis.snapshot.get("event_date") is not None
                and url_match.snapshot.get("event_date") is not None
                and analysis.snapshot.get("event_date")
                != url_match.snapshot.get("event_date")
            ):
                return ResolutionDecision(
                    status=ResolutionStatus.NEW_EVENT_PROPOSED,
                    candidate_id=candidate.id,
                    canonical_event_id=None,
                    identity_key=analysis.identity_key,
                    title_key=analysis.title_key,
                    snapshot=analysis.snapshot,
                    reason="proposed_recurring_instance_without_reschedule_evidence",
                )
            return self._merge_existing(
                candidate,
                analysis,
                url_match,
                reason="matched_normalized_url",
            )
        if identity_match is not None:
            return self._merge_existing(
                candidate,
                analysis,
                identity_match,
                reason="matched_title_and_event_date",
            )
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
                return ResolutionDecision(
                    status=ResolutionStatus.REVIEW_REQUIRED,
                    candidate_id=candidate.id,
                    canonical_event_id=None,
                    identity_key=analysis.identity_key,
                    title_key=analysis.title_key,
                    snapshot=analysis.snapshot,
                    reason="same URL matches an existing event with insufficient evidence",
                )
            if candidate_url and known_urls and candidate_url not in known_urls:
                return ResolutionDecision(
                    status=ResolutionStatus.NEW_EVENT_PROPOSED,
                    candidate_id=candidate.id,
                    canonical_event_id=None,
                    identity_key=analysis.identity_key,
                    title_key=analysis.title_key,
                    snapshot=analysis.snapshot,
                    reason="proposed_recurring_instance",
                )
            return ResolutionDecision(
                status=ResolutionStatus.REVIEW_REQUIRED,
                candidate_id=candidate.id,
                canonical_event_id=None,
                identity_key=analysis.identity_key,
                title_key=analysis.title_key,
                snapshot=analysis.snapshot,
                reason="title matches multiple or insufficiently distinct events",
            )

        return ResolutionDecision(
            status=ResolutionStatus.NEW_EVENT_PROPOSED,
            candidate_id=candidate.id,
            canonical_event_id=None,
            identity_key=analysis.identity_key,
            title_key=analysis.title_key,
            snapshot=analysis.snapshot,
            reason="new_event_proposed",
        )

    def _merge_existing(
        self,
        candidate: ResolutionCandidate,
        analysis: CandidateAnalysis,
        existing: CanonicalSnapshot,
        *,
        reason: str,
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
            elif field_name == "url":
                if normalize_url(current) != normalize_url(incoming):
                    conflicts.append(field_name)
            elif field_name == "title":
                if normalize_title(current) != normalize_title(incoming):
                    conflicts.append(field_name)
            elif current != incoming:
                conflicts.append(field_name)
        if conflicts:
            return ResolutionDecision(
                status=ResolutionStatus.REVIEW_REQUIRED,
                candidate_id=candidate.id,
                canonical_event_id=existing.id,
                identity_key=existing.identity_key,
                title_key=existing.title_key,
                snapshot=existing.snapshot,
                reason=reason + "; conflicting fields: " + ",".join(conflicts),
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
                reason=reason + "; updated fields: " + ",".join(changes),
            )
        return ResolutionDecision(
            status=ResolutionStatus.MATCHED,
            candidate_id=candidate.id,
            canonical_event_id=existing.id,
            identity_key=existing.identity_key,
            title_key=existing.title_key,
            snapshot=existing.snapshot,
            reason=reason,
        )

    def compare(
        self,
        left: ResolutionCandidate,
        right: ResolutionCandidate,
    ) -> str:
        if _NOT_EVENT in {left.classification, right.classification}:
            return _PAIR_NO_MATCH
        left_analysis = self.analyze(left)
        right_analysis = self.analyze(right)
        left_date = left_analysis.snapshot.get("event_date")
        right_date = right_analysis.snapshot.get("event_date")
        same_source_identity = (
            left_analysis.source_identity_key is not None
            and left_analysis.source_identity_key == right_analysis.source_identity_key
        )
        if same_source_identity:
            return self._pair_outcome(left_date, right_date)
        if left_analysis.url_key and left_analysis.url_key == right_analysis.url_key:
            return self._pair_outcome(left_date, right_date)
        if left_analysis.url_key and right_analysis.url_key:
            return _PAIR_UNCERTAIN
        if left_analysis.title_key and left_analysis.title_key == right_analysis.title_key:
            if left_date and right_date:
                if left_date == right_date:
                    return _PAIR_MATCH
                return _PAIR_NO_MATCH
            return _PAIR_UNCERTAIN
        return _PAIR_NO_MATCH

    @staticmethod
    def _pair_outcome(
        left_date: str | None,
        right_date: str | None,
    ) -> str:
        if left_date and right_date and left_date != right_date:
            return _PAIR_UNCERTAIN
        return _PAIR_MATCH