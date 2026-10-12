import json
from dataclasses import dataclass
from enum import StrEnum
from pathlib import Path
from typing import Mapping

from graph_etl.resolution.models import (
    CandidateFieldValue,
    ResolutionCandidate,
)
from graph_etl.resolution.resolver import CanonicalResolver

_PAIR_FIELDS = (
    "title",
    "event_date",
    "url",
    "venue",
    "cancellation_status",
    "description",
    "source_identifier",
    "classification",
)


class PairLabel(StrEnum):
    MATCH = "MATCH"
    NO_MATCH = "NO_MATCH"
    UNCERTAIN = "UNCERTAIN"


@dataclass(frozen=True, slots=True)
class LabeledPair:
    name: str
    expected: PairLabel
    left: Mapping[str, str | None]
    right: Mapping[str, str | None]


@dataclass(frozen=True, slots=True)
class EvaluationMetrics:
    total: int
    true_positive: int
    false_positive: int
    false_negative: int
    precision: float | None
    recall: float | None
    false_merge_rate: float | None
    uncertain_rate: float


def evaluate_pairs(pairs: list[LabeledPair]) -> EvaluationMetrics:
    resolver = CanonicalResolver()
    true_positive = 0
    false_positive = 0
    false_negative = 0
    no_match_expected = 0
    uncertain = 0
    for pair in pairs:
        predicted = predict_pair(pair.left, pair.right, resolver=resolver)
        if predicted == PairLabel.MATCH and pair.expected == PairLabel.MATCH:
            true_positive += 1
        elif predicted == PairLabel.MATCH and pair.expected == PairLabel.NO_MATCH:
            false_positive += 1
        elif predicted != PairLabel.MATCH and pair.expected == PairLabel.MATCH:
            false_negative += 1
        if pair.expected == PairLabel.NO_MATCH:
            no_match_expected += 1
        if predicted == PairLabel.UNCERTAIN:
            uncertain += 1
    precision_denominator = true_positive + false_positive
    recall_denominator = true_positive + false_negative
    return EvaluationMetrics(
        total=len(pairs),
        true_positive=true_positive,
        false_positive=false_positive,
        false_negative=false_negative,
        precision=(
            true_positive / precision_denominator
            if precision_denominator
            else None
        ),
        recall=(
            true_positive / recall_denominator
            if recall_denominator
            else None
        ),
        false_merge_rate=(
            false_positive / no_match_expected
            if no_match_expected
            else None
        ),
        uncertain_rate=uncertain / len(pairs) if pairs else 0.0,
    )


def predict_pair(
    left: Mapping[str, str | None],
    right: Mapping[str, str | None],
    *,
    resolver: CanonicalResolver | None = None,
) -> PairLabel:
    active = resolver if resolver is not None else CanonicalResolver()
    outcome = active.compare(
        _as_candidate(left, 0),
        _as_candidate(right, 1),
    )
    return PairLabel(outcome)


def load_labeled_pairs(path: str | Path) -> list[LabeledPair]:
    payload = json.loads(Path(path).read_text())
    return [
        LabeledPair(
            name=item["name"],
            expected=PairLabel(item["expected"]),
            left=item["left"],
            right=item["right"],
        )
        for item in payload
    ]


def _as_candidate(
    payload: Mapping[str, str | None],
    position: int,
) -> ResolutionCandidate:
    fields = tuple(
        CandidateFieldValue(
            name=name,
            raw_value=payload.get(name),
            normalized_value=payload.get(name),
            normalization_status="normalized",
            source_timezone=None,
            locator=f"labels[{position}].{name}",
        )
        for name in _PAIR_FIELDS
        if payload.get(name) is not None
    )
    classification = payload.get("classification")
    return ResolutionCandidate(
        id=position,
        source_key=payload.get("source_key") or "",
        external_id=payload.get("external_id") or "",
        classification=classification or "",
        fields=fields,
    )