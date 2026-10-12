import argparse
import json
import sqlite3
import sys
from collections.abc import Callable
from dataclasses import asdict
from pathlib import Path

from graph_etl.domain import Source, SourceKind
from graph_etl.evaluation import evaluate_pairs, load_labeled_pairs
from graph_etl.extraction import (
    EXTRACTION_VERSION,
    NORMALIZATION_VERSION,
    EventCandidateExtractor,
    ExtractionError,
    ExtractionService,
)
from graph_etl.ingestion.http import HttpFeedClient
from graph_etl.ingestion.models import IngestionError
from graph_etl.ingestion.service import IngestionService
from graph_etl.persistence.database import Database
from graph_etl.resolution import (
    RESOLVER_VERSION,
    CanonicalResolver,
    ResolutionError,
    ResolutionService,
)


def number_type(
    minimum: float,
    *,
    integer: bool,
    exclusive: bool = False,
) -> Callable[[str], int | float]:
    def parse(value: str) -> int | float:
        try:
            parsed = int(value) if integer else float(value)
        except ValueError as error:
            raise argparse.ArgumentTypeError("value must be numeric") from error
        invalid = parsed <= minimum if exclusive else parsed < minimum
        if invalid:
            raise argparse.ArgumentTypeError("value is below the allowed minimum")
        return parsed

    return parse


positive_int = number_type(1, integer=True)
positive_float = number_type(0.0, integer=False, exclusive=True)
non_negative_float = number_type(0.0, integer=False)


def add_database_argument(parser: argparse.ArgumentParser) -> None:
    parser.add_argument(
        "--database",
        type=Path,
        default=Path("graph_etl.sqlite3"),
    )


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="graph-etl")
    subparsers = parser.add_subparsers(dest="command", required=True)

    ingest = subparsers.add_parser(
        "ingest-rss",
        help="ingest an RSS, RDF, or Atom feed into SQLite",
    )
    ingest.add_argument("--source-key", required=True)
    ingest.add_argument("--name", required=True)
    ingest.add_argument("--url", required=True)
    add_database_argument(ingest)
    ingest.add_argument("--timeout", type=positive_float, default=20.0)
    ingest.add_argument("--max-attempts", type=positive_int, default=3)
    ingest.add_argument("--retry-delay", type=non_negative_float, default=1.0)

    extract = subparsers.add_parser(
        "extract",
        help="extract versioned event candidates from stored observations",
    )
    add_database_argument(extract)
    extract.add_argument("--source-key")
    extract.add_argument("--extraction-version", default=EXTRACTION_VERSION)
    extract.add_argument("--normalization-version", default=NORMALIZATION_VERSION)

    candidates = subparsers.add_parser(
        "candidates",
        help="query persisted event candidates",
    )
    add_database_argument(candidates)
    candidates.add_argument("--source-key")
    candidates.add_argument("--extraction-version")
    candidates.add_argument("--normalization-version")
    candidates.add_argument("--limit", type=positive_int)

    resolve = subparsers.add_parser(
        "resolve",
        help="resolve candidates into conservative canonical events",
    )
    add_database_argument(resolve)
    resolve.add_argument("--source-key")
    resolve.add_argument("--extraction-version", default=EXTRACTION_VERSION)
    resolve.add_argument("--normalization-version", default=NORMALIZATION_VERSION)
    resolve.add_argument("--resolver-version", default=RESOLVER_VERSION)

    canonical = subparsers.add_parser(
        "canonical-events",
        help="query canonical events and version history",
    )
    add_database_argument(canonical)
    canonical.add_argument("--limit", type=positive_int)

    resolutions = subparsers.add_parser(
        "resolutions",
        help="query candidate resolution decisions",
    )
    add_database_argument(resolutions)
    resolutions.add_argument("--source-key")
    resolutions.add_argument("--status")
    resolutions.add_argument("--limit", type=positive_int)

    outcomes = subparsers.add_parser(
        "extraction-outcomes",
        help="query per-observation extraction outcomes",
    )
    add_database_argument(outcomes)
    outcomes.add_argument("--source-key")
    outcomes.add_argument("--status")
    outcomes.add_argument("--limit", type=positive_int)

    review = subparsers.add_parser(
        "review-candidate",
        help="reject or split a candidate while preserving audit history",
    )
    add_database_argument(review)
    review.add_argument("--candidate-id", type=positive_int, required=True)
    review.add_argument(
        "--action",
        choices=("reject", "split", "restore"),
        required=True,
    )
    review.add_argument("--canonical-event-id", type=positive_int)
    review.add_argument("--resolver-version", default=RESOLVER_VERSION)
    review.add_argument("--reason", required=True)
    review.add_argument("--review-note")

    evaluate = subparsers.add_parser(
        "evaluate",
        help="evaluate conservative candidate-pair matching against labels",
    )
    evaluate.add_argument("--labels", type=Path, required=True)

    handlers = {
        "ingest-rss": run_ingest,
        "extract": run_extract,
        "candidates": run_candidates,
        "resolve": run_resolve,
        "canonical-events": run_canonical_events,
        "resolutions": run_resolutions,
        "extraction-outcomes": run_extraction_outcomes,
        "review-candidate": run_review_candidate,
        "evaluate": run_evaluate,
    }
    for command, handler in handlers.items():
        subparsers.choices[command].set_defaults(handler=handler)
    return parser


def run_ingest(args: argparse.Namespace) -> int:
    source = Source(
        key=args.source_key,
        name=args.name,
        kind=SourceKind.FEED,
        uri=args.url,
    )
    with Database(args.database) as database:
        service = IngestionService(database, HttpFeedClient())
        result = service.ingest(
            source,
            max_attempts=args.max_attempts,
            retry_delay=args.retry_delay,
            timeout=args.timeout,
        )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


def run_resolve(args: argparse.Namespace) -> int:
    _require_existing_database(args.database)
    resolver = CanonicalResolver(version=args.resolver_version)
    with Database(args.database) as database:
        result = ResolutionService(database, resolver=resolver).run(
            source_key=args.source_key,
            extraction_version=args.extraction_version,
            normalization_version=args.normalization_version,
        )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


def run_canonical_events(args: argparse.Namespace) -> int:
    _require_existing_database(args.database)
    with Database(args.database) as database:
        events = database.query_canonical_events(limit=args.limit)
    print(json.dumps({"events": events}, sort_keys=True))
    return 0


def _require_existing_database(path: Path) -> None:
    if not path.is_file():
        raise FileNotFoundError(f"database does not exist: {path}")


def run_extract(args: argparse.Namespace) -> int:
    _require_existing_database(args.database)
    extractor = EventCandidateExtractor(
        extraction_version=args.extraction_version,
        normalization_version=args.normalization_version,
    )
    with Database(args.database) as database:
        result = ExtractionService(database, extractor=extractor).run(
            source_key=args.source_key
        )
    print(json.dumps(asdict(result), sort_keys=True))
    return 0


def run_candidates(args: argparse.Namespace) -> int:
    _require_existing_database(args.database)
    with Database(args.database) as database:
        candidates = database.query_candidates(
            source_key=args.source_key,
            extraction_version=args.extraction_version,
            normalization_version=args.normalization_version,
            limit=args.limit,
        )
    print(json.dumps({"candidates": candidates}, sort_keys=True))
    return 0


def run_resolutions(args: argparse.Namespace) -> int:
    _require_existing_database(args.database)
    with Database(args.database) as database:
        rows = database.query_resolutions(
            source_key=args.source_key,
            status=args.status,
            limit=args.limit,
        )
    print(json.dumps({"resolutions": rows}, sort_keys=True))
    return 0


def run_extraction_outcomes(args: argparse.Namespace) -> int:
    _require_existing_database(args.database)
    with Database(args.database) as database:
        rows = database.query_extraction_outcomes(
            source_key=args.source_key,
            status=args.status,
            limit=args.limit,
        )
    print(json.dumps({"outcomes": rows}, sort_keys=True))
    return 0


def run_review_candidate(args: argparse.Namespace) -> int:
    _require_existing_database(args.database)
    with Database(args.database) as database:
        if args.action == "restore":
            if args.canonical_event_id is None:
                raise ValueError("--canonical-event-id is required for restore")
            decision = database.restore_candidate_link(
                args.candidate_id,
                args.canonical_event_id,
                resolver_version=args.resolver_version,
                reason=args.reason,
                review_note=args.review_note,
            )
        else:
            decision = database.change_candidate_link(
                args.candidate_id,
                resolver_version=args.resolver_version,
                action=args.action,
                reason=args.reason,
                review_note=args.review_note,
            )
    print(json.dumps(decision, sort_keys=True))
    return 0


def run_evaluate(args: argparse.Namespace) -> int:
    metrics = evaluate_pairs(load_labeled_pairs(args.labels))
    print(json.dumps(asdict(metrics), sort_keys=True))
    return 0


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        return args.handler(args)
    except (
        ExtractionError,
        IngestionError,
        ResolutionError,
        OSError,
        RuntimeError,
        ValueError,
        sqlite3.Error,
    ) as error:
        print(json.dumps({"error": str(error)}, sort_keys=True), file=sys.stderr)
        return 1
