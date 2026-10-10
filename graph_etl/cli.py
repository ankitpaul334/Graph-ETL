import argparse
import json
import sqlite3
import sys
from dataclasses import asdict
from pathlib import Path

from graph_etl.domain import Source, SourceKind
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


def positive_int(value: str) -> int:
    parsed = int(value)
    if parsed < 1:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def positive_float(value: str) -> float:
    parsed = float(value)
    if parsed <= 0:
        raise argparse.ArgumentTypeError("value must be positive")
    return parsed


def non_negative_float(value: str) -> float:
    parsed = float(value)
    if parsed < 0:
        raise argparse.ArgumentTypeError("value cannot be negative")
    return parsed


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


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        if args.command == "ingest-rss":
            return run_ingest(args)
        if args.command == "extract":
            return run_extract(args)
        if args.command == "candidates":
            return run_candidates(args)
        if args.command == "resolve":
            return run_resolve(args)
        return run_canonical_events(args)
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
