import argparse
import json
import sqlite3
import sys
from dataclasses import asdict
from pathlib import Path

from graph_etl.domain import Source, SourceKind
from graph_etl.ingestion.http import HttpFeedClient
from graph_etl.ingestion.models import IngestionError
from graph_etl.ingestion.service import IngestionService
from graph_etl.persistence.database import Database


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
    ingest.add_argument("--database", type=Path, default=Path("graph_etl.sqlite3"))
    ingest.add_argument("--timeout", type=positive_float, default=20.0)
    ingest.add_argument("--max-attempts", type=positive_int, default=3)
    ingest.add_argument("--retry-delay", type=non_negative_float, default=1.0)
    return parser


def main(argv: list[str] | None = None) -> int:
    args = build_parser().parse_args(argv)
    try:
        source = Source(
            key=args.source_key,
            name=args.name,
            kind=SourceKind.FEED,
            uri=args.url,
        )
        client = HttpFeedClient()
        with Database(args.database) as database:
            service = IngestionService(database, client)
            result = service.ingest(
                source,
                max_attempts=args.max_attempts,
                retry_delay=args.retry_delay,
                timeout=args.timeout,
            )
    except (IngestionError, OSError, ValueError, sqlite3.Error) as error:
        print(json.dumps({"error": str(error)}, sort_keys=True), file=sys.stderr)
        return 1

    print(json.dumps(asdict(result), sort_keys=True))
    return 0
