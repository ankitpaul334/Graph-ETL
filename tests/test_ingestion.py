from dataclasses import replace
from pathlib import Path
import tempfile
import unittest

from graph_etl.domain import Source, SourceKind
from graph_etl.ingestion.feeds import RssAtomFeedParser
from graph_etl.ingestion.models import (
    FetchedDocument,
    IngestionError,
    SourceFetchError,
)
from graph_etl.ingestion.service import IngestionService
from graph_etl.persistence.database import Database


FIXTURES = Path(__file__).parent / "fixtures"


class StubClient:
    def __init__(self, *outcomes: FetchedDocument | SourceFetchError) -> None:
        self.outcomes = list(outcomes)

    def fetch(self, uri: str, *, timeout: float) -> FetchedDocument:
        del uri, timeout
        outcome = self.outcomes.pop(0)
        if isinstance(outcome, SourceFetchError):
            raise outcome
        return outcome


class IngestionServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "graph_etl.sqlite3"
        self.payload = (FIXTURES / "rss.xml").read_bytes()
        self.source = Source(
            key="rss:python-tech-calendar",
            name="Python Tech Calendar",
            kind=SourceKind.FEED,
            uri="https://example.org/feed.xml",
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def test_ingestion_is_idempotent_and_retains_provenance(self) -> None:
        client = StubClient(
            FetchedDocument(self.payload, "application/rss+xml"),
            FetchedDocument(self.payload, "application/rss+xml"),
        )
        with Database(self.database_path) as database:
            service = IngestionService(database, client, sleep=lambda _: None)
            first = service.ingest(self.source)
            second = service.ingest(self.source)

            self.assertTrue(first.raw_document_created)
            self.assertEqual(1, first.observations_inserted)
            self.assertFalse(second.raw_document_created)
            self.assertEqual(0, second.observations_inserted)
            self.assertEqual(1, second.observations_existing)
            self.assertEqual(
                RssAtomFeedParser.VERSION,
                database.connection.execute(
                    "SELECT parser_version FROM observations"
                ).fetchone()[0],
            )
            self.assertEqual(
                1,
                database.connection.execute(
                    "SELECT COUNT(*) FROM raw_documents"
                ).fetchone()[0],
            )
            self.assertEqual(
                1,
                database.connection.execute(
                    "SELECT COUNT(*) FROM observations"
                ).fetchone()[0],
            )
            raw_payload = database.connection.execute(
                "SELECT payload FROM raw_documents"
            ).fetchone()[0]
            self.assertEqual(self.payload, bytes(raw_payload))
            title = database.connection.execute(
                """
                SELECT raw_value, locator, is_present
                FROM observation_fields
                WHERE field_name = 'title'
                """
            ).fetchone()
            self.assertEqual("PyData London", title["raw_value"])
            self.assertEqual("rss:item[1]/title", title["locator"])
            self.assertEqual(1, title["is_present"])
            missing = database.connection.execute(
                """
                SELECT raw_value, is_present
                FROM observation_fields
                WHERE field_name = 'updated_at'
                """
            ).fetchone()
            self.assertIsNone(missing["raw_value"])
            self.assertEqual(0, missing["is_present"])

    def test_retry_records_failed_then_successful_attempt(self) -> None:
        client = StubClient(
            SourceFetchError("temporary failure", retryable=True),
            FetchedDocument(self.payload, "application/rss+xml"),
        )
        delays: list[float] = []
        with Database(self.database_path) as database:
            service = IngestionService(
                database,
                client,
                sleep=delays.append,
            )
            result = service.ingest(self.source, retry_delay=0.25)

            self.assertEqual(2, result.attempts)
            self.assertEqual([0.25], delays)
            statuses = [
                row["status"]
                for row in database.connection.execute(
                    """
                    SELECT status
                    FROM ingestion_runs
                    ORDER BY attempt_number
                    """
                )
            ]
            self.assertEqual(["failed", "succeeded"], statuses)

    def test_malformed_feed_retains_raw_document_and_failed_run(self) -> None:
        malformed = b"<rss><channel></rss>"
        client = StubClient(FetchedDocument(malformed, "application/rss+xml"))
        with Database(self.database_path) as database:
            service = IngestionService(database, client, sleep=lambda _: None)
            with self.assertRaises(IngestionError):
                service.ingest(self.source, max_attempts=1)

            document = database.connection.execute(
                "SELECT payload FROM raw_documents"
            ).fetchone()
            self.assertEqual(malformed, bytes(document[0]))
            run = database.connection.execute(
                "SELECT status, raw_document_id FROM ingestion_runs"
            ).fetchone()
            self.assertEqual("failed", run["status"])
            self.assertIsNotNone(run["raw_document_id"])
            self.assertEqual(
                0,
                database.connection.execute(
                    "SELECT COUNT(*) FROM observations"
                ).fetchone()[0],
            )

    def test_changed_feed_preserves_previous_observation(self) -> None:
        changed = self.payload.replace(b"PyData London", b"PyData London 2027")
        client = StubClient(
            FetchedDocument(self.payload, "application/rss+xml"),
            FetchedDocument(changed, "application/rss+xml"),
        )
        with Database(self.database_path) as database:
            service = IngestionService(database, client, sleep=lambda _: None)
            service.ingest(self.source)
            result = service.ingest(self.source)

            self.assertEqual(1, result.observations_inserted)
            self.assertEqual(
                2,
                database.connection.execute(
                    "SELECT COUNT(*) FROM observations"
                ).fetchone()[0],
            )
            self.assertEqual(
                2,
                database.connection.execute(
                    "SELECT COUNT(*) FROM raw_documents"
                ).fetchone()[0],
            )

    def test_changed_document_reuses_unchanged_item_observation(self) -> None:
        payload = (
            b"<rss><channel><title>Feed</title>"
            b"<item><guid>a</guid><title>Alpha</title></item>"
            b"<item><guid>b</guid><title>Beta</title></item>"
            b"</channel></rss>"
        )
        changed = payload.replace(b"Alpha", b"Alpha Updated")
        client = StubClient(
            FetchedDocument(payload, "application/rss+xml"),
            FetchedDocument(changed, "application/rss+xml"),
        )
        with Database(self.database_path) as database:
            service = IngestionService(database, client, sleep=lambda _: None)
            service.ingest(self.source)
            result = service.ingest(self.source)

            self.assertEqual(1, result.observations_inserted)
            self.assertEqual(1, result.observations_existing)
            self.assertEqual(
                3,
                database.connection.execute(
                    "SELECT COUNT(*) FROM observations"
                ).fetchone()[0],
            )
            self.assertEqual(
                2,
                database.connection.execute(
                    "SELECT COUNT(*) FROM raw_documents"
                ).fetchone()[0],
            )

    def test_duplicate_entry_index_is_rejected(self) -> None:
        payload = (
            b"<rss><channel><title>Feed</title>"
            b"<item><guid>a</guid><title>Alpha</title></item>"
            b"<item><guid>b</guid><title>Beta</title></item>"
            b"</channel></rss>"
        )
        parser = RssAtomFeedParser()
        feed = parser.parse(payload)
        invalid_feed = replace(
            feed,
            entries=(
                feed.entries[0],
                replace(feed.entries[1], index=0),
            ),
        )
        with Database(self.database_path) as database:
            source_id = database.upsert_source(self.source)
            document_id, _ = database.save_raw_document(
                source_id,
                FetchedDocument(payload, "application/rss+xml"),
                fetched_at="2026-09-26T00:00:00Z",
            )
            run_id = database.start_run(source_id)
            database.attach_document(run_id, document_id)

            with self.assertRaises(ValueError):
                database.persist_feed(run_id, document_id, invalid_feed)


if __name__ == "__main__":
    unittest.main()
