import hashlib
from pathlib import Path
import tempfile
import unittest

from graph_etl.domain import Source, SourceKind
from graph_etl.extraction import ExtractionService
from graph_etl.ingestion.models import FieldValue, FetchedDocument, ParsedEntry, ParsedFeed
from graph_etl.persistence.database import Database
from graph_etl.resolution import CanonicalResolver, ResolutionService


class ResolutionIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "graph_etl.sqlite3"

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _seed_candidate(
        self,
        database: Database,
        *,
        source_key: str,
        external_id: str,
        title: str | None,
        event_date: str | None,
        url: str | None = None,
        venue: str | None = None,
        cancellation_status: str | None = None,
    ) -> int:
        source = Source(
            key=source_key,
            name=source_key,
            kind=SourceKind.FEED,
            uri=f"https://example.org/{source_key}.xml",
        )
        source_id = database.upsert_source(source)
        payload = f"<fixture>{external_id}</fixture>".encode()
        document_id, _ = database.save_raw_document(
            source_id,
            FetchedDocument(payload, "application/rss+xml"),
            fetched_at="2026-09-26T00:00:00Z",
        )
        run_id = database.start_run(source_id)
        database.attach_document(run_id, document_id)
        values = [
            ("title", title),
            ("event_date", event_date),
            ("link", url),
            ("venue", venue),
            ("cancellation_status", cancellation_status),
        ]
        fields = tuple(
            FieldValue(name, value, f"fixture:{name}")
            for name, value in values
            if value is not None
        )
        entry = ParsedEntry(
            external_id=external_id,
            content_sha256=hashlib.sha256(payload).hexdigest(),
            index=0,
            fields=fields,
        )
        database.persist_feed(
            run_id,
            document_id,
            ParsedFeed("rss", source_key, "1", (entry,)),
        )
        ExtractionService(database).run(source_key=source_key)
        return int(
            database.connection.execute(
                """
                SELECT ec.id
                FROM event_candidates AS ec
                JOIN observations AS o ON o.id = ec.observation_id
                WHERE o.external_id = ? AND ec.extraction_version = '1'
                """,
                (external_id,),
            ).fetchone()[0]
        )

    def test_duplicate_candidates_resolve_to_one_canonical_event(self) -> None:
        with Database(self.database_path) as database:
            first = self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Python Conference",
                event_date="2026-11-05",
                url="https://example.org/python",
                venue="Main Hall",
            )
            second = self._seed_candidate(
                database,
                source_key="feed:b",
                external_id="b-1",
                title="Python Conference",
                event_date="2026-11-05",
                url="https://example.org/python",
                venue="Main Hall",
            )
            result = ResolutionService(database).run()

            self.assertEqual(2, result.candidates_processed)
            events = database.query_canonical_events()
            self.assertEqual(1, len(events))
            self.assertEqual(1, len(events[0]["versions"]))
            resolutions = database.connection.execute(
                """
                SELECT candidate_id, status, canonical_event_id
                FROM candidate_resolutions
                ORDER BY candidate_id
                """
            ).fetchall()
            self.assertEqual(["created", "matched"], [row["status"] for row in resolutions])
            self.assertEqual(1, len({row["canonical_event_id"] for row in resolutions}))
            self.assertEqual({first, second}, {row["candidate_id"] for row in resolutions})

    def test_resolution_rerun_is_idempotent(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Idempotent Event",
                event_date="2026-11-05",
                url="https://example.org/idempotent",
            )
            first = ResolutionService(database).run()
            second = ResolutionService(database).run()

            self.assertEqual(1, first.decisions_created)
            self.assertEqual(0, second.decisions_created)
            self.assertEqual(1, second.decisions_existing)
            self.assertEqual(1, len(database.query_canonical_events()))

    def test_recurring_instances_with_distinct_urls_stay_separate(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Recurring Summit",
                event_date="2026-11-05",
                url="https://example.org/summit-1",
            )
            self._seed_candidate(
                database,
                source_key="feed:b",
                external_id="b-1",
                title="Recurring Summit",
                event_date="2026-11-06",
                url="https://example.org/summit-2",
            )
            result = ResolutionService(database).run()

            self.assertEqual(2, result.candidates_processed)
            self.assertEqual(0, result.unresolved)
            events = database.query_canonical_events()
            self.assertEqual(2, len(events))
            self.assertEqual(
                {"2026-11-05", "2026-11-06"},
                {event["event_date_key"] for event in events},
            )

    def test_missing_event_date_is_unmatched(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Undated Event",
                event_date=None,
                url="https://example.org/undated",
            )
            result = ResolutionService(database).run()

            self.assertEqual(1, result.unresolved)
            self.assertEqual(0, len(database.query_canonical_events()))
            status = database.connection.execute(
                "SELECT status, reason FROM candidate_resolutions"
            ).fetchone()
            self.assertEqual("unmatched", status["status"])
            self.assertIn("event_date", status["reason"])

    def test_ambiguous_title_without_distinct_url_is_not_merged(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Ambiguous Series",
                event_date="2026-11-05",
            )
            self._seed_candidate(
                database,
                source_key="feed:b",
                external_id="b-1",
                title="Ambiguous Series",
                event_date="2026-11-06",
            )
            result = ResolutionService(database).run()

            self.assertEqual(1, result.unresolved)
            self.assertEqual(1, len(database.query_canonical_events()))
            statuses = [
                row["status"]
                for row in database.connection.execute(
                    "SELECT status FROM candidate_resolutions ORDER BY id"
                )
            ]
            self.assertEqual(["created", "ambiguous"], statuses)

    def test_conflicting_venue_is_audited_without_overwrite(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Conflict Event",
                event_date="2026-11-05",
                url="https://example.org/conflict",
                venue="Main Hall",
            )
            ResolutionService(database).run()
            self._seed_candidate(
                database,
                source_key="feed:b",
                external_id="b-1",
                title="Conflict Event",
                event_date="2026-11-05",
                url="https://example.org/conflict",
                venue="Other Hall",
            )
            result = ResolutionService(database).run()

            self.assertEqual(1, result.conflicts)
            event = database.query_canonical_events()[0]
            self.assertEqual("Main Hall", event["snapshot"]["venue"])
            self.assertEqual(2, len(event["versions"]))
            self.assertIn("conflicting fields: venue", event["versions"][1]["change_reason"])
            resolution = database.connection.execute(
                """
                SELECT status, canonical_event_id, reason
                FROM candidate_resolutions
                WHERE status = 'conflict'
                """
            ).fetchone()
            self.assertEqual("conflict", resolution["status"])
            self.assertIsNotNone(resolution["canonical_event_id"])

    def test_date_change_with_same_url_is_recorded_as_conflict(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Date Change Event",
                event_date="2026-11-05",
                url="https://example.org/date-change",
            )
            ResolutionService(database).run()
            self._seed_candidate(
                database,
                source_key="feed:b",
                external_id="b-1",
                title="Date Change Event",
                event_date="2026-11-06",
                url="https://example.org/date-change",
            )
            result = ResolutionService(database).run()

            self.assertEqual(1, result.conflicts)
            event = database.query_canonical_events()[0]
            self.assertEqual("2026-11-05", event["snapshot"]["event_date"])
            self.assertIn("event_date", event["versions"][1]["change_reason"])

    def test_new_field_fills_missing_value_and_creates_version(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Enriched Event",
                event_date="2026-11-05",
                url="https://example.org/enriched",
            )
            ResolutionService(database).run()
            self._seed_candidate(
                database,
                source_key="feed:b",
                external_id="b-1",
                title="Enriched Event",
                event_date="2026-11-05",
                url="https://example.org/enriched",
                venue="New Hall",
            )
            result = ResolutionService(database).run()

            self.assertEqual(0, result.conflicts)
            event = database.query_canonical_events()[0]
            self.assertEqual("New Hall", event["snapshot"]["venue"])
            self.assertEqual(2, event["current_version"])
            self.assertIn("updated fields: venue", event["versions"][1]["change_reason"])

    def test_resolver_version_upgrade_preserves_prior_decisions(self) -> None:
        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Versioned Event",
                event_date="2026-11-05",
                url="https://example.org/versioned",
            )
            ResolutionService(database).run()
            ResolutionService(
                database,
                resolver=CanonicalResolver(version="2"),
            ).run()

            self.assertEqual(1, len(database.query_canonical_events()))
            self.assertEqual(
                1,
                database.connection.execute(
                    "SELECT COUNT(*) FROM canonical_event_versions"
                ).fetchone()[0],
            )
            self.assertEqual(
                ["1", "2"],
                [
                    row["resolver_version"]
                    for row in database.connection.execute(
                        "SELECT resolver_version FROM candidate_resolutions ORDER BY id"
                    )
                ],
            )

    def test_resolution_run_failure_is_audited(self) -> None:
        class FailingResolver(CanonicalResolver):
            def analyze(self, candidate):
                raise ValueError("synthetic resolution failure")

        with Database(self.database_path) as database:
            self._seed_candidate(
                database,
                source_key="feed:a",
                external_id="a-1",
                title="Failure Event",
                event_date="2026-11-05",
                url="https://example.org/failure",
            )
            with self.assertRaises(RuntimeError):
                ResolutionService(database, resolver=FailingResolver()).run()

            run = database.connection.execute(
                "SELECT status, error FROM resolution_runs"
            ).fetchone()
            self.assertEqual("failed", run["status"])
            self.assertIn("synthetic resolution failure", run["error"])


if __name__ == "__main__":
    unittest.main()
