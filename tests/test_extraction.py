import hashlib
import json
from pathlib import Path
import tempfile
import unittest

from graph_etl.domain import Source, SourceKind
from graph_etl.extraction import (
    EventCandidateExtractor,
    ExtractionError,
    ExtractionService,
)
from graph_etl.extraction.models import StoredObservation, StoredObservationField
from graph_etl.ingestion.models import FieldValue, FetchedDocument, ParsedEntry, ParsedFeed
from graph_etl.ingestion.service import IngestionService
from graph_etl.persistence.database import Database


FIXTURES = Path(__file__).parent / "fixtures"


class FailingExtractor(EventCandidateExtractor):
    def extract(self, observation: StoredObservation):
        raise ValueError("synthetic extractor failure")


def stored_observation(
    observation_id: int,
    external_id: str,
    fields: list[dict[str, object]],
) -> StoredObservation:
    return StoredObservation(
        id=observation_id,
        source_id=1,
        external_id=external_id,
        content_sha256="0" * 64,
        fields=tuple(
            StoredObservationField(
                name=str(field["name"]),
                field_ordinal=int(field.get("field_ordinal", 0)),
                raw_value=field["raw_value"],
                locator=str(field["locator"]),
                is_present=bool(field.get("is_present", field["raw_value"] is not None)),
            )
            for field in fields
        ),
    )


class EventCandidateExtractorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.fixture = json.loads(
            (FIXTURES / "extraction_observation.json").read_text()
        )

    def test_extracts_multiple_candidates_and_preserves_provenance(self) -> None:
        observation = stored_observation(
            1,
            self.fixture["external_id"],
            self.fixture["fields"],
        )

        drafts = EventCandidateExtractor().extract(observation)

        self.assertEqual(2, len(drafts))
        first = drafts[0]
        self.assertEqual(0, first.candidate_ordinal)
        fields = {field.field_name: field for field in first.fields}
        self.assertEqual("  Example   Conference  ", fields["title"].raw_value)
        self.assertEqual("Example Conference", fields["title"].normalized_value)
        self.assertEqual("2026-11-05", fields["event_date"].normalized_value)
        self.assertEqual("date_only", fields["event_date"].normalization_status)
        self.assertEqual(
            "2026-09-25T08:00:00Z",
            fields["publication_timestamp"].normalized_value,
        )
        self.assertEqual("+01:00", fields["publication_timestamp"].source_timezone)
        self.assertEqual("missing", fields["url"].normalization_status)
        self.assertEqual("event_date", fields["event_date"].source_field_name)
        self.assertEqual(0, fields["event_date"].source_field_ordinal)
        self.assertEqual("fixture:item/eventDate[1]", fields["event_date"].locator)
        self.assertEqual(
            "2026-11-06T08:00:00Z",
            {field.field_name: field for field in drafts[1].fields}[
                "event_date"
            ].normalized_value,
        )

    def test_publication_timestamp_is_not_used_as_event_date(self) -> None:
        observation = stored_observation(
            1,
            "publication-only",
            [
                {
                    "name": "title",
                    "raw_value": "Publication only",
                    "locator": "fixture:title",
                    "is_present": True,
                },
                {
                    "name": "published_at",
                    "raw_value": "2026-09-25T08:00:00Z",
                    "locator": "fixture:published",
                    "is_present": True,
                },
            ],
        )

        drafts = EventCandidateExtractor().extract(observation)

        self.assertEqual(1, len(drafts))
        fields = {field.field_name: field for field in drafts[0].fields}
        self.assertEqual("missing", fields["event_date"].normalization_status)
        self.assertEqual("normalized", fields["publication_timestamp"].normalization_status)


class ExtractionIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.temporary_directory = tempfile.TemporaryDirectory()
        self.database_path = Path(self.temporary_directory.name) / "graph_etl.sqlite3"
        self.fixture = json.loads(
            (FIXTURES / "extraction_observation.json").read_text()
        )
        self.source = Source(
            key="feed:fixture",
            name="Fixture Feed",
            kind=SourceKind.FEED,
            uri="https://example.org/feed.xml",
        )

    def tearDown(self) -> None:
        self.temporary_directory.cleanup()

    def _seed_observation(
        self,
        database: Database,
        fields: list[dict[str, object]],
        *,
        external_id: str = "fixture-event-1",
    ) -> int:
        source_id = database.upsert_source(self.source)
        payload = b"<fixture-document/>"
        document_id, _ = database.save_raw_document(
            source_id,
            FetchedDocument(payload, "application/rss+xml"),
            fetched_at="2026-09-26T00:00:00Z",
        )
        run_id = database.start_run(source_id)
        database.attach_document(run_id, document_id)
        entry = ParsedEntry(
            external_id=external_id,
            content_sha256=hashlib.sha256(
                (payload + external_id.encode("utf-8"))
            ).hexdigest(),
            index=0,
            fields=tuple(
                FieldValue(
                    name=str(field["name"]),
                    raw_value=field["raw_value"],
                    locator=str(field["locator"]),
                )
                for field in fields
            ),
        )
        database.persist_feed(
            run_id,
            document_id,
            ParsedFeed("rss", "Fixture Feed", "1", (entry,)),
        )
        return int(
            database.connection.execute(
                "SELECT id FROM observations WHERE external_id = ?",
                (external_id,),
            ).fetchone()[0]
        )

    def test_extraction_is_idempotent_and_version_upgrade_preserves_outputs(self) -> None:
        with Database(self.database_path) as database:
            self._seed_observation(database, self.fixture["fields"])
            first = ExtractionService(database).run(source_key=self.source.key)
            rerun = ExtractionService(database).run(source_key=self.source.key)

            self.assertEqual(2, first.candidates_created)
            self.assertEqual(0, rerun.candidates_created)
            self.assertEqual(2, rerun.candidates_existing)
            run = database.get_extraction_run(first.run_id)
            self.assertEqual("succeeded", run["status"])
            self.assertEqual(1, run["observations_processed"])
            self.assertEqual(2, run["candidates_created"])

            upgraded = EventCandidateExtractor(
                extraction_version="2",
                normalization_version="2",
            )
            upgrade_result = ExtractionService(database, extractor=upgraded).run(
                source_key=self.source.key
            )
            self.assertEqual(2, upgrade_result.candidates_created)
            self.assertEqual(
                4,
                database.connection.execute(
                    "SELECT COUNT(*) FROM event_candidates"
                ).fetchone()[0],
            )
            self.assertEqual(
                2,
                len(
                    database.query_candidates(
                        source_key=self.source.key,
                        extraction_version="1",
                    )
                ),
            )
            self.assertEqual(
                2,
                len(
                    database.query_candidates(
                        source_key=self.source.key,
                        extraction_version="2",
                    )
                ),
            )

    def test_candidate_query_exposes_raw_normalized_and_provenance_fields(self) -> None:
        with Database(self.database_path) as database:
            observation_id = self._seed_observation(database, self.fixture["fields"])
            ExtractionService(database).run(source_key=self.source.key)

            candidates = database.query_candidates(source_key=self.source.key)
            self.assertEqual(2, len(candidates))
            candidate = candidates[0]
            self.assertEqual(observation_id, candidate["observation_id"])
            fields = {field["field_name"]: field for field in candidate["fields"]}
            self.assertEqual(
                "  Example   Conference  ",
                fields["title"]["raw_value"],
            )
            self.assertEqual(
                "Example Conference",
                fields["title"]["normalized_value"],
            )
            self.assertEqual(observation_id, fields["title"]["source_observation_id"])
            self.assertEqual("title", fields["title"]["source_field_name"])
            self.assertEqual(0, fields["title"]["source_field_ordinal"])
            self.assertEqual("fixture:item/title", fields["title"]["locator"])

    def test_invalid_and_ambiguous_event_dates_are_retained_without_guessing(self) -> None:
        fields = [
            {
                "name": "title",
                "raw_value": "Ambiguous date event",
                "locator": "fixture:title",
                "is_present": True,
            },
            {
                "name": "event_date",
                "raw_value": "2026-11-06T09:00:00",
                "locator": "fixture:event-date",
                "is_present": True,
            },
            {
                "name": "event_date",
                "raw_value": "not-a-date",
                "locator": "fixture:event-date-2",
                "is_present": True,
            },
        ]
        with Database(self.database_path) as database:
            self._seed_observation(database, fields)
            ExtractionService(database).run(source_key=self.source.key)
            candidates = database.query_candidates(source_key=self.source.key)

            statuses = sorted(
                field["normalization_status"]
                for candidate in candidates
                for field in candidate["fields"]
                if field["field_name"] == "event_date"
            )
            self.assertEqual(["invalid", "timezone_ambiguous"], statuses)
            self.assertTrue(
                all(
                    field["normalized_value"] is None
                    for candidate in candidates
                    for field in candidate["fields"]
                    if field["field_name"] == "event_date"
                )
            )

    def test_document_membership_is_recorded_for_each_entry(self) -> None:
        with Database(self.database_path) as database:
            self._seed_observation(database, self.fixture["fields"])
            self.assertEqual(
                1,
                database.connection.execute(
                    "SELECT COUNT(*) FROM document_observations"
                ).fetchone()[0],
            )

    def test_extraction_failure_is_recorded_on_run(self) -> None:
        with Database(self.database_path) as database:
            self._seed_observation(database, self.fixture["fields"])
            with self.assertRaises(ExtractionError):
                ExtractionService(database, extractor=FailingExtractor()).run()

            run = database.connection.execute(
                "SELECT status, error FROM extraction_runs"
            ).fetchone()
            self.assertEqual("failed", run["status"])
            self.assertIn("synthetic extractor failure", run["error"])

    def test_explicit_event_date_elements_create_multiple_candidates(self) -> None:
        class StubClient:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def fetch(self, uri: str, *, timeout: float) -> FetchedDocument:
                del uri, timeout
                return FetchedDocument(self.payload, "application/rss+xml")

        source = Source(
            key="feed:event-dates",
            name="Event Dates",
            kind=SourceKind.FEED,
            uri="https://example.org/event-dates.xml",
        )
        with Database(self.database_path) as database:
            IngestionService(
                database,
                StubClient((FIXTURES / "rss_event_dates.xml").read_bytes()),
                sleep=lambda _: None,
            ).ingest(source)
            result = ExtractionService(database).run(source_key=source.key)
            candidates = database.query_candidates(source_key=source.key)

            self.assertEqual(2, result.candidates_created)
            self.assertEqual(2, len(candidates))
            statuses = sorted(
                field["normalization_status"]
                for candidate in candidates
                for field in candidate["fields"]
                if field["field_name"] == "event_date"
            )
            self.assertEqual(["date_only", "normalized"], statuses)

    def test_real_rss_atom_and_rdf_observations_extract_candidates(self) -> None:
        class StubClient:
            def __init__(self, payload: bytes) -> None:
                self.payload = payload

            def fetch(self, uri: str, *, timeout: float) -> FetchedDocument:
                del uri, timeout
                return FetchedDocument(self.payload, "application/xml")

        with Database(self.database_path) as database:
            for format_name in ("rss", "atom", "rdf"):
                source = Source(
                    key=f"feed:{format_name}",
                    name=format_name,
                    kind=SourceKind.FEED,
                    uri=f"https://example.org/{format_name}.xml",
                )
                payload = (FIXTURES / f"{format_name}.xml").read_bytes()
                ingestion = IngestionService(
                    database,
                    StubClient(payload),
                    sleep=lambda _: None,
                )
                ingestion.ingest(source)
                result = ExtractionService(database).run(source_key=source.key)
                candidates = database.query_candidates(source_key=source.key)
                self.assertEqual(1, result.candidates_created, format_name)
                self.assertEqual(1, len(candidates), format_name)
                fields = {
                    field["field_name"]: field
                    for field in candidates[0]["fields"]
                }
                self.assertEqual("missing", fields["event_date"]["normalization_status"])
                self.assertEqual("event_date", fields["event_date"]["source_field_name"])
                self.assertEqual(
                    "normalized",
                    fields["publication_timestamp"]["normalization_status"],
                    format_name,
                )

    def test_source_uri_changes_are_audited(self) -> None:
        with Database(self.database_path) as database:
            database.upsert_source(self.source)
            changed_source = Source(
                key=self.source.key,
                name=self.source.name,
                kind=self.source.kind,
                uri="https://example.org/renamed-feed.xml",
            )
            database.upsert_source(changed_source)

            history = database.connection.execute(
                """
                SELECT previous_uri, current_uri
                FROM source_uri_history
                """
            ).fetchone()
            self.assertEqual(self.source.uri, history["previous_uri"])
            self.assertEqual(changed_source.uri, history["current_uri"])


if __name__ == "__main__":
    unittest.main()
