from collections.abc import Callable
from datetime import UTC, datetime
from time import sleep

from graph_etl.domain import Source
from graph_etl.ingestion.feeds import RssAtomFeedParser
from graph_etl.ingestion.http import FeedClient
from graph_etl.ingestion.models import (
    FeedParseError,
    IngestionError,
    IngestionResult,
    SourceFetchError,
)
from graph_etl.persistence.database import Database, timestamp

Sleep = Callable[[float], None]


class IngestionService:
    def __init__(
        self,
        database: Database,
        client: FeedClient,
        *,
        parser: RssAtomFeedParser | None = None,
        sleep: Sleep = sleep,
        clock: Callable[[], datetime] | None = None,
    ) -> None:
        self.database = database
        self.client = client
        self.parser = parser or RssAtomFeedParser()
        self.sleep = sleep
        self.clock = clock or (lambda: datetime.now(UTC))

    def ingest(
        self,
        source: Source,
        *,
        max_attempts: int = 3,
        retry_delay: float = 1.0,
        timeout: float = 20.0,
    ) -> IngestionResult:
        if max_attempts < 1:
            raise ValueError("max_attempts must be positive")
        if retry_delay < 0:
            raise ValueError("retry_delay cannot be negative")
        if timeout <= 0:
            raise ValueError("timeout must be positive")

        source_id = self.database.upsert_source(source)
        for attempt in range(1, max_attempts + 1):
            run_id = self.database.start_run(source_id)
            try:
                fetched = self.client.fetch(source.uri, timeout=timeout)
            except SourceFetchError as error:
                self.database.fail_run(run_id, error)
                if not error.retryable or attempt == max_attempts:
                    raise IngestionError(str(error)) from error
                self.sleep(min(retry_delay * 2 ** (attempt - 1), 60.0))
                continue
            except Exception as error:
                self.database.fail_run(run_id, error)
                raise IngestionError("feed fetch failed") from error

            try:
                document_id, document_created = self.database.save_raw_document(
                    source_id,
                    fetched,
                    fetched_at=timestamp(self.clock()),
                )
                self.database.attach_document(run_id, document_id)
                feed = self.parser.parse(fetched.content)
                inserted, existing = self.database.persist_feed(
                    run_id,
                    document_id,
                    feed,
                )
            except FeedParseError as error:
                self.database.fail_run(run_id, error)
                raise IngestionError(str(error)) from error
            except Exception as error:
                self.database.fail_run(run_id, error)
                raise IngestionError("raw observation persistence failed") from error

            return IngestionResult(
                source_key=source.key,
                run_id=run_id,
                document_id=document_id,
                attempts=attempt,
                raw_document_created=document_created,
                observations_inserted=inserted,
                observations_existing=existing,
            )

        raise IngestionError("ingestion exhausted without a result")
