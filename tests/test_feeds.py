from pathlib import Path
import unittest

from graph_etl.ingestion.feeds import RssAtomFeedParser
from graph_etl.ingestion.models import FeedParseError


FIXTURES = Path(__file__).parent / "fixtures"


class RssAtomFeedParserTests(unittest.TestCase):
    def setUp(self) -> None:
        self.parser = RssAtomFeedParser()

    def test_parses_rss_with_provenance_and_missing_fields(self) -> None:
        feed = self.parser.parse((FIXTURES / "rss.xml").read_bytes())

        self.assertEqual("rss", feed.feed_format)
        self.assertEqual(RssAtomFeedParser.VERSION, feed.parser_version)
        self.assertEqual("Python Tech Calendar", feed.title)
        self.assertEqual(1, len(feed.entries))
        entry = feed.entries[0]
        self.assertEqual("pydata-london-2026", entry.external_id)
        self.assertEqual(64, len(entry.content_sha256))
        fields = {field.name: field for field in entry.fields}
        self.assertEqual("PyData London", fields["title"].raw_value)
        self.assertEqual("rss:item[1]/guid", fields["identifier"].locator)
        self.assertTrue(fields["content"].is_present)
        self.assertEqual(
            "rss:item[1]/content:encoded",
            fields["content"].locator,
        )
        self.assertFalse(fields["updated_at"].is_present)
        self.assertEqual(
            "rss:item[1]#absent:updated_at",
            fields["updated_at"].locator,
        )

    def test_parses_atom_link_and_author(self) -> None:
        feed = self.parser.parse((FIXTURES / "atom.xml").read_bytes())

        self.assertEqual("atom", feed.feed_format)
        fields = {field.name: field for field in feed.entries[0].fields}
        self.assertEqual("oss-systems-2026", fields["identifier"].raw_value)
        self.assertEqual(
            "https://example.org/oss-systems-2026",
            fields["link"].raw_value,
        )
        self.assertEqual("Program Committee", fields["authors"].raw_value)
        self.assertEqual(
            "atom:entry[1]/author[1]/name",
            fields["authors"].locator,
        )
        self.assertEqual("atom:entry[1]/updated", fields["updated_at"].locator)

    def test_parses_rdf_feed_with_dublin_core_date(self) -> None:
        feed = self.parser.parse((FIXTURES / "rdf.xml").read_bytes())

        self.assertEqual("rdf", feed.feed_format)
        fields = {field.name: field for field in feed.entries[0].fields}
        self.assertEqual("rdf:item[1]/date", fields["published_at"].locator)
        self.assertEqual("2026-11-05T09:00:00+00:00", fields["published_at"].raw_value)
        self.assertEqual(
            "https://example.org/events/rdf-summit",
            feed.entries[0].external_id,
        )

    def test_entry_hash_excludes_surrounding_whitespace(self) -> None:
        compact = b"<rss><channel><title>Feed</title><item><guid>a</guid></item></channel></rss>"
        padded = b"<rss><channel><title>Feed</title><item><guid>a</guid></item>\n</channel></rss>"

        first = self.parser.parse(compact).entries[0]
        second = self.parser.parse(padded).entries[0]

        self.assertEqual(first.content_sha256, second.content_sha256)

    def test_rejects_malformed_xml(self) -> None:
        with self.assertRaises(FeedParseError):
            self.parser.parse(b"<rss><channel></rss>")

    def test_rejects_unsupported_feed(self) -> None:
        with self.assertRaises(FeedParseError):
            self.parser.parse(b"<html><body>Not a feed</body></html>")


if __name__ == "__main__":
    unittest.main()
