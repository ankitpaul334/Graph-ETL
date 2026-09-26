import hashlib
import xml.etree.ElementTree as ET

from .models import FeedParseError, FieldValue, ParsedEntry, ParsedFeed


class RssAtomFeedParser:
    VERSION = "1"

    _field_names = (
        "title",
        "link",
        "identifier",
        "published_at",
        "updated_at",
        "summary",
        "content",
        "authors",
    )

    def parse(self, payload: bytes) -> ParsedFeed:
        try:
            root = ET.fromstring(payload)
        except (ET.ParseError, ValueError) as error:
            raise FeedParseError("feed is not well-formed XML") from error

        root_name = self._local_name(root.tag)
        normalized_root = root_name.lower()
        if normalized_root == "feed":
            return self._parse_atom(root)
        if normalized_root == "rss":
            return self._parse_rss(root)
        if normalized_root == "rdf":
            return self._parse_rdf(root)
        raise FeedParseError(f"unsupported feed root element: {root_name}")

    def _parse_rss(self, root: ET.Element) -> ParsedFeed:
        channel = self._first_child(root, "channel")
        container = channel if channel is not None else root
        entries = self._children(container, "item")
        title = self._child_text(container, "title")
        return self._build_feed("rss", title, entries)

    def _parse_rdf(self, root: ET.Element) -> ParsedFeed:
        channel = self._first_child(root, "channel")
        title = self._child_text(channel, "title") if channel is not None else None
        return self._build_feed("rdf", title, self._children(root, "item"))

    def _parse_atom(self, root: ET.Element) -> ParsedFeed:
        return self._build_feed(
            "atom",
            self._child_text(root, "title"),
            self._children(root, "entry"),
        )

    def _build_feed(
        self,
        feed_format: str,
        title: str | None,
        entries: list[ET.Element],
    ) -> ParsedFeed:
        parsed_entries = tuple(
            self._parse_entry(entry, feed_format, index)
            for index, entry in enumerate(entries)
        )
        return ParsedFeed(feed_format, title, self.VERSION, parsed_entries)

    def _parse_entry(
        self,
        entry: ET.Element,
        feed_format: str,
        index: int,
    ) -> ParsedEntry:
        entry.tail = None
        serialized = ET.tostring(entry, encoding="utf-8")
        content_sha256 = hashlib.sha256(serialized).hexdigest()
        item_name = "entry" if feed_format == "atom" else "item"
        item_path = f"{feed_format}:{item_name}[{index + 1}]"
        values: dict[str, list[FieldValue]] = {name: [] for name in self._field_names}

        if feed_format == "atom":
            self._add_child_text(values, entry, "title", f"{item_path}/title")
            self._add_atom_link(values, entry, item_path)
            self._add_child_text(
                values,
                entry,
                "id",
                f"{item_path}/id",
                field_name="identifier",
            )
            self._add_child_text(
                values,
                entry,
                "published",
                f"{item_path}/published",
                field_name="published_at",
            )
            self._add_child_text(
                values,
                entry,
                "updated",
                f"{item_path}/updated",
                field_name="updated_at",
            )
            self._add_child_text(values, entry, "summary", f"{item_path}/summary")
            self._add_child_text(values, entry, "content", f"{item_path}/content")
            for author_index, author in enumerate(self._children(entry, "author"), 1):
                self._add_text(
                    values,
                    "authors",
                    self._child_text(author, "name"),
                    f"{item_path}/author[{author_index}]/name",
                )
        else:
            self._add_child_text(values, entry, "title", f"{item_path}/title")
            self._add_child_text(values, entry, "link", f"{item_path}/link")
            self._add_child_text(
                values,
                entry,
                "guid",
                f"{item_path}/guid",
                field_name="identifier",
            )
            self._add_rss_date(values, entry, item_path)
            self._add_child_text(
                values,
                entry,
                "description",
                f"{item_path}/description",
                field_name="summary",
            )
            content = self._first_child(entry, "encoded")
            content_locator = f"{item_path}/content:encoded"
            if content is None:
                content = self._first_child(entry, "content")
                content_locator = f"{item_path}/content"
            if content is not None:
                self._add_text(
                    values,
                    "content",
                    self._element_text(content),
                    content_locator,
                )
            self._add_child_text(
                values,
                entry,
                "author",
                f"{item_path}/author",
                field_name="authors",
            )

        fields: list[FieldValue] = []
        for name in self._field_names:
            if values[name]:
                fields.extend(values[name])
            else:
                fields.append(FieldValue(name, None, f"{item_path}#absent:{name}"))

        external_id = self._entry_external_id(values, content_sha256)
        return ParsedEntry(external_id, content_sha256, index, tuple(fields))

    def _add_rss_date(
        self,
        values: dict[str, list[FieldValue]],
        entry: ET.Element,
        item_path: str,
    ) -> None:
        published = self._first_child(entry, "pubDate")
        if published is not None:
            self._add_text(
                values,
                "published_at",
                self._element_text(published),
                f"{item_path}/pubDate",
            )
        date = self._first_child(entry, "date")
        if date is not None:
            self._add_text(
                values,
                "published_at",
                self._element_text(date),
                f"{item_path}/date",
            )

    def _add_atom_link(
        self,
        values: dict[str, list[FieldValue]],
        entry: ET.Element,
        item_path: str,
    ) -> None:
        links = self._children(entry, "link")
        selected = next(
            (link for link in links if link.get("rel") == "alternate"),
            next((link for link in links if link.get("rel") is None), None),
        )
        if selected is None and links:
            selected = links[0]
        if selected is None:
            return
        value = selected.get("href")
        if value is None:
            value = self._element_text(selected)
        if value is not None:
            self._add_text(values, "link", value, f"{item_path}/link/@href")

    def _add_child_text(
        self,
        values: dict[str, list[FieldValue]],
        element: ET.Element,
        child_name: str,
        locator: str,
        *,
        field_name: str | None = None,
    ) -> None:
        child = self._first_child(element, child_name)
        if child is not None:
            self._add_text(
                values,
                field_name or child_name,
                self._element_text(child),
                locator,
            )

    def _add_text(
        self,
        values: dict[str, list[FieldValue]],
        name: str,
        value: str | None,
        locator: str,
    ) -> None:
        if value is not None:
            values[name].append(FieldValue(name, value, locator))

    def _entry_external_id(
        self,
        values: dict[str, list[FieldValue]],
        content_sha256: str,
    ) -> str:
        for name in ("identifier", "link"):
            if values[name]:
                value = values[name][0].raw_value
                if value:
                    return value
        return f"sha256:{content_sha256}"

    @staticmethod
    def _local_name(tag: str) -> str:
        return tag.rsplit("}", 1)[-1]

    @classmethod
    def _children(cls, element: ET.Element, name: str) -> list[ET.Element]:
        return [child for child in element if cls._local_name(child.tag) == name]

    @classmethod
    def _first_child(cls, element: ET.Element | None, name: str) -> ET.Element | None:
        if element is None:
            return None
        return next(iter(cls._children(element, name)), None)

    @classmethod
    def _child_text(cls, element: ET.Element | None, name: str) -> str | None:
        child = cls._first_child(element, name)
        return cls._element_text(child) if child is not None else None

    @staticmethod
    def _element_text(element: ET.Element) -> str:
        return "".join(element.itertext())
