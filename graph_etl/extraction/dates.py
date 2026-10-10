from datetime import UTC, date, datetime
from email.utils import parsedate_to_datetime
import re

from .models import NormalizationStatus, NormalizedDate


_TIMEZONE_SUFFIX = re.compile(r"(Z|z|[+-]\d{2}:?\d{2})\s*$")
_NAMED_ZONES = (
    "GMT",
    "UTC",
    "UT",
    "EST",
    "EDT",
    "CST",
    "CDT",
    "MST",
    "MDT",
    "PST",
    "PDT",
    "CET",
    "CEST",
    "EET",
    "EEST",
    "BST",
    "IST",
    "JST",
    "AEST",
    "AEDT",
)


def normalize_date(raw_value: str | None) -> NormalizedDate:
    if raw_value is None:
        return NormalizedDate(NormalizationStatus.MISSING, None, None, None)
    text = raw_value.strip()
    if not text:
        return NormalizedDate(NormalizationStatus.MISSING, None, None, None)

    try:
        parsed_date = date.fromisoformat(text)
    except ValueError:
        parsed_date = None
    if parsed_date is not None:
        return NormalizedDate(
            NormalizationStatus.DATE_ONLY,
            parsed_date.isoformat(),
            None,
            None,
        )

    parsed_datetime: datetime | None = None
    iso_text = f"{text[:-1]}+00:00" if text.endswith(("Z", "z")) else text
    try:
        parsed_datetime = datetime.fromisoformat(iso_text)
    except ValueError:
        try:
            parsed_datetime = parsedate_to_datetime(text)
        except (TypeError, ValueError, IndexError, OverflowError):
            parsed_datetime = None

    if parsed_datetime is None:
        return NormalizedDate(
            NormalizationStatus.INVALID,
            None,
            _source_timezone(text),
            "unrecognized date format",
        )
    if parsed_datetime.tzinfo is None or parsed_datetime.utcoffset() is None:
        source_timezone = _source_timezone(text)
        if source_timezone is not None:
            return NormalizedDate(
                NormalizationStatus.INVALID,
                None,
                source_timezone,
                "timezone could not be interpreted",
            )
        return NormalizedDate(
            NormalizationStatus.TIMEZONE_AMBIGUOUS,
            None,
            None,
            "timezone offset is missing",
        )

    normalized = parsed_datetime.astimezone(UTC).isoformat().replace("+00:00", "Z")
    return NormalizedDate(
        NormalizationStatus.NORMALIZED,
        normalized,
        _source_timezone(text),
        None,
    )


def _source_timezone(text: str) -> str | None:
    match = _TIMEZONE_SUFFIX.search(text)
    if match is not None:
        token = match.group(1)
        if token in {"Z", "z"}:
            return "Z"
        sign = token[0]
        digits = token[1:].replace(":", "")
        return f"{sign}{digits[:2]}:{digits[2:]}"
    upper_text = text.upper()
    for zone in _NAMED_ZONES:
        if upper_text.endswith(zone):
            return zone
    return None
