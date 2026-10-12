import hashlib
import json
import re
import unicodedata
from urllib.parse import urlsplit


def normalize_title(value: str | None) -> str | None:
    if value is None:
        return None
    normalized = unicodedata.normalize("NFKC", value).casefold()
    normalized = re.sub(r"[^\w\s]", " ", normalized, flags=re.UNICODE)
    return " ".join(normalized.split())


def normalize_url(value: str | None) -> str | None:
    if value is None:
        return None
    trimmed = value.strip()
    if not trimmed:
        return None
    parsed = urlsplit(trimmed)
    if not parsed.scheme or not parsed.netloc:
        return trimmed.casefold().rstrip("/")
    host = parsed.netloc.casefold()
    if host.startswith("www."):
        host = host[4:]
    path = parsed.path.rstrip("/") or "/"
    return f"{host}{path}"


def title_date_identity_key(
    title: str | None,
    event_date: str | None,
) -> str | None:
    if title is None or event_date is None:
        return None
    payload = json.dumps(
        {"title": normalize_title(title), "event_date": event_date},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()


def source_identity_key(source_key: str, external_id: str) -> str | None:
    if not source_key or not external_id:
        return None
    payload = json.dumps(
        {"source_key": source_key, "external_id": external_id},
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    )
    return hashlib.sha256(payload.encode("utf-8")).hexdigest()