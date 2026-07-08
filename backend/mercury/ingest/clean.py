"""Article cleaning rules ported from the Foundry transform."""

from __future__ import annotations

import calendar
import hashlib
import html
import re
from dataclasses import dataclass
from datetime import UTC, datetime
from html.parser import HTMLParser
from typing import Any
from urllib.parse import urlparse

import feedparser

from mercury import config

NON_NEWS_PATTERNS = [
    re.compile(pattern, re.IGNORECASE)
    for pattern in (
        r"\blotto\b",
        r"\blottery\b",
        r"\bloteri[ae]\b",
        r"\beurojackpot\b",
        r"\bwyniki\s+lotto\b",
        r"\bhoroskop\b",
        r"\bquiz\b",
        r"\bsudoku\b",
        r"\bkrzy[zż]ówka\b",
        r"\bkrzyzowka\b",
    )
]


class _HTMLStripper(HTMLParser):
    def __init__(self) -> None:
        super().__init__()
        self.parts: list[str] = []

    def handle_data(self, data: str) -> None:
        self.parts.append(data)

    def text(self) -> str:
        return " ".join(self.parts)


@dataclass(frozen=True)
class SourceInfo:
    source_id: str
    name: str
    language: str | None = None
    country: str | None = None


def clean_html(value: str | None) -> str:
    if not value:
        return ""
    stripper = _HTMLStripper()
    stripper.feed(html.unescape(value))
    return re.sub(r"\s+", " ", stripper.text()).strip()


def normalise_datetime(entry: Any) -> str:
    parsed = entry.get("published_parsed") or entry.get("updated_parsed")
    if parsed:
        return datetime.fromtimestamp(calendar.timegm(parsed), tz=UTC).strftime("%Y-%m-%d %H:%M:%S")
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def is_valid_url(url: str | None) -> bool:
    if not url:
        return False
    parsed = urlparse(url)
    return parsed.scheme in {"http", "https"} and bool(parsed.netloc)


def stable_report_id(url: str) -> str:
    return hashlib.sha256(url.encode("utf-8")).hexdigest()


def is_non_news_item(title: str, description: str, url: str) -> bool:
    searchable = f"{title} {description} {url}"
    return any(pattern.search(searchable) for pattern in NON_NEWS_PATTERNS)


def clean_entry(entry: feedparser.FeedParserDict, source: SourceInfo, ingested_at: str | None = None) -> dict[str, Any] | None:
    title = clean_html(entry.get("title"))
    url = (entry.get("link") or entry.get("id") or "").strip()
    if len(title) <= 15 or not is_valid_url(url):
        return None

    description = clean_html(entry.get("summary") or entry.get("description"))
    if is_non_news_item(title, description, url):
        return None

    llm_input_text = f"{title} | {description}"[:500]
    parsed_url = urlparse(url)

    return {
        "report_id": stable_report_id(url),
        "title": title,
        "url": url,
        "source_id": source.source_id,
        "source_domain": source.name or parsed_url.netloc,
        "description": description,
        "language": source.language,
        "source_country": source.country,
        "published_date": normalise_datetime(entry),
        "tone_score": 0.0,
        "query_term": "rss_feed",
        "data_source": "rss",
        "llm_input_text": llm_input_text,
        "ingested_at": ingested_at
        or datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S"),
        "cleaning_version": config.CLEANING_VERSION,
    }
