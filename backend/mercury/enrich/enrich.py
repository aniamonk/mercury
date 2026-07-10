"""Worker that enriches new articles and writes entities/link rows."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from mercury import config
from mercury.enrich.llm import EnrichedArticle, LLMUnavailable, enrich_article
from mercury.events import EventBus

PROMPT_PATH = Path(__file__).parent / "prompts" / "enrich_article.txt"

EXTRA_SOURCE_BLOCKLIST = {
    "bbc",
    "bbc news",
    "reuters",
    "tvp",
}

POLAND_RELEVANCE_MARKERS = (
    "poland",
    "polish",
    "warsaw",
    "sejm",
    "senate of poland",
    "law and justice",
    "civic platform",
    "donald tusk",
    "karol nawrocki",
    "andrzej duda",
    "polish government",
    "polish parliament",
    "polsk",
    "polsce",
    "polski",
    "polska",
    "polskiej",
    "warszaw",
    "rząd pol",
    "rzad pol",
    "sejmu",
    "nfz",
    "zus",
)


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def entity_id(canonical_name: str) -> str:
    return hashlib.sha256(canonical_name.encode("utf-8")).hexdigest()


async def source_blocklist(db: aiosqlite.Connection) -> set[str]:
    cursor = await db.execute("SELECT name FROM sources")
    rows = await cursor.fetchall()
    names = {row["name"].strip().lower() for row in rows}
    return names | EXTRA_SOURCE_BLOCKLIST


def build_prompt(article: aiosqlite.Row) -> str:
    template = PROMPT_PATH.read_text()
    replacements = {
        "{TOPICS}": json.dumps(config.TOPICS),
        "{SUBTOPICS}": json.dumps(config.SUBTOPICS),
        "{LEANINGS}": json.dumps(config.LEANINGS),
        "{ENTITY_TYPES}": json.dumps(config.ENTITY_TYPES),
        "{REPORT_ID}": article["report_id"],
        "{TITLE}": article["title"],
        "{SOURCE}": article["source_domain"] or "",
        "{SOURCE_COUNTRY}": article["source_country"] or "",
        "{PUBLISHED_DATE}": article["published_date"] or "",
        "{LLM_INPUT_TEXT}": article["llm_input_text"] or "",
    }
    for key, value in replacements.items():
        template = template.replace(key, value)
    return template


async def fetch_new_articles(db: aiosqlite.Connection, limit: int) -> list[aiosqlite.Row]:
    window = f"-{config.MONITOR_WINDOW_HOURS} hours"
    cursor = await db.execute(
        """SELECT * FROM articles
           WHERE status = 'new'
             AND datetime(published_date) >= datetime('now', ?)
             AND datetime(published_date) <= datetime('now')
           ORDER BY published_date DESC
           LIMIT ?""",
        (window, limit),
    )
    return await cursor.fetchall()


def _article_and_result_text(article: aiosqlite.Row, result: EnrichedArticle) -> str:
    parts = [
        article["title"] or "",
        article["llm_input_text"] or "",
        result.summary,
        " ".join(entity.canonical_name for entity in result.entities),
        " ".join(entity.surface_form for entity in result.entities),
    ]
    return " ".join(parts).casefold()


def _has_poland_relevance(article: aiosqlite.Row, result: EnrichedArticle) -> bool:
    text = _article_and_result_text(article, result)
    return any(marker.casefold() in text for marker in POLAND_RELEVANCE_MARKERS)


def _has_foreign_domestic_signal(article: aiosqlite.Row, result: EnrichedArticle) -> bool:
    source_country = (article["source_country"] or "").casefold()
    if source_country and source_country != "poland":
        return True
    return any(
        entity.entity_type == "country" and entity.canonical_name.casefold() != "poland"
        for entity in result.entities
    )


def _normalise_topic_scope(article: aiosqlite.Row, result: EnrichedArticle) -> EnrichedArticle:
    if result.topic != "domestic" or _has_poland_relevance(article, result):
        return result
    if _has_foreign_domestic_signal(article, result):
        return result.model_copy(update={"topic": "other"})
    return result


async def _upsert_entity(
    db: aiosqlite.Connection,
    *,
    canonical_name: str,
    surface_form: str,
    entity_type: str,
    first_seen_date: str,
) -> str:
    eid = entity_id(canonical_name)
    cursor = await db.execute("SELECT aliases FROM entities WHERE entity_id = ?", (eid,))
    row = await cursor.fetchone()
    aliases = {canonical_name, surface_form}
    if row:
        try:
            aliases.update(json.loads(row["aliases"] or "[]"))
        except json.JSONDecodeError:
            pass
        await db.execute(
            "UPDATE entities SET entity_type = COALESCE(entity_type, ?), aliases = ? WHERE entity_id = ?",
            (entity_type, json.dumps(sorted(aliases)), eid),
        )
    else:
        await db.execute(
            """INSERT INTO entities
               (entity_id, entity_name, entity_type, aliases, first_seen_date)
               VALUES (?, ?, ?, ?, ?)""",
            (eid, canonical_name, entity_type, json.dumps(sorted(aliases)), first_seen_date),
        )
    return eid


async def enrich_one(db: aiosqlite.Connection, bus: EventBus, article: aiosqlite.Row) -> None:
    try:
        result = _normalise_topic_scope(article, await enrich_article(build_prompt(article)))
        blocked_names = await source_blocklist(db)
        now = utc_now()

        for entity in result.entities:
            canonical = entity.canonical_name.strip()
            if canonical.lower() in blocked_names:
                continue
            eid = await _upsert_entity(
                db,
                canonical_name=canonical,
                surface_form=entity.surface_form.strip(),
                entity_type=entity.entity_type,
                first_seen_date=article["published_date"] or now,
            )
            await db.execute(
                """INSERT OR IGNORE INTO report_entity_links
                   (report_id, entity_id, linked_at)
                   VALUES (?, ?, ?)""",
                (article["report_id"], eid, now),
            )

        await db.execute(
            """UPDATE articles
               SET summary = ?, topic = ?, subtopics = ?, distilled_topics = ?, leaning = ?,
                   status = 'enriched', enriched_at = ?, enrich_error = NULL
               WHERE report_id = ?""",
            (
                result.summary,
                result.topic,
                json.dumps(result.subtopics),
                json.dumps(result.distilled_topics),
                result.leaning,
                now,
                article["report_id"],
            ),
        )
        await db.commit()
        await bus.publish(
            "article.enriched",
            {"report_id": article["report_id"], "topic": result.topic, "subtopics": result.subtopics},
        )
    except Exception as exc:
        await db.execute(
            """UPDATE articles
               SET status = 'failed', enrich_error = ?
               WHERE report_id = ?""",
            (f"{type(exc).__name__}: {exc}", article["report_id"]),
        )
        await db.commit()
        await bus.publish("article.failed", {"report_id": article["report_id"], "error": str(exc)})


async def run_enrich_worker(db: aiosqlite.Connection, bus: EventBus) -> None:
    warned_no_key = False
    while True:
        if not config.OPENROUTER_API_KEY:
            if not warned_no_key:
                print("Mercury enrichment idle: OPENROUTER_API_KEY is not set")
                warned_no_key = True
            await asyncio.sleep(30)
            continue

        articles = await fetch_new_articles(db, config.ENRICH_CONCURRENCY)
        if not articles:
            await asyncio.sleep(5)
            continue

        try:
            await asyncio.gather(*(enrich_one(db, bus, article) for article in articles))
        except LLMUnavailable:
            await asyncio.sleep(30)
