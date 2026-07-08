"""Worker that enriches new articles and writes entities/link rows."""

from __future__ import annotations

import asyncio
import hashlib
import json
from datetime import UTC, datetime
from pathlib import Path

import aiosqlite

from mercury import config
from mercury.enrich.llm import LLMUnavailable, enrich_article
from mercury.events import EventBus

PROMPT_PATH = Path(__file__).parent / "prompts" / "enrich_article.txt"

EXTRA_SOURCE_BLOCKLIST = {
    "bbc",
    "bbc news",
    "reuters",
    "tvp",
}


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
        "{PUBLISHED_DATE}": article["published_date"] or "",
        "{LLM_INPUT_TEXT}": article["llm_input_text"] or "",
    }
    for key, value in replacements.items():
        template = template.replace(key, value)
    return template


async def fetch_new_articles(db: aiosqlite.Connection, limit: int) -> list[aiosqlite.Row]:
    cursor = await db.execute(
        """SELECT * FROM articles
           WHERE status = 'new'
           ORDER BY published_date DESC
           LIMIT ?""",
        (limit,),
    )
    return await cursor.fetchall()


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
        result = await enrich_article(build_prompt(article))
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
               SET summary = ?, topic = ?, subtopics = ?, leaning = ?,
                   status = 'enriched', enriched_at = ?, enrich_error = NULL
               WHERE report_id = ?""",
            (
                result.summary,
                result.topic,
                json.dumps(result.subtopics),
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

