"""Async RSS poller with conditional GET and per-feed failure isolation."""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime
from typing import Any

import aiosqlite
import feedparser
import httpx

from mercury import config
from mercury.events import EventBus
from mercury.ingest.clean import SourceInfo, clean_entry

USER_AGENT = "Mercury/0.1 local media monitor"


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


async def active_sources(db: aiosqlite.Connection) -> list[aiosqlite.Row]:
    cursor = await db.execute(
        """SELECT * FROM sources
           WHERE active = 1 AND feed_url IS NOT NULL AND feed_url != ''
           ORDER BY name"""
    )
    return await cursor.fetchall()


async def insert_article(db: aiosqlite.Connection, article: dict[str, Any]) -> bool:
    cursor = await db.execute(
        """INSERT OR IGNORE INTO articles
           (report_id, title, url, source_id, source_domain, description, language,
            source_country, published_date, tone_score, query_term, data_source,
            llm_input_text, ingested_at, cleaning_version)
           VALUES
           (:report_id, :title, :url, :source_id, :source_domain, :description, :language,
            :source_country, :published_date, :tone_score, :query_term, :data_source,
            :llm_input_text, :ingested_at, :cleaning_version)""",
        article,
    )
    return cursor.rowcount == 1


async def poll_source(
    db: aiosqlite.Connection,
    bus: EventBus,
    source: aiosqlite.Row,
    client: httpx.AsyncClient,
) -> int:
    headers = {"User-Agent": USER_AGENT}
    if source["etag"]:
        headers["If-None-Match"] = source["etag"]
    if source["last_modified"]:
        headers["If-Modified-Since"] = source["last_modified"]

    inserted = 0
    now = utc_now()
    try:
        response = await client.get(source["feed_url"], headers=headers)
        if response.status_code == 304:
            await db.execute(
                "UPDATE sources SET last_polled_at = ?, last_status = ? WHERE source_id = ?",
                (now, "304 not modified", source["source_id"]),
            )
            await db.commit()
            return 0
        response.raise_for_status()
        parsed = feedparser.parse(response.content)
        if parsed.bozo and not parsed.entries:
            raise ValueError(str(parsed.bozo_exception))

        source_info = SourceInfo(
            source_id=source["source_id"],
            name=source["name"],
            language=source["language"],
            country=source["country"],
        )
        for entry in parsed.entries:
            article = clean_entry(entry, source_info, ingested_at=now)
            if article and await insert_article(db, article):
                inserted += 1
                await bus.publish(
                    "article.new",
                    {
                        "report_id": article["report_id"],
                        "source_id": source["source_id"],
                        "title": article["title"],
                    },
                )

        await db.execute(
            """UPDATE sources
               SET last_polled_at = ?, last_status = ?, etag = COALESCE(?, etag),
                   last_modified = COALESCE(?, last_modified), active = 1
               WHERE source_id = ?""",
            (
                now,
                f"ok: {len(parsed.entries)} entries, {inserted} new",
                response.headers.get("etag"),
                response.headers.get("last-modified"),
                source["source_id"],
            ),
        )
        await db.commit()
        return inserted
    except Exception as exc:
        await db.execute(
            "UPDATE sources SET last_polled_at = ?, last_status = ? WHERE source_id = ?",
            (now, f"error: {type(exc).__name__}: {exc}", source["source_id"]),
        )
        await db.commit()
        await bus.publish(
            "source.error",
            {"source_id": source["source_id"], "name": source["name"], "error": str(exc)},
        )
        return 0


async def poll_once(db: aiosqlite.Connection, bus: EventBus) -> int:
    sources = await active_sources(db)
    limits = httpx.Limits(max_connections=10, max_keepalive_connections=5)
    async with httpx.AsyncClient(timeout=30, follow_redirects=True, limits=limits) as client:
        results = await asyncio.gather(*(poll_source(db, bus, source, client) for source in sources))
    total = sum(results)
    if total:
        await bus.publish("poll.complete", {"inserted": total})
    return total


async def _poll_loop_for_source(
    db: aiosqlite.Connection,
    bus: EventBus,
    source_id: str,
    initial_delay: float,
) -> None:
    await asyncio.sleep(initial_delay)
    limits = httpx.Limits(max_connections=4, max_keepalive_connections=2)
    async with httpx.AsyncClient(timeout=30, follow_redirects=True, limits=limits) as client:
        while True:
            cursor = await db.execute("SELECT * FROM sources WHERE source_id = ?", (source_id,))
            source = await cursor.fetchone()
            if not source or not source["active"] or not source["feed_url"]:
                return
            await poll_source(db, bus, source, client)
            await asyncio.sleep(source["poll_seconds"] or config.DEFAULT_POLL_SECONDS)


async def start_pollers(db: aiosqlite.Connection, bus: EventBus) -> list[asyncio.Task[None]]:
    tasks: list[asyncio.Task[None]] = []
    for index, source in enumerate(await active_sources(db)):
        tasks.append(
            asyncio.create_task(
                _poll_loop_for_source(db, bus, source["source_id"], initial_delay=index * 2.0),
                name=f"poll:{source['source_id']}",
            )
        )
    return tasks

