"""FastAPI application for Mercury."""

from __future__ import annotations

import asyncio
import json
import os
import re
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from pathlib import Path
from typing import Annotated, Any

import aiosqlite
from fastapi import FastAPI, HTTPException, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field, field_validator

from mercury import config
from mercury.db.database import close_db, get_db
from mercury.enrich.enrich import enrich_one, fetch_new_articles, run_enrich_worker
from mercury.events import EventBus, events
from mercury.ingest.poller import poll_once, start_pollers
from mercury.stories.stories import analyse_focused, refresh_dashboard_stories, run_story_refresh_loop


class AnalyseRequest(BaseModel):
    categories: list[str] = Field(default_factory=list)
    topics: list[str] = Field(default_factory=list)
    subtopics: list[str] = Field(default_factory=list)

    @field_validator("categories")
    @classmethod
    def valid_categories(cls, value: list[str]) -> list[str]:
        invalid = [item for item in value if item not in config.TOPICS]
        if invalid:
            raise ValueError(f"invalid categories: {invalid}")
        return value

    @field_validator("topics")
    @classmethod
    def clean_topics(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in value:
            topic = re.sub(r"\s+", " ", item).strip()
            key = topic.lower()
            if topic and key not in seen:
                cleaned.append(topic[:60])
                seen.add(key)
        return cleaned

    @field_validator("subtopics")
    @classmethod
    def valid_subtopics(cls, value: list[str]) -> list[str]:
        invalid = [item for item in value if item not in config.SUBTOPICS]
        if invalid:
            raise ValueError(f"invalid subtopics: {invalid}")
        return value


class UpdateRequest(BaseModel):
    poll_feeds: bool = True
    enrich_limit: int = Field(default=12, ge=0, le=100)
    refresh_stories: bool = True


def _json_array(value: str | None) -> list[Any]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _article(row: aiosqlite.Row) -> dict[str, Any]:
    keys = row.keys()
    return {
        "report_id": row["report_id"],
        "title": row["title"],
        "url": row["url"],
        "source_id": row["source_id"],
        "source_domain": row["source_domain"],
        "source_political_leaning": row["source_political_leaning"] if "source_political_leaning" in keys else None,
        "description": row["description"],
        "language": row["language"],
        "source_country": row["source_country"],
        "published_date": row["published_date"],
        "summary": row["summary"],
        "topic": row["topic"],
        "subtopics": _json_array(row["subtopics"]),
        "distilled_topics": _json_array(row["distilled_topics"] if "distilled_topics" in keys else None),
        "leaning": row["leaning"],
        "status": row["status"],
    }


def _story(row: aiosqlite.Row) -> dict[str, Any]:
    return {
        "scope": row["scope"],
        "story_id": row["story_id"],
        "story_title": row["story_title"],
        "synthesis": row["synthesis"],
        "framing_notes": row["framing_notes"],
        "member_report_ids": _json_array(row["member_report_ids"]),
        "generated_at": row["generated_at"],
    }


def _entity(row: aiosqlite.Row) -> dict[str, Any]:
    return {
        "entity_id": row["entity_id"],
        "entity_name": row["entity_name"],
        "entity_type": row["entity_type"],
        "aliases": _json_array(row["aliases"]),
        "first_seen_date": row["first_seen_date"],
        "total_mention_count": row["total_mention_count"],
        "recent_mention_count": row["recent_mention_count"],
        "salience_tier": row["salience_tier"],
        "mention_velocity": row["mention_velocity"],
    }


def _filter_option(value: str, count: int) -> dict[str, Any]:
    label = value.replace("_", " ").replace("-", " ")
    return {"value": value, "label": label, "count": count}


async def _source_names(db: aiosqlite.Connection) -> set[str]:
    cursor = await db.execute("SELECT name FROM sources")
    rows = await cursor.fetchall()
    return {row["name"].lower() for row in rows}


def _fts_match_query(q: str) -> str:
    terms = re.findall(r"[\w-]+", q.lower())
    if not terms:
        return '""'
    return " OR ".join(f"{term}*" for term in terms[:8])


async def _count_rows(db: aiosqlite.Connection, sql: str, params: tuple[Any, ...] = ()) -> int:
    cursor = await db.execute(sql, params)
    row = await cursor.fetchone()
    return int(row[0])


async def _monitoring_status(db: aiosqlite.Connection) -> dict[str, Any]:
    window = f"-{config.MONITOR_WINDOW_HOURS} hours"
    cursor = await db.execute(
        """SELECT
               COUNT(*) AS reports,
               SUM(CASE WHEN status = 'enriched' THEN 1 ELSE 0 END) AS enriched,
               SUM(CASE WHEN status = 'new' THEN 1 ELSE 0 END) AS pending,
               SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed
           FROM articles
           WHERE datetime(published_date) >= datetime('now', ?)
             AND datetime(published_date) <= datetime('now')""",
        (window,),
    )
    row = await cursor.fetchone()
    reports = int(row["reports"] or 0)
    enriched = int(row["enriched"] or 0)
    poll_cursor = await db.execute("SELECT MAX(last_polled_at) AS last_polled_at FROM sources")
    poll_row = await poll_cursor.fetchone()
    state_cursor = await db.execute(
        """SELECT scope, last_attempted_at, last_successful_at, status,
                  eligible_articles, story_count, detail
           FROM story_refresh_state
           WHERE scope IN ('domestic', 'poland_uk')
           ORDER BY scope"""
    )
    return {
        "window_hours": config.MONITOR_WINDOW_HOURS,
        "reports": reports,
        "enriched": enriched,
        "pending": int(row["pending"] or 0),
        "failed": int(row["failed"] or 0),
        "coverage_percent": round((enriched / reports) * 100, 1) if reports else 100.0,
        "last_polled_at": poll_row["last_polled_at"],
        "story_scopes": [dict(state) for state in await state_cursor.fetchall()],
    }


async def _run_manual_enrichment(db: aiosqlite.Connection, bus: EventBus, limit: int) -> dict[str, int]:
    if limit <= 0 or not config.OPENROUTER_API_KEY:
        return {"attempted": 0, "enriched": 0, "failed": 0}

    rows = await fetch_new_articles(db, limit)
    if not rows:
        return {"attempted": 0, "enriched": 0, "failed": 0}

    semaphore = asyncio.Semaphore(max(1, min(config.ENRICH_CONCURRENCY, limit)))

    async def bounded_enrich(row: aiosqlite.Row) -> None:
        async with semaphore:
            await enrich_one(db, bus, row)

    await asyncio.gather(*(bounded_enrich(row) for row in rows))
    report_ids = [row["report_id"] for row in rows]
    placeholders = ",".join("?" for _ in report_ids)
    enriched = await _count_rows(
        db,
        f"SELECT COUNT(*) FROM articles WHERE status = 'enriched' AND report_id IN ({placeholders})",
        tuple(report_ids),
    )
    failed = await _count_rows(
        db,
        f"SELECT COUNT(*) FROM articles WHERE status = 'failed' AND report_id IN ({placeholders})",
        tuple(report_ids),
    )
    return {"attempted": len(rows), "enriched": enriched, "failed": failed}


async def _background_tasks(db: aiosqlite.Connection, bus: EventBus) -> list[asyncio.Task[Any]]:
    tasks: list[asyncio.Task[Any]] = []
    if os.environ.get("MERCURY_START_WORKERS", "1") == "0":
        return tasks
    tasks.extend(await start_pollers(db, bus))
    tasks.append(asyncio.create_task(run_enrich_worker(db, bus), name="enrich"))
    tasks.append(asyncio.create_task(run_story_refresh_loop(db, bus), name="stories"))
    return tasks


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    db = await get_db()
    bus = events
    app.state.db = db
    app.state.bus = bus
    app.state.update_lock = asyncio.Lock()
    tasks = await _background_tasks(db, bus)
    try:
        yield
    finally:
        for task in tasks:
            task.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        await close_db()


def create_app() -> FastAPI:
    app = FastAPI(title="Mercury", lifespan=lifespan)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[
            "http://localhost:5173",
            "http://127.0.0.1:5173",
            f"http://{config.HOST}:5173",
        ],
        allow_credentials=True,
        allow_methods=["*"],
        allow_headers=["*"],
    )

    @app.get("/api/health")
    async def health() -> dict[str, str]:
        return {"status": "ok"}

    @app.get("/api/stories/{scope}")
    async def stories(scope: str, request: Request) -> list[dict[str, Any]]:
        if scope not in {"domestic", "poland_uk", "focused"}:
            raise HTTPException(status_code=404, detail="unknown story scope")
        db: aiosqlite.Connection = request.app.state.db
        window = f"-{config.MONITOR_WINDOW_HOURS} hours"
        cursor = await db.execute(
            """SELECT * FROM stories
               WHERE scope = ?
                 AND NOT EXISTS (
                     SELECT 1
                     FROM json_each(stories.member_report_ids) AS member
                     LEFT JOIN articles ON articles.report_id = member.value
                     WHERE articles.report_id IS NULL
                        OR articles.status != 'enriched'
                        OR datetime(articles.published_date) < datetime('now', ?)
                        OR datetime(articles.published_date) > datetime('now')
                 )
               ORDER BY generated_at DESC, story_id""",
            (scope, window),
        )
        return [_story(row) for row in await cursor.fetchall()]

    @app.get("/api/monitoring/status")
    async def monitoring_status(request: Request) -> dict[str, Any]:
        db: aiosqlite.Connection = request.app.state.db
        return await _monitoring_status(db)

    @app.post("/api/analyse")
    async def analyse(body: AnalyseRequest, request: Request) -> list[dict[str, Any]]:
        db: aiosqlite.Connection = request.app.state.db
        bus: EventBus = request.app.state.bus
        cards = await analyse_focused(db, bus, categories=body.categories, topics=body.topics, subtopics=body.subtopics)
        return [
            {
                "scope": "focused",
                "story_id": card.story_id,
                "story_title": card.story_title,
                "synthesis": card.synthesis,
                "framing_notes": card.framing_notes,
                "member_report_ids": card.member_report_ids,
                "generated_at": None,
            }
            for card in cards
        ]

    @app.get("/api/filters")
    async def filters(request: Request) -> dict[str, list[dict[str, Any]]]:
        db: aiosqlite.Connection = request.app.state.db
        window = f"-{config.MONITOR_WINDOW_HOURS} hours"
        category_cursor = await db.execute(
            """SELECT topic AS value, COUNT(*) AS count
               FROM articles
               WHERE status = 'enriched'
                 AND topic IS NOT NULL AND topic != ''
                 AND datetime(published_date) >= datetime('now', ?)
                 AND datetime(published_date) <= datetime('now')
               GROUP BY topic
               ORDER BY count DESC, topic""",
            (window,),
        )
        category_rows = await category_cursor.fetchall()

        topic_cursor = await db.execute(
            """SELECT value, SUM(count) AS count
               FROM (
                   SELECT json_each.value AS value, COUNT(*) AS count
                   FROM articles, json_each(articles.distilled_topics)
                   WHERE status = 'enriched'
                     AND datetime(published_date) >= datetime('now', ?)
                     AND datetime(published_date) <= datetime('now')
                     AND json_each.value IS NOT NULL AND json_each.value != ''
                   GROUP BY json_each.value
                   UNION ALL
                   SELECT json_each.value AS value, COUNT(*) AS count
                   FROM articles, json_each(articles.subtopics)
                   WHERE status = 'enriched'
                     AND datetime(published_date) >= datetime('now', ?)
                     AND datetime(published_date) <= datetime('now')
                     AND json_each.value IS NOT NULL
                     AND json_each.value != ''
                     AND json_each.value NOT IN ('UK_Poland_bilateral', 'Poland_on_UK')
                     AND json_array_length(COALESCE(NULLIF(articles.distilled_topics, ''), '[]')) = 0
                   GROUP BY json_each.value
               )
               GROUP BY value
               ORDER BY count DESC, value""",
            (window, window),
        )
        topic_rows = await topic_cursor.fetchall()

        category_order = {value: index for index, value in enumerate(config.TOPICS)}
        categories = [_filter_option(row["value"], row["count"]) for row in category_rows]
        categories.sort(key=lambda item: (category_order.get(item["value"], 999), item["label"]))
        topics = [_filter_option(row["value"], row["count"]) for row in topic_rows]
        return {"categories": categories, "topics": topics}

    @app.post("/api/update")
    async def update(body: UpdateRequest, request: Request) -> dict[str, Any]:
        db: aiosqlite.Connection = request.app.state.db
        bus: EventBus = request.app.state.bus
        update_lock: asyncio.Lock = request.app.state.update_lock
        if update_lock.locked():
            raise HTTPException(status_code=409, detail="update already running")

        async with update_lock:
            before_articles = await _count_rows(db, "SELECT COUNT(*) FROM articles")
            before_enriched = await _count_rows(db, "SELECT COUNT(*) FROM articles WHERE status = 'enriched'")
            inserted = await poll_once(db, bus) if body.poll_feeds else 0
            enrichment = await _run_manual_enrichment(db, bus, body.enrich_limit)

            stories_refreshed = False
            stories_skipped_reason = None
            story_results: list[dict[str, object]] = []
            if body.refresh_stories:
                results = await refresh_dashboard_stories(db, bus)
                story_results = [result.as_dict() for result in results]
                stories_refreshed = any(result.changed for result in results)
                retained = [result.scope for result in results if not result.changed]
                if retained:
                    stories_skipped_reason = f"previous cards retained for: {', '.join(retained)}"

            after_articles = await _count_rows(db, "SELECT COUNT(*) FROM articles")
            after_enriched = await _count_rows(db, "SELECT COUNT(*) FROM articles WHERE status = 'enriched'")
            pending = await _count_rows(db, "SELECT COUNT(*) FROM articles WHERE status = 'new'")
            failed = await _count_rows(db, "SELECT COUNT(*) FROM articles WHERE status = 'failed'")
            stories_count = await _count_rows(db, "SELECT COUNT(*) FROM stories")
            coverage = await _monitoring_status(db)
            await bus.publish("update.complete", {"inserted": inserted, **enrichment})

            return {
                "poll_feeds": body.poll_feeds,
                "llm_available": bool(config.OPENROUTER_API_KEY),
                "inserted": inserted,
                "articles_before": before_articles,
                "articles_after": after_articles,
                "enriched_before": before_enriched,
                "enriched_after": after_enriched,
                "enrichment": enrichment,
                "pending": pending,
                "failed": failed,
                "stories_refreshed": stories_refreshed,
                "stories_skipped_reason": stories_skipped_reason,
                "story_scopes": story_results,
                "stories_count": stories_count,
                "coverage": coverage,
            }

    @app.get("/api/entities/trending")
    async def trending_entities(
        request: Request,
        limit: int = Query(default=20, ge=1, le=100),
        exclude: str | None = Query(default="Poland"),
    ) -> list[dict[str, Any]]:
        db: aiosqlite.Connection = request.app.state.db
        excluded = {item.strip().lower() for item in (exclude or "").split(",") if item.strip()}
        excluded |= await _source_names(db)
        params: list[Any] = []
        excluded_clause = ""
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            excluded_clause = f"AND lower(entity_name) NOT IN ({placeholders})"
            params.extend(sorted(excluded))
        window = f"-{config.MONITOR_WINDOW_HOURS} hours"
        history_window = f"-{config.MONITOR_WINDOW_HOURS * 8} hours"
        params = [window, window, history_window, *params]
        params.append(limit)
        cursor = await db.execute(
            f"""WITH counts AS (
                    SELECT
                        e.entity_id,
                        e.entity_name,
                        e.entity_type,
                        e.aliases,
                        e.first_seen_date,
                        SUM(CASE
                            WHEN datetime(a.published_date) >= datetime('now', ?)
                             AND datetime(a.published_date) <= datetime('now') THEN 1 ELSE 0
                        END) AS recent_mentions,
                        SUM(CASE
                            WHEN datetime(a.published_date) < datetime('now', ?)
                             AND datetime(a.published_date) >= datetime('now', ?) THEN 1 ELSE 0
                        END) AS prior_mentions
                    FROM entities AS e
                    JOIN report_entity_links AS l ON l.entity_id = e.entity_id
                    JOIN articles AS a ON a.report_id = l.report_id
                    WHERE a.status = 'enriched'
                    GROUP BY e.entity_id
                ), current_entities AS (
                    SELECT
                        entity_id,
                        entity_name,
                        entity_type,
                        aliases,
                        first_seen_date,
                        recent_mentions AS total_mention_count,
                        recent_mentions AS recent_mention_count,
                        CASE
                            WHEN recent_mentions > 20 THEN 'key_actor'
                            WHEN recent_mentions > 5 THEN 'regular'
                            ELSE 'peripheral'
                        END AS salience_tier,
                        ROUND(
                            CAST(recent_mentions AS REAL)
                            / MAX(1.0, CAST(prior_mentions AS REAL) / 7.0),
                            2
                        ) AS mention_velocity
                    FROM counts
                    WHERE recent_mentions > 0
                    {excluded_clause}
                ), ranked AS (
                    SELECT *, ROW_NUMBER() OVER (
                        PARTITION BY COALESCE(entity_type, 'other')
                        ORDER BY recent_mention_count DESC, mention_velocity DESC, entity_name
                    ) AS type_rank
                    FROM current_entities
                )
                SELECT * FROM ranked
                WHERE type_rank <= ?
                ORDER BY entity_type, type_rank""",
            params,
        )
        return [_entity(row) for row in await cursor.fetchall()]

    @app.get("/api/articles")
    async def articles(
        request: Request,
        category: Annotated[list[str] | None, Query()] = None,
        topic: Annotated[list[str] | None, Query()] = None,
        subtopic: Annotated[list[str] | None, Query()] = None,
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[dict[str, Any]]:
        db: aiosqlite.Connection = request.app.state.db
        window = f"-{config.MONITOR_WINDOW_HOURS} hours"
        where: list[str] = [
            "datetime(published_date) >= datetime('now', ?)",
            "datetime(published_date) <= datetime('now')",
        ]
        params: list[Any] = [window]
        if category:
            placeholders = ",".join("?" for _ in category)
            where.append(f"topic IN ({placeholders})")
            params.extend(category)
        if topic:
            placeholders = ",".join("?" for _ in topic)
            where.append(
                f"""(
                    EXISTS (
                        SELECT 1 FROM json_each(articles.distilled_topics)
                        WHERE value IN ({placeholders})
                    )
                OR EXISTS (
                    SELECT 1 FROM json_each(articles.subtopics)
                    WHERE json_array_length(COALESCE(NULLIF(articles.distilled_topics, ''), '[]')) = 0
                      AND value IN ({placeholders})
                )
            )"""
            )
            params.extend(topic)
            params.extend(topic)
        if subtopic:
            placeholders = ",".join("?" for _ in subtopic)
            where.append(
                f"""EXISTS (
                SELECT 1 FROM json_each(articles.subtopics)
                WHERE value IN ({placeholders})
            )"""
            )
            params.extend(subtopic)
        clause = f"WHERE {' AND '.join(where)}" if where else ""
        params.append(limit)
        cursor = await db.execute(
            f"""SELECT articles.*, sources.political_leaning AS source_political_leaning
                FROM articles
                LEFT JOIN sources ON sources.source_id = articles.source_id
                {clause}
                ORDER BY published_date DESC
                LIMIT ?""",
            params,
        )
        return [_article(row) for row in await cursor.fetchall()]

    @app.get("/api/search")
    async def search(
        request: Request,
        q: str = Query(min_length=1),
        limit: int = Query(default=25, ge=1, le=100),
    ) -> list[dict[str, Any]]:
        db: aiosqlite.Connection = request.app.state.db
        cursor = await db.execute(
            """SELECT articles.*, sources.political_leaning AS source_political_leaning
               FROM articles_fts
               JOIN articles ON articles_fts.rowid = articles.rowid
               LEFT JOIN sources ON sources.source_id = articles.source_id
               WHERE articles_fts MATCH ?
               ORDER BY rank
               LIMIT ?""",
            (_fts_match_query(q), limit),
        )
        return [_article(row) for row in await cursor.fetchall()]

    @app.get("/api/sources")
    async def sources(request: Request) -> list[dict[str, Any]]:
        db: aiosqlite.Connection = request.app.state.db
        cursor = await db.execute("SELECT * FROM sources ORDER BY name")
        return [dict(row) for row in await cursor.fetchall()]

    @app.get("/api/events")
    async def stream_events(request: Request) -> StreamingResponse:
        bus: EventBus = request.app.state.bus

        async def event_stream() -> AsyncIterator[str]:
            async for event in bus.subscribe():
                if await request.is_disconnected():
                    break
                data = json.dumps({"type": event.type, "payload": event.payload, "emitted_at": event.emitted_at})
                yield f"event: {event.type}\ndata: {data}\n\n"

        return StreamingResponse(event_stream(), media_type="text/event-stream")

    frontend_dist = config.BACKEND_DIR.parent / "frontend" / "dist"
    if frontend_dist.exists():
        app.mount("/", StaticFiles(directory=frontend_dist, html=True), name="frontend")

    return app


app = create_app()
