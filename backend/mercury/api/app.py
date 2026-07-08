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
    topics: list[str] = Field(default_factory=list)
    subtopics: list[str] = Field(default_factory=list)

    @field_validator("topics")
    @classmethod
    def valid_topics(cls, value: list[str]) -> list[str]:
        invalid = [item for item in value if item not in config.TOPICS]
        if invalid:
            raise ValueError(f"invalid topics: {invalid}")
        return value

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
        cursor = await db.execute(
            "SELECT * FROM stories WHERE scope = ? ORDER BY generated_at DESC, story_id",
            (scope,),
        )
        return [_story(row) for row in await cursor.fetchall()]

    @app.post("/api/analyse")
    async def analyse(body: AnalyseRequest, request: Request) -> list[dict[str, Any]]:
        db: aiosqlite.Connection = request.app.state.db
        bus: EventBus = request.app.state.bus
        cards = await analyse_focused(db, bus, topics=body.topics, subtopics=body.subtopics)
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
            if body.refresh_stories and not config.OPENROUTER_API_KEY:
                stories_skipped_reason = "OPENROUTER_API_KEY is not set"
            elif body.refresh_stories and enrichment["enriched"] == 0:
                stories_skipped_reason = "no new enriched articles"
            elif body.refresh_stories:
                await refresh_dashboard_stories(db, bus)
                stories_refreshed = True

            after_articles = await _count_rows(db, "SELECT COUNT(*) FROM articles")
            after_enriched = await _count_rows(db, "SELECT COUNT(*) FROM articles WHERE status = 'enriched'")
            pending = await _count_rows(db, "SELECT COUNT(*) FROM articles WHERE status = 'new'")
            failed = await _count_rows(db, "SELECT COUNT(*) FROM articles WHERE status = 'failed'")
            stories_count = await _count_rows(db, "SELECT COUNT(*) FROM stories")
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
                "stories_count": stories_count,
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
        where = ""
        if excluded:
            placeholders = ",".join("?" for _ in excluded)
            where = f"WHERE lower(entity_name) NOT IN ({placeholders})"
            params.extend(sorted(excluded))
        params.append(limit)
        cursor = await db.execute(
            f"""SELECT * FROM entity_stats
                {where}
                ORDER BY total_mention_count DESC, recent_mention_count DESC, entity_name
                LIMIT ?""",
            params,
        )
        return [_entity(row) for row in await cursor.fetchall()]

    @app.get("/api/articles")
    async def articles(
        request: Request,
        topic: Annotated[list[str] | None, Query()] = None,
        subtopic: Annotated[list[str] | None, Query()] = None,
        limit: int = Query(default=50, ge=1, le=200),
    ) -> list[dict[str, Any]]:
        db: aiosqlite.Connection = request.app.state.db
        where: list[str] = []
        params: list[Any] = []
        if topic:
            placeholders = ",".join("?" for _ in topic)
            where.append(f"topic IN ({placeholders})")
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
