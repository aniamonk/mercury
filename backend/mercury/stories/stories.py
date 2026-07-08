"""Story clustering and focused analysis workflows."""

from __future__ import annotations

import asyncio
import json
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import aiosqlite

from mercury import config
from mercury.enrich.llm import LLMUnavailable, StoryCard, generate_story_cards
from mercury.events import EventBus

CONDENSED_PROMPT_PATH = Path(__file__).parent / "prompts" / "top_stories_condensed.txt"
WORKFLOW_PROMPT_PATH = Path(__file__).parent / "prompts" / "top_stories_workflow.txt"


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


def _loads_json_array(value: str | None) -> list[str]:
    if not value:
        return []
    try:
        parsed = json.loads(value)
    except json.JSONDecodeError:
        return []
    return parsed if isinstance(parsed, list) else []


def _article_payload(row: aiosqlite.Row) -> dict[str, object]:
    return {
        "report_id": row["report_id"],
        "title": row["title"],
        "source": row["source_domain"],
        "published_date": row["published_date"],
        "summary": row["summary"],
        "topic": row["topic"],
        "subtopics": _loads_json_array(row["subtopics"]),
        "leaning": row["leaning"],
        "llm_input_text": row["llm_input_text"],
    }


async def _fetch_scope_articles(db: aiosqlite.Connection, scope: str, limit: int = 80) -> list[aiosqlite.Row]:
    window = f"-{config.STORY_WINDOW_HOURS} hours"
    if scope == "domestic":
        cursor = await db.execute(
            """SELECT * FROM articles
               WHERE status = 'enriched'
                 AND topic = 'domestic'
                 AND datetime(published_date) >= datetime('now', ?)
               ORDER BY published_date DESC
               LIMIT ?""",
            (window, limit),
        )
    elif scope == "poland_uk":
        cursor = await db.execute(
            """SELECT * FROM articles
               WHERE status = 'enriched'
                 AND datetime(published_date) >= datetime('now', ?)
                 AND EXISTS (
                     SELECT 1 FROM json_each(articles.subtopics)
                     WHERE value IN ('UK_Poland_bilateral', 'Poland_on_UK')
                 )
               ORDER BY published_date DESC
               LIMIT ?""",
            (window, limit),
        )
    else:
        raise ValueError(f"unsupported story scope: {scope}")
    return await cursor.fetchall()


async def fetch_filtered_articles(
    db: aiosqlite.Connection,
    *,
    topics: list[str] | None = None,
    subtopics: list[str] | None = None,
    limit: int = 80,
) -> list[aiosqlite.Row]:
    where = ["status = 'enriched'"]
    params: list[object] = []

    if topics:
        placeholders = ",".join("?" for _ in topics)
        where.append(f"topic IN ({placeholders})")
        params.extend(topics)
    if subtopics:
        placeholders = ",".join("?" for _ in subtopics)
        where.append(
            f"""EXISTS (
                SELECT 1 FROM json_each(articles.subtopics)
                WHERE value IN ({placeholders})
            )"""
        )
        params.extend(subtopics)

    params.append(limit)
    cursor = await db.execute(
        f"""SELECT * FROM articles
            WHERE {' AND '.join(where)}
            ORDER BY published_date DESC
            LIMIT ?""",
        params,
    )
    return await cursor.fetchall()


def _build_prompt(prompt_path: Path, articles: Iterable[aiosqlite.Row]) -> str:
    payload = [_article_payload(row) for row in articles]
    template = prompt_path.read_text()
    return (
        template.replace("{K}", str(config.STORY_K))
        .replace("{ARTICLES_JSON}", json.dumps(payload, ensure_ascii=False, indent=2))
    )


def _post_validate(stories: list[StoryCard], valid_report_ids: set[str]) -> list[StoryCard]:
    accepted: list[StoryCard] = []
    used_report_ids: set[str] = set()
    used_story_ids: set[str] = set()

    for story in stories:
        members: list[str] = []
        for report_id in story.member_report_ids:
            if report_id in valid_report_ids and report_id not in used_report_ids and report_id not in members:
                members.append(report_id)
        if len(members) < 2:
            continue

        used_report_ids.update(members)
        story_id = story.story_id
        if story_id in used_story_ids:
            story_id = f"{story_id}-{len(used_story_ids) + 1}"
        used_story_ids.add(story_id)
        accepted.append(story.model_copy(update={"story_id": story_id, "member_report_ids": members}))
        if len(accepted) >= config.STORY_K:
            break
    return accepted


async def generate_stories_for_rows(
    rows: list[aiosqlite.Row],
    *,
    workflow: bool = False,
) -> list[StoryCard] | None:
    if len(rows) < 2:
        return []
    if not config.OPENROUTER_API_KEY:
        return None

    prompt = _build_prompt(WORKFLOW_PROMPT_PATH if workflow else CONDENSED_PROMPT_PATH, rows)
    try:
        result = await generate_story_cards(prompt, workflow=workflow)
    except LLMUnavailable:
        return None
    valid_report_ids = {row["report_id"] for row in rows}
    return _post_validate(result.stories, valid_report_ids)


async def save_stories(db: aiosqlite.Connection, scope: str, stories: list[StoryCard]) -> None:
    now = utc_now()
    await db.execute("DELETE FROM stories WHERE scope = ?", (scope,))
    for story in stories:
        await db.execute(
            """INSERT INTO stories
               (scope, story_id, story_title, synthesis, framing_notes, member_report_ids, generated_at)
               VALUES (?, ?, ?, ?, ?, ?, ?)""",
            (
                scope,
                story.story_id,
                story.story_title,
                story.synthesis,
                story.framing_notes,
                json.dumps(story.member_report_ids),
                now,
            ),
        )
    await db.commit()


async def refresh_scope(db: aiosqlite.Connection, scope: str) -> list[StoryCard] | None:
    rows = await _fetch_scope_articles(db, scope)
    stories = await generate_stories_for_rows(rows, workflow=False)
    if stories is not None:
        await save_stories(db, scope, stories)
    return stories


async def refresh_dashboard_stories(db: aiosqlite.Connection, bus: EventBus) -> None:
    changed = False
    for scope in ("domestic", "poland_uk"):
        stories = await refresh_scope(db, scope)
        changed = changed or stories is not None
    if changed:
        await bus.publish("stories.updated", {"scopes": ["domestic", "poland_uk"]})


async def analyse_focused(
    db: aiosqlite.Connection,
    bus: EventBus,
    *,
    topics: list[str] | None = None,
    subtopics: list[str] | None = None,
) -> list[StoryCard]:
    rows = await fetch_filtered_articles(db, topics=topics, subtopics=subtopics)
    stories = await generate_stories_for_rows(rows, workflow=True)
    if stories is None:
        return []
    await save_stories(db, "focused", stories)
    await bus.publish("stories.updated", {"scopes": ["focused"]})
    return stories


async def run_story_refresh_loop(db: aiosqlite.Connection, bus: EventBus) -> None:
    dirty = asyncio.Event()

    async def listen() -> None:
        async for event in bus.subscribe():
            if event.type == "article.enriched":
                dirty.set()

    listener = asyncio.create_task(listen(), name="stories:events")
    try:
        while True:
            try:
                await asyncio.wait_for(dirty.wait(), timeout=config.STORY_REFRESH_SECONDS)
            except TimeoutError:
                pass
            dirty.clear()
            await asyncio.sleep(5)
            await refresh_dashboard_stories(db, bus)
    finally:
        listener.cancel()

