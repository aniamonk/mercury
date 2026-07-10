"""Story clustering and focused analysis workflows."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from typing import Iterable

import aiosqlite

from mercury import config
from mercury.enrich.llm import LLMUnavailable, StoryCard, generate_story_cards
from mercury.events import EventBus

CONDENSED_PROMPT_PATH = Path(__file__).parent / "prompts" / "top_stories_condensed.txt"
WORKFLOW_PROMPT_PATH = Path(__file__).parent / "prompts" / "top_stories_workflow.txt"


@dataclass(frozen=True)
class StoryGenerationResult:
    stories: list[StoryCard]
    candidate_count: int
    error: str | None = None


@dataclass(frozen=True)
class ScopeRefreshResult:
    scope: str
    status: str
    eligible_articles: int
    story_count: int
    detail: str | None = None

    @property
    def changed(self) -> bool:
        return self.status in {"refreshed", "empty"}

    def as_dict(self) -> dict[str, object]:
        return {
            "scope": self.scope,
            "status": self.status,
            "eligible_articles": self.eligible_articles,
            "story_count": self.story_count,
            "detail": self.detail,
        }


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
        "distilled_topics": _loads_json_array(row["distilled_topics"] if "distilled_topics" in row.keys() else None),
        "leaning": row["leaning"],
        "llm_input_text": row["llm_input_text"],
    }


async def _fetch_scope_articles(db: aiosqlite.Connection, scope: str, limit: int = 80) -> list[aiosqlite.Row]:
    window = f"-{config.MONITOR_WINDOW_HOURS} hours"
    if scope == "domestic":
        cursor = await db.execute(
            """SELECT * FROM articles
               WHERE status = 'enriched'
                 AND topic = 'domestic'
                 AND datetime(published_date) >= datetime('now', ?)
                 AND datetime(published_date) <= datetime('now')
               ORDER BY published_date DESC
               LIMIT ?""",
            (window, limit),
        )
    elif scope == "poland_uk":
        cursor = await db.execute(
            """SELECT * FROM articles
               WHERE status = 'enriched'
                 AND datetime(published_date) >= datetime('now', ?)
                 AND datetime(published_date) <= datetime('now')
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
    categories: list[str] | None = None,
    topics: list[str] | None = None,
    subtopics: list[str] | None = None,
    limit: int = 80,
) -> list[aiosqlite.Row]:
    window = f"-{config.MONITOR_WINDOW_HOURS} hours"
    where = [
        "status = 'enriched'",
        "datetime(published_date) >= datetime('now', ?)",
        "datetime(published_date) <= datetime('now')",
    ]
    params: list[object] = [window]

    if categories:
        placeholders = ",".join("?" for _ in categories)
        where.append(f"topic IN ({placeholders})")
        params.extend(categories)
    if topics:
        placeholders = ",".join("?" for _ in topics)
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
        params.extend(topics)
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


async def _generate_stories_for_rows(
    rows: list[aiosqlite.Row],
    *,
    workflow: bool = False,
) -> StoryGenerationResult:
    if not config.OPENROUTER_API_KEY:
        return StoryGenerationResult([], 0, "OPENROUTER_API_KEY is not set")

    prompt = _build_prompt(WORKFLOW_PROMPT_PATH if workflow else CONDENSED_PROMPT_PATH, rows)
    try:
        result = await generate_story_cards(prompt, workflow=workflow)
    except (LLMUnavailable, RuntimeError) as exc:
        return StoryGenerationResult([], 0, f"{type(exc).__name__}: {exc}")
    valid_report_ids = {row["report_id"] for row in rows}
    return StoryGenerationResult(
        stories=_post_validate(result.stories, valid_report_ids),
        candidate_count=len(result.stories),
    )


async def generate_stories_for_rows(
    rows: list[aiosqlite.Row],
    *,
    workflow: bool = False,
) -> list[StoryCard] | None:
    if len(rows) < 2:
        return []
    result = await _generate_stories_for_rows(rows, workflow=workflow)
    return None if result.error else result.stories


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


async def _story_count(db: aiosqlite.Connection, scope: str) -> int:
    cursor = await db.execute("SELECT COUNT(*) FROM stories WHERE scope = ?", (scope,))
    row = await cursor.fetchone()
    return int(row[0])


async def _record_refresh_state(
    db: aiosqlite.Connection,
    result: ScopeRefreshResult,
    *,
    successful: bool,
) -> None:
    now = utc_now()
    await db.execute(
        """INSERT INTO story_refresh_state
           (scope, last_attempted_at, last_successful_at, status,
            eligible_articles, story_count, detail)
           VALUES (?, ?, ?, ?, ?, ?, ?)
           ON CONFLICT(scope) DO UPDATE SET
               last_attempted_at = excluded.last_attempted_at,
               last_successful_at = COALESCE(excluded.last_successful_at, story_refresh_state.last_successful_at),
               status = excluded.status,
               eligible_articles = excluded.eligible_articles,
               story_count = excluded.story_count,
               detail = excluded.detail""",
        (
            result.scope,
            now,
            now if successful else None,
            result.status,
            result.eligible_articles,
            result.story_count,
            result.detail,
        ),
    )
    await db.commit()


async def refresh_scope(db: aiosqlite.Connection, scope: str) -> ScopeRefreshResult:
    rows = await _fetch_scope_articles(db, scope)
    eligible_articles = len(rows)
    if eligible_articles < 2:
        await save_stories(db, scope, [])
        result = ScopeRefreshResult(scope, "empty", eligible_articles, 0)
        await _record_refresh_state(db, result, successful=True)
        return result

    generation = await _generate_stories_for_rows(rows, workflow=False)
    if generation.error:
        result = ScopeRefreshResult(
            scope,
            "retained_error",
            eligible_articles,
            await _story_count(db, scope),
            generation.error[:500],
        )
        await _record_refresh_state(db, result, successful=False)
        return result

    if generation.candidate_count > 0 and not generation.stories:
        result = ScopeRefreshResult(
            scope,
            "retained_invalid",
            eligible_articles,
            await _story_count(db, scope),
            "The model returned story candidates without two valid report IDs.",
        )
        await _record_refresh_state(db, result, successful=False)
        return result

    await save_stories(db, scope, generation.stories)
    result = ScopeRefreshResult(
        scope,
        "refreshed" if generation.stories else "empty",
        eligible_articles,
        len(generation.stories),
    )
    await _record_refresh_state(db, result, successful=True)
    return result


async def refresh_dashboard_stories(
    db: aiosqlite.Connection,
    bus: EventBus,
    scopes: Iterable[str] = ("domestic", "poland_uk"),
) -> list[ScopeRefreshResult]:
    results = [await refresh_scope(db, scope) for scope in scopes]
    changed_scopes = [result.scope for result in results if result.changed]
    if changed_scopes:
        await bus.publish("stories.updated", {"scopes": changed_scopes})
    return results


async def analyse_focused(
    db: aiosqlite.Connection,
    bus: EventBus,
    *,
    categories: list[str] | None = None,
    topics: list[str] | None = None,
    subtopics: list[str] | None = None,
) -> list[StoryCard]:
    rows = await fetch_filtered_articles(db, categories=categories, topics=topics, subtopics=subtopics)
    if len(rows) < 2:
        return []
    generation = await _generate_stories_for_rows(rows, workflow=True)
    if generation.error or (generation.candidate_count > 0 and not generation.stories):
        return []
    await save_stories(db, "focused", generation.stories)
    await bus.publish("stories.updated", {"scopes": ["focused"]})
    return generation.stories


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
