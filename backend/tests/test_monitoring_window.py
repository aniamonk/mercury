from __future__ import annotations

import unittest
from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import aiosqlite

from mercury.db.database import SCHEMA_PATH
from mercury.enrich.enrich import fetch_new_articles
from mercury.enrich.llm import StoryCard, StoryList
from mercury.stories.stories import fetch_filtered_articles, refresh_scope, save_stories


def timestamp(offset: timedelta) -> str:
    return (datetime.now(UTC) + offset).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


class MonitoringWindowTests(unittest.IsolatedAsyncioTestCase):
    async def asyncSetUp(self) -> None:
        self.db = await aiosqlite.connect(":memory:")
        self.db.row_factory = aiosqlite.Row
        await self.db.executescript(SCHEMA_PATH.read_text())

    async def asyncTearDown(self) -> None:
        await self.db.close()

    async def add_article(
        self,
        report_id: str,
        *,
        published_date: str,
        status: str,
        topic: str = "domestic",
    ) -> None:
        await self.db.execute(
            """INSERT INTO articles
               (report_id, title, url, published_date, ingested_at, status, topic,
                subtopics, distilled_topics, llm_input_text)
               VALUES (?, ?, ?, ?, ?, ?, ?, '[]', '[]', ?)""",
            (
                report_id,
                f"Article {report_id}",
                f"https://example.com/{report_id}",
                published_date,
                timestamp(timedelta()),
                status,
                topic,
                f"Article {report_id}",
            ),
        )
        await self.db.commit()

    async def test_enrichment_and_focused_analysis_only_use_current_window(self) -> None:
        await self.add_article("current-new", published_date=timestamp(timedelta(hours=-1)), status="new")
        await self.add_article("old-new", published_date=timestamp(timedelta(hours=-30)), status="new")
        await self.add_article("future-new", published_date=timestamp(timedelta(hours=1)), status="new")
        await self.add_article("current-enriched", published_date=timestamp(timedelta(hours=-2)), status="enriched")
        await self.add_article("old-enriched", published_date=timestamp(timedelta(hours=-31)), status="enriched")

        pending = await fetch_new_articles(self.db, 20)
        focused = await fetch_filtered_articles(self.db, categories=["domestic"])

        self.assertEqual([row["report_id"] for row in pending], ["current-new"])
        self.assertEqual([row["report_id"] for row in focused], ["current-enriched"])

    async def test_invalid_story_output_retains_cache_but_confirmed_empty_clears_it(self) -> None:
        await self.add_article("report-one", published_date=timestamp(timedelta(hours=-1)), status="enriched")
        await self.add_article("report-two", published_date=timestamp(timedelta(hours=-2)), status="enriched")
        existing = StoryCard(
            story_id="existing-story",
            story_title="Existing Story",
            synthesis="Existing synthesis.",
            framing_notes="Existing framing.",
            member_report_ids=["report-one", "report-two"],
        )
        await save_stories(self.db, "domestic", [existing])

        invalid = StoryList(
            stories=[
                StoryCard(
                    story_id="invalid-story",
                    story_title="Invalid Story",
                    synthesis="Invalid synthesis.",
                    framing_notes="Invalid framing.",
                    member_report_ids=["missing-one", "missing-two"],
                )
            ]
        )
        with (
            patch("mercury.stories.stories.config.OPENROUTER_API_KEY", "test-key"),
            patch("mercury.stories.stories.generate_story_cards", new=AsyncMock(return_value=invalid)),
        ):
            retained = await refresh_scope(self.db, "domestic")

        cursor = await self.db.execute("SELECT story_id FROM stories WHERE scope = 'domestic'")
        self.assertEqual(retained.status, "retained_invalid")
        self.assertEqual([row["story_id"] for row in await cursor.fetchall()], ["existing-story"])

        with (
            patch("mercury.stories.stories.config.OPENROUTER_API_KEY", "test-key"),
            patch("mercury.stories.stories.generate_story_cards", new=AsyncMock(return_value=StoryList(stories=[]))),
        ):
            emptied = await refresh_scope(self.db, "domestic")

        cursor = await self.db.execute("SELECT COUNT(*) FROM stories WHERE scope = 'domestic'")
        self.assertEqual(emptied.status, "empty")
        self.assertEqual((await cursor.fetchone())[0], 0)


if __name__ == "__main__":
    unittest.main()
