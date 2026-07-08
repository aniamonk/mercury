#!/usr/bin/env python3
"""Verify configured RSS feeds and update source health columns."""

from __future__ import annotations

import asyncio
import sys
from datetime import UTC, datetime
from pathlib import Path

import feedparser
import httpx

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from mercury.db.database import close_db, get_db  # noqa: E402


def utc_now() -> str:
    return datetime.now(UTC).replace(microsecond=0).strftime("%Y-%m-%d %H:%M:%S")


async def verify_one(db, client: httpx.AsyncClient, source) -> tuple[str, bool, str]:
    if not source["feed_url"]:
        status = "inactive: no public RSS configured"
        await db.execute(
            "UPDATE sources SET active = 0, last_polled_at = ?, last_status = ? WHERE source_id = ?",
            (utc_now(), status, source["source_id"]),
        )
        return source["name"], False, status

    try:
        response = await client.get(source["feed_url"], headers={"User-Agent": "Mercury/0.1 feed verifier"})
        response.raise_for_status()
        parsed = feedparser.parse(response.content)
        ok = bool(parsed.entries)
        status = f"ok: {len(parsed.entries)} entries" if ok else "error: parsed 0 entries"
        if parsed.bozo and not ok:
            status = f"error: {parsed.bozo_exception}"
        await db.execute(
            "UPDATE sources SET active = ?, last_polled_at = ?, last_status = ? WHERE source_id = ?",
            (1 if ok else 0, utc_now(), status, source["source_id"]),
        )
        return source["name"], ok, status
    except Exception as exc:
        status = f"error: {type(exc).__name__}: {exc}"
        await db.execute(
            "UPDATE sources SET active = 0, last_polled_at = ?, last_status = ? WHERE source_id = ?",
            (utc_now(), status, source["source_id"]),
        )
        return source["name"], False, status


async def main() -> int:
    db = await get_db()
    cursor = await db.execute("SELECT * FROM sources ORDER BY name")
    sources = await cursor.fetchall()
    async with httpx.AsyncClient(timeout=30, follow_redirects=True) as client:
        results = await asyncio.gather(*(verify_one(db, client, source) for source in sources))
    await db.commit()
    await close_db()

    width = max(len(name) for name, _, _ in results)
    ok_count = sum(1 for _, ok, _ in results if ok)
    for name, ok, status in results:
        marker = "OK " if ok else "ERR"
        print(f"{marker}  {name:<{width}}  {status}")
    print(f"\n{ok_count}/{len(results)} active feeds verified")
    return 0 if ok_count else 1


if __name__ == "__main__":
    raise SystemExit(asyncio.run(main()))
