"""SQLite access layer: one shared aiosqlite connection, WAL mode."""

import json
from pathlib import Path

import aiosqlite

from mercury import config

SCHEMA_PATH = Path(__file__).parent / "schema.sql"
SEED_PATH = Path(__file__).parent.parent / "ingest" / "sources_seed.json"

_db: aiosqlite.Connection | None = None


async def get_db() -> aiosqlite.Connection:
    global _db
    if _db is None:
        config.DB_PATH.parent.mkdir(parents=True, exist_ok=True)
        _db = await aiosqlite.connect(config.DB_PATH)
        _db.row_factory = aiosqlite.Row
        await _db.execute("PRAGMA journal_mode=WAL")
        await _db.execute("PRAGMA foreign_keys=ON")
        await _db.executescript(SCHEMA_PATH.read_text())
        await _seed_sources(_db)
        await _db.commit()
    return _db


async def close_db() -> None:
    global _db
    if _db is not None:
        await _db.close()
        _db = None


async def _seed_sources(db: aiosqlite.Connection) -> None:
    """Insert registry rows that aren't present yet; never overwrites edits."""
    sources = json.loads(SEED_PATH.read_text())
    for s in sources:
        await db.execute(
            """INSERT OR IGNORE INTO sources
               (source_id, name, source_type, country, language,
                political_leaning, feed_url, active, notes)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                s["source_id"], s["name"], s["source_type"], s["country"],
                s["language"], s["political_leaning"], s.get("feed_url"),
                1 if s.get("active", bool(s.get("feed_url"))) else 0, s.get("notes"),
            ),
        )
