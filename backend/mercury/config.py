"""Central configuration, all overridable via environment variables."""

import os
from pathlib import Path

from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parent.parent
ROOT_DIR = BACKEND_DIR.parent

load_dotenv(ROOT_DIR / ".env")

_db_path = Path(os.environ.get("MERCURY_DB_PATH", BACKEND_DIR / "data" / "mercury.db"))
DB_PATH = _db_path if _db_path.is_absolute() else ROOT_DIR / _db_path

OPENROUTER_API_KEY = os.environ.get("OPENROUTER_API_KEY", "")
OPENROUTER_BASE_URL = os.environ.get("OPENROUTER_BASE_URL", "https://openrouter.ai/api/v1")

# Keep all LLM calls on one cheap, fast OpenRouter model for now.
MERCURY_MODEL = "deepseek/deepseek-v4-flash"
ENRICH_MODEL = MERCURY_MODEL
STORY_MODEL = MERCURY_MODEL

# Pipeline tuning
DEFAULT_POLL_SECONDS = int(os.environ.get("MERCURY_POLL_SECONDS", "300"))
ENRICH_CONCURRENCY = int(os.environ.get("MERCURY_ENRICH_CONCURRENCY", "4"))
STORY_REFRESH_SECONDS = int(os.environ.get("MERCURY_STORY_REFRESH_SECONDS", "900"))
MONITOR_WINDOW_HOURS = int(os.environ.get("MERCURY_MONITOR_WINDOW_HOURS", "24"))
STORY_K = int(os.environ.get("MERCURY_STORY_K", "3"))

CLEANING_VERSION = "v1.1"

HOST = os.environ.get("MERCURY_HOST", "127.0.0.1")
PORT = int(os.environ.get("MERCURY_PORT", "8000"))

# Controlled vocabularies (ported verbatim from Mercury-Core)
TOPICS = ["defence", "economy", "diplomacy", "energy", "domestic", "other"]
SUBTOPICS = [
    "UK_Poland_bilateral",
    "Poland_on_UK",
    "EU",
    "NATO",
    "Russia_Ukraine",
    "Belarus",
    "migration",
    "Germany",
    "USA",
    "elections",
]
LEANINGS = ["left-leaning", "centrist", "right-leaning"]
ENTITY_TYPES = ["person", "country", "institution", "organisation"]
