"""OpenRouter/OpenAI SDK helpers and validated LLM response models."""

from __future__ import annotations

import json
import re
from typing import Any, TypeVar

from openai import AsyncOpenAI
from pydantic import BaseModel, Field, ValidationError, field_validator

from mercury import config

T = TypeVar("T", bound=BaseModel)


class LLMUnavailable(RuntimeError):
    """Raised when the LLM client cannot be used."""


class ExtractedEntity(BaseModel):
    canonical_name: str = Field(min_length=1, max_length=160)
    surface_form: str = Field(min_length=1, max_length=160)
    entity_type: str

    @field_validator("canonical_name", "surface_form")
    @classmethod
    def clean_name(cls, value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    @field_validator("entity_type")
    @classmethod
    def valid_entity_type(cls, value: str) -> str:
        if value not in config.ENTITY_TYPES:
            raise ValueError(f"entity_type must be one of {config.ENTITY_TYPES}")
        return value


class EnrichedArticle(BaseModel):
    summary: str = Field(min_length=1, max_length=420)
    leaning: str
    topic: str
    subtopics: list[str] = Field(default_factory=list)
    distilled_topics: list[str] = Field(default_factory=list)
    entities: list[ExtractedEntity] = Field(default_factory=list)

    @field_validator("summary")
    @classmethod
    def clean_summary(cls, value: str) -> str:
        return re.sub(r"\s+", " ", value).strip()

    @field_validator("leaning")
    @classmethod
    def valid_leaning(cls, value: str) -> str:
        if value not in config.LEANINGS:
            raise ValueError(f"leaning must be one of {config.LEANINGS}")
        return value

    @field_validator("topic")
    @classmethod
    def valid_topic(cls, value: str) -> str:
        if value not in config.TOPICS:
            raise ValueError(f"topic must be one of {config.TOPICS}")
        return value

    @field_validator("subtopics")
    @classmethod
    def valid_subtopics(cls, value: list[str]) -> list[str]:
        invalid = [item for item in value if item not in config.SUBTOPICS]
        if invalid:
            raise ValueError(f"invalid subtopics: {invalid}")
        return value

    @field_validator("distilled_topics")
    @classmethod
    def clean_distilled_topics(cls, value: list[str]) -> list[str]:
        cleaned: list[str] = []
        seen: set[str] = set()
        for item in value:
            topic = re.sub(r"\s+", " ", item).strip()
            key = topic.lower()
            if topic and key not in seen:
                cleaned.append(topic[:60])
                seen.add(key)
        return cleaned[:5]


class StoryCard(BaseModel):
    story_id: str = Field(min_length=1, max_length=80)
    story_title: str = Field(min_length=1, max_length=180)
    synthesis: str = Field(min_length=1, max_length=1200)
    framing_notes: str = Field(min_length=1, max_length=1200)
    member_report_ids: list[str] = Field(min_length=2)

    @field_validator("story_id")
    @classmethod
    def kebabish_story_id(cls, value: str) -> str:
        cleaned = re.sub(r"[^a-z0-9-]+", "-", value.lower()).strip("-")
        return cleaned or "story"


class StoryList(BaseModel):
    stories: list[StoryCard] = Field(default_factory=list)


def client() -> AsyncOpenAI:
    if not config.OPENROUTER_API_KEY:
        raise LLMUnavailable("OPENROUTER_API_KEY is not set")
    return AsyncOpenAI(api_key=config.OPENROUTER_API_KEY, base_url=config.OPENROUTER_BASE_URL)


def _json_schema_response_format(model: type[BaseModel]) -> dict[str, Any]:
    return {
        "type": "json_schema",
        "json_schema": {
            "name": model.__name__,
            "strict": True,
            "schema": model.model_json_schema(),
        },
    }


def _extract_json(content: str) -> Any:
    try:
        return json.loads(content)
    except json.JSONDecodeError:
        pass

    fenced = re.search(r"```(?:json)?\s*(.*?)```", content, re.DOTALL | re.IGNORECASE)
    if fenced:
        return json.loads(fenced.group(1))

    start_candidates = [idx for idx in (content.find("{"), content.find("[")) if idx >= 0]
    if not start_candidates:
        raise ValueError("LLM response did not contain JSON")
    start = min(start_candidates)
    end = max(content.rfind("}"), content.rfind("]"))
    if end <= start:
        raise ValueError("LLM response contained malformed JSON")
    return json.loads(content[start : end + 1])


async def complete_json(
    prompt: str,
    schema: type[T],
    *,
    model: str,
    temperature: float = 0.2,
    max_tokens: int = 1200,
) -> T:
    llm = client()
    last_error: Exception | None = None
    for attempt in range(2):
        try:
            completion = await llm.chat.completions.create(
                model=model,
                temperature=temperature,
                max_tokens=max_tokens,
                response_format=_json_schema_response_format(schema),
                messages=[
                    {
                        "role": "system",
                        "content": "Return only valid JSON matching the requested schema.",
                    },
                    {"role": "user", "content": prompt},
                ],
            )
            content = completion.choices[0].message.content or "{}"
            return schema.model_validate(_extract_json(content))
        except (ValidationError, ValueError, json.JSONDecodeError) as exc:
            last_error = exc
            if attempt == 1:
                break
        except Exception as exc:
            last_error = exc
            if attempt == 1:
                break
    raise RuntimeError(f"LLM completion failed after retry: {last_error}") from last_error


async def enrich_article(prompt: str) -> EnrichedArticle:
    return await complete_json(
        prompt,
        EnrichedArticle,
        model=config.ENRICH_MODEL,
        temperature=0.2,
        max_tokens=1400,
    )


async def generate_story_cards(prompt: str, workflow: bool = False) -> StoryList:
    return await complete_json(
        prompt,
        StoryList,
        model=config.STORY_MODEL,
        temperature=0.3,
        max_tokens=4000 if workflow else 1500,
    )
