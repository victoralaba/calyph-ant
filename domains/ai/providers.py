# domains/ai/providers.py
"""
AI domain.

Provider selection
------------------
Every endpoint accepts an optional `provider` field:
  "anthropic" | "google" | null

When null, the configured AI_PROVIDER setting is used as the default.
If the requested provider has no API key configured, the endpoint returns
HTTP 400 immediately — this is a caller mistake, not an infrastructure
failure, so there is no fallback or retry.

Both providers must be independently reachable. "Both down is impossible"
is the operational contract — there is no cross-provider fallback.

Supported operations per provider
----------------------------------
Both Anthropic and Google support all three operations:
  - natural_language_to_sql  (POST /ai/{connection_id}/sql)
  - explain_sql              (POST /ai/{connection_id}/explain)
  - optimize_query           (POST /ai/{connection_id}/optimize)

The provider implementations share the same LLMProvider protocol so
adding a third provider later requires only a new class + one entry
in the factory — no router changes.

Daily quota
-----------
Per-user daily quota is enforced regardless of which provider is used.
The quota key is provider-agnostic: "ai_quota:{user_id}:{YYYY-MM-DD}".

Router endpoints
----------------
  GET  /ai/quota                        — current quota status
  GET  /ai/providers                    — list configured providers
  POST /ai/{connection_id}/sql          — natural language → SQL
  POST /ai/{connection_id}/explain      — explain SQL in plain English
  POST /ai/{connection_id}/optimize     — index + rewrite suggestions
"""

from __future__ import annotations

import json
from abc import ABC, abstractmethod
from datetime import datetime, timezone
from typing import Any, AsyncGenerator, Literal
from uuid import UUID

import asyncpg
import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.config import settings
from core.db import get_db, get_connection_url
from core.middleware import limiter


# ---------------------------------------------------------------------------
# Provider protocol
# ---------------------------------------------------------------------------

ProviderName = Literal["anthropic", "google"]


class LLMProvider(ABC):
    """Abstract LLM provider. Implement for each AI backend."""

    @property
    @abstractmethod
    def name(self) -> ProviderName:
        """Canonical provider name returned in API responses."""
        ...

    @abstractmethod
    async def complete(
        self,
        system: str,
        prompt: str,
        max_tokens: int = 2048,
        temperature: float = 0.2,
    ) -> str:
        """Single-shot completion. Returns text."""
        ...

    @abstractmethod
    async def stream(
        self,
        system: str,
        prompt: str,
        max_tokens: int = 2048,
    ) -> AsyncGenerator[str, None]:
        """Streaming completion. Yields text chunks."""
        ...


# ---------------------------------------------------------------------------
# Anthropic implementation
# ---------------------------------------------------------------------------

class AnthropicProvider(LLMProvider):

    @property
    def name(self) -> ProviderName:
        return "anthropic"

    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model
        self._client = httpx.AsyncClient(
            base_url="https://api.anthropic.com",
            headers={
                "x-api-key": api_key,
                "anthropic-version": "2023-06-01",
                "content-type": "application/json",
            },
            timeout=60.0,
        )

    async def complete(
        self,
        system: str,
        prompt: str,
        max_tokens: int = 2048,
        temperature: float = 0.2,
    ) -> str:
        response = await self._client.post(
            "/v1/messages",
            json={
                "model": self.model,
                "max_tokens": max_tokens,
                "temperature": temperature,
                "system": system,
                "messages": [{"role": "user", "content": prompt}],
            },
        )
        response.raise_for_status()
        data = response.json()
        return data["content"][0]["text"]

    async def stream(
        self,
        system: str,
        prompt: str,
        max_tokens: int = 2048,
    ) -> AsyncGenerator[str, None]:
        async def _gen() -> AsyncGenerator[str, None]:
            async with self._client.stream(
                "POST",
                "/v1/messages",
                json={
                    "model": self.model,
                    "max_tokens": max_tokens,
                    "system": system,
                    "stream": True,
                    "messages": [{"role": "user", "content": prompt}],
                },
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        raw = line[5:].strip()
                        if raw == "[DONE]":
                            break
                        try:
                            event = json.loads(raw)
                            if event.get("type") == "content_block_delta":
                                yield event["delta"].get("text", "")
                        except json.JSONDecodeError:
                            continue

        return _gen()


# ---------------------------------------------------------------------------
# Google GenAI implementation
# ---------------------------------------------------------------------------

class GoogleGenAIProvider(LLMProvider):

    @property
    def name(self) -> ProviderName:
        return "google"

    def __init__(self, api_key: str, model: str) -> None:
        self.api_key = api_key
        self.model = model
        self._client = httpx.AsyncClient(
            base_url="https://generativelanguage.googleapis.com",
            timeout=60.0,
        )

    async def complete(
        self,
        system: str,
        prompt: str,
        max_tokens: int = 2048,
        temperature: float = 0.2,
    ) -> str:
        full_prompt = f"{system}\n\n{prompt}"
        response = await self._client.post(
            f"/v1/models/{self.model}:generateContent",
            params={"key": self.api_key},
            json={
                "contents": [{"parts": [{"text": full_prompt}]}],
                "generationConfig": {
                    "maxOutputTokens": max_tokens,
                    "temperature": temperature,
                },
            },
        )
        response.raise_for_status()
        data = response.json()
        return data["candidates"][0]["content"]["parts"][0]["text"]

    async def stream(
        self,
        system: str,
        prompt: str,
        max_tokens: int = 2048,
    ) -> AsyncGenerator[str, None]:
        full_prompt = f"{system}\n\n{prompt}"

        async def _gen() -> AsyncGenerator[str, None]:
            async with self._client.stream(
                "POST",
                f"/v1/models/{self.model}:streamGenerateContent",
                params={"key": self.api_key, "alt": "sse"},
                json={
                    "contents": [{"parts": [{"text": full_prompt}]}],
                    "generationConfig": {"maxOutputTokens": max_tokens},
                },
            ) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        raw = line[5:].strip()
                        try:
                            event = json.loads(raw)
                            text = (
                                event.get("candidates", [{}])[0]
                                .get("content", {})
                                .get("parts", [{}])[0]
                                .get("text", "")
                            )
                            if text:
                                yield text
                        except (json.JSONDecodeError, IndexError, KeyError):
                            continue

        return _gen()


# ---------------------------------------------------------------------------
# Provider registry
# ---------------------------------------------------------------------------

def _configured_providers() -> dict[ProviderName, dict[str, str]]:
    """
    Return metadata for every provider that has a key configured.
    Used by GET /ai/providers.
    """
    providers: dict[ProviderName, dict[str, Any]] = {}

    if settings.ANTHROPIC_API_KEY:
        providers["anthropic"] = {
            "name": "anthropic",
            "model": settings.ANTHROPIC_MODEL,
            "is_default": settings.AI_PROVIDER == "anthropic",
        }

    if settings.GOOGLE_GENAI_API_KEY:
        providers["google"] = {
            "name": "google",
            "model": settings.GOOGLE_GENAI_MODEL,
            "is_default": settings.AI_PROVIDER == "google",
        }

    return providers


def get_provider(requested: ProviderName | None = None) -> LLMProvider:
    """
    Resolve and return an LLMProvider instance.

    Args:
        requested: Provider name from the API caller. When None, the
                   AI_PROVIDER setting is used as the default.

    Raises:
        HTTPException 400: Caller requested a provider that has no API
                           key configured. This is a caller error.
        HTTPException 500: The default provider (from config) has no API
                           key — this is a deployment configuration error.
    """
    # Resolve which provider to use
    target: str = requested or settings.AI_PROVIDER

    if target == "anthropic":
        if not settings.ANTHROPIC_API_KEY:
            if requested:
                # Caller explicitly asked for a provider we don't have
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Provider 'anthropic' is not configured on this server. "
                        "Use 'google' or contact your administrator."
                    ),
                )
            raise RuntimeError(
                "AI_PROVIDER is set to 'anthropic' but ANTHROPIC_API_KEY is not configured."
            )
        return AnthropicProvider(
            api_key=settings.ANTHROPIC_API_KEY,
            model=settings.ANTHROPIC_MODEL,
        )

    if target == "google":
        if not settings.GOOGLE_GENAI_API_KEY:
            if requested:
                raise HTTPException(
                    status_code=400,
                    detail=(
                        "Provider 'google' is not configured on this server. "
                        "Use 'anthropic' or contact your administrator."
                    ),
                )
            raise RuntimeError(
                "AI_PROVIDER is set to 'google' but GOOGLE_GENAI_API_KEY is not configured."
            )
        return GoogleGenAIProvider(
            api_key=settings.GOOGLE_GENAI_API_KEY,
            model=settings.GOOGLE_GENAI_MODEL,
        )

    raise HTTPException(
        status_code=400,
        detail=(
            f"Unknown provider '{target}'. "
            "Supported values: 'anthropic', 'google'."
        ),
    )


# ---------------------------------------------------------------------------
# Per-user daily quota
# ---------------------------------------------------------------------------

_QUOTA_KEY_PREFIX = "ai_quota"
_QUOTA_TTL_SECONDS = 25 * 3600


async def _check_ai_quota(user_id: UUID, tier: str) -> None:
    """
    Enforce per-user daily AI request quota.
    Quota is provider-agnostic — all providers share the same counter.
    """
    from domains.billing.flutterwave import get_limits

    limits = get_limits(tier)
    daily_limit: int = limits.get("ai_requests_per_day", 5)

    if daily_limit == -1:
        return  # Enterprise — unlimited

    try:
        from core.db import get_redis
        redis = await get_redis()

        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"{_QUOTA_KEY_PREFIX}:{user_id}:{today}"

        current: int = await redis.incr(key)
        if current == 1:
            await redis.expire(key, _QUOTA_TTL_SECONDS)

        if current > daily_limit:
            now_utc = datetime.now(timezone.utc)
            midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
            from datetime import timedelta
            next_midnight = midnight + timedelta(days=1)
            secs = int((next_midnight - now_utc).total_seconds())
            hours, minutes = secs // 3600, (secs % 3600) // 60

            raise HTTPException(
                status_code=429,
                detail=(
                    f"Daily AI request quota exceeded ({daily_limit} requests/day "
                    f"on the {tier} plan). "
                    f"Quota resets in {hours}h {minutes}m (midnight UTC). "
                    "Upgrade your plan for a higher limit."
                ),
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(
            f"AI quota check skipped (Redis unavailable): {exc}. "
            "Request allowed through."
        )


async def _get_ai_quota_status(user_id: UUID, tier: str) -> dict[str, Any]:
    from domains.billing.flutterwave import get_limits

    limits = get_limits(tier)
    daily_limit: int = limits.get("ai_requests_per_day", 5)

    if daily_limit == -1:
        return {
            "tier": tier,
            "daily_limit": -1,
            "used_today": None,
            "remaining": -1,
            "unlimited": True,
            "resets_at": None,
        }

    used = 0
    try:
        from core.db import get_redis
        redis = await get_redis()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        raw = await redis.get(f"{_QUOTA_KEY_PREFIX}:{user_id}:{today}")
        used = int(raw) if raw else 0
    except Exception as exc:
        logger.warning(f"Could not read AI quota counter: {exc}")

    now_utc = datetime.now(timezone.utc)
    from datetime import timedelta
    next_midnight = now_utc.replace(hour=0, minute=0, second=0, microsecond=0) + timedelta(days=1)

    return {
        "tier": tier,
        "daily_limit": daily_limit,
        "used_today": used,
        "remaining": max(0, daily_limit - used),
        "unlimited": False,
        "resets_at": next_midnight.isoformat(),
    }


# ---------------------------------------------------------------------------
# Prompt templates
# ---------------------------------------------------------------------------

_SQL_SYSTEM = """\
You are a PostgreSQL expert assistant embedded in Calyphant, a database workspace tool.
Given a database schema and a natural language request, generate a single, correct SQL query.

Rules:
- Output ONLY the SQL query. No explanation, no markdown fences, no preamble.
- Use double-quoted identifiers for table and column names.
- Prefer CTEs over nested subqueries for readability.
- Always include a LIMIT clause for SELECT queries unless the user explicitly asks for all rows.
- Never generate DROP, TRUNCATE, or DELETE without an explicit WHERE clause.
- If the request is ambiguous, generate the safest interpretation.
"""

_EXPLAIN_SYSTEM = """\
You are a PostgreSQL expert. Explain the following SQL query in plain English.
Be concise. Describe what the query does, what tables it touches, and any potential
performance concerns. Do not rewrite the query unless asked.
"""

_OPTIMIZE_SYSTEM = """\
You are a PostgreSQL performance expert. Analyze the following SQL query and schema,
then suggest specific improvements:
1. Missing indexes (provide exact CREATE INDEX statements)
2. Query rewrites that would improve performance
3. Any anti-patterns detected

Format your response as JSON with keys: "indexes" (list of SQL strings),
"rewrites" (list of {original, improved, reason}), "warnings" (list of strings).
Output only the JSON object.
"""


def _build_sql_prompt(schema_context: str, user_request: str) -> str:
    return f"""Database schema:
{schema_context}

Request: {user_request}

SQL query:"""


def _build_optimize_prompt(schema_context: str, sql: str) -> str:
    return f"""Schema:
{schema_context}

Query to optimize:
{sql}"""


def _schema_context_from_rows(tables: list[dict]) -> str:
    lines = []
    for t in tables:
        cols = ", ".join(
            f"{c['name']} {c['data_type']}"
            + (" NOT NULL" if not c.get("nullable") else "")
            for c in t.get("columns", [])
        )
        lines.append(f"  {t['name']}({cols})")
    return "\n".join(lines) if lines else "No schema available."


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/ai", tags=["ai"])


class NlToSqlRequest(BaseModel):
    natural_language: str = Field(..., min_length=1, max_length=2000)
    connection_id: UUID
    schema_context: str | None = None
    provider: ProviderName | None = Field(
        default=None,
        description=(
            "AI provider to use: 'anthropic' or 'google'. "
            "Defaults to the server's configured AI_PROVIDER setting."
        ),
    )


class ExplainSqlRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    provider: ProviderName | None = Field(
        default=None,
        description="AI provider to use. Defaults to server default.",
    )


class OptimizeRequest(BaseModel):
    sql: str = Field(..., min_length=1)
    connection_id: UUID
    schema_context: str | None = None
    provider: ProviderName | None = Field(
        default=None,
        description="AI provider to use. Defaults to server default.",
    )


async def _get_target_pg(
    connection_id: UUID,
    workspace_id: UUID,
    db: AsyncSession,
) -> asyncpg.Connection:
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=404, detail="Connection not found.")
    try:
        return await asyncpg.connect(dsn=url, timeout=15)
    except Exception as exc:
        raise HTTPException(status_code=503, detail=f"Database unreachable: {exc}")


@router.get("/providers")
async def list_providers(user: CurrentUser):
    """
    List all AI providers configured on this server.

    Returns each provider's name, model, and whether it is the default.
    Use the `provider` field in request bodies to select a specific one.
    """
    configured = _configured_providers()
    if not configured:
        raise HTTPException(
            status_code=503,
            detail=(
                "No AI providers are configured. "
                "Set ANTHROPIC_API_KEY and/or GOOGLE_GENAI_API_KEY."
            ),
        )
    return {
        "providers": list(configured.values()),
        "default": settings.AI_PROVIDER,
    }


@router.get("/quota")
async def get_quota_status(user: CurrentUser):
    """Return the calling user's current AI request quota status."""
    return await _get_ai_quota_status(user.id, user.tier)


def require_workspace(user: CurrentUser) -> UUID:
    """Dependency to ensure the current user has an active workspace."""
    if not user.workspace_id:
        raise HTTPException(status_code=403, detail="Workspace context is required.")
    return user.workspace_id


@router.post("/{connection_id}/sql")
@limiter.limit(settings.RATE_LIMIT_AI)
async def natural_language_to_sql(
    request: Request,
    connection_id: UUID,
    body: NlToSqlRequest,
    user: CurrentUser,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
):
    """
    Translate a natural language request into a SQL query.

    Set `provider` to `"anthropic"` or `"google"` to choose explicitly.
    Omit `provider` to use the server default (AI_PROVIDER setting).
    """
    await _check_ai_quota(user.id, user.tier)

    # Resolve provider early so a bad provider name fails before the DB hit
    provider = get_provider(body.provider)

    schema_ctx = body.schema_context
    if not schema_ctx:
        try:
            pg_conn = await _get_target_pg(connection_id, workspace_id, db)
            tables = await pg_conn.fetch(
                """
                SELECT t.table_name,
                       json_agg(json_build_object(
                           'name', c.column_name,
                           'data_type', c.data_type,
                           'nullable', c.is_nullable = 'YES'
                       ) ORDER BY c.ordinal_position) AS columns
                FROM information_schema.tables t
                JOIN information_schema.columns c
                  ON c.table_name = t.table_name AND c.table_schema = t.table_schema
                WHERE t.table_schema = 'public' AND t.table_type = 'BASE TABLE'
                GROUP BY t.table_name
                ORDER BY t.table_name
                """
            )
            await pg_conn.close()
            schema_ctx = _schema_context_from_rows(
                [{"name": r["table_name"], "columns": r["columns"]} for r in tables]
            )
        except HTTPException:
            raise
        except Exception as exc:
            logger.warning(f"Could not fetch schema context: {exc}")
            schema_ctx = "Schema unavailable."

    try:
        sql = await provider.complete(
            system=_SQL_SYSTEM,
            prompt=_build_sql_prompt(schema_ctx, body.natural_language),
        )
        return {
            "sql": sql.strip(),
            "provider": provider.name,
            "model": (
                settings.ANTHROPIC_MODEL
                if provider.name == "anthropic"
                else settings.GOOGLE_GENAI_MODEL
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"AI completion failed ({provider.name}): {exc}",
        )


@router.post("/{connection_id}/explain")
@limiter.limit(settings.RATE_LIMIT_AI)
async def explain_sql(
    request: Request,
    connection_id: UUID,
    body: ExplainSqlRequest,
    user: CurrentUser,
):
    """
    Explain a SQL query in plain English.

    Set `provider` to choose between 'anthropic' and 'google'.
    """
    await _check_ai_quota(user.id, user.tier)

    provider = get_provider(body.provider)

    try:
        explanation = await provider.complete(
            system=_EXPLAIN_SYSTEM,
            prompt=body.sql,
        )
        return {
            "explanation": explanation.strip(),
            "provider": provider.name,
            "model": (
                settings.ANTHROPIC_MODEL
                if provider.name == "anthropic"
                else settings.GOOGLE_GENAI_MODEL
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"AI completion failed ({provider.name}): {exc}",
        )


@router.post("/{connection_id}/optimize")
@limiter.limit(settings.RATE_LIMIT_AI)
async def optimize_query(
    request: Request,
    connection_id: UUID,
    body: OptimizeRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Suggest indexes and rewrites for a SQL query.

    Set `provider` to choose between 'anthropic' and 'google'.
    """
    await _check_ai_quota(user.id, user.tier)

    provider = get_provider(body.provider)
    schema_ctx = body.schema_context or "Schema unavailable."

    try:
        raw = await provider.complete(
            system=_OPTIMIZE_SYSTEM,
            prompt=_build_optimize_prompt(schema_ctx, body.sql),
            max_tokens=1500,
        )
        clean = raw.strip().lstrip("```json").lstrip("```").rstrip("```").strip()
        try:
            suggestions = json.loads(clean)
        except json.JSONDecodeError:
            suggestions = {"raw": raw, "parse_error": True}

        return {
            "suggestions": suggestions,
            "provider": provider.name,
            "model": (
                settings.ANTHROPIC_MODEL
                if provider.name == "anthropic"
                else settings.GOOGLE_GENAI_MODEL
            ),
        }
    except HTTPException:
        raise
    except Exception as exc:
        raise HTTPException(
            status_code=500,
            detail=f"AI completion failed ({provider.name}): {exc}",
        )