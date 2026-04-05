# domains/ai/providers.py
"""
AI domain — Powered strictly by Google Gemini.

Architecture Notes:
- Module-level httpx.AsyncClient prevents TCP port exhaustion.
- Quota is burned post-flight via BackgroundTasks to eliminate critical-path latency.
- Database introspection is BANNED here. The router expects a Google Prompt Cache URI
  to be present in Redis (generated asynchronously by the worker). 

UI Contract & Behaviors:
- The UI MUST handle Server-Sent Events (SSE) for the /sql and /explain endpoints.
- If the schema cache is missing, the endpoint returns 400. The UI MUST catch this 
  and prompt the user to manually trigger a background schema sync before trying again.
- Toggling the 'use_thinking_model' flag in the request body will drain the user's
  daily quota 3x faster. The UI should display this cost clearly.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import AsyncGenerator, Any
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, BackgroundTasks
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field

from core.auth import CurrentUser
from core.config import settings
from core.db import get_redis
from core.middleware import limiter

# ---------------------------------------------------------------------------
# Global Singleton HTTP Client (Prevents TCP Socket Leaks)
# ---------------------------------------------------------------------------
_ai_client = httpx.AsyncClient(
    base_url="https://generativelanguage.googleapis.com",
    timeout=httpx.Timeout(connect=5.0, read=120.0, write=10.0, pool=10.0),
    limits=httpx.Limits(max_keepalive_connections=100, max_connections=500)
)

# ---------------------------------------------------------------------------
# Tier-Weighted Economics & Quota
# ---------------------------------------------------------------------------
_QUOTA_KEY_PREFIX = "ai_quota"
_QUOTA_TTL_SECONDS = 25 * 3600

async def verify_quota_preflight(user_id: UUID, tier: str, requested_cost: int) -> None:
    """Fast, non-blocking check to ensure user has enough quota."""
    from domains.billing.flutterwave import get_limits
    limits = get_limits(tier)
    daily_limit: int = limits.get("ai_requests_per_day", 5)
    
    if daily_limit == -1: # Unlimited Enterprise
        return

    try:
        redis = await get_redis()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        used_raw = await redis.get(f"{_QUOTA_KEY_PREFIX}:{user_id}:{today}")
        used = int(used_raw) if used_raw else 0
        
        if used + requested_cost > daily_limit:
            raise HTTPException(
                status_code=429, 
                detail=f"Insufficient AI quota. Requested: {requested_cost}, Remaining: {daily_limit - used}. Upgrade plan to increase limits."
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"AI quota preflight check skipped (Redis unavailable): {exc}")


def burn_quota_postflight(user_id: UUID, cost: int) -> None:
    """Executes in the background AFTER the response has shipped to the UI."""
    import asyncio
    async def _burn():
        try:
            redis = await get_redis()
            today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
            key = f"{_QUOTA_KEY_PREFIX}:{user_id}:{today}"
            
            current = await redis.incrby(key, cost)
            if current == cost:  # First hit today, set expiry
                await redis.expire(key, _QUOTA_TTL_SECONDS)
        except Exception as exc:
            logger.error(f"Failed to burn post-flight AI quota for {user_id}: {exc}")
            
    # Fire and forget in the background task loop
    asyncio.create_task(_burn())


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
        redis = await get_redis()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        raw = await redis.get(f"{_QUOTA_KEY_PREFIX}:{user_id}:{today}")
        used = int(raw) if raw else 0
    except Exception as exc:
        logger.warning(f"Could not read AI quota counter: {exc}")

    now_utc = datetime.now(timezone.utc)
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
# Cache Retrieval
# ---------------------------------------------------------------------------
async def get_cached_schema_name(connection_id: UUID) -> str | None:
    """
    Fetches the Google `cachedContents/xxx` identifier from Redis.
    Zero PostgreSQL introspection.
    """
    try:
        redis = await get_redis()
        # The background worker MUST populate this key using the Google File API / Cached Content API
        cache_ref = await redis.get(f"schema_cache:google:{connection_id}")
        return cache_ref.decode("utf-8") if cache_ref else None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Request Models
# ---------------------------------------------------------------------------
class BaseAIRequest(BaseModel):
    use_thinking_model: bool = Field(
        default=False, 
        description="If true, uses the advanced reasoning model. Burns 3x quota."
    )

class NlToSqlRequest(BaseAIRequest):
    natural_language: str = Field(..., min_length=1, max_length=2000)

class ExplainSqlRequest(BaseAIRequest):
    sql: str = Field(..., min_length=1)

class OptimizeRequest(BaseAIRequest):
    sql: str = Field(..., min_length=1)

# ---------------------------------------------------------------------------
# Router & Endpoints
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/ai", tags=["ai"])

@router.get("/status")
async def get_ai_status():
    """UI uses this to check if AI features are globally enabled."""
    return {
        "available": settings.AI_AVAILABLE,
        "default_model": settings.GOOGLE_GENAI_MODEL,
        "thinking_model": settings.GOOGLE_GENAI_MODEL_THINKING
    }

@router.get("/quota")
async def get_quota_status(user: CurrentUser):
    """Return the calling user's current AI request quota status."""
    return await _get_ai_quota_status(user.id, user.tier)


@router.post("/{connection_id}/sql")
@limiter.limit(settings.RATE_LIMIT_AI)
async def stream_natural_language_to_sql(
    connection_id: UUID,
    body: NlToSqlRequest,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
):
    """
    Translates Natural Language to SQL and streams the result back to the UI via SSE.
    """
    if not settings.AI_AVAILABLE:
        raise HTTPException(status_code=503, detail="AI features are currently disabled.")

    cost = settings.QUOTA_COST_THINKING if body.use_thinking_model else settings.QUOTA_COST_STANDARD
    await verify_quota_preflight(user.id, user.tier, cost)
    
    schema_cache_name = await get_cached_schema_name(connection_id)
    if not schema_cache_name:
        raise HTTPException(
            status_code=400, 
            detail="Schema cache is not ready. Please sync the database connection first."
        )

    model = settings.GOOGLE_GENAI_MODEL_THINKING if body.use_thinking_model else settings.GOOGLE_GENAI_MODEL
    
    payload = {
        "contents": [{"role": "user", "parts": [{"text": body.natural_language}]}],
        "systemInstruction": {
            "role": "system",
            "parts": [{"text": "You are a PostgreSQL expert. Output ONLY valid SQL. No markdown fences. Limit queries to 100 rows unless specified."}]
        },
        "cachedContent": schema_cache_name,
        "generationConfig": {"temperature": 0.1}
    }

    async def _stream_gemini() -> AsyncGenerator[str, None]:
        url = f"/v1beta/models/{model}:streamGenerateContent?key={settings.GOOGLE_GENAI_API_KEY}&alt=sse"
        
        try:
            async with _ai_client.stream("POST", url, json=payload) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        raw = line[5:].strip()
                        if not raw:
                            continue
                        try:
                            event = json.loads(raw)
                            text = event.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                            if text:
                                yield text
                        except (json.JSONDecodeError, IndexError, KeyError):
                            continue
        except httpx.HTTPStatusError as e:
            logger.error(f"Gemini API Error: {e.response.text}")
            yield f"-- Error generating SQL. The AI provider rejected the request."

    background_tasks.add_task(burn_quota_postflight, user.id, cost)
    return StreamingResponse(_stream_gemini(), media_type="text/event-stream")


@router.post("/{connection_id}/explain")
@limiter.limit(settings.RATE_LIMIT_AI)
async def explain_sql(
    connection_id: UUID,
    body: ExplainSqlRequest,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
):
    """
    Explains a SQL query in plain English via SSE Stream.
    """
    if not settings.AI_AVAILABLE:
        raise HTTPException(status_code=503, detail="AI features are currently disabled.")

    cost = settings.QUOTA_COST_THINKING if body.use_thinking_model else settings.QUOTA_COST_STANDARD
    await verify_quota_preflight(user.id, user.tier, cost)

    model = settings.GOOGLE_GENAI_MODEL_THINKING if body.use_thinking_model else settings.GOOGLE_GENAI_MODEL
    
    payload = {
        "contents": [{"role": "user", "parts": [{"text": body.sql}]}],
        "systemInstruction": {
            "role": "system",
            "parts": [{"text": "You are a PostgreSQL expert. Explain the following SQL query in plain English. Be concise. Describe what the query does, what tables it touches, and any potential performance concerns. Do not rewrite the query unless asked."}]
        },
        "generationConfig": {"temperature": 0.2}
    }

    async def _stream_gemini() -> AsyncGenerator[str, None]:
        url = f"/v1beta/models/{model}:streamGenerateContent?key={settings.GOOGLE_GENAI_API_KEY}&alt=sse"
        try:
            async with _ai_client.stream("POST", url, json=payload) as response:
                response.raise_for_status()
                async for line in response.aiter_lines():
                    if line.startswith("data:"):
                        raw = line[5:].strip()
                        if not raw:
                            continue
                        try:
                            event = json.loads(raw)
                            text = event.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "")
                            if text:
                                yield text
                        except (json.JSONDecodeError, IndexError, KeyError):
                            continue
        except httpx.HTTPStatusError as e:
            logger.error(f"Gemini API Error: {e.response.text}")
            yield "Error explaining SQL."

    background_tasks.add_task(burn_quota_postflight, user.id, cost)
    return StreamingResponse(_stream_gemini(), media_type="text/event-stream")


@router.post("/{connection_id}/optimize")
@limiter.limit(settings.RATE_LIMIT_AI)
async def optimize_query(
    connection_id: UUID,
    body: OptimizeRequest,
    user: CurrentUser,
    background_tasks: BackgroundTasks,
):
    """
    Analyzes SQL and returns specific indexing and rewrite suggestions.
    Returns a strict JSON response directly to the UI using Gemini's native JSON mode.
    """
    if not settings.AI_AVAILABLE:
        raise HTTPException(status_code=503, detail="AI features are currently disabled.")

    cost = settings.QUOTA_COST_THINKING if body.use_thinking_model else settings.QUOTA_COST_STANDARD
    await verify_quota_preflight(user.id, user.tier, cost)

    schema_cache_name = await get_cached_schema_name(connection_id)
    if not schema_cache_name:
        raise HTTPException(
            status_code=400, 
            detail="Schema cache is not ready. Please sync the database connection first."
        )

    model = settings.GOOGLE_GENAI_MODEL_THINKING if body.use_thinking_model else settings.GOOGLE_GENAI_MODEL
    url = f"/v1beta/models/{model}:generateContent?key={settings.GOOGLE_GENAI_API_KEY}"

    payload = {
        "contents": [{"role": "user", "parts": [{"text": body.sql}]}],
        "systemInstruction": {
            "role": "system",
            "parts": [{"text": "You are a PostgreSQL performance expert. Analyze the provided query against the schema. Output JSON EXACTLY matching this schema: {'indexes': ['CREATE INDEX...'], 'rewrites': [{'original': '...', 'improved': '...', 'reason': '...'}], 'warnings': ['...']}"}]
        },
        "cachedContent": schema_cache_name,
        "generationConfig": {
            "temperature": 0.1,
            "responseMimeType": "application/json"
        }
    }

    try:
        response = await _ai_client.post(url, json=payload)
        response.raise_for_status()
        data = response.json()
        raw_text = data.get("candidates", [{}])[0].get("content", {}).get("parts", [{}])[0].get("text", "{}")
        
        # Enqueue burn task
        background_tasks.add_task(burn_quota_postflight, user.id, cost)
        
        return {
            "suggestions": json.loads(raw_text),
            "model_used": model
        }
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Failed to parse structured output from AI.")
    except httpx.HTTPStatusError as e:
        logger.error(f"Gemini API Error: {e.response.text}")
        raise HTTPException(status_code=502, detail="Upstream AI provider error.")