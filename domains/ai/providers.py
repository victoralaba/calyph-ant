# domains/ai/providers.py
"""
AI domain — Powered strictly by Google Gemini.

Architecture Notes:
- Module-level httpx.AsyncClient prevents TCP port exhaustion.
- Quota is burned ATOMICALLY via Redis Lua scripts pre-flight to prevent race conditions.
- Database introspection is BANNED here. The router expects a Google Prompt Cache URI
  to be present in Redis (generated asynchronously by the worker).
- Strict workspace-connection IDOR gating enforced via Dependencies.
- Strict L7 proxy bypass mechanics (Buffer blowouts & Heartbeats) for SSE streams.
"""

from __future__ import annotations

import json
from datetime import datetime, timezone, timedelta
from typing import AsyncGenerator, Any
from uuid import UUID

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request
from fastapi.responses import StreamingResponse
from loguru import logger
from pydantic import BaseModel, Field
from sqlalchemy.ext.asyncio import AsyncSession

from core.auth import CurrentUser
from core.config import settings
from core.db import get_redis, get_db, get_connection_url
from core.middleware import limiter

# ---------------------------------------------------------------------------
# Global Singleton HTTP Client (Prevents TCP Socket Leaks)
# ---------------------------------------------------------------------------
_ai_client = httpx.AsyncClient(
    base_url="https://generativelanguage.googleapis.com",
    timeout=httpx.Timeout(connect=5.0, read=120.0, write=10.0, pool=10.0),
    limits=httpx.Limits(max_keepalive_connections=100, max_connections=500)
)

# Standardized Proxy Disarmament Headers
_SSE_HEADERS = {
    "Cache-Control": "no-cache",
    "X-Accel-Buffering": "no",       # Bypasses Nginx buffering
    "Content-Encoding": "none",      # Forbids proxies from gzip buffering
    "Connection": "keep-alive",
    "Transfer-Encoding": "chunked",  # Explicit streaming declaration
}

# ---------------------------------------------------------------------------
# Tier-Weighted Economics & Atomic Quota
# ---------------------------------------------------------------------------
_QUOTA_KEY_PREFIX = "ai_quota"
_QUOTA_TTL_SECONDS = 25 * 3600

async def consume_quota_atomic(user_id: UUID, tier: str, requested_cost: int) -> None:
    """
    Atomic check-and-deduct using a Redis Lua Script.
    Mathematically guarantees quota limits even under extreme concurrent load.
    """
    from domains.billing.flutterwave import get_limits
    limits = get_limits(tier)
    daily_limit: int = limits.get("ai_requests_per_day", 5)
    
    if daily_limit == -1: # Unlimited Enterprise
        return

    try:
        redis = await get_redis()
        today = datetime.now(timezone.utc).strftime("%Y-%m-%d")
        key = f"{_QUOTA_KEY_PREFIX}:{user_id}:{today}"
        
        # Atomic LUA Script: Check limit and increment in one continuous operation
        lua_script = """
        local current = redis.call("GET", KEYS[1])
        local used = tonumber(current) or 0
        local cost = tonumber(ARGV[1])
        local limit = tonumber(ARGV[2])
        local ttl = tonumber(ARGV[3])

        if used + cost > limit then
            return {0, limit - used}
        end

        local new_used = redis.call("INCRBY", KEYS[1], cost)
        if new_used == cost then
            redis.call("EXPIRE", KEYS[1], ttl)
        end
        return {1, limit - new_used}
        """
        
        # Cast all variadic arguments to strings to satisfy the redis-py type signature.
        # type: ignore is applied to bypass the flawed return type stub in redis.asyncio
        result: Any = await redis.eval(  # type: ignore
            lua_script, 
            1, 
            key, 
            str(requested_cost), 
            str(daily_limit), 
            str(_QUOTA_TTL_SECONDS)
        )
        
        success, remaining = result[0], result[1]
        
        if success == 0:
            raise HTTPException(
                status_code=429, 
                detail=f"Insufficient AI quota. Requested: {requested_cost}, Remaining: {remaining}. Upgrade plan to increase limits."
            )
    except HTTPException:
        raise
    except Exception as exc:
        logger.warning(f"AI quota atomic check skipped (Redis unavailable): {exc}")


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
# Authorization & Cache Retrieval
# ---------------------------------------------------------------------------
def require_workspace(user: CurrentUser) -> UUID:
    """Dependency to ensure the current user has an active workspace."""
    if not user.workspace_id:
        raise HTTPException(status_code=403, detail="Workspace context is required.")
    return user.workspace_id

async def verify_connection_ownership(
    connection_id: UUID,
    workspace_id: UUID = Depends(require_workspace),
    db: AsyncSession = Depends(get_db),
) -> None:
    """
    Prevents Insecure Direct Object Reference (IDOR).
    Forces a database check to ensure the workspace actually owns the connection.
    """
    url = await get_connection_url(db, connection_id, workspace_id)
    if not url:
        raise HTTPException(status_code=403, detail="Forbidden. Connection does not belong to the active workspace.")


async def get_cached_schema_name(connection_id: UUID) -> str | None:
    """
    Fetches the Google `cachedContents/xxx` identifier from Redis.
    """
    try:
        redis = await get_redis()
        cache_ref = await redis.get(f"schema_cache:google:{connection_id}")
        return cache_ref.decode("utf-8") if cache_ref else None
    except Exception:
        return None

# ---------------------------------------------------------------------------
# Request Models (Strict Memory Boundaries)
# ---------------------------------------------------------------------------
class BaseAIRequest(BaseModel):
    use_thinking_model: bool = Field(
        default=False, 
        description="If true, uses the advanced reasoning model. Burns 3x quota."
    )

class NlToSqlRequest(BaseAIRequest):
    natural_language: str = Field(..., min_length=1, max_length=2000)

class ExplainSqlRequest(BaseAIRequest):
    sql: str = Field(..., min_length=1, max_length=15000)

class OptimizeRequest(BaseAIRequest):
    sql: str = Field(..., min_length=1, max_length=15000)

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


@router.post("/{connection_id}/sql", dependencies=[Depends(verify_connection_ownership)])
@limiter.limit(settings.RATE_LIMIT_AI)
async def stream_natural_language_to_sql(
    connection_id: UUID,
    body: NlToSqlRequest,
    user: CurrentUser,
):
    """
    Translates Natural Language to SQL and streams the result back to the UI via SSE.
    """
    if not settings.AI_AVAILABLE:
        raise HTTPException(status_code=503, detail="AI features are currently disabled.")

    cost = settings.QUOTA_COST_THINKING if body.use_thinking_model else settings.QUOTA_COST_STANDARD
    await consume_quota_atomic(user.id, user.tier, cost)
    
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
        # Force proxies to flush the initial connection headers immediately
        yield f": {' ' * 4096}\n\n"
        
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
                                yield f"data: {json.dumps({'type': 'chunk', 'text': text})}\n\n"
                        except (json.JSONDecodeError, IndexError, KeyError):
                            continue
                # Terminate the stream cleanly for the UI
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
        
        except httpx.HTTPStatusError as e:
            logger.error(f"Gemini API Error: {e.response.text}")
            yield f"data: {json.dumps({'type': 'error', 'error': 'The AI provider rejected the request.'})}\n\n"
        except Exception as e:
            logger.error(f"Gemini Stream Failure: {e}")
            yield f"data: {json.dumps({'type': 'error', 'error': 'Upstream connection failed.'})}\n\n"

    return StreamingResponse(_stream_gemini(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.post("/{connection_id}/explain", dependencies=[Depends(verify_connection_ownership)])
@limiter.limit(settings.RATE_LIMIT_AI)
async def explain_sql(
    connection_id: UUID,
    body: ExplainSqlRequest,
    user: CurrentUser,
):
    """
    Explains a SQL query in plain English via SSE Stream.
    """
    if not settings.AI_AVAILABLE:
        raise HTTPException(status_code=503, detail="AI features are currently disabled.")

    cost = settings.QUOTA_COST_THINKING if body.use_thinking_model else settings.QUOTA_COST_STANDARD
    await consume_quota_atomic(user.id, user.tier, cost)

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
        yield f": {' ' * 4096}\n\n"
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
                                yield f"data: {json.dumps({'type': 'chunk', 'text': text})}\n\n"
                        except (json.JSONDecodeError, IndexError, KeyError):
                            continue
                yield f"data: {json.dumps({'type': 'done'})}\n\n"
        except httpx.HTTPStatusError as e:
            logger.error(f"Gemini API Error: {e.response.text}")
            yield f"data: {json.dumps({'type': 'error', 'error': 'Error explaining SQL.'})}\n\n"
        except Exception as e:
            logger.error(f"Gemini Stream Failure: {e}")
            yield f"data: {json.dumps({'type': 'error', 'error': 'Upstream connection failed.'})}\n\n"

    return StreamingResponse(_stream_gemini(), media_type="text/event-stream", headers=_SSE_HEADERS)


@router.post("/{connection_id}/optimize", dependencies=[Depends(verify_connection_ownership)])
@limiter.limit(settings.RATE_LIMIT_AI)
async def optimize_query(
    connection_id: UUID,
    body: OptimizeRequest,
    user: CurrentUser,
):
    """
    Analyzes SQL and returns specific indexing and rewrite suggestions.
    Returns a strict JSON response directly to the UI using Gemini's native JSON mode.
    """
    if not settings.AI_AVAILABLE:
        raise HTTPException(status_code=503, detail="AI features are currently disabled.")

    cost = settings.QUOTA_COST_THINKING if body.use_thinking_model else settings.QUOTA_COST_STANDARD
    await consume_quota_atomic(user.id, user.tier, cost)

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
        
        return {
            "suggestions": json.loads(raw_text),
            "model_used": model
        }
    except json.JSONDecodeError:
        raise HTTPException(status_code=500, detail="Failed to parse structured output from AI.")
    except httpx.HTTPStatusError as e:
        logger.error(f"Gemini API Error: {e.response.text}")
        raise HTTPException(status_code=502, detail="Upstream AI provider error.")