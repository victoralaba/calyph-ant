from __future__ import annotations

from dataclasses import dataclass

from loguru import logger

from core.db import get_redis


@dataclass(frozen=True)
class RateLimitResult:
    allowed: bool
    reason: str | None = None


async def enforce_redis_limit(
    *,
    key: str,
    limit: int,
    window_seconds: int,
    fail_closed: bool,
    action: str,
) -> RateLimitResult:
    try:
        redis = await get_redis()
        current = await redis.incr(key)
        if current == 1:
            await redis.expire(key, window_seconds)
        if current > limit:
            return RateLimitResult(allowed=False, reason="rate_limited")
        return RateLimitResult(allowed=True)
    except Exception as exc:
        if fail_closed:
            logger.error(f"Rate limiter unavailable for high-risk action={action}: {exc}")
            return RateLimitResult(allowed=False, reason="redis_unavailable")

        logger.warning(f"Rate limiter unavailable for fail-open action={action}; allowing: {exc}")
        return RateLimitResult(allowed=True, reason="redis_unavailable")
