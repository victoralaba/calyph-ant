# domains/notifications/emails.py
"""
SendPulse API Gateway.

Strict separation of concerns. This module only talks to SendPulse and Redis
(for OAuth2 token caching). It has zero knowledge of our SQLAlchemy database
or Notification ORM models.

SendPulse OAuth2 token lifecycle:
  - Tokens live for 3600 seconds (1 hour).
  - We cache with a 60-second buffer (3540s TTL) to prevent race conditions
    on expiry boundaries.
  - If a send returns HTTP 401, we force-refresh the token and retry ONCE.
    This covers the edge case where the token expires between cache check
    and the API call (clock skew / Redis eviction).

Sender routing:
  "ceo"    → SENDPULSE_SENDER_EMAIL / SENDPULSE_SENDER_NAME
  "system" → SENDPULSE_SYSTEM_EMAIL / SENDPULSE_SYSTEM_NAME
"""

from __future__ import annotations

import base64
import httpx
from loguru import logger
from typing import Any

from core.config import settings
from core.db import get_redis

SENDPULSE_OAUTH_URL = "https://api.sendpulse.com/oauth/access_token"
SENDPULSE_SMTP_URL = "https://api.sendpulse.com/smtp/emails"

_REDIS_TOKEN_KEY = "sendpulse:oauth2:token"
_TOKEN_TTL_SECONDS = 3540  # 1 hour minus 60s safety buffer


# ---------------------------------------------------------------------------
# Internal: token management
# ---------------------------------------------------------------------------

async def _fetch_fresh_token() -> str | None:
    """
    Exchange Client ID/Secret for a new OAuth2 Bearer token from SendPulse.
    Caches the result in Redis. Returns the token or None on failure.
    """
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                SENDPULSE_OAUTH_URL,
                json={
                    "grant_type": "client_credentials",
                    "client_id": settings.SENDPULSE_CLIENT_ID,
                    "client_secret": settings.SENDPULSE_CLIENT_SECRET,
                },
            )
            resp.raise_for_status()
            data = resp.json()
            token: str = data["access_token"]
            # Honour the server's stated TTL with a safety buffer
            server_ttl = int(data.get("expires_in", 3600))
            cache_ttl = max(server_ttl - 60, 60)

            redis = await get_redis()
            await redis.setex(_REDIS_TOKEN_KEY, cache_ttl, token)
            return token
    except Exception as exc:
        logger.error(f"SendPulse OAuth2 token fetch failed: {exc}")
        return None


async def _get_sendpulse_token(force_refresh: bool = False) -> str | None:
    """
    Return a valid SendPulse Bearer token.
    Uses Redis as a cache. Pass force_refresh=True to bypass the cache
    (used after a 401 response to recover from token expiry edge cases).
    """
    if not force_refresh:
        try:
            redis = await get_redis()
            cached = await redis.get(_REDIS_TOKEN_KEY)
            if cached:
                return cached.decode() if isinstance(cached, bytes) else cached
        except Exception as exc:
            logger.warning(f"SendPulse: Redis token read failed, fetching fresh token: {exc}")

    return await _fetch_fresh_token()


# ---------------------------------------------------------------------------
# Public: send a single transactional email
# ---------------------------------------------------------------------------

async def send_sendpulse_email(
    to_email: str,
    to_name: str,
    subject: str,
    html_content: str,
    sender_type: str = "system",  # "system" | "ceo"
) -> bool:
    """
    Send a transactional email via the SendPulse SMTP API.

    KEY PROTOCOL REQUIREMENT:
    The SendPulse SMTP API requires the "html" field to be Base64-encoded.
    This function encodes html_content to Base64 before sending.
    Without this encoding SendPulse either rejects the payload or renders
    garbage — the API does not return a meaningful error in all cases.

    On HTTP 401, force-refreshes the token and retries exactly once.
    Returns True on success, False on any failure (never raises).

    sender_type routing:
      "ceo"    → SENDPULSE_SENDER_EMAIL / SENDPULSE_SENDER_NAME
                 Personal tone, signed by the founder.
      "system" → SENDPULSE_SYSTEM_EMAIL / SENDPULSE_SYSTEM_NAME
                 Operational tone, signed as Calyphant Team.
    """
    if sender_type == "ceo":
        from_email = settings.SENDPULSE_SENDER_EMAIL
        from_name  = settings.SENDPULSE_SENDER_NAME
    else:
        from_email = settings.SENDPULSE_SYSTEM_EMAIL
        from_name  = settings.SENDPULSE_SYSTEM_NAME

    # -------------------------------------------------------------------
    # BASE64 ENCODING — SendPulse SMTP API requirement.
    # The "html" field must be a Base64-encoded string.
    # Encoding happens here, at the delivery boundary, so that all callers
    # (templates, service, tasks) can work with raw HTML strings throughout
    # the rest of the pipeline without any encoding awareness.
    # -------------------------------------------------------------------
    html_b64: str = base64.b64encode(html_content.encode("utf-8")).decode("ascii")

    payload: dict[str, Any] = {
        "email": {
            "html":    html_b64,
            "text":    "Please view this email in an HTML-compatible email client.",
            "subject": subject,
            "from": {
                "name":  from_name,
                "email": from_email,
            },
            "to": [
                {
                    "name":  to_name or to_email,
                    "email": to_email,
                }
            ],
        }
    }

    for attempt in range(2):  # attempt 0 = normal, attempt 1 = after force-refresh
        token = await _get_sendpulse_token(force_refresh=(attempt == 1))
        if not token:
            logger.error(f"SendPulse: No token available, cannot send to {to_email}")
            return False

        try:
            async with httpx.AsyncClient(timeout=15.0) as client:
                resp = await client.post(
                    SENDPULSE_SMTP_URL,
                    headers={
                        "Authorization": f"Bearer {token}",
                        "Content-Type":  "application/json",
                    },
                    json=payload,
                )

                if resp.status_code == 401 and attempt == 0:
                    # Token expired between cache-check and send — force refresh and retry
                    logger.warning(
                        f"SendPulse: 401 on attempt {attempt + 1} for {to_email}. "
                        "Force-refreshing token and retrying."
                    )
                    try:
                        redis = await get_redis()
                        await redis.delete(_REDIS_TOKEN_KEY)
                    except Exception:
                        pass
                    continue  # retry with force_refresh=True

                resp.raise_for_status()
                return True

        except httpx.HTTPStatusError as exc:
            logger.error(
                f"SendPulse: HTTP {exc.response.status_code} delivering to {to_email} "
                f"(attempt {attempt + 1}): {exc.response.text[:200]}"
            )
            if attempt == 0 and exc.response.status_code == 401:
                continue  # retry path handled above, but guard here too
            return False

        except Exception as exc:
            logger.error(f"SendPulse: delivery failed for {to_email} (attempt {attempt + 1}): {exc}")
            return False

    return False