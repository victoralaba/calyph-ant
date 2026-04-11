# domains/notifications/emails.py
"""
SendPulse API Gateway.

Strict separation of concerns. This module only talks to SendPulse and Redis (for token caching).
It has zero knowledge of our SQLAlchemy database or Notification ORM models.
"""

from __future__ import annotations

import httpx
from loguru import logger
from typing import Any

from core.config import settings
from core.db import get_redis

SENDPULSE_OAUTH_URL = "https://api.sendpulse.com/oauth/access_token"
SENDPULSE_SMTP_URL = "https://api.sendpulse.com/smtp/emails"


async def _get_sendpulse_token() -> str | None:
    """
    Exchanges Client ID/Secret for an OAuth Bearer token.
    Tokens live for 1 hour. We cache it in Redis for 3540 seconds (1 hr - 1 min buffer).
    """
    redis = await get_redis()
    token_key = "sendpulse:oauth2:token"
    
    cached_token = await redis.get(token_key)
    if cached_token:
        return cached_token.decode() if isinstance(cached_token, bytes) else cached_token

    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(
                SENDPULSE_OAUTH_URL,
                json={
                    "grant_type": "client_credentials",
                    "client_id": settings.SENDPULSE_CLIENT_ID,
                    "client_secret": settings.SENDPULSE_CLIENT_SECRET,
                }
            )
            resp.raise_for_status()
            data = resp.json()
            token = data["access_token"]
            expires_in = int(data["expires_in"]) - 60  # 60s buffer
            
            await redis.setex(token_key, expires_in, token)
            return token
    except Exception as exc:
        logger.error(f"SendPulse Auth Failed: {exc}")
        return None


async def send_sendpulse_email(
    to_email: str,
    to_name: str,
    subject: str,
    html_content: str,
    sender_type: str = "system" # "system" | "ceo"
) -> bool:
    """
    Core sender function mapping to SendPulse 2.0 standard.
    """
    token = await _get_sendpulse_token()
    if not token:
        logger.error("Email aborted: Could not acquire SendPulse token.")
        return False

    # Determine sender profile
    if sender_type == "ceo":
        from_email = settings.SENDPULSE_SENDER_EMAIL
        from_name = settings.SENDPULSE_SENDER_NAME
    else:
        from_email = settings.SENDPULSE_SYSTEM_EMAIL
        from_name = settings.SENDPULSE_SYSTEM_NAME

    payload: dict[str, Any] = {
        "email": {
            "html": html_content,
            "text": "Please view this email in an HTML compatible client.",
            "subject": subject,
            "from": {
                "name": from_name,
                "email": from_email
            },
            "to": [
                {
                    "name": to_name,
                    "email": to_email
                }
            ]
        }
    }

    try:
        async with httpx.AsyncClient(timeout=15.0) as client:
            resp = await client.post(
                SENDPULSE_SMTP_URL,
                headers={
                    "Authorization": f"Bearer {token}",
                    "Content-Type": "application/json"
                },
                json=payload
            )
            resp.raise_for_status()
            return True
    except Exception as exc:
        logger.error(f"SendPulse delivery failed for {to_email}: {exc}")
        return False