# shared/storage.py
"""
Cloudflare R2 storage client wrapper.

Thin abstraction over boto3's S3-compatible API.
All R2 operations go through here — no domain imports boto3 directly.

Also contains the pagination helper used across domains.
"""

from __future__ import annotations

import asyncio
import mimetypes
from typing import Any
from uuid import UUID

import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from loguru import logger

from core.config import settings


# ---------------------------------------------------------------------------
# R2 client factory
# ---------------------------------------------------------------------------

def _get_client():
    """Build a boto3 S3 client pointed at Cloudflare R2."""
    return boto3.client(
        "s3",
        endpoint_url=settings.R2_ENDPOINT_URL,
        aws_access_key_id=settings.R2_ACCESS_KEY_ID,
        aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            connect_timeout=10,
            read_timeout=60,
        ),
    )


def _check_r2_enabled() -> None:
    if not settings.r2_enabled:
        raise RuntimeError(
            "R2 storage is not configured. "
            "Set R2_ENDPOINT_URL, R2_ACCESS_KEY_ID, R2_SECRET_ACCESS_KEY."
        )


# ---------------------------------------------------------------------------
# Core operations
# ---------------------------------------------------------------------------

async def upload(
    key: str,
    data: bytes,
    content_type: str | None = None,
    metadata: dict[str, str] | None = None,
) -> int:
    """
    Upload bytes to R2. Returns size in bytes.
    key: e.g. "backups/workspace_id/connection_id/backup_id.calyph.gz"
    """
    _check_r2_enabled()
    client = _get_client()
    kwargs: dict[str, Any] = {
        "Bucket": settings.R2_BUCKET_NAME,
        "Key": key,
        "Body": data,
    }
    if content_type:
        kwargs["ContentType"] = content_type
    elif key.endswith(".gz"):
        kwargs["ContentType"] = "application/gzip"
    if metadata:
        kwargs["Metadata"] = metadata

    await asyncio.to_thread(client.put_object, **kwargs)
    return len(data)


async def download(key: str) -> bytes:
    """Download an object from R2. Raises ValueError if not found."""
    _check_r2_enabled()
    client = _get_client()
    try:
        response = await asyncio.to_thread(
            client.get_object,
            Bucket=settings.R2_BUCKET_NAME,
            Key=key,
        )
        return response["Body"].read()
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            raise ValueError(f"Object not found in R2: {key}") from exc
        raise


async def delete(key: str) -> bool:
    """Delete an object from R2. Returns True if deleted, False if not found."""
    _check_r2_enabled()
    client = _get_client()
    try:
        await asyncio.to_thread(
            client.delete_object,
            Bucket=settings.R2_BUCKET_NAME,
            Key=key,
        )
        return True
    except ClientError as exc:
        logger.warning(f"R2 delete failed for key '{key}': {exc}")
        return False


async def generate_presigned_url(
    key: str,
    expires_in: int = 3600,
    method: str = "get_object",
) -> str:
    """Generate a pre-signed URL for direct client access."""
    _check_r2_enabled()
    client = _get_client()
    return await asyncio.to_thread(
        client.generate_presigned_url,
        method,
        Params={"Bucket": settings.R2_BUCKET_NAME, "Key": key},
        ExpiresIn=expires_in,
    )


async def object_exists(key: str) -> bool:
    """Check if an object exists without downloading it."""
    _check_r2_enabled()
    client = _get_client()
    try:
        await asyncio.to_thread(
            client.head_object,
            Bucket=settings.R2_BUCKET_NAME,
            Key=key,
        )
        return True
    except ClientError:
        return False


async def list_objects(prefix: str) -> list[dict[str, Any]]:
    """List objects under a key prefix."""
    _check_r2_enabled()
    client = _get_client()
    response = await asyncio.to_thread(
        client.list_objects_v2,
        Bucket=settings.R2_BUCKET_NAME,
        Prefix=prefix,
    )
    return [
        {
            "key": obj["Key"],
            "size": obj["Size"],
            "last_modified": obj["LastModified"].isoformat(),
        }
        for obj in response.get("Contents", [])
    ]


# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------

def backup_key(workspace_id: UUID, connection_id: UUID, backup_id: UUID, fmt: str) -> str:
    ext = ".calyph.gz" if fmt == "calyph" else ".sql.gz"
    return f"backups/{workspace_id}/{connection_id}/{backup_id}{ext}"


def avatar_key(user_id: UUID, filename: str) -> str:
    ext = filename.rsplit(".", 1)[-1].lower() if "." in filename else "jpg"
    return f"avatars/{user_id}.{ext}"
