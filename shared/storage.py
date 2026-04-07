# shared/storage.py
"""
Cloudflare R2 storage client wrapper.

Thin abstraction over boto3's S3-compatible API and aiobotocore for streaming.
All R2 operations go through here — no domain imports boto3 directly.

Also contains the pagination helper used across domains.
"""

from __future__ import annotations

import asyncio
from functools import lru_cache
from typing import Any
from uuid import UUID

import aiobotocore.session
import boto3
from botocore.config import Config
from botocore.exceptions import ClientError
from loguru import logger

from core.config import settings


# ---------------------------------------------------------------------------
# R2 client factory
# ---------------------------------------------------------------------------

@lru_cache(maxsize=1)
def _get_client():
    """
    Build and cache a boto3 S3 client pointed at Cloudflare R2.
    Prevents re-instantiation overhead and reuses the underlying connection pool.
    Strictly for lightweight I/O and cryptographic operations.
    """
    return boto3.client(
        "s3",
        endpoint_url=settings.R2_ENDPOINT_URL,
        aws_access_key_id=settings.R2_ACCESS_KEY_ID,
        aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(
            max_pool_connections=50,
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
# Async Streaming Engine
# ---------------------------------------------------------------------------

class R2MultipartStreamer:
    """
    Native Asyncio Context Manager for pass-through file streaming to R2.
    Uses aiobotocore to prevent thread-pool exhaustion under heavy concurrent load.
    Guarantees peak memory usage of ~5MB per active upload regardless of file size.
    """
    def __init__(self, key: str, content_type: str = "application/octet-stream", metadata: dict[str, str] | None = None):
        _check_r2_enabled()
        self.key = key
        self.content_type = content_type
        self.metadata = metadata or {}
        
        self.upload_id: str | None = None
        self.parts: list[dict[str, Any]] = []
        self.buffer = bytearray()
        self.part_number = 1
        
        # S3/R2 requires multipart chunks to be at least 5MB, except for the final part.
        self.chunk_size_limit = 5 * 1024 * 1024 
        
        # Aiobotocore session is safe to instantiate locally
        self.session = aiobotocore.session.get_session()
        self._client_ctx = None
        self.client = None

    async def __aenter__(self) -> "R2MultipartStreamer":
        self._client_ctx = self.session.create_client(
            "s3",
            endpoint_url=settings.R2_ENDPOINT_URL,
            aws_access_key_id=settings.R2_ACCESS_KEY_ID,
            aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
            region_name="auto",
            config=Config(retries={"max_attempts": 3, "mode": "adaptive"}),
        )
        self.client = await self._client_ctx.__aenter__()

        res = await self.client.create_multipart_upload(
            Bucket=settings.R2_BUCKET_NAME,
            Key=self.key,
            ContentType=self.content_type,
            Metadata=self.metadata
        )
        self.upload_id = res["UploadId"]
        return self

    async def push_chunk(self, chunk: bytes) -> None:
        """Absorb bytes. Block and flush to R2 natively when buffer breaches 5MB."""
        self.buffer.extend(chunk)
        if len(self.buffer) >= self.chunk_size_limit:
            await self._flush_part()

    async def _flush_part(self) -> None:
        if not self.buffer:
            return

        payload = bytes(self.buffer)
        current_part = self.part_number
        
        res = await self.client.upload_part(
            Bucket=settings.R2_BUCKET_NAME,
            Key=self.key,
            PartNumber=current_part,
            UploadId=self.upload_id,
            Body=payload
        )
        
        self.parts.append({"PartNumber": current_part, "ETag": res["ETag"]})
        self.part_number += 1
        self.buffer.clear()

    async def __aexit__(self, exc_type: Any, exc_val: Any, exc_tb: Any) -> None:
        try:
            if exc_type is not None:
                logger.error(f"Stream interrupted for {self.key}. Aborting R2 multipart upload.")
                if self.upload_id and self.client:
                    await self.client.abort_multipart_upload(
                        Bucket=settings.R2_BUCKET_NAME,
                        Key=self.key,
                        UploadId=self.upload_id
                    )
                return

            await self._flush_part()

            if self.upload_id and self.parts and self.client:
                await self.client.complete_multipart_upload(
                    Bucket=settings.R2_BUCKET_NAME,
                    Key=self.key,
                    UploadId=self.upload_id,
                    MultipartUpload={"Parts": self.parts}
                )
        finally:
            if self._client_ctx:
                await self._client_ctx.__aexit__(exc_type, exc_val, exc_tb)


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
    Strictly limited to 5MB to prevent OOM crashes on the backend.
    For large files, use R2MultipartStreamer instead.
    """
    if len(data) > 5 * 1024 * 1024:
        raise ValueError("Payload exceeds 5MB limit for in-memory upload. Use streaming.")
        
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
    """
    Download an object from R2 into memory.
    Strictly limited to 5MB to prevent OOM crashes on the backend.
    For larger assets (like backups), the client MUST use generate_presigned_url.
    """
    _check_r2_enabled()
    client = _get_client()
    try:
        response = await asyncio.to_thread(
            client.get_object,
            Bucket=settings.R2_BUCKET_NAME,
            Key=key,
        )
        
        # OOM Protection: Enforce 5MB limit before reading into RAM
        content_length = response.get("ContentLength", 0)
        if content_length > 5 * 1024 * 1024:
            raise ValueError(
                f"Object '{key}' is {content_length} bytes, exceeding the 5MB in-memory limit. "
                "Use generate_presigned_url for this asset."
            )
            
        return await asyncio.to_thread(response["Body"].read)
        
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
    """
    Generate a pre-signed URL for direct client access.
    Purely local cryptographic math.
    """
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
    """
    List objects under a key prefix.
    Safely paginates through R2 to prevent silent truncation at 1,000 items.
    """
    _check_r2_enabled()
    client = _get_client()
    
    def _fetch_all_pages() -> list[dict[str, Any]]:
        paginator = client.get_paginator("list_objects_v2")
        results = []
        
        for page in paginator.paginate(Bucket=settings.R2_BUCKET_NAME, Prefix=prefix):
            for obj in page.get("Contents", []):
                results.append({
                    "key": obj["Key"],
                    "size": obj["Size"],
                    "last_modified": obj["LastModified"].isoformat(),
                })
        return results

    return await asyncio.to_thread(_fetch_all_pages)


# ---------------------------------------------------------------------------
# Key builders
# ---------------------------------------------------------------------------

def backup_key(workspace_id: UUID, connection_id: UUID, backup_id: UUID, fmt: str) -> str:
    ext = ".calyph.gz" if fmt == "calyph" else ".sql.gz"
    return f"backups/{workspace_id}/{connection_id}/{backup_id}{ext}"


def avatar_key(user_id: UUID, mime_type: str) -> str:
    """
    Generates storage key for avatars.
    Exploit Fixed: Never trust client filenames. Derive extension strictly from MIME type.
    """
    allowed_types = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp"
    }
    if mime_type not in allowed_types:
        raise ValueError("Invalid image type. Only JPG, PNG, and WebP are allowed.")
    return f"avatars/{user_id}.{allowed_types[mime_type]}"