# shared/storage.py
"""
Cloudflare R2 storage client wrapper using native aiobotocore.

Native asynchronous abstraction over S3-compatible API.
Utilizes underlying aiohttp connection pooling via application lifespan events.
"""

from __future__ import annotations

import mimetypes
from typing import Any
from uuid import UUID

import aiobotocore.session
from botocore.config import Config
from botocore.exceptions import ClientError
from loguru import logger

from core.config import settings

# ---------------------------------------------------------------------------
# Global State & Lifespan Management
# ---------------------------------------------------------------------------

_S3_CLIENT = None
_session = None
_client_context = None

async def init_r2_client() -> None:
    """
    Establish the async HTTP connection pool natively via aiobotocore.
    Must be invoked by the FastAPI lifespan on application startup.
    """
    global _S3_CLIENT, _session, _client_context
    if not settings.r2_enabled:
        logger.warning("R2 storage is disabled.")
        return

    logger.info("Initializing global Async R2 storage client (aiobotocore)...")
    _session = aiobotocore.session.get_session()
    
    _client_context = _session.create_client(
        "s3",
        endpoint_url=settings.R2_ENDPOINT_URL,
        aws_access_key_id=settings.R2_ACCESS_KEY_ID,
        aws_secret_access_key=settings.R2_SECRET_ACCESS_KEY,
        region_name="auto",
        config=Config(
            retries={"max_attempts": 3, "mode": "adaptive"},
            connect_timeout=10,
            read_timeout=60,
            max_pool_connections=50, # Handles 50 concurrent requests without queueing
        ),
    )
    # Manually entering the async context to keep the connection pool alive globally
    _S3_CLIENT = await _client_context.__aenter__()

async def close_r2_client() -> None:
    """
    Gracefully close the connection pool.
    Must be invoked by the FastAPI lifespan on application shutdown.
    """
    global _client_context, _S3_CLIENT
    if _client_context:
        logger.info("Closing global Async R2 storage client...")
        await _client_context.__aexit__(None, None, None)
        _S3_CLIENT = None

def _get_async_client():
    """Retrieve the initialized singleton client."""
    if _S3_CLIENT is None:
        raise RuntimeError(
            "R2 Client accessed before initialization. "
            "Ensure init_r2_client() is called in the FastAPI lifespan."
        )
    return _S3_CLIENT

# ---------------------------------------------------------------------------
# Streaming Operations
# ---------------------------------------------------------------------------

class R2MultipartStreamer:
    """
    Asynchronous Context Manager for pass-through file streaming to R2.
    Absorbs stream chunks, buffers to 5MB, and flushes to storage natively.
    Guarantees peak memory usage of ~5MB per active upload regardless of file size.
    """
    def __init__(self, key: str, content_type: str = "application/octet-stream", metadata: dict[str, str] | None = None):
        self.client = _get_async_client()
        self.key = key
        self.content_type = content_type
        self.metadata = metadata or {}
        
        self.upload_id: str | None = None
        self.parts: list[dict[str, Any]] = []
        self.buffer = bytearray()
        self.part_number = 1
        
        # S3/R2 requires multipart chunks to be at least 5MB, except for the final part.
        self.chunk_size_limit = 5 * 1024 * 1024 

    async def __aenter__(self) -> "R2MultipartStreamer":
        kwargs = {
            "Bucket": settings.R2_BUCKET_NAME,
            "Key": self.key,
            "ContentType": self.content_type,
            "Metadata": self.metadata
        }
        res = await self.client.create_multipart_upload(**kwargs)
        self.upload_id = res["UploadId"]
        return self

    async def push_chunk(self, chunk: bytes) -> None:
        """Absorb bytes. Block and flush to R2 only when buffer breaches 5MB."""
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
        if exc_type is not None:
            logger.error(f"Stream interrupted for {self.key}. Aborting R2 multipart upload.")
            if self.upload_id:
                await self.client.abort_multipart_upload(
                    Bucket=settings.R2_BUCKET_NAME,
                    Key=self.key,
                    UploadId=self.upload_id
                )
            return

        await self._flush_part()

        if self.upload_id and self.parts:
            await self.client.complete_multipart_upload(
                Bucket=settings.R2_BUCKET_NAME,
                Key=self.key,
                UploadId=self.upload_id,
                MultipartUpload={"Parts": self.parts}
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
    Upload bytes natively to R2. Returns size in bytes.
    Strictly limited to 5MB to prevent OOM crashes on the backend.
    
    UI CONSIDERATION: The frontend MUST enforce a 5MB hard limit on standard 
    file uploads (like avatars or small configs). If the payload exceeds 5MB, 
    the UI must pivot to the chunked streaming endpoint that utilizes R2MultipartStreamer.
    """
    if len(data) > 5 * 1024 * 1024:
        raise ValueError("Payload exceeds 5MB limit for in-memory upload. Use streaming.")
        
    client = _get_async_client()
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

    await client.put_object(**kwargs)
    return len(data)


async def download(key: str) -> bytes:
    """Download an object from R2 natively. Raises ValueError if not found."""
    client = _get_async_client()
    try:
        response = await client.get_object(
            Bucket=settings.R2_BUCKET_NAME,
            Key=key,
        )
        # aiobotocore returns an AsyncIO StreamReader for the Body
        async with response["Body"] as stream:
            return await stream.read()
    except ClientError as exc:
        code = exc.response["Error"]["Code"]
        if code in ("NoSuchKey", "404"):
            raise ValueError(f"Object not found in R2: {key}") from exc
        raise


async def delete(key: str) -> bool:
    """Delete an object from R2 natively. Returns True if deleted, False if not found."""
    client = _get_async_client()
    try:
        await client.delete_object(
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
    
    UI CONSIDERATION: The frontend should always prefer requesting presigned URLs 
    for downloading large files (like backups) rather than pulling bytes through 
    the backend API to save server bandwidth and drop latency.
    """
    client = _get_async_client()
    return await client.generate_presigned_url(
        method,
        Params={"Bucket": settings.R2_BUCKET_NAME, "Key": key},
        ExpiresIn=expires_in,
    )


async def object_exists(key: str) -> bool:
    """Check if an object exists without downloading it natively."""
    client = _get_async_client()
    try:
        await client.head_object(
            Bucket=settings.R2_BUCKET_NAME,
            Key=key,
        )
        return True
    except ClientError:
        return False


async def list_objects(prefix: str) -> list[dict[str, Any]]:
    """List objects under a key prefix natively."""
    client = _get_async_client()
    response = await client.list_objects_v2(
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


def avatar_key(user_id: UUID, mime_type: str) -> str:
    """
    Generates storage key for avatars.
    
    UI CONSIDERATION: The UI must validate that the file being uploaded is strictly 
    one of these three MIME types (image/jpeg, image/png, image/webp) before transmitting.
    Any attempt to upload SVGs, GIFs, or other formats will result in a hard HTTP 400 rejection.
    """
    allowed_types = {
        "image/jpeg": "jpg",
        "image/png": "png",
        "image/webp": "webp"
    }
    if mime_type not in allowed_types:
        raise ValueError("Invalid image type. Only JPG, PNG, and WebP are allowed.")
    return f"avatars/{user_id}.{allowed_types[mime_type]}"