# shared/pagination.py
"""
Pagination helpers.

Two strategies:
  - Offset pagination: simple, for small/medium datasets
  - Cursor pagination: for large datasets where OFFSET is slow

Both return a consistent shape that maps to PaginatedResponse.
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, TypeVar

from sqlalchemy import Select, func, select
from sqlalchemy.ext.asyncio import AsyncSession

T = TypeVar("T")


@dataclass
class OffsetPage:
    items: list[Any]
    total: int
    limit: int
    offset: int

    @property
    def has_more(self) -> bool:
        return self.offset + len(self.items) < self.total


@dataclass
class CursorPage:
    items: list[Any]
    next_cursor: str | None
    has_more: bool
    limit: int


# ---------------------------------------------------------------------------
# Offset pagination
# ---------------------------------------------------------------------------

async def paginate(
    db: AsyncSession,
    query: Select,
    limit: int = 50,
    offset: int = 0,
    max_limit: int = 200,
) -> OffsetPage:
    """
    Execute a SQLAlchemy SELECT with offset pagination.
    Also runs a COUNT query to return the total.
    """
    limit = min(limit, max_limit)

    count_query = select(func.count()).select_from(query.subquery())
    total = await db.scalar(count_query) or 0

    result = await db.execute(query.offset(offset).limit(limit))
    items = list(result.scalars().all())

    return OffsetPage(items=items, total=total, limit=limit, offset=offset)


# ---------------------------------------------------------------------------
# Cursor pagination (for large tables / query history)
# ---------------------------------------------------------------------------

import base64
import json


def encode_cursor(values: dict[str, Any]) -> str:
    return base64.urlsafe_b64encode(json.dumps(values).encode()).decode()


def decode_cursor(cursor: str) -> dict[str, Any]:
    try:
        return json.loads(base64.urlsafe_b64decode(cursor.encode()).decode())
    except Exception:
        raise ValueError("Invalid pagination cursor.")
