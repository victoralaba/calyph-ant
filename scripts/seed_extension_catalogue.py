#!/usr/bin/env python
# scripts/seed_extension_catalogue.py
"""
One-off script: populate the extension_catalog table with all SEED_EXTENSIONS.

Run from the project root:
    python -m scripts.seed_extension_catalogue

This is idempotent — it skips entries that already exist and are curated.
Use --force to overwrite existing non-curated entries.
"""

from __future__ import annotations

import asyncio
import os
import sys

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))


async def main() -> None:
    import argparse
    from loguru import logger
    from shared.telemetry import init_logging
    from core.db import init_db
    import core.db as _core_db
    from domains.platform.extension_catalogue import (
        seed_catalogue,
        get_all_entries,
        SEED_EXTENSIONS,
    )

    parser = argparse.ArgumentParser(description="Seed extension catalogue")
    parser.add_argument(
        "--force",
        action="store_true",
        help="Overwrite existing non-curated entries with fresh seed data",
    )
    args = parser.parse_args()

    init_logging(is_production=False)
    await init_db()

    session_factory = _core_db._session_factory
    if session_factory is None:
        logger.error("Database session factory not initialised. Aborting.")
        sys.exit(1)

    async with session_factory() as db:
        if args.force:
            # Force mode: upsert every seed entry regardless of existing state
            from domains.platform.extension_catalogue import upsert_entry, SOURCE_SEED
            count = 0
            for ext in SEED_EXTENSIONS:
                await upsert_entry(
                    db=db,
                    slug=ext["slug"],
                    display_name=ext.get("display_name", ext["slug"]),
                    description=ext.get("description"),
                    category=ext.get("category", "other"),
                    docs_url=ext.get("docs_url"),
                    adds_types=ext.get("adds_types", []),
                    adds_functions=ext.get("adds_functions", []),
                    adds_index_methods=ext.get("adds_index_methods", []),
                    requires_superuser=ext.get("requires_superuser", False),
                    source=SOURCE_SEED,
                    is_curated=True,
                    force_overwrite=True,
                )
                count += 1
            logger.info(f"Force-upserted {count} extension catalogue entries")
        else:
            n = await seed_catalogue(db)
            logger.info(f"Seeded {n} new extension catalogue entries")

        entries = await get_all_entries(db)

    # Print a summary table
    from collections import Counter
    cats = Counter(e.category for e in entries)
    logger.info("=" * 50)
    logger.info(f"extension_catalog now has {len(entries)} entries:")
    for cat, cnt in sorted(cats.items()):
        logger.info(f"  {cat:20s}: {cnt}")
    logger.info("=" * 50)


if __name__ == "__main__":
    asyncio.run(main())
