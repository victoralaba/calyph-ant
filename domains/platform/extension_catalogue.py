# domains/platform/extension_catalogue.py
"""
Platform-level extension catalogue.

This module owns the global knowledge base about PostgreSQL extensions.
It is NOT per-workspace or per-connection — it is a shared platform
resource consumed by the extensions domain and potentially others.

Responsibilities:
  - ORM model: ExtensionCatalogEntry (persisted in Calyphant's own DB)
  - CRUD service functions for reading and writing catalogue entries
  - External metadata enrichment from PGXN and Trunk
  - Celery background task for async enrichment and staleness refresh
  - Seed data (migrated from the old hardcoded EXTENSION_REGISTRY dict)

Dependency direction:
  extensions domain  →  platform catalogue  (one way only)
  The catalogue has no knowledge of connections, workspaces, or asyncpg.

External sources (in priority order):
  1. PGXN  — https://api.pgxn.org/v1/extension/{name}.json
  2. Trunk — https://registry.pgtrunk.io/extensions/{name}
  3. Manual / seed data — populated from the old hardcoded dict

Staleness policy:
  Entries sourced from external APIs are re-fetched after METADATA_TTL_DAYS.
  Manual / seed entries are never automatically overwritten unless
  force_refresh=True is passed to enrich_extension().
"""

from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any
from uuid import UUID, uuid4

import httpx
from loguru import logger
from sqlalchemy import (
    Boolean,
    DateTime,
    Integer,
    String,
    Text,
    UniqueConstraint,
    select,
)
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from shared.types import Base


# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

METADATA_TTL_DAYS = 30          # Re-fetch external metadata after this many days
EXTERNAL_FETCH_TIMEOUT = 5.0    # Seconds before giving up on an external source

# Valid source labels — written to ExtensionCatalogEntry.source
SOURCE_SEED    = "seed"          # Migrated from the old hardcoded dict
SOURCE_PGXN    = "pgxn"          # Fetched from api.pgxn.org
SOURCE_TRUNK   = "trunk"         # Fetched from registry.pgtrunk.io
SOURCE_MANUAL  = "manual"        # Entered manually via admin API (future)

VALID_SOURCES = {SOURCE_SEED, SOURCE_PGXN, SOURCE_TRUNK, SOURCE_MANUAL}

# Extension categories used across the UI
CATEGORIES = {
    "ai",
    "data_types",
    "geospatial",
    "language",
    "monitoring",
    "performance",
    "search",
    "security",
    "time_series",
    "utility",
    "other",
}


# ---------------------------------------------------------------------------
# ORM model
# ---------------------------------------------------------------------------

class ExtensionCatalogEntry(Base):
    """
    Platform-wide metadata for a known PostgreSQL extension.

    One row per extension slug (the canonical name Postgres uses in
    pg_available_extensions, e.g. "uuid-ossp", "vector", "postgis").

    This table is global — not scoped to any workspace or connection.
    """

    __tablename__ = "extension_catalog"
    __table_args__ = (
        UniqueConstraint("slug", name="uq_extension_catalog_slug"),
    )

    id: Mapped[UUID] = mapped_column(
        PG_UUID(as_uuid=True), primary_key=True, default=uuid4
    )

    # Canonical extension name as Postgres knows it
    slug: Mapped[str] = mapped_column(
        String(120), nullable=False, unique=True, index=True
    )

    # Display / UI metadata
    display_name: Mapped[str] = mapped_column(String(120), nullable=False)
    description: Mapped[str | None] = mapped_column(Text, nullable=True)
    category: Mapped[str] = mapped_column(
        String(60), nullable=False, default="other"
    )
    docs_url: Mapped[str | None] = mapped_column(Text, nullable=True)

    # Capability metadata — stored as JSON arrays
    # e.g. adds_types: ["vector"]  adds_functions: ["uuid_generate_v4()"]
    adds_types: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    adds_functions: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)
    adds_index_methods: Mapped[list] = mapped_column(JSONB, default=list, nullable=False)

    # Whether this extension requires superuser to install
    requires_superuser: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # Provenance — where did this metadata come from?
    source: Mapped[str] = mapped_column(
        String(30), nullable=False, default=SOURCE_SEED
    )

    # Whether a human has reviewed and approved this entry
    is_curated: Mapped[bool] = mapped_column(
        Boolean, default=False, nullable=False
    )

    # How many distinct Calyphant connections have this installed.
    # Incremented by the extensions domain when it encounters an installed
    # extension. Used for sorting "popular" extensions in the UI.
    install_count: Mapped[int] = mapped_column(
        Integer, default=0, nullable=False
    )

    # Timestamps
    last_verified_at: Mapped[datetime | None] = mapped_column(
        DateTime(timezone=True), nullable=True
    )
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )
    updated_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Seed data — migrated from the old hardcoded EXTENSION_REGISTRY dict
# ---------------------------------------------------------------------------

SEED_EXTENSIONS: list[dict[str, Any]] = [
    {
        "slug": "vector",
        "display_name": "pgvector",
        "description": "Vector similarity search for AI/ML embeddings.",
        "category": "ai",
        "adds_types": ["vector"],
        "adds_index_methods": ["ivfflat", "hnsw"],
        "requires_superuser": False,
        "docs_url": "https://github.com/pgvector/pgvector",
    },
    {
        "slug": "uuid-ossp",
        "display_name": "uuid-ossp",
        "description": "UUID generation functions including uuid_generate_v4().",
        "category": "utility",
        "adds_functions": ["uuid_generate_v4()", "uuid_generate_v1()"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/uuid-ossp.html",
    },
    {
        "slug": "pgcrypto",
        "display_name": "pgcrypto",
        "description": "Cryptographic functions: hashing, encryption, random bytes.",
        "category": "security",
        "adds_functions": ["crypt()", "gen_random_bytes()", "pgp_sym_encrypt()"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/pgcrypto.html",
    },
    {
        "slug": "pg_trgm",
        "display_name": "pg_trgm",
        "description": "Trigram-based fuzzy text search and similarity.",
        "category": "search",
        "adds_index_methods": ["gin", "gist"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/pgtrgm.html",
    },
    {
        "slug": "postgis",
        "display_name": "PostGIS",
        "description": "Geospatial data types, functions, and indexes.",
        "category": "geospatial",
        "adds_types": ["geometry", "geography", "box2d", "box3d"],
        "adds_index_methods": ["gist", "brin"],
        "requires_superuser": True,
        "docs_url": "https://postgis.net/",
    },
    {
        "slug": "pg_stat_statements",
        "display_name": "pg_stat_statements",
        "description": "Track execution statistics of all SQL statements.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgstatstatements.html",
    },
    {
        "slug": "hstore",
        "display_name": "hstore",
        "description": "Key-value store data type.",
        "category": "data_types",
        "adds_types": ["hstore"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/hstore.html",
    },
    {
        "slug": "ltree",
        "display_name": "ltree",
        "description": "Hierarchical tree-like data structures.",
        "category": "data_types",
        "adds_types": ["ltree"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/ltree.html",
    },
    {
        "slug": "pg_partman",
        "display_name": "pg_partman",
        "description": "Partition management for time-series and serial data.",
        "category": "performance",
        "requires_superuser": True,
        "docs_url": "https://github.com/pgpartman/pg_partman",
    },
    {
        "slug": "timescaledb",
        "display_name": "TimescaleDB",
        "description": "Time-series data optimisation and hypertables.",
        "category": "time_series",
        "requires_superuser": True,
        "docs_url": "https://docs.timescale.com/",
    },
    {
        "slug": "citext",
        "display_name": "citext",
        "description": "Case-insensitive text data type.",
        "category": "data_types",
        "adds_types": ["citext"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/citext.html",
    },
    {
        "slug": "pg_cron",
        "display_name": "pg_cron",
        "description": "Cron-based job scheduler for PostgreSQL.",
        "category": "utility",
        "requires_superuser": True,
        "docs_url": "https://github.com/citusdata/pg_cron",
    },
    {
        "slug": "plpgsql",
        "display_name": "PL/pgSQL",
        "description": "Procedural language for stored procedures and triggers.",
        "category": "language",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/plpgsql.html",
    },
    # ---------------------------------------------------------------------------
    # Additional well-known extensions beyond the original hardcoded list
    # ---------------------------------------------------------------------------
    {
        "slug": "pg_jsonschema",
        "display_name": "pg_jsonschema",
        "description": "JSON Schema validation for jsonb columns.",
        "category": "data_types",
        "requires_superuser": False,
        "docs_url": "https://github.com/supabase/pg_jsonschema",
    },
    {
        "slug": "pgaudit",
        "display_name": "pgAudit",
        "description": "Detailed session and object audit logging.",
        "category": "security",
        "requires_superuser": True,
        "docs_url": "https://www.pgaudit.org/",
    },
    {
        "slug": "pg_repack",
        "display_name": "pg_repack",
        "description": "Remove bloat from tables and indexes without locking.",
        "category": "performance",
        "requires_superuser": True,
        "docs_url": "https://reorg.github.io/pg_repack/",
    },
    {
        "slug": "tablefunc",
        "display_name": "tablefunc",
        "description": "Functions that return tables, including crosstab (pivot).",
        "category": "utility",
        "adds_functions": ["crosstab()", "normal_rand()", "connectby()"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/tablefunc.html",
    },
    {
        "slug": "earthdistance",
        "display_name": "earthdistance",
        "description": "Calculate great-circle distances on the surface of the Earth.",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/earthdistance.html",
    },
    {
        "slug": "cube",
        "display_name": "cube",
        "description": "Multi-dimensional cube data type.",
        "category": "data_types",
        "adds_types": ["cube"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/cube.html",
    },
    {
        "slug": "intarray",
        "display_name": "intarray",
        "description": "Functions, operators, and index support for arrays of integers.",
        "category": "data_types",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/intarray.html",
    },
    {
        "slug": "fuzzystrmatch",
        "display_name": "fuzzystrmatch",
        "description": "Fuzzy string matching: soundex, levenshtein, metaphone.",
        "category": "search",
        "adds_functions": ["soundex()", "levenshtein()", "metaphone()"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/fuzzystrmatch.html",
    },
    {
        "slug": "unaccent",
        "display_name": "unaccent",
        "description": "Text search dictionary that removes accents.",
        "category": "search",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/unaccent.html",
    },
    {
        "slug": "pg_hint_plan",
        "display_name": "pg_hint_plan",
        "description": "Control query planner with hints in SQL comments.",
        "category": "performance",
        "requires_superuser": False,
        "docs_url": "https://pghintplan.osdn.jp/",
    },
    {
        "slug": "hypopg",
        "display_name": "HypoPG",
        "description": "Hypothetical indexes — test index impact without building them.",
        "category": "performance",
        "requires_superuser": False,
        "docs_url": "https://hypopg.readthedocs.io/",
    },
    {
        "slug": "pg_net",
        "display_name": "pg_net",
        "description": "Async HTTP/HTTPS requests directly from SQL.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://github.com/supabase/pg_net",
    },
    {
        "slug": "pgsodium",
        "display_name": "pgsodium",
        "description": "Modern cryptography for PostgreSQL using libsodium.",
        "category": "security",
        "requires_superuser": True,
        "docs_url": "https://github.com/michelp/pgsodium",
    },
    {
        "slug": "pg_graphql",
        "display_name": "pg_graphql",
        "description": "GraphQL support inside PostgreSQL.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://supabase.github.io/pg_graphql/",
    },
    {
        "slug": "wrappers",
        "display_name": "Supabase Wrappers",
        "description": "Foreign Data Wrappers framework for external data sources.",
        "category": "utility",
        "requires_superuser": True,
        "docs_url": "https://supabase.github.io/wrappers/",
    },
    {
        "slug": "rum",
        "display_name": "RUM",
        "description": "RUM index for faster full-text search than GIN.",
        "category": "search",
        "adds_index_methods": ["rum"],
        "requires_superuser": False,
        "docs_url": "https://github.com/postgrespro/rum",
    },
    {
        "slug": "bloom",
        "display_name": "bloom",
        "description": "Bloom filter index for equality queries on many columns.",
        "category": "performance",
        "adds_index_methods": ["bloom"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/bloom.html",
    },
    {
        "slug": "pg_visibility",
        "display_name": "pg_visibility",
        "description": "Examine the visibility map and page-level visibility info.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgvisibility.html",
    },
    {
        "slug": "pgstattuple",
        "display_name": "pgstattuple",
        "description": "Obtain tuple-level statistics for tables and indexes.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgstattuple.html",
    },
    {
        "slug": "pg_buffercache",
        "display_name": "pg_buffercache",
        "description": "Examine what is happening in the shared buffer cache.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgbuffercache.html",
    },
    {
        "slug": "auto_explain",
        "display_name": "auto_explain",
        "description": "Automatically log execution plans of slow queries.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/auto-explain.html",
    },
    {
        "slug": "pg_prewarm",
        "display_name": "pg_prewarm",
        "description": "Load relation data into the buffer cache on startup.",
        "category": "performance",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/pgprewarm.html",
    },
    {
        "slug": "plv8",
        "display_name": "PL/V8",
        "description": "JavaScript procedural language powered by V8.",
        "category": "language",
        "requires_superuser": True,
        "docs_url": "https://plv8.github.io/",
    },
    {
        "slug": "plpython3u",
        "display_name": "PL/Python3",
        "description": "Python 3 procedural language (untrusted).",
        "category": "language",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/plpython.html",
    },
    {
        "slug": "pltcl",
        "display_name": "PL/Tcl",
        "description": "Tcl procedural language for PostgreSQL.",
        "category": "language",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pltcl.html",
    },
    {
        "slug": "address_standardizer",
        "display_name": "address_standardizer",
        "description": "Parse addresses into constituent elements (PostGIS companion).",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://postgis.net/docs/Address_Standardizer.html",
    },
    {
        "slug": "h3",
        "display_name": "H3",
        "description": "Uber's H3 hierarchical geospatial indexing system.",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://github.com/zachasme/h3-pg",
    },
    {
        "slug": "pgrouting",
        "display_name": "pgRouting",
        "description": "Geospatial routing and network analysis.",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://pgrouting.org/",
    },
    {
        "slug": "pg_uuidv7",
        "display_name": "pg_uuidv7",
        "description": "Generate UUIDv7 values (time-ordered, sortable).",
        "category": "utility",
        "adds_functions": ["uuid_generate_v7()"],
        "requires_superuser": False,
        "docs_url": "https://github.com/fboulnois/pg_uuidv7",
    },
    {
        "slug": "pg_mooncake",
        "display_name": "pg_mooncake",
        "description": "Columnstore tables and DuckDB execution engine for analytics.",
        "category": "performance",
        "requires_superuser": False,
        "docs_url": "https://github.com/Mooncake-Labs/pg_mooncake",
    },
    {
        "slug": "pg_search",
        "display_name": "pg_search",
        "description": "Full-text search over SQL tables using BM25 ranking.",
        "category": "search",
        "requires_superuser": False,
        "docs_url": "https://github.com/paradedb/paradedb",
    },
    {
        "slug": "pgroonga",
        "display_name": "PGroonga",
        "description": "Fast full-text search index for all languages using Groonga.",
        "category": "search",
        "requires_superuser": False,
        "docs_url": "https://pgroonga.github.io/",
    },
    {
        "slug": "orafce",
        "display_name": "Orafce",
        "description": "Oracle-compatible functions and packages for migration.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://github.com/orafce/orafce",
    },
    {
        "slug": "pg_tle",
        "display_name": "pg_tle",
        "description": "Trusted Language Extensions for safe in-database extension development.",
        "category": "utility",
        "requires_superuser": True,
        "docs_url": "https://github.com/aws/pg_tle",
    },
    # ---------------------------------------------------------------------------
    # Additional built-in and community extensions
    # ---------------------------------------------------------------------------
    {
        "slug": "address_standardizer_data_us",
        "display_name": "address_standardizer_data_us",
        "description": "Address Standardizer US dataset — rules for parsing US postal addresses.",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://postgis.net/docs/Address_Standardizer.html",
    },
    {
        "slug": "amcheck",
        "display_name": "amcheck",
        "description": "Functions for verifying relation integrity (B-tree index consistency checks).",
        "category": "monitoring",
        "adds_functions": ["bt_index_check()", "bt_index_parent_check()"],
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/amcheck.html",
    },
    {
        "slug": "anon",
        "display_name": "PostgreSQL Anonymizer",
        "description": "Anonymization and data masking for PostgreSQL.",
        "category": "security",
        "requires_superuser": True,
        "docs_url": "https://postgresql-anonymizer.readthedocs.io/",
    },
    {
        "slug": "autoinc",
        "display_name": "autoinc",
        "description": "Functions for auto-incrementing fields (legacy trigger-based).",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/contrib-spi.html",
    },
    {
        "slug": "btree_gin",
        "display_name": "btree_gin",
        "description": "GIN operator classes for B-tree-compatible data types.",
        "category": "performance",
        "adds_index_methods": ["gin"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/btree-gin.html",
    },
    {
        "slug": "btree_gist",
        "display_name": "btree_gist",
        "description": "GiST operator classes for B-tree-compatible data types (enables exclusion constraints).",
        "category": "performance",
        "adds_index_methods": ["gist"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/btree-gist.html",
    },
    {
        "slug": "dblink",
        "display_name": "dblink",
        "description": "Connect to and query other PostgreSQL databases from within a session.",
        "category": "utility",
        "adds_functions": ["dblink()", "dblink_exec()", "dblink_connect()"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/dblink.html",
    },
    {
        "slug": "dict_int",
        "display_name": "dict_int",
        "description": "Text-search dictionary template for integers.",
        "category": "search",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/dict-int.html",
    },
    {
        "slug": "dict_xsyn",
        "display_name": "dict_xsyn",
        "description": "Text-search dictionary for extended synonym processing.",
        "category": "search",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/dict-xsyn.html",
    },
    {
        "slug": "file_fdw",
        "display_name": "file_fdw",
        "description": "Foreign-data wrapper for reading flat files from the server filesystem.",
        "category": "utility",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/file-fdw.html",
    },
    {
        "slug": "insert_username",
        "display_name": "insert_username",
        "description": "Trigger function to track which user modified a table row.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/contrib-spi.html",
    },
    {
        "slug": "intagg",
        "display_name": "intagg",
        "description": "Integer aggregator and enumerator (obsolete — use built-in arrays).",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/intagg.html",
    },
    {
        "slug": "isn",
        "display_name": "isn",
        "description": "Data types for international product numbering standards (ISBN, EAN, ISSN, etc.).",
        "category": "data_types",
        "adds_types": ["isbn", "ean13", "issn", "ismn", "upc"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/isn.html",
    },
    {
        "slug": "lo",
        "display_name": "lo",
        "description": "Large Object maintenance triggers to prevent orphaned large objects.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/lo.html",
    },
    {
        "slug": "moddatetime",
        "display_name": "moddatetime",
        "description": "Trigger function to track last modification time of a row.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/contrib-spi.html",
    },
    {
        "slug": "old_snapshot",
        "display_name": "old_snapshot",
        "description": "Utilities to investigate old_snapshot_threshold behaviour.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/oldsnapshot.html",
    },
    {
        "slug": "pageinspect",
        "display_name": "pageinspect",
        "description": "Inspect raw contents of database pages at a low level.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pageinspect.html",
    },
    {
        "slug": "pg_freespacemap",
        "display_name": "pg_freespacemap",
        "description": "Examine the free space map (FSM) for tables and indexes.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgfreespacemap.html",
    },
    {
        "slug": "pg_surgery",
        "display_name": "pg_surgery",
        "description": "Perform surgery on a damaged relation (emergency data recovery).",
        "category": "utility",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgsurgery.html",
    },
    {
        "slug": "pg_walinspect",
        "display_name": "pg_walinspect",
        "description": "Inspect WAL log contents at a low level.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgwalinspect.html",
    },
    {
        "slug": "pgrowlocks",
        "display_name": "pgrowlocks",
        "description": "Show row-locking information for a given table.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/pgrowlocks.html",
    },
    {
        "slug": "plperl",
        "display_name": "PL/Perl",
        "description": "Trusted Perl procedural language for PostgreSQL stored procedures.",
        "category": "language",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/plperl.html",
    },
    {
        "slug": "plperlu",
        "display_name": "PL/PerlU",
        "description": "Untrusted Perl procedural language (full Perl access).",
        "category": "language",
        "requires_superuser": True,
        "docs_url": "https://www.postgresql.org/docs/current/plperl.html",
    },
    {
        "slug": "postgres_fdw",
        "display_name": "postgres_fdw",
        "description": "Foreign-data wrapper for remote PostgreSQL servers.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/postgres-fdw.html",
    },
    {
        "slug": "refint",
        "display_name": "refint",
        "description": "Trigger functions for implementing referential integrity (legacy).",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/contrib-spi.html",
    },
    {
        "slug": "seg",
        "display_name": "seg",
        "description": "Data type for representing line segments or floating-point intervals.",
        "category": "data_types",
        "adds_types": ["seg"],
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/seg.html",
    },
    {
        "slug": "sslinfo",
        "display_name": "sslinfo",
        "description": "Information about the SSL certificate of the current connection.",
        "category": "security",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/sslinfo.html",
    },
    {
        "slug": "tcn",
        "display_name": "tcn",
        "description": "Triggered change notification trigger function.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/tcn.html",
    },
    {
        "slug": "tsm_system_rows",
        "display_name": "tsm_system_rows",
        "description": "TABLESAMPLE method that accepts number of rows as a limit.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/tsm-system-rows.html",
    },
    {
        "slug": "tsm_system_time",
        "display_name": "tsm_system_time",
        "description": "TABLESAMPLE method that accepts time in milliseconds as a limit.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/tsm-system-time.html",
    },
    {
        "slug": "xml2",
        "display_name": "xml2",
        "description": "XPath querying and XSLT functions (legacy — prefer built-in XML support).",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/xml2.html",
    },
    {
        "slug": "pg_ivm",
        "display_name": "pg_ivm",
        "description": "Incremental View Maintenance — keep materialized views up to date incrementally.",
        "category": "performance",
        "requires_superuser": False,
        "docs_url": "https://github.com/sraoss/pg_ivm",
    },
    {
        "slug": "age",
        "display_name": "Apache AGE",
        "description": "Graph database extension for PostgreSQL (openCypher queries).",
        "category": "data_types",
        "requires_superuser": True,
        "docs_url": "https://age.apache.org/",
    },
    {
        "slug": "pg_qualstats",
        "display_name": "pg_qualstats",
        "description": "Collect statistics about predicates in WHERE and JOIN clauses.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://github.com/powa-team/pg_qualstats",
    },
    {
        "slug": "pg_squeeze",
        "display_name": "pg_squeeze",
        "description": "Remove unused space from a relation without heavy locking.",
        "category": "performance",
        "requires_superuser": True,
        "docs_url": "https://github.com/cybertec-postgresql/pg_squeeze",
    },
    {
        "slug": "pgtap",
        "display_name": "pgTAP",
        "description": "Unit testing framework for PostgreSQL using TAP protocol.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://pgtap.org/",
    },
    {
        "slug": "postgis_raster",
        "display_name": "PostGIS Raster",
        "description": "Raster data type and functions for PostGIS.",
        "category": "geospatial",
        "requires_superuser": True,
        "docs_url": "https://postgis.net/docs/using_raster_dataman.html",
    },
    {
        "slug": "postgis_sfcgal",
        "display_name": "PostGIS SFCGAL",
        "description": "PostGIS wrapper for SFCGAL 3D geometry operations.",
        "category": "geospatial",
        "requires_superuser": True,
        "docs_url": "https://postgis.net/docs/reference.html#reference_sfcgal",
    },
    {
        "slug": "postgis_tiger_geocoder",
        "display_name": "PostGIS Tiger Geocoder",
        "description": "US address geocoder using TIGER census data.",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://postgis.net/docs/Extras.html#Tiger_Geocoder",
    },
    {
        "slug": "postgis_topology",
        "display_name": "PostGIS Topology",
        "description": "Topological data types and functions for PostGIS.",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://postgis.net/docs/Topology.html",
    },
    {
        "slug": "pg_tde",
        "display_name": "pg_tde",
        "description": "Transparent Data Encryption at the table level.",
        "category": "security",
        "requires_superuser": True,
        "docs_url": "https://github.com/Percona-Lab/pg_tde",
    },
    {
        "slug": "pg_profile",
        "display_name": "pg_profile",
        "description": "PostgreSQL load profile reports from pg_stat_statements snapshots.",
        "category": "monitoring",
        "requires_superuser": False,
        "docs_url": "https://github.com/zubkov-andrei/pg_profile",
    },
    {
        "slug": "pg_store_plans",
        "display_name": "pg_store_plans",
        "description": "Track execution plan statistics of all SQL statements.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://github.com/ossc-db/pg_store_plans",
    },
    {
        "slug": "powa",
        "display_name": "PoWA",
        "description": "PostgreSQL Workload Analyser — collect and analyze performance metrics.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://powa.readthedocs.io/",
    },
    {
        "slug": "pg_wait_sampling",
        "display_name": "pg_wait_sampling",
        "description": "Sampling-based statistics on wait events.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://github.com/postgrespro/pg_wait_sampling",
    },
    {
        "slug": "pg_show_plans",
        "display_name": "pg_show_plans",
        "description": "Show query execution plans for currently running SQL statements.",
        "category": "monitoring",
        "requires_superuser": True,
        "docs_url": "https://github.com/cybertec-postgresql/pg_show_plans",
    },
    {
        "slug": "set_user",
        "display_name": "set_user",
        "description": "Privilege escalation with audit logging — SET ROLE with trail.",
        "category": "security",
        "requires_superuser": True,
        "docs_url": "https://github.com/pgaudit/set_user",
    },
    {
        "slug": "pgmemcache",
        "display_name": "pgmemcache",
        "description": "Memcached client interface for PostgreSQL.",
        "category": "utility",
        "requires_superuser": False,
        "docs_url": "https://github.com/ohmu/pgmemcache",
    },
    {
        "slug": "periods",
        "display_name": "periods",
        "description": "Temporal table support: PERIOD columns and system-time versioning.",
        "category": "time_series",
        "requires_superuser": False,
        "docs_url": "https://github.com/xocolatl/periods",
    },
    {
        "slug": "temporal_tables",
        "display_name": "temporal_tables",
        "description": "Temporal table support with system-period and application-period.",
        "category": "time_series",
        "requires_superuser": False,
        "docs_url": "https://github.com/arkhipov/temporal_tables",
    },
    {
        "slug": "unit",
        "display_name": "unit",
        "description": "SI units extension — store and convert physical quantities.",
        "category": "data_types",
        "adds_types": ["unit"],
        "requires_superuser": False,
        "docs_url": "https://github.com/df7cb/postgresql-unit",
    },
    {
        "slug": "uri",
        "display_name": "uri",
        "description": "URI data type for PostgreSQL.",
        "category": "data_types",
        "adds_types": ["uri"],
        "requires_superuser": False,
        "docs_url": "https://github.com/petere/pguri",
    },
    {
        "slug": "ip4r",
        "display_name": "ip4r",
        "description": "IPv4/IPv6 range type with GiST index support.",
        "category": "data_types",
        "adds_types": ["ip4", "ip4r", "ip6", "ip6r"],
        "requires_superuser": False,
        "docs_url": "https://github.com/RhodiumToad/ip4r",
    },
    {
        "slug": "pgpcre",
        "display_name": "pgpcre",
        "description": "Perl-compatible regular expressions (PCRE) for PostgreSQL.",
        "category": "search",
        "requires_superuser": False,
        "docs_url": "https://github.com/petere/pgpcre",
    },
    {
        "slug": "pg_rational",
        "display_name": "pg_rational",
        "description": "Rational numbers (fractions) as a native PostgreSQL type.",
        "category": "data_types",
        "adds_types": ["rational"],
        "requires_superuser": False,
        "docs_url": "https://github.com/begriffs/pg_rational",
    },
    {
        "slug": "pg_sphere",
        "display_name": "pg_sphere",
        "description": "Spherical geometry types and operators for astronomical coordinates.",
        "category": "geospatial",
        "requires_superuser": False,
        "docs_url": "https://pgsphere.github.io/",
    },
    {
        "slug": "unaccent",
        "display_name": "unaccent",
        "description": "Text search dictionary that strips accents from lexemes.",
        "category": "search",
        "requires_superuser": False,
        "docs_url": "https://www.postgresql.org/docs/current/unaccent.html",
    },
    {
        "slug": "pg_background",
        "display_name": "pg_background",
        "description": "Run SQL statements in a background worker process.",
        "category": "utility",
        "requires_superuser": True,
        "docs_url": "https://github.com/vibhorkum/pg_background",
    },
    {
        "slug": "plsh",
        "display_name": "PL/sh",
        "description": "Shell scripting procedural language for PostgreSQL.",
        "category": "language",
        "requires_superuser": True,
        "docs_url": "https://github.com/petere/plsh",
    },
    {
        "slug": "pg_cron",
        "display_name": "pg_cron",
        "description": "Cron-based job scheduler for PostgreSQL.",
        "category": "utility",
        "requires_superuser": True,
        "docs_url": "https://github.com/citusdata/pg_cron",
    },
]


# ---------------------------------------------------------------------------
# Service functions
# ---------------------------------------------------------------------------

async def get_entry(db: AsyncSession, slug: str) -> ExtensionCatalogEntry | None:
    """Fetch a single catalogue entry by extension slug."""
    result = await db.execute(
        select(ExtensionCatalogEntry).where(ExtensionCatalogEntry.slug == slug)
    )
    return result.scalar_one_or_none()


async def get_all_entries(db: AsyncSession) -> list[ExtensionCatalogEntry]:
    """Return all entries ordered by slug."""
    result = await db.execute(
        select(ExtensionCatalogEntry).order_by(ExtensionCatalogEntry.slug)
    )
    return list(result.scalars().all())


async def get_entries_by_slugs(
    db: AsyncSession,
    slugs: list[str],
) -> dict[str, ExtensionCatalogEntry]:
    """
    Bulk-fetch catalogue entries for a list of slugs.
    Returns a dict keyed by slug for O(1) lookup in the caller.
    """
    if not slugs:
        return {}
    result = await db.execute(
        select(ExtensionCatalogEntry).where(
            ExtensionCatalogEntry.slug.in_(slugs)
        )
    )
    return {entry.slug: entry for entry in result.scalars().all()}


async def upsert_entry(
    db: AsyncSession,
    slug: str,
    display_name: str,
    description: str | None = None,
    category: str = "other",
    docs_url: str | None = None,
    adds_types: list[str] | None = None,
    adds_functions: list[str] | None = None,
    adds_index_methods: list[str] | None = None,
    requires_superuser: bool = False,
    source: str = SOURCE_SEED,
    is_curated: bool = False,
    force_overwrite: bool = False,
) -> ExtensionCatalogEntry:
    """
    Insert or update a catalogue entry.

    Curated (manually reviewed) entries are never overwritten by
    auto-fetched data unless force_overwrite=True. This protects human
    corrections from being clobbered by a stale external source.
    """
    existing = await get_entry(db, slug)

    if existing:
        # Protect curated entries from auto-fetch overwrites
        if existing.is_curated and not force_overwrite:
            logger.debug(
                f"Catalogue: skipping overwrite of curated entry '{slug}'"
            )
            return existing

        existing.display_name = display_name
        if description:
            existing.description = description
        existing.category = category
        if docs_url:
            existing.docs_url = docs_url
        existing.adds_types = adds_types or []
        existing.adds_functions = adds_functions or []
        existing.adds_index_methods = adds_index_methods or []
        existing.requires_superuser = requires_superuser
        existing.source = source
        if is_curated:
            existing.is_curated = True
        existing.last_verified_at = datetime.now(timezone.utc)
        existing.updated_at = datetime.now(timezone.utc)
        await db.commit()
        await db.refresh(existing)
        return existing

    entry = ExtensionCatalogEntry(
        slug=slug,
        display_name=display_name,
        description=description,
        category=category,
        docs_url=docs_url,
        adds_types=adds_types or [],
        adds_functions=adds_functions or [],
        adds_index_methods=adds_index_methods or [],
        requires_superuser=requires_superuser,
        source=source,
        is_curated=is_curated,
        last_verified_at=datetime.now(timezone.utc),
    )
    db.add(entry)
    await db.commit()
    await db.refresh(entry)
    logger.info(f"Catalogue: new entry added for '{slug}' (source={source})")
    return entry


async def increment_install_count(slug: str) -> None:
    """
    Increment the install_count for an extension.
    Called by the extensions domain when it observes an installed extension.
    Fire-and-forget — never raises.
    """
    try:
        from core.db import _session_factory
        if not _session_factory:
            return
        async with _session_factory() as db:
            entry = await get_entry(db, slug)
            if entry:
                entry.install_count += 1
                entry.updated_at = datetime.now(timezone.utc)
                await db.commit()
    except Exception as exc:
        logger.warning(f"Catalogue: failed to increment install_count for '{slug}': {exc}")


def is_stale(entry: ExtensionCatalogEntry) -> bool:
    """
    Return True if the entry's metadata is old enough to warrant a refresh.
    Seed and manual entries are never considered stale.
    """
    if entry.source in (SOURCE_SEED, SOURCE_MANUAL):
        return False
    if entry.last_verified_at is None:
        return True
    age = datetime.now(timezone.utc) - entry.last_verified_at
    return age > timedelta(days=METADATA_TTL_DAYS)


async def seed_catalogue(db: AsyncSession) -> int:
    """
    Populate the catalogue with SEED_EXTENSIONS data.
    Called during application startup (init_db) and by the seed script.
    Skips entries that already exist and are curated.
    Returns the count of newly inserted entries.
    """
    inserted = 0
    for ext in SEED_EXTENSIONS:
        existing = await get_entry(db, ext["slug"])
        if existing and existing.is_curated:
            continue
        if existing:
            # Seed data only back-fills missing fields, never overwrites
            continue
        entry = ExtensionCatalogEntry(
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
            is_curated=True,     # Seed data is considered curated
            last_verified_at=datetime.now(timezone.utc),
        )
        db.add(entry)
        inserted += 1

    if inserted:
        await db.commit()
        logger.info(f"Catalogue: seeded {inserted} extension entries")

    return inserted


# ---------------------------------------------------------------------------
# External metadata enrichment
# ---------------------------------------------------------------------------

async def fetch_from_pgxn(slug: str) -> dict[str, Any] | None:
    """
    Attempt to fetch extension metadata from PGXN.
    Returns a normalised dict or None on failure.
    """
    url = f"https://api.pgxn.org/v1/extension/{slug}.json"
    try:
        async with httpx.AsyncClient(timeout=EXTERNAL_FETCH_TIMEOUT) as client:
            response = await client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()

        # PGXN response shape varies — normalise defensively
        abstract = data.get("abstract") or data.get("description") or ""
        latest = data.get("latest", "")
        releases = data.get("releases", {})
        latest_meta = releases.get(latest, {}) if latest else {}

        return {
            "display_name": data.get("name", slug),
            "description": abstract.strip() or None,
            "docs_url": (
                f"https://pgxn.org/dist/{slug}/"
                if slug else None
            ),
            "category": "other",   # PGXN has no category field; leave as other
            "source": SOURCE_PGXN,
        }
    except Exception as exc:
        logger.debug(f"PGXN fetch failed for '{slug}': {exc}")
        return None


async def fetch_from_trunk(slug: str) -> dict[str, Any] | None:
    """
    Attempt to fetch extension metadata from the Trunk registry.
    Returns a normalised dict or None on failure.
    """
    url = f"https://registry.pgtrunk.io/extensions/detail/{slug}"
    try:
        async with httpx.AsyncClient(timeout=EXTERNAL_FETCH_TIMEOUT) as client:
            response = await client.get(url)
            if response.status_code == 404:
                return None
            response.raise_for_status()
            data = response.json()

        # Trunk response normalisation
        description = (
            data.get("description")
            or data.get("abstract")
            or ""
        ).strip() or None

        # Trunk sometimes includes a categories list
        categories = data.get("categories") or []
        category = categories[0].lower() if categories else "other"
        if category not in CATEGORIES:
            category = "other"

        return {
            "display_name": data.get("name", slug),
            "description": description,
            "docs_url": data.get("documentation_link") or data.get("repository"),
            "category": category,
            "source": SOURCE_TRUNK,
        }
    except Exception as exc:
        logger.debug(f"Trunk fetch failed for '{slug}': {exc}")
        return None


async def enrich_extension(
    db: AsyncSession,
    slug: str,
    force_refresh: bool = False,
) -> ExtensionCatalogEntry | None:
    """
    Enrich a catalogue entry for *slug* by fetching from external sources.

    Strategy:
      1. If an up-to-date entry already exists and force_refresh=False, return it.
      2. Try PGXN first, then Trunk.
      3. Persist whatever we find.
      4. If both sources fail, create a minimal stub so we don't re-attempt
         on every single request (stub will be refreshed after METADATA_TTL_DAYS).

    Returns the upserted entry, or None if slug is completely unknown and
    all external sources failed.
    """
    existing = await get_entry(db, slug)
    if existing and not is_stale(existing) and not force_refresh:
        return existing

    logger.info(f"Catalogue: enriching '{slug}' from external sources")

    # Try sources in priority order
    meta: dict[str, Any] | None = None
    for fetch_fn in (fetch_from_pgxn, fetch_from_trunk):
        meta = await fetch_fn(slug)
        if meta:
            break

    if meta:
        return await upsert_entry(
            db=db,
            slug=slug,
            display_name=meta.get("display_name", slug),
            description=meta.get("description"),
            category=meta.get("category", "other"),
            docs_url=meta.get("docs_url"),
            source=meta.get("source", SOURCE_PGXN),
            is_curated=False,
            force_overwrite=force_refresh,
        )

    # Both sources failed — create a minimal stub so we stop retrying
    # until the TTL expires
    if not existing:
        logger.info(
            f"Catalogue: no metadata found for '{slug}' — creating stub entry"
        )
        return await upsert_entry(
            db=db,
            slug=slug,
            display_name=slug,
            description=None,
            category="other",
            source=SOURCE_PGXN,   # Mark as pgxn-attempted so TTL applies
            is_curated=False,
        )

    # Existing entry that we failed to refresh — update timestamp to reset TTL
    existing.last_verified_at = datetime.now(timezone.utc)
    existing.updated_at = datetime.now(timezone.utc)
    await db.commit()
    return existing


# ---------------------------------------------------------------------------
# Celery background task
# ---------------------------------------------------------------------------

def register_catalogue_tasks(celery_app) -> None:
    """
    Register catalogue-related Celery tasks on the given app instance.
    Called from worker/celery.py after the app is created.

    Keeping registration here (rather than in worker/celery.py) means the
    task logic lives next to the service it orchestrates, while the
    scheduling and queue config remain in the worker module.
    """

    @celery_app.task(
        name="platform.catalogue.enrich_extension_async",
        max_retries=2,
        default_retry_delay=60,
        queue="maintenance",
    )
    def enrich_extension_async(slug: str) -> None:
        """
        Background task: fetch external metadata for a single unknown extension.

        Dispatched by the extensions domain the first time it encounters an
        extension slug that is missing from (or stale in) the catalogue.
        The user gets an immediate response with whatever we have; this task
        enriches the entry for subsequent requests.
        """
        import asyncio

        async def _run():
            from core.db import _session_factory
            if not _session_factory:
                logger.warning(
                    "Catalogue task: DB not ready, cannot enrich extension"
                )
                return
            async with _session_factory() as db:
                await enrich_extension(db, slug)

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_run())
        finally:
            loop.close()

    @celery_app.task(
        name="platform.catalogue.refresh_stale_entries",
        queue="maintenance",
    )
    def refresh_stale_entries() -> None:
        """
        Beat-scheduled task: refresh all stale external-source entries.
        Runs nightly (schedule defined in worker/celery.py beat_schedule).
        """
        import asyncio

        async def _run():
            from core.db import _session_factory
            if not _session_factory:
                return
            async with _session_factory() as db:
                entries = await get_all_entries(db)
                stale = [e for e in entries if is_stale(e)]
                logger.info(
                    f"Catalogue refresh: {len(stale)} stale entries out of {len(entries)}"
                )
                for entry in stale:
                    try:
                        await enrich_extension(db, entry.slug, force_refresh=True)
                    except Exception as exc:
                        logger.warning(
                            f"Catalogue refresh failed for '{entry.slug}': {exc}"
                        )

        loop = asyncio.new_event_loop()
        try:
            loop.run_until_complete(_run())
        finally:
            loop.close()

    # Expose tasks so callers can dispatch them by name
    celery_app.enrich_extension_async = enrich_extension_async
    celery_app.refresh_stale_entries = refresh_stale_entries
