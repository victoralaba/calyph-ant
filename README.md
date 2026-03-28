# Calyphant Backend

FastAPI backend for the Calyphant Universal PostgreSQL Workspace.

## Requirements

- Python 3.12+
- PostgreSQL (for the platform database)
- Optional: Redis (for Celery background tasks)

## Setup

```bash
cd Backend
pip install -e ".[dev]"
cp .env.example .env
# Edit .env with your PLATFORM_DATABASE_URL and APP_SECRET_KEY
```

## Running

```bash
uvicorn app.main:app --reload --host 0.0.0.0 --port 8000
```

## CLI

```bash
calyphant connect postgresql://user:pass@host/db
calyphant schema postgresql://user:pass@host/db
calyphant migrate postgresql://user:pass@host/db ./migration.sql
calyphant diff postgresql://source/db postgresql://target/db
calyphant backup postgresql://user:pass@host/db --output dump.sql
calyphant extensions postgresql://user:pass@host/db
calyphant extensions-enable postgresql://user:pass@host/db pgvector
```

## API Docs

Visit http://localhost:8000/docs for Swagger UI.

## Structure

```
Backend/
├── app/
│   ├── main.py          # FastAPI app + CORS + router mounting
│   ├── config.py        # Settings (pydantic-settings)
│   ├── database.py      # SQLAlchemy async engine + session
│   ├── models.py        # ORM models (User, SavedConnection, etc.)
│   ├── schemas.py       # Pydantic request/response schemas
│   ├── auth.py          # Argon2id + JWT utilities
│   ├── connections.py   # PostgreSQL connection logic (asyncpg)
│   ├── cli.py           # Typer CLI
│   └── routers/
│       ├── auth.py
│       ├── connections.py
│       ├── extensions.py
│       ├── migrations.py
│       ├── schema.py
│       ├── storage.py
│       ├── vectors.py
│       └── admin.py
├── alembic/             # Database migrations
├── pyproject.toml
├── alembic.ini
└── .env.example
```
