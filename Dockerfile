# Dockerfile
FROM python:3.12-slim

# System deps: pg_dump for backups, migra for schema diff, build tools
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    libpq-dev \
    gcc \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python deps first (layer cache — only rebuilt on pyproject.toml changes)
COPY pyproject.toml .
RUN pip install --no-cache-dir --upgrade pip \
    && pip install --no-cache-dir -e ".[dev]"

# Install migra separately (needs psycopg2 which needs libpq-dev)
RUN pip install --no-cache-dir migra psycopg2-binary

# Copy source
COPY . .

# Non-root user for security
RUN useradd -m -u 1000 calyphant && chown -R calyphant:calyphant /app
USER calyphant

EXPOSE 8000

# Default: API server
# Override with `command:` in docker-compose for worker/beat
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]
