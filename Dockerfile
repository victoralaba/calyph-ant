# syntax=docker/dockerfile:1
FROM python:3.12-slim

# 1. System dependencies
# We keep these in a single layer to minimize image size.
RUN apt-get update && apt-get install -y --no-install-recommends \
    postgresql-client \
    libpq-dev \
    gcc \
    libc-dev \
    python3-dev \
    curl \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# 2. Dependency Management with BuildKit Caching
# We mount the pip cache directory. If pyproject.toml hasn't changed, 
# this entire block is skipped. If it HAS changed, pip only downloads 
# the new or updated packages.
COPY pyproject.toml .
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install --upgrade pip && \
    pip install -e ".[dev]"

# 3. Architecture Specifics
# Explicitly installing migra and psycopg2 to handle potential 
# ARM64/x86 build variations.
RUN --mount=type=cache,target=/root/.cache/pip \
    pip install migra psycopg2

# 4. Source Code Injection
# We copy the rest of the application. This is placed AFTER the heavy 
# dependency installs so that code changes don't trigger a re-install.
COPY . .

# 5. Security: Non-root execution
# Running as a superuser is a liability. We map to your local UID 1000.
RUN useradd -m -u 1000 calyphant && chown -R calyphant:calyphant /app
USER calyphant

EXPOSE 8000

# 6. Execution Engine
# Defaulting to the API server. Workers and Beat will override 
# this via docker-compose.yml commands.
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000", "--workers", "2"]