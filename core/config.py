# core/config.py
"""
Application configuration.

All  environment variables are declared here via pydantic-settings.
Nothing reads os.environ directly anywhere else in the codebase.

POOLING_ENABLED=false in dev — each request gets a direct asyncpg connection.
POOLING_ENABLED=true in prod — asyncpg pool is shared across requests.

Redis TLS requirement
---------------------
In production (APP_ENV=production) the REDIS_URL must use the rediss://
scheme (TLS). This is enforced by a validator. Schema snapshots are
encrypted with Fernet before being written to Redis, but TLS provides an
additional layer of protection for data in transit and prevents other
values (rate limit counters, password reset tokens) from being intercepted.

Load order:
  1. .env file (if present)
  2. Environment variables (override .env)
  3. Defaults defined here
"""

from __future__ import annotations

from functools import lru_cache
from typing import Literal

from pydantic import EmailStr, Field, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        case_sensitive=False,
        extra="ignore",
    )

    # ------------------------------------------------------------------
    # App
    # ------------------------------------------------------------------
    APP_NAME: str = "Calyphant"
    APP_ENV: Literal["development", "staging", "production"] = "development"
    APP_BASE_URL: str = "http://localhost:3000"
    APP_LOGO_URL: str = "https://calyphant.com/logo.png"
    DEBUG: bool = False
    SECRET_KEY: str = Field(..., description="Random secret for signing tokens. Min 32 chars.")

    @field_validator("SECRET_KEY")
    @classmethod
    def secret_key_must_be_long(cls, v: str) -> str:
        if len(v) < 32:
            raise ValueError("SECRET_KEY must be at least 32 characters.")
        return v

    # ------------------------------------------------------------------
    # Database (Calyphant's own Postgres — NOT the user's target DB)
    # ------------------------------------------------------------------
    DATABASE_URL: str = Field(
        ...,
        description="PostgreSQL URL for Calyphant's own database.",
    )

    POOLING_ENABLED: bool = Field(
        default=False,
        description="Enable asyncpg connection pooling. Set True in production.",
    )
    POOL_MIN_SIZE: int = 5
    POOL_MAX_SIZE: int = 20
    POOL_MAX_INACTIVE_CONNECTION_LIFETIME: float = 300.0

    SYNC_DATABASE_URL: str | None = None

    def get_sync_url(self) -> str:
        if self.SYNC_DATABASE_URL:
            return self.SYNC_DATABASE_URL
        url = self.DATABASE_URL
        url = url.replace("postgresql+asyncpg://", "postgresql+psycopg://")
        url = url.replace("postgres://", "postgresql+psycopg://")
        if not url.startswith("postgresql+"):
            url = url.replace("postgresql://", "postgresql+psycopg://")
        return url

    # ------------------------------------------------------------------
    # Redis
    # In production REDIS_URL must use rediss:// (TLS). This is validated
    # in the model_validator below. Schema snapshots written to Redis are
    # Fernet-encrypted, but TLS protects all Redis traffic including rate
    # limit counters and password reset tokens.
    # ------------------------------------------------------------------
    REDIS_URL: str = "redis://localhost:6379/0"
    REDIS_MAX_CONNECTIONS: int = 20

    # ------------------------------------------------------------------
    # Auth
    # ------------------------------------------------------------------
    JWT_ALGORITHM: str = "HS256"
    JWT_ACCESS_TOKEN_EXPIRE_MINUTES: int = 60
    JWT_REFRESH_TOKEN_EXPIRE_DAYS: int = 30
    ARGON2_TIME_COST: int = 2
    ARGON2_MEMORY_COST: int = 65536
    ARGON2_PARALLELISM: int = 2

# ------------------------------------------------------------------
    # Encryption (Multi-Key Rotation Strategy for URLs and Snapshots)
    # ------------------------------------------------------------------
    ENCRYPTION_KEYS: str = Field(
        ...,
        description=(
            "Comma-separated list of Fernet keys (base64, 32 bytes each). "
            "The first key is Primary (used for all new encryption). "
            "All subsequent keys are Secondary (used as fallbacks for decryption). "
            "Generate keys with: python -c \"from cryptography.fernet import Fernet; print(Fernet.generate_key().decode())\""
        ),
    )

    @field_validator("ENCRYPTION_KEYS", mode="after")
    @classmethod
    def validate_encryption_keys(cls, v: str) -> list[str]:
        keys = [k.strip() for k in v.split(",") if k.strip()]
        if not keys:
            raise ValueError("At least one ENCRYPTION_KEY must be provided.")
        for key in keys:
            if len(key) < 32:
                raise ValueError("Each Fernet key must be a valid 32-byte base64 string.")
        return keys


    # ------------------------------------------------------------------
    # Cloudflare R2
    # ------------------------------------------------------------------
    R2_ENDPOINT_URL: str = ""
    R2_ACCESS_KEY_ID: str = ""
    R2_SECRET_ACCESS_KEY: str = ""
    R2_BUCKET_NAME: str = "calyphant-backups"

    # ------------------------------------------------------------------
    # AI (Consolidated Google Gemini Architecture)
    # ------------------------------------------------------------------
    AI_AVAILABLE: bool = True

    # Google AI Configuration
    GOOGLE_GENAI_API_KEY: str = ""
    GOOGLE_GENAI_MODEL: str = "gemini-2.5-flash"  # Configurable in .env for easy upgrades
    GOOGLE_GENAI_MODEL_THINKING: str = "gemini-2.5-flash-thinking"
    
    # AI Rate Limiting & Billing Weights
    RATE_LIMIT_AI: str = "10/minute" # For core.middleware.limiter
    QUOTA_COST_STANDARD: int = 1
    QUOTA_COST_THINKING: int = 3  # Burns 3x quota when UI requests deep reasoning

    # ------------------------------------------------------------------
    # Flutterwave
    # ------------------------------------------------------------------
    FLUTTERWAVE_SECRET_KEY: str = ""
    FLUTTERWAVE_PUBLIC_KEY: str = ""
    FLUTTERWAVE_WEBHOOK_SECRET: str = ""

    # ------------------------------------------------------------------
    # SendPulse (Transactional Email Service)
    # ------------------------------------------------------------------
    # ── SendPulse (Transactional Email Service) ──────────────────────────────
    SENDPULSE_CLIENT_ID: str
    SENDPULSE_CLIENT_SECRET: str
    
    # University Workspace address (Activation)
    SENDPULSE_ADMIN_EMAIL: EmailStr
    
    # SendPulse Single Sender
    SENDPULSE_SENDER_EMAIL: EmailStr
    SENDPULSE_SENDER_NAME: str = "Victor Pamilerin"
    
    # SendPulse System Identity
    SENDPULSE_SYSTEM_EMAIL: EmailStr
    SENDPULSE_SYSTEM_NAME: str = "Calyphant Team"

    # ------------------------------------------------------------------
    # PostHog
    # ------------------------------------------------------------------
    POSTHOG_API_KEY: str = ""
    POSTHOG_HOST: str = "https://app.posthog.com"
    POSTHOG_ENABLED: bool = True

    # ------------------------------------------------------------------
    # Sentry
    # ------------------------------------------------------------------
    SENTRY_DSN: str = ""
    SENTRY_TRACES_SAMPLE_RATE: float = 0.1
    SENTRY_PROFILES_SAMPLE_RATE: float = 0.1

    # ------------------------------------------------------------------
    # Rate limiting
    # ------------------------------------------------------------------
    RATE_LIMIT_ENABLED: bool = True
    RATE_LIMIT_DEFAULT: str = "100/minute"
    RATE_LIMIT_AUTH: str = "10/minute"
    # Note: RATE_LIMIT_AI is now defined in the AI section above.
    
    # ------------------------------------------------------------------
    # CORS
    # ------------------------------------------------------------------
    CORS_ORIGINS: list[str] = [
        "http://localhost:3000",
        "http://localhost:5173",
        "http://localhost:4173",
    ]
    CORS_ALLOW_CREDENTIALS: bool = True

    # ------------------------------------------------------------------
    # Celery
    # ------------------------------------------------------------------
    CELERY_BROKER_URL: str = "redis://localhost:6379/1"
    CELERY_RESULT_BACKEND: str = "redis://localhost:6379/2"

    # ------------------------------------------------------------------
    # Cross-field validators
    # ------------------------------------------------------------------

    @model_validator(mode="after")
    def enforce_production_tls(self) -> "Settings":
        """
        In production, all Redis URLs (REDIS_URL, CELERY_BROKER_URL,
        CELERY_RESULT_BACKEND) must use the TLS scheme (rediss://).

        Schema snapshots stored in Redis are Fernet-encrypted, but TLS
        is still required because other Redis values — rate limit counters,
        password reset tokens, feature flag overrides — are not encrypted
        and must not be transmitted in plaintext over the network.
        """
        if self.APP_ENV != "production":
            return self

        for attr, label in [
            ("REDIS_URL", "REDIS_URL"),
            ("CELERY_BROKER_URL", "CELERY_BROKER_URL"),
            ("CELERY_RESULT_BACKEND", "CELERY_RESULT_BACKEND"),
        ]:
            url: str = getattr(self, attr)
            if url and not url.startswith("rediss://"):
                raise ValueError(
                    f"{label} must use rediss:// (TLS) in production. "
                    f"Got: {url[:30]}... — "
                    "Set it to rediss://user:pass@host:6380/N or use "
                    "SKIP_REDIS_TLS_CHECK=true only for local integration tests."
                )

        return self

    # ------------------------------------------------------------------
    # Derived properties
    # ------------------------------------------------------------------

    @property
    def is_production(self) -> bool:
        return self.APP_ENV == "production"

    @property
    def is_development(self) -> bool:
        return self.APP_ENV == "development"

    @property
    def sentry_enabled(self) -> bool:
        return bool(self.SENTRY_DSN)

    @property
    def r2_enabled(self) -> bool:
        return bool(self.R2_ENDPOINT_URL and self.R2_ACCESS_KEY_ID)


@lru_cache
def get_settings() -> Settings:
    """
    Cached settings — .env is read once per process.
    Always use: from core.config import settings
    """
    return Settings() #type: ignore


settings: Settings = get_settings()
