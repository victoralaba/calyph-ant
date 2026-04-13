# domains/billing/flutterwave.py
"""
Billing domain.

Flutterwave v3 payment integration, subscription lifecycle,
tier definitions, limits, and feature flag enforcement.

Feature flags live here because they exist solely to gate tier features.
They use Redis for fast evaluation — no extra service needed.

Currency support: NGN and USD.
Users choose currency at checkout. Prices are defined per-currency.
Flutterwave handles the actual charge in the chosen currency natively.

Router endpoints:
  GET    /billing/plans                    — list plans with prices in NGN + USD
  GET    /billing/me                       — current subscription + billing currency
  POST   /billing/checkout                 — initiate payment (choose currency)
  POST   /billing/webhook                  — Flutterwave webhook receiver
  GET    /billing/invoices                 — payment history with currency per invoice
  GET    /billing/limits                   — current usage vs limits
"""

from __future__ import annotations

import hashlib
import hmac
import json
from datetime import datetime, timezone
from enum import Enum
from typing import Any, Literal
from uuid import UUID, uuid4

import httpx
from fastapi import APIRouter, Depends, HTTPException, Request, status
from loguru import logger
from pydantic import BaseModel, Field, field_validator
from sqlalchemy import DateTime, String, Text, Boolean, Integer
from sqlalchemy import select
from sqlalchemy.dialects.postgresql import JSONB, UUID as PG_UUID
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Mapped, mapped_column

from core.auth import CurrentUser
from core.config import settings
from core.db import get_db
from shared.types import Base


# ---------------------------------------------------------------------------
# Currency config
# ---------------------------------------------------------------------------

SupportedCurrency = Literal["NGN", "USD"]
SUPPORTED_CURRENCIES: list[str] = ["NGN", "USD"]

# Flutterwave API currency codes
FW_CURRENCY_MAP: dict[str, str] = {
    "NGN": "NGN",
    "USD": "USD",
}

# Minor unit divisors — all prices are stored in minor units
# NGN: kobo  (1 NGN = 100 kobo)
# USD: cents  (1 USD = 100 cents)
MINOR_UNIT_DIVISOR: dict[str, int] = {
    "NGN": 100,
    "USD": 100,
}

CURRENCY_SYMBOLS: dict[str, str] = {
    "NGN": "₦",
    "USD": "$",
}


def format_price(amount_minor: int, currency: str) -> str:
    """Format a minor-unit amount as a display string."""
    divisor = MINOR_UNIT_DIVISOR.get(currency, 100)
    symbol = CURRENCY_SYMBOLS.get(currency, currency)
    amount = amount_minor / divisor
    if currency == "NGN":
        return f"{symbol}{amount:,.0f}"
    return f"{symbol}{amount:.2f}"


# ---------------------------------------------------------------------------
# Tier definitions
# ---------------------------------------------------------------------------

class Tier(str, Enum):
    explorer = "explorer"
    builder = "builder"
    team = "team"
    mega_team = "mega_team"
    enterprise = "enterprise"

LEGACY_TIER_ALIASES: dict[str, str] = {
    "free": Tier.explorer,
    "pro": Tier.builder,
}


def normalize_tier(tier: str) -> str:
    return LEGACY_TIER_ALIASES.get(tier, tier)


TIER_LIMITS: dict[str, dict[str, Any]] = {
    Tier.explorer: {
        "connections": 2,
        "backups": 10,
        "backup_retention_days": 14,
        "workspace_members": 1,
        "saved_queries": 25,
        "ai_requests_per_day": 20,
        "streaming_rows": 50_000,
        "query_timeout_seconds": 60,
    },
    Tier.builder: {
        "connections": 10,
        "backups": 100,
        "backup_retention_days": 60,
        "workspace_members": 1,
        "saved_queries": 500,
        "ai_requests_per_day": 200,
        "streaming_rows": 1_000_000,
        "query_timeout_seconds": 180,
    },
    Tier.team: {
        "connections": 30,
        "backups": 300,
        "backup_retention_days": 90,
        "workspace_members": 15,
        "saved_queries": 1_500,
        "ai_requests_per_day": 500,
        "streaming_rows": 5_000_000,
        "query_timeout_seconds": 300,
    },
    Tier.mega_team: {
        "connections": 100,
        "backups": 1_500,
        "backup_retention_days": 180,
        "workspace_members": 50,
        "saved_queries": 5_000,
        "ai_requests_per_day": 1_500,
        "streaming_rows": 20_000_000,
        "query_timeout_seconds": 600,
    },
    Tier.enterprise: {
        "connections": 200,
        "backups": 2_000,
        "backup_retention_days": 365,
        "workspace_members": 200,
        "saved_queries": 20_000,
        "ai_requests_per_day": 10_000,
        "streaming_rows": 100_000_000,
        "query_timeout_seconds": 1_200,
        "contact_sales": True,
    },
}

# All prices in minor units (kobo for NGN, cents for USD)
# 0 = not available / contact sales
TIER_PRICES: dict[str, dict[str, int]] = {
    Tier.explorer: {
        "NGN": 0,
        "USD": 0,
    },
    Tier.builder: {
        "NGN": 1_700_000,    # ₦17,000 / month
        "USD": 1_200,        # $12.00  / month
    },
    Tier.team: {
        "NGN": 4_500_000,    # ₦45,000 / month
        "USD": 2_900,        # $29.00  / month
    },
    Tier.mega_team: {
        "NGN": 12_000_000,   # ₦120,000 / month
        "USD": 7_900,        # $79.00  / month
    },
    Tier.enterprise: {
        "NGN": 0,            # Custom — contact sales
        "USD": 0,
    },
}

TIER_FEATURES: dict[str, list[str]] = {
    Tier.explorer: ["basic_editor", "schema_viewer", "query_editor", "extensions", "ai_assist_lite"],
    Tier.builder: ["basic_editor", "schema_viewer", "query_editor", "ai_assist",
               "backups", "migrations", "extensions"],
    Tier.team: ["basic_editor", "schema_viewer", "query_editor", "ai_assist",
                "backups", "migrations", "extensions", "team_collab",
                "monitoring", "advanced_diff", "shared_query_history", "basic_roles_permissions",
                "workspace_subdomain"],
    Tier.mega_team: ["basic_editor", "schema_viewer", "query_editor", "ai_assist",
                     "backups", "migrations", "extensions", "team_collab", "monitoring", "advanced_diff",
                     "priority_execution", "advanced_monitoring", "role_based_access", "audit_logs",
                     "shared_ai_context", "workspace_subdomain"],
    Tier.enterprise: ["dedicated_instance", "sla", "priority_support", "custom_integrations",
                      "workspace_subdomain", "custom_domain", "advanced_compliance"],
}


def get_limits(tier: str) -> dict[str, Any]:
    t = normalize_tier(tier)
    return TIER_LIMITS.get(t, TIER_LIMITS[Tier.explorer])


def has_feature(tier: str, feature: str) -> bool:
    features = TIER_FEATURES.get(normalize_tier(tier), [])
    return "*" in features or feature in features


def check_limit(tier: str, resource: str, current_count: int) -> bool:
    limits = get_limits(tier)
    limit = limits.get(resource, 0)
    return current_count < limit


def get_price(tier: str, currency: str) -> int:
    """
    Returns price in minor units for the given tier and currency.
    Raises ValueError for unsupported currencies.
    """
    currency = currency.upper()
    if currency not in SUPPORTED_CURRENCIES:
        raise ValueError(
            f"Unsupported currency '{currency}'. Supported: {', '.join(SUPPORTED_CURRENCIES)}"
        )
    return TIER_PRICES.get(normalize_tier(tier), {}).get(currency, 0)


# ---------------------------------------------------------------------------
# Feature flags (Redis-backed, tier-aware)
# ---------------------------------------------------------------------------

async def evaluate_flag(
    flag_name: str,
    user_id: UUID,
    tier: str,
    redis=None,
) -> bool:
    if redis:
        try:
            override = await redis.get(f"flag:{flag_name}:user:{user_id}")
            if override is not None:
                return override == b"1"
        except Exception:
            pass
    return has_feature(tier, flag_name)


async def set_user_flag(
    flag_name: str,
    user_id: UUID,
    enabled: bool,
    redis=None,
    ttl_seconds: int = 86400 * 30,
) -> None:
    if redis:
        await redis.setex(
            f"flag:{flag_name}:user:{user_id}",
            ttl_seconds,
            "1" if enabled else "0",
        )


# ---------------------------------------------------------------------------
# ORM models
# ---------------------------------------------------------------------------

class Subscription(Base):
    __tablename__ = "subscriptions"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    workspace_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True, index=True)
    tier: Mapped[str] = mapped_column(String(30), nullable=False, default=Tier.explorer)
    status: Mapped[str] = mapped_column(String(30), nullable=False, default="active")

    # Currency the user is billed in — locked after first payment
    billing_currency: Mapped[str] = mapped_column(String(10), default="USD", nullable=False)

    fw_transaction_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    fw_subscription_id: Mapped[str | None] = mapped_column(String(100), nullable=True)
    fw_customer_id: Mapped[str | None] = mapped_column(String(100), nullable=True)

    current_period_start: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    current_period_end: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)
    cancelled_at: Mapped[datetime | None] = mapped_column(DateTime(timezone=True), nullable=True)

    meta: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


class PaymentRecord(Base):
    __tablename__ = "payment_records"

    id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), primary_key=True, default=uuid4)
    user_id: Mapped[UUID] = mapped_column(PG_UUID(as_uuid=True), nullable=False, index=True)
    workspace_id: Mapped[UUID | None] = mapped_column(PG_UUID(as_uuid=True), nullable=True, index=True)
    fw_transaction_id: Mapped[str] = mapped_column(String(100), nullable=False, unique=True)
    # Stored in minor units (kobo / cents)
    amount: Mapped[int] = mapped_column(Integer, nullable=False)
    currency: Mapped[str] = mapped_column(String(10), default="USD", nullable=False)
    status: Mapped[str] = mapped_column(String(30), nullable=False)
    tier: Mapped[str] = mapped_column(String(30), nullable=False)
    meta: Mapped[dict] = mapped_column(JSONB, default=dict, nullable=False)
    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
        default=lambda: datetime.now(timezone.utc),
    )


# ---------------------------------------------------------------------------
# Flutterwave v3 client
# ---------------------------------------------------------------------------

FW_BASE = "https://api.flutterwave.com/v3"


async def _fw_request(method: str, path: str, **kwargs) -> dict:
    async with httpx.AsyncClient(
        base_url=FW_BASE,
        headers={
            "Authorization": f"Bearer {settings.FLUTTERWAVE_SECRET_KEY}",
            "Content-Type": "application/json",
        },
        timeout=30.0,
    ) as client:
        response = await getattr(client, method)(path, **kwargs)
        response.raise_for_status()
        return response.json()


async def initiate_payment(
    user_id: UUID,
    user_email: str,
    tier: str,
    currency: str,
    redirect_url: str,
    workspace_id: UUID | None = None,
) -> dict[str, Any]:
    """
    Create a Flutterwave payment link for a tier upgrade.

    currency: "NGN" or "USD" — user's explicit choice.
    Prices are resolved server-side from TIER_PRICES.
    Flutterwave receives full units (not minor units) in its payload.
    """
    currency = currency.upper()
    tier = normalize_tier(tier)
    amount_minor = get_price(tier, currency)

    if amount_minor == 0:
        raise ValueError(
            f"'{tier}' tier has no {currency} price. "
            "Enterprise requires contacting sales."
        )

    # FW expects full units (NGN not kobo, USD not cents)
    amount_full = amount_minor / MINOR_UNIT_DIVISOR[currency]
    tx_ref = f"caly_{user_id}_{uuid4().hex[:8]}"

    payload = {
        "tx_ref": tx_ref,
        "amount": amount_full,
        "currency": FW_CURRENCY_MAP[currency],
        "redirect_url": redirect_url,
        "customer": {
            "email": user_email,
        },
        "customizations": {
            "title": "Calyphant",
            "description": f"Upgrade to {tier.capitalize()} plan",
            "logo": settings.APP_LOGO_URL,
        },
        "meta": {
            "user_id": str(user_id),
            "workspace_id": str(workspace_id) if workspace_id else None,
            "tier": tier,
            "currency": currency,
        },
    }

    data = await _fw_request("post", "/payments", json=payload)
    return {
        "payment_link": data["data"]["link"],
        "tx_ref": tx_ref,
        "amount": amount_full,
        "amount_display": format_price(amount_minor, currency),
        "currency": currency,
    }


async def verify_transaction(transaction_id: str) -> dict[str, Any]:
    data = await _fw_request("get", f"/transactions/{transaction_id}/verify")
    return data["data"]


def verify_webhook_signature(payload: bytes, signature: str) -> bool:
    expected = hmac.new(
        settings.FLUTTERWAVE_WEBHOOK_SECRET.encode(),
        payload,
        hashlib.sha256,
    ).hexdigest()
    return hmac.compare_digest(expected, signature)


async def handle_webhook_event(db: AsyncSession, event: dict[str, Any]) -> None:
    event_type = event.get("event")

    if event_type == "charge.completed":
        data = event.get("data", {})
        if data.get("status") != "successful":
            return

        meta = data.get("meta", {})
        user_id_str = meta.get("user_id")
        workspace_id_str = meta.get("workspace_id")
        tier = meta.get("tier")
        currency = meta.get("currency", "USD").upper()

        if not user_id_str or not tier:
            logger.warning(f"Webhook missing user_id or tier: {meta}")
            return

        user_id = UUID(user_id_str)
        workspace_id = UUID(workspace_id_str) if workspace_id_str else None
        tier = normalize_tier(tier)
        tx_id = str(data.get("id"))

        # FW sends full units — convert back to minor units for storage
        amount_full = float(data.get("amount", 0))
        amount_minor = int(amount_full * MINOR_UNIT_DIVISOR.get(currency, 100))

        payment = PaymentRecord(
            user_id=user_id,
            workspace_id=workspace_id,
            fw_transaction_id=tx_id,
            amount=amount_minor,
            currency=currency,
            status="successful",
            tier=tier,
            meta=data,
        )
        db.add(payment)

        result = await db.execute(
            select(Subscription).where(
                Subscription.user_id == user_id,
                Subscription.workspace_id == workspace_id,
            )
        )
        sub = result.scalar_one_or_none()
        if sub:
            sub.tier = tier
            sub.status = "active"
            sub.fw_transaction_id = tx_id
            sub.billing_currency = currency
        else:
            sub = Subscription(
                user_id=user_id,
                workspace_id=workspace_id,
                tier=tier,
                status="active",
                fw_transaction_id=tx_id,
                billing_currency=currency,
            )
            db.add(sub)

        from domains.users.service import User
        from domains.teams.service import Workspace
        user = await db.get(User, user_id)
        if user:
            user.tier = tier
        if workspace_id:
            workspace = await db.get(Workspace, workspace_id)
            if workspace:
                workspace.billing_tier = tier
                if tier in (Tier.team, Tier.mega_team, Tier.enterprise) and not workspace.subdomain:
                    base = (workspace.slug.split("-")[0] or "workspace")[:63]
                    candidate = base
                    n = 1
                    while True:
                        existing = await db.execute(
                            select(Workspace.id).where(
                                Workspace.subdomain == candidate,
                                Workspace.id != workspace.id,
                            ).limit(1)
                        )
                        if existing.scalar_one_or_none() is None:
                            workspace.subdomain = candidate
                            break
                        n += 1
                        candidate = f"{base}-{n}"

        await db.commit()
        logger.info(
            f"Payment confirmed: user={user_id} tier={tier} "
            f"{format_price(amount_minor, currency)}"
        )

        # Dispatch through the notifications engine so policy + preferences
        # stay centralized across domains.
        try:
            from domains.notifications.service import dispatch_event
            from domains.notifications.templates import build_billing_upgraded_email

            # Retrieve user name for the email (already fetched above as `user`)
            display_name = ""
            if user and user.full_name:
                display_name = user.full_name
            elif user:
                display_name = user.email

            email_subject, email_html = build_billing_upgraded_email(
                name=display_name,
                new_tier=tier,
                amount_display=format_price(amount_minor, currency),
                currency=currency,
            )
            if user:
                await dispatch_event(
                    db=db,
                    user_id=user.id,
                    kind="billing_plan_upgraded",
                    title=f"Your plan is now {tier.title()}",
                    body=(
                        f"Payment confirmed: {format_price(amount_minor, currency)}. "
                        "Your new plan is active."
                    ),
                    meta={"tier": tier, "currency": currency, "amount_minor": amount_minor},
                    email_subject=email_subject,
                    email_html=email_html,
                    _user_email=user.email,
                    _user_name=display_name,
                )
        except Exception as notif_exc:
            # Notification failure must NEVER roll back a successful payment
            logger.warning(f"billing_plan_upgraded notification failed (non-fatal): {notif_exc}")

    elif event_type == "subscription.cancelled":
        data = event.get("data", {})
        meta = data.get("meta", {})
        user_id_str = meta.get("user_id")
        if user_id_str:
            user_id = UUID(user_id_str)
            result = await db.execute(
                select(Subscription).where(Subscription.user_id == user_id)
            )
            sub = result.scalar_one_or_none()
            if sub:
                sub.status = "cancelled"
                sub.cancelled_at = datetime.now(timezone.utc)
                sub.tier = Tier.explorer

                from domains.users.service import User
                user = await db.get(User, user_id)
                if user:
                    user.tier = Tier.explorer
                if sub.workspace_id:
                    from domains.teams.service import Workspace

                    ws = await db.get(Workspace, sub.workspace_id)
                    if ws:
                        ws.billing_tier = Tier.explorer

                await db.commit()
                logger.info(f"Subscription cancelled: user={user_id}")


# ---------------------------------------------------------------------------
# Router
# ---------------------------------------------------------------------------

router = APIRouter(prefix="/billing", tags=["billing"])


class CheckoutRequest(BaseModel):
    tier: str = Field(..., description="Target tier: builder | team | mega_team")
    currency: str = Field("USD", description="Payment currency: NGN or USD")
    redirect_url: str = Field(..., description="URL to redirect after payment")
    workspace_id: UUID | None = Field(
        default=None,
        description="Workspace receiving the subscription upgrade.",
    )

    @field_validator("currency")
    @classmethod
    def currency_must_be_supported(cls, v: str) -> str:
        v = v.upper()
        if v not in SUPPORTED_CURRENCIES:
            raise ValueError(
                f"Currency must be one of: {', '.join(SUPPORTED_CURRENCIES)}"
            )
        return v

    @field_validator("tier")
    @classmethod
    def tier_must_be_known(cls, v: str) -> str:
        normalized = normalize_tier(v)
        if normalized not in TIER_LIMITS:
            raise ValueError("Unknown tier.")
        return normalized


@router.get("/contact-sales")
async def contact_sales_info():
    return {
        "tier": Tier.enterprise,
        "checkout_enabled": False,
        "message": "Enterprise is customized. Contact sales to negotiate limits and pricing.",
        "email": "sales@calyphant.com",
    }


@router.get("/plans")
async def list_plans():
    """
    List all plans with prices in both NGN and USD.
    Frontend uses this to render the pricing table with a currency toggle.
    """
    return {
        "supported_currencies": SUPPORTED_CURRENCIES,
        "plans": [
            {
                "tier": tier,
                "prices": {
                    currency: {
                        "amount_minor": TIER_PRICES.get(tier, {}).get(currency, 0),
                        "display": format_price(
                            TIER_PRICES.get(tier, {}).get(currency, 0), currency
                        ),
                        "currency": currency,
                    }
                    for currency in SUPPORTED_CURRENCIES
                },
                "limits": TIER_LIMITS.get(tier, {}),
                "features": TIER_FEATURES.get(tier, []),
            }
            for tier in [Tier.explorer, Tier.builder, Tier.team, Tier.mega_team, Tier.enterprise]
        ],
    }


@router.get("/me")
async def get_subscription(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(Subscription)
        .where(Subscription.user_id == user.id, Subscription.workspace_id == user.workspace_id)
        .order_by(Subscription.created_at.desc())
        .limit(1)
    )
    sub = result.scalar_one_or_none()
    if not sub:
        return {
            "tier": Tier.explorer,
            "workspace_id": str(user.workspace_id) if user.workspace_id else None,
            "status": "active",
            "billing_currency": None,
            "subscription": None,
        }
    return {
        "tier": sub.tier,
        "workspace_id": str(sub.workspace_id) if sub.workspace_id else None,
        "status": sub.status,
        "billing_currency": sub.billing_currency,
        "current_period_end": (
            sub.current_period_end.isoformat() if sub.current_period_end else None
        ),
        "cancelled_at": sub.cancelled_at.isoformat() if sub.cancelled_at else None,
    }


@router.post("/checkout")
async def checkout(
    body: CheckoutRequest,
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    """
    Initiate a Flutterwave payment.
    User explicitly chooses NGN or USD — price is resolved server-side.
    """
    if body.tier not in (Tier.builder, Tier.team, Tier.mega_team):
        raise HTTPException(status_code=400, detail="Invalid tier.")
    workspace_id = body.workspace_id or user.workspace_id
    if body.tier in (Tier.team, Tier.mega_team) and not workspace_id:
        raise HTTPException(status_code=400, detail="Workspace ID is required for team plans.")
    if body.tier in (Tier.team, Tier.mega_team):
        from domains.teams.service import WorkspaceMember, WorkspaceRole

        membership = await db.execute(
            select(WorkspaceMember.role).where(
                WorkspaceMember.workspace_id == workspace_id,
                WorkspaceMember.user_id == user.id,
            ).limit(1)
        )
        actor_role = membership.scalar_one_or_none()
        if actor_role not in (WorkspaceRole.owner, WorkspaceRole.admin):
            raise HTTPException(
                status_code=403,
                detail="Only workspace owners/admins can upgrade a workspace to team plans.",
            )
    try:
        return await initiate_payment(
            user_id=user.id,
            user_email=user.email,
            tier=body.tier,
            currency=body.currency,
            redirect_url=body.redirect_url,
            workspace_id=workspace_id,
        )
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc))
    except Exception as exc:
        raise HTTPException(status_code=500, detail=f"Payment initiation failed: {exc}")


@router.post("/webhook", include_in_schema=False)
async def flutterwave_webhook(
    request: Request,
    db: AsyncSession = Depends(get_db),
):
    body = await request.body()
    signature = request.headers.get("verif-hash", "")
    if not verify_webhook_signature(body, signature):
        raise HTTPException(status_code=400, detail="Invalid webhook signature.")
    try:
        await handle_webhook_event(db, json.loads(body))
    except Exception as exc:
        logger.error(f"Webhook error: {exc}")
    return {"status": "ok"}


@router.get("/limits")
async def get_usage_limits(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    from domains.users.service import get_user
    db_user = await get_user(db, user.id)
    tier = db_user.tier if db_user else Tier.explorer
    if user.workspace_id:
        from domains.teams.service import Workspace
        ws = await db.get(Workspace, user.workspace_id)
        if ws and ws.billing_tier:
            tier = ws.billing_tier
    return {
        "tier": tier,
        "limits": get_limits(tier),
        "features": TIER_FEATURES.get(tier, []),
    }


@router.get("/invoices")
async def get_invoices(
    user: CurrentUser,
    db: AsyncSession = Depends(get_db),
):
    result = await db.execute(
        select(PaymentRecord)
        .where(PaymentRecord.user_id == user.id)
        .order_by(PaymentRecord.created_at.desc())
    )
    records = result.scalars().all()
    return {
        "invoices": [
            {
                "id": str(r.id),
                "amount_minor": r.amount,
                "amount_display": format_price(r.amount, r.currency),
                "currency": r.currency,
                "status": r.status,
                "tier": r.tier,
                "created_at": r.created_at.isoformat(),
            }
            for r in records
        ]
    }
