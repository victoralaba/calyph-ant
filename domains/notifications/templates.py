# domains/notifications/templates.py
"""
Email HTML template builder.

Single source of truth for all transactional email HTML.
No external templating engine — pure Python f-strings with a shared base layout.

Every public function returns a tuple: (subject: str, html: str)
so callers never have to construct either independently.

Sender routing:
  "ceo"    — personal tone, signed by Victor Pamilerin, used for welcome/billing
  "system" — operational tone, signed as Calyphant Team, used for security/team/backup

UI NOTE: All URLs use settings.APP_BASE_URL so they resolve correctly across
environments. Never hardcode https://calyphant.com in template bodies.
"""

from __future__ import annotations

from core.config import settings


# ---------------------------------------------------------------------------
# Shared base layout
# ---------------------------------------------------------------------------

def _base(title: str, preview_text: str, body_html: str) -> str:
    """
    Wraps content in a minimal, dark-mode-safe HTML email shell.
    Inline styles only — email clients strip <style> blocks.
    """
    return f"""<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta name="color-scheme" content="light dark">
  <title>{title}</title>
  <!--[if mso]><noscript><xml><o:OfficeDocumentSettings><o:PixelsPerInch>96</o:PixelsPerInch></o:OfficeDocumentSettings></xml></noscript><![endif]-->
</head>
<body style="margin:0;padding:0;background-color:#0f172a;font-family:-apple-system,BlinkMacSystemFont,'Segoe UI',Roboto,Oxygen,Ubuntu,sans-serif;">
  <!-- Preview text (hidden, shows in inbox) -->
  <span style="display:none;max-height:0;overflow:hidden;">{preview_text}</span>

  <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="background-color:#0f172a;">
    <tr>
      <td align="center" style="padding:40px 16px;">
        <table role="presentation" width="100%" cellpadding="0" cellspacing="0" style="max-width:560px;">

          <!-- Logo / Brand -->
          <tr>
            <td align="center" style="padding-bottom:32px;">
              <span style="font-size:22px;font-weight:700;color:#e2e8f0;letter-spacing:-0.5px;">
                ⚡ Calyphant
              </span>
            </td>
          </tr>

          <!-- Card -->
          <tr>
            <td style="background-color:#1e293b;border-radius:12px;border:1px solid #334155;padding:40px 36px;">
              {body_html}
            </td>
          </tr>

          <!-- Footer -->
          <tr>
            <td align="center" style="padding-top:28px;">
              <p style="margin:0;font-size:12px;color:#475569;line-height:1.6;">
                Calyphant · Universal PostgreSQL workspace<br>
                <a href="{settings.APP_BASE_URL}" style="color:#6366f1;text-decoration:none;">calyphant.com</a>
                &nbsp;·&nbsp;
                <a href="{settings.APP_BASE_URL}/unsubscribe" style="color:#475569;text-decoration:none;">Unsubscribe</a>
              </p>
            </td>
          </tr>

        </table>
      </td>
    </tr>
  </table>
</body>
</html>"""


def _cta_button(text: str, url: str) -> str:
    return f"""<table role="presentation" cellpadding="0" cellspacing="0" style="margin:28px 0;">
  <tr>
    <td style="background-color:#6366f1;border-radius:8px;">
      <a href="{url}" style="display:inline-block;padding:14px 28px;font-size:15px;font-weight:600;color:#ffffff;text-decoration:none;letter-spacing:0.2px;">{text}</a>
    </td>
  </tr>
</table>"""


def _h1(text: str) -> str:
    return f'<h1 style="margin:0 0 16px 0;font-size:24px;font-weight:700;color:#f1f5f9;line-height:1.3;">{text}</h1>'


def _p(text: str) -> str:
    return f'<p style="margin:0 0 16px 0;font-size:15px;color:#94a3b8;line-height:1.7;">{text}</p>'


def _code_block(text: str) -> str:
    return f'<div style="background-color:#0f172a;border:1px solid #334155;border-radius:8px;padding:16px;margin:20px 0;font-family:monospace;font-size:14px;color:#a5f3fc;word-break:break-all;">{text}</div>'


def _divider() -> str:
    return '<hr style="border:none;border-top:1px solid #334155;margin:24px 0;">'


def _muted(text: str) -> str:
    return f'<p style="margin:0 0 12px 0;font-size:13px;color:#64748b;line-height:1.6;">{text}</p>'


# ---------------------------------------------------------------------------
# Welcome email  (sender: "ceo")
# ---------------------------------------------------------------------------

def build_welcome_email(name: str) -> tuple[str, str]:
    """
    Sent immediately after registration.
    name: user's full_name or email prefix if name is None.
    Returns (subject, html).
    """
    display = name or "there"
    subject = "Welcome to Calyphant — your PostgreSQL workspace is ready"

    body = (
        _h1(f"Welcome aboard, {display} 👋")
        + _p(
            "Calyphant gives you a unified workspace to connect, inspect, query, and "
            "migrate any PostgreSQL database — from your laptop to Neon, Supabase, RDS, and beyond."
        )
        + _cta_button("Open your workspace", f"{settings.APP_BASE_URL}/dashboard")
        + _divider()
        + _p("Here's what you can do right now:")
        + """<ul style="margin:0 0 16px 0;padding-left:20px;color:#94a3b8;font-size:15px;line-height:1.9;">
  <li>Connect a PostgreSQL database (cloud or local)</li>
  <li>Browse tables and run SQL in the visual editor</li>
  <li>Create and apply schema migrations with one click</li>
  <li>Back up and restore your data at any time</li>
</ul>"""
        + _divider()
        + _muted(
            "If you didn't create this account, you can safely ignore this email. "
            "No action is needed."
        )
    )

    return subject, _base(subject, f"Your PostgreSQL workspace is ready, {display}.", body)


# ---------------------------------------------------------------------------
# Password reset email  (sender: "system")
# ---------------------------------------------------------------------------

def build_password_reset_email(token: str) -> tuple[str, str]:
    """
    Sent when a user requests a password reset.
    token: the raw URL-safe token (not hashed).
    Returns (subject, html).

    UI NOTE: The reset link expires in 1 hour (matches Redis TTL in core/auth.py).
    Show that expiry prominently so users don't waste time on expired links.
    """
    reset_url = f"{settings.APP_BASE_URL}/auth/reset-password?token={token}"
    subject = "Reset your Calyphant password"

    body = (
        _h1("Reset your password")
        + _p(
            "We received a request to reset the password for your Calyphant account. "
            "Click the button below to choose a new password."
        )
        + _cta_button("Reset password", reset_url)
        + _divider()
        + _p("Or paste this link into your browser:")
        + _code_block(reset_url)
        + _divider()
        + _muted("This link expires in <strong style='color:#94a3b8;'>1 hour</strong>.")
        + _muted(
            "If you didn't request a password reset, you can safely ignore this email. "
            "Your password will not change."
        )
    )

    return subject, _base(subject, "Reset your Calyphant password — link expires in 1 hour.", body)


# ---------------------------------------------------------------------------
# Workspace invite email  (sender: "system")
# ---------------------------------------------------------------------------

def build_invite_email(
    inviter_name: str,
    workspace_name: str,
    invite_token: str,
    role: str = "viewer",
) -> tuple[str, str]:
    """
    Sent when a workspace admin invites a new member.
    invite_token: the raw token from WorkspaceInvite.token.
    Returns (subject, html).

    UI NOTE: Invite links expire in 7 days (see domains/teams/service.py create_invite).
    """
    accept_url = f"{settings.APP_BASE_URL}/invites/{invite_token}/accept"
    subject = f"{inviter_name} invited you to join {workspace_name} on Calyphant"

    body = (
        _h1("You've been invited")
        + _p(
            f"<strong style='color:#f1f5f9;'>{inviter_name}</strong> has invited you to "
            f"collaborate on the <strong style='color:#f1f5f9;'>{workspace_name}</strong> "
            f"workspace as a <strong style='color:#f1f5f9;'>{role}</strong>."
        )
        + _cta_button("Accept invitation", accept_url)
        + _divider()
        + _muted("This invitation expires in 7 days.")
        + _muted(
            "If you weren't expecting this invite, you can safely ignore this email. "
            "The invitation will expire automatically."
        )
    )

    return subject, _base(subject, f"Join {workspace_name} on Calyphant.", body)


# ---------------------------------------------------------------------------
# Migration failed email  (sender: "system")
# ---------------------------------------------------------------------------

def build_migration_failed_email(
    workspace_name: str,
    migration_label: str,
    migration_version: str,
    error_detail: str,
) -> tuple[str, str]:
    """
    Sent when a migration apply or rollback fails.
    Returns (subject, html).
    """
    subject = f"Migration failed: {migration_label}"

    body = (
        _h1("A migration failed ⚠️")
        + _p(
            f"Migration <strong style='color:#f1f5f9;'>{migration_label}</strong> "
            f"(version <code style='color:#a5f3fc;font-size:13px;'>{migration_version}</code>) "
            f"failed to apply on workspace <strong style='color:#f1f5f9;'>{workspace_name}</strong>."
        )
        + _divider()
        + '<p style="margin:0 0 8px 0;font-size:13px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:0.5px;">Error detail</p>'
        + _code_block(error_detail[:1000] if error_detail else "No additional detail available.")
        + _divider()
        + _cta_button("View migrations", f"{settings.APP_BASE_URL}/dashboard")
        + _muted("The migration has been marked as failed. No schema changes were applied.")
    )

    return subject, _base(subject, f"Migration {migration_label} failed to apply.", body)


# ---------------------------------------------------------------------------
# Backup completed email  (sender: "system")
# ---------------------------------------------------------------------------

def build_backup_completed_email(
    workspace_name: str,
    connection_name: str,
    backup_label: str,
    size_bytes: int | None,
) -> tuple[str, str]:
    """
    Sent when a scheduled backup completes successfully.
    Only dispatched for *scheduled* backups — manual backups use UI polling only.
    Returns (subject, html).
    """
    size_str = ""
    if size_bytes:
        size_mb = size_bytes / (1024 * 1024)
        size_str = f" ({size_mb:.1f} MB)"

    subject = f"Backup completed: {backup_label}"

    body = (
        _h1("Backup completed ✅")
        + _p(
            f"A backup of <strong style='color:#f1f5f9;'>{connection_name}</strong> "
            f"on workspace <strong style='color:#f1f5f9;'>{workspace_name}</strong> "
            f"completed successfully{size_str}."
        )
        + _p(f"Label: <code style='color:#a5f3fc;font-size:13px;'>{backup_label}</code>")
        + _cta_button("View backups", f"{settings.APP_BASE_URL}/dashboard")
    )

    return subject, _base(subject, f"Backup {backup_label} completed successfully.", body)


# ---------------------------------------------------------------------------
# Backup failed email  (sender: "system")
# ---------------------------------------------------------------------------

def build_backup_failed_email(
    workspace_name: str,
    connection_name: str,
    backup_label: str,
    error_detail: str,
) -> tuple[str, str]:
    """
    Sent when a backup (scheduled or manual) fails.
    Returns (subject, html).
    """
    subject = f"Backup failed: {backup_label}"

    body = (
        _h1("Backup failed ⚠️")
        + _p(
            f"A backup of <strong style='color:#f1f5f9;'>{connection_name}</strong> "
            f"on workspace <strong style='color:#f1f5f9;'>{workspace_name}</strong> failed."
        )
        + _divider()
        + '<p style="margin:0 0 8px 0;font-size:13px;font-weight:600;color:#94a3b8;text-transform:uppercase;letter-spacing:0.5px;">Error detail</p>'
        + _code_block(error_detail[:1000] if error_detail else "No additional detail available.")
        + _divider()
        + _cta_button("View backups", f"{settings.APP_BASE_URL}/dashboard")
    )

    return subject, _base(subject, f"Backup {backup_label} failed.", body)


# ---------------------------------------------------------------------------
# Billing plan upgraded email  (sender: "ceo")
# ---------------------------------------------------------------------------

def build_billing_upgraded_email(
    name: str,
    new_tier: str,
    amount_display: str,
    currency: str,
) -> tuple[str, str]:
    """
    Sent when a user successfully upgrades their subscription.
    Returns (subject, html).
    """
    display = name or "there"
    tier_label = new_tier.capitalize()
    subject = f"You're now on the {tier_label} plan — welcome!"

    body = (
        _h1(f"You're on {tier_label} now, {display} 🎉")
        + _p(
            f"Your payment of <strong style='color:#f1f5f9;'>{amount_display}</strong> "
            f"({currency}) was received and your workspace has been upgraded to the "
            f"<strong style='color:#f1f5f9;'>{tier_label}</strong> plan."
        )
        + _cta_button("Explore your new limits", f"{settings.APP_BASE_URL}/dashboard")
        + _divider()
        + _muted(
            "If you have any questions about your subscription or billing, "
            f"reply to this email or visit {settings.APP_BASE_URL}/billing."
        )
    )

    return subject, _base(subject, f"Welcome to {tier_label} — your workspace is upgraded.", body)


# ---------------------------------------------------------------------------
# Security: new login detected  (sender: "system")
# ---------------------------------------------------------------------------

def build_new_login_email(
    name: str,
    ip_address: str | None,
) -> tuple[str, str]:
    """
    Sent when a login is detected from a new device/IP.
    Currently triggered manually — not auto-detected (future enhancement).
    Returns (subject, html).
    """
    display = name or "there"
    subject = "New login detected on your Calyphant account"
    ip_str = ip_address or "unknown"

    body = (
        _h1("New login detected")
        + _p(f"Hi {display}, we noticed a new sign-in to your Calyphant account.")
        + _divider()
        + f'<table style="width:100%;border-collapse:collapse;margin:0 0 20px 0;">'
        + f'<tr><td style="padding:8px 0;font-size:14px;color:#64748b;">IP address</td><td style="padding:8px 0;font-size:14px;color:#e2e8f0;">{ip_str}</td></tr>'
        + f'</table>'
        + _divider()
        + _p(
            "If this was you, no action is needed. If you don't recognise this sign-in, "
            "reset your password immediately."
        )
        + _cta_button("Reset my password", f"{settings.APP_BASE_URL}/auth/forgot-password")
    )

    return subject, _base(subject, "A new sign-in was detected on your account.", body)
