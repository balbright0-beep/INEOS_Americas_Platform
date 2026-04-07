"""
Tiny SMTP helper for sending MFA codes.

Configured via environment variables:
  SMTP_HOST       (e.g. smtp.sendgrid.net, smtp.gmail.com, smtp.office365.com)
  SMTP_PORT       (default 587)
  SMTP_USERNAME   (e.g. apikey for SendGrid)
  SMTP_PASSWORD
  SMTP_FROM       (the "From" address, e.g. "INEOS Americas <noreply@ineos.com>")
  SMTP_USE_TLS    (default "true")

If SMTP_HOST is unset, emails are logged to stdout instead of sent — handy
for local development and CI.
"""
import os
import smtplib
import ssl
from email.message import EmailMessage

SMTP_HOST = os.environ.get("SMTP_HOST", "")
SMTP_PORT = int(os.environ.get("SMTP_PORT", "587"))
SMTP_USERNAME = os.environ.get("SMTP_USERNAME", "")
SMTP_PASSWORD = os.environ.get("SMTP_PASSWORD", "")
SMTP_FROM = os.environ.get("SMTP_FROM", "INEOS Americas <noreply@ineos.com>")
SMTP_USE_TLS = os.environ.get("SMTP_USE_TLS", "true").lower() == "true"


def send_email(to: str, subject: str, body_text: str, body_html: str | None = None) -> bool:
    """Send an email. Returns True on success, False on failure."""
    if not to:
        return False

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = SMTP_FROM
    msg["To"] = to
    msg.set_content(body_text)
    if body_html:
        msg.add_alternative(body_html, subtype="html")

    if not SMTP_HOST:
        # Dev mode — no SMTP configured. Print so the code is visible in Render logs.
        print(f"[email:DEV] To={to} Subject={subject}\n{body_text}")
        return True

    try:
        if SMTP_USE_TLS:
            ctx = ssl.create_default_context()
            with smtplib.SMTP(SMTP_HOST, SMTP_PORT, timeout=15) as s:
                s.starttls(context=ctx)
                if SMTP_USERNAME:
                    s.login(SMTP_USERNAME, SMTP_PASSWORD)
                s.send_message(msg)
        else:
            with smtplib.SMTP_SSL(SMTP_HOST, SMTP_PORT, timeout=15) as s:
                if SMTP_USERNAME:
                    s.login(SMTP_USERNAME, SMTP_PASSWORD)
                s.send_message(msg)
        return True
    except Exception as e:
        print(f"[email:ERROR] Failed to send to {to}: {e}")
        return False


def send_mfa_code(to: str, username: str, code: str, expiry_minutes: int = 5) -> bool:
    subject = f"INEOS Americas — Your sign-in code is {code}"
    text = (
        f"Hello {username},\n\n"
        f"Your INEOS Americas Platform sign-in code is: {code}\n\n"
        f"This code is valid for {expiry_minutes} minutes. "
        f"If you did not request this code, please ignore this email and "
        f"consider changing your password.\n\n"
        f"— INEOS Americas"
    )
    html = f"""\
<!DOCTYPE html><html><body style="font-family:Arial,sans-serif;color:#1D1D1D;max-width:560px;margin:0 auto;padding:24px">
  <div style="border-top:4px solid #FF4639;padding-top:20px">
    <h2 style="margin:0 0 16px;font-weight:900;letter-spacing:0.5px">INEOS AMERICAS</h2>
    <p>Hello {username},</p>
    <p>Your sign-in code is:</p>
    <div style="font-size:32px;font-weight:900;letter-spacing:6px;background:#f5f5f5;padding:16px 24px;text-align:center;border-radius:4px;margin:20px 0">{code}</div>
    <p style="color:#666;font-size:13px">This code is valid for <strong>{expiry_minutes} minutes</strong>. If you did not request this code, please ignore this email and consider changing your password.</p>
    <hr style="border:none;border-top:1px solid #e5e5e5;margin:24px 0">
    <p style="color:#999;font-size:11px">INEOS Americas Platform · automated message, please do not reply</p>
  </div>
</body></html>"""
    return send_email(to, subject, text, html)
