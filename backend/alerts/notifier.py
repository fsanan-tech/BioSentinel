"""
BioSentinel — Alert Notification System

Sends email and SMS alerts when HIGH or CRITICAL assessments are detected.
No external dependencies — uses Python's built-in smtplib.

SMS is delivered via email-to-SMS gateways (free, works with any carrier):
  AT&T:      number@txt.att.net
  Verizon:   number@vtext.com
  T-Mobile:  number@tmomail.net
  Sprint:    number@messaging.sprintpcs.com

Environment variables required:
  ALERT_EMAIL_TO       Recipient email (your address)
  ALERT_EMAIL_FROM     Sender email (Gmail recommended)
  ALERT_SMTP_PASSWORD  Gmail App Password (not your regular password)
  ALERT_SMS_TO         Optional: phone@carrier.com for SMS

Gmail setup:
  1. Enable 2FA on your Google account
  2. Go to myaccount.google.com/apppasswords
  3. Generate App Password for "Mail"
  4. Use that 16-char password as ALERT_SMTP_PASSWORD

Alert thresholds:
  CRITICAL (5): Immediate alert — email + SMS
  HIGH (4):     Email alert only
  ANOMALY:      Email alert when z-score >= 3.0

Suppression: same disease+region suppressed for 6 hours
to prevent alert fatigue.
"""

import logging
import os
import smtplib
from datetime import datetime, timedelta
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from typing import List, Optional

from backend.models.schemas import FusedAssessment

log = logging.getLogger(__name__)

# Alert thresholds
ALERT_MIN_RISK_LEVEL = 4       # HIGH and above
ANOMALY_Z_THRESHOLD = 3.0     # Only alert on high z-scores
SUPPRESSION_HOURS = 6          # Don't re-alert same disease+region within 6h

# In-memory suppression cache (resets on restart — acceptable for MVP)
_suppression_cache: dict = {}


def _is_suppressed(disease: str, region: str) -> bool:
    key = f"{disease.lower()}_{region.lower()}"
    if key in _suppression_cache:
        if datetime.utcnow() < _suppression_cache[key]:
            return True
    return False


def _suppress(disease: str, region: str):
    key = f"{disease.lower()}_{region.lower()}"
    _suppression_cache[key] = datetime.utcnow() + timedelta(hours=SUPPRESSION_HOURS)


def _get_smtp_config() -> dict:
    return {
        "email_to": os.getenv("ALERT_EMAIL_TO", ""),
        "email_from": os.getenv("ALERT_EMAIL_FROM", ""),
        "smtp_password": os.getenv("ALERT_SMTP_PASSWORD", ""),
        "smtp_host": os.getenv("ALERT_SMTP_HOST", "smtp.gmail.com"),
        "smtp_port": int(os.getenv("ALERT_SMTP_PORT", "587")),
        "sms_to": os.getenv("ALERT_SMS_TO", ""),
    }


def _build_email_body(assessment: FusedAssessment, is_anomaly: bool = False) -> str:
    """Build plain text email body."""
    risk_labels = {1: "MINIMAL", 2: "LOW", 3: "MODERATE", 4: "HIGH", 5: "CRITICAL"}
    risk = risk_labels.get(int(assessment.risk_level), "UNKNOWN")
    conf = int(assessment.confidence * 100)

    # Extract intelligence brief if present
    brief = ""
    for sig in assessment.key_signals:
        if sig.startswith("📋 INTELLIGENCE BRIEF:"):
            brief = sig.replace("📋 INTELLIGENCE BRIEF:", "").strip()
            break

    anomaly_header = "⚠ STATISTICAL ANOMALY DETECTED\n" if is_anomaly else ""
    brief_section = f"\nINTELLIGENCE BRIEF:\n{brief}\n" if brief else ""

    body = f"""
{'='*60}
BIOSENTINEL ALERT — {risk} RISK
{'='*60}

{anomaly_header}
DISEASE/PATHOGEN: {assessment.primary_disease.upper()}
LOCATION:         {assessment.primary_location.place_name}
RISK LEVEL:       {risk}
CONFIDENCE:       {conf}%
SOURCES:          {assessment.source_count}
SPREAD RISK 14D:  {assessment.spread_risk_14d}
TIME:             {datetime.utcnow().strftime('%Y-%m-%d %H:%M UTC')}
{brief_section}
HEADLINE:
{assessment.headline}

SUMMARY:
{assessment.summary}

KEY SIGNALS:
""" + "\n".join(f"  • {s}" for s in assessment.key_signals[:6] if not s.startswith("📋")) + f"""

RECOMMENDED ACTION:
{assessment.recommended_action}

{'='*60}
Assessment ID: {assessment.assessment_id[:12]}
BioSentinel Biosecurity Intelligence Platform
{'='*60}
"""
    return body.strip()


def _build_sms_body(assessment: FusedAssessment, is_anomaly: bool = False) -> str:
    """Build short SMS body (160 char limit)."""
    risk_labels = {4: "HIGH", 5: "CRITICAL"}
    risk = risk_labels.get(int(assessment.risk_level), "ALERT")
    loc = assessment.primary_location.place_name[:20]
    disease = assessment.primary_disease[:20]
    conf = int(assessment.confidence * 100)
    anom = "[ANOMALY] " if is_anomaly else ""
    return f"BioSentinel {anom}{risk}: {disease} in {loc} ({conf}% confidence). Check dashboard."


def send_alert(
    assessment: FusedAssessment,
    is_anomaly: bool = False,
) -> bool:
    """
    Send email (and optionally SMS) alert for a high-priority assessment.
    Returns True if alert was sent, False if suppressed or failed.
    """
    config = _get_smtp_config()

    if not config["email_to"] or not config["email_from"] or not config["smtp_password"]:
        log.debug("[alerts] Email not configured — skipping alert")
        return False

    disease = assessment.primary_disease
    region = assessment.primary_location.place_name

    if _is_suppressed(disease, region):
        log.debug(f"[alerts] Suppressed duplicate alert: {disease}/{region}")
        return False

    risk_level = int(assessment.risk_level)
    if risk_level < ALERT_MIN_RISK_LEVEL and not is_anomaly:
        return False

    # Check anomaly z-score threshold
    if is_anomaly:
        headline = assessment.headline
        import re
        z_match = re.search(r"([\d.]+)σ", headline)
        if z_match:
            z = float(z_match.group(1))
            if z < ANOMALY_Z_THRESHOLD:
                return False

    risk_labels = {4: "HIGH", 5: "CRITICAL"}
    risk = risk_labels.get(risk_level, "ALERT")
    anom_tag = "[ANOMALY] " if is_anomaly else ""

    subject = (
        f"🚨 BioSentinel {anom_tag}{risk}: "
        f"{disease.upper()} — {region} "
        f"({int(assessment.confidence*100)}% confidence)"
    )

    try:
        # Build email
        msg = MIMEMultipart("alternative")
        msg["Subject"] = subject
        msg["From"] = config["email_from"]
        msg["To"] = config["email_to"]

        body = _build_email_body(assessment, is_anomaly)
        msg.attach(MIMEText(body, "plain"))

        # Send via SMTP
        with smtplib.SMTP(config["smtp_host"], config["smtp_port"]) as server:
            server.ehlo()
            server.starttls()
            server.login(config["email_from"], config["smtp_password"])
            server.sendmail(
                config["email_from"],
                config["email_to"],
                msg.as_string(),
            )

        log.info(f"[alerts] 📧 Email sent: {subject}")

        # Send SMS if configured
        if config["sms_to"]:
            sms_msg = MIMEText(_build_sms_body(assessment, is_anomaly))
            sms_msg["Subject"] = ""
            sms_msg["From"] = config["email_from"]
            sms_msg["To"] = config["sms_to"]

            with smtplib.SMTP(config["smtp_host"], config["smtp_port"]) as server:
                server.ehlo()
                server.starttls()
                server.login(config["email_from"], config["smtp_password"])
                server.sendmail(
                    config["email_from"],
                    config["sms_to"],
                    sms_msg.as_string(),
                )
            log.info(f"[alerts] 📱 SMS sent to {config['sms_to']}")

        _suppress(disease, region)
        return True

    except smtplib.SMTPAuthenticationError:
        log.error("[alerts] SMTP auth failed — check ALERT_SMTP_PASSWORD (use App Password for Gmail)")
        return False
    except Exception as e:
        log.error(f"[alerts] Alert send failed: {e}")
        return False


def process_alerts(
    assessments: List[FusedAssessment],
    anomaly_assessments: Optional[List[FusedAssessment]] = None,
) -> int:
    """
    Process all assessments and send alerts for HIGH/CRITICAL ones.
    Returns count of alerts sent.
    """
    sent = 0

    # Standard fusion assessments
    for assessment in assessments:
        if int(assessment.risk_level) >= ALERT_MIN_RISK_LEVEL:
            if send_alert(assessment, is_anomaly=False):
                sent += 1

    # Anomaly assessments
    if anomaly_assessments:
        for assessment in anomaly_assessments:
            if send_alert(assessment, is_anomaly=True):
                sent += 1

    return sent
