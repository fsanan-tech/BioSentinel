"""
BioSentinel Restricted Data Interface
Governed plug-in layer for classified / restricted signal sources.

ARCHITECTURE CONTRACT:
  - Raw classified data NEVER enters this system
  - The originating classified system processes its own data
  - Only derived confidence scores + anomaly flags transit here
  - All restricted signals are tagged as "DERIVED" classification level
  - Full audit trail is maintained

INTEGRATION SCENARIOS:
  1. Military wastewater surveillance labs
     → Submit: pathogen_class_hint + anomaly_detected + confidence_score
  2. Navy expeditionary medical records
     → Submit: syndromic_surveillance_spike + region_code + confidence_score
  3. DoD bio-environment sensors
     → Submit: bioenv_sensor_alert + coordinates + confidence_score
  4. IC finished intelligence reports
     → Submit: threat_assessment_score + region + confidence_score

SECURITY NOTE:
  When deployed in a real environment, this interface should sit behind
  a separate authentication layer (e.g., PKI cert-based or SAML).
  The BIOSENTINEL_RESTRICTED_API_KEY env var gates access.
  Restrict to specific IP ranges / network segments as required.
"""
import logging
import os
from datetime import datetime
from typing import List, Optional

import httpx
from pydantic import BaseModel, Field

from backend.models.schemas import RestrictedSignal
from config import RESTRICTED_INTERFACE

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Submission schema for restricted sources
# ─────────────────────────────────────────────
class RestrictedSignalSubmission(BaseModel):
    """
    What a restricted source sends to BioSentinel.
    Deliberately minimal — no raw data.
    """
    signal_type: str                            # Must be in ALLOWED_SIGNAL_TYPES
    region_code: str                            # ISO country code or military grid zone
    confidence_score: float = Field(ge=0, le=1)
    anomaly_detected: bool
    pathogen_class_hint: Optional[str] = None   # "respiratory", "hemorrhagic", etc.
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    # Submitter provides an opaque reference for audit trail
    source_reference: str = ""


# ─────────────────────────────────────────────
# Validation
# ─────────────────────────────────────────────
ALLOWED_SIGNAL_TYPES = RESTRICTED_INTERFACE["allowed_signal_types"]


def validate_restricted_submission(submission: RestrictedSignalSubmission) -> tuple[bool, str]:
    """Returns (is_valid, reason)."""
    if submission.signal_type not in ALLOWED_SIGNAL_TYPES:
        return False, f"Unknown signal_type: {submission.signal_type}"
    if not (0.0 <= submission.confidence_score <= 1.0):
        return False, "confidence_score must be in [0, 1]"
    if not submission.region_code:
        return False, "region_code is required"
    return True, "OK"


# ─────────────────────────────────────────────
# In-memory store for received restricted signals
# (In production: use a separate encrypted store)
# ─────────────────────────────────────────────
_restricted_signals: List[RestrictedSignal] = []


def accept_restricted_signal(submission: RestrictedSignalSubmission) -> RestrictedSignal:
    """
    Accept a validated restricted submission and convert to RestrictedSignal.
    Adds to the in-memory pool used by the fusion engine.
    """
    signal = RestrictedSignal(
        signal_type=submission.signal_type,
        region_code=submission.region_code,
        confidence_score=submission.confidence_score,
        anomaly_detected=submission.anomaly_detected,
        pathogen_class_hint=submission.pathogen_class_hint,
        timestamp=submission.timestamp,
        classification_level="DERIVED",
        source_reference=submission.source_reference,
    )
    _restricted_signals.append(signal)
    log.info(
        f"[restricted] Accepted signal: type={signal.signal_type} "
        f"region={signal.region_code} anomaly={signal.anomaly_detected} "
        f"confidence={signal.confidence_score:.2f}"
    )
    return signal


def get_active_restricted_signals(max_age_hours: int = 72) -> List[RestrictedSignal]:
    """Return restricted signals within the activity window."""
    cutoff = datetime.utcnow().timestamp() - (max_age_hours * 3600)
    return [s for s in _restricted_signals
            if s.timestamp.timestamp() >= cutoff]


def clear_restricted_signals():
    """Clear all restricted signals (for testing / rotation)."""
    global _restricted_signals
    _restricted_signals = []


# ─────────────────────────────────────────────
# External push client
# Used when BioSentinel is the CLIENT pulling
# from a governed restricted data endpoint
# (rather than receiving push submissions)
# ─────────────────────────────────────────────
class RestrictedDataClient:
    """
    Optional: Pull interface for when BioSentinel calls an
    external restricted endpoint to fetch derived scores.
    Only enabled if RESTRICTED_INTERFACE.enabled == True.
    """

    def __init__(self):
        self.endpoint = RESTRICTED_INTERFACE.get("endpoint", "")
        self.api_key = os.getenv(RESTRICTED_INTERFACE.get("api_key_env", ""), "")
        self.enabled = RESTRICTED_INTERFACE.get("enabled", False) and bool(self.endpoint)

    async def fetch(self) -> List[RestrictedSignal]:
        if not self.enabled:
            return []
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.get(
                    self.endpoint,
                    headers={"Authorization": f"Bearer {self.api_key}"},
                )
                resp.raise_for_status()
                data = resp.json()
                signals = []
                for item in data.get("signals", []):
                    try:
                        signals.append(RestrictedSignal(**item))
                    except Exception as e:
                        log.warning(f"[restricted] Failed to parse signal: {e}")
                log.info(f"[restricted] Pulled {len(signals)} derived signals")
                return signals
        except Exception as e:
            log.error(f"[restricted] Pull failed: {e}")
            return []
