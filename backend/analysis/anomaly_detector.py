"""
BioSentinel — 90-Day Baseline Anomaly Detector

Architecture:
  1. After each ingestion cycle, record signal counts per disease+region per day
  2. After 14 days of history, compute rolling mean + std deviation (90-day window)
  3. Flag when current day's count exceeds mean + 2σ (p < 0.023)
  4. Generate ANOMALY assessments with:
       - z-score (how many standard deviations above baseline)
       - Estimated lead time vs clinical reporting
       - Trend direction (accelerating vs decelerating)
       - Historical context ("highest in N days")

This is the core of what BlueDot charges enterprise for.
The claim it enables: "BioSentinel detected [disease] anomaly in [region]
X days before CDC/WHO clinical reporting."

DB schema:
  signal_baseline  — daily signal counts per disease+region
  anomaly_log      — detected anomalies with z-scores
"""

import json
import math
import logging
import uuid
from collections import defaultdict
from datetime import datetime, timedelta
from typing import List, Dict, Optional, Tuple

from sqlalchemy import (
    Column, String, Float, Integer, Date, DateTime, Text,
    select, desc, func
)
from sqlalchemy.ext.asyncio import AsyncSession

from backend.database.db import Base, get_session
from backend.models.schemas import (
    ProcessedEvent, FusedAssessment, GeoLocation,
    ConfidenceBreakdown, SourceContribution, RiskLevel,
)

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────
# Database Tables
# ─────────────────────────────────────────────────────────

class SignalBaselineRecord(Base):
    """
    Daily signal count per disease + region.
    Accumulated over time to build the statistical baseline.
    """
    __tablename__ = "signal_baseline"

    id          = Column(Integer, primary_key=True, autoincrement=True)
    record_date = Column(Date, index=True, nullable=False)
    disease     = Column(String, index=True, nullable=False)
    region      = Column(String, index=True, nullable=False)
    signal_count = Column(Integer, default=0)
    source_breakdown = Column(Text)   # JSON: {source_id: count}
    created_at  = Column(DateTime, default=datetime.utcnow)


class AnomalyLogRecord(Base):
    """
    Log of detected anomalies with statistical context.
    Used for audit trail and to avoid duplicate alerts.
    """
    __tablename__ = "anomaly_log"

    id              = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    detected_at     = Column(DateTime, default=datetime.utcnow)
    disease         = Column(String, index=True)
    region          = Column(String, index=True)
    z_score         = Column(Float)
    current_count   = Column(Integer)
    baseline_mean   = Column(Float)
    baseline_std    = Column(Float)
    days_of_history = Column(Integer)
    assessment_id   = Column(String)   # Links to FusedAssessment
    suppressed      = Column(Integer, default=0)  # 1 = duplicate, skip


# ─────────────────────────────────────────────────────────
# Statistical Engine
# ─────────────────────────────────────────────────────────

def _mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: List[float], mean: float) -> float:
    if len(values) < 2:
        return 0.0
    variance = sum((v - mean) ** 2 for v in values) / (len(values) - 1)
    return math.sqrt(variance)


def compute_z_score(current: float, history: List[float]) -> Tuple[float, float, float]:
    """Returns (z_score, mean, std)."""
    if len(history) < 7:
        return 0.0, 0.0, 0.0
    mean = _mean(history)
    std = _std(history, mean)
    if std < 0.5:
        # Very stable baseline — use mean as reference with min std of 1
        std = max(1.0, mean * 0.2)
    z = (current - mean) / std
    return z, mean, std


def estimate_lead_time(disease: str, source_ids: List[str]) -> str:
    """
    Estimate how many days ahead of clinical reporting this signal typically is.
    Based on published literature.
    """
    has_wastewater = any(s.startswith("nwss") for s in source_ids)
    has_trends = "google_trends" in source_ids
    has_pubmed = "pubmed" in source_ids

    if has_wastewater:
        return "4–7 days ahead of clinical case reporting (wastewater lead time)"
    elif has_trends:
        return "7–10 days ahead of clinical surveillance (search trend lead time)"
    elif has_pubmed:
        return "3–14 days ahead of mainstream reporting (academic preprint lead time)"
    else:
        return "1–3 days ahead of official reporting"


# ─────────────────────────────────────────────────────────
# Baseline Accumulator
# ─────────────────────────────────────────────────────────

async def record_daily_signals(events: List[ProcessedEvent]):
    """
    After each ingestion cycle, tally signal counts by disease+region
    and store in the baseline table. Called once per ingestion cycle.
    """
    today = datetime.utcnow().date()

    # Tally: {(disease, region): {source_id: count}}
    tallies: Dict[Tuple[str, str], Dict[str, int]] = defaultdict(lambda: defaultdict(int))

    for event in events:
        disease = (event.primary_disease or "unknown").lower().strip()
        region = "unknown"
        if event.primary_location and event.primary_location.place_name:
            region = event.primary_location.place_name.lower().strip()

        source_id = event.raw_signal.source_id
        tallies[(disease, region)][source_id] += 1

    async with get_session() as s:
        for (disease, region), source_counts in tallies.items():
            total = sum(source_counts.values())

            # Check if we already have a record for today
            existing = await s.execute(
                select(SignalBaselineRecord).where(
                    SignalBaselineRecord.record_date == today,
                    SignalBaselineRecord.disease == disease,
                    SignalBaselineRecord.region == region,
                )
            )
            existing_record = existing.scalars().first()

            if existing_record:
                # Update existing record
                existing_record.signal_count += total
                existing_record.source_breakdown = json.dumps(
                    {**json.loads(existing_record.source_breakdown or "{}"),
                     **source_counts}
                )
            else:
                # New record for today
                record = SignalBaselineRecord(
                    record_date=today,
                    disease=disease,
                    region=region,
                    signal_count=total,
                    source_breakdown=json.dumps(dict(source_counts)),
                )
                s.add(record)

        await s.commit()

    log.info(f"[anomaly] Recorded baseline data for {len(tallies)} disease+region pairs")


# ─────────────────────────────────────────────────────────
# Anomaly Detector
# ─────────────────────────────────────────────────────────

# Anomaly thresholds
Z_SCORE_MODERATE  = 2.0   # p < 0.023 — alert threshold
Z_SCORE_HIGH      = 3.0   # p < 0.001 — elevated alert
Z_SCORE_CRITICAL  = 4.0   # p < 0.00003 — critical alert
MIN_HISTORY_DAYS  = 14    # Need at least 2 weeks before alerting
WINDOW_DAYS       = 90    # Rolling baseline window

# Suppress re-alerting for same disease+region within this many hours
SUPPRESSION_HOURS = 24

# Disease-region pairs to skip (too noisy or always elevated)
SKIP_PAIRS = {
    ("influenza", "unknown"),
    ("flu", "unknown"),
    ("respiratory illness", "unknown"),
}


async def detect_anomalies(
    today_events: List[ProcessedEvent],
) -> List[FusedAssessment]:
    """
    Main anomaly detection function. Called after each ingestion cycle.

    Steps:
    1. Get today's signal counts by disease+region
    2. Fetch 90-day history for each pair
    3. Compute z-score
    4. If z >= 2.0 and >= 14 days history: generate anomaly assessment
    5. Check suppression (don't re-alert within 24h for same pair)
    """
    today = datetime.utcnow().date()
    window_start = today - timedelta(days=WINDOW_DAYS)
    anomaly_assessments = []

    # Tally today's events
    today_tallies: Dict[Tuple[str, str], List[ProcessedEvent]] = defaultdict(list)
    for event in today_events:
        disease = (event.primary_disease or "unknown").lower().strip()
        region = "unknown"
        if event.primary_location and event.primary_location.place_name:
            region = event.primary_location.place_name.lower().strip()
        today_tallies[(disease, region)].append(event)

    async with get_session() as s:
        for (disease, region), events in today_tallies.items():
            if (disease, region) in SKIP_PAIRS:
                continue
            if not disease or disease == "unknown":
                continue

            current_count = len(events)

            # Fetch historical daily counts (last 90 days, excluding today)
            history_result = await s.execute(
                select(SignalBaselineRecord)
                .where(
                    SignalBaselineRecord.disease == disease,
                    SignalBaselineRecord.region == region,
                    SignalBaselineRecord.record_date >= window_start,
                    SignalBaselineRecord.record_date < today,
                )
                .order_by(SignalBaselineRecord.record_date)
            )
            history_records = history_result.scalars().all()
            history_counts = [r.signal_count for r in history_records]
            days_of_history = len(history_counts)

            if days_of_history < MIN_HISTORY_DAYS:
                continue   # Not enough history yet

            z_score, baseline_mean, baseline_std = compute_z_score(
                current_count, history_counts
            )

            if z_score < Z_SCORE_MODERATE:
                continue   # Not anomalous

            # Check suppression — don't re-alert within 24h
            suppression_cutoff = datetime.utcnow() - timedelta(hours=SUPPRESSION_HOURS)
            recent_alert = await s.execute(
                select(AnomalyLogRecord)
                .where(
                    AnomalyLogRecord.disease == disease,
                    AnomalyLogRecord.region == region,
                    AnomalyLogRecord.detected_at >= suppression_cutoff,
                    AnomalyLogRecord.suppressed == 0,
                )
                .order_by(desc(AnomalyLogRecord.detected_at))
                .limit(1)
            )
            if recent_alert.scalars().first():
                log.info(f"[anomaly] Suppressed duplicate alert: {disease}/{region}")
                continue

            # ── Generate anomaly assessment ────────────────────────
            assessment = _build_anomaly_assessment(
                disease=disease,
                region=region,
                events=events,
                z_score=z_score,
                baseline_mean=baseline_mean,
                baseline_std=baseline_std,
                current_count=current_count,
                days_of_history=days_of_history,
                history_counts=history_counts,
            )
            anomaly_assessments.append(assessment)

            # Log the anomaly
            alert_log = AnomalyLogRecord(
                disease=disease,
                region=region,
                z_score=z_score,
                current_count=current_count,
                baseline_mean=baseline_mean,
                baseline_std=baseline_std,
                days_of_history=days_of_history,
                assessment_id=assessment.assessment_id,
            )
            s.add(alert_log)

        await s.commit()

    if anomaly_assessments:
        log.info(
            f"[anomaly] {len(anomaly_assessments)} statistical anomalies detected"
        )

    return anomaly_assessments


def _build_anomaly_assessment(
    disease: str,
    region: str,
    events: List[ProcessedEvent],
    z_score: float,
    baseline_mean: float,
    baseline_std: float,
    current_count: int,
    days_of_history: int,
    history_counts: List[float],
) -> FusedAssessment:
    """Build a FusedAssessment from an anomaly detection result."""

    # Determine risk level from z-score
    if z_score >= Z_SCORE_CRITICAL:
        risk = RiskLevel.CRITICAL
        severity_label = "CRITICAL ANOMALY"
        confidence = 0.90
    elif z_score >= Z_SCORE_HIGH:
        risk = RiskLevel.HIGH
        severity_label = "HIGH ANOMALY"
        confidence = 0.78
    else:
        risk = RiskLevel.MODERATE
        severity_label = "STATISTICAL ANOMALY"
        confidence = 0.62

    # Historical context
    max_historical = max(history_counts) if history_counts else 0
    is_all_time_high = current_count > max_historical
    pct_above_mean = int(((current_count - baseline_mean) / baseline_mean * 100)
                         if baseline_mean > 0 else 0)

    # Trend: compare last 7 days vs prior 7 days
    recent_7 = history_counts[-7:] if len(history_counts) >= 7 else history_counts
    prior_7 = history_counts[-14:-7] if len(history_counts) >= 14 else []
    trend = "accelerating" if (
        prior_7 and _mean(recent_7) > _mean(prior_7) * 1.2
    ) else "stable" if (
        prior_7 and _mean(recent_7) <= _mean(prior_7) * 1.2
    ) else "insufficient data"

    # Source context
    source_ids = list(set(e.raw_signal.source_id for e in events))
    lead_time = estimate_lead_time(disease, source_ids)

    # Primary location
    locations = [e.primary_location for e in events if e.primary_location]
    primary_location = locations[0] if locations else GeoLocation(
        place_name=region.title(), geo_confidence=0.5
    )

    # Build headline and summary
    headline = (
        f"[{severity_label}] {disease.upper()} — {region.title()} "
        f"({z_score:.1f}σ above {days_of_history}-day baseline)"
    )

    all_time_str = " — HIGHEST IN RECORDED HISTORY" if is_all_time_high else ""
    summary = (
        f"Statistical anomaly detected: {disease} signal in {region.title()} "
        f"is {z_score:.1f} standard deviations above the {days_of_history}-day baseline. "
        f"Current signal count: {current_count} "
        f"(baseline mean: {baseline_mean:.1f} ± {baseline_std:.1f}). "
        f"This is {pct_above_mean}% above normal levels{all_time_str}. "
        f"Signal trend: {trend}. "
        f"Estimated lead time: {lead_time}. "
        f"Sources contributing: {', '.join(s.upper() for s in source_ids[:5])}."
    )

    key_signals = [
        f"Z-score: {z_score:.2f}σ (threshold: {Z_SCORE_MODERATE}σ)",
        f"Current: {current_count} signals vs baseline {baseline_mean:.1f} ± {baseline_std:.1f}",
        f"History: {days_of_history} days | Trend: {trend}",
        f"Lead time estimate: {lead_time}",
    ]
    if is_all_time_high:
        key_signals.append(f"⚠ HIGHEST SIGNAL COUNT IN {days_of_history}-DAY RECORD")

    # Confidence breakdown
    breakdown = ConfidenceBreakdown(
        base_prior=0.12,
        source_agreement=min(len(source_ids) * 0.08, 0.32),
        source_credibility=sum(
            e.raw_signal.credibility_weight for e in events
        ) / len(events),
        high_consequence_bonus=0.15 if any(
            e.entities.is_high_consequence for e in events
        ) else 0.0,
        pandemic_keyword_bonus=0.10 if any(
            e.entities.has_pandemic_keywords for e in events
        ) else 0.0,
        case_data_bonus=0.08 if any(
            e.entities.case_count for e in events
        ) else 0.0,
        restricted_data_uplift=0.0,
        final_confidence=confidence,
    )

    # Source contributions
    source_contributions = []
    for sid in source_ids:
        src_events = [e for e in events if e.raw_signal.source_id == sid]
        cw = src_events[0].raw_signal.credibility_weight
        source_contributions.append(SourceContribution(
            source_id=sid,
            credibility_weight=cw,
            event_ids=[e.event_id for e in src_events],
            contribution_score=cw * len(src_events) / len(events),
        ))

    times = [e.raw_signal.published_at for e in events]

    return FusedAssessment(
        assessment_id=str(uuid.uuid4()),
        primary_disease=disease,
        primary_location=primary_location,
        risk_level=risk,
        confidence=confidence,
        confidence_breakdown=breakdown,
        headline=headline,
        summary=summary,
        key_signals=key_signals,
        source_count=len(source_ids),
        contributing_sources=source_contributions,
        event_ids=[e.event_id for e in events],
        earliest_signal=min(times),
        latest_signal=max(times),
        spread_risk_14d="HIGH" if z_score >= Z_SCORE_HIGH else "MODERATE",
        recommended_action=(
            "Immediate escalation — statistical anomaly exceeds critical threshold. "
            "Cross-reference with clinical surveillance and wastewater data."
            if z_score >= Z_SCORE_CRITICAL else
            "Elevated monitoring. Confirm with additional sources. "
            "Check CDC/WHO for corroborating clinical reports."
            if z_score >= Z_SCORE_HIGH else
            "Watch brief. Monitor for signal continuation over next 48–72h. "
            "Anomaly may resolve or escalate."
        ),
        restricted_data_incorporated=False,
    )


async def get_anomaly_stats() -> dict:
    """Summary stats for the API."""
    async with get_session() as s:
        total_baseline_records = (await s.execute(
            select(func.count(SignalBaselineRecord.id))
        )).scalar()

        total_anomalies = (await s.execute(
            select(func.count(AnomalyLogRecord.id))
            .where(AnomalyLogRecord.suppressed == 0)
        )).scalar()

        oldest_baseline = (await s.execute(
            select(func.min(SignalBaselineRecord.record_date))
        )).scalar()

        return {
            "baseline_records": total_baseline_records,
            "anomalies_detected": total_anomalies,
            "baseline_start_date": str(oldest_baseline) if oldest_baseline else None,
            "days_of_history": (
                (datetime.utcnow().date() - oldest_baseline).days
                if oldest_baseline else 0
            ),
            "min_days_for_detection": MIN_HISTORY_DAYS,
        }
