"""
BioSentinel Fusion Engine
Converts a stream of ProcessedEvents into FusedAssessments.

Design philosophy:
  - Bayesian updating: each independent source confirmation revises P(real outbreak)
  - Credibility weighting: not all sources are equal
  - Spatial + temporal clustering: nearby events about the same disease are merged
  - Explainable scores: every confidence value has a breakdown
  - Restricted data uplift: classified signals can lift confidence without exposing raw data
"""
import uuid
import logging
import math
from datetime import datetime
from typing import List, Dict, Optional, Tuple
from collections import defaultdict

from backend.models.schemas import (
    ProcessedEvent, FusedAssessment, GeoLocation,
    ConfidenceBreakdown, SourceContribution, RiskLevel,
    RestrictedSignal,
)
from config import FUSION, RISK_LEVELS

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Spatial helpers
# ─────────────────────────────────────────────
def haversine_km(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance in km."""
    R = 6371.0
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    dphi = math.radians(lat2 - lat1)
    dlambda = math.radians(lon2 - lon1)
    a = math.sin(dphi / 2) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda / 2) ** 2
    return R * 2 * math.atan2(math.sqrt(a), math.sqrt(1 - a))


def events_are_proximate(e1: ProcessedEvent, e2: ProcessedEvent) -> bool:
    """True if two events are spatially and temporally close enough to merge."""
    if not (e1.primary_location and e2.primary_location):
        # Can't compare without geocoords — fall back to disease-only matching
        return False
    if not all([
        e1.primary_location.latitude, e1.primary_location.longitude,
        e2.primary_location.latitude, e2.primary_location.longitude,
    ]):
        return False

    dist = haversine_km(
        e1.primary_location.latitude, e1.primary_location.longitude,
        e2.primary_location.latitude, e2.primary_location.longitude,
    )
    if dist > FUSION["spatial_merge_radius_km"]:
        return False

    dt = abs((e1.raw_signal.published_at - e2.raw_signal.published_at).total_seconds())
    max_hours = FUSION["temporal_merge_hours"] * 3600
    return dt <= max_hours


def same_disease(e1: ProcessedEvent, e2: ProcessedEvent) -> bool:
    if not e1.primary_disease or not e2.primary_disease:
        return False
    return e1.primary_disease.lower() == e2.primary_disease.lower()


# ─────────────────────────────────────────────
# Confidence Scoring
# ─────────────────────────────────────────────
def compute_confidence(
    events: List[ProcessedEvent],
    restricted_signal: Optional[RestrictedSignal] = None,
) -> ConfidenceBreakdown:
    """
    Bayesian-flavored confidence scoring.
    P(real_outbreak | signals) using log-odds updating.
    """
    prior = FUSION["prior_probability"]

    # Convert prior to log-odds
    log_odds = math.log(prior / (1 - prior))

    # Source credibility contribution
    credibility_scores = [e.raw_signal.credibility_weight for e in events]
    avg_credibility = sum(credibility_scores) / len(credibility_scores) if credibility_scores else 0.5

    # Each independent source confirmation updates log-odds
    unique_sources = set(e.raw_signal.source_id for e in events)
    n_independent = len(unique_sources)

    source_agreement_bonus = 0.0
    for i in range(1, n_independent):
        # Each additional source adds a credibility-weighted boost
        boost = FUSION["source_confirmation_boost"] * avg_credibility
        log_odds += boost
        source_agreement_bonus += boost * (1 - prior)  # Linearized contribution

    # High-consequence pathogen
    is_hcp = any(e.entities.is_high_consequence for e in events)
    hcp_bonus = 0.0
    if is_hcp:
        hcp_bonus = 0.25
        log_odds += math.log((1 + hcp_bonus) / 1)

    # Pandemic keywords
    has_pk = any(e.entities.has_pandemic_keywords for e in events)
    pk_bonus = 0.0
    if has_pk:
        pk_bonus = 0.12
        log_odds += math.log((1 + pk_bonus) / 1)

    # Case data available
    has_case_data = any(
        e.entities.case_count is not None or e.entities.death_count is not None
        for e in events
    )
    case_bonus = 0.0
    if has_case_data:
        case_bonus = 0.10
        log_odds += math.log((1 + case_bonus) / 1)

    # Restricted data uplift (derived score only, never raw data)
    restricted_uplift = 0.0
    if restricted_signal and restricted_signal.anomaly_detected:
        w = restricted_signal.confidence_score
        restricted_uplift = w * 0.40   # Significant uplift from authoritative source
        log_odds += math.log((1 + restricted_uplift) / 1)

    # Convert log-odds back to probability
    final_p = 1 / (1 + math.exp(-log_odds))
    final_p = max(0.0, min(1.0, final_p))

    return ConfidenceBreakdown(
        base_prior=prior,
        source_agreement=min(source_agreement_bonus, 0.40),
        source_credibility=avg_credibility,
        high_consequence_bonus=hcp_bonus,
        pandemic_keyword_bonus=pk_bonus,
        case_data_bonus=case_bonus,
        restricted_data_uplift=restricted_uplift,
        final_confidence=final_p,
    )


def confidence_to_risk_level(confidence: float, is_hcp: bool) -> RiskLevel:
    # HCP events get a minimum risk of MODERATE
    min_level = 3 if is_hcp else 1
    for level in [5, 4, 3, 2, 1]:
        if confidence >= RISK_LEVELS[level]["min_confidence"]:
            return RiskLevel(max(level, min_level))
    return RiskLevel(min_level)


# ─────────────────────────────────────────────
# Assessment Generation
# ─────────────────────────────────────────────
def generate_headline(disease: str, location: str, risk: RiskLevel, confidence: float) -> str:
    pct = int(confidence * 100)
    labels = {1: "MINIMAL", 2: "LOW", 3: "MODERATE", 4: "HIGH", 5: "CRITICAL"}
    label = labels.get(int(risk), "UNKNOWN")
    return f"[{label}] {disease.upper()} — {location} ({pct}% confidence)"


def generate_summary(
    disease: str,
    location: str,
    events: List[ProcessedEvent],
    confidence: float,
    is_hcp: bool,
) -> str:
    n = len(events)
    sources = list(set(e.raw_signal.source_id for e in events))
    src_str = ", ".join(s.upper() for s in sources)
    case_counts = [e.entities.case_count for e in events if e.entities.case_count]
    death_counts = [e.entities.death_count for e in events if e.entities.death_count]

    parts = [
        f"{n} signal{'s' if n > 1 else ''} from {src_str} report "
        f"{'a high-consequence pathogen (' if is_hcp else ''}"
        f"{disease}{')' if is_hcp else ''} activity in {location}.",
    ]

    if case_counts:
        parts.append(f"Estimated case count: {max(case_counts):,}.")
    if death_counts:
        parts.append(f"Reported deaths: {max(death_counts):,}.")

    parts.append(
        f"Fusion confidence: {int(confidence * 100)}% based on "
        f"{len(sources)} independent source{'s' if len(sources) > 1 else ''}."
    )

    if is_hcp:
        parts.append("⚠ HIGH-CONSEQUENCE PATHOGEN — elevated monitoring recommended.")

    return " ".join(parts)


def estimate_spread_risk(disease: str, confidence: float, is_hcp: bool) -> str:
    if confidence > 0.75 or is_hcp:
        return "HIGH"
    elif confidence > 0.45:
        return "MODERATE"
    return "LOW"


def recommend_action(risk: RiskLevel, is_hcp: bool) -> str:
    if int(risk) >= 5 or is_hcp:
        return "Immediate escalation to biosecurity response team. Activate monitoring protocols."
    elif int(risk) == 4:
        return "Elevated monitoring. Cross-reference with additional intelligence sources."
    elif int(risk) == 3:
        return "Watch brief. Continue monitoring for 72h for signal confirmation."
    else:
        return "Routine surveillance. No immediate action required."


# ─────────────────────────────────────────────
# Main Fusion Function
# ─────────────────────────────────────────────
def fuse_events(
    events: List[ProcessedEvent],
    restricted_signals: Optional[List[RestrictedSignal]] = None,
) -> List[FusedAssessment]:
    """
    Cluster events by disease + geography + time, then compute
    a FusedAssessment for each cluster.
    """
    if not events:
        return []

    # ── Step 1: Cluster events ──────────────────
    clusters: List[List[ProcessedEvent]] = []
    assigned = set()

    for i, event in enumerate(events):
        if i in assigned:
            continue
        cluster = [event]
        assigned.add(i)
        for j, other in enumerate(events):
            if j in assigned:
                continue
            if same_disease(event, other) and events_are_proximate(event, other):
                cluster.append(other)
                assigned.add(j)
        clusters.append(cluster)

    log.info(f"Fusion: {len(events)} events → {len(clusters)} clusters")

    assessments = []
    for cluster in clusters:
        if not cluster:
            continue

        # ── Step 2: Choose representative disease + location ──
        diseases = [e.primary_disease for e in cluster if e.primary_disease]
        disease = max(set(diseases), key=diseases.count) if diseases else "Unknown"

        locations = [e.primary_location for e in cluster if e.primary_location]
        primary_location = locations[0] if locations else GeoLocation(
            place_name="Unknown", geo_confidence=0.0
        )

        # ── Step 3: Find applicable restricted signal ──
        applicable_restricted = None
        if restricted_signals:
            loc_name = primary_location.place_name.lower()
            for rs in restricted_signals:
                if rs.region_code.lower() in loc_name or loc_name in rs.region_code.lower():
                    applicable_restricted = rs
                    break

        # ── Step 4: Score ──
        breakdown = compute_confidence(cluster, applicable_restricted)
        confidence = breakdown.final_confidence

        if confidence < FUSION["min_confidence_threshold"]:
            continue

        is_hcp = any(e.entities.is_high_consequence for e in cluster)
        risk = confidence_to_risk_level(confidence, is_hcp)

        # ── Step 5: Build assessment ──
        times = [e.raw_signal.published_at for e in cluster]
        source_contributions = []
        for source_id in set(e.raw_signal.source_id for e in cluster):
            src_events = [e for e in cluster if e.raw_signal.source_id == source_id]
            cw = src_events[0].raw_signal.credibility_weight
            source_contributions.append(SourceContribution(
                source_id=source_id,
                credibility_weight=cw,
                event_ids=[e.event_id for e in src_events],
                contribution_score=cw * len(src_events) / len(cluster),
            ))

        key_signals = []
        for e in cluster[:5]:
            kw = e.entities.severity_keywords[:2]
            if kw:
                key_signals.append(f"{e.raw_signal.source_id.upper()}: {', '.join(kw)}")
            else:
                key_signals.append(f"{e.raw_signal.source_id.upper()}: {e.raw_signal.title[:80]}")

        loc_name = primary_location.place_name

        assessment = FusedAssessment(
            assessment_id=str(uuid.uuid4()),
            primary_disease=disease,
            primary_location=primary_location,
            risk_level=risk,
            confidence=confidence,
            confidence_breakdown=breakdown,
            headline=generate_headline(disease, loc_name, risk, confidence),
            summary=generate_summary(disease, loc_name, cluster, confidence, is_hcp),
            key_signals=key_signals,
            source_count=len(set(e.raw_signal.source_id for e in cluster)),
            contributing_sources=source_contributions,
            event_ids=[e.event_id for e in cluster],
            earliest_signal=min(times),
            latest_signal=max(times),
            spread_risk_14d=estimate_spread_risk(disease, confidence, is_hcp),
            recommended_action=recommend_action(risk, is_hcp),
            restricted_data_incorporated=applicable_restricted is not None,
        )
        assessments.append(assessment)

    assessments.sort(key=lambda a: a.confidence, reverse=True)
    return assessments
