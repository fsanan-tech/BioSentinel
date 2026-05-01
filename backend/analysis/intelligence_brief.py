"""
BioSentinel — LLM Intelligence Brief Generator

Uses Claude API to generate plain-English intelligence summaries
for HIGH and CRITICAL assessments. Transforms raw confidence scores
into decision-grade language a non-technical analyst can act on.

Output example:
  "Three independent sources — CDC wastewater surveillance (HIGH),
   PubMed preprint on novel orthomyxovirus, and r/indonesia community
   reports — converge on an unusual respiratory cluster in Java,
   Indonesia. The multi-source agreement and novel pathogen language
   closely match the early COVID-19 signal pattern from December 2019.
   Recommended: Cross-reference with WHO GOARN and request APSED
   situational report within 24 hours."

This module is called after fusion for assessments above the
BRIEF_THRESHOLD confidence level. Briefs are cached in the DB
to avoid redundant API calls.
"""

import logging
import os
from datetime import datetime
from typing import Optional

import httpx

from backend.models.schemas import FusedAssessment

log = logging.getLogger(__name__)

# Only generate briefs for assessments above this confidence
BRIEF_THRESHOLD = 0.45

# Historical signal patterns for context injection
HISTORICAL_PATTERNS = """
Key historical biosurveillance signal patterns to reference when relevant:
- COVID-19 early signal (Dec 2019): Unusual pneumonia cluster in Wuhan, market disinfection, 
  healthcare workers sick, atypical presentation in young adults
- Ebola West Africa (2013): Hemorrhagic fever in Guinea forest region, high case fatality, 
  rapid spread to urban areas
- H5N1 2022-2024: Poultry die-offs preceding human cases, farm worker exposure, 
  progression from birds → mammals → isolated human cases
- Mpox 2022: Community spread outside endemic regions, unusual demographics, 
  skin lesion cluster reports preceding official confirmation
- MERS: Camel exposure in Arabian Peninsula, healthcare facility amplification
"""


async def generate_intelligence_brief(
    assessment: FusedAssessment,
    api_key: Optional[str] = None,
) -> Optional[str]:
    """
    Generate a plain-English intelligence brief for a given assessment.
    Returns the brief text, or None if generation fails or threshold not met.
    """
    if assessment.confidence < BRIEF_THRESHOLD:
        return None

    key = api_key or os.getenv("ANTHROPIC_API_KEY", "")
    if not key:
        log.warning("[llm_brief] ANTHROPIC_API_KEY not set — skipping brief generation")
        return None

    # Build context from assessment
    risk_label = {1: "MINIMAL", 2: "LOW", 3: "MODERATE", 4: "HIGH", 5: "CRITICAL"}.get(
        int(assessment.risk_level), "UNKNOWN"
    )

    sources_str = ", ".join(
        s.source_id.upper() for s in assessment.contributing_sources
    )

    key_signals_str = "\n".join(f"- {s}" for s in assessment.key_signals[:5])

    prompt = f"""You are a biosecurity intelligence analyst producing decision-grade assessments.

Generate a concise intelligence brief (3-5 sentences) for the following signal:

ASSESSMENT DATA:
- Disease/Pathogen: {assessment.primary_disease}
- Location: {assessment.primary_location.place_name}
- Risk Level: {risk_label}
- Fusion Confidence: {assessment.confidence:.0%}
- Sources: {sources_str}
- Spread Risk (14-day): {assessment.spread_risk_14d}
- Key Signals:
{key_signals_str}
- System Summary: {assessment.summary}

HISTORICAL PATTERNS FOR CONTEXT:
{HISTORICAL_PATTERNS}

Write a brief that:
1. Opens with the core finding in plain English (what, where, confidence level)
2. Explains WHY multiple sources converging matters (if applicable)
3. Notes any historical pattern similarity if relevant (COVID-19, Ebola, H5N1, etc.)
4. Ends with a specific, actionable recommendation for a biosecurity analyst

Use authoritative but accessible language. No bullet points. No headers.
Do not exceed 5 sentences. Write in present tense. Be direct."""

    try:
        async with httpx.AsyncClient(timeout=30) as client:
            resp = await client.post(
                "https://api.anthropic.com/v1/messages",
                headers={
                    "x-api-key": key,
                    "anthropic-version": "2023-06-01",
                    "content-type": "application/json",
                },
                json={
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 300,
                    "messages": [{"role": "user", "content": prompt}],
                },
            )
            resp.raise_for_status()
            data = resp.json()
            brief = data["content"][0]["text"].strip()
            log.info(
                f"[llm_brief] Generated brief for {assessment.primary_disease} "
                f"@ {assessment.primary_location.place_name}"
            )
            return brief

    except Exception as e:
        log.warning(f"[llm_brief] Brief generation failed: {e}")
        return None


async def enrich_assessments_with_briefs(
    assessments: list,
    api_key: Optional[str] = None,
) -> list:
    """
    Add LLM briefs to a list of assessments.
    Returns assessments with .intelligence_brief field populated.
    Only processes HIGH (4) and CRITICAL (5) assessments to limit API calls.
    """
    enriched = 0
    for assessment in assessments:
        if int(assessment.risk_level) >= 4:
            brief = await generate_intelligence_brief(assessment, api_key)
            if brief:
                # Store brief in key_signals for display (no schema change needed)
                assessment.key_signals.insert(0, f"📋 INTELLIGENCE BRIEF: {brief}")
                enriched += 1

    if enriched:
        log.info(f"[llm_brief] Generated {enriched} intelligence briefs")
    return assessments
