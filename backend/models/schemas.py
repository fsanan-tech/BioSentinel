"""
BioSentinel Data Schemas
Core data models flowing through the pipeline:
  RawSignal → ProcessedEvent → FusedAssessment
"""
from __future__ import annotations
from datetime import datetime
from typing import Optional, List, Dict, Any
from pydantic import BaseModel, Field
from enum import IntEnum


class RiskLevel(IntEnum):
    MINIMAL  = 1
    LOW      = 2
    MODERATE = 3
    HIGH     = 4
    CRITICAL = 5


# ─────────────────────────────────────────────────────
# 1. RAW SIGNAL — straight from ingestion layer
# ─────────────────────────────────────────────────────
class RawSignal(BaseModel):
    """Unprocessed signal from any ingestion source."""
    source_id: str                          # e.g. "promed", "who_don"
    source_url: str
    external_id: str                        # Original article/post ID
    title: str
    body: str
    published_at: datetime
    ingested_at: datetime = Field(default_factory=datetime.utcnow)
    raw_metadata: Dict[str, Any] = Field(default_factory=dict)
    credibility_weight: float = 0.5        # From config


# ─────────────────────────────────────────────────────
# 2. PROCESSED EVENT — after NLP and geocoding
# ─────────────────────────────────────────────────────
class GeoLocation(BaseModel):
    place_name: str
    country: Optional[str] = None
    admin1: Optional[str] = None           # State / province
    latitude: Optional[float] = None
    longitude: Optional[float] = None
    geo_confidence: float = 0.0            # 0-1 geocoding confidence


class ExtractedEntities(BaseModel):
    diseases: List[str] = Field(default_factory=list)
    pathogens: List[str] = Field(default_factory=list)
    locations: List[str] = Field(default_factory=list)
    case_count: Optional[int] = None
    death_count: Optional[int] = None
    is_high_consequence: bool = False
    has_pandemic_keywords: bool = False
    severity_keywords: List[str] = Field(default_factory=list)


class ProcessedEvent(BaseModel):
    """Signal after NLP extraction and geocoding."""
    event_id: str                           # UUID
    raw_signal: RawSignal
    entities: ExtractedEntities
    primary_location: Optional[GeoLocation] = None
    all_locations: List[GeoLocation] = Field(default_factory=list)
    primary_disease: Optional[str] = None
    event_severity_score: float = 0.0      # 0-1, pre-fusion
    processed_at: datetime = Field(default_factory=datetime.utcnow)


# ─────────────────────────────────────────────────────
# 3. FUSED ASSESSMENT — final intelligence product
# ─────────────────────────────────────────────────────
class SourceContribution(BaseModel):
    """How each source contributed to a fused assessment."""
    source_id: str
    credibility_weight: float
    event_ids: List[str]
    contribution_score: float


class ConfidenceBreakdown(BaseModel):
    """Explainable confidence scoring."""
    base_prior: float                       # Bayesian prior
    source_agreement: float                 # Multi-source bonus
    source_credibility: float               # Weighted credibility
    high_consequence_bonus: float           # HCP pathogen detected
    pandemic_keyword_bonus: float           # Pandemic language detected
    case_data_bonus: float                  # Numeric case data found
    restricted_data_uplift: float = 0.0    # From classified feeds
    final_confidence: float                 # Combined 0-1


class FusedAssessment(BaseModel):
    """
    Decision-grade biosecurity intelligence product.
    This is what gets rendered on the dashboard and served to clients.
    """
    assessment_id: str
    primary_disease: str
    primary_location: GeoLocation
    risk_level: RiskLevel
    confidence: float                       # 0-1
    confidence_breakdown: ConfidenceBreakdown

    # Summary
    headline: str
    summary: str
    key_signals: List[str] = Field(default_factory=list)

    # Event metadata
    source_count: int
    contributing_sources: List[SourceContribution]
    event_ids: List[str]
    earliest_signal: datetime
    latest_signal: datetime

    # Scenario projections (simple rule-based for MVP)
    spread_risk_14d: Optional[str] = None  # "LOW" / "MODERATE" / "HIGH"
    recommended_action: Optional[str] = None

    # Restricted data flag (never includes actual classified content)
    restricted_data_incorporated: bool = False

    created_at: datetime = Field(default_factory=datetime.utcnow)
    updated_at: datetime = Field(default_factory=datetime.utcnow)

    class Config:
        use_enum_values = True


# ─────────────────────────────────────────────────────
# 4. RESTRICTED DATA SIGNAL — governed interface
# Raw classified data NEVER enters this system.
# Only derived confidence scores and binary flags.
# ─────────────────────────────────────────────────────
class RestrictedSignal(BaseModel):
    """
    Derived confidence score from a classified/restricted source.
    This model deliberately excludes raw data fields.
    The originating system processes the data and returns
    only a scored signal + metadata.
    """
    signal_type: str                        # From ALLOWED_SIGNAL_TYPES
    region_code: str                        # ISO country or military grid zone
    confidence_score: float                 # 0-1 derived by the originating system
    anomaly_detected: bool
    pathogen_class_hint: Optional[str] = None   # e.g. "respiratory", "hemorrhagic"
    timestamp: datetime
    classification_level: str = "DERIVED"   # Always "DERIVED" — never raw
    source_reference: str = ""              # Opaque reference ID for audit trail


# ─────────────────────────────────────────────────────
# 5. API RESPONSE MODELS
# ─────────────────────────────────────────────────────
class DashboardSummary(BaseModel):
    total_signals_ingested: int
    total_assessments: int
    high_risk_count: int
    critical_count: int
    last_ingestion: Optional[datetime]
    sources_active: List[str]


class AssessmentsResponse(BaseModel):
    count: int
    assessments: List[FusedAssessment]


class SignalsResponse(BaseModel):
    count: int
    signals: List[ProcessedEvent]


class IngestionStatus(BaseModel):
    source: str
    status: str
    records_fetched: int
    last_run: Optional[datetime]
    error: Optional[str] = None
