"""
BioSentinel Database Layer
SQLite-backed persistence for signals and assessments.
Uses SQLAlchemy async for non-blocking I/O.
"""
import json
import uuid
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional

from sqlalchemy import (
    Column, String, Float, Integer, Boolean,
    DateTime, Text, create_engine, select, desc
)
from sqlalchemy.ext.asyncio import create_async_engine, AsyncSession, async_sessionmaker
from sqlalchemy.orm import DeclarativeBase

log = logging.getLogger(__name__)


class Base(DeclarativeBase):
    pass


class RawSignalRecord(Base):
    __tablename__ = "raw_signals"
    id            = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    source_id     = Column(String, index=True)
    external_id   = Column(String, unique=True, index=True)
    title         = Column(Text)
    body          = Column(Text)
    published_at  = Column(DateTime)
    ingested_at   = Column(DateTime, default=datetime.utcnow)
    raw_json      = Column(Text)   # Full serialized RawSignal


class ProcessedEventRecord(Base):
    __tablename__ = "processed_events"
    id                = Column(String, primary_key=True)
    raw_signal_id     = Column(String, index=True)
    source_id         = Column(String, index=True)
    primary_disease   = Column(String, index=True)
    primary_location  = Column(String)          # place_name
    latitude          = Column(Float)
    longitude         = Column(Float)
    severity_score    = Column(Float)
    is_high_consequence = Column(Boolean, default=False)
    processed_at      = Column(DateTime, default=datetime.utcnow)
    full_json         = Column(Text)            # Full serialized ProcessedEvent


class AssessmentRecord(Base):
    __tablename__ = "assessments"
    id                 = Column(String, primary_key=True)
    primary_disease    = Column(String, index=True)
    location_name      = Column(String)
    latitude           = Column(Float)
    longitude          = Column(Float)
    risk_level         = Column(Integer, index=True)
    confidence         = Column(Float)
    headline           = Column(Text)
    source_count       = Column(Integer)
    earliest_signal    = Column(DateTime)
    latest_signal      = Column(DateTime)
    restricted_data    = Column(Boolean, default=False)
    created_at         = Column(DateTime, default=datetime.utcnow)
    updated_at         = Column(DateTime, default=datetime.utcnow)
    full_json          = Column(Text)            # Full FusedAssessment JSON


class IngestionLogRecord(Base):
    __tablename__ = "ingestion_log"
    id             = Column(Integer, primary_key=True, autoincrement=True)
    source         = Column(String, index=True)
    status         = Column(String)
    records_fetched = Column(Integer, default=0)
    run_at         = Column(DateTime, default=datetime.utcnow)
    error          = Column(Text)


# ─────────────────────────────────────────────
# Database singleton
# ─────────────────────────────────────────────
_engine = None
_async_session = None


def _db_url(path: str) -> str:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    return f"sqlite+aiosqlite:///{path}"


async def init_db(db_path: str = "data/biosentinel.db"):
    global _engine, _async_session
    _engine = create_async_engine(_db_url(db_path), echo=False)
    _async_session = async_sessionmaker(_engine, expire_on_commit=False)
    async with _engine.begin() as conn:
        # Import anomaly tables so they get created too
        from backend.analysis.anomaly_detector import SignalBaselineRecord, AnomalyLogRecord  # noqa
        await conn.run_sync(Base.metadata.create_all)
    log.info(f"Database initialized at {db_path}")


def get_session() -> AsyncSession:
    return _async_session()


# ─────────────────────────────────────────────
# CRUD Helpers
# ─────────────────────────────────────────────

async def upsert_raw_signal(signal_json: dict) -> bool:
    """Returns True if new, False if duplicate."""
    async with get_session() as s:
        existing = await s.execute(
            select(RawSignalRecord).where(
                RawSignalRecord.external_id == signal_json["external_id"]
            )
        )
        if existing.scalars().first():
            return False
        record = RawSignalRecord(
            id=str(uuid.uuid4()),
            source_id=signal_json["source_id"],
            external_id=signal_json["external_id"],
            title=signal_json["title"],
            body=signal_json.get("body", ""),
            published_at=datetime.fromisoformat(signal_json["published_at"])
                if isinstance(signal_json["published_at"], str)
                else signal_json["published_at"],
            raw_json=json.dumps(signal_json),
        )
        s.add(record)
        await s.commit()
        return True


async def save_processed_event(event_json: dict):
    async with get_session() as s:
        loc = event_json.get("primary_location") or {}
        record = ProcessedEventRecord(
            id=event_json["event_id"],
            raw_signal_id=event_json["raw_signal"]["external_id"],
            source_id=event_json["raw_signal"]["source_id"],
            primary_disease=event_json.get("primary_disease", ""),
            primary_location=loc.get("place_name", ""),
            latitude=loc.get("latitude"),
            longitude=loc.get("longitude"),
            severity_score=event_json.get("event_severity_score", 0),
            is_high_consequence=event_json["entities"].get("is_high_consequence", False),
            full_json=json.dumps(event_json),
        )
        s.add(record)
        await s.commit()


async def upsert_assessment(assessment_json: dict):
    async with get_session() as s:
        existing = await s.execute(
            select(AssessmentRecord).where(
                AssessmentRecord.id == assessment_json["assessment_id"]
            )
        )
        loc = assessment_json.get("primary_location", {})
        if existing.scalars().first():
            await s.execute(
                AssessmentRecord.__table__.update()
                .where(AssessmentRecord.id == assessment_json["assessment_id"])
                .values(
                    confidence=assessment_json["confidence"],
                    risk_level=assessment_json["risk_level"],
                    source_count=assessment_json["source_count"],
                    updated_at=datetime.utcnow(),
                    full_json=json.dumps(assessment_json),
                )
            )
        else:
            record = AssessmentRecord(
                id=assessment_json["assessment_id"],
                primary_disease=assessment_json["primary_disease"],
                location_name=loc.get("place_name", ""),
                latitude=loc.get("latitude"),
                longitude=loc.get("longitude"),
                risk_level=assessment_json["risk_level"],
                confidence=assessment_json["confidence"],
                headline=assessment_json["headline"],
                source_count=assessment_json["source_count"],
                earliest_signal=datetime.fromisoformat(assessment_json["earliest_signal"])
                    if isinstance(assessment_json["earliest_signal"], str)
                    else assessment_json["earliest_signal"],
                latest_signal=datetime.fromisoformat(assessment_json["latest_signal"])
                    if isinstance(assessment_json["latest_signal"], str)
                    else assessment_json["latest_signal"],
                restricted_data=assessment_json.get("restricted_data_incorporated", False),
                full_json=json.dumps(assessment_json),
            )
            s.add(record)
        await s.commit()


async def get_recent_assessments(limit: int = 100) -> List[dict]:
    async with get_session() as s:
        result = await s.execute(
            select(AssessmentRecord)
            .order_by(desc(AssessmentRecord.updated_at))
            .limit(limit)
        )
        return [json.loads(r.full_json) for r in result.scalars().all()]


async def get_recent_events(limit: int = 200) -> List[dict]:
    async with get_session() as s:
        result = await s.execute(
            select(ProcessedEventRecord)
            .order_by(desc(ProcessedEventRecord.processed_at))
            .limit(limit)
        )
        return [json.loads(r.full_json) for r in result.scalars().all()]


async def log_ingestion(source: str, status: str, count: int, error: str = None):
    async with get_session() as s:
        record = IngestionLogRecord(
            source=source,
            status=status,
            records_fetched=count,
            error=error,
        )
        s.add(record)
        await s.commit()


async def get_last_ingestion(source: str) -> Optional[dict]:
    async with get_session() as s:
        result = await s.execute(
            select(IngestionLogRecord)
            .where(IngestionLogRecord.source == source)
            .order_by(desc(IngestionLogRecord.run_at))
            .limit(1)
        )
        record = result.scalars().first()
        if not record:
            return None
        return {
            "source": record.source,
            "status": record.status,
            "records_fetched": record.records_fetched,
            "last_run": record.run_at.isoformat(),
            "error": record.error,
        }


async def get_dashboard_stats() -> dict:
    async with get_session() as s:
        total_signals = (await s.execute(
            select(ProcessedEventRecord).with_only_columns(
                ProcessedEventRecord.id
            )
        )).all()
        assessments = (await s.execute(select(AssessmentRecord))).scalars().all()
        high_risk = [a for a in assessments if a.risk_level >= 4]
        critical = [a for a in assessments if a.risk_level == 5]
        return {
            "total_signals_ingested": len(total_signals),
            "total_assessments": len(assessments),
            "high_risk_count": len(high_risk),
            "critical_count": len(critical),
        }
