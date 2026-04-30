"""
BioSentinel FastAPI Application
REST API serving the fusion engine's outputs.

Routes:
  GET  /              → Health check
  GET  /api/dashboard → Summary statistics
  GET  /api/assessments → All fused assessments (GeoJSON-compatible)
  GET  /api/signals   → Recent processed events
  GET  /api/ingest/status → Source ingestion status
  POST /api/ingest/run    → Manually trigger ingestion cycle
  POST /api/restricted    → Submit restricted/derived signal (governed)
"""
import os
import logging
import asyncio
from datetime import datetime
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Header, BackgroundTasks
from fastapi.middleware.cors import CORSMiddleware
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from apscheduler.schedulers.asyncio import AsyncIOScheduler

from backend.models.schemas import (
    FusedAssessment, ProcessedEvent, DashboardSummary,
    RestrictedSignal,
)
from backend.ingestion.promed_ingester import PromedIngester
from backend.ingestion.who_ingester import WHODonIngester, WHONewsIngester
from backend.ingestion.multi_ingester import GDELTIngester, ReliefWebIngester
from backend.ingestion.restricted_interface import (
    RestrictedSignalSubmission, validate_restricted_submission,
    accept_restricted_signal, get_active_restricted_signals,
)
from backend.analysis.fusion_engine import fuse_events
from backend.analysis.anomaly_detector import (
    record_daily_signals, detect_anomalies, get_anomaly_stats,
)
from backend.database.db import (
    init_db, upsert_raw_signal, save_processed_event,
    upsert_assessment, get_recent_assessments, get_recent_events,
    log_ingestion, get_last_ingestion, get_dashboard_stats,
)
from config import (
    APP_NAME, APP_VERSION, INGESTION_INTERVAL_MINUTES,
    SOURCES, RESTRICTED_INTERFACE, DB_PATH,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# FastAPI App
# ─────────────────────────────────────────────
app = FastAPI(
    title=APP_NAME,
    version=APP_VERSION,
    description="Probabilistic biosecurity intelligence fusion platform",
)

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Serve frontend
frontend_dir = os.path.join(os.path.dirname(os.path.dirname(__file__)), "frontend")
if os.path.exists(frontend_dir):
    app.mount("/static", StaticFiles(directory=frontend_dir), name="static")


# ─────────────────────────────────────────────
# Ingestion Pipeline
# ─────────────────────────────────────────────
INGESTERS = [
    PromedIngester(),
    WHODonIngester(),
    WHONewsIngester(),
    GDELTIngester(),
    ReliefWebIngester(),
]


async def run_ingestion_cycle():
    """
    Full ingestion cycle:
      1. Fetch from all enabled sources
      2. Process (NLP + geocoding)
      3. Persist raw signals + processed events
      4. Run fusion engine → standard assessments
      5. Record daily baseline counts
      6. Run anomaly detector → anomaly assessments
      7. Persist all assessments
    """
    log.info("─── Ingestion cycle starting ───")
    all_events = []

    for ingester in INGESTERS:
        source_id = ingester.source_id
        try:
            events = await ingester.run()
            new_count = 0
            for event in events:
                is_new = await upsert_raw_signal(
                    event.raw_signal.model_dump(mode="json")
                )
                if is_new:
                    await save_processed_event(event.model_dump(mode="json"))
                    new_count += 1
                    all_events.append(event)
            await log_ingestion(source_id, "success", new_count)
        except Exception as e:
            log.error(f"Ingestion error for {source_id}: {e}")
            await log_ingestion(source_id, "error", 0, str(e))

    restricted_signals = get_active_restricted_signals()

    if all_events:
        log.info(f"Fusing {len(all_events)} new events...")

        # ── Standard fusion assessments ──────────────────────
        assessments = fuse_events(all_events, restricted_signals)
        for assessment in assessments:
            await upsert_assessment(assessment.model_dump(mode="json"))
        log.info(f"Generated {len(assessments)} fusion assessments")

        # ── Baseline accumulation ────────────────────────────
        await record_daily_signals(all_events)

        # ── Anomaly detection ────────────────────────────────
        anomaly_assessments = await detect_anomalies(all_events)
        for assessment in anomaly_assessments:
            await upsert_assessment(assessment.model_dump(mode="json"))
        if anomaly_assessments:
            log.info(f"⚠ {len(anomaly_assessments)} STATISTICAL ANOMALIES DETECTED")
    else:
        log.info("No new events this cycle — recording zero baseline entry")
        # Still record zero counts to maintain continuous baseline
        await record_daily_signals([])

    log.info("─── Ingestion cycle complete ───")


# ─────────────────────────────────────────────
# Lifecycle
# ─────────────────────────────────────────────
scheduler = AsyncIOScheduler()


@app.on_event("startup")
async def startup():
    await init_db(DB_PATH)
    # Schedule recurring ingestion
    scheduler.add_job(
        run_ingestion_cycle,
        "interval",
        minutes=INGESTION_INTERVAL_MINUTES,
        id="ingestion",
        replace_existing=True,
    )
    scheduler.start()
    # Run first cycle immediately (in background)
    asyncio.create_task(run_ingestion_cycle())
    log.info(f"{APP_NAME} v{APP_VERSION} started. "
             f"Ingestion every {INGESTION_INTERVAL_MINUTES} min.")


@app.on_event("shutdown")
async def shutdown():
    scheduler.shutdown()


# ─────────────────────────────────────────────
# Routes
# ─────────────────────────────────────────────

@app.get("/")
async def index():
    """Serve dashboard frontend."""
    index_path = os.path.join(frontend_dir, "index.html")
    if os.path.exists(index_path):
        return FileResponse(index_path)
    return {"name": APP_NAME, "version": APP_VERSION, "status": "running"}


@app.get("/health")
async def health():
    return {"status": "ok", "version": APP_VERSION, "time": datetime.utcnow().isoformat()}


@app.get("/api/dashboard")
async def dashboard():
    stats = await get_dashboard_stats()
    sources_active = [
        sid for sid, cfg in SOURCES.items() if cfg.get("enabled")
    ]
    return {
        **stats,
        "sources_active": sources_active,
        "last_ingestion": datetime.utcnow().isoformat(),
    }


@app.get("/api/assessments")
async def get_assessments(limit: int = 100, min_confidence: float = 0.0):
    """Return fused assessments, optionally filtered by confidence."""
    assessments = await get_recent_assessments(limit)
    if min_confidence > 0:
        assessments = [a for a in assessments
                       if a.get("confidence", 0) >= min_confidence]
    return {"count": len(assessments), "assessments": assessments}


@app.get("/api/signals")
async def get_signals(limit: int = 200):
    """Return recent processed events (pre-fusion)."""
    events = await get_recent_events(limit)
    return {"count": len(events), "events": events}


@app.get("/api/ingest/status")
async def ingestion_status():
    """Per-source ingestion status."""
    statuses = []
    for ingester in INGESTERS:
        last = await get_last_ingestion(ingester.source_id)
        statuses.append(last or {
            "source": ingester.source_id,
            "status": "never_run",
            "records_fetched": 0,
            "last_run": None,
            "error": None,
        })
    return {"sources": statuses}


@app.post("/api/ingest/run")
async def trigger_ingestion(background_tasks: BackgroundTasks):
    """Manually trigger an ingestion cycle."""
    background_tasks.add_task(run_ingestion_cycle)
    return {"status": "ingestion_triggered", "time": datetime.utcnow().isoformat()}


@app.post("/api/restricted", status_code=201)
async def submit_restricted_signal(
    submission: RestrictedSignalSubmission,
    x_api_key: Optional[str] = Header(None),
):
    """
    Governed endpoint for restricted/classified derived signals.

    Security:
    - Requires X-API-Key header matching RESTRICTED_API_KEY env var
    - Only accepts signal types in ALLOWED_SIGNAL_TYPES
    - Raw classified data is NEVER accepted — derived scores only

    Authentication: Bearer token via X-API-Key header.
    In production: add mTLS or PKI on top of this.
    """
    expected_key = os.getenv(RESTRICTED_INTERFACE.get("api_key_env", ""), "")
    if expected_key and x_api_key != expected_key:
        raise HTTPException(status_code=403, detail="Invalid API key")

    is_valid, reason = validate_restricted_submission(submission)
    if not is_valid:
        raise HTTPException(status_code=422, detail=reason)

    signal = accept_restricted_signal(submission)
    log.info(f"Restricted signal accepted: {signal.signal_type} / {signal.region_code}")

    return {
        "status": "accepted",
        "signal_type": signal.signal_type,
        "region_code": signal.region_code,
        "anomaly_detected": signal.anomaly_detected,
        "timestamp": signal.timestamp.isoformat(),
    }


@app.get("/api/restricted/status")
async def restricted_status():
    """Return count of active restricted signals (no content exposed)."""
    active = get_active_restricted_signals()
    return {
        "interface_enabled": RESTRICTED_INTERFACE.get("enabled", False),
        "active_signal_count": len(active),
        "signal_types": list(set(s.signal_type for s in active)),
    }


@app.get("/api/anomaly/stats")
async def anomaly_stats():
    """Return baseline accumulation progress and anomaly detection statistics."""
    return await get_anomaly_stats()

