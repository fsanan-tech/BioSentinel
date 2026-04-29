# ⬡ BioSentinel
**Probabilistic Biosecurity Intelligence Fusion Platform**

## Overview
BioSentinel fuses global open-source disease signals into decision-grade
biosecurity intelligence assessments with explainable confidence scoring.

### Architecture
```
OSINT Sources → Ingestion Layer → NLP Processing → Fusion Engine → Dashboard
                                                  ↑
                              Restricted Data Interface (governed plug-in)
```

## Quick Start

### 1. Install dependencies
```bash
pip install -r requirements.txt
```

### 2. Configure
```bash
cp .env.example .env
# Edit .env as needed
```

### 3. Run
```bash
python run.py
```

Open browser: **http://localhost:8000**

## Data Sources (MVP)
| Source | Type | Credibility | Notes |
|--------|------|-------------|-------|
| ProMED-mail | RSS | 0.90 | Expert-curated since 1994 |
| WHO DON | RSS | 0.95 | Official outbreak notices |
| WHO News | RSS | 0.90 | General WHO health news |
| GDELT | API | 0.55 | Global news stream |
| ReliefWeb | API | 0.80 | UN OCHA humanitarian reports |

## API Endpoints
| Method | Path | Description |
|--------|------|-------------|
| GET | `/api/dashboard` | Summary statistics |
| GET | `/api/assessments` | Fused intelligence assessments |
| GET | `/api/signals` | Raw processed events |
| GET | `/api/ingest/status` | Source ingestion status |
| POST | `/api/ingest/run` | Trigger ingestion cycle |
| POST | `/api/restricted` | Submit restricted derived signal |

## Restricted Data Interface
The governed plug-in layer accepts **derived confidence scores only** —
never raw classified data. Submit via `POST /api/restricted` with an
`X-API-Key` header.

Supported signal types:
- `wastewater_pathogen_detection`
- `syndromic_surveillance_spike`
- `military_medical_anomaly`
- `lab_confirmed_novel_pathogen`
- `bioenv_sensor_alert`

Example:
```json
{
  "signal_type": "wastewater_pathogen_detection",
  "region_code": "NG",
  "confidence_score": 0.82,
  "anomaly_detected": true,
  "pathogen_class_hint": "respiratory",
  "source_reference": "LAB-REF-2024-001"
}
```

## Next Steps
- Add HealthMap API integration
- Add wastewater surveillance feeds (biobot.io, CDC NWSS)
- Add airline/mobility data for spread modeling
- Integrate LLM-backed NLP (Claude API) for richer extraction
- Add historical baseline modeling for anomaly detection
- Deploy to cloud (Railway/AWS) with auth layer
