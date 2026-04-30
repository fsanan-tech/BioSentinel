"""
BioSentinel Configuration
Railway-compatible: reads all settings from environment variables.
"""
import os
from dotenv import load_dotenv

load_dotenv()

APP_NAME = "BioSentinel"
APP_VERSION = "0.1.0"
DEBUG = os.getenv("BIOSENTINEL_DEBUG", "false").lower() == "true"

HOST = os.getenv("HOST", "0.0.0.0")
PORT = int(os.getenv("PORT", 8000))

# Railway provides a persistent volume at /data if configured,
# otherwise use a local path (works for dev and Railway ephemeral)
DB_PATH = os.getenv("DB_PATH", "data/biosentinel.db")

INGESTION_INTERVAL_MINUTES = int(os.getenv("INGESTION_INTERVAL", 30))

SOURCES = {
    "cdc_eid": {
        "enabled": True,
        "url": "https://wwwnc.cdc.gov/eid/rss/ahead-of-print.xml",
        "credibility_weight": 0.90,
        "type": "rss",
    },
    "cdc_han": {
        "enabled": True,
        "url": "https://emergency.cdc.gov/han/rss.asp",
        "credibility_weight": 0.95,
        "type": "rss",
    },
    "ecdc_epi": {
        "enabled": True,
        "url": "https://www.ecdc.europa.eu/en/taxonomy/term/1310/feed",
        "credibility_weight": 0.90,
        "type": "rss",
    },
    "ecdc_cdtr": {
        "enabled": True,
        "url": "https://www.ecdc.europa.eu/en/taxonomy/term/1505/feed",
        "credibility_weight": 0.88,
        "type": "rss",
    },
    "ecdc_risk": {
        "enabled": True,
        "url": "https://www.ecdc.europa.eu/en/taxonomy/term/1295/feed",
        "credibility_weight": 0.88,
        "type": "rss",
    },
    "gdelt": {
        "enabled": True,
        "url": "https://api.gdeltproject.org/api/v2/doc/doc",
        "credibility_weight": 0.55,
        "type": "gdelt_api",
        "query": "disease outbreak epidemic pandemic OR biological",
        "max_records": 50,
    },
}

RESTRICTED_INTERFACE = {
    "enabled": os.getenv("RESTRICTED_ENABLED", "false").lower() == "true",
    "endpoint": os.getenv("RESTRICTED_ENDPOINT", ""),
    "api_key_env": "RESTRICTED_API_KEY",
    "credibility_weight": 0.98,
    "allowed_signal_types": [
        "wastewater_pathogen_detection",
        "syndromic_surveillance_spike",
        "military_medical_anomaly",
        "lab_confirmed_novel_pathogen",
        "bioenv_sensor_alert",
    ],
    "output_mode": "derived_score",
}

FUSION = {
    "min_sources_for_assessment": 1,
    "min_confidence_threshold": 0.05,
    "spatial_merge_radius_km": 300,
    "temporal_merge_hours": 72,
    "prior_probability": 0.12,
    "source_confirmation_boost": 0.18,
}

RISK_LEVELS = {
    1: {"label": "MINIMAL",  "min_confidence": 0.00, "color": "#4ade80"},
    2: {"label": "LOW",      "min_confidence": 0.25, "color": "#facc15"},
    3: {"label": "MODERATE", "min_confidence": 0.45, "color": "#fb923c"},
    4: {"label": "HIGH",     "min_confidence": 0.65, "color": "#f87171"},
    5: {"label": "CRITICAL", "min_confidence": 0.85, "color": "#dc2626"},
}

HIGH_CONSEQUENCE_PATHOGENS = {
    "anthrax", "bacillus anthracis", "botulism", "clostridium botulinum",
    "plague", "yersinia pestis", "smallpox", "variola",
    "tularemia", "francisella tularensis",
    "viral hemorrhagic fever", "vhf",
    "ebola", "marburg", "lassa", "crimean-congo",
    "brucellosis", "brucella", "glanders", "burkholderia mallei",
    "melioidosis", "burkholderia pseudomallei",
    "q fever", "coxiella burnetii", "ricin",
    "mpox", "monkeypox", "nipah", "nipah virus", "hendra",
    "mers", "mers-cov", "sars", "sars-cov",
    "h5n1", "avian influenza", "bird flu",
    "novel coronavirus", "unknown respiratory",
    "unknown hemorrhagic", "unidentified pathogen",
}

PANDEMIC_KEYWORDS = {
    "novel", "unknown", "unidentified", "unprecedented",
    "rapid spread", "community transmission", "sustained human-to-human",
    "exponential", "unusual cluster", "unexplained deaths",
    "laboratory confirmed", "emerging", "re-emerging",
}

# Legacy source keys - kept for backward compatibility with old ingester files
# These ingesters are imported but not actively used in build_all_ingesters()
SOURCES["promed"] = {"enabled": False, "credibility_weight": 0.90}
SOURCES["who_don"] = {"enabled": False, "credibility_weight": 0.95}
SOURCES["who_news"] = {"enabled": False, "credibility_weight": 0.90}
SOURCES["cdc_outbreaks"] = {"enabled": False, "credibility_weight": 0.88}
SOURCES["reliefweb"] = {"enabled": False, "credibility_weight": 0.80}
