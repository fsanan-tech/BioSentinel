"""
REPLACEMENT SOURCES SECTION for config.py
Copy this over the SOURCES dict in your existing config.py
(lines starting with SOURCES = { ... through the closing })
"""

SOURCES = {
    # CDC Emerging Infectious Diseases Journal — active RSS, no auth required
    "cdc_eid": {
        "enabled": True,
        "url": "https://wwwnc.cdc.gov/eid/rss/ahead-of-print.xml",
        "credibility_weight": 0.90,
        "type": "rss",
    },
    # CDC Health Matters / Public Health Matters blog RSS
    "cdc_news": {
        "enabled": True,
        "url": "https://tools.cdc.gov/api/v2/resources/media/403372.rss",
        "credibility_weight": 0.88,
        "type": "rss",
    },
    # WHO Disease Outbreak News — scraped directly (RSS removed in 2023 redesign)
    "who_don": {
        "enabled": True,
        "url": "https://www.who.int/emergencies/disease-outbreak-news",
        "credibility_weight": 0.95,
        "type": "who_scrape",
    },
    # PAHO (Pan American Health Organization) epidemiological alerts RSS
    "paho": {
        "enabled": True,
        "url": "https://www.paho.org/en/rss.xml",
        "credibility_weight": 0.88,
        "type": "rss",
    },
    # ECDC (European Centre for Disease Prevention and Control) news RSS
    "ecdc": {
        "enabled": True,
        "url": "https://www.ecdc.europa.eu/en/rss.xml",
        "credibility_weight": 0.90,
        "type": "rss",
    },
    # GDELT global news stream — disease/epidemic filtered
    "gdelt": {
        "enabled": True,
        "url": "https://api.gdeltproject.org/api/v2/doc/doc",
        "credibility_weight": 0.55,
        "type": "gdelt_api",
        "query": "disease outbreak epidemic pandemic OR biological",
        "max_records": 50,
    },
    # Healthmap JSON API — free, no key needed for basic access
    "healthmap": {
        "enabled": True,
        "url": "https://healthmap.org/getAll.php",
        "credibility_weight": 0.72,
        "type": "healthmap_api",
    },
}
