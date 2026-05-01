"""
BioSentinel — Regional News RSS Ingesters

High-value regional media sources that surface outbreak signals
before international English-language media.

Historical significance:
  - Xinhua English reported Wuhan pneumonia on Dec 31, 2019
    (same day China notified WHO — before CNN/BBC)
  - Dawn Pakistan covered Middle East respiratory cases early
  - Jakarta Post reported H5N1 farm worker cases before WHO
  - AllAfrica aggregates 130+ African news sources

Sources (all free RSS, no auth required):
  xinhua_en     — Xinhua News Agency English (China state media)
  dawn_pk       — Dawn Pakistan (South Asia's largest English paper)
  the_hindu     — The Hindu India (authoritative South Asia)
  jakarta_post  — Jakarta Post (Indonesia, SE Asia)
  vanguard_ng   — Vanguard Nigeria (West Africa)
  allafrica     — AllAfrica.com health section (130+ African sources)
  paho_alerts   — PAHO (Pan American Health Org) epidemiological alerts
  who_searo     — WHO South-East Asia Regional Office
  reliefweb_hth — ReliefWeb health reports (Atom feed, back online)
  promed_digest — ProMED-AHEAD (new URL after 2023 reorganization)
  cidrap        — CIDRAP (Univ Minnesota disease reporting)
  flunewseurope — FluNewsEurope (ECDC/WHO European flu surveillance)
"""

import hashlib
import logging
import re
from datetime import datetime
from typing import List

import feedparser
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal

log = logging.getLogger(__name__)


class GenericHealthRSSIngester(BaseIngester):
    """
    Reusable RSS ingester with health-relevance keyword filtering.
    Only passes signals that contain disease/outbreak keywords.
    """

    HEALTH_KEYWORDS = {
        "disease", "outbreak", "virus", "infection", "epidemic", "pandemic",
        "pathogen", "illness", "fever", "pneumonia", "respiratory", "deaths",
        "cases", "health emergency", "quarantine", "vaccine", "treatment",
        "hospital", "patient", "symptoms", "ebola", "mpox", "influenza",
        "cholera", "plague", "dengue", "malaria", "tuberculosis", "hiv",
        "h5n1", "avian", "novel", "emerging", "outbreak", "spread",
        "cluster", "contamination", "poisoning", "epidemic", "mortality",
        "fatality", "casualties", "medical", "diagnosis", "surveillance",
    }

    def __init__(
        self,
        source_id: str,
        url: str,
        credibility: float,
        filter_keywords: bool = True,
        label: str = "",
    ):
        self.source_id = source_id
        self._url = url
        self.credibility_weight = credibility
        self._filter = filter_keywords
        self._label = label or source_id

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info(f"[{self.source_id}] Fetching {self._label} RSS")
        headers = {
            "User-Agent": "BioSentinel/0.1 (biosecurity research)",
            "Accept": "application/rss+xml, application/xml, text/xml, */*",
        }
        async with httpx.AsyncClient(
            timeout=20,
            follow_redirects=True,
            headers=headers,
        ) as client:
            resp = await client.get(self._url)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        signals = []

        for entry in feed.entries[:30]:
            try:
                title = entry.get("title", "")
                summary = entry.get("summary", "") or entry.get("description", "")
                # Strip HTML from summary
                summary_clean = re.sub(r"<[^>]+>", " ", summary).strip()[:600]
                link = entry.get("link", "")
                external_id = hashlib.sha256(link.encode()).hexdigest()[:16]

                # Health keyword filter
                if self._filter:
                    full_text = f"{title} {summary_clean}".lower()
                    if not any(kw in full_text for kw in self.HEALTH_KEYWORDS):
                        continue

                published_at = datetime.utcnow()
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        published_at = datetime(*entry.published_parsed[:6])
                    except Exception:
                        pass

                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url=link,
                    external_id=f"{self.source_id}_{external_id}",
                    title=title,
                    body=summary_clean or title,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                ))
            except Exception as e:
                log.warning(f"[{self.source_id}] Entry error: {e}")

        log.info(f"[{self.source_id}] {len(signals)} health-relevant articles")
        return signals


def build_regional_ingesters() -> List[BaseIngester]:
    """
    Returns all regional news RSS ingesters.
    Call this from multi_ingester.py build_all_ingesters().
    """
    return [
        # ── TIER 1: Official/Semi-official regional health ─────────────────────

        # PAHO — Pan American Health Organization epidemiological alerts
        GenericHealthRSSIngester(
            "paho_alerts",
            "https://www.paho.org/en/epidemiological-alerts-and-updates/rss.xml",
            credibility=0.90,
            filter_keywords=False,  # Already health-specific
            label="PAHO Epidemiological Alerts",
        ),

        # WHO SEARO — WHO South-East Asia Regional Office
        GenericHealthRSSIngester(
            "who_searo",
            "https://www.who.int/southeastasia/news/rss",
            credibility=0.88,
            filter_keywords=True,
            label="WHO SEARO",
        ),

        # CIDRAP — Center for Infectious Disease Research and Policy (Univ Minnesota)
        # Authoritative, peer-reviewed outbreak reporting
        GenericHealthRSSIngester(
            "cidrap",
            "https://www.cidrap.umn.edu/news-perspective/rss.xml",
            credibility=0.88,
            filter_keywords=False,
            label="CIDRAP",
        ),

        # FluNewsEurope — ECDC/WHO joint European flu surveillance bulletin
        GenericHealthRSSIngester(
            "flunews_eu",
            "https://flunewseurope.org/feed/",
            credibility=0.87,
            filter_keywords=False,
            label="FluNewsEurope",
        ),

        # ── TIER 2: High-value regional English-language media ─────────────────

        # Xinhua English Health — China state media, first to report domestic outbreaks
        GenericHealthRSSIngester(
            "xinhua_health",
            "https://english.news.cn/rss/health.xml",
            credibility=0.62,   # State media: factual but may downplay severity
            filter_keywords=True,
            label="Xinhua Health",
        ),

        # Dawn Pakistan — South Asia's largest English daily
        GenericHealthRSSIngester(
            "dawn_pk",
            "https://www.dawn.com/feeds/health",
            credibility=0.65,
            filter_keywords=True,
            label="Dawn Pakistan Health",
        ),

        # The Hindu — India's authoritative English paper
        GenericHealthRSSIngester(
            "the_hindu",
            "https://www.thehindu.com/sci-tech/health/feeder/default.rss",
            credibility=0.67,
            filter_keywords=True,
            label="The Hindu Health",
        ),

        # Jakarta Post — Indonesia, primary English SE Asia source
        GenericHealthRSSIngester(
            "jakarta_post",
            "https://www.thejakartapost.com/feed/health",
            credibility=0.63,
            filter_keywords=True,
            label="Jakarta Post Health",
        ),

        # Vanguard Nigeria — West Africa's major English daily
        GenericHealthRSSIngester(
            "vanguard_ng",
            "https://www.vanguardngr.com/category/health/feed/",
            credibility=0.60,
            filter_keywords=True,
            label="Vanguard Nigeria Health",
        ),

        # AllAfrica Health — aggregates 130+ African news sources
        GenericHealthRSSIngester(
            "allafrica",
            "https://allafrica.com/tools/headlines/rss/health/headlines.xml",
            credibility=0.58,
            filter_keywords=True,
            label="AllAfrica Health",
        ),

        # South China Morning Post — Hong Kong, regional Asia coverage
        GenericHealthRSSIngester(
            "scmp_health",
            "https://www.scmp.com/rss/91/feed",
            credibility=0.65,
            filter_keywords=True,
            label="SCMP Health",
        ),

        # ReliefWeb Health — UN OCHA via Atom (different endpoint from old v1 API)
        GenericHealthRSSIngester(
            "reliefweb_atom",
            "https://reliefweb.int/updates/rss.xml?primary_theme=Health",
            credibility=0.80,
            filter_keywords=False,
            label="ReliefWeb Health Atom",
        ),
    ]
