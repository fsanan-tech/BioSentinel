"""
BioSentinel Ingesters — v4 (+ NWSS Wastewater)

Source tiers:
  TIER 0 — Wastewater (4-7 day early warning lead)
    nwss_wval     — CDC NWSS Viral Activity Level (SARS-CoV-2, Flu A, RSV by state)
    nwss_mpox     — CDC NWSS Mpox wastewater detections
    nwss_emerging — CDC NWSS emerging pathogen detections (H5, measles, etc.)

  TIER 1 — Authoritative Official
    cdc_eid       — CDC Emerging Infectious Diseases journal
    cdc_han       — CDC Health Alert Network emergency alerts
    ecdc_epi      — ECDC Epidemiological Updates
    ecdc_cdtr     — ECDC Communicable Disease Threats Report
    ecdc_risk     — ECDC Risk Assessments

  TIER 2 — Scientific / Epidemiological
    pubmed        — NIH PubMed novel pathogen papers (14 day window)
    delphi_flu    — CMU FluView US regional flu surveillance
    openfda       — FDA drug shortage signals

  TIER 3 — Open OSINT
    outbreak_news — OutbreakNewsToday curated journalism
    gdacs         — GDACS UN biological event alerts
    gdelt         — GDELT global news stream
"""
import hashlib
import json
import logging
import re
from datetime import datetime
from typing import List

import feedparser
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal
from backend.ingestion.additional_ingesters import (
    PubMedIngester,
    CDCHANIngester,
    OutbreakNewsTodayIngester,
    GDACSIngester,
    DelphiFluIngester,
    OpenFDAShortageIngester,
    WHODonJSONIngester,
)
from backend.ingestion.nwss_ingester import (
    NWSSWastewaterIngester,
    NWSSMpoxIngester,
)
from backend.ingestion.social_ingesters import (
    GoogleTrendsIngester,
    RedditOSINTIngester,
    MastodonOSINTIngester,
    WikipediaEditSpikeIngester,
)
from backend.ingestion.community_ingesters import (
    RedditJSONIngester,
    LemmyCommunityIngester,
)

log = logging.getLogger(__name__)


class GenericRSSIngester(BaseIngester):
    def __init__(self, source_id: str, url: str, credibility: float):
        self.source_id = source_id
        self._url = url
        self.credibility_weight = credibility

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info(f"[{self.source_id}] Fetching RSS from {self._url}")
        headers = {"User-Agent": "BioSentinel/0.1 (biosecurity research)"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(self._url, headers=headers)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        signals = []
        for entry in feed.entries:
            try:
                title = entry.get("title", "")
                summary = entry.get("summary", "") or entry.get("description", "")
                link = entry.get("link", "")
                external_id = hashlib.sha256(link.encode()).hexdigest()[:16]

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
                    body=re.sub(r"<[^>]+>", " ", summary).strip() or title,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                ))
            except Exception as e:
                log.warning(f"[{self.source_id}] Entry error: {e}")

        log.info(f"[{self.source_id}] Got {len(signals)} entries")
        return signals


class GDELTIngester(BaseIngester):
    source_id = "gdelt"
    credibility_weight = 0.55

    GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=3, min=5, max=30))
    async def fetch_raw(self) -> List[RawSignal]:
        params = {
            "query": "disease outbreak epidemic pandemic OR biological",
            "mode": "artlist",
            "maxrecords": 50,
            "format": "json",
            "timespan": "1440",
            "sort": "DateDesc",
        }
        log.info("[gdelt] Fetching from GDELT API")
        async with httpx.AsyncClient(timeout=50) as client:
            resp = await client.get(self.GDELT_URL, params=params)
            resp.raise_for_status()

        body = resp.text.strip()
        if not body or body.startswith("<"):
            log.warning("[gdelt] Empty or non-JSON response")
            return []

        try:
            data = json.loads(body)
        except json.JSONDecodeError as e:
            log.warning(f"[gdelt] JSON error: {e}")
            return []

        KEYWORDS = {
            "disease", "outbreak", "epidemic", "pandemic", "virus", "infection",
            "pathogen", "ebola", "cholera", "plague", "mpox", "dengue", "measles",
            "influenza", "flu", "h5n1", "marburg", "nipah", "novel", "hemorrhagic",
        }

        signals = []
        for article in data.get("articles", []):
            try:
                title = article.get("title", "")
                url = article.get("url", "")
                if not any(kw in title.lower() for kw in KEYWORDS):
                    continue

                seendate = article.get("seendate", "")
                published_at = datetime.utcnow()
                if seendate:
                    try:
                        published_at = datetime.strptime(seendate[:14], "%Y%m%dT%H%M%S")
                    except ValueError:
                        pass

                external_id = hashlib.sha256(url.encode()).hexdigest()[:16]
                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url=url,
                    external_id=f"gdelt_{external_id}",
                    title=title,
                    body=title,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                ))
            except Exception as e:
                log.warning(f"[gdelt] Article error: {e}")

        log.info(f"[gdelt] {len(signals)} relevant articles")
        return signals


def build_all_ingesters():
    return [
        # ── TIER 0: Wastewater (4-7 day early warning lead) ──────────
        NWSSWastewaterIngester(),       # CDC NWSS viral activity levels
        NWSSMpoxIngester(),             # CDC NWSS mpox detections
        # NWSSEmergingIngester(),       # Disabled — dataset 2ew6-ywp6 returns 400

        # ── TIER 1: Authoritative Official ───────────────────────────
        CDCHANIngester(),               # CDC Health Alert Network
        GenericRSSIngester(
            "cdc_eid",
            "https://wwwnc.cdc.gov/eid/rss/ahead-of-print.xml",
            0.90,
        ),
        GenericRSSIngester(
            "ecdc_epi",
            "https://www.ecdc.europa.eu/en/taxonomy/term/1310/feed",
            0.90,
        ),
        GenericRSSIngester(
            "ecdc_cdtr",
            "https://www.ecdc.europa.eu/en/taxonomy/term/1505/feed",
            0.88,
        ),
        GenericRSSIngester(
            "ecdc_risk",
            "https://www.ecdc.europa.eu/en/taxonomy/term/1295/feed",
            0.88,
        ),

        # ── TIER 2: Scientific / Epidemiological ─────────────────────
        PubMedIngester(),               # NIH novel pathogen papers
        DelphiFluIngester(),            # CMU FluView US flu surveillance

        # ── TIER 3: Behavioral / Social OSINT ────────────────────────
        GoogleTrendsIngester(),         # Symptom search spikes (7-10d lead time)
        RedditOSINTIngester(),          # Reddit via OAuth (needs credentials)
        RedditJSONIngester(),           # Reddit via public JSON API (no auth needed)
        LemmyCommunityIngester(),       # Lemmy federated communities (open API)
        MastodonOSINTIngester(),        # Mastodon health professional signal
        WikipediaEditSpikeIngester(),   # Disease article edit frequency spikes

        # ── TIER 4: Open OSINT ───────────────────────────────────────
        OutbreakNewsTodayIngester(),
        GDACSIngester(),
        GDELTIngester(),
    ]
