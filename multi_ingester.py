"""
BioSentinel Ingesters — Fixed for 2025 API landscape
Replaces: backend/ingestion/multi_ingester.py

Changes vs original:
  - GDELT timeout raised to 30s, retry logic adjusted
  - ReliefWeb removed (API v1 deprecated, 410 Gone)
  - WHO DON now scraped via HTML (RSS removed in 2023)
  - HealthMap JSON API added
  - ECDC RSS added
  - PAHO RSS added
"""
import hashlib
import logging
import re
from datetime import datetime, timedelta
from typing import List

import httpx
from bs4 import BeautifulSoup
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal
from config import SOURCES

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────
# Generic RSS ingester (reusable for CDC, PAHO, ECDC)
# ─────────────────────────────────────────────
import feedparser


class GenericRSSIngester(BaseIngester):
    """Reusable RSS ingester for any feed."""

    def __init__(self, source_id: str, url: str, credibility: float):
        self.source_id = source_id
        self._url = url
        self.credibility_weight = credibility

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info(f"[{self.source_id}] Fetching RSS from {self._url}")
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(self._url)
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
                    published_at = datetime(*entry.published_parsed[:6])

                signal = RawSignal(
                    source_id=self.source_id,
                    source_url=link,
                    external_id=f"{self.source_id}_{external_id}",
                    title=title,
                    body=summary,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                )
                signals.append(signal)
            except Exception as e:
                log.warning(f"[{self.source_id}] Error parsing entry: {e}")

        log.info(f"[{self.source_id}] Got {len(signals)} entries")
        return signals


# ─────────────────────────────────────────────
# WHO Disease Outbreak News — HTML scraper
# (WHO removed their RSS feed in their 2023 redesign)
# ─────────────────────────────────────────────
class WHODonIngester(BaseIngester):
    source_id = "who_don"
    credibility_weight = 0.95

    WHO_DON_URL = "https://www.who.int/emergencies/disease-outbreak-news"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info(f"[who_don] Scraping WHO DON page")
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
        }
        async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
            resp = await client.get(self.WHO_DON_URL, headers=headers)
            resp.raise_for_status()

        soup = BeautifulSoup(resp.text, "html.parser")
        signals = []

        # WHO DON items are in list-type-DON or similar containers
        items = (
            soup.find_all("li", class_=re.compile(r"list-type-DON|don-item", re.I))
            or soup.find_all("div", class_=re.compile(r"sf-item|list-item", re.I))
            or []
        )

        # Fallback: find all links under /emergencies/disease-outbreak-news/item/
        if not items:
            links = soup.find_all("a", href=re.compile(r"/emergencies/disease-outbreak-news/item/"))
            for link_tag in links[:30]:
                try:
                    href = link_tag.get("href", "")
                    if not href:
                        continue
                    if href.startswith("/"):
                        href = "https://www.who.int" + href
                    title = link_tag.get_text(strip=True)
                    if not title or len(title) < 5:
                        continue

                    external_id = hashlib.sha256(href.encode()).hexdigest()[:16]
                    signal = RawSignal(
                        source_id=self.source_id,
                        source_url=href,
                        external_id=f"who_don_{external_id}",
                        title=title,
                        body=title,
                        published_at=datetime.utcnow(),
                        credibility_weight=self.credibility_weight,
                    )
                    signals.append(signal)
                except Exception as e:
                    log.warning(f"[who_don] Parse error: {e}")
        else:
            for item in items[:30]:
                try:
                    link_tag = item.find("a")
                    if not link_tag:
                        continue
                    href = link_tag.get("href", "")
                    if href.startswith("/"):
                        href = "https://www.who.int" + href
                    title = link_tag.get_text(strip=True)
                    external_id = hashlib.sha256(href.encode()).hexdigest()[:16]
                    signal = RawSignal(
                        source_id=self.source_id,
                        source_url=href,
                        external_id=f"who_don_{external_id}",
                        title=title,
                        body=title,
                        published_at=datetime.utcnow(),
                        credibility_weight=self.credibility_weight,
                    )
                    signals.append(signal)
                except Exception as e:
                    log.warning(f"[who_don] Parse error: {e}")

        log.info(f"[who_don] Scraped {len(signals)} DON items")
        return signals


# ─────────────────────────────────────────────
# GDELT — fixed with longer timeout
# ─────────────────────────────────────────────
class GDELTIngester(BaseIngester):
    source_id = "gdelt"
    credibility_weight = 0.55

    GDELT_URL = "https://api.gdeltproject.org/api/v2/doc/doc"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=20))
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
        # GDELT can be slow — 45s timeout
        async with httpx.AsyncClient(timeout=45) as client:
            resp = await client.get(self.GDELT_URL, params=params)
            resp.raise_for_status()

        data = resp.json()
        articles = data.get("articles", [])
        signals = []

        for article in articles:
            try:
                title = article.get("title", "")
                url = article.get("url", "")
                seendate = article.get("seendate", "")

                title_lower = title.lower()
                relevant = any(kw in title_lower for kw in [
                    "disease", "outbreak", "epidemic", "pandemic", "virus",
                    "infection", "pathogen", "ebola", "cholera", "plague",
                    "mpox", "dengue", "measles", "influenza", "flu", "h5n1",
                    "biological", "biosafety", "health emergency", "marburg",
                    "nipah", "novel", "unknown illness", "unexplained",
                ])
                if not relevant:
                    continue

                external_id = hashlib.sha256(url.encode()).hexdigest()[:16]
                published_at = datetime.utcnow()
                if seendate:
                    try:
                        published_at = datetime.strptime(seendate[:14], "%Y%m%dT%H%M%S")
                    except ValueError:
                        pass

                signal = RawSignal(
                    source_id=self.source_id,
                    source_url=url,
                    external_id=f"gdelt_{external_id}",
                    title=title,
                    body=title,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={
                        "domain": article.get("domain", ""),
                        "language": article.get("language", ""),
                        "sourcecountry": article.get("sourcecountry", ""),
                    },
                )
                signals.append(signal)
            except Exception as e:
                log.warning(f"[gdelt] Error: {e}")

        log.info(f"[gdelt] {len(signals)} relevant articles")
        return signals


# ─────────────────────────────────────────────
# HealthMap JSON API
# ─────────────────────────────────────────────
class HealthMapIngester(BaseIngester):
    source_id = "healthmap"
    credibility_weight = 0.72

    HEALTHMAP_URL = "https://healthmap.org/getAll.php"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[healthmap] Fetching from HealthMap API")
        params = {
            "admin1": "",
            "striphtml": "1",
            "languageId": "1",  # English
        }
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(self.HEALTHMAP_URL, params=params)
            resp.raise_for_status()

        try:
            data = resp.json()
        except Exception:
            # HealthMap sometimes returns JSONP — strip callback wrapper
            text = resp.text.strip()
            if text.startswith("mapData("):
                text = text[8:-1]
            import json
            data = json.loads(text)

        signals = []
        alerts = data if isinstance(data, list) else data.get("data", [])

        for alert in alerts[:100]:
            try:
                title = alert.get("summary", "") or alert.get("head", "")
                link = alert.get("link", "") or alert.get("url", "")
                place = alert.get("place", {})
                place_name = place.get("name", "") if isinstance(place, dict) else str(place)
                body = f"{title} Location: {place_name}"

                if not title:
                    continue

                external_id = hashlib.sha256((link or title).encode()).hexdigest()[:16]

                # Parse date if available
                published_at = datetime.utcnow()
                date_str = alert.get("date", "")
                if date_str:
                    try:
                        published_at = datetime.strptime(date_str[:10], "%Y-%m-%d")
                    except ValueError:
                        pass

                signal = RawSignal(
                    source_id=self.source_id,
                    source_url=link,
                    external_id=f"healthmap_{external_id}",
                    title=title,
                    body=body,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={
                        "place": place_name,
                        "lat": place.get("lat") if isinstance(place, dict) else None,
                        "lon": place.get("lng") if isinstance(place, dict) else None,
                    },
                )
                signals.append(signal)
            except Exception as e:
                log.warning(f"[healthmap] Error: {e}")

        log.info(f"[healthmap] {len(signals)} alerts")
        return signals


# ─────────────────────────────────────────────
# Convenience factory — builds all active ingesters
# ─────────────────────────────────────────────
def build_all_ingesters():
    """Returns list of all enabled ingester instances."""
    ingesters = [
        WHODonIngester(),
        GDELTIngester(),
        HealthMapIngester(),
        GenericRSSIngester(
            "cdc_eid",
            "https://wwwnc.cdc.gov/eid/rss/ahead-of-print.xml",
            0.90,
        ),
        GenericRSSIngester(
            "ecdc",
            "https://www.ecdc.europa.eu/en/rss.xml",
            0.88,
        ),
        GenericRSSIngester(
            "paho",
            "https://www.paho.org/en/rss.xml",
            0.85,
        ),
    ]
    return ingesters
