"""
BioSentinel WHO Ingesters
Parses WHO Disease Outbreak News (DON) and WHO News RSS feeds.
WHO DON is gold-standard — official outbreak declarations.
"""
import hashlib
import logging
from datetime import datetime
from typing import List

import httpx
import feedparser
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal
from config import SOURCES

log = logging.getLogger(__name__)


class WHODonIngester(BaseIngester):
    source_id = "who_don"
    credibility_weight = SOURCES["who_don"]["credibility_weight"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        url = SOURCES["who_don"]["url"]
        log.info(f"[who_don] Fetching RSS from {url}")

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, follow_redirects=True)
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
                    external_id=f"who_don_{external_id}",
                    title=title,
                    body=summary,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                )
                signals.append(signal)
            except Exception as e:
                log.warning(f"[who_don] Error parsing entry: {e}")

        return signals


class WHONewsIngester(BaseIngester):
    source_id = "who_news"
    credibility_weight = SOURCES["who_news"]["credibility_weight"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        url = SOURCES["who_news"]["url"]
        log.info(f"[who_news] Fetching RSS from {url}")

        async with httpx.AsyncClient(timeout=15) as client:
            resp = await client.get(url, follow_redirects=True)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        signals = []

        for entry in feed.entries:
            try:
                title = entry.get("title", "")
                # Filter to health/disease relevant entries
                kw_hits = any(
                    kw in title.lower()
                    for kw in ["disease", "outbreak", "virus", "epidemic", "health",
                               "infection", "pathogen", "alert", "emergency"]
                )
                if not kw_hits:
                    continue

                summary = entry.get("summary", "") or entry.get("description", "")
                link = entry.get("link", "")
                external_id = hashlib.sha256(link.encode()).hexdigest()[:16]

                published_at = datetime.utcnow()
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    published_at = datetime(*entry.published_parsed[:6])

                signal = RawSignal(
                    source_id=self.source_id,
                    source_url=link,
                    external_id=f"who_news_{external_id}",
                    title=title,
                    body=summary,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                )
                signals.append(signal)
            except Exception as e:
                log.warning(f"[who_news] Error parsing entry: {e}")

        return signals
