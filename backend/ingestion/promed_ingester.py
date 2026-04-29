"""
BioSentinel ProMED Ingester
Parses the ProMED-mail RSS feed — one of the highest-fidelity
open-source disease outbreak feeds, expert-curated since 1994.
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


class PromedIngester(BaseIngester):
    source_id = "promed"
    credibility_weight = SOURCES["promed"]["credibility_weight"]

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        url = SOURCES["promed"]["url"]
        log.info(f"[promed] Fetching RSS from {url}")

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

                # Use link as external ID (stable)
                external_id = hashlib.sha256(link.encode()).hexdigest()[:16]

                # Parse date
                published_at = datetime.utcnow()
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    import time
                    published_at = datetime(*entry.published_parsed[:6])

                signal = RawSignal(
                    source_id=self.source_id,
                    source_url=link,
                    external_id=f"promed_{external_id}",
                    title=title,
                    body=summary,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={
                        "feed_title": feed.feed.get("title", ""),
                        "tags": [t.term for t in getattr(entry, "tags", [])],
                    },
                )
                signals.append(signal)
            except Exception as e:
                log.warning(f"[promed] Error parsing entry: {e}")

        return signals
