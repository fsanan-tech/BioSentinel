"""
BioSentinel — Social & Behavioral OSINT Ingesters

These sources capture the PUBLIC BEHAVIORAL SIGNAL — what people are
searching for, posting about, and reading — which leads clinical
case reporting by days to weeks.

Sources:
  google_trends  — Symptom/disease search spike detection (pytrends)
                   Validated to detect outbreaks 7-10 days before CDC
                   No API key needed. Completely free.

  reddit_osint   — Disease outbreak posts from key subreddits
                   Free OAuth API. Requires REDDIT_CLIENT_ID +
                   REDDIT_CLIENT_SECRET env vars (free Reddit app).

  bluesky_osint  — Disease posts from Bluesky public social network
                   Open AT Protocol. No auth needed for public search.

  wikipedia_spike — Disease article edit frequency spike detection
                    Free MediaWiki API. No auth needed.
                    Edit spikes strongly correlate with outbreak events.

NOTE: pytrends is rate-limited by Google. The ingester runs at most
once per hour internally to avoid bans. On the 30-min scheduler cycle
it will skip if the last run was <50 minutes ago.
"""

import hashlib
import json
import logging
import os
import time
from datetime import datetime, timedelta
from typing import List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────────────
# 1. GOOGLE TRENDS — Symptom Search Spike Detection
#    Uses pytrends (unofficial Google Trends API, no key needed)
#    Monitors disease + symptom keywords globally and by country.
#    Flags when current 7-day interest is 1.5x+ the 90-day average.
#    Scientifically validated: leads CDC reporting by 7-10 days.
# ─────────────────────────────────────────────────────────────
_TRENDS_LAST_RUN: float = 0.0
_TRENDS_MIN_INTERVAL = 55 * 60  # 55 minutes between runs (avoid rate limit)

# Symptom keyword clusters — each cluster represents a disease signal
TREND_KEYWORD_CLUSTERS = [
    {
        "name": "hemorrhagic_fever",
        "keywords": ["hemorrhagic fever", "bleeding from eyes"],
        "credibility_boost": 0.15,
    },
    {
        "name": "novel_respiratory",
        "keywords": ["unknown pneumonia", "mystery illness"],
        "credibility_boost": 0.10,
    },
    {
        "name": "avian_influenza",
        "keywords": ["bird flu symptoms", "h5n1"],
        "credibility_boost": 0.12,
    },
    {
        "name": "mpox",
        "keywords": ["mpox symptoms", "monkeypox rash"],
        "credibility_boost": 0.10,
    },
]

# Only query Global to minimize rate limit exposure
# Country-level queries can be added once Global is stable
TREND_GEO_TARGETS = [
    ("", "Global"),
]

# Geographic centroids for signal mapping
GEO_CENTROIDS = {
    "": (20.0, 0.0),
    "US": (37.09, -95.71),
    "CN": (35.86, 104.20),
    "IN": (20.59, 78.96),
    "NG": (9.08, 8.68),
    "CD": (-4.04, 21.76),
    "BD": (23.69, 90.36),
    "PK": (30.38, 69.35),
    "ID": (-0.79, 113.92),
    "BR": (-14.24, -51.93),
}


class GoogleTrendsIngester(BaseIngester):
    """
    Monitors Google search interest for disease/symptom keywords.
    Flags spikes where current 7-day interest > 1.5x the 90-day average.
    This is the behavioral early warning layer — people search symptoms
    before they see a doctor, and before doctors report to CDC.
    """
    source_id = "google_trends"
    credibility_weight = 0.72

    SPIKE_THRESHOLD = 1.5   # Current must be 50% above baseline
    MIN_ABSOLUTE = 30       # Must be at least 30/100 interest score

    async def fetch_raw(self) -> List[RawSignal]:
        global _TRENDS_LAST_RUN

        # Rate limit: only run every 55 minutes
        elapsed = time.time() - _TRENDS_LAST_RUN
        if elapsed < _TRENDS_MIN_INTERVAL:
            remaining = int((_TRENDS_MIN_INTERVAL - elapsed) / 60)
            log.info(f"[google_trends] Skipping — next run in ~{remaining}m (rate limit)")
            return []

        try:
            from pytrends.request import TrendReq
        except ImportError:
            log.warning("[google_trends] pytrends not installed. Run: pip install pytrends")
            return []

        log.info("[google_trends] Starting symptom spike scan")
        _TRENDS_LAST_RUN = time.time()

        # Fix: urllib3 v2 removed 'method_whitelist', use retries=0 to bypass
        pytrends = TrendReq(hl="en-US", tz=0, timeout=(10, 30), retries=0)
        signals = []

        for cluster in TREND_KEYWORD_CLUSTERS:
            keywords = cluster["keywords"][:3]  # Max 5 per request, use 3
            cluster_name = cluster["name"]

            for geo_code, geo_name in TREND_GEO_TARGETS:
                try:
                    # Get current 7-day interest
                    pytrends.build_payload(
                        keywords,
                        timeframe="now 7-d",
                        geo=geo_code,
                    )
                    current_df = pytrends.interest_over_time()

                    if current_df.empty:
                        continue

                    # Average the keywords' current interest
                    kw_cols = [k for k in keywords if k in current_df.columns]
                    if not kw_cols:
                        continue
                    current_avg = float(current_df[kw_cols].mean().mean())

                    # Get 90-day baseline
                    pytrends.build_payload(
                        keywords,
                        timeframe="today 3-m",
                        geo=geo_code,
                    )
                    baseline_df = pytrends.interest_over_time()

                    if baseline_df.empty:
                        continue

                    baseline_avg = float(baseline_df[kw_cols].mean().mean())

                    # Detect spike
                    if baseline_avg < 1:
                        continue

                    ratio = current_avg / baseline_avg
                    if ratio < self.SPIKE_THRESHOLD or current_avg < self.MIN_ABSOLUTE:
                        continue

                    # It's a spike — generate signal
                    log.info(
                        f"[google_trends] SPIKE: {cluster_name} in {geo_name} "
                        f"— {current_avg:.0f} vs baseline {baseline_avg:.0f} "
                        f"({ratio:.1f}x)"
                    )

                    lat, lon = GEO_CENTROIDS.get(geo_code, (20.0, 0.0))
                    keyword_str = ", ".join(keywords)
                    pct_above = int((ratio - 1) * 100)

                    title = (
                        f"[SEARCH SPIKE] {cluster_name.replace('_', ' ').title()} "
                        f"symptoms — {geo_name} ({pct_above}% above baseline)"
                    )
                    body = (
                        f"Google Trends symptom surveillance detected a {ratio:.1f}x spike "
                        f"in searches for '{keyword_str}' in {geo_name}. "
                        f"Current 7-day interest: {current_avg:.0f}/100. "
                        f"90-day baseline: {baseline_avg:.0f}/100. "
                        f"Search volume spikes for disease symptoms have been validated "
                        f"to precede clinical outbreak detection by 7-10 days. "
                        f"This is a behavioral early warning signal."
                    )

                    dedup_key = f"trends_{cluster_name}_{geo_code}_{datetime.utcnow().strftime('%Y%W')}"
                    external_id = hashlib.sha256(dedup_key.encode()).hexdigest()[:16]

                    signals.append(RawSignal(
                        source_id=self.source_id,
                        source_url=f"https://trends.google.com/trends/explore?q={keywords[0]}&geo={geo_code}",
                        external_id=f"trends_{external_id}",
                        title=title,
                        body=body,
                        published_at=datetime.utcnow(),
                        credibility_weight=self.credibility_weight + cluster.get("credibility_boost", 0),
                        raw_metadata={
                            "cluster": cluster_name,
                            "geo": geo_code,
                            "geo_name": geo_name,
                            "current_interest": current_avg,
                            "baseline_interest": baseline_avg,
                            "spike_ratio": ratio,
                            "lat": lat,
                            "lon": lon,
                        },
                    ))

                    # Longer delay to avoid Google rate limiting (429)
                    time.sleep(15)

                except Exception as e:
                    log.warning(f"[google_trends] Error for {cluster_name}/{geo_name}: {e}")
                    time.sleep(15)

        log.info(f"[google_trends] {len(signals)} symptom spikes detected")
        return signals


# ─────────────────────────────────────────────────────────────
# 2. REDDIT OSINT — Disease outbreak community surveillance
#    Monitors r/epidemiology, r/medicine, r/china, etc.
#    Requires free Reddit OAuth app credentials.
#    Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in .env
# ─────────────────────────────────────────────────────────────
REDDIT_SUBREDDITS = [
    "epidemiology",
    "medicine",
    "publichealth",
    "coronavirus",
    "china",          # Early COVID signal came from here Dec 2019
    "worldnews",
    "biology",
    "Futurology",
]

REDDIT_DISEASE_KEYWORDS = {
    "outbreak", "epidemic", "pandemic", "novel", "emerging",
    "hemorrhagic", "fever", "pneumonia", "cluster", "cases",
    "deaths", "quarantine", "containment", "ebola", "mpox",
    "influenza", "h5n1", "avian", "plague", "cholera", "measles",
    "unknown illness", "mystery disease", "unusual", "spreading",
    "mass illness", "mass casualty", "biological",
}


class RedditOSINTIngester(BaseIngester):
    """
    Scans disease-relevant subreddits for outbreak signals.
    Requires a free Reddit app: reddit.com/prefs/apps
    Set REDDIT_CLIENT_ID and REDDIT_CLIENT_SECRET in your .env file.
    """
    source_id = "reddit_osint"
    credibility_weight = 0.58

    TOKEN_URL = "https://www.reddit.com/api/v1/access_token"
    API_BASE = "https://oauth.reddit.com"

    def __init__(self):
        self.client_id = os.getenv("REDDIT_CLIENT_ID", "")
        self.client_secret = os.getenv("REDDIT_CLIENT_SECRET", "")
        self.enabled = bool(self.client_id and self.client_secret)
        self._token: Optional[str] = None
        self._token_expiry: float = 0

    async def _get_token(self) -> Optional[str]:
        if self._token and time.time() < self._token_expiry:
            return self._token
        try:
            async with httpx.AsyncClient(timeout=10) as client:
                resp = await client.post(
                    self.TOKEN_URL,
                    auth=(self.client_id, self.client_secret),
                    data={"grant_type": "client_credentials"},
                    headers={"User-Agent": "BioSentinel/0.1 (biosecurity research by /u/biosentinel_bot)"},
                )
                resp.raise_for_status()
                data = resp.json()
                self._token = data["access_token"]
                self._token_expiry = time.time() + data.get("expires_in", 3600) - 60
                return self._token
        except Exception as e:
            log.warning(f"[reddit_osint] Token fetch failed: {e}")
            return None

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=15))
    async def fetch_raw(self) -> List[RawSignal]:
        if not self.enabled:
            log.info("[reddit_osint] Skipping — REDDIT_CLIENT_ID/SECRET not set in .env")
            return []

        token = await self._get_token()
        if not token:
            return []

        headers = {
            "Authorization": f"Bearer {token}",
            "User-Agent": "BioSentinel/0.1 (biosecurity research)",
        }

        signals = []
        seen = set()
        cutoff = datetime.utcnow() - timedelta(hours=48)

        for subreddit in REDDIT_SUBREDDITS:
            try:
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(
                        f"{self.API_BASE}/r/{subreddit}/new.json",
                        headers=headers,
                        params={"limit": 25},
                    )
                    resp.raise_for_status()

                data = resp.json()
                posts = data.get("data", {}).get("children", [])

                for post in posts:
                    p = post.get("data", {})
                    title = p.get("title", "")
                    selftext = p.get("selftext", "")[:500]
                    url = f"https://reddit.com{p.get('permalink', '')}"
                    score = p.get("score", 0)
                    created_utc = p.get("created_utc", 0)

                    # Only posts from last 48h with some upvotes
                    post_time = datetime.utcfromtimestamp(created_utc)
                    if post_time < cutoff:
                        continue

                    # Filter for disease-relevant content
                    full_text = f"{title} {selftext}".lower()
                    matching_kw = [kw for kw in REDDIT_DISEASE_KEYWORDS if kw in full_text]
                    if not matching_kw:
                        continue

                    post_id = p.get("id", "")
                    if post_id in seen:
                        continue
                    seen.add(post_id)

                    # Higher score = more community validation
                    cred_boost = min(score / 5000, 0.12)  # Max +0.12 for viral posts

                    body = (
                        f"Reddit r/{subreddit}: {title}. "
                        f"{selftext} "
                        f"Keywords matched: {', '.join(matching_kw[:5])}. "
                        f"Post score: {score:,}. "
                        f"Community source — lower credibility but high volume signal."
                    )

                    signals.append(RawSignal(
                        source_id=self.source_id,
                        source_url=url,
                        external_id=f"reddit_{post_id}",
                        title=f"[REDDIT/{subreddit.upper()}] {title[:120]}",
                        body=body,
                        published_at=post_time,
                        credibility_weight=self.credibility_weight + cred_boost,
                        raw_metadata={
                            "subreddit": subreddit,
                            "score": score,
                            "keywords_matched": matching_kw,
                        },
                    ))

                await asyncio.sleep(1)  # Reddit rate limit respect

            except Exception as e:
                log.warning(f"[reddit_osint] Error for r/{subreddit}: {e}")

        log.info(f"[reddit_osint] {len(signals)} disease-relevant posts")
        return signals


# ─────────────────────────────────────────────────────────────
# 3. BLUESKY OSINT — Public social network health professional signal
#    Open AT Protocol. No auth needed for public search.
#    Health professionals, epidemiologists increasingly use Bluesky.
# ─────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────
# 3. MASTODON OSINT — Federated open social network
#    Replaces Bluesky (which now requires auth for search).
#    Mastodon.social has a fully public search API with no auth.
#    Strong health professional + science journalist community.
# ─────────────────────────────────────────────────────────────
MASTODON_SEARCH_TERMS = [
    "outbreak",
    "epidemic",
    "novel pathogen",
    "h5n1",
    "mpox",
    "hemorrhagic fever",
    "emerging disease",
]


class MastodonOSINTIngester(BaseIngester):
    """
    Searches Mastodon.social public posts for disease outbreak signals.
    No authentication required — fully open public API.
    Strong epidemiology/public health community on Mastodon.
    """
    source_id = "mastodon_osint"
    credibility_weight = 0.58

    SEARCH_URL = "https://mastodon.social/api/v2/search"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[mastodon_osint] Searching Mastodon for disease signals")
        signals = []
        seen = set()
        cutoff = datetime.utcnow() - timedelta(hours=48)

        for term in MASTODON_SEARCH_TERMS[:5]:
            try:
                params = {
                    "q": term,
                    "type": "statuses",
                    "limit": 10,
                    "resolve": "false",
                }
                headers = {
                    "User-Agent": "BioSentinel/0.1 (biosecurity research)",
                }
                async with httpx.AsyncClient(timeout=15) as client:
                    resp = await client.get(
                        self.SEARCH_URL, params=params, headers=headers
                    )
                    if resp.status_code in (401, 403, 422):
                        log.warning(f"[mastodon_osint] {resp.status_code} for '{term}' — skipping")
                        continue
                    resp.raise_for_status()

                data = resp.json()
                statuses = data.get("statuses", [])

                for status in statuses:
                    try:
                        import re as _re
                        content_raw = status.get("content", "")
                        content = _re.sub(r"<[^>]+>", " ", content_raw).strip()
                        post_id = status.get("id", "")
                        url = status.get("url", "")
                        account = status.get("account", {})
                        username = account.get("acct", "unknown")
                        created_at = status.get("created_at", "")

                        post_time = datetime.utcnow()
                        if created_at:
                            try:
                                post_time = datetime.fromisoformat(
                                    created_at.replace("Z", "+00:00")
                                ).replace(tzinfo=None)
                            except ValueError:
                                pass

                        if post_time < cutoff or not content:
                            continue
                        if post_id in seen:
                            continue
                        seen.add(post_id)

                        title = f"[MASTODON] {content[:120]}"
                        body = (
                            f"Mastodon post by @{username}: {content[:400]} "
                            f"Search term: '{term}'. "
                            f"Mastodon has a growing public health professional "
                            f"and science journalist community."
                        )

                        external_id = hashlib.sha256(
                            (post_id + content[:50]).encode()
                        ).hexdigest()[:16]
                        signals.append(RawSignal(
                            source_id=self.source_id,
                            source_url=url or "https://mastodon.social",
                            external_id=f"mastodon_{external_id}",
                            title=title,
                            body=body,
                            published_at=post_time,
                            credibility_weight=self.credibility_weight,
                            raw_metadata={"username": username, "term": term},
                        ))
                    except Exception as e:
                        log.warning(f"[mastodon_osint] Status parse error: {e}")

                await asyncio.sleep(1)

            except Exception as e:
                log.warning(f"[mastodon_osint] Search error for '{term}': {e}")

        log.info(f"[mastodon_osint] {len(signals)} Mastodon disease signals")
        return signals


# ─────────────────────────────────────────────────────────────
# 4. WIKIPEDIA EDIT SPIKE DETECTOR
#    Disease article edit frequency spikes before and during outbreaks.
#    This is a validated indirect signal — Wikipedia is rapidly updated
#    during health crises. Completely free, no auth.
# ─────────────────────────────────────────────────────────────

# Disease articles to monitor
WIKIPEDIA_DISEASE_ARTICLES = [
    "Ebola virus disease",
    "Marburg virus disease",
    "Mpox",
    "H5N1",
    "Avian influenza",
    "Nipah virus infection",
    "COVID-19",
    "Dengue fever",
    "Cholera",
    "Bubonic plague",
    "Lassa fever",
    "Rift Valley fever",
    "MERS",
    "Measles",
    "West Nile fever",
]


class WikipediaEditSpikeIngester(BaseIngester):
    """
    Monitors Wikipedia disease article edit frequency.
    Spike in edits = people updating outbreak information in real time.
    Validated indirect signal: edit velocity correlates with outbreak events.
    """
    source_id = "wikipedia_edits"
    credibility_weight = 0.55

    WIKI_API = "https://en.wikipedia.org/w/api.php"
    EDIT_SPIKE_THRESHOLD = 5  # Edits in last 24h to be considered a spike

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[wikipedia_edits] Checking disease article edit frequency")
        signals = []
        cutoff_24h = (datetime.utcnow() - timedelta(hours=24)).strftime("%Y%m%d%H%M%S")

        # Wikimedia requires a descriptive User-Agent to avoid 403
        wiki_headers = {
            "User-Agent": "BioSentinel/0.1 (biosecurity research tool; biosentinel@research.io)"
        }

        async with httpx.AsyncClient(timeout=15, headers=wiki_headers) as client:
            for article in WIKIPEDIA_DISEASE_ARTICLES:
                try:
                    params = {
                        "action": "query",
                        "format": "json",
                        "prop": "revisions",
                        "titles": article,
                        "rvprop": "timestamp|user|comment",
                        "rvlimit": 30,
                        "rvstart": datetime.utcnow().strftime("%Y%m%d%H%M%S"),
                        "rvend": cutoff_24h,
                        "rvdir": "older",
                    }

                    resp = await client.get(self.WIKI_API, params=params)
                    resp.raise_for_status()
                    data = resp.json()

                    pages = data.get("query", {}).get("pages", {})
                    for page_id, page in pages.items():
                        revisions = page.get("revisions", [])
                        edit_count = len(revisions)

                        if edit_count < self.EDIT_SPIKE_THRESHOLD:
                            continue

                        # Spike detected — check for meaningful edit comments
                        comments = [r.get("comment", "") for r in revisions[:5]]
                        editors = list(set(r.get("user", "") for r in revisions[:10]))

                        title = (
                            f"[WIKIPEDIA SPIKE] {article}: "
                            f"{edit_count} edits in 24h"
                        )
                        body = (
                            f"Wikipedia article '{article}' has received {edit_count} "
                            f"edits in the last 24 hours — above the spike threshold of "
                            f"{self.EDIT_SPIKE_THRESHOLD}. "
                            f"High edit velocity on disease articles correlates with "
                            f"active outbreak reporting. "
                            f"Recent editors: {', '.join(editors[:5])}. "
                            f"Edit comments suggest: {'; '.join(c for c in comments if c)[:200]}."
                        )

                        article_url = f"https://en.wikipedia.org/wiki/{article.replace(' ', '_')}"
                        external_id = hashlib.sha256(
                            f"wiki_{article}_{datetime.utcnow().strftime('%Y%m%d')}".encode()
                        ).hexdigest()[:16]

                        signals.append(RawSignal(
                            source_id=self.source_id,
                            source_url=article_url,
                            external_id=f"wiki_{external_id}",
                            title=title,
                            body=body,
                            published_at=datetime.utcnow(),
                            credibility_weight=self.credibility_weight,
                            raw_metadata={
                                "article": article,
                                "edit_count_24h": edit_count,
                                "editors": editors[:5],
                            },
                        ))

                    await asyncio.sleep(0.3)  # Wikimedia rate limit respect

                except Exception as e:
                    log.warning(f"[wikipedia_edits] Error for '{article}': {e}")

        log.info(f"[wikipedia_edits] {len(signals)} article edit spikes detected")
        return signals


# Need asyncio for sleep calls in ingesters
import asyncio
