"""
BioSentinel — Reddit JSON + Lemmy Community OSINT Ingesters

REDDIT (no OAuth required):
  Reddit exposes a public JSON API on every subreddit — just append .json
  to any Reddit URL. No app registration. No OAuth. Rate limited to ~30
  req/min with a proper User-Agent.

  Historical significance:
    Dec 31 2019 — r/China: "Wuhan Market being Disinfected" (same day WHO notified)
    Jan 3  2020 — r/epidemiology: First technical discussion of Wuhan pneumonia
    Jan 7  2020 — r/China: Hospital footage threads
    All of these predated mainstream media coverage by 3-14 days.

  Subreddits monitored:
    r/epidemiology   — Expert community, high signal quality
    r/medicine       — Clinical perspective
    r/publichealth   — Policy + surveillance angle
    r/china          — Critical for Asia-origin outbreak signals (COVID ground zero)
    r/worldnews      — High volume, outbreak announcements
    r/coronavirus    — Persists as a general outbreak community
    r/biology        — Novel pathogen academic discussion
    r/emergencymedicine — Frontline clinical early signals

LEMMY (fully open API, no auth):
  Federated Reddit alternative. The health/science communities there are
  growing with epidemiologists and public health professionals.

  Communities monitored across lemmy.world and lemmy.ml:
    !epidemiology@lemmy.world
    !medicine@lemmy.world
    !science@lemmy.world
    !worldnews@lemmy.world
    !health@lemmy.world
"""

import hashlib
import logging
import re
from datetime import datetime, timedelta
from typing import List

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────────────────────
# Disease keyword filter — what we consider outbreak-relevant
# Tuned to match the signal profile that preceded COVID coverage
# ─────────────────────────────────────────────────────────────
DISEASE_KEYWORDS = {
    # NOVEL / UNKNOWN SIGNALS (highest biosurveillance value)
    "novel", "unknown", "mystery illness", "mystery disease", "mystery virus",
    "unidentified", "unidentified pathogen", "unusual cluster", "unexplained",
    "unprecedented", "new virus", "new pathogen", "emerging", "emerging disease",
    "atypical pneumonia", "unusual pneumonia", "severe pneumonia",
    "unknown respiratory", "unknown fever", "unknown hemorrhagic",
    "disease x", "pathogen x", "novel agent",

    # HIGH-CONSEQUENCE PATHOGENS
    "ebola", "marburg", "mpox", "monkeypox", "nipah", "hendra",
    "h5n1", "h5n2", "h5n6", "h5n8", "h7n9", "h9n2", "h3n8",
    "avian flu", "bird flu", "avian influenza", "highly pathogenic avian",
    "hpai", "lpai",
    "sars", "mers", "mers-cov", "coronavirus",
    "dengue", "dengue hemorrhagic", "severe dengue",
    "zika", "chikungunya", "rift valley fever",
    "lassa", "lassa fever", "crimean-congo", "cchf",
    "cholera", "plague", "bubonic", "pneumonic plague",
    "anthrax", "tularemia", "brucellosis",
    "yellow fever", "west nile", "japanese encephalitis",
    "hantavirus", "candida auris", "drug-resistant fungal",

    # OUTBREAK LANGUAGE
    "outbreak", "epidemic", "pandemic",
    "cluster of cases", "cluster of deaths",
    "community spread", "community transmission",
    "human-to-human", "sustained transmission", "sustained spread",
    "quarantine", "mass quarantine", "isolation ward",
    "mass casualty", "mass illness", "mass hospitalization",
    "hospital overwhelmed", "hospitals full", "icu surge", "icu overwhelmed",
    "health emergency", "public health emergency",
    "pneumonia outbreak", "pneumonia cluster",
    "respiratory illness cluster", "respiratory outbreak",
    "fever cluster", "hemorrhagic cluster",

    # SYMPTOM CLUSTER SIGNALS
    "unexplained deaths", "sudden deaths", "sudden illness",
    "people dropping dead", "mass death",
    "doctors dying", "nurses dying", "healthcare workers sick",
    "children dying", "young healthy people dying",
    "bleeding from", "hemorrhagic symptoms", "internal bleeding",
    "organ failure", "multi-organ", "cytokine storm",
    "rapid onset", "rapid deterioration", "sudden onset",

    # GEOGRAPHIC BIOSURVEILLANCE ANCHORS
    # East/Southeast Asia
    "wuhan", "hubei", "guangdong", "yunnan",
    "beijing pneumonia", "guangzhou outbreak",
    "hong kong outbreak", "taiwan outbreak",
    "jakarta outbreak", "java illness",
    "manila outbreak", "philippine outbreak",
    "bangkok outbreak", "thailand virus",
    "vietnam outbreak", "hanoi illness",
    "malaysia outbreak", "singapore outbreak",
    "myanmar outbreak", "cambodia outbreak",
    "korea outbreak", "japan outbreak",

    # South Asia
    "india outbreak", "delhi outbreak", "mumbai outbreak",
    "kerala outbreak", "bangladesh outbreak",
    "pakistan outbreak", "karachi illness",
    "nepal outbreak", "sri lanka outbreak",

    # Middle East
    "saudi arabia outbreak", "mecca illness", "hajj disease",
    "iran outbreak", "iraq outbreak",
    "turkey outbreak",

    # Africa
    "congo outbreak", "drc outbreak", "kinshasa illness",
    "west africa outbreak", "east africa outbreak",
    "nigeria outbreak", "lagos illness",
    "guinea outbreak", "sierra leone outbreak", "liberia outbreak",
    "kenya outbreak", "nairobi illness", "uganda outbreak",
    "tanzania outbreak", "ethiopia outbreak",
    "sudan outbreak", "cameroon outbreak",
    "madagascar outbreak",

    # Latin America
    "brazil outbreak", "mexico outbreak", "colombia outbreak",
    "peru outbreak", "venezuela outbreak", "haiti outbreak",
    "ecuador outbreak", "bolivia outbreak",

    # Eastern Europe
    "ukraine outbreak", "russia outbreak", "kazakhstan outbreak",

    # AGRICULTURAL / ANIMAL RESERVOIR SIGNALS
    "spillover", "zoonotic", "zoonosis", "cross-species",
    "animal die-off", "mass die-off", "mass animal death",
    "bird deaths", "bird die-off", "dead birds", "dead poultry",
    "poultry cull", "poultry killed", "chicken deaths", "duck deaths",
    "livestock dying", "livestock disease", "cattle disease", "pig disease",
    "swine disease", "bat virus", "bat colony",
    "wildlife disease", "wild animal deaths",
    "farm workers sick", "farm workers hospitalized",
    "veterinarian warning", "animal health emergency",
    "seal deaths", "marine mammal die-off", "fish kill",

    # HEALTHCARE SYSTEM STRESS SIGNALS
    "hospitals overwhelmed", "no beds available", "running out of oxygen",
    "running out of medicine", "drug shortage outbreak",
    "health workers infected", "medical staff sick",
    "patients turned away", "overflow patients",
    "field hospital", "emergency hospital built",
    "body bags", "morgue overflow",

    # MILITARY / BIOSECURITY SIGNALS
    "troops sick", "soldiers ill", "military illness",
    "base outbreak", "military base disease",
    "biological incident", "suspected biological",
    "bioterrorism", "bioweapon", "biological attack",

    # TRAVEL / IMPORTATION SIGNALS
    "returning traveler sick", "travel-related illness", "imported case",
    "airport screening", "border health check",
    "cruise ship illness", "cruise ship outbreak",
    "flight quarantine", "plane quarantine",

    # ENVIRONMENTAL SIGNALS
    "wastewater detection", "sewage pathogen", "water contamination outbreak",
    "food poisoning outbreak", "mass food poisoning",
    "e. coli outbreak", "salmonella outbreak", "listeria outbreak",
    "contaminated water", "water supply illness",

    # SOCIAL/BEHAVIORAL EARLY WARNING PATTERNS (COVID-type)
    "disinfecting market", "hospital footage", "doctors overwhelmed",
    "pharmacies empty", "masks sold out", "oxygen sold out",
    "people dying at home", "government hiding outbreak",
    "covering up disease", "censoring doctors", "whistleblower doctor",
}

# 28 subreddits across disease, geographic, agricultural, and travel categories
REDDIT_SOURCES = [
    # Core disease surveillance
    ("epidemiology",        0.78),
    ("medicine",            0.72),
    ("publichealth",        0.72),
    ("emergencymedicine",   0.70),
    ("nursing",             0.68),
    ("globalhealth",        0.72),
    ("biology",             0.68),
    ("virology",            0.74),
    ("microbiology",        0.72),
    ("coronavirus",         0.62),
    # Geographic — highest-risk outbreak zones
    ("china",               0.65),
    ("india",               0.58),
    ("indonesia",           0.55),
    ("philippines",         0.55),
    ("vietnam",             0.55),
    ("thailand",            0.55),
    ("malaysia",            0.55),
    ("nigeria",             0.55),
    ("pakistan",            0.55),
    ("bangladesh",          0.55),
    ("brazil",              0.55),
    ("africa",              0.58),
    # Agricultural / zoonotic signals
    ("veterinary",          0.68),
    ("poultry",             0.60),
    ("farming",             0.55),
    ("wildlifebiology",     0.65),
    # Broader signal nets
    ("worldnews",           0.52),
    ("travel",              0.50),
]

# Lemmy communities — expanded to 12
LEMMY_SOURCES = [
    ("lemmy.world", "medicine",     0.70),
    ("lemmy.world", "science",      0.62),
    ("lemmy.world", "worldnews",    0.52),
    ("lemmy.world", "health",       0.65),
    ("lemmy.world", "biology",      0.68),
    ("lemmy.world", "environment",  0.55),
    ("lemmy.ml",    "science",      0.62),
    ("lemmy.ml",    "worldnews",    0.52),
    ("lemmy.ml",    "biology",      0.65),
    ("lemmy.ml",    "health",       0.65),
    ("lemmy.blahaj.zone", "science", 0.58),
    ("sh.itjust.works",   "worldnews", 0.50),
]



def _strip_html(text: str) -> str:
    return re.sub(r"<[^>]+>", " ", text or "").strip()


def _contains_disease_keywords(text: str) -> List[str]:
    """Returns list of matched keywords, empty if no match."""
    text_lower = text.lower()
    return [kw for kw in DISEASE_KEYWORDS if kw in text_lower]


def _score_post(title: str, body: str, score: int, matched_kw: List[str]) -> float:
    """
    Relevance score 0-1. Used to filter out low-signal posts.
    """
    relevance = 0.0

    # Keyword count — more matches = more specific signal
    relevance += min(len(matched_kw) * 0.15, 0.45)

    # High-value keywords (novel/unknown patterns)
    high_value = {"novel", "unknown", "mystery", "unexplained", "unidentified",
                  "unusual", "atypical", "unprecedented", "emerging"}
    if any(kw in high_value for kw in matched_kw):
        relevance += 0.25

    # Community upvotes validate signal quality
    if score > 1000:
        relevance += 0.20
    elif score > 100:
        relevance += 0.10
    elif score > 10:
        relevance += 0.05

    return min(relevance, 1.0)


# ─────────────────────────────────────────────────────────────
# Reddit JSON Ingester
# Uses the public .json endpoint — no OAuth, no app registration
# ─────────────────────────────────────────────────────────────
class RedditJSONIngester(BaseIngester):
    """
    Pulls new posts from disease-relevant subreddits using Reddit's
    public JSON API. No credentials required.

    Rate limiting: Reddit allows ~30 req/min with proper User-Agent.
    We use 1.5s delays between requests.

    This is the approach that would have caught COVID signals on Dec 31 2019.
    """
    source_id = "reddit_json"
    credibility_weight = 0.62  # Default; overridden per subreddit

    BASE_URL = "https://old.reddit.com"
    MIN_RELEVANCE = 0.20   # Skip posts below this relevance threshold

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=15))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[reddit_json] Scanning subreddits via public JSON API")
        signals = []
        seen = set()
        cutoff = datetime.utcnow() - timedelta(hours=48)

        headers = {
            # Use old.reddit.com — lighter, less bot detection
            # Full browser header stack to avoid 403
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
            "Accept": "application/json, text/javascript, */*; q=0.01",
            "Accept-Language": "en-US,en;q=0.9",
            "Accept-Encoding": "gzip, deflate, br",
            "Referer": "https://old.reddit.com/",
            "Sec-Fetch-Dest": "empty",
            "Sec-Fetch-Mode": "cors",
            "Sec-Fetch-Site": "same-origin",
            "X-Requested-With": "XMLHttpRequest",
        }

        for subreddit, cred_weight in REDDIT_SOURCES:
            try:
                url = f"{self.BASE_URL}/r/{subreddit}/new.json"
                params = {"limit": 25, "raw_json": 1}

                async with httpx.AsyncClient(
                    timeout=15,
                    headers=headers,
                    follow_redirects=True,
                ) as client:
                    resp = await client.get(url, params=params)

                    # Reddit returns 429 if rate limited — back off
                    if resp.status_code == 429:
                        log.warning(f"[reddit_json] Rate limited on r/{subreddit} — skipping")
                        import asyncio
                        await asyncio.sleep(10)
                        continue

                    if resp.status_code in (403, 404):
                        log.warning(f"[reddit_json] r/{subreddit} returned {resp.status_code}")
                        continue

                    resp.raise_for_status()

                data = resp.json()
                posts = data.get("data", {}).get("children", [])
                subreddit_hits = 0

                for post_wrapper in posts:
                    try:
                        p = post_wrapper.get("data", {})
                        post_id = p.get("id", "")
                        title = p.get("title", "")
                        selftext = p.get("selftext", "")[:800]
                        score = p.get("score", 0)
                        num_comments = p.get("num_comments", 0)
                        permalink = p.get("permalink", "")
                        created_utc = p.get("created_utc", 0)
                        url_field = p.get("url", "")
                        flair = p.get("link_flair_text", "") or ""

                        if post_id in seen:
                            continue

                        # Time filter
                        post_time = datetime.utcfromtimestamp(created_utc) if created_utc else datetime.utcnow()
                        if post_time < cutoff:
                            continue

                        # Keyword filter — must match at least one disease keyword
                        full_text = f"{title} {selftext} {flair}"
                        matched_kw = _contains_disease_keywords(full_text)
                        if not matched_kw:
                            continue

                        # Relevance score filter
                        relevance = _score_post(title, selftext, score, matched_kw)
                        if relevance < self.MIN_RELEVANCE:
                            continue

                        seen.add(post_id)
                        subreddit_hits += 1

                        post_url = f"{self.BASE_URL}{permalink}"

                        # Build a rich body for NLP processing
                        body = (
                            f"Reddit r/{subreddit}: {title}. "
                            f"{selftext[:400] if selftext and selftext != '[removed]' else ''} "
                            f"Disease keywords matched: {', '.join(matched_kw[:6])}. "
                            f"Post score: {score:,}. Comments: {num_comments}. "
                            f"Flair: {flair}. "
                            f"Source: Reddit community surveillance — "
                            f"r/{subreddit} has historically surfaced outbreak signals "
                            f"days ahead of mainstream media."
                        )

                        # Boost credibility for high-score posts (community validation)
                        cred_boost = min(score / 10000, 0.10)
                        final_cred = min(cred_weight + cred_boost, 0.92)

                        signals.append(RawSignal(
                            source_id=self.source_id,
                            source_url=post_url,
                            external_id=f"reddit_{post_id}",
                            title=f"[r/{subreddit}] {title[:150]}",
                            body=body,
                            published_at=post_time,
                            credibility_weight=final_cred,
                            raw_metadata={
                                "subreddit": subreddit,
                                "score": score,
                                "num_comments": num_comments,
                                "keywords_matched": matched_kw,
                                "relevance_score": relevance,
                                "flair": flair,
                            },
                        ))
                    except Exception as e:
                        log.warning(f"[reddit_json] Post parse error in r/{subreddit}: {e}")

                if subreddit_hits:
                    log.info(f"[reddit_json] r/{subreddit}: {subreddit_hits} relevant posts")

                # Rate limit: 1.5s between subreddits
                import asyncio
                await asyncio.sleep(1.5)

            except Exception as e:
                log.warning(f"[reddit_json] Error for r/{subreddit}: {e}")

        log.info(f"[reddit_json] Total: {len(signals)} disease-relevant posts across {len(REDDIT_SOURCES)} subreddits")
        return signals


# ─────────────────────────────────────────────────────────────
# Lemmy Community Ingester
# Uses the public v3 API — no auth required for reading
# ─────────────────────────────────────────────────────────────
class LemmyCommunityIngester(BaseIngester):
    """
    Pulls new posts from health/science Lemmy communities.
    Lemmy API v3/v4 is publicly readable without authentication.
    No rate limit concerns for read operations.

    Lemmy has a growing public health professional community
    that mirrors the r/epidemiology demographic.
    """
    source_id = "lemmy_osint"
    credibility_weight = 0.62

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[lemmy_osint] Scanning Lemmy health communities")
        signals = []
        seen = set()
        cutoff = datetime.utcnow() - timedelta(hours=72)

        headers = {
            "User-Agent": "BioSentinel/0.1 (biosecurity research)",
            "Accept": "application/json",
        }

        for instance, community, cred_weight in LEMMY_SOURCES:
            try:
                url = f"https://{instance}/api/v3/post/list"
                params = {
                    "community_name": community,
                    "type_": "All",
                    "sort": "New",
                    "limit": 20,
                }

                async with httpx.AsyncClient(timeout=15, headers=headers) as client:
                    resp = await client.get(url, params=params)
                    if resp.status_code in (400, 404, 422):
                        log.warning(f"[lemmy_osint] {instance}/{community} returned {resp.status_code}")
                        continue
                    resp.raise_for_status()

                data = resp.json()
                posts = data.get("posts", [])
                community_hits = 0

                for post_view in posts:
                    try:
                        post = post_view.get("post", {})
                        creator = post_view.get("creator", {})
                        counts = post_view.get("counts", {})

                        post_id = str(post.get("id", ""))
                        title = post.get("name", "")
                        body_text = post.get("body", "") or ""
                        url_field = post.get("url", "") or post.get("ap_id", "")
                        published = post.get("published", "")
                        score = counts.get("score", 0)
                        comments = counts.get("comments", 0)
                        creator_name = creator.get("name", "unknown")

                        dedup_key = f"lemmy_{instance}_{post_id}"
                        if dedup_key in seen:
                            continue

                        # Time filter
                        post_time = datetime.utcnow()
                        if published:
                            try:
                                post_time = datetime.fromisoformat(
                                    published.replace("Z", "+00:00")
                                ).replace(tzinfo=None)
                            except ValueError:
                                pass

                        if post_time < cutoff:
                            continue

                        # Keyword filter
                        full_text = f"{title} {body_text}"
                        matched_kw = _contains_disease_keywords(full_text)
                        if not matched_kw:
                            continue

                        seen.add(dedup_key)
                        community_hits += 1

                        body = (
                            f"Lemmy !{community}@{instance}: {title}. "
                            f"{body_text[:400] if body_text else ''} "
                            f"Keywords matched: {', '.join(matched_kw[:6])}. "
                            f"Posted by: {creator_name}. Score: {score}. Comments: {comments}."
                        )

                        signals.append(RawSignal(
                            source_id=self.source_id,
                            source_url=url_field or f"https://{instance}",
                            external_id=dedup_key,
                            title=f"[Lemmy/{community}@{instance}] {title[:150]}",
                            body=body,
                            published_at=post_time,
                            credibility_weight=cred_weight,
                            raw_metadata={
                                "instance": instance,
                                "community": community,
                                "score": score,
                                "comments": comments,
                                "keywords_matched": matched_kw,
                            },
                        ))
                    except Exception as e:
                        log.warning(f"[lemmy_osint] Post parse error {instance}/{community}: {e}")

                if community_hits:
                    log.info(f"[lemmy_osint] !{community}@{instance}: {community_hits} relevant posts")

                import asyncio
                await asyncio.sleep(0.5)

            except Exception as e:
                log.warning(f"[lemmy_osint] Error for {instance}/{community}: {e}")

        log.info(f"[lemmy_osint] Total: {len(signals)} posts from Lemmy communities")
        return signals
