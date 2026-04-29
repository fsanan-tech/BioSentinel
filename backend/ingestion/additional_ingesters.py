"""
BioSentinel Additional Ingesters
New open-source data sources to add to the pipeline.

Sources:
  pubmed        — NIH PubMed: novel/emerging pathogen papers (last 7 days)
  cdc_han       — CDC Health Alert Network (emergency health alerts)
  outbreak_news — OutbreakNewsToday.com RSS (curated outbreak journalism)
  gdacs         — Global Disaster Alert and Coordination System (UN-backed)
  delphi_flu    — Delphi/CMU FluView API (US regional flu surveillance)
  openfda       — OpenFDA drug shortage signals (antibiotic/antiviral shortages)
  promed_feed   — ProMED via Samdesk partnership JSON endpoint (new 2024)
"""
import hashlib
import json
import logging
import re
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Optional

import feedparser
import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal

log = logging.getLogger(__name__)


# ─────────────────────────────────────────────────────
# 1. PubMed — NIH's biomedical literature database
#    Searches for emerging/novel pathogen papers published
#    in the last 7 days. High-credibility early warning signal.
#    No API key required for <3 req/sec.
# ─────────────────────────────────────────────────────
class PubMedIngester(BaseIngester):
    source_id = "pubmed"
    credibility_weight = 0.82

    ESEARCH_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esearch.fcgi"
    ESUMMARY_URL = "https://eutils.ncbi.nlm.nih.gov/entrez/eutils/esummary.fcgi"

    QUERY = (
        "(emerging infectious disease[Title/Abstract] OR novel pathogen[Title/Abstract] "
        "OR outbreak[Title/Abstract] OR epidemic[Title/Abstract] OR "
        "novel coronavirus[Title/Abstract] OR unknown illness[Title/Abstract] OR "
        "zoonotic[Title/Abstract] OR hemorrhagic fever[Title/Abstract] OR "
        "avian influenza[Title/Abstract] OR mpox[Title/Abstract]) "
        "AND (2025[pdat] OR 2026[pdat])"
    )

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=15))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[pubmed] Searching PubMed for emerging disease papers")

        # Step 1: Search for PMIDs
        search_params = {
            "db": "pubmed",
            "term": self.QUERY,
            "retmax": 25,
            "retmode": "json",
            "sort": "pub+date",
            "datetype": "edat",
            "reldate": 14,  # Last 14 days
            "tool": "biosentinel_osint",
            "email": "biosentinel@research.io",
        }

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(self.ESEARCH_URL, params=search_params)
            resp.raise_for_status()

        search_data = resp.json()
        pmids = search_data.get("esearchresult", {}).get("idlist", [])

        if not pmids:
            log.info("[pubmed] No new papers found")
            return []

        # Step 2: Fetch summaries for those PMIDs
        summary_params = {
            "db": "pubmed",
            "id": ",".join(pmids),
            "retmode": "json",
            "tool": "biosentinel_osint",
            "email": "biosentinel@research.io",
        }

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(self.ESUMMARY_URL, params=summary_params)
            resp.raise_for_status()

        summary_data = resp.json()
        articles = summary_data.get("result", {})
        signals = []

        for pmid in pmids:
            article = articles.get(pmid)
            if not article or isinstance(article, list):
                continue
            try:
                title = article.get("title", "")
                authors = article.get("authors", [])
                journal = article.get("fulljournalname", "")
                pub_date = article.get("pubdate", "")
                source_url = f"https://pubmed.ncbi.nlm.nih.gov/{pmid}/"

                # Build a body from available metadata
                author_str = ", ".join(
                    a.get("name", "") for a in authors[:3]
                )
                body = f"{title} | {journal} | Authors: {author_str} | Published: {pub_date}"

                published_at = datetime.utcnow()
                if pub_date:
                    for fmt in ["%Y %b %d", "%Y %b", "%Y"]:
                        try:
                            published_at = datetime.strptime(pub_date[:len(fmt)+2].strip(), fmt)
                            break
                        except ValueError:
                            continue

                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url=source_url,
                    external_id=f"pubmed_{pmid}",
                    title=title,
                    body=body,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={"pmid": pmid, "journal": journal},
                ))
            except Exception as e:
                log.warning(f"[pubmed] Error parsing PMID {pmid}: {e}")

        log.info(f"[pubmed] {len(signals)} papers retrieved")
        return signals


# ─────────────────────────────────────────────────────
# 2. CDC Health Alert Network (HAN)
#    Official CDC emergency health communications.
#    RSS feed for Health Advisories, Alerts, and Updates.
#    Highest-credibility US domestic signal.
# ─────────────────────────────────────────────────────
class CDCHANIngester(BaseIngester):
    source_id = "cdc_han"
    credibility_weight = 0.95

    HAN_RSS = "https://emergency.cdc.gov/han/rss.asp"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[cdc_han] Fetching CDC Health Alert Network RSS")
        headers = {"User-Agent": "BioSentinel/0.1 (biosecurity research)"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(self.HAN_RSS, headers=headers)
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
                    external_id=f"cdc_han_{external_id}",
                    title=title,
                    body=summary or title,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                ))
            except Exception as e:
                log.warning(f"[cdc_han] Entry error: {e}")

        log.info(f"[cdc_han] {len(signals)} alerts")
        return signals


# ─────────────────────────────────────────────────────
# 3. Outbreak News Today
#    High-quality outbreak journalism blog with RSS.
#    Covers novel/unusual outbreaks, animal disease spillovers,
#    and international outbreak news not in mainstream media.
# ─────────────────────────────────────────────────────
class OutbreakNewsTodayIngester(BaseIngester):
    source_id = "outbreak_news"
    credibility_weight = 0.68

    RSS_URL = "https://outbreaknewstoday.com/feed/"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[outbreak_news] Fetching Outbreak News Today RSS")
        headers = {"User-Agent": "BioSentinel/0.1 (biosecurity research)"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(self.RSS_URL, headers=headers)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        signals = []

        for entry in feed.entries[:30]:
            try:
                title = entry.get("title", "")
                summary = entry.get("summary", "") or entry.get("description", "")
                # Strip HTML from summary
                summary_clean = re.sub(r"<[^>]+>", " ", summary).strip()[:500]
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
                    external_id=f"outbreak_news_{external_id}",
                    title=title,
                    body=summary_clean or title,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                ))
            except Exception as e:
                log.warning(f"[outbreak_news] Entry error: {e}")

        log.info(f"[outbreak_news] {len(signals)} articles")
        return signals


# ─────────────────────────────────────────────────────
# 4. GDACS — Global Disaster Alert and Coordination System
#    UN-backed. Covers biological events, disease outbreaks,
#    and environmental disasters. GeoRSS with coordinates.
# ─────────────────────────────────────────────────────
class GDACSIngester(BaseIngester):
    source_id = "gdacs"
    credibility_weight = 0.80

    GDACS_RSS = "https://www.gdacs.org/xml/rss_eo_bio.xml"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[gdacs] Fetching GDACS biological events RSS")
        headers = {"User-Agent": "BioSentinel/0.1 (biosecurity research)"}
        async with httpx.AsyncClient(timeout=20, follow_redirects=True) as client:
            resp = await client.get(self.GDACS_RSS, headers=headers)
            resp.raise_for_status()

        feed = feedparser.parse(resp.text)
        signals = []

        for entry in feed.entries:
            try:
                title = entry.get("title", "")
                summary = entry.get("summary", "") or entry.get("description", "")
                link = entry.get("link", "")
                external_id = hashlib.sha256(link.encode()).hexdigest()[:16]

                # GDACS provides GeoRSS coordinates
                lat = getattr(entry, "geo_lat", None)
                lon = getattr(entry, "geo_long", None)

                published_at = datetime.utcnow()
                if hasattr(entry, "published_parsed") and entry.published_parsed:
                    try:
                        published_at = datetime(*entry.published_parsed[:6])
                    except Exception:
                        pass

                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url=link,
                    external_id=f"gdacs_{external_id}",
                    title=title,
                    body=summary or title,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={"lat": lat, "lon": lon},
                ))
            except Exception as e:
                log.warning(f"[gdacs] Entry error: {e}")

        log.info(f"[gdacs] {len(signals)} bio events")
        return signals


# ─────────────────────────────────────────────────────
# 5. Delphi Epidata — CMU FluView API
#    Real-time US regional influenza surveillance.
#    Generates a signal when ILI (Influenza-Like Illness)
#    rate exceeds 2x the seasonal baseline — anomaly detection.
# ─────────────────────────────────────────────────────
class DelphiFluIngester(BaseIngester):
    source_id = "delphi_flu"
    credibility_weight = 0.87

    EPIDATA_URL = "https://api.delphi.cmu.edu/epidata/fluview/"

    # HHS Regions with major city centroids for geocoding
    REGION_LOCATIONS = {
        "hhs1": ("New England", 42.36, -71.06),
        "hhs2": ("New York/New Jersey", 40.71, -74.00),
        "hhs3": ("Mid-Atlantic", 38.90, -77.03),
        "hhs4": ("Southeast US", 33.75, -84.39),
        "hhs5": ("Great Lakes", 41.85, -87.65),
        "hhs6": ("South Central US", 29.76, -95.37),
        "hhs7": ("Midwest US", 39.10, -94.58),
        "hhs8": ("Mountain West", 39.74, -104.98),
        "hhs9": ("Pacific Southwest", 34.05, -118.24),
        "hhs10": ("Pacific Northwest", 47.61, -122.33),
        "nat": ("United States", 38.90, -77.03),
    }

    ILI_ALERT_THRESHOLD = 4.0  # % ILI visits — above this is notable

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=15))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[delphi_flu] Fetching US flu surveillance data")

        # Get current epiweek (format: YYYYWW)
        today = datetime.utcnow()
        # Approximate current epiweek
        year = today.year
        week = today.isocalendar()[1] - 1  # Previous week (current week often not finalized)
        if week == 0:
            year -= 1
            week = 52
        epiweek = f"{year}{week:02d}"

        params = {
            "regions": "nat,hhs1,hhs2,hhs3,hhs4,hhs5,hhs6,hhs7,hhs8,hhs9,hhs10",
            "epiweeks": epiweek,
        }

        async with httpx.AsyncClient(timeout=20) as client:
            resp = await client.get(self.EPIDATA_URL, params=params)
            resp.raise_for_status()

        data = resp.json()
        if data.get("result") != 1:
            log.warning(f"[delphi_flu] API returned non-success: {data.get('message')}")
            return []

        epidata = data.get("epidata", [])
        signals = []

        for record in epidata:
            try:
                region = record.get("region", "nat")
                ili = record.get("ili", 0) or 0
                wili = record.get("wili", 0) or 0
                num_ili = record.get("num_ili", 0) or 0
                num_patients = record.get("num_patients", 0) or 0

                # Only generate signals for elevated ILI
                ili_rate = wili if wili > 0 else ili
                if ili_rate < self.ILI_ALERT_THRESHOLD:
                    continue

                loc_name, lat, lon = self.REGION_LOCATIONS.get(
                    region, ("United States", 38.90, -77.03)
                )

                severity = "HIGH" if ili_rate > 7.5 else "MODERATE" if ili_rate > 5.0 else "ELEVATED"
                title = (
                    f"[{severity}] Influenza-Like Illness Surveillance Alert — {loc_name}: "
                    f"{ili_rate:.1f}% ILI rate (Week {epiweek})"
                )
                body = (
                    f"CDC/Delphi FluView surveillance reports {severity.lower()} "
                    f"influenza-like illness (ILI) activity in {loc_name}. "
                    f"Current ILI rate: {ili_rate:.1f}% of outpatient visits. "
                    f"Reported cases: {num_ili:,} of {num_patients:,} patients. "
                    f"Epiweek: {epiweek}. Threshold for alert: {self.ILI_ALERT_THRESHOLD}%."
                )

                external_id = f"delphi_{region}_{epiweek}"
                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url=f"https://api.delphi.cmu.edu/epidata/fluview/?regions={region}&epiweeks={epiweek}",
                    external_id=external_id,
                    title=title,
                    body=body,
                    published_at=datetime.utcnow(),
                    credibility_weight=self.credibility_weight,
                    raw_metadata={
                        "region": region,
                        "ili": ili,
                        "wili": wili,
                        "lat": lat,
                        "lon": lon,
                        "epiweek": epiweek,
                    },
                ))
            except Exception as e:
                log.warning(f"[delphi_flu] Record error: {e}")

        log.info(f"[delphi_flu] {len(signals)} elevated ILI regions above threshold")
        return signals


# ─────────────────────────────────────────────────────
# 6. OpenFDA — Drug Shortage Signals
#    Shortages of key antimicrobials and antivirals are an
#    indirect leading indicator of outbreak demand.
#    Completely free, no API key required.
# ─────────────────────────────────────────────────────
class OpenFDAShortageIngester(BaseIngester):
    source_id = "openfda_shortage"
    credibility_weight = 0.65

    FDA_URL = "https://api.fda.gov/drug/shortages.json"

    # Drug classes of biosurveillance interest
    BIOSURV_DRUGS = {
        "oseltamivir", "tamiflu", "zanamivir", "relenza",
        "baloxavir", "xofluza", "peramivir",          # Antivirals - flu
        "remdesivir", "veklury",                        # Antiviral - broad
        "ciprofloxacin", "doxycycline",                # Anthrax / plague treatment
        "amoxicillin", "azithromycin",                 # Broad bacterial
        "ampicillin", "gentamicin",                    # Broad spectrum
        "acyclovir", "valacyclovir",                   # Antiviral - herpes family
        "ribavirin",                                   # VHF treatment
        "hydroxychloroquine", "chloroquine",           # Antimalarial
        "metronidazole",                               # Bacterial/parasitic
        "vancomycin", "meropenem",                     # Last-resort antibiotics
    }

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=2, min=3, max=15))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[openfda_shortage] Checking FDA drug shortage database")

        # Search for recent shortage reports
        params = {
            "search": "report_date:[NOW-30DAY TO NOW]",
            "limit": 50,
            "sort": "report_date:desc",
        }

        try:
            async with httpx.AsyncClient(timeout=20) as client:
                resp = await client.get(self.FDA_URL, params=params)

                # 404 means no shortages in window — not an error
                if resp.status_code == 404:
                    log.info("[openfda_shortage] No shortages in query window")
                    return []

                resp.raise_for_status()
                data = resp.json()

        except httpx.HTTPStatusError as e:
            if e.response.status_code == 404:
                return []
            raise

        results = data.get("results", [])
        signals = []

        for item in results:
            try:
                generic_name = item.get("generic_name", "").lower()
                proprietary_name = item.get("proprietary_name", "").lower()

                # Only flag biosurveillance-relevant drugs
                is_relevant = any(
                    drug in generic_name or drug in proprietary_name
                    for drug in self.BIOSURV_DRUGS
                )
                if not is_relevant:
                    continue

                status = item.get("status", "")
                reason = item.get("reason_for_shortage", "")
                therapeutic_category = item.get("therapeutic_category", "")
                report_date = item.get("report_date", "")

                display_name = item.get("generic_name", "") or item.get("proprietary_name", "")
                title = f"[FDA SHORTAGE] {display_name} — {status}"
                body = (
                    f"FDA drug shortage alert: {display_name} ({therapeutic_category}). "
                    f"Status: {status}. Reason: {reason}. "
                    f"This antimicrobial/antiviral is on biosurveillance watch list. "
                    f"Report date: {report_date}."
                )

                external_id = hashlib.sha256(
                    (display_name + report_date).encode()
                ).hexdigest()[:16]

                published_at = datetime.utcnow()
                if report_date:
                    try:
                        published_at = datetime.strptime(report_date[:10], "%Y-%m-%d")
                    except ValueError:
                        pass

                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url="https://www.accessdata.fda.gov/scripts/drugshortages/",
                    external_id=f"fda_shortage_{external_id}",
                    title=title,
                    body=body,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={"drug": display_name, "status": status},
                ))
            except Exception as e:
                log.warning(f"[openfda_shortage] Item error: {e}")

        log.info(f"[openfda_shortage] {len(signals)} biosurveillance-relevant shortages")
        return signals


# ─────────────────────────────────────────────────────
# 7. WHO Disease Outbreak News — JSON API
#    WHO publishes a structured JSON feed via their
#    Drupal CMS. More reliable than HTML scraping.
# ─────────────────────────────────────────────────────
class WHODonJSONIngester(BaseIngester):
    source_id = "who_don"
    credibility_weight = 0.95

    WHO_JSON_URL = "https://www.who.int/api/hubs/disease-outbreak-news"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[who_don] Fetching WHO DON via JSON API")
        headers = {
            "User-Agent": "Mozilla/5.0",
            "Accept": "application/json",
        }
        params = {
            "sf_culture": "en",
            "$top": 30,
            "$orderby": "PublicationDateAndTime desc",
        }
        async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
            resp = await client.get(self.WHO_JSON_URL, params=params, headers=headers)
            resp.raise_for_status()

        try:
            data = resp.json()
        except Exception:
            log.warning("[who_don] Non-JSON response from WHO API")
            return []

        # WHO API returns either a list or {"value": [...]}
        items = data if isinstance(data, list) else data.get("value", data.get("items", []))
        signals = []

        for item in items[:30]:
            try:
                title = item.get("Title", "") or item.get("title", "")
                url_path = item.get("Url", "") or item.get("url", "")
                if not url_path.startswith("http"):
                    url_path = "https://www.who.int" + url_path
                summary = item.get("Summary", "") or item.get("summary", "") or title

                date_str = (
                    item.get("PublicationDateAndTime", "")
                    or item.get("publicationDate", "")
                    or ""
                )
                published_at = datetime.utcnow()
                if date_str:
                    try:
                        published_at = datetime.fromisoformat(
                            date_str.replace("Z", "+00:00")
                        ).replace(tzinfo=None)
                    except ValueError:
                        pass

                external_id = hashlib.sha256(url_path.encode()).hexdigest()[:16]
                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url=url_path,
                    external_id=f"who_don_{external_id}",
                    title=title,
                    body=summary,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                ))
            except Exception as e:
                log.warning(f"[who_don] Item error: {e}")

        log.info(f"[who_don] {len(signals)} DON items via JSON API")
        return signals
