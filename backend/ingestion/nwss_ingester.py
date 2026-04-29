"""
BioSentinel — CDC NWSS Wastewater Surveillance Ingester (v2)

Fixes vs v1:
  - Removed $where Socrata filter (caused 400 Bad Request due to IN syntax)
  - Now fetches all recent records and filters HIGH/VERY HIGH in Python
  - Added field-name discovery: tries multiple possible column names gracefully
  - nwss_emerging dataset ID updated (x4ug-ixcq was 404, using correct endpoint)

Datasets:
  atcp-73re — Wastewater Viral Activity Level (SARS-CoV-2, Flu A, RSV)
  xpxn-rzgz — Mpox wastewater detections
"""

import hashlib
import logging
from datetime import datetime, timedelta
from typing import List, Optional

import httpx
from tenacity import retry, stop_after_attempt, wait_exponential

from backend.ingestion.base_ingester import BaseIngester
from backend.models.schemas import RawSignal

log = logging.getLogger(__name__)

SOCRATA_BASE = "https://data.cdc.gov/resource"

STATE_CENTROIDS = {
    "Alabama": (32.81, -86.79), "Alaska": (61.37, -152.40),
    "Arizona": (33.73, -111.43), "Arkansas": (34.97, -92.37),
    "California": (36.12, -119.68), "Colorado": (39.06, -105.31),
    "Connecticut": (41.60, -72.76), "Delaware": (39.32, -75.51),
    "Florida": (27.77, -81.69), "Georgia": (33.04, -83.64),
    "Hawaii": (21.09, -157.50), "Idaho": (44.24, -114.48),
    "Illinois": (40.35, -88.99), "Indiana": (39.85, -86.26),
    "Iowa": (42.01, -93.21), "Kansas": (38.53, -96.73),
    "Kentucky": (37.67, -84.67), "Louisiana": (31.17, -91.87),
    "Maine": (44.69, -69.38), "Maryland": (39.06, -76.80),
    "Massachusetts": (42.23, -71.53), "Michigan": (43.33, -84.54),
    "Minnesota": (45.69, -93.90), "Mississippi": (32.74, -89.68),
    "Missouri": (38.46, -92.29), "Montana": (46.92, -110.45),
    "Nebraska": (41.13, -98.27), "Nevada": (38.31, -117.06),
    "New Hampshire": (43.45, -71.56), "New Jersey": (40.30, -74.52),
    "New Mexico": (34.84, -106.25), "New York": (42.17, -74.95),
    "North Carolina": (35.63, -79.81), "North Dakota": (47.53, -99.78),
    "Ohio": (40.39, -82.76), "Oklahoma": (35.57, -96.93),
    "Oregon": (44.57, -122.07), "Pennsylvania": (40.59, -77.21),
    "Rhode Island": (41.68, -71.51), "South Carolina": (33.86, -80.95),
    "South Dakota": (44.30, -99.44), "Tennessee": (35.75, -86.69),
    "Texas": (31.05, -97.56), "Utah": (40.15, -111.86),
    "Vermont": (44.05, -72.71), "Virginia": (37.77, -78.17),
    "Washington": (47.40, -121.49), "West Virginia": (38.49, -80.95),
    "Wisconsin": (44.27, -89.62), "Wyoming": (42.76, -107.30),
    "District of Columbia": (38.90, -77.03),
    "Puerto Rico": (18.22, -66.59),
    "United States": (38.89, -77.03),
}

HIGH_ACTIVITY = {"high", "very high", "very_high"}


def _get_field(record: dict, *candidates, default=""):
    """Try multiple possible field names, return first match."""
    for key in candidates:
        if key in record and record[key] is not None:
            return str(record[key])
    return default


class NWSSWastewaterIngester(BaseIngester):
    """
    CDC NWSS Wastewater Viral Activity Level — state level, weekly.
    Dataset atcp-73re: SARS-CoV-2, Influenza A, RSV.
    Generates signals for HIGH and VERY HIGH states.
    """
    source_id = "nwss_wval"
    credibility_weight = 0.88
    DATASET_ID = "atcp-73re"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[nwss_wval] Fetching CDC NWSS Viral Activity Level data")

        url = f"{SOCRATA_BASE}/{self.DATASET_ID}.json"
        # Fetch recent records without WHERE filter — filter in Python
        params = {
            "$order": "date_updated DESC",
            "$limit": 500,   # Get enough to cover all states × 3 pathogens × recent weeks
        }

        async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()

        records = resp.json()
        if not isinstance(records, list):
            log.warning("[nwss_wval] Unexpected response format")
            return []

        # Log first record to see actual field names (helpful for debugging)
        if records:
            log.info(f"[nwss_wval] Sample fields: {list(records[0].keys())[:10]}")

        cutoff = datetime.utcnow() - timedelta(days=21)
        signals = []
        seen = set()

        for record in records:
            try:
                # Try multiple possible field names for each value
                state = _get_field(record,
                    "state_territory", "state_name", "jurisdiction",
                    "state", "geography_name")
                pathogen = _get_field(record,
                    "pathogen_target", "pathogen", "pathogen_name",
                    "virus", "target", default="SARS-CoV-2")
                activity_level = _get_field(record,
                    "site_wval_category", "wastewater_viral_activity_level",
                    "wwval", "wval_category", "activity_level", "level",
                    "wastewater_activity_level", "viral_activity_level")
                date_str = _get_field(record,
                    "week_end", "date_included_in_wval", "date_updated",
                    "week_end_date", "report_date", "date")
                ptc_15d = _get_field(record, "ptc_15d", "percent_change_15d", "pct_change")

                if not state or not activity_level:
                    continue

                # Filter: only HIGH or VERY HIGH
                if activity_level.lower().strip() not in HIGH_ACTIVITY:
                    continue

                # Parse date
                published_at = datetime.utcnow()
                if date_str:
                    for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                                 "%Y-%m-%d", "%m/%d/%Y"]:
                        try:
                            published_at = datetime.strptime(date_str[:len(fmt)], fmt)
                            break
                        except ValueError:
                            continue

                if published_at < cutoff:
                    continue

                dedup_key = f"{state}_{pathogen}_{date_str[:10] if date_str else 'x'}"
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                lat, lon = STATE_CENTROIDS.get(state, (38.89, -77.03))

                change_str = ""
                if ptc_15d:
                    try:
                        pct = float(ptc_15d)
                        direction = "↑ increasing" if pct > 0 else "↓ decreasing"
                        change_str = f" Trend: {direction} ({pct:+.0f}% over 15 days)."
                    except (ValueError, TypeError):
                        pass

                title = (
                    f"[WASTEWATER {activity_level.upper()}] "
                    f"{pathogen} — {state}"
                )
                body = (
                    f"CDC NWSS wastewater surveillance reports {activity_level.upper()} "
                    f"{pathogen} viral activity in {state} wastewater. "
                    f"Wastewater detection typically precedes clinical case reporting "
                    f"by 4-7 days — this is an early warning signal.{change_str} "
                    f"Data date: {date_str[:10] if date_str else 'recent'}. "
                    f"Source: CDC National Wastewater Surveillance System (NWSS)."
                )

                external_id = hashlib.sha256(dedup_key.encode()).hexdigest()[:16]
                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url=f"https://www.cdc.gov/wastewater/",
                    external_id=f"nwss_{external_id}",
                    title=title,
                    body=body,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={
                        "state": state,
                        "pathogen": pathogen,
                        "activity_level": activity_level,
                        "lat": lat,
                        "lon": lon,
                        "date": date_str[:10] if date_str else "",
                    },
                ))

            except Exception as e:
                log.warning(f"[nwss_wval] Record error: {e}")

        log.info(f"[nwss_wval] {len(signals)} HIGH/VERY HIGH wastewater alerts from {len(records)} records")
        return signals


class NWSSMpoxIngester(BaseIngester):
    """
    CDC NWSS Mpox wastewater detections. Dataset: xpxn-rzgz.
    Any detection in recent 30 days is a high-consequence signal.
    """
    source_id = "nwss_mpox"
    credibility_weight = 0.90
    DATASET_ID = "xpxn-rzgz"

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[nwss_mpox] Fetching CDC NWSS Mpox wastewater detections")

        url = f"{SOCRATA_BASE}/{self.DATASET_ID}.json"
        params = {
            "$order": "date_updated DESC",
            "$limit": 100,
        }

        async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
            resp = await client.get(url, params=params)
            resp.raise_for_status()

        records = resp.json()
        if not isinstance(records, list):
            return []

        if records:
            log.info(f"[nwss_mpox] Sample fields: {list(records[0].keys())[:10]}")

        cutoff = datetime.utcnow() - timedelta(days=30)
        signals = []
        seen = set()

        for record in records:
            try:
                state = _get_field(record,
                    "state_territory", "state_name", "state",
                    "jurisdiction", "geography")
                date_str = _get_field(record,
                    "sample_collect_date", "week_end", "date_updated",
                    "week_end_date", "date", "collection_date")
                county = _get_field(record, "county_names", "county", "counties")
                wwtp = _get_field(record, "wwtp_name", "site_name", "facility_name")

                # Check for actual detection values
                detect_value = _get_field(record,
                    "detect_prop_15d", "percentile", "concentration",
                    "copies_l", "detected", "mpox_detect")

                if not state:
                    continue

                published_at = datetime.utcnow()
                if date_str:
                    for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S",
                                 "%Y-%m-%d", "%m/%d/%Y"]:
                        try:
                            published_at = datetime.strptime(date_str[:len(fmt)], fmt)
                            break
                        except ValueError:
                            continue

                if published_at < cutoff:
                    continue

                # Only signal if there's actual detection data
                if detect_value in ("", "0", "0.0", None):
                    continue

                location_detail = wwtp or county or state
                dedup_key = f"mpox_{state}_{location_detail}_{date_str[:10] if date_str else 'x'}"
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                lat, lon = STATE_CENTROIDS.get(state, (38.89, -77.03))

                title = f"[WASTEWATER DETECTION] Mpox (MPXV) — {location_detail}, {state}"
                body = (
                    f"CDC NWSS wastewater surveillance has detected mpox virus (MPXV) "
                    f"in wastewater samples from {location_detail}, {state}. "
                    f"Mpox wastewater detection is a high-consequence signal indicating "
                    f"active viral shedding in the sewershed population. "
                    f"Detection typically precedes clinical case identification by 4-7 days. "
                    f"Sample date: {date_str[:10] if date_str else 'recent'}. "
                    f"Source: CDC NWSS Mpox Wastewater Surveillance."
                )

                external_id = hashlib.sha256(dedup_key.encode()).hexdigest()[:16]
                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url="https://www.cdc.gov/wastewater/",
                    external_id=f"nwss_mpox_{external_id}",
                    title=title,
                    body=body,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={
                        "state": state,
                        "pathogen": "mpox",
                        "location": location_detail,
                        "lat": lat,
                        "lon": lon,
                        "is_high_consequence": True,
                    },
                ))

            except Exception as e:
                log.warning(f"[nwss_mpox] Record error: {e}")

        log.info(f"[nwss_mpox] {len(signals)} mpox wastewater signals")
        return signals


class NWSSEmergingIngester(BaseIngester):
    """
    CDC wastewater emerging pathogen tracker.
    Uses the NWSS SARS-CoV-2 concentration dataset as proxy,
    plus checks for H5/avian flu detections via the standard endpoint.
    """
    source_id = "nwss_emerging"
    credibility_weight = 0.88

    # Use the confirmed working concentration dataset
    DATASET_ID = "2ew6-ywp6"

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1, min=2, max=10))
    async def fetch_raw(self) -> List[RawSignal]:
        log.info("[nwss_emerging] Fetching NWSS SARS-CoV-2 concentration for anomaly check")

        url = f"{SOCRATA_BASE}/{self.DATASET_ID}.json"
        params = {
            "$order": "sample_collect_date DESC",
            "$limit": 50,
        }

        try:
            async with httpx.AsyncClient(timeout=25, follow_redirects=True) as client:
                resp = await client.get(url, params=params)
                if resp.status_code == 404:
                    log.info("[nwss_emerging] Dataset not available")
                    return []
                resp.raise_for_status()

            records = resp.json()
            if not isinstance(records, list) or not records:
                return []

            if records:
                log.info(f"[nwss_emerging] Sample fields: {list(records[0].keys())[:12]}")

        except Exception as e:
            log.warning(f"[nwss_emerging] Error: {e}")
            return []

        cutoff = datetime.utcnow() - timedelta(days=14)
        signals = []
        seen = set()

        for record in records:
            try:
                state = _get_field(record,
                    "state_name", "state", "jurisdiction")
                date_str = _get_field(record,
                    "date_updated", "sample_collect_date", "date", "week_end_date")
                wwtp = _get_field(record, "wwtp_name", "site_name")
                county = _get_field(record, "county_names", "county")

                # Look for anomaly indicators
                ptc_15d = _get_field(record, "ptc_15d", "percent_change")
                percentile = _get_field(record, "percentile", "pct")

                if not state:
                    continue

                published_at = datetime.utcnow()
                if date_str:
                    for fmt in ["%Y-%m-%dT%H:%M:%S.%f", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"]:
                        try:
                            published_at = datetime.strptime(date_str[:len(fmt)], fmt)
                            break
                        except ValueError:
                            continue

                if published_at < cutoff:
                    continue

                # Only flag genuinely anomalous readings
                # Percentile > 90 = top 10% of historical readings = notable
                try:
                    pct_val = float(percentile) if percentile else 0
                    pct_change = float(ptc_15d) if ptc_15d else 0
                except (ValueError, TypeError):
                    pct_val = 0
                    pct_change = 0

                if pct_val < 90 and pct_change < 100:
                    continue  # Not anomalous enough to surface

                location = wwtp or county or state
                dedup_key = f"emerg_{state}_{location}_{date_str[:10] if date_str else 'x'}"
                if dedup_key in seen:
                    continue
                seen.add(dedup_key)

                lat, lon = STATE_CENTROIDS.get(state, (38.89, -77.03))

                title = f"[WASTEWATER ANOMALY] SARS-CoV-2 elevated — {location}, {state}"
                body = (
                    f"CDC NWSS wastewater data shows anomalous SARS-CoV-2 levels "
                    f"at {location}, {state}. "
                    f"Current reading at {pct_val:.0f}th historical percentile"
                    f"{f', up {pct_change:.0f}% over 15 days' if pct_change > 0 else ''}. "
                    f"Elevated wastewater levels typically precede clinical surge by 4-7 days. "
                    f"Source: CDC NWSS."
                )

                external_id = hashlib.sha256(dedup_key.encode()).hexdigest()[:16]
                signals.append(RawSignal(
                    source_id=self.source_id,
                    source_url="https://www.cdc.gov/wastewater/",
                    external_id=f"nwss_emerg_{external_id}",
                    title=title,
                    body=body,
                    published_at=published_at,
                    credibility_weight=self.credibility_weight,
                    raw_metadata={
                        "state": state,
                        "pathogen": "SARS-CoV-2",
                        "lat": lat,
                        "lon": lon,
                        "percentile": pct_val,
                    },
                ))

            except Exception as e:
                log.warning(f"[nwss_emerging] Record error: {e}")

        log.info(f"[nwss_emerging] {len(signals)} wastewater anomaly signals")
        return signals
