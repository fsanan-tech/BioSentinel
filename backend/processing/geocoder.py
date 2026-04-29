"""
BioSentinel Geocoder
Converts location strings → GeoLocation objects with lat/lon.

Strategy:
  1. Check in-memory + disk cache (avoids hammering Nominatim)
  2. Try static lookup table for common country centroids (fast, no API)
  3. Fall back to Nominatim (free, OSM-based) with rate limiting

Nominatim usage policy: max 1 request/second, cache aggressively.
"""
import json
import asyncio
import logging
import hashlib
from pathlib import Path
from typing import Optional, Dict
from datetime import datetime, timedelta

from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderServiceError

from backend.models.schemas import GeoLocation

log = logging.getLogger(__name__)

CACHE_PATH = Path("data/geocache.json")

# ─────────────────────────────────────────────
# Static country centroids — instant lookup
# ─────────────────────────────────────────────
COUNTRY_CENTROIDS: Dict[str, tuple] = {
    "afghanistan": (33.9391, 67.7100), "albania": (41.1533, 20.1683),
    "algeria": (28.0339, 1.6596), "angola": (-11.2027, 17.8739),
    "argentina": (-38.4161, -63.6167), "armenia": (40.0691, 45.0382),
    "australia": (-25.2744, 133.7751), "austria": (47.5162, 14.5501),
    "bangladesh": (23.6850, 90.3563), "belgium": (50.5039, 4.4699),
    "benin": (9.3077, 2.3158), "bolivia": (-16.2902, -63.5887),
    "brazil": (-14.2350, -51.9253), "burkina faso": (12.3641, -1.5275),
    "burundi": (-3.3731, 29.9189), "cambodia": (12.5657, 104.9910),
    "cameroon": (7.3697, 12.3547), "canada": (56.1304, -106.3468),
    "central african republic": (6.6111, 20.9394), "chad": (15.4542, 18.7322),
    "chile": (-35.6751, -71.5430), "china": (35.8617, 104.1954),
    "colombia": (4.5709, -74.2973), "congo": (-0.2280, 15.8277),
    "costa rica": (9.7489, -83.7534), "cuba": (21.5218, -77.7812),
    "democratic republic of congo": (-4.0383, 21.7587),
    "drc": (-4.0383, 21.7587),
    "ecuador": (-1.8312, -78.1834), "egypt": (26.8206, 30.8025),
    "ethiopia": (9.1450, 40.4897), "france": (46.2276, 2.2137),
    "gabon": (-0.8037, 11.6094), "germany": (51.1657, 10.4515),
    "ghana": (7.9465, -1.0232), "guinea": (9.9456, -11.3247),
    "guinea-bissau": (11.8037, -15.1804), "haiti": (18.9712, -72.2852),
    "india": (20.5937, 78.9629), "indonesia": (-0.7893, 113.9213),
    "iran": (32.4279, 53.6880), "iraq": (33.2232, 43.6793),
    "italy": (41.8719, 12.5674), "ivory coast": (7.5400, -5.5471),
    "japan": (36.2048, 138.2529), "jordan": (30.5852, 36.2384),
    "kenya": (-0.0236, 37.9062), "laos": (19.8563, 102.4955),
    "lebanon": (33.8547, 35.8623), "liberia": (6.4281, -9.4295),
    "libya": (26.3351, 17.2283), "madagascar": (-18.7669, 46.8691),
    "malawi": (-13.2543, 34.3015), "malaysia": (4.2105, 101.9758),
    "mali": (17.5707, -3.9962), "mauritania": (21.0079, -10.9408),
    "mexico": (23.6345, -102.5528), "morocco": (31.7917, -7.0926),
    "mozambique": (-18.6657, 35.5296), "myanmar": (21.9162, 95.9560),
    "nepal": (28.3949, 84.1240), "nigeria": (9.0820, 8.6753),
    "north korea": (40.3399, 127.5101), "pakistan": (30.3753, 69.3451),
    "philippines": (12.8797, 121.7740), "russia": (61.5240, 105.3188),
    "rwanda": (-1.9403, 29.8739), "saudi arabia": (23.8859, 45.0792),
    "senegal": (14.4974, -14.4524), "sierra leone": (8.4606, -11.7799),
    "somalia": (5.1521, 46.1996), "south africa": (-30.5595, 22.9375),
    "south korea": (35.9078, 127.7669), "south sudan": (4.8594, 31.5713),
    "spain": (40.4637, -3.7492), "sudan": (12.8628, 30.2176),
    "syria": (34.8021, 38.9968), "taiwan": (23.6978, 120.9605),
    "tanzania": (-6.3690, 34.8888), "thailand": (15.8700, 100.9925),
    "turkey": (38.9637, 35.2433), "uganda": (1.3733, 32.2903),
    "ukraine": (48.3794, 31.1656), "united kingdom": (55.3781, -3.4360),
    "united states": (37.0902, -95.7129), "usa": (37.0902, -95.7129),
    "venezuela": (6.4238, -66.5897), "vietnam": (14.0583, 108.2772),
    "yemen": (15.5527, 48.5164), "zambia": (-13.1339, 27.8493),
    "zimbabwe": (-19.0154, 29.1549),
    # Regions
    "west africa": (12.0, -2.0), "east africa": (1.0, 36.0),
    "central africa": (0.0, 20.0), "southern africa": (-20.0, 25.0),
    "north africa": (27.0, 20.0), "sub-saharan africa": (5.0, 20.0),
    "middle east": (29.0, 42.0), "southeast asia": (5.0, 115.0),
    "south asia": (20.0, 77.0), "central asia": (44.0, 65.0),
    "eastern europe": (52.0, 25.0), "latin america": (-15.0, -60.0),
    "caribbean": (17.0, -72.0), "horn of africa": (8.0, 45.0),
    # Major cities
    "wuhan": (30.5928, 114.3055), "beijing": (39.9042, 116.4074),
    "shanghai": (31.2304, 121.4737), "hong kong": (22.3193, 114.1694),
    "delhi": (28.6139, 77.2090), "mumbai": (19.0760, 72.8777),
    "kinshasa": (-4.3217, 15.3222), "lagos": (6.5244, 3.3792),
    "nairobi": (-1.2921, 36.8219), "johannesburg": (-26.2041, 28.0473),
    "cairo": (30.0444, 31.2357), "baghdad": (33.3128, 44.3615),
    "karachi": (24.8607, 67.0011), "dhaka": (23.8103, 90.4125),
    "jakarta": (-6.2088, 106.8456), "manila": (14.5995, 120.9842),
    "bangkok": (13.7563, 100.5018), "yangon": (16.8661, 96.1951),
    "kathmandu": (27.7172, 85.3240), "freetown": (8.4897, -13.2343),
    "monrovia": (6.2907, -10.7605), "conakry": (9.6412, -13.5784),
    "bamako": (12.6392, -8.0029), "khartoum": (15.5007, 32.5599),
}

# In-memory geocache
_geocache: Dict[str, Optional[GeoLocation]] = {}
_nominatim_last_call: float = 0.0
_NOMINATIM_RATE_LIMIT = 1.1  # seconds between calls


def _cache_key(place: str) -> str:
    return hashlib.md5(place.lower().strip().encode()).hexdigest()


def _load_cache():
    global _geocache
    if CACHE_PATH.exists():
        try:
            data = json.loads(CACHE_PATH.read_text())
            for k, v in data.items():
                _geocache[k] = GeoLocation(**v) if v else None
        except Exception:
            pass


def _save_cache():
    CACHE_PATH.parent.mkdir(parents=True, exist_ok=True)
    serializable = {
        k: v.model_dump() if v else None
        for k, v in _geocache.items()
    }
    CACHE_PATH.write_text(json.dumps(serializable, indent=2, default=str))


_load_cache()

_geolocator = Nominatim(user_agent="biosentinel_osint/0.1")


async def geocode(place_name: str) -> Optional[GeoLocation]:
    """
    Returns a GeoLocation for the given place name.
    Uses cache → static lookup → Nominatim in that order.
    """
    if not place_name or len(place_name) < 2:
        return None

    key = _cache_key(place_name)

    # 1. Cache hit
    if key in _geocache:
        return _geocache[key]

    # 2. Static lookup
    normalized = place_name.lower().strip()
    if normalized in COUNTRY_CENTROIDS:
        lat, lon = COUNTRY_CENTROIDS[normalized]
        geo = GeoLocation(
            place_name=place_name.title(),
            latitude=lat,
            longitude=lon,
            geo_confidence=0.85,
        )
        _geocache[key] = geo
        _save_cache()
        return geo

    # 3. Nominatim (rate limited)
    global _nominatim_last_call
    import time
    elapsed = time.time() - _nominatim_last_call
    if elapsed < _NOMINATIM_RATE_LIMIT:
        await asyncio.sleep(_NOMINATIM_RATE_LIMIT - elapsed)

    try:
        loop = asyncio.get_event_loop()
        location = await loop.run_in_executor(
            None, lambda: _geolocator.geocode(place_name, timeout=5)
        )
        _nominatim_last_call = time.time()
        if location:
            geo = GeoLocation(
                place_name=location.address.split(",")[0],
                latitude=location.latitude,
                longitude=location.longitude,
                geo_confidence=0.70,
            )
            _geocache[key] = geo
            _save_cache()
            return geo
    except (GeocoderTimedOut, GeocoderServiceError) as e:
        log.warning(f"Geocoding failed for '{place_name}': {e}")

    _geocache[key] = None
    return None


async def geocode_locations(place_names: list) -> list:
    """Geocode a list of place names, returning GeoLocation objects."""
    results = []
    for name in place_names:
        geo = await geocode(name)
        if geo:
            results.append(geo)
    return results
