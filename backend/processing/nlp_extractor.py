"""
BioSentinel NLP Extractor
Extracts diseases, locations, case counts, and severity signals
from free-text without requiring GPU or transformer models.

Architecture: Rule-based gazetteer + regex → fast, deterministic, auditable.
Designed to be swappable with an LLM-backed extractor for production.
"""
import re
import logging
from typing import List, Optional, Tuple, Set
from config import HIGH_CONSEQUENCE_PATHOGENS, PANDEMIC_KEYWORDS

log = logging.getLogger(__name__)

# ─────────────────────────────────────────────
# Disease / Pathogen Dictionary
# ─────────────────────────────────────────────
DISEASE_TERMS = {
    # Respiratory
    "influenza", "flu", "h1n1", "h3n2", "h5n1", "h5n6", "h7n9", "h9n2",
    "avian influenza", "bird flu", "swine flu", "pneumonia", "respiratory illness",
    "sars", "mers", "covid", "coronavirus", "covid-19", "sars-cov-2",
    "respiratory syncytial virus", "rsv", "parainfluenza",
    # Hemorrhagic / VHF
    "ebola", "marburg", "lassa", "rift valley fever", "dengue", "yellow fever",
    "hemorrhagic fever", "viral hemorrhagic fever", "vhf",
    "crimean-congo hemorrhagic fever", "cchf", "hantavirus",
    # Vector-borne
    "malaria", "zika", "chikungunya", "west nile", "west nile virus",
    "leishmaniasis", "trypanosomiasis", "sleeping sickness", "chagas",
    "japanese encephalitis", "tick-borne encephalitis",
    # Gastrointestinal
    "cholera", "typhoid", "salmonella", "e. coli", "escherichia coli",
    "norovirus", "rotavirus", "hepatitis a", "hepatitis e", "dysentery",
    # Neurological
    "meningitis", "meningococcal", "encephalitis", "rabies",
    "nipah", "hendra", "listeria",
    # Bacterial
    "plague", "anthrax", "tularemia", "brucellosis", "leptospirosis",
    "q fever", "typhus", "rickettsia", "whooping cough", "pertussis",
    "tuberculosis", "tb", "multidrug-resistant tb", "mdr-tb",
    "legionella", "legionnaires", "melioidosis",
    # Skin / Rash
    "mpox", "monkeypox", "smallpox", "variola", "measles", "rubella",
    "chickenpox", "varicella", "hand foot and mouth",
    # Emerging / Unknown
    "novel pathogen", "unidentified pathogen", "unknown disease",
    "novel respiratory", "novel hemorrhagic", "disease x",
    "unusual cluster", "unexplained deaths", "unexplained illness",
    # Other notable
    "polio", "poliovirus", "diphtheria", "tetanus", "botulism",
    "hepatitis b", "hepatitis c", "hepatitis d",
    "hiv", "aids", "monkeypox", "candida auris",
}

# Severity / escalation keywords that add weight
SEVERITY_KEYWORDS = {
    "deaths", "fatalities", "killed", "dead", "mortality",
    "rapid spread", "exponential", "overwhelmed", "surge",
    "community transmission", "human-to-human",
    "emergency", "crisis", "outbreak", "epidemic", "pandemic",
    "alert", "warning", "concern", "unusual", "unprecedented",
    "novel", "unknown", "unidentified", "laboratory confirmed",
    "quarantine", "isolation", "lockdown", "containment",
    "mass casualty", "mass casualties",
    "animal die-off", "mass die-off",
    "weaponized", "bioterrorism", "bioweapon", "biological attack",
}

# Patterns for numeric extraction
CASE_PATTERNS = [
    r'(\d[\d,]*)\s*(?:confirmed\s+)?cases?',
    r'(\d[\d,]*)\s*(?:people|persons?|individuals?)\s+(?:infected|affected|sick)',
    r'infected\s+(?:at least\s+)?(\d[\d,]*)',
    r'(?:at least\s+)?(\d[\d,]*)\s*(?:new\s+)?infections?',
]

DEATH_PATTERNS = [
    r'(\d[\d,]*)\s*deaths?',
    r'(\d[\d,]*)\s*(?:people|persons?)\s+(?:have\s+)?died',
    r'(\d[\d,]*)\s*fatalities',
    r'killed\s+(?:at least\s+)?(\d[\d,]*)',
    r'(?:at least\s+)?(\d[\d,]*)\s*(?:have\s+)?died',
]


def extract_diseases(text: str) -> Tuple[List[str], bool]:
    """
    Returns (list_of_diseases_found, is_high_consequence)
    """
    text_lower = text.lower()
    found = []
    is_hcp = False

    for term in DISEASE_TERMS:
        if re.search(r'\b' + re.escape(term) + r'\b', text_lower):
            found.append(term)

    # Check HCP separately for flag
    for hcp in HIGH_CONSEQUENCE_PATHOGENS:
        if re.search(r'\b' + re.escape(hcp) + r'\b', text_lower):
            is_hcp = True
            if hcp not in found:
                found.append(hcp)

    return found, is_hcp


def extract_severity_keywords(text: str) -> List[str]:
    text_lower = text.lower()
    return [kw for kw in SEVERITY_KEYWORDS
            if re.search(r'\b' + re.escape(kw) + r'\b', text_lower)]


def has_pandemic_keywords(text: str) -> bool:
    text_lower = text.lower()
    return any(
        re.search(r'\b' + re.escape(kw) + r'\b', text_lower)
        for kw in PANDEMIC_KEYWORDS
    )


def extract_case_count(text: str) -> Optional[int]:
    for pattern in CASE_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return None


def extract_death_count(text: str) -> Optional[int]:
    for pattern in DEATH_PATTERNS:
        m = re.search(pattern, text, re.IGNORECASE)
        if m:
            try:
                return int(m.group(1).replace(",", ""))
            except ValueError:
                pass
    return None


# ─────────────────────────────────────────────
# Location extraction
# Simple pattern matching + country list
# Geocoding is handled separately by geocoder.py
# ─────────────────────────────────────────────

# Expanded country + major city list
KNOWN_LOCATIONS = {
    # Countries
    "afghanistan", "albania", "algeria", "angola", "argentina", "armenia",
    "australia", "austria", "azerbaijan", "bangladesh", "belarus", "belgium",
    "benin", "bolivia", "brazil", "burkina faso", "burundi", "cambodia",
    "cameroon", "canada", "central african republic", "chad", "chile", "china",
    "colombia", "congo", "costa rica", "croatia", "cuba", "czech republic",
    "democratic republic of congo", "drc", "denmark", "ecuador", "egypt",
    "el salvador", "eritrea", "ethiopia", "finland", "france", "gabon",
    "georgia", "germany", "ghana", "greece", "guatemala", "guinea",
    "guinea-bissau", "haiti", "honduras", "hungary", "india", "indonesia",
    "iran", "iraq", "israel", "italy", "ivory coast", "japan", "jordan",
    "kenya", "laos", "lebanon", "liberia", "libya", "madagascar", "malawi",
    "malaysia", "mali", "mauritania", "mexico", "moldova", "mongolia",
    "morocco", "mozambique", "myanmar", "nepal", "netherlands", "nicaragua",
    "niger", "nigeria", "north korea", "norway", "pakistan", "panama",
    "papua new guinea", "paraguay", "peru", "philippines", "poland",
    "portugal", "romania", "russia", "rwanda", "saudi arabia", "senegal",
    "sierra leone", "somalia", "south africa", "south korea", "south sudan",
    "spain", "sri lanka", "sudan", "sweden", "switzerland", "syria",
    "taiwan", "tajikistan", "tanzania", "thailand", "timor-leste", "togo",
    "turkey", "turkmenistan", "uganda", "ukraine", "united kingdom",
    "united states", "usa", "uzbekistan", "venezuela", "vietnam",
    "yemen", "zambia", "zimbabwe",
    # Regions
    "sub-saharan africa", "west africa", "east africa", "central africa",
    "southern africa", "north africa", "middle east", "southeast asia",
    "south asia", "central asia", "eastern europe", "latin america",
    "caribbean", "pacific islands", "horn of africa",
    # Major cities (add as needed)
    "wuhan", "beijing", "shanghai", "hong kong", "delhi", "mumbai",
    "kinshasa", "lagos", "nairobi", "johannesburg", "cairo", "baghdad",
    "kabul", "karachi", "dhaka", "jakarta", "manila", "bangkok",
    "yangon", "kathmandu", "colombo", "freetown", "monrovia", "conakry",
    "bamako", "ouagadougou", "niamey", "ndjamena", "khartoum",
}


def extract_location_strings(text: str) -> List[str]:
    """Extract candidate location strings from text."""
    text_lower = text.lower()
    found = []

    # Match known locations
    for loc in KNOWN_LOCATIONS:
        # Word boundary match to avoid partial matches
        if re.search(r'\b' + re.escape(loc) + r'\b', text_lower):
            found.append(loc.title())

    # Also try to catch capitalized proper nouns that might be locations
    # Pattern: "in [City]", "from [Country]", "[Province], [Country]"
    prep_pattern = r'\b(?:in|from|at|near|across|throughout)\s+([A-Z][a-z]+(?:\s+[A-Z][a-z]+){0,2})'
    for m in re.finditer(prep_pattern, text):
        candidate = m.group(1)
        if candidate.lower() not in DISEASE_TERMS:
            found.append(candidate)

    # Deduplicate while preserving order
    seen: Set[str] = set()
    result = []
    for loc in found:
        key = loc.lower()
        if key not in seen:
            seen.add(key)
            result.append(loc)

    return result


def compute_severity_score(
    diseases: List[str],
    is_high_consequence: bool,
    has_pandemic_kw: bool,
    severity_kws: List[str],
    case_count: Optional[int],
    death_count: Optional[int],
) -> float:
    """
    Pre-fusion severity score: 0.0 – 1.0
    Does NOT incorporate source credibility (handled by fusion engine).
    """
    score = 0.0

    # Base: any disease detected
    if diseases:
        score += 0.15

    # High-consequence pathogen
    if is_high_consequence:
        score += 0.35

    # Pandemic language
    if has_pandemic_kw:
        score += 0.15

    # Severity keywords (capped)
    kw_bonus = min(len(severity_kws) * 0.04, 0.20)
    score += kw_bonus

    # Case data
    if case_count is not None:
        if case_count > 1000:
            score += 0.10
        elif case_count > 100:
            score += 0.07
        elif case_count > 10:
            score += 0.04

    # Deaths
    if death_count is not None:
        if death_count > 100:
            score += 0.15
        elif death_count > 10:
            score += 0.10
        elif death_count > 0:
            score += 0.05

    return min(score, 1.0)
