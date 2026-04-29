"""
BioSentinel Base Ingester
Abstract base class for all data source ingesters.
"""
import uuid
import logging
from abc import ABC, abstractmethod
from datetime import datetime
from typing import List

from backend.models.schemas import RawSignal, ProcessedEvent
from backend.processing.nlp_extractor import (
    extract_diseases, extract_severity_keywords,
    has_pandemic_keywords, extract_case_count,
    extract_death_count, extract_location_strings,
    compute_severity_score,
)
from backend.processing.geocoder import geocode_locations
from backend.models.schemas import ExtractedEntities

log = logging.getLogger(__name__)


class BaseIngester(ABC):
    source_id: str = "base"
    credibility_weight: float = 0.5

    @abstractmethod
    async def fetch_raw(self) -> List[RawSignal]:
        """Fetch raw signals from the data source."""
        ...

    async def process(self, signal: RawSignal) -> ProcessedEvent:
        """
        Run NLP extraction + geocoding on a RawSignal.
        Returns a ProcessedEvent ready for the fusion engine.
        """
        full_text = f"{signal.title} {signal.body}"

        diseases, is_hcp = extract_diseases(full_text)
        severity_kws = extract_severity_keywords(full_text)
        has_pk = has_pandemic_keywords(full_text)
        case_count = extract_case_count(full_text)
        death_count = extract_death_count(full_text)
        location_strings = extract_location_strings(full_text)

        severity_score = compute_severity_score(
            diseases, is_hcp, has_pk, severity_kws, case_count, death_count
        )

        entities = ExtractedEntities(
            diseases=diseases,
            locations=location_strings,
            case_count=case_count,
            death_count=death_count,
            is_high_consequence=is_hcp,
            has_pandemic_keywords=has_pk,
            severity_keywords=severity_kws,
        )

        # Geocode locations
        geo_locations = await geocode_locations(location_strings[:3])  # Top 3 candidates
        primary_location = geo_locations[0] if geo_locations else None

        primary_disease = diseases[0] if diseases else None
        # Prefer high-consequence diseases
        for d in diseases:
            if any(hcp in d.lower() for hcp in
                   ["ebola", "marburg", "anthrax", "plague", "smallpox", "mpox",
                    "nipah", "h5n1", "unknown", "novel", "unidentified"]):
                primary_disease = d
                break

        return ProcessedEvent(
            event_id=str(uuid.uuid4()),
            raw_signal=signal,
            entities=entities,
            primary_location=primary_location,
            all_locations=geo_locations,
            primary_disease=primary_disease,
            event_severity_score=severity_score,
        )

    async def run(self) -> List[ProcessedEvent]:
        """Full ingestion cycle: fetch → process."""
        try:
            raw_signals = await self.fetch_raw()
            log.info(f"[{self.source_id}] Fetched {len(raw_signals)} signals")
            processed = []
            for signal in raw_signals:
                try:
                    event = await self.process(signal)
                    processed.append(event)
                except Exception as e:
                    log.warning(f"[{self.source_id}] Error processing signal: {e}")
            log.info(f"[{self.source_id}] Processed {len(processed)} events")
            return processed
        except Exception as e:
            log.error(f"[{self.source_id}] Ingestion failed: {e}")
            return []
