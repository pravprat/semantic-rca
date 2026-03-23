from typing import Dict

from semantic.entity_extractor import extract_event_semantics
from semantic.signature import build_signature


def enrich_event(event: Dict) -> Dict:
    """
    Single semantic entry point.
    Adds semantic meaning + signature.
    """

    sem = extract_event_semantics(event)

    event["semantic"] = sem
    event["signature"] = build_signature(sem)

    return event