# cluster/causal/validation/event_checks.py

from __future__ import annotations

from typing import List, Dict, Any


def validate_grounded_events(root_events: List[Dict[str, Any]]) -> None:
    for e in root_events:
        if not e.get("event_id"):
            raise RuntimeError("[causal_analysis] grounded event missing event_id")
        rc = e.get("response_code")
        if rc is not None:
            try:
                if int(rc) < 400:
                    raise RuntimeError("[causal_analysis] grounded event is not a failure")
            except Exception:
                pass