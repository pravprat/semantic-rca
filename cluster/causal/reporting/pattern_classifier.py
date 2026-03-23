# 7D.cluster/causal/reporting/pattern_classifier.py

from __future__ import annotations

from collections import Counter
from typing import Dict, Any, List


def classify_failure_pattern(root_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not root_events:
        return {
            "pattern": "unknown",
            "primary_code": None,
            "dominant_code": None,
            "affected_actors": [],
            "affected_resources": [],
            "code_counts": {},
        }

    primary_event = root_events[0]
    primary_code = primary_event.get("response_code")
    primary_actor = primary_event.get("actor") or ""

    response_codes = [
        e.get("response_code")
        for e in root_events
        if e.get("response_code") is not None
    ]
    actors = [e.get("actor") for e in root_events if e.get("actor")]
    resources = [e.get("resource") for e in root_events if e.get("resource")]

    code_counts = Counter(response_codes)
    actor_counts = Counter(actors)
    resource_counts = Counter(resources)

    dominant_code = code_counts.most_common(1)[0][0] if code_counts else None

    pattern = "unknown"

    # --------------------------------------------------
    # Primary-event-first classification
    # --------------------------------------------------
    if primary_code == 403:
        pattern = "authorization_failure"
    elif primary_code == 401:
        pattern = "authentication_failure"
    elif primary_code == 404:
        pattern = "resource_missing_or_unavailable"
    elif primary_code and int(primary_code) >= 500:
        pattern = "server_or_control_plane_failure"
    elif primary_code and int(primary_code) >= 400:
        pattern = "client_or_api_failure"

    # --------------------------------------------------
    # Stronger systemic label for authz failures
    # --------------------------------------------------
    if primary_code == 403:
        if primary_actor.startswith("system:") and (
            len(resource_counts) >= 2 or len(actor_counts) >= 2
        ):
            pattern = "cluster_wide_authorization_failure"

    return {
        "pattern": pattern,
        "primary_code": primary_code,
        "dominant_code": dominant_code,
        "affected_actors": list(actor_counts.keys()),
        "affected_resources": list(resource_counts.keys()),
        "code_counts": dict(code_counts),
    }