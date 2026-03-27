from __future__ import annotations

from typing import Dict, Any, List
from collections import Counter


def compute_blast_radius(
    profiles: Dict[str, Any] | None,
    root_events: List[Dict[str, Any]],
    pattern_info: Dict[str, Any],
    incident: Dict[str, Any],
) -> Dict[str, Any]:
    """
    Compute blast radius of an incident.

    Uses:
    - root_events (primary signal)
    - pattern_info (semantic classification)
    - incident window (temporal impact)
    - optional cluster profiles (future extensibility)

    Returns a structured blast radius object.
    """

    # --------------------------------------------------
    # Basic aggregations from root events
    # --------------------------------------------------

    actors = [e.get("actor") for e in root_events if e.get("actor")]
    resources = [e.get("resource") for e in root_events if e.get("resource")]
    response_codes = [
        e.get("response_code")
        for e in root_events
        if e.get("response_code") is not None
    ]

    actor_counts = Counter(actors)
    resource_counts = Counter(resources)
    code_counts = Counter(response_codes)

    affected_actors = list(actor_counts.keys())
    affected_resources = list(resource_counts.keys())

    dominant_code = None
    if code_counts:
        dominant_code = code_counts.most_common(1)[0][0]

    # --------------------------------------------------
    # Cluster scope (if profiles available)
    # --------------------------------------------------

    affected_clusters = len(profiles) if profiles else None

    # --------------------------------------------------
    # Temporal impact
    # --------------------------------------------------

    duration = incident.get("duration_seconds") or (
        incident.get("incident_window", {}).get("duration_seconds")
    )

    # --------------------------------------------------
    # Scope classification
    # --------------------------------------------------

    if affected_clusters is not None:
        if affected_clusters >= 20:
            scope = "cluster-wide"
        elif affected_clusters >= 5:
            scope = "multi-component"
        else:
            scope = "localized"
    else:
        # fallback based on resource spread
        if len(affected_resources) >= 5:
            scope = "multi-component"
        else:
            scope = "localized"

    # --------------------------------------------------
    # Severity heuristic
    # --------------------------------------------------

    severity = "low"

    if duration:
        if duration >= 120:
            severity = "high"
        elif duration >= 30:
            severity = "medium"

    # strengthen severity if systemic failure
    if pattern_info.get("pattern") in [
        "cluster_wide_authorization_failure",
        "server_or_control_plane_failure",
    ]:
        severity = "high"

    # --------------------------------------------------
    # Propagation estimate (simple but effective)
    # --------------------------------------------------

    if affected_clusters:
        propagation_depth = min(5, max(1, affected_clusters // 5))
    else:
        propagation_depth = min(3, max(1, len(affected_resources)))

    # --------------------------------------------------
    # Final object
    # --------------------------------------------------

    return {
        "scope": scope,
        "affected_clusters": affected_clusters,
        "affected_actors": len(affected_actors),
        "affected_resources": len(affected_resources),
        "actors": affected_actors,
        "resources": affected_resources,
        "dominant_code": dominant_code,
        "pattern": pattern_info.get("pattern"),
        "duration_seconds": duration,
        "propagation_depth": propagation_depth,
        "severity": severity,
    }