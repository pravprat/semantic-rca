# 7D.cluster/causal/reporting/pattern_classifier.py

from __future__ import annotations

from collections import Counter
from typing import Dict, Any, List


def _is_failure_event(ev: Dict[str, Any]) -> bool:
    rc = ev.get("response_code")
    try:
        if rc is not None and int(rc) >= 400:
            return True
    except Exception:
        pass
    status_family = str(ev.get("status_family") or "").lower()
    if status_family == "failure":
        return True
    if ev.get("failure_hint"):
        return True
    sem = ev.get("semantic") if isinstance(ev.get("semantic"), dict) else {}
    mode = str((sem or {}).get("failure_mode") or "").lower()
    return bool(mode and mode not in {"normal", "unknown"})


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
    primary_status_family = str(primary_event.get("status_family") or "").lower()
    primary_failure_hint = primary_event.get("failure_hint")
    primary_severity = str(primary_event.get("severity") or "").upper()
    primary_actor = primary_event.get("actor") or ""

    response_codes = [
        e.get("response_code")
        for e in root_events
        if e.get("response_code") is not None
    ]
    actors = [e.get("actor") for e in root_events if e.get("actor")]
    resources = [e.get("resource") for e in root_events if e.get("resource")]
    services = [e.get("service") for e in root_events if e.get("service")]
    components = [e.get("component") for e in root_events if e.get("component")]

    code_counts = Counter(response_codes)
    actor_counts = Counter(actors)
    resource_counts = Counter(resources)
    service_counts = Counter(services)
    component_counts = Counter(components)

    dominant_code = code_counts.most_common(1)[0][0] if code_counts else None

    pattern = "unknown"
    sem_modes = [
        str((e.get("semantic") or {}).get("failure_mode") or "").lower()
        for e in root_events
        if isinstance(e.get("semantic"), dict)
    ]
    sem_mode_counts = Counter([m for m in sem_modes if m and m not in {"normal", "unknown"}])
    dominant_mode = sem_mode_counts.most_common(1)[0][0] if sem_mode_counts else None
    top_failure_domain = str(root_events[0].get("failure_domain") or "").lower()
    dominant_component = component_counts.most_common(1)[0][0] if component_counts else None
    dominant_service = service_counts.most_common(1)[0][0] if service_counts else None
    corpus_text = " ".join(
        str(x or "")
        for e in root_events
        for x in (e.get("actor"), e.get("service"), e.get("resource"), e.get("failure_hint"))
    ).lower()

    # --------------------------------------------------
    # Primary-event-first classification
    # --------------------------------------------------
    if (
        ("milvus" in corpus_text or (dominant_component or "").startswith("milvus"))
        and ("autoscaler" in corpus_text or "timeslicing" in corpus_text or "timeslice" in corpus_text)
    ):
        pattern = "milvus_autoscaler_scaling_misconfiguration"
    # Component evidence should be favored over generic API labels.
    elif dominant_component and dominant_component != "unknown_component":
        pattern = "component_specific_failure"
    elif primary_code == 403:
        pattern = "authorization_failure"
    elif primary_code == 401:
        pattern = "authentication_failure"
    elif primary_code == 404:
        pattern = "resource_missing_or_unavailable"
    elif primary_code and int(primary_code) >= 500:
        pattern = "server_or_control_plane_failure"
    elif primary_code and int(primary_code) >= 400:
        pattern = "client_or_api_failure"
    elif dominant_mode in {"forbidden", "unauthorized", "access_denied", "permission_denied"}:
        pattern = "authorization_failure"
    elif dominant_mode in {"resource_not_found"}:
        pattern = "resource_missing_or_unavailable"
    elif dominant_mode in {"service_failure", "timeout", "connection_refused", "connection_reset", "dns_failure"}:
        pattern = "server_or_control_plane_failure"
    elif "auth" in top_failure_domain or "rbac" in top_failure_domain:
        pattern = "authorization_failure"
    elif "resource" in top_failure_domain:
        pattern = "resource_missing_or_unavailable"
    elif "availability" in top_failure_domain or "service" in top_failure_domain:
        pattern = "server_or_control_plane_failure"
    elif any(_is_failure_event(e) for e in root_events):
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
        "affected_services": list(service_counts.keys()),
        "affected_components": list(component_counts.keys()),
        "code_counts": dict(code_counts),
        "dominant_mode": dominant_mode,
        "dominant_component": dominant_component,
        "dominant_service": dominant_service,
        "primary_status_family": primary_status_family or None,
        "primary_failure_hint": primary_failure_hint,
        "primary_severity": primary_severity or None,
    }