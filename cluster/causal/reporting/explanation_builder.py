# 7B. cluster/causal/reporting/explanation_builder.py

from __future__ import annotations

from typing import Dict, Any, List


def build_explanation(
    top_candidate: Dict[str, Any],
    root_events: List[Dict[str, Any]],
    pattern_info: Dict[str, Any],
) -> Dict[str, Any]:
    if not root_events:
        actor = top_candidate.get("actor")
        resource = top_candidate.get("resource")
        ts = top_candidate.get("first_seen")
        fd = top_candidate.get("failure_domain")
        return {
            "summary": (
                "Grounded root events were sparse for this incident window. "
                f"Using cluster-level evidence, the earliest candidate pattern starts around {ts} "
                f"with actor={actor} resource={resource} and failure_domain={fd}."
            ),
            "evidence": [
                {
                    "type": "candidate_fallback",
                    "cluster_id": top_candidate.get("cluster_id"),
                    "first_seen": ts,
                    "actor": actor,
                    "resource": resource,
                    "failure_domain": fd,
                    "candidate_score": top_candidate.get("candidate_score"),
                }
            ],
        }

    earliest = root_events[0]

    actors = pattern_info.get("affected_actors", [])
    resources = pattern_info.get("affected_resources", [])
    services = pattern_info.get("affected_services", [])
    components = pattern_info.get("affected_components", [])
    primary_code = pattern_info.get("primary_code")
    primary_status_family = pattern_info.get("primary_status_family")
    primary_failure_hint = pattern_info.get("primary_failure_hint")
    primary_severity = pattern_info.get("primary_severity")
    dominant_code = pattern_info.get("dominant_code")
    pattern = pattern_info.get("pattern", "unknown")
    dominant_component = pattern_info.get("dominant_component")
    dominant_service = pattern_info.get("dominant_service")

    primary_actor = earliest.get("actor")
    primary_service = earliest.get("service")
    primary_component = earliest.get("component")
    primary_resource = earliest.get("resource")
    primary_ts = earliest.get("timestamp")

    if pattern == "cluster_wide_authorization_failure":
        summary = (
            f"Earliest failures were observed at {primary_ts} with HTTP {primary_code} "
            f"responses for {primary_actor} accessing {primary_resource}, indicating a "
            f"likely cluster-wide authorization failure affecting core system access. "
            f"Subsequent failures across additional resources appear to be downstream effects."
        )
    elif pattern == "authorization_failure":
        summary = (
            f"Earliest failures were observed at {primary_ts} with HTTP {primary_code} "
            f"responses for {primary_actor} accessing {primary_resource}, indicating an "
            f"authorization failure at incident onset."
        )
    elif pattern == "authentication_failure":
        summary = (
            f"Earliest failures were observed at {primary_ts} with HTTP {primary_code} "
            f"responses, indicating an authentication failure at incident onset."
        )
    elif pattern == "resource_missing_or_unavailable":
        summary = (
            f"Earliest failures were observed at {primary_ts} with HTTP {primary_code} "
            f"responses for {primary_resource}, suggesting missing or unavailable resources "
            f"during incident onset."
        )
    elif pattern == "server_or_control_plane_failure":
        summary = (
            f"Earliest failures were observed at {primary_ts} with HTTP {primary_code} "
            f"responses, suggesting a server or control-plane failure."
        )
    elif pattern == "milvus_autoscaler_scaling_misconfiguration":
        summary = (
            f"Earliest failures were observed at {primary_ts} around Milvus component signals "
            f"(component={primary_component or dominant_component}, service={primary_service or dominant_service}), "
            "consistent with autoscaler/timeslicing scaling-state instability and downstream API degradation."
        )
    elif pattern == "component_specific_failure":
        signal = f"status_family={primary_status_family}, severity={primary_severity}, failure_hint={primary_failure_hint}"
        if primary_code is not None:
            signal = f"http={primary_code}, " + signal
        summary = (
            f"Earliest failures were observed at {primary_ts} for component={primary_component or dominant_component} "
            f"(service={primary_service or dominant_service}) and actor={primary_actor}. "
            f"Primary failure signal: {signal}. This indicates a component-local "
            "failure pattern that later surfaced as API symptoms."
        )
    else:
        signal = f"status_family={primary_status_family}, severity={primary_severity}, failure_hint={primary_failure_hint}"
        if primary_code is not None:
            signal = f"http={primary_code}, " + signal
        summary = (
            f"Earliest grounded failures were observed at {primary_ts} for actor={primary_actor} "
            f"resource={primary_resource}. Primary failure signal: {signal}."
        )

    evidence = [
        {
            "type": "primary_event",
            "event_id": earliest.get("event_id"),
            "timestamp": earliest.get("timestamp"),
            "actor": earliest.get("actor"),
            "service": earliest.get("service"),
            "component": earliest.get("component"),
            "resource": earliest.get("resource"),
            "response_code": earliest.get("response_code"),
            "reason": earliest.get("reason"),
        },
        {
            "type": "candidate_support",
            "cluster_id": top_candidate.get("cluster_id"),
            "candidate_score": top_candidate.get("candidate_score"),
            "temporal_rank": top_candidate.get("temporal_rank"),
            "out_degree": top_candidate.get("out_degree"),
            "in_degree": top_candidate.get("in_degree"),
        },
        {
            "type": "pattern_support",
            "affected_actors": actors,
            "affected_resources": resources,
            "affected_services": services,
            "affected_components": components,
            "primary_code": primary_code,
            "primary_status_family": primary_status_family,
            "primary_failure_hint": primary_failure_hint,
            "primary_severity": primary_severity,
            "dominant_code": dominant_code,
            "dominant_component": dominant_component,
            "dominant_service": dominant_service,
            "pattern": pattern,
        },
    ]

    return {
        "summary": summary,
        "evidence": evidence,
    }