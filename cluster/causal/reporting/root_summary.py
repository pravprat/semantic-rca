# 7A. cluster/causal/reporting/root_summary.py

from __future__ import annotations

from typing import Dict, Any, List


PATTERN_LABELS = {
    "cluster_wide_authorization_failure": "Cluster-wide Authorization Failure",
    "authorization_failure": "Authorization Failure",
    "authentication_failure": "Authentication Failure",
    "resource_missing_or_unavailable": "Resource Missing / Unavailable",
    "server_or_control_plane_failure": "Server / Control Plane Failure",
    "component_specific_failure": "Component-Specific Failure",
    "milvus_autoscaler_scaling_misconfiguration": "Milvus Autoscaler / Timeslicing Misconfiguration",
    "client_or_api_failure": "Client / API Failure",
    "unknown": "Unknown Failure Pattern",
}


def build_root_cause_summary(
    incident_id: str,
    top_candidate: Dict[str, Any],
    root_events: List[Dict[str, Any]],
    pattern_info: Dict[str, Any],
) -> Dict[str, Any]:
    primary_event = root_events[0] if root_events else {}
    primary_actor = primary_event.get("actor") or top_candidate.get("actor")
    primary_resource = primary_event.get("resource") or top_candidate.get("resource")
    primary_service = primary_event.get("service")
    primary_component = primary_event.get("component")
    primary_timestamp = primary_event.get("timestamp") or top_candidate.get("first_seen")
    primary_response_code = primary_event.get("response_code")
    primary_status_family = primary_event.get("status_family")
    primary_failure_hint = primary_event.get("failure_hint")
    primary_severity = primary_event.get("severity")

    pattern = pattern_info.get("pattern", "unknown")

    return {
        "incident_id": incident_id,
        "root_cause": {
            "type": PATTERN_LABELS.get(pattern, "Unknown Failure Pattern"),
            "pattern": pattern,
            "primary_cluster_id": top_candidate.get("cluster_id"),
            "primary_event_id": primary_event.get("event_id"),
            "start_time": primary_timestamp,
            "primary_actor": primary_actor,
            "primary_service": primary_service,
            "primary_component": primary_component,
            "primary_resource": primary_resource,
            "primary_response_code": primary_response_code,
            "primary_status_family": primary_status_family,
            "primary_failure_hint": primary_failure_hint,
            "primary_severity": primary_severity,
        }
    }