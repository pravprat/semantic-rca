# 7A. cluster/causal/reporting/root_summary.py

from __future__ import annotations

from typing import Dict, Any, List


PATTERN_LABELS = {
    "cluster_wide_authorization_failure": "Cluster-wide Authorization Failure",
    "authorization_failure": "Authorization Failure",
    "authentication_failure": "Authentication Failure",
    "resource_missing_or_unavailable": "Resource Missing / Unavailable",
    "server_or_control_plane_failure": "Server / Control Plane Failure",
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

    pattern = pattern_info.get("pattern", "unknown")

    return {
        "incident_id": incident_id,
        "root_cause": {
            "type": PATTERN_LABELS.get(pattern, "Unknown Failure Pattern"),
            "pattern": pattern,
            "primary_cluster_id": top_candidate.get("cluster_id"),
            "primary_event_id": primary_event.get("event_id"),
            "start_time": primary_event.get("timestamp"),
            "primary_actor": primary_event.get("actor"),
            "primary_resource": primary_event.get("resource"),
            "primary_response_code": primary_event.get("response_code"),
        }
    }