# 7B. cluster/causal/reporting/explanation_builder.py

from __future__ import annotations

from typing import Dict, Any, List


def build_explanation(
    top_candidate: Dict[str, Any],
    root_events: List[Dict[str, Any]],
    pattern_info: Dict[str, Any],
) -> Dict[str, Any]:
    if not root_events:
        return {
            "summary": "No grounded root events were available for explanation.",
            "evidence": [],
        }

    earliest = root_events[0]

    actors = pattern_info.get("affected_actors", [])
    resources = pattern_info.get("affected_resources", [])
    primary_code = pattern_info.get("primary_code")
    dominant_code = pattern_info.get("dominant_code")
    pattern = pattern_info.get("pattern", "unknown")

    primary_actor = earliest.get("actor")
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
    else:
        summary = (
            f"Earliest grounded failures were observed at {primary_ts} with HTTP {primary_code} "
            f"responses for {primary_actor} accessing {primary_resource}."
        )

    evidence = [
        {
            "type": "primary_event",
            "event_id": earliest.get("event_id"),
            "timestamp": earliest.get("timestamp"),
            "actor": earliest.get("actor"),
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
            "primary_code": primary_code,
            "dominant_code": dominant_code,
            "pattern": pattern,
        },
    ]

    return {
        "summary": summary,
        "evidence": evidence,
    }