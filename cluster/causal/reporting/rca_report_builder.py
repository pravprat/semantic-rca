# 7A to 7D cluster/causal/reporting/rca_report_builder.py

from __future__ import annotations

from typing import Dict, Any, List

from cluster.causal.reporting.confidence import compute_confidence
from cluster.causal.reporting.explanation_builder import build_explanation
from cluster.causal.reporting.pattern_classifier import classify_failure_pattern
from cluster.causal.reporting.root_summary import build_root_cause_summary
from cluster.causal.utils.io_utils import load_json, write_json
from cluster.causal.reporting.blast_radius import compute_blast_radius


def _group_root_events_by_incident(root_events_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for item in root_events_data:
        out[item["incident_id"]] = item.get("root_events", [])
    return out


def _group_candidates_by_incident(candidates_data: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    out: Dict[str, List[Dict[str, Any]]] = {}
    for item in candidates_data:
        out[item["incident_id"]] = item.get("candidates", [])
    return out


def build_rca_report(
    incidents_path: str,
    candidates_path: str,
    root_events_path: str,
    output_path: str,
) -> None:
    incidents: List[Dict[str, Any]] = load_json(incidents_path)
    candidates_data: List[Dict[str, Any]] = load_json(candidates_path)
    root_events_data: List[Dict[str, Any]] = load_json(root_events_path)

    candidates_by_incident = _group_candidates_by_incident(candidates_data)
    root_events_by_incident = _group_root_events_by_incident(root_events_data)

    reports: List[Dict[str, Any]] = []

    for incident in incidents:
        incident_id = incident["incident_id"]

        candidates = candidates_by_incident.get(incident_id, [])
        root_events = root_events_by_incident.get(incident_id, [])

        if not candidates:
            reports.append({
                "incident_id": incident_id,
                "status": "no_candidates",
                "message": "No root cause candidates available.",
            })
            continue

        top_candidate = candidates[0]

        pattern_info = classify_failure_pattern(root_events)
        summary = build_root_cause_summary(
            incident_id=incident_id,
            top_candidate=top_candidate,
            root_events=root_events,
            pattern_info=pattern_info,
        )
        explanation = build_explanation(
            top_candidate=top_candidate,
            root_events=root_events,
            pattern_info=pattern_info,
        )
        confidence = compute_confidence(
            top_candidate=top_candidate,
            all_candidates=candidates,
            root_events=root_events,
        )

        blast_radius = compute_blast_radius(
            profiles=None,  # optional for now
            root_events=root_events,
            pattern_info=pattern_info,
            incident=incident,
        )

        reports.append({
            "incident_id": incident_id,
            "incident_window": {
                "start_time": incident.get("start_time"),
                "end_time": incident.get("end_time"),
                "duration_seconds": incident.get("duration_seconds"),
            },
            "root_cause_summary": summary["root_cause"],
            "explanation": explanation["summary"],
            "evidence": explanation["evidence"],
            "blast_radius": blast_radius,
            "confidence": confidence,
            "top_candidates": candidates[:5],
        })

    write_json(output_path, reports)
    print(f"[rca_report] incidents={len(reports)} -> {output_path}")