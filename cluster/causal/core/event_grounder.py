# cluster/causal/core/event_grounder.py

from __future__ import annotations

from typing import Any, Dict, List

from cluster.causal.utils.time_utils import parse_ts


def ground_events_for_incident(
    incident: Dict[str, Any],
    candidate_clusters: List[str],
    event_cluster_map: Dict[str, str],
    events: List[Dict[str, Any]],
    top_k_per_cluster: int = 3,
) -> List[Dict[str, Any]]:
    incident_start = parse_ts(incident["start_time"])
    incident_end = parse_ts(incident["end_time"])

    cluster_events: Dict[str, List[Dict[str, Any]]] = {cid: [] for cid in candidate_clusters}

    for e in events:
        eid = e.get("event_id")
        if not eid:
            continue

        cid = event_cluster_map.get(eid)
        if cid not in cluster_events:
            continue

        ts = parse_ts(e.get("timestamp"))
        if not ts or not incident_start or not incident_end:
            continue

        if not (incident_start <= ts <= incident_end):
            continue

        rc = e.get("response_code") or e.get("status_code") or e.get("code") or 0
        try:
            rc = int(rc)
        except Exception:
            rc = 0

        if rc < 400:
            continue

        cluster_events[cid].append(e)

    grounded: List[Dict[str, Any]] = []

    for cid, evs in cluster_events.items():
        ranked = []

        for e in evs:
            ts = parse_ts(e.get("timestamp"))
            if not ts:
                continue

            rc = e.get("response_code") or e.get("status_code") or e.get("code") or 0
            try:
                rc = int(rc)
            except Exception:
                rc = 0

            score = 0.0
            # earlier is better
            if incident_start:
                score += max(0.0, 1.0 - ((ts - incident_start).total_seconds() / 300.0))
            # severity
            if rc >= 500:
                score += 2.0
            elif rc >= 400:
                score += 1.0

            ranked.append((score, e))

        ranked.sort(key=lambda x: x[0], reverse=True)

        for idx, (_, e) in enumerate(ranked[:top_k_per_cluster]):
            grounded.append(
                {
                    "cluster_id": cid,
                    "event_id": e.get("event_id"),
                    "timestamp": e.get("timestamp"),
                    "response_code": e.get("response_code"),
                    "actor": e.get("actor"),
                    "resource": e.get("resource"),
                    "reason": "earliest_failure" if idx == 0 else "supporting_failure",
                }
            )

    return grounded