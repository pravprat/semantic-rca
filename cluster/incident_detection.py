# /cluster/incident_detection.py

import json
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        s = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def run_incident_detection(
    cluster_trigger_stats_path: str,
    output_path: str,
    gap_seconds: int = 30,
    max_seeds: int = 3,
    status_output_path: str | None = None,
):

    # -------------------------------
    # Load stats
    # -------------------------------
    with open(cluster_trigger_stats_path, "r", encoding="utf-8") as f:
        stats: Dict[str, Any] = json.load(f)

    # -------------------------------
    # Step 1: Extract trigger clusters
    # -------------------------------
    triggers = []

    for cid, s in stats.items():

        if not s.get("is_candidate"):
            continue

        start = _parse_ts(s.get("first_seen"))
        end = _parse_ts(s.get("last_seen"))

        if not start or not end:
            continue

        triggers.append({
            "cluster_id": cid,
            "start": start,
            "end": end,
            "trigger_score": float(s.get("trigger_score", 0.0))
        })

    if not triggers:
        output: List[Dict[str, Any]] = []
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(output, f, indent=2)

        if status_output_path:
            status = {
                "status": "no_incident",
                "reason": "no_trigger_clusters",
                "candidate_clusters": 0,
                "incidents_count": 0,
            }
            with open(status_output_path, "w", encoding="utf-8") as f:
                json.dump(status, f, indent=2)

        print("[incident_detection] no trigger clusters found -> wrote empty incidents.json")
        return output

    # -------------------------------
    # Step 2: Sort by time
    # -------------------------------
    triggers.sort(key=lambda x: x["start"])

    # -------------------------------
    # Step 3: Merge into incidents
    # -------------------------------
    incidents: List[List[Dict[str, Any]]] = []
    current = []

    for c in triggers:

        if not current:
            current = [c]
            continue

        last = current[-1]

        if c["start"] <= last["end"] + timedelta(seconds=gap_seconds):
            current.append(c)
        else:
            incidents.append(current)
            current = [c]

    if current:
        incidents.append(current)

    # -------------------------------
    # Step 4: Build output
    # -------------------------------
    output = []

    for i, inc in enumerate(incidents, start=1):

        start_time = min(c["start"] for c in inc)
        end_time = max(c["end"] for c in inc)

        sorted_clusters = sorted(
            inc,
            key=lambda x: x["trigger_score"],
            reverse=True
        )

        seed_clusters = [
            {
                "cluster_id": c["cluster_id"],
                "trigger_score": round(c["trigger_score"], 6)
            }
            for c in sorted_clusters[:max_seeds]
        ]

        trigger_ids = [c["cluster_id"] for c in inc]

        duration = int((end_time - start_time).total_seconds())

        output.append({
            "incident_id": f"I{i}",
            "start_time": start_time.isoformat(),
            "end_time": end_time.isoformat(),
            "duration_seconds": duration,
            "seed_clusters": seed_clusters,
            "trigger_clusters": trigger_ids,
            "context_clusters": [],   # 🔒 intentionally empty
            "all_clusters": trigger_ids,
            "cluster_count": len(trigger_ids)
        })

    # -------------------------------
    # Write output
    # -------------------------------
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    if status_output_path:
        status = {
            "status": "incident_detected",
            "reason": "trigger_clusters_found",
            "candidate_clusters": len(triggers),
            "incidents_count": len(output),
        }
        with open(status_output_path, "w", encoding="utf-8") as f:
            json.dump(status, f, indent=2)

    print(f"[incident_detection] incidents={len(output)} -> {output_path}")
    return output