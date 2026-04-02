from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        dt = datetime.fromisoformat(ts.replace("Z", "+00:00"))
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _is_failure_event(e: Dict[str, Any]) -> bool:
    rc = e.get("response_code")
    try:
        if rc is not None and int(rc) >= 400:
            return True
    except Exception:
        pass
    sev = str(e.get("severity") or "").upper()
    sf = str(e.get("status_family") or "").lower()
    if sev in {"ERROR", "FATAL"} or sf == "failure" or e.get("failure_hint"):
        return True
    return False


def _failure_mode(e: Dict[str, Any]) -> str:
    sem = e.get("semantic") if isinstance(e.get("semantic"), dict) else {}
    mode = str((sem or {}).get("failure_mode") or "").strip()
    if mode:
        return mode
    sf = str(e.get("status_family") or "").lower()
    if sf == "failure":
        return "failure"
    return "unknown"


def _dep_target(e: Dict[str, Any]) -> str:
    sf = e.get("structured_fields") if isinstance(e.get("structured_fields"), dict) else {}
    tgt = sf.get("target_dependency_service")
    if tgt:
        return str(tgt)
    return "none"


def _jaccard(a: set[str], b: set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / max(1, len(a | b))


def _build_episodes_for_cluster(
    cluster_id: str,
    events: List[Dict[str, Any]],
    event_cluster_map: Dict[str, str],
    intra_cluster_gap_seconds: int,
    baseline_failure_eps: float,
) -> List[Dict[str, Any]]:
    rows: List[Tuple[datetime, Dict[str, Any]]] = []
    for e in events:
        eid = e.get("event_id")
        if event_cluster_map.get(eid) != cluster_id:
            continue
        if not _is_failure_event(e):
            continue
        ts = _parse_ts(e.get("timestamp"))
        if not ts:
            continue
        rows.append((ts, e))

    if not rows:
        return []

    rows.sort(key=lambda x: x[0])
    episodes: List[List[Tuple[datetime, Dict[str, Any]]]] = []
    cur: List[Tuple[datetime, Dict[str, Any]]] = []
    gap = timedelta(seconds=max(1, intra_cluster_gap_seconds))
    for row in rows:
        if not cur:
            cur = [row]
            continue
        if row[0] <= cur[-1][0] + gap:
            cur.append(row)
        else:
            episodes.append(cur)
            cur = [row]
    if cur:
        episodes.append(cur)

    out: List[Dict[str, Any]] = []
    for idx, ep in enumerate(episodes, start=1):
        start = ep[0][0]
        end = ep[-1][0]
        duration = max(1.0, (end - start).total_seconds())
        failure_count = len(ep)
        failure_eps = failure_count / duration
        burst = failure_eps / max(1e-9, baseline_failure_eps)
        mode_ctr = Counter(_failure_mode(e) for _, e in ep)
        dep_ctr = Counter(_dep_target(e) for _, e in ep)
        dominant_mode = mode_ctr.most_common(1)[0][0] if mode_ctr else "unknown"
        dominant_dep = dep_ctr.most_common(1)[0][0] if dep_ctr else "none"
        mode_consistency = mode_ctr.most_common(1)[0][1] / max(1, failure_count) if mode_ctr else 0.0

        # Deterministic episode score.
        intensity = min(1.0, burst / 5.0)
        consistency = min(1.0, mode_consistency)
        dep_signal = 1.0 if dominant_dep != "none" else 0.0
        score = round(0.45 * intensity + 0.35 * consistency + 0.20 * dep_signal, 6)

        out.append(
            {
                "episode_id": f"{cluster_id}.E{idx}",
                "cluster_id": cluster_id,
                "start": start,
                "end": end,
                "duration_seconds": int((end - start).total_seconds()),
                "failure_count": failure_count,
                "failure_eps": round(failure_eps, 6),
                "burst_factor": round(burst, 6),
                "dominant_failure_mode": dominant_mode,
                "dominant_dependency_target": dominant_dep,
                "failure_modes": sorted(set(mode_ctr.keys())),
                "dependency_targets": sorted(t for t in set(dep_ctr.keys()) if t != "none"),
                "episode_score": score,
            }
        )
    return out


def run_incident_detection_v2(
    cluster_trigger_stats_path: str,
    output_path: str,
    events_path: str,
    event_cluster_map_path: str,
    gap_seconds: int = 30,
    max_seeds: int = 3,
    intra_cluster_gap_seconds: int = 60,
    episode_score_threshold: float = 0.45,
    inter_episode_gap_seconds: int = 120,
    max_incident_duration_seconds: int = 14400,
    semantic_jaccard_threshold: float = 0.3,
    status_output_path: str | None = None,
) -> List[Dict[str, Any]]:
    with open(cluster_trigger_stats_path, "r", encoding="utf-8") as f:
        stats: Dict[str, Any] = json.load(f)
    with open(event_cluster_map_path, "r", encoding="utf-8") as f:
        event_cluster_map: Dict[str, str] = json.load(f)
    events: List[Dict[str, Any]] = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                events.append(json.loads(line))

    # Baseline failure eps for intensity normalization.
    all_fail_ts = []
    for e in events:
        if _is_failure_event(e):
            ts = _parse_ts(e.get("timestamp"))
            if ts:
                all_fail_ts.append(ts)
    baseline_failure_eps = 1e-6
    if all_fail_ts:
        t0, t1 = min(all_fail_ts), max(all_fail_ts)
        baseline_failure_eps = len(all_fail_ts) / max(1.0, (t1 - t0).total_seconds())

    candidate_clusters = [cid for cid, s in stats.items() if s.get("is_candidate")]
    episodes: List[Dict[str, Any]] = []
    for cid in candidate_clusters:
        episodes.extend(
            _build_episodes_for_cluster(
                cluster_id=cid,
                events=events,
                event_cluster_map=event_cluster_map,
                intra_cluster_gap_seconds=intra_cluster_gap_seconds,
                baseline_failure_eps=baseline_failure_eps,
            )
        )

    episodes = [e for e in episodes if e["episode_score"] >= episode_score_threshold]
    episodes.sort(key=lambda x: x["start"])

    if not episodes:
        out: List[Dict[str, Any]] = []
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        if status_output_path:
            with open(status_output_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "status": "no_incident",
                        "reason": "no_trigger_episodes",
                        "candidate_clusters": len(candidate_clusters),
                        "candidate_episodes": 0,
                        "incidents_count": 0,
                    },
                    f,
                    indent=2,
                )
        print("[incident_detection_v2] no trigger episodes found -> wrote empty incidents.json")
        return out

    incidents: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []
    for ep in episodes:
        if not current:
            current = [ep]
            continue
        last = current[-1]
        cur_start = current[0]["start"]
        new_end = max(last["end"], ep["end"])
        cand_duration = int((new_end - cur_start).total_seconds())
        temporal_ok = ep["start"] <= last["end"] + timedelta(seconds=max(1, inter_episode_gap_seconds))

        cur_modes = set().union(*(set(x["failure_modes"]) for x in current))
        nxt_modes = set(ep["failure_modes"])
        semantic_ok = _jaccard(cur_modes, nxt_modes) >= semantic_jaccard_threshold

        cur_deps = set().union(*(set(x["dependency_targets"]) for x in current))
        nxt_deps = set(ep["dependency_targets"])
        dep_ok = _jaccard(cur_deps, nxt_deps) >= 0.2 or (not cur_deps and not nxt_deps)

        duration_ok = cand_duration <= max_incident_duration_seconds

        if temporal_ok and semantic_ok and dep_ok and duration_ok:
            current.append(ep)
        else:
            incidents.append(current)
            current = [ep]
    if current:
        incidents.append(current)

    out: List[Dict[str, Any]] = []
    for i, inc in enumerate(incidents, start=1):
        start = min(x["start"] for x in inc)
        end = max(x["end"] for x in inc)
        duration = int((end - start).total_seconds())

        cluster_scores = defaultdict(float)
        for ep in inc:
            cluster_scores[ep["cluster_id"]] += float(ep["episode_score"])
        ranked_clusters = sorted(cluster_scores.items(), key=lambda x: x[1], reverse=True)
        seed_clusters = [
            {"cluster_id": cid, "trigger_score": round(score, 6)}
            for cid, score in ranked_clusters[:max(1, max_seeds)]
        ]
        trigger_clusters = [cid for cid, _ in ranked_clusters]

        out.append(
            {
                "incident_id": f"I{i}",
                "start_time": start.isoformat(),
                "end_time": end.isoformat(),
                "duration_seconds": duration,
                "seed_clusters": seed_clusters,
                "trigger_clusters": trigger_clusters,
                "context_clusters": [],
                "all_clusters": trigger_clusters,
                "cluster_count": len(trigger_clusters),
                "incident_version": "v2",
                "episode_count": len(inc),
            }
        )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)
    if status_output_path:
        with open(status_output_path, "w", encoding="utf-8") as f:
            json.dump(
                {
                    "status": "incident_detected",
                    "reason": "trigger_episodes_found",
                    "candidate_clusters": len(candidate_clusters),
                    "candidate_episodes": len(episodes),
                    "incidents_count": len(out),
                },
                f,
                indent=2,
            )

    print(
        f"[incident_detection_v2] candidate_clusters={len(candidate_clusters)} "
        f"episodes={len(episodes)} incidents={len(out)} -> {output_path}"
    )
    return out

