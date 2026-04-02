from __future__ import annotations

import json
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set


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


def _safe_float(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _jaccard(a: Set[str], b: Set[str]) -> float:
    if not a and not b:
        return 1.0
    if not a or not b:
        return 0.0
    return len(a & b) / max(1, len(a | b))


def _extract_modes(stat: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    for row in stat.get("top_failure_modes", []) or []:
        mode = row.get("mode")
        if mode:
            out.add(str(mode))
    if not out:
        for hint in stat.get("top_failure_hints", []) or []:
            out.add(str(hint))
    return out


def _extract_targets(stat: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    for row in stat.get("dependency_targets", []) or []:
        svc = row.get("service")
        if svc:
            out.add(str(svc))
    return out


def _extract_actor_resource_tokens(stat: Dict[str, Any]) -> Set[str]:
    out: Set[str] = set()
    for row in stat.get("top_actors", []) or []:
        a = row.get("actor")
        if a:
            out.add(f"actor:{a}")
    for row in stat.get("top_resources", []) or []:
        r = row.get("resource")
        if r:
            out.add(f"resource:{r}")
    if not out:
        if stat.get("actor"):
            out.add(f"actor:{stat.get('actor')}")
        if stat.get("resource"):
            out.add(f"resource:{stat.get('resource')}")
    return out


def _build_cluster_windows(
    stats: Dict[str, Any],
    cluster_window_cap_seconds: int,
) -> List[Dict[str, Any]]:
    windows: List[Dict[str, Any]] = []
    for cid, s in stats.items():
        if not s.get("is_candidate"):
            continue
        start = _parse_ts(s.get("first_seen"))
        end = _parse_ts(s.get("last_seen"))
        if not start or not end:
            continue
        capped_end = end
        if cluster_window_cap_seconds > 0:
            cap_end = start + timedelta(seconds=cluster_window_cap_seconds)
            if cap_end < capped_end:
                capped_end = cap_end
        if capped_end < start:
            continue
        windows.append(
            {
                "cluster_id": cid,
                "start": start,
                "end": capped_end,
                "trigger_score": _safe_float(s.get("trigger_score")),
                "stat": s,
            }
        )
    windows.sort(key=lambda x: x["start"])
    return windows


def _build_episodes(
    windows: List[Dict[str, Any]],
    episode_gap_seconds: int,
    max_episode_duration_seconds: int,
) -> List[List[Dict[str, Any]]]:
    episodes: List[List[Dict[str, Any]]] = []
    current: List[Dict[str, Any]] = []

    for w in windows:
        if not current:
            current = [w]
            continue
        last = current[-1]
        cur_start = current[0]["start"]
        candidate_end = max(last["end"], w["end"])
        candidate_duration = int((candidate_end - cur_start).total_seconds())
        contiguous = w["start"] <= last["end"] + timedelta(seconds=max(1, episode_gap_seconds))
        within_duration = (
            max_episode_duration_seconds <= 0
            or candidate_duration <= max_episode_duration_seconds
        )
        if contiguous and within_duration:
            current.append(w)
        else:
            episodes.append(current)
            current = [w]
    if current:
        episodes.append(current)
    return episodes


def _episode_signature(episode: List[Dict[str, Any]]) -> Dict[str, Any]:
    modes: Set[str] = set()
    targets: Set[str] = set()
    tokens: Set[str] = set()
    score_sum = 0.0
    for w in episode:
        s = w["stat"]
        modes |= _extract_modes(s)
        targets |= _extract_targets(s)
        tokens |= _extract_actor_resource_tokens(s)
        score_sum += _safe_float(w.get("trigger_score"))
    return {
        "modes": modes,
        "targets": targets,
        "tokens": tokens,
        "score_sum": score_sum,
    }


def run_incident_detection_v2(
    cluster_trigger_stats_path: str,
    output_path: str,
    gap_seconds: int = 30,
    max_seeds: int = 3,
    cluster_window_cap_seconds: int = 900,
    max_incident_duration_seconds: int = 3600,
    episode_gap_seconds: int = 120,
    max_episode_duration_seconds: int = 1200,
    semantic_jaccard_threshold: float = 0.35,
    status_output_path: str | None = None,
) -> List[Dict[str, Any]]:
    with open(cluster_trigger_stats_path, "r", encoding="utf-8") as f:
        stats: Dict[str, Any] = json.load(f)

    windows = _build_cluster_windows(stats, cluster_window_cap_seconds)
    if not windows:
        out: List[Dict[str, Any]] = []
        with open(output_path, "w", encoding="utf-8") as f:
            json.dump(out, f, indent=2)
        if status_output_path:
            with open(status_output_path, "w", encoding="utf-8") as f:
                json.dump(
                    {
                        "status": "no_incident",
                        "reason": "no_trigger_clusters",
                        "candidate_clusters": 0,
                        "candidate_episodes": 0,
                        "incidents_count": 0,
                    },
                    f,
                    indent=2,
                )
        print("[incident_detection_v2] no trigger clusters found -> wrote empty incidents.json")
        return out

    episodes = _build_episodes(
        windows=windows,
        episode_gap_seconds=episode_gap_seconds,
        max_episode_duration_seconds=max_episode_duration_seconds,
    )

    # Merge episodes into incidents with deterministic multi-gate checks.
    incidents: List[List[List[Dict[str, Any]]]] = []
    current: List[List[Dict[str, Any]]] = []
    for ep in episodes:
        if not current:
            current = [ep]
            continue
        last_ep = current[-1]
        last_end = max(x["end"] for x in last_ep)
        ep_start = min(x["start"] for x in ep)
        temporal_ok = ep_start <= last_end + timedelta(seconds=max(1, gap_seconds))

        cur_start = min(x["start"] for seg in current for x in seg)
        cand_end = max(max(x["end"] for x in seg) for seg in current + [ep])
        cand_duration = int((cand_end - cur_start).total_seconds())
        duration_ok = (
            max_incident_duration_seconds <= 0
            or cand_duration <= max_incident_duration_seconds
        )

        sig_cur = _episode_signature([x for seg in current for x in seg])
        sig_ep = _episode_signature(ep)
        mode_sim = _jaccard(sig_cur["modes"], sig_ep["modes"])
        token_sim = _jaccard(sig_cur["tokens"], sig_ep["tokens"])
        dep_sim = _jaccard(sig_cur["targets"], sig_ep["targets"])
        semantic_ok = (
            max(mode_sim, token_sim) >= semantic_jaccard_threshold
            or dep_sim >= 0.2
        )

        cur_score = max(1e-9, sig_cur["score_sum"])
        ep_score = max(1e-9, sig_ep["score_sum"])
        ratio = max(cur_score, ep_score) / min(cur_score, ep_score)
        score_continuity_ok = ratio <= 8.0

        if temporal_ok and duration_ok and semantic_ok and score_continuity_ok:
            current.append(ep)
        else:
            incidents.append(current)
            current = [ep]
    if current:
        incidents.append(current)

    # Flatten and build outputs.
    out: List[Dict[str, Any]] = []
    for i, inc_eps in enumerate(incidents, start=1):
        flat = [x for ep in inc_eps for x in ep]
        start = min(x["start"] for x in flat)
        end = max(x["end"] for x in flat)
        duration = int((end - start).total_seconds())

        score_by_cluster: Dict[str, float] = {}
        for w in flat:
            cid = w["cluster_id"]
            score_by_cluster[cid] = score_by_cluster.get(cid, 0.0) + _safe_float(w["trigger_score"])
        ranked_clusters = sorted(score_by_cluster.items(), key=lambda x: x[1], reverse=True)

        seed_clusters = [
            {"cluster_id": cid, "trigger_score": round(score, 6)}
            for cid, score in ranked_clusters[: max(1, max_seeds)]
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
                "episode_count": len(inc_eps),
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
                    "candidate_clusters": len(windows),
                    "candidate_episodes": len(episodes),
                    "incidents_count": len(out),
                },
                f,
                indent=2,
            )

    print(
        f"[incident_detection_v2] candidate_clusters={len(windows)} "
        f"episodes={len(episodes)} incidents={len(out)} -> {output_path}"
    )
    return out

