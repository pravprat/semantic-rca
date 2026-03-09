# semantic-rca/graph/rank_causes_debug_only.py
from __future__ import annotations

import json
import math
from collections import defaultdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from tools.churn_filter import expected_churn_penalty
from tools.confidence import compute_rca_confidence

HTTP_CLASS_WEIGHT = {
    "5xx": 3.0,
    "4xx": 2.0,
    "3xx": 1.2,
    "2xx": 0.8,
}


def cluster_type_weight(cluster_type: str) -> float:
    if cluster_type == "baseline":
        return -2.0
    if cluster_type == "candidate":
        return 2.0
    return 0.5  # contextual


def _parse_ts(ts: Any) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    s = ts.strip()
    try:
        if s.endswith("Z"):
            s = s[:-1] + "+00:00"
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _load_events_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


def classify_http_class(code: Any) -> str:
    try:
        c = int(code)
        if 500 <= c < 600:
            return "5xx"
        if 400 <= c < 500:
            return "4xx"
        if 300 <= c < 400:
            return "3xx"
        if 200 <= c < 300:
            return "2xx"
    except Exception:
        pass
    return "2xx"


def _cluster_member_indices(cluster: Dict[str, Any]) -> List[int]:
    idxs = cluster.get("member_indices") or cluster.get("event_indices") or []
    out: List[int] = []
    if isinstance(idxs, list):
        for x in idxs:
            try:
                out.append(int(x))
            except Exception:
                continue
    return out


def cluster_is_all_success(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> bool:
    codes: List[int] = []
    for idx in _cluster_member_indices(cluster):
        if 0 <= idx < len(events):
            ev = events[idx]
            code = ev.get("response_code") or ev.get("status") or ev.get("code")
            if code is None:
                continue
            try:
                codes.append(int(code))
            except Exception:
                continue
    return bool(codes) and all(200 <= c < 300 for c in codes)


def cluster_worst_http_class(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> str:
    worst = 200
    saw = False
    for idx in _cluster_member_indices(cluster):
        if 0 <= idx < len(events):
            ev = events[idx]
            code = ev.get("response_code") or ev.get("status") or ev.get("code")
            if code is None:
                continue
            try:
                worst = max(worst, int(code))
                saw = True
            except Exception:
                continue
    return classify_http_class(worst) if saw else "2xx"


def _cluster_first_seen(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> Optional[datetime]:
    ts_list: List[datetime] = []
    for idx in _cluster_member_indices(cluster):
        if 0 <= idx < len(events):
            dt = _parse_ts(events[idx].get("timestamp"))
            if dt:
                ts_list.append(dt)
    return min(ts_list) if ts_list else None


def _incident_window_from_incident(
    incident: Dict[str, Any],
    cluster_ids: List[str],
    clusters: Dict[str, Any],
    events: List[Dict[str, Any]],
) -> Tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
    """
    Return (start, end, abnormal_start). Prefer incident's explicit start/end if present.
    abnormal_start = earliest >=400 event time within window, else start.
    """
    inc_start = _parse_ts(incident.get("start_time"))
    inc_end = _parse_ts(incident.get("end_time"))

    all_ts: List[datetime] = []
    abnormal_ts: List[datetime] = []

    for cid in cluster_ids:
        c = clusters.get(cid, {})
        for idx in _cluster_member_indices(c):
            if not (0 <= idx < len(events)):
                continue
            ev = events[idx]
            dt = _parse_ts(ev.get("timestamp"))
            if not dt:
                continue
            if inc_start and dt < inc_start:
                continue
            if inc_end and dt > inc_end:
                continue

            all_ts.append(dt)

            code = ev.get("response_code") or ev.get("status") or ev.get("code")
            try:
                if int(code) >= 400:
                    abnormal_ts.append(dt)
            except Exception:
                pass

    if inc_start is None and all_ts:
        inc_start = min(all_ts)
    if inc_end is None and all_ts:
        inc_end = max(all_ts)

    if inc_start is None or inc_end is None:
        return None, None, None

    abnormal_start = min(abnormal_ts) if abnormal_ts else inc_start
    return inc_start, inc_end, abnormal_start


def _trigger_proximity_score(
    cluster: Dict[str, Any],
    incident_start: Optional[datetime],
    incident_end: Optional[datetime],
    abnormal_start: Optional[datetime],
    events: List[Dict[str, Any]],
) -> float:
    """
    0..1: 1 means cluster appears closest to abnormal_start.
    """
    if incident_start is None or incident_end is None or abnormal_start is None:
        return 0.0
    first_seen = _cluster_first_seen(cluster, events)
    if first_seen is None:
        return 0.0
    window = max(60.0, (incident_end - incident_start).total_seconds())
    dt_sec = max(0.0, (first_seen - abnormal_start).total_seconds())
    return 1.0 - min(1.0, dt_sec / window)


def _normalize_graph_precedes(graph: Dict[str, Any]) -> Tuple[Dict[str, float], Dict[str, float], Dict[str, int]]:
    """
    Returns out_weight, in_weight, out_degree for precedes edges.
    """
    out_w = defaultdict(float)
    in_w = defaultdict(float)
    out_deg = defaultdict(int)

    for e in graph.get("edges", []):
        rel = e.get("relation")
        if rel is not None and rel != "precedes":
            continue
        src = e.get("src") or e.get("from")
        dst = e.get("dst") or e.get("to")
        if not src or not dst:
            continue
        w = float(e.get("weight", 1.0))
        out_w[str(src)] += w
        in_w[str(dst)] += w
        out_deg[str(src)] += 1

    return dict(out_w), dict(in_w), dict(out_deg)

######### added for churn filter and confidence ################
def response_code_weight(http_class_counts: Dict) -> float:
    """
    Bias severe failures above noisy 2xx/404 churn.
    """
    http_class_counts = http_class_counts or {}

    score = 0.0
    score += http_class_counts.get("5xx", 0) * 2.5
    score += http_class_counts.get("4xx", 0) * 1.0
    score += http_class_counts.get("2xx", 0) * 0.05

    return score


def temporal_consistency_penalty(
    cluster_first_seen,
    incident_start,
    downstream_first_seen_list,
) -> float:
    """
    Penalize candidates that appear after most downstream effects.
    """
    if not cluster_first_seen or not incident_start:
        return 0.0

    if not downstream_first_seen_list:
        return 0.0

    later_count = sum(1 for ts in downstream_first_seen_list if ts and cluster_first_seen > ts)
    ratio = later_count / max(len(downstream_first_seen_list), 1)

    if ratio >= 0.75:
        return 12.0
    if ratio >= 0.50:
        return 6.0
    if ratio >= 0.25:
        return 3.0

    return 0.0
################ended ######################################

def rank_clusters_debug(
    *,
    graph: Dict[str, Any],
    clusters: Dict[str, Any],
    events: List[Dict[str, Any]],
    trigger_stats: Optional[Dict[str, Any]] = None,
    incident: Optional[Dict[str, Any]] = None,
    top_k: int = 20,
) -> List[Dict[str, Any]]:
    """
    Debug-only ranking:
      - structural influence (out - 0.5*in)
      - proximity to incident abnormal_start (if incident provided)
      - trigger_score / error_count (from cluster_trigger_stats.json if provided)
      - HTTP class multiplier
      - noise guards
    """
    out_w, in_w, out_deg = _normalize_graph_precedes(graph)

    # scope to incident clusters if incident provided
    if incident:
        cluster_ids = (
            incident.get("clusters")
            or incident.get("cluster_ids")
            or incident.get("cluster_id_list")
            or []
        )
        cluster_ids = [str(x) for x in cluster_ids]
        cluster_iter = {cid: clusters.get(cid, {}) for cid in cluster_ids if cid in clusters}
        inc_start, inc_end, abnormal_start = _incident_window_from_incident(incident, cluster_ids, clusters, events)
        total_events = sum(int(clusters.get(cid, {}).get("size", 0)) for cid in cluster_ids) or 1
    else:
        cluster_iter = clusters
        inc_start = inc_end = abnormal_start = None
        total_events = sum(int(c.get("size", 0)) for c in clusters.values()) or 1

    trigger_stats = trigger_stats or {}

    out: List[Dict[str, Any]] = []

    for cid, c in cluster_iter.items():
        if not isinstance(c, dict):
            continue

        size = int(c.get("size", 0) or len(_cluster_member_indices(c)))
        ctype = c.get("cluster_type", "contextual")

        ow = float(out_w.get(cid, 0.0))
        iw = float(in_w.get(cid, 0.0))
        od = int(out_deg.get(cid, 0))

        structural = (ow - 0.5 * iw) + 0.005 * size + cluster_type_weight(ctype) + 0.1 * od

        proximity = _trigger_proximity_score(c, inc_start, inc_end, abnormal_start, events) if incident else 0.0

        ts = trigger_stats.get(cid, {}) if isinstance(trigger_stats, dict) else {}
        trig = float(ts.get("trigger_score", 0.0))
        err = int(ts.get("error_count", 0))
        trig_sev = str(ts.get("severity", ""))

        http_class = cluster_worst_http_class(c, events)
        http_mult = HTTP_CLASS_WEIGHT.get(http_class, 1.0)

        all_success = cluster_is_all_success(c, events)
        success_guard = 0.2 if (all_success and ow <= 0.0) else 1.0

        # ---------------------------------------------
        # NEW ENTERPRISE HARDENING SIGNALS
        # ---------------------------------------------

        cluster_events = []
        for idx in _cluster_member_indices(c):
            if 0 <= idx < len(events):
                cluster_events.append(events[idx])

        churn_pen = expected_churn_penalty(cluster_events)

        # HTTP response weighting
        http_class_counts = {
            "5xx": 0,
            "4xx": 0,
            "3xx": 0,
            "2xx": 0,
        }

        for ev in cluster_events:
            code = ev.get("response_code") or ev.get("status") or ev.get("code")
            cls = classify_http_class(code)
            http_class_counts[cls] += 1

        resp_weight = response_code_weight(http_class_counts)

        # temporal consistency check
        cluster_first_seen = _cluster_first_seen(c, events)

        downstream_first_seen = []
        for e in graph.get("edges", []):
            src = e.get("src") or e.get("from")
            dst = e.get("dst") or e.get("to")

            if src == cid and dst in clusters:
                ts = _cluster_first_seen(clusters.get(dst), events)
                if ts:
                    downstream_first_seen.append(ts)

        temp_pen = temporal_consistency_penalty(
            cluster_first_seen,
            inc_start,
            downstream_first_seen,
        )

        baseline_rate = size / float(total_events)
        background_penalty = 0.3 if (baseline_rate > 0.05 and ow <= 0.0) else 1.0

        # ---------------------------------------------------------
        # anomaly signal (primary RCA signal)
        # ---------------------------------------------------------

        anomaly_score = (trig * 4.0) + (math.log1p(err) * 3.0) + resp_weight

        # stronger weight for real failures
        if http_class == "5xx":
            anomaly_score *= 2.0

        # ---------------------------------------------------------
        # temporal signal
        # ---------------------------------------------------------

        temporal_score = proximity * 3.0

        # ---------------------------------------------------------
        # structural signal (graph influence)
        # ---------------------------------------------------------

        structural_raw = ow - (0.5 * iw)

        # graph structure should not dominate anomaly signals
        structural_score = min(structural_raw, 2.0)

        # ---------------------------------------------------------
        # downstream blast radius
        # ---------------------------------------------------------

        blast_radius_score = len(downstream_first_seen) * 0.5

        # ---------------------------------------------------------
        # combine signals
        # ---------------------------------------------------------

        score = (
            anomaly_score
            + temporal_score
            + structural_score
            + blast_radius_score
        )


        # ---------------------------------------------------------
        # existing multipliers and penalties
        # ---------------------------------------------------------

        score = score * http_mult * success_guard * background_penalty
        score = score - churn_pen - temp_pen

        # very small clusters should not win RCA
        if err < 10:
            score *= 0.10
        if size < 10:
            score *= 0.50

        temporal_consistent = temp_pen == 0

        confidence = compute_rca_confidence(
            trigger_score=trig,
            error_count=err,
            out_weight=ow,
            in_weight=iw,
            trigger_proximity=proximity,
            temporal_consistent=temporal_consistent,
            churn_penalty=churn_pen,
        )

        out.append(
            {
                "cluster_id": cid,
                "score": float(score),
                "confidence": confidence,
                "score_breakdown": {
                    "structural": round(structural, 3),
                    "anomaly_score": round(anomaly_score, 3),
                    "temporal_score": round(temporal_score, 3),
                    "blast_radius_score": round(blast_radius_score, 3),
                    "response_weight": round(resp_weight, 3),
                    "expected_churn_penalty": round(churn_pen, 3),
                    "temporal_penalty": round(temp_pen, 3),
                },
                "size": int(size),
                "cluster_type": ctype,
                "out_precedes_weight": float(ow),
                "in_precedes_weight": float(iw),
                "out_degree": int(od),
                "http_class": http_class,
                "http_multiplier": float(http_mult),
                "trigger_proximity": float(proximity),
                "trigger_score": float(trig),
                "error_count": int(err),
                "trigger_severity": trig_sev,
                "baseline_rate": float(baseline_rate),
                "success_guard": float(success_guard),
                "background_penalty": float(background_penalty),
            }
        )

    out.sort(key=lambda x: x["confidence"], reverse=True)
    return out[:top_k]


def rank_causes_from_outputs(
    *,
    outputs_dir: Path,
    top_k: int = 20,
    incident_id: Optional[str] = None,
    write_path: Optional[Path] = None,
) -> List[Dict[str, Any]]:
    """
    Convenience entrypoint for CLI/debug:
      - Loads outputs/
      - If incident_id is provided, scopes to that incident clusters.
      - Optionally writes root_cause_candidates.json (debug only).
    """
    outputs_dir = Path(outputs_dir)

    clusters = json.loads((outputs_dir / "clusters.json").read_text(encoding="utf-8"))
    graph = json.loads((outputs_dir / "graph.json").read_text(encoding="utf-8"))
    events = _load_events_jsonl(outputs_dir / "events.jsonl")

    trigger_stats_path = outputs_dir / "cluster_trigger_stats.json"
    trigger_stats = json.loads(trigger_stats_path.read_text(encoding="utf-8")) if trigger_stats_path.exists() else {}

    incident = None
    if incident_id:
        incs = json.loads((outputs_dir / "incidents.json").read_text(encoding="utf-8"))
        if isinstance(incs, list):
            for inc in incs:
                if str(inc.get("incident_id")) == str(incident_id):
                    incident = inc
                    break

    ranked = rank_clusters_debug(
        graph=graph,
        clusters=clusters,
        events=events,
        trigger_stats=trigger_stats,
        incident=incident,
        top_k=top_k,
    )

    if write_path:
        write_path = Path(write_path)
        write_path.write_text(json.dumps(ranked, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")

    return ranked