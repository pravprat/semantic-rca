# semantic-rca/incident_rca.py
from __future__ import annotations

import json
import statistics
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Dict, List, Any, Tuple, Optional
import math
from graph.cluster_behavior import extract_cluster_behavior


@dataclass
class IncidentRcaCandidate:
    cluster_id: str
    score: float
    cluster_type: str
    size: int
    out_weight: float
    in_weight: float
    representative_index: int
    evidence_neighbors: List[Dict[str, Any]]

    # Kept (existing)
    severity_counts: Dict[str, int]
    change_score: Optional[float]

    # Deterministic RCA signals
    trigger_proximity: float
    incident_start: Optional[str]
    abnormal_start: Optional[str]
    cluster_first_seen: Optional[str]

    # HTTP + noise controls
    http_class: str
    http_multiplier: float
    baseline_rate: float
    success_guard: float
    background_penalty: float

    # NEW: trigger-analysis signals (Step 4)
    trigger_score: float
    error_count: int
    trigger_severity: str

    # cluster behavior explanation
    cluster_behavior: str
    dominant_actor: str
    dominant_operation: str
    dominant_resource: str
    dominant_status: str
    frequency: int
    behavior_signature: str


# ---------------------------------------------------------------------
# Weights / knobs (keep small, deterministic, easy to tune)
# ---------------------------------------------------------------------

HTTP_CLASS_WEIGHT = {
    "5xx": 3.0,
    "4xx": 2.0,
    "3xx": 1.2,
    "2xx": 0.8,
}

# Step 4 influence in RCA
TRIGGER_SCORE_WEIGHT = 2.0
ERROR_COUNT_WEIGHT = 0.4  # capped effect via min()
PROXIMITY_WEIGHT = 3.0

# structural score is already meaningful, keep it moderate
STRUCTURAL_WEIGHT = 1.0


def cluster_type_weight(cluster_type: str) -> float:
    """
    Penalize contextual clusters so background traffic
    cannot dominate RCA ranking.
    """
    if cluster_type == "candidate":
        return 2.0

    if cluster_type == "baseline":
        return -2.0

    # contextual
    return -1.5


# ---------------------------------------------------------------------
# Timestamp helpers
# ---------------------------------------------------------------------

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


def _parse_ts_to_float(ts: Any) -> Optional[float]:
    dt = _parse_ts(ts)
    return dt.timestamp() if dt else None


def _iso(dt: Optional[datetime]) -> Optional[str]:
    if dt is None:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


# ---------------------------------------------------------------------
# Event/cluster helpers
# ---------------------------------------------------------------------

def _http_class(code: Any) -> str:
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


def _worst_http_class_for_cluster(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> str:
    worst = 200
    saw = False
    for idx in _cluster_member_indices(cluster):
        if not (0 <= idx < len(events)):
            continue
        ev = events[idx]
        code = ev.get("response_code") or ev.get("status") or ev.get("code")
        if code is None:
            continue
        try:
            worst = max(worst, int(code))
            saw = True
        except Exception:
            continue
    return _http_class(worst) if saw else "2xx"


def _cluster_is_all_success(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> bool:
    codes: List[int] = []
    for idx in _cluster_member_indices(cluster):
        if not (0 <= idx < len(events)):
            continue
        ev = events[idx]
        code = ev.get("response_code") or ev.get("status") or ev.get("code")
        if code is None:
            continue
        try:
            codes.append(int(code))
        except Exception:
            continue
    return bool(codes) and all(200 <= c < 300 for c in codes)


def _cluster_first_seen(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> Optional[datetime]:
    ts_list: List[datetime] = []
    for idx in _cluster_member_indices(cluster):
        if not (0 <= idx < len(events)):
            continue
        dt = _parse_ts(events[idx].get("timestamp"))
        if dt:
            ts_list.append(dt)
    return min(ts_list) if ts_list else None


def _severity_counts_for_cluster(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> Dict[str, int]:
    counts = {"ERROR": 0, "WARN": 0, "INFO": 0, "OTHER": 0}
    for idx in _cluster_member_indices(cluster):
        if not (0 <= idx < len(events)):
            continue
        ev = events[idx]
        lvl = (ev.get("level") or ev.get("severity") or "").strip().upper()
        if lvl == "WARNING":
            lvl = "WARN"
        if lvl in ("ERROR", "WARN", "INFO"):
            counts[lvl] += 1
        else:
            counts["OTHER"] += 1
    return counts


def _cluster_member_indices(c: Dict[str, Any]) -> List[int]:
    idxs = c.get("member_indices") or c.get("event_indices") or []
    out: List[int] = []
    if isinstance(idxs, list):
        for x in idxs:
            if isinstance(x, int):
                out.append(x)
            else:
                try:
                    out.append(int(x))
                except Exception:
                    continue
    return out


def _choose_representative_index(c: Dict[str, Any], events: List[Dict[str, Any]]) -> int:
    rep = c.get("representative_index")
    try:
        rep_i = int(rep)
        if 0 <= rep_i < len(events):
            return rep_i
    except Exception:
        pass

    for idx in _cluster_member_indices(c):
        if 0 <= idx < len(events):
            ev = events[idx]
            txt = ev.get("raw_text") or ev.get("message") or ev.get("msg") or ev.get("text")
            if isinstance(txt, str) and txt.strip():
                return idx

    idxs = _cluster_member_indices(c)
    return idxs[0] if idxs and 0 <= idxs[0] < len(events) else -1


# ---------------------------------------------------------------------
# Change score (kept)
# ---------------------------------------------------------------------

def _split_ts_for_incident(
    incident_clusters: set,
    clusters_by_id: Dict[str, Any],
    events: List[Dict[str, Any]],
) -> Optional[float]:
    ts_vals: List[float] = []
    for cid in sorted(incident_clusters):
        c = clusters_by_id.get(cid, {})
        for idx in _cluster_member_indices(c):
            if not (0 <= idx < len(events)):
                continue
            ts = _parse_ts_to_float(events[idx].get("timestamp"))
            if ts is not None:
                ts_vals.append(ts)
    if not ts_vals:
        return None
    return float(statistics.median(ts_vals))


def compute_change_score(cluster: Dict[str, Any], events: List[Dict[str, Any]], split_ts: float) -> float:
    pre = 0
    post = 0
    for idx in _cluster_member_indices(cluster):
        if not (0 <= idx < len(events)):
            continue
        ts = _parse_ts_to_float(events[idx].get("timestamp"))
        if ts is None:
            continue
        if ts < split_ts:
            pre += 1
        else:
            post += 1
    total = pre + post
    if total == 0:
        return 0.0
    return (post - pre) / float(total)


# ---------------------------------------------------------------------
# Incident window helpers (NEW pipeline: use incident start/end if present)
# ---------------------------------------------------------------------

def _incident_clusters_from_incident(incident: Dict[str, Any]) -> List[str]:
    # tolerate a few shapes
    for key in ("clusters", "cluster_ids", "cluster_id_list"):
        v = incident.get(key)
        if isinstance(v, list):
            return [str(x) for x in v]
    # fallback: sometimes seed only
    seed = incident.get("seed_cluster")
    return [str(seed)] if seed else []


def _incident_window_from_incident(
    incident: Dict[str, Any],
    incident_clusters: set,
    clusters_by_id: Dict[str, Any],
    events: List[Dict[str, Any]],
) -> Tuple[Optional[datetime], Optional[datetime], Optional[datetime]]:
    """
    Returns (incident_start, incident_end, abnormal_start).

    Priority:
      1) use incident.start_time/end_time if present (new incident_detection output)
      2) else fall back to computing from cluster member events
    abnormal_start = earliest timestamp among 'abnormal' events (>=400 or ERROR/WARN),
    else equals incident_start.
    """
    inc_start = _parse_ts(incident.get("start_time"))
    inc_end = _parse_ts(incident.get("end_time"))

    all_ts: List[datetime] = []
    abnormal_ts: List[datetime] = []

    for cid in sorted(incident_clusters):
        c = clusters_by_id.get(cid, {})
        for idx in _cluster_member_indices(c):
            if not (0 <= idx < len(events)):
                continue
            ev = events[idx]
            dt = _parse_ts(ev.get("timestamp"))
            if not dt:
                continue

            # if incident has explicit window, ignore events outside it
            if inc_start and dt < inc_start:
                continue
            if inc_end and dt > inc_end:
                continue

            all_ts.append(dt)

            code = ev.get("response_code") or ev.get("status") or ev.get("code")
            is_abnormal = False
            try:
                is_abnormal = int(code) >= 400
            except Exception:
                lvl = (ev.get("level") or ev.get("severity") or "").strip().upper()
                if lvl in ("ERROR", "WARN", "WARNING"):
                    is_abnormal = True

            if is_abnormal:
                abnormal_ts.append(dt)

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
    events: List[Dict[str, Any]],
    incident_start: Optional[datetime],
    incident_end: Optional[datetime],
    abnormal_start: Optional[datetime],
) -> float:
    if incident_start is None or incident_end is None or abnormal_start is None:
        return 0.0

    first_seen = _cluster_first_seen(cluster, events)
    if first_seen is None:
        return 0.0

    window = max(60.0, (incident_end - incident_start).total_seconds())
    dt_sec = max(0.0, (first_seen - abnormal_start).total_seconds())
    return 1.0 - min(1.0, dt_sec / window)


# ---------------------------------------------------------------------
# IO helpers
# ---------------------------------------------------------------------

def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_events_jsonl(path: Path) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


# ---------------------------------------------------------------------
# Graph normalization (keep only precedes edges)
# ---------------------------------------------------------------------

def _normalize_graph(graph_obj: Any) -> Tuple[List[str], List[Dict[str, Any]]]:
    nodes: List[str] = []
    edges_out: List[Dict[str, Any]] = []

    if isinstance(graph_obj, dict):
        if isinstance(graph_obj.get("nodes"), list):
            # nodes can be dicts {"id": ...} or raw ids
            tmp = []
            for n in graph_obj["nodes"]:
                if isinstance(n, dict) and "id" in n:
                    tmp.append(str(n["id"]))
                else:
                    tmp.append(str(n))
            nodes = tmp

        edges = graph_obj.get("edges", [])
        if isinstance(edges, list):
            for e in edges:
                if not isinstance(e, dict):
                    continue
                rel = e.get("relation")
                if rel is not None and rel != "precedes":
                    continue
                src = e.get("src") or e.get("from")
                dst = e.get("dst") or e.get("to")
                if not src or not dst:
                    continue
                edges_out.append(
                    {"src": str(src), "dst": str(dst), "weight": float(e.get("weight", 1.0))}
                )

    return nodes, edges_out


# ---------------------------------------------------------------------
# Incident subgraph utilities
# ---------------------------------------------------------------------

def _incident_subgraph_edges(
    incident_clusters: set,
    all_edges: List[Dict[str, Any]]
) -> List[Dict[str, Any]]:

    sub_edges = []

    for e in all_edges:
        src = e.get("src")
        if src in incident_clusters:
            sub_edges.append(e)

    return sub_edges

def _compute_in_out_weights(
    incident_clusters: set,
    edges: List[Dict[str, Any]],
) -> Tuple[Dict[str, float], Dict[str, float]]:

    in_w = {cid: 0.0 for cid in incident_clusters}
    out_w = {cid: 0.0 for cid in incident_clusters}

    for e in edges:
        src, dst, w = e["src"], e["dst"], float(e.get("weight", 1.0))

        if src in out_w:
            out_w[src] += w

        if dst in in_w:
            in_w[dst] += w

    return in_w, out_w

def _top_evidence_neighbors(cid: str, edges: List[Dict[str, Any]], k: int = 5) -> List[Dict[str, Any]]:
    outs = [e for e in edges if e["src"] == cid]
    outs.sort(
        key=lambda x: (-x["weight"], x["dst"])
    )
    return [{"cluster_id": e["dst"], "weight": float(e["weight"])} for e in outs[:k]]


# ---------------------------------------------------------------------
# Step 7: Per-incident ranking (uses Step 4 trigger stats)
# ---------------------------------------------------------------------

def rank_root_causes_for_incident(
    incident: Dict[str, Any],
    clusters_by_id: Dict[str, Any],
    graph_edges: List[Dict[str, Any]],
    events: List[Dict[str, Any]],
    trigger_stats_by_cluster: Dict[str, Any],
    top_k: int = 5,
) -> List[IncidentRcaCandidate]:


    incident_clusters = sorted(set(_incident_clusters_from_incident(incident)))
    if not incident_clusters:
        return []

    sub_edges = _incident_subgraph_edges(incident_clusters, graph_edges)
    in_w, out_w = _compute_in_out_weights(incident_clusters, sub_edges)

    from collections import defaultdict

    edges_by_src = defaultdict(list)

    for e in graph_edges:
        src = e.get("src")
        if src:
            edges_by_src[src].append(e)

    # incident window + abnormal onset
    incident_start, incident_end, abnormal_start = _incident_window_from_incident(
        incident, incident_clusters, clusters_by_id, events
    )

    # change detection split
    split_ts = _split_ts_for_incident(incident_clusters, clusters_by_id, events)

    # for baseline/noise: baseline_rate relative to incident mass
    total_incident_events = 0
    for cid in sorted(incident_clusters):
        c = clusters_by_id.get(cid, {})
        member_indices = _cluster_member_indices(c)
        total_incident_events += int(c.get("size") or len(member_indices) or 0)
    total_incident_events = total_incident_events or 1

    candidates: List[IncidentRcaCandidate] = []

    # -------------------------------------------------
    # Filter clusters to anomalous candidates
    # -------------------------------------------------
    candidate_clusters = [
        cid for cid in incident_clusters
        if (
                trigger_stats_by_cluster.get(cid, {}).get("trigger_score", 0) > 0.5
                or trigger_stats_by_cluster.get(cid, {}).get("error_count", 0) > 5
        )
    ]

    # fallback if filter removes everything
    if not candidate_clusters:
        candidate_clusters = incident_clusters

    for cid in sorted(candidate_clusters):
        c = clusters_by_id.get(cid, {})
        ctype = c.get("cluster_type", "contextual")
        member_indices = _cluster_member_indices(c)
        size = int(c.get("size") or len(member_indices) or 0)
        rep_idx = _choose_representative_index(c, events)
        if rep_idx < 0 or rep_idx >= len(events):
            rep_idx = -1

        # ---------------------------------
        # cluster behavior extraction
        # ---------------------------------

        behavior = extract_cluster_behavior(c, events)

        cluster_behavior = behavior["cluster_behavior"]
        dominant_actor = behavior["dominant_actor"]
        dominant_operation = behavior["dominant_operation"]
        dominant_resource = behavior["dominant_resource"]
        dominant_status = behavior["dominant_status"]
        frequency = behavior["frequency"]
        behavior_signature = behavior["behavior_signature"]

        # ---------------------------------
        # downstream blast radius score
        # ---------------------------------

        # unique clusters only
        downstream_unique = {
            e["dst"]
            for e in edges_by_src.get(cid, [])
            if e.get("weight", 0) > 1.0
        }

        # reduce blast radius influence
        downstream_score = min(4.0, len(downstream_unique) * 0.4)

        # -----------------------------
        # structural score (graph + type + size scaling)
        # -----------------------------

        size_score = 0.1 * math.log1p(size)

        structural_raw = (
                (out_w.get(cid, 0.0) - 0.5 * in_w.get(cid, 0.0))
                + size_score
                + cluster_type_weight(ctype)
        )

        # prevent graph structure dominating anomaly signals
        structural_score = max(0.0, min(structural_raw, 3.0))

        # -----------------------------
        # trigger proximity (time-to-onset)
        # -----------------------------
        proximity = _trigger_proximity_score(c, events, incident_start, incident_end, abnormal_start)

        cluster_first_seen = _cluster_first_seen(c, events)

        temporal_penalty = 1.0

        if cluster_first_seen and incident_start:
            if cluster_first_seen < incident_start:
                temporal_penalty = 0.4

        # -----------------------------
        # severity from log levels (kept, reduced impact)
        # -----------------------------
        sev_counts = _severity_counts_for_cluster(c, events)
        total = max(1, size)
        error_ratio = sev_counts.get("ERROR", 0) / total
        warn_ratio = sev_counts.get("WARN", 0) / total
        info_ratio = sev_counts.get("INFO", 0) / total
        severity_factor = (2.0 * error_ratio) + (1.0 * warn_ratio) - (0.7 * info_ratio)
        level_mult = max(0.5, min(2.0, 1.0 + 0.25 * severity_factor))

        # -----------------------------
        # change-aware multiplier (kept)
        # -----------------------------
        if split_ts is not None:
            ch = compute_change_score(c, events, split_ts)
            change_mult = max(0.25, min(2.0, 1.0 + ch))
            change_score_val: Optional[float] = float(ch)
        else:
            change_mult = 1.0
            change_score_val = None

        # -----------------------------
        # HTTP class multiplier
        # -----------------------------
        http_class = _worst_http_class_for_cluster(c, events)
        http_mult = HTTP_CLASS_WEIGHT.get(http_class, 1.0)

        # -----------------------------
        # Step 4 trigger stats (cluster_trigger_stats.json)
        # -----------------------------
        ts = trigger_stats_by_cluster.get(cid, {}) if isinstance(trigger_stats_by_cluster, dict) else {}
        trigger_score = float(ts.get("trigger_score", 0.0))
        error_count = int(ts.get("error_count", 0))
        trig_sev = str(ts.get("severity", ""))

        # -----------------------------
        # success cluster guard
        # -----------------------------
        all_success = _cluster_is_all_success(c, events)
        success_guard = 0.2 if (all_success and out_w.get(cid, 0.0) <= 0.0) else 1.0

        # -----------------------------
        # background penalty (high freq + low influence)
        # -----------------------------
        baseline_rate = size / float(total_incident_events)
        background_penalty = 0.3 if (baseline_rate > 0.05 and out_w.get(cid, 0.0) <= 0.0) else 1.0

        # -----------------------------
        # final score: causal + onset + trigger
        # -----------------------------
        score = (
                STRUCTURAL_WEIGHT * (structural_score * change_mult * level_mult)
                + PROXIMITY_WEIGHT * proximity
                + TRIGGER_SCORE_WEIGHT * trigger_score
                + (ERROR_COUNT_WEIGHT * math.sqrt(error_count))
                + downstream_score
        )

        score = score * http_mult * success_guard * background_penalty * temporal_penalty

        candidates.append(
            IncidentRcaCandidate(
                cluster_id=cid,
                score=float(score),
                cluster_type=ctype,
                size=size,
                out_weight=float(out_w.get(cid, 0.0)),
                in_weight=float(in_w.get(cid, 0.0)),
                representative_index=rep_idx,
                evidence_neighbors=_top_evidence_neighbors(cid, sub_edges),
                severity_counts=sev_counts,
                change_score=change_score_val,
                trigger_proximity=float(proximity),
                incident_start=_iso(incident_start),
                abnormal_start=_iso(abnormal_start),
                cluster_first_seen=_iso(_cluster_first_seen(c, events)),
                http_class=str(http_class),
                http_multiplier=float(http_mult),
                baseline_rate=float(baseline_rate),
                success_guard=float(success_guard),
                background_penalty=float(background_penalty),
                trigger_score=float(trigger_score),
                error_count=int(error_count),
                trigger_severity=trig_sev,
                cluster_behavior=cluster_behavior,
                dominant_actor=dominant_actor,
                dominant_operation=dominant_operation,
                dominant_resource=dominant_resource,
                dominant_status=dominant_status,
                frequency=frequency,
                behavior_signature=behavior_signature,
            )
        )

    candidates.sort(
        key=lambda x: (-x.score, x.cluster_id)
    )
    return candidates[:top_k]


# ---------------------------------------------------------------------
# Public API (Step 7 entry)
# ---------------------------------------------------------------------

def build_incident_root_causes(
    outputs_dir: Path,
    incidents_path: Optional[Path] = None,
    top_k_per_incident: int = 5,
) -> Dict[str, Any]:

    outputs_dir = Path(outputs_dir)
    if not outputs_dir.exists():
        raise FileNotFoundError(f"outputs_dir does not exist: {outputs_dir}")

    # NEW pipeline default: incidents.json
    if incidents_path is None:
        incidents_path = outputs_dir / "incidents.json"

    clusters_path = outputs_dir / "clusters.json"
    graph_path = outputs_dir / "graph.json"
    events_path = outputs_dir / "events.jsonl"
    trigger_stats_path = outputs_dir / "cluster_trigger_stats.json"

    for p in [clusters_path, graph_path, incidents_path, events_path]:
        if not p.exists():
            raise FileNotFoundError(f"Required artifact missing: {p}")

    clusters_by_id = _load_json(clusters_path)
    graph_obj = _load_json(graph_path)
    _, edges = _normalize_graph(graph_obj)
    edges = sorted(edges, key=lambda e:(e.get("src",""), e.get("dst","")))
    incidents = _load_json(incidents_path)
    events = _load_events_jsonl(events_path)
    # Load trigger statistics
    trigger_stats_path = outputs_dir / "cluster_trigger_stats.json"


    if not isinstance(incidents, list):
        raise ValueError("incidents.json must be a list")

    trigger_stats_by_cluster: Dict[str, Any] = {}
    if trigger_stats_path.exists():
        obj = _load_json(trigger_stats_path)
        if isinstance(obj, dict):
            trigger_stats_by_cluster = obj

    out: Dict[str, Any] = {"incidents": []}

    for idx, inc in enumerate(incidents, start=1):
        ranked = rank_root_causes_for_incident(
            incident=inc,
            clusters_by_id=clusters_by_id,
            graph_edges=edges,
            events=events,
            trigger_stats_by_cluster=trigger_stats_by_cluster,
            top_k=top_k_per_incident,
        )

        rc_list: List[Dict[str, Any]] = []
        for cand in ranked:
            rep_text = None
            if 0 <= cand.representative_index < len(events):
                ev = events[cand.representative_index]
                rep_text = (
                    ev.get("raw_text")
                    or ev.get("message")
                    or ev.get("msg")
                    or ev.get("text")
                    or ev.get("log")
                    or ev.get("body")
                    or ev.get("line")
                    or ""
                )

            rc_list.append(
                {
                    "cluster_id": cand.cluster_id,
                    "score": cand.score,
                    "cluster_type": cand.cluster_type,
                    "size": cand.size,
                    "in_weight": cand.in_weight,
                    "out_weight": cand.out_weight,
                    "representative_index": cand.representative_index,
                    "representative_raw_text": rep_text,
                    "evidence_neighbors": cand.evidence_neighbors,
                    "downstream_neighbors": cand.evidence_neighbors,

                    # kept
                    "severity_counts": cand.severity_counts,
                    "change_score": cand.change_score,

                    # deterministic signals
                    "trigger_proximity": cand.trigger_proximity,
                    "incident_start": cand.incident_start,
                    "abnormal_start": cand.abnormal_start,
                    "cluster_first_seen": cand.cluster_first_seen,

                    # http/noise
                    "http_class": cand.http_class,
                    "http_multiplier": cand.http_multiplier,
                    "baseline_rate": cand.baseline_rate,
                    "success_guard": cand.success_guard,
                    "background_penalty": cand.background_penalty,

                    # step4 trigger stats
                    "trigger_score": cand.trigger_score,
                    "error_count": cand.error_count,
                    "trigger_severity": cand.trigger_severity,

                    # cluster behavior
                    "cluster_behavior": cand.cluster_behavior,
                    "dominant_actor": cand.dominant_actor,
                    "dominant_operation": cand.dominant_operation,
                    "dominant_resource": cand.dominant_resource,
                    "dominant_status": cand.dominant_status,
                    "frequency": cand.frequency,
                    "behavior_signature": cand.behavior_signature,
                }
            )

        incident_id = inc.get("incident_id") or f"I{idx}"

        out["incidents"].append(
            {
                "incident_id": str(incident_id),
                "seed_cluster": inc.get("seed_cluster"),
                "clusters": _incident_clusters_from_incident(inc),
                "start_time": inc.get("start_time"),
                "end_time": inc.get("end_time"),
                "root_cause_candidates": rc_list,
            }
        )

    return out