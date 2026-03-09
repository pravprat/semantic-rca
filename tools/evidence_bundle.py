from __future__ import annotations

import json
import math
from collections import Counter, defaultdict
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


# -------------------------------------------------------------------
# Generic IO
# -------------------------------------------------------------------

def _load_json(path: Path, default: Any) -> Any:
    if not path.exists():
        return default
    try:
        with path.open("r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return default


def _load_events_jsonl(path: Path) -> Dict[int, Dict[str, Any]]:
    events: Dict[int, Dict[str, Any]] = {}
    if not path.exists():
        return events

    with path.open("r", encoding="utf-8") as f:
        for line_no, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            try:
                rec = json.loads(line)
            except Exception:
                continue

            idx = rec.get("index")
            if isinstance(idx, int):
                events[idx] = rec
            else:
                events[line_no] = rec
    return events


# -------------------------------------------------------------------
# Event helpers
# -------------------------------------------------------------------

def _as_int(v: Any) -> Optional[int]:
    if isinstance(v, int):
        return v
    if isinstance(v, str) and v.strip().isdigit():
        return int(v.strip())
    return None


def _event_timestamp(rec: Dict[str, Any]) -> Optional[str]:
    for k in ("timestamp", "ts", "time"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return None


def _event_text(rec: Dict[str, Any]) -> str:
    for k in ("raw_text", "message", "msg", "text", "log"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def _event_service(rec: Dict[str, Any]) -> str:
    for k in ("service", "actor"):
        v = rec.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()

    sf = rec.get("structured_fields")
    if isinstance(sf, dict):
        for k in ("service", "actor"):
            v = sf.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()

    return ""


def _event_response_code(rec: Dict[str, Any]) -> Optional[int]:
    for k in ("response_code", "status", "code"):
        val = _as_int(rec.get(k))
        if val is not None:
            return val

    sf = rec.get("structured_fields")
    if isinstance(sf, dict):
        for k in ("response_code", "status", "code"):
            val = _as_int(sf.get(k))
            if val is not None:
                return val

    return None


def _http_class(code: Optional[int]) -> str:
    if code is None:
        return "unknown"
    if 200 <= code < 300:
        return "2xx"
    if 300 <= code < 400:
        return "3xx"
    if 400 <= code < 500:
        return "4xx"
    if 500 <= code < 600:
        return "5xx"
    return "other"


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


# -------------------------------------------------------------------
# Cluster / graph normalization
# -------------------------------------------------------------------

def _normalize_clusters(raw: Any) -> Dict[str, Dict[str, Any]]:
    if isinstance(raw, dict):
        out = {}
        for k, v in raw.items():
            if isinstance(v, dict):
                cid = str(v.get("cluster_id") or v.get("id") or k)
                out[cid] = v
        return out

    if isinstance(raw, list):
        out = {}
        for item in raw:
            if not isinstance(item, dict):
                continue
            cid = item.get("cluster_id") or item.get("id")
            if cid is not None:
                out[str(cid)] = item
        return out

    return {}


def _normalize_incident_root_causes(raw: Any) -> List[Dict[str, Any]]:
    """
    Expected stabilized shape:
    {
      "incidents": [
        {
          "incident_id": "I1",
          "root_cause_candidates": [...]
        }
      ]
    }

    Also tolerates a direct list.
    """
    if isinstance(raw, dict) and isinstance(raw.get("incidents"), list):
        return [x for x in raw["incidents"] if isinstance(x, dict)]
    if isinstance(raw, list):
        return [x for x in raw if isinstance(x, dict)]
    return []


def _compute_graph_indexes(graph: Dict[str, Any]) -> Tuple[
    Dict[str, List[Dict[str, Any]]],
    Dict[str, List[Dict[str, Any]]]
]:
    incoming: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    outgoing: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for edge in graph.get("edges", []) or []:
        if not isinstance(edge, dict):
            continue
        src = edge.get("from")
        dst = edge.get("to")
        if not src or not dst:
            continue
        outgoing[str(src)].append(edge)
        incoming[str(dst)].append(edge)

    return incoming, outgoing


# -------------------------------------------------------------------
# Evidence helpers
# -------------------------------------------------------------------

def _cluster_member_indices(cluster_obj: Dict[str, Any]) -> List[int]:
    vals = cluster_obj.get("member_indices") or []
    out: List[int] = []
    if not isinstance(vals, list):
        return out

    for v in vals:
        if isinstance(v, int):
            out.append(v)
        elif isinstance(v, str) and v.strip().isdigit():
            out.append(int(v.strip()))
    return out


def _representative_event(
    candidate: Dict[str, Any],
    cluster_obj: Dict[str, Any],
    events_index: Dict[int, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    rep_idx = candidate.get("representative_index")
    if isinstance(rep_idx, int) and rep_idx in events_index:
        return events_index[rep_idx]

    if isinstance(rep_idx, str) and rep_idx.isdigit() and int(rep_idx) in events_index:
        return events_index[int(rep_idx)]

    for idx in _cluster_member_indices(cluster_obj):
        rec = events_index.get(idx)
        if rec:
            return rec

    return None


def _earliest_event(
    cluster_obj: Dict[str, Any],
    events_index: Dict[int, Dict[str, Any]],
) -> Optional[Dict[str, Any]]:
    best: Optional[Dict[str, Any]] = None
    best_ts: Optional[str] = None

    for idx in _cluster_member_indices(cluster_obj):
        rec = events_index.get(idx)
        if not rec:
            continue
        ts = _event_timestamp(rec)
        if ts is None:
            continue
        if best_ts is None or ts < best_ts:
            best_ts = ts
            best = rec

    return best


def _cluster_summary(
    cluster_id: str,
    cluster_obj: Dict[str, Any],
    events_index: Dict[int, Dict[str, Any]],
) -> Dict[str, Any]:
    indices = _cluster_member_indices(cluster_obj)
    services = Counter()
    http_counts = Counter()
    examples: List[str] = []
    timestamps: List[str] = []

    for idx in indices[:100]:
        rec = events_index.get(idx)
        if not rec:
            continue

        svc = _event_service(rec)
        if svc:
            services[svc] += 1

        code = _event_response_code(rec)
        http_counts[_http_class(code)] += 1

        ts = _event_timestamp(rec)
        if ts:
            timestamps.append(ts)

        txt = _event_text(rec)
        if txt and txt not in examples and len(examples) < 3:
            examples.append(txt)

    timestamps = sorted(timestamps)

    return {
        "cluster_id": cluster_id,
        "size": int(cluster_obj.get("size", len(indices)) or 0),
        "cluster_type": cluster_obj.get("cluster_type"),
        "first_seen_ts": cluster_obj.get("first_seen_ts") or (timestamps[0] if timestamps else None),
        "last_seen_ts": cluster_obj.get("last_seen_ts") or (timestamps[-1] if timestamps else None),
        "top_services": [{"service": k, "count": v} for k, v in services.most_common(5)],
        "http_class_counts": dict(http_counts),
        "example_events": examples,
    }


def _candidate_confidence(candidate: Dict[str, Any]) -> Dict[str, Any]:
    """
    Heuristic confidence for explainability only.
    This should not replace the deterministic RCA score.
    """
    score = _safe_float(candidate.get("score"), 0.0)
    trigger_score = _safe_float(candidate.get("trigger_score"), 0.0)
    error_count = _safe_float(candidate.get("error_count"), 0.0)
    out_w = _safe_float(candidate.get("out_weight"), 0.0)
    in_w = _safe_float(candidate.get("in_weight"), 0.0)
    proximity = _safe_float(candidate.get("trigger_proximity"), 0.0)

    raw = (
        min(score / 100.0, 1.0) * 0.40 +
        min(trigger_score / 5.0, 1.0) * 0.20 +
        min(math.log1p(error_count) / 5.0, 1.0) * 0.15 +
        min(max(out_w - 0.5 * in_w, 0.0) / 10.0, 1.0) * 0.15 +
        min(proximity, 1.0) * 0.10
    )

    value = round(max(0.0, min(raw, 1.0)), 3)

    if value >= 0.80:
        label = "high"
    elif value >= 0.55:
        label = "medium"
    else:
        label = "low"

    return {
        "value": value,
        "label": label,
    }


def _timeline_for_incident(
    incident: Dict[str, Any],
    clusters_by_id: Dict[str, Dict[str, Any]],
    events_index: Dict[int, Dict[str, Any]],
    top_k_clusters: int = 5,
) -> List[Dict[str, Any]]:
    """
    Build a concise timeline using candidate clusters.
    """
    candidates = incident.get("root_cause_candidates") or []
    if not isinstance(candidates, list):
        candidates = []

    timeline: List[Dict[str, Any]] = []

    for cand in candidates[:top_k_clusters]:
        if not isinstance(cand, dict):
            continue
        cid = str(cand.get("cluster_id"))
        cluster_obj = clusters_by_id.get(cid)
        if not cluster_obj:
            continue

        ev = _earliest_event(cluster_obj, events_index)
        if not ev:
            continue

        timeline.append({
            "timestamp": _event_timestamp(ev),
            "cluster_id": cid,
            "score": _safe_float(cand.get("score")),
            "service": _event_service(ev),
            "response_code": _event_response_code(ev),
            "summary": _event_text(ev),
        })

    timeline.sort(key=lambda x: (x.get("timestamp") or "", -x.get("score", 0.0)))
    return timeline


def _graph_context_for_candidate(
    cluster_id: str,
    incoming_idx: Dict[str, List[Dict[str, Any]]],
    outgoing_idx: Dict[str, List[Dict[str, Any]]],
    restrict_to: Optional[set[str]] = None,
) -> Dict[str, Any]:
    incoming_edges = []
    outgoing_edges = []

    for e in incoming_idx.get(cluster_id, []):
        src = str(e.get("from"))
        if restrict_to is not None and src not in restrict_to:
            continue
        incoming_edges.append({
            "from": src,
            "relation": e.get("relation"),
            "weight": _safe_float(e.get("weight")),
            "confidence": _safe_float(e.get("confidence")),
        })

    for e in outgoing_idx.get(cluster_id, []):
        dst = str(e.get("to"))
        if restrict_to is not None and dst not in restrict_to:
            continue
        outgoing_edges.append({
            "to": dst,
            "relation": e.get("relation"),
            "weight": _safe_float(e.get("weight")),
            "confidence": _safe_float(e.get("confidence")),
        })

    incoming_edges.sort(key=lambda x: x["weight"], reverse=True)
    outgoing_edges.sort(key=lambda x: x["weight"], reverse=True)

    return {
        "incoming_edges": incoming_edges[:10],
        "outgoing_edges": outgoing_edges[:10],
        "in_degree": len(incoming_edges),
        "out_degree": len(outgoing_edges),
    }


# -------------------------------------------------------------------
# Main builder
# -------------------------------------------------------------------

def build_evidence_bundle(
    *,
    outputs_dir: Path = Path("outputs"),
    out_path: Optional[Path] = None,
    write_json: bool = True,
) -> Dict[str, Any]:
    """
    Step 9:
    Build incident-scoped explainability packets from stabilized RCA artifacts.
    """

    events_path = outputs_dir / "events.jsonl"
    clusters_path = outputs_dir / "clusters.json"
    graph_path = outputs_dir / "graph.json"
    trigger_stats_path = outputs_dir / "cluster_trigger_stats.json"
    rca_path = outputs_dir / "incident_root_causes.json"

    events_index = _load_events_jsonl(events_path)
    clusters_by_id = _normalize_clusters(_load_json(clusters_path, {}))
    graph = _load_json(graph_path, {})
    trigger_stats = _load_json(trigger_stats_path, {})
    incident_rca = _normalize_incident_root_causes(_load_json(rca_path, {}))

    incoming_idx, outgoing_idx = _compute_graph_indexes(graph)

    bundle_incidents: List[Dict[str, Any]] = []

    for incident in incident_rca:
        incident_id = incident.get("incident_id")
        incident_start = incident.get("start_time")
        incident_end = incident.get("end_time")

        raw_candidates = incident.get("root_cause_candidates") or []
        candidates = [c for c in raw_candidates if isinstance(c, dict)]

        incident_cluster_ids = {
            str(c.get("cluster_id"))
            for c in candidates
            if c.get("cluster_id") is not None
        }

        root = candidates[0] if candidates else None
        root_cid = str(root.get("cluster_id")) if root and root.get("cluster_id") is not None else None

        root_payload = None
        if root_cid and root_cid in clusters_by_id:
            cluster_obj = clusters_by_id[root_cid]
            rep_event = _representative_event(root, cluster_obj, events_index)
            first_event = _earliest_event(cluster_obj, events_index)
            graph_ctx = _graph_context_for_candidate(
                root_cid,
                incoming_idx,
                outgoing_idx,
                restrict_to=incident_cluster_ids if incident_cluster_ids else None,
            )

            root_payload = {
                "cluster_id": root_cid,
                "score": _safe_float(root.get("score")),
                "confidence": _candidate_confidence(root),
                "cluster_summary": _cluster_summary(root_cid, cluster_obj, events_index),
                "representative_event": {
                    "timestamp": _event_timestamp(rep_event) if rep_event else None,
                    "service": _event_service(rep_event) if rep_event else None,
                    "response_code": _event_response_code(rep_event) if rep_event else None,
                    "text": _event_text(rep_event) if rep_event else None,
                },
                "first_seen_event": {
                    "timestamp": _event_timestamp(first_event) if first_event else None,
                    "service": _event_service(first_event) if first_event else None,
                    "response_code": _event_response_code(first_event) if first_event else None,
                    "text": _event_text(first_event) if first_event else None,
                },
                "signals": {
                    "trigger_score": _safe_float(root.get("trigger_score")),
                    "error_count": int(_safe_float(root.get("error_count"))),
                    "trigger_proximity": _safe_float(root.get("trigger_proximity")),
                    "in_weight": _safe_float(root.get("in_weight")),
                    "out_weight": _safe_float(root.get("out_weight")),
                    "severity_counts": root.get("severity_counts", {}),
                    "trigger_stats": trigger_stats.get(root_cid, {}) if isinstance(trigger_stats, dict) else {},
                },
                "graph_context": graph_ctx,
                "why_ranked_first": [
                    "highest composite RCA score for this incident",
                    "strong downstream influence relative to incoming influence",
                    "appears early relative to incident onset",
                    "shows abnormal trigger/error behavior",
                ],
            }

        other_candidates_payload = []
        for cand in candidates[1:6]:
            cid = cand.get("cluster_id")
            if cid is None:
                continue
            cid = str(cid)
            cluster_obj = clusters_by_id.get(cid)
            if not cluster_obj:
                continue

            rep_event = _representative_event(cand, cluster_obj, events_index)

            other_candidates_payload.append({
                "cluster_id": cid,
                "score": _safe_float(cand.get("score")),
                "confidence": _candidate_confidence(cand),
                "trigger_score": _safe_float(cand.get("trigger_score")),
                "error_count": int(_safe_float(cand.get("error_count"))),
                "cluster_summary": _cluster_summary(cid, cluster_obj, events_index),
                "representative_event": {
                    "timestamp": _event_timestamp(rep_event) if rep_event else None,
                    "service": _event_service(rep_event) if rep_event else None,
                    "response_code": _event_response_code(rep_event) if rep_event else None,
                    "text": _event_text(rep_event) if rep_event else None,
                },
            })

        timeline = _timeline_for_incident(
            incident=incident,
            clusters_by_id=clusters_by_id,
            events_index=events_index,
            top_k_clusters=5,
        )

        bundle_incidents.append({
            "incident_id": incident_id,
            "incident_window": {
                "start_time": incident_start,
                "end_time": incident_end,
            },
            "root_cause": root_payload,
            "other_candidates": other_candidates_payload,
            "timeline": timeline,
            "stats": {
                "candidate_count": len(candidates),
                "has_root_cause": root_payload is not None,
            },
        })

    bundle = {
        "bundle_version": "step9.v2",
        "artifacts_used": {
            "events": str(events_path.name),
            "clusters": str(clusters_path.name),
            "graph": str(graph_path.name),
            "trigger_stats": str(trigger_stats_path.name),
            "incident_root_causes": str(rca_path.name),
        },
        "incidents": bundle_incidents,
    }

    if write_json:
        final_out = out_path or (outputs_dir / "evidence_bundle.json")
        final_out.parent.mkdir(parents=True, exist_ok=True)
        with final_out.open("w", encoding="utf-8") as f:
            json.dump(bundle, f, ensure_ascii=False, indent=2)

    return bundle


if __name__ == "__main__":
    result = build_evidence_bundle()
    print(json.dumps({
        "bundle_version": result.get("bundle_version"),
        "incident_count": len(result.get("incidents", [])),
    }, indent=2))