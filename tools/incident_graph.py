from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, List, Optional, Tuple
from datetime import datetime, timedelta, timezone

def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None

def _root_service(incident: Dict) -> str:
    root = incident.get("root_cause", {})
    summary = root.get("cluster_summary", {})
    top_services = summary.get("top_services") or []

    if top_services:
        return top_services[0].get("service", "unknown")

    rep = root.get("representative_event", {})
    svc = rep.get("service")
    if svc:
        return svc

    first = root.get("first_seen_event", {})
    svc = first.get("service")
    if svc:
        return svc

    return "unknown"

def _timeline_services(incident: Dict, limit: int = 8) -> List[str]:
    seen = []
    for row in incident.get("timeline", [])[:limit]:
        svc = row.get("service")
        if svc and svc not in seen:
            seen.append(svc)
    return seen

def _http_mix(incident: Dict) -> Dict[str, int]:
    root = incident.get("root_cause", {})
    summary = root.get("cluster_summary", {})
    return summary.get("http_class_counts", {}) or {}


def _incident_window(incident: Dict) -> Tuple[Optional[datetime], Optional[datetime]]:
    win = incident.get("incident_window", {})
    return _parse_ts(win.get("start_time")), _parse_ts(win.get("end_time"))

def _incident_type(incident: Dict) -> str:
    http = _http_mix(incident)
    if http.get("5xx", 0) > 0:
        return "control_plane_failure"
    if http.get("4xx", 0) > 0:
        return "authorization_or_resource_access"
    return "operational_anomaly"

def _overlap_or_near(
    a_start: Optional[datetime],
    a_end: Optional[datetime],
    b_start: Optional[datetime],
    b_end: Optional[datetime],
    max_gap_seconds: int,
) -> Tuple[bool, Optional[int]]:
    if not a_start or not a_end or not b_start or not b_end:
        return False, None

    # overlap
    if b_start <= a_end and b_end >= a_start:
        return True, int((b_start - a_end).total_seconds())

    gap = int((b_start - a_end).total_seconds())
    return 0 <= gap <= max_gap_seconds, gap

def _build_edge(
    parent: Dict,
    child: Dict,
    max_gap_seconds: int,
) -> Optional[Dict]:
    p_id = parent.get("incident_id")
    c_id = child.get("incident_id")

    if not p_id or not c_id or p_id == c_id:
        return None

    p_start, p_end = _incident_window(parent)
    c_start, c_end = _incident_window(child)

    near, gap_seconds = _overlap_or_near(p_start, p_end, c_start, c_end, max_gap_seconds)
    if not near:
        return None

    p_root = _root_service(parent)
    c_root = _root_service(child)

    p_chain = _timeline_services(parent)
    c_chain = _timeline_services(child)

    score = 0.0
    reasons: List[str] = []

    # Rule 1: temporal continuity
    score += 0.35
    reasons.append("incident starts during or shortly after parent window")

    # Rule 2: parent propagation contains child root
    if c_root in p_chain:
        score += 0.35
        reasons.append("child root service appears in parent propagation chain")

    # Rule 3: exact root continuity
    if p_root == c_root:
        score += 0.20
        reasons.append("same root service appears in both incidents")

    # Rule 4: HTTP continuity (5xx in parent, 4xx in child)
    p_http = _http_mix(parent)
    c_http = _http_mix(child)
    if p_http.get("5xx", 0) > 0 and c_http.get("4xx", 0) > 0:
        score += 0.10
        reasons.append("parent shows 5xx while child shows downstream 4xx failures")

    if score < 0.45:
        return None

    relationship = "downstream_incident"
    if p_root == c_root:
        relationship = "continued_incident_phase"

    return {
        "from": p_id,
        "to": c_id,
        "relationship": relationship,
        "confidence": round(min(score, 0.99), 3),
        "gap_seconds": gap_seconds,
        "from_root_service": p_root,
        "to_root_service": c_root,
        "reasons": reasons,
    }

def build_incident_graph(
    evidence_bundle: Dict,
    max_gap_seconds: int = 300,
) -> Dict:
    incidents = evidence_bundle.get("incidents", []) or []

    nodes = []
    for inc in incidents:
        inc_id = inc.get("incident_id")
        if not inc_id:
            continue

        start_ts, end_ts = _incident_window(inc)
        nodes.append(
            {
                "incident_id": inc_id,
                "root_service": _root_service(inc),
                "incident_type": _incident_type(inc),
                "start_time": start_ts.isoformat() if start_ts else None,
                "end_time": end_ts.isoformat() if end_ts else None,
            }
        )

    # sort for deterministic linking
    ordered = sorted(
        incidents,
        key=lambda x: _incident_window(x)[0] or datetime.min.replace(tzinfo=timezone.utc),
    )

    edges = []
    for i, parent in enumerate(ordered):
        for child in ordered[i + 1:]:
            edge = _build_edge(parent, child, max_gap_seconds=max_gap_seconds)
            if edge:
                edges.append(edge)

    # annotate parent/child convenience fields
    parent_map = {}
    child_map = {}
    for e in edges:
        parent_map.setdefault(e["to"], []).append(e["from"])
        child_map.setdefault(e["from"], []).append(e["to"])

    for n in nodes:
        iid = n["incident_id"]
        n["parent_incidents"] = parent_map.get(iid, [])
        n["child_incidents"] = child_map.get(iid, [])
        n["is_downstream_incident"] = bool(n["parent_incidents"])

    return {
        "graph_version": "incident_graph.v1",
        "max_gap_seconds": max_gap_seconds,
        "node_count": len(nodes),
        "edge_count": len(edges),
        "nodes": nodes,
        "edges": edges,
    }


def write_incident_graph(outputs_dir: Path, max_gap_seconds: int = 300) -> Path:
    evidence_path = outputs_dir / "evidence" / "evidence_bundle.json"
    if not evidence_path.exists():
        raise RuntimeError(f"Evidence bundle not found: {evidence_path}")

    bundle = json.loads(evidence_path.read_text(encoding="utf-8"))
    graph = build_incident_graph(bundle, max_gap_seconds=max_gap_seconds)

    out_dir = outputs_dir / "incident_graph"
    out_dir.mkdir(parents=True, exist_ok=True)

    out_path = out_dir / "incident_graph.json"
    out_path.write_text(json.dumps(graph, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    return out_path