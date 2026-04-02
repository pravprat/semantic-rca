#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path
from typing import Any, Dict, List, Tuple
from datetime import datetime, timedelta
from event_io import load_events

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from semantic.component_registry import resolve_component

SVC_FQDN_RE = re.compile(
    r"https?://([a-z0-9-]+(?:\.[a-z0-9-]+){1,6}\.svc)(?::\d+)?",
    re.IGNORECASE,
)

SYSTEM_OWNER_HINTS = {
    "control_plane": ("k8s_control_plane", "platform_sre"),
    "networking": ("networking", "networking_team"),
    "storage": ("storage_data_services", "storage_team"),
    "data_platform": ("data_platform", "data_platform_team"),
    "policy": ("policy_security", "security_platform_team"),
    "observability": ("observability", "observability_team"),
    "gitops": ("gitops", "platform_ops_team"),
    "hardware": ("infrastructure_hardware", "infra_team"),
    "node": ("k8s_nodes", "platform_sre"),
}


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return load_events(path)


def _incident_map(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {x.get("incident_id"): x for x in items if x.get("incident_id")}


def _cluster_node_map(graph_item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {n.get("cluster_id"): n for n in graph_item.get("nodes", []) if n.get("cluster_id")}


def _cluster_edge_index(graph_item: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in graph_item.get("edges", []):
        src = e.get("source")
        dst = e.get("target")
        if src and dst:
            idx[(src, dst)] = e
    return idx


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _pick_chain_edges(top_cluster: str, graph_item: Dict[str, Any], max_edges: int = 5) -> List[Dict[str, Any]]:
    edges = [e for e in graph_item.get("edges", []) if e.get("source") == top_cluster]
    edges.sort(key=lambda x: (_safe_float(x.get("lag_seconds"), 1e9), -_safe_float(x.get("score"), 0.0)))
    return edges[:max_edges]


def _parse_ts(ts: Any) -> datetime | None:
    if not isinstance(ts, str) or not ts:
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


def _compute_anomaly_onset(
    root_events: List[Dict[str, Any]],
    primary_event: Dict[str, Any] | None,
) -> Dict[str, Any]:
    anomalous: List[Dict[str, Any]] = []
    for ev in root_events:
        rc = ev.get("response_code")
        try:
            if rc is not None and int(rc) >= 400:
                anomalous.append(ev)
        except Exception:
            continue

    anomalous.sort(key=lambda e: (_parse_ts(e.get("timestamp")) is None, _parse_ts(e.get("timestamp"))))
    first = anomalous[0] if anomalous else None

    delta_to_primary_seconds = None
    if first and primary_event:
        t1 = _parse_ts(first.get("timestamp"))
        t2 = _parse_ts(primary_event.get("timestamp"))
        if t1 and t2:
            delta_to_primary_seconds = round((t2 - t1).total_seconds(), 3)

    return {
        "detection_rule": "first_failure_response_code_gte_400",
        "first_anomaly_timestamp": (first or {}).get("timestamp"),
        "first_anomaly_event_id": (first or {}).get("event_id"),
        "first_anomaly_cluster_id": (first or {}).get("cluster_id"),
        "first_anomaly_response_code": (first or {}).get("response_code"),
        "delta_to_primary_seconds": delta_to_primary_seconds,
    }


def _status_class_from_event(ev: Dict[str, Any]) -> str:
    rc = ev.get("response_code")
    if rc is None:
        # Explicitly separate missing HTTP code from unparseable/non-standard values.
        return "null"
    try:
        c = int(rc)
        return f"{c // 100}xx"
    except Exception:
        semantic_sc = str((ev.get("semantic") or {}).get("status_class") or "").strip()
        if semantic_sc in {"1xx", "2xx", "3xx", "4xx", "5xx"}:
            return semantic_sc
        return "unknown"


def _failure_mode_from_event(ev: Dict[str, Any]) -> str:
    semantic = ev.get("semantic") or {}
    mode = semantic.get("failure_mode")
    if isinstance(mode, str) and mode:
        return mode
    sc = _status_class_from_event(ev)
    if sc == "5xx":
        return "service_failure"
    if sc == "4xx":
        return "client_or_auth_failure"
    return "normal"


def _system_owner_for_service(service: str, text: str = "") -> Dict[str, str]:
    comp, domain = resolve_component(service or "", text or "")
    system, owner = SYSTEM_OWNER_HINTS.get(domain or "", ("unknown_system", "unknown_owner"))
    return {
        "component": comp,
        "domain": domain or "unknown_domain",
        "system": system,
        "owner_hint": owner,
    }


def _extract_dependency_targets(text: str) -> List[Dict[str, str]]:
    if not text:
        return []
    out: List[Dict[str, str]] = []
    for m in SVC_FQDN_RE.finditer(text):
        fqdn = m.group(1).lower()
        svc = fqdn.split(".")[0]
        out.append({"target_service": svc, "target_fqdn": fqdn})
    return out


def _safe_rate(count: int, seconds: float) -> float:
    if seconds <= 0:
        return 0.0
    return count / seconds


def _format_lift(pre_rate: float, post_rate: float) -> float | None:
    if pre_rate <= 0:
        return None
    return round(post_rate / pre_rate, 4)


def _compute_post_anomaly_impacts(
    anomaly_onset: Dict[str, Any],
    incident: Dict[str, Any],
    all_events: List[Dict[str, Any]],
    baseline_window_minutes: int = 5,
) -> Dict[str, Any]:
    t0 = _parse_ts(anomaly_onset.get("first_anomaly_timestamp"))
    end = _parse_ts(incident.get("end_time"))
    start = _parse_ts(incident.get("start_time"))
    if not t0:
        return {
            "window_start": None,
            "window_end": incident.get("end_time"),
            "events_after_anomaly": 0,
            "failure_events_after_anomaly": 0,
            "first_5xx_timestamp": None,
            "first_5xx_delta_seconds": None,
            "status_class_counts_after_anomaly": {},
            "failure_domain_breakdown_after_anomaly": [],
            "service_failure_breakdown_after_anomaly": [],
            "resource_failure_breakdown_after_anomaly": [],
            "pre_vs_post_failure_lift": {},
            "top_impacted_services": [],
            "top_impacted_resources": [],
            "summary": "No anomaly onset timestamp available to compute downstream impacts.",
        }

    def in_window(ev: Dict[str, Any]) -> bool:
        ts = _parse_ts(ev.get("timestamp"))
        if not ts:
            return False
        if ts < t0:
            return False
        if end and ts > end:
            return False
        return True

    sliced = [e for e in all_events if in_window(e)]
    # Fixed N-minute pre/post windows around anomaly onset for always-available comparison.
    n = max(1, int(baseline_window_minutes))
    pre_start = t0 - timedelta(minutes=n)
    post_end = t0 + timedelta(minutes=n)
    pre_window = []
    post_window = []
    for e in all_events:
        ts = _parse_ts(e.get("timestamp"))
        if not ts:
            continue
        if pre_start <= ts < t0:
            pre_window.append(e)
        if t0 <= ts <= post_end:
            post_window.append(e)
    # Use in-incident pre-window [incident_start, t0) for degradation comparison.
    pre_slice: List[Dict[str, Any]] = []
    if start:
        for e in all_events:
            ts = _parse_ts(e.get("timestamp"))
            if not ts:
                continue
            if start <= ts < t0:
                pre_slice.append(e)
    failure_events = []
    status_counts: Dict[str, int] = {}
    svc_counts: Dict[str, int] = {}
    res_counts: Dict[str, int] = {}
    mode_counts: Dict[str, int] = {}
    component_counts: Dict[str, int] = {}
    first_5xx_ts = None
    dep_edges: Dict[Tuple[str, str], Dict[str, Any]] = {}

    for e in sliced:
        sc = _status_class_from_event(e)
        status_counts[sc] = status_counts.get(sc, 0) + 1

        rc = e.get("response_code")
        status_family = str(e.get("status_family") or "").lower()
        is_failure = False
        try:
            is_failure = int(rc) >= 400
        except Exception:
            is_failure = status_family == "failure"
        if is_failure:
            failure_events.append(e)
            mode = _failure_mode_from_event(e)
            mode_counts[mode] = mode_counts.get(mode, 0) + 1
            svc = e.get("service")
            res = e.get("resource")
            if isinstance(svc, str) and svc:
                svc_counts[svc] = svc_counts.get(svc, 0) + 1
                comp = _system_owner_for_service(svc, str(e.get("raw_text") or ""))
                c_key = comp.get("component") or "unknown_component"
                component_counts[c_key] = component_counts.get(c_key, 0) + 1
            if isinstance(res, str) and res:
                res_counts[res] = res_counts.get(res, 0) + 1
            raw_text = str(e.get("raw_text") or "")
            sf = e.get("structured_fields") if isinstance(e.get("structured_fields"), dict) else {}
            src = str((sf.get("source_service") if isinstance(sf, dict) else None) or svc or "unknown_service")
            ts = _parse_ts(e.get("timestamp"))
            dep_candidates: List[Dict[str, str]] = []
            from_structured = False
            if isinstance(sf, dict) and sf.get("target_dependency_service"):
                dep_candidates.append(
                    {
                        "target_service": str(sf.get("target_dependency_service")),
                        "target_fqdn": str(sf.get("target_dependency_fqdn") or ""),
                    }
                )
                from_structured = True
            if not dep_candidates:
                dep_candidates = _extract_dependency_targets(raw_text)
            if from_structured:
                failure_location = str((sf.get("failure_location") if isinstance(sf, dict) else None) or "dependency_target")
                confidence_tier = str((sf.get("causal_confidence_tier") if isinstance(sf, dict) else None) or "observed")
            elif dep_candidates:
                failure_location = "dependency_target"
                confidence_tier = "observed"
            else:
                failure_location = str((sf.get("failure_location") if isinstance(sf, dict) else None) or "source_service")
                confidence_tier = str((sf.get("causal_confidence_tier") if isinstance(sf, dict) else None) or "likely")
            for dep in dep_candidates:
                tgt = dep.get("target_service") or "unknown_target"
                key = (src, tgt)
                row = dep_edges.get(key)
                if row is None:
                    dep_edges[key] = {
                        "source_service": src,
                        "target_service": tgt,
                        "target_fqdn": dep.get("target_fqdn"),
                        "count": 1,
                        "first_seen": e.get("timestamp"),
                        "failure_location": failure_location,
                        "causal_confidence_tier": confidence_tier,
                        "source_meta": _system_owner_for_service(src, raw_text),
                        "target_meta": _system_owner_for_service(tgt, raw_text),
                    }
                else:
                    row["count"] = int(row.get("count", 0)) + 1
                    prev_ts = _parse_ts(row.get("first_seen"))
                    if ts and (not prev_ts or ts < prev_ts):
                        row["first_seen"] = e.get("timestamp")
            try:
                if int(rc) >= 500:
                    ts = _parse_ts(e.get("timestamp"))
                    if ts and (first_5xx_ts is None or ts < first_5xx_ts):
                        first_5xx_ts = ts
            except Exception:
                pass

    top_services = sorted(svc_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_resources = sorted(res_counts.items(), key=lambda x: x[1], reverse=True)[:5]
    top_modes = sorted(mode_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    top_components = sorted(component_counts.items(), key=lambda x: x[1], reverse=True)[:8]
    top_dep_edges = sorted(dep_edges.values(), key=lambda x: int(x.get("count", 0)), reverse=True)[:10]
    primary_dep = None
    secondary_deps: List[Dict[str, Any]] = []
    if top_dep_edges:
        # Primary dependency impact ranking: earliest first, then highest count.
        ranked = sorted(
            top_dep_edges,
            key=lambda x: (
                _parse_ts(x.get("first_seen")) is None,
                _parse_ts(x.get("first_seen")),
                -int(x.get("count", 0)),
            ),
        )
        primary_dep = dict(ranked[0])
        primary_dep["impact_rank"] = "primary"
        secondary_deps = [dict(x) for x in ranked[1:]]
        for r in secondary_deps:
            r["impact_rank"] = "secondary"

    # Pre/post failure lift.
    pre_fail = 0
    pre_mode_counts: Dict[str, int] = {}
    for e in pre_slice:
        rc = e.get("response_code")
        status_family = str(e.get("status_family") or "").lower()
        is_failure = False
        try:
            is_failure = int(rc) >= 400
        except Exception:
            is_failure = status_family == "failure"
        if is_failure:
            pre_fail += 1
            mode = _failure_mode_from_event(e)
            pre_mode_counts[mode] = pre_mode_counts.get(mode, 0) + 1

    post_seconds = 0.0
    if end:
        post_seconds = max(0.0, (end - t0).total_seconds())
    elif sliced:
        last_ts = max((_parse_ts(e.get("timestamp")) for e in sliced if _parse_ts(e.get("timestamp"))), default=t0)
        post_seconds = max(0.0, (last_ts - t0).total_seconds())
    pre_seconds = max(0.0, (t0 - start).total_seconds()) if start else 0.0
    pre_rate = _safe_rate(pre_fail, pre_seconds)
    post_rate = _safe_rate(len(failure_events), post_seconds)

    # Fixed-window pre/post rates around anomaly onset.
    def _is_failure_event(e: Dict[str, Any]) -> bool:
        rc = e.get("response_code")
        status_family = str(e.get("status_family") or "").lower()
        try:
            return int(rc) >= 400
        except Exception:
            return status_family == "failure"

    pre_window_fail = [e for e in pre_window if _is_failure_event(e)]
    post_window_fail = [e for e in post_window if _is_failure_event(e)]
    fixed_pre_5xx_count = 0
    fixed_post_5xx_count = 0
    for e in pre_window:
        rc = e.get("response_code")
        try:
            if int(rc) >= 500:
                fixed_pre_5xx_count += 1
        except Exception:
            continue
    for e in post_window:
        rc = e.get("response_code")
        try:
            if int(rc) >= 500:
                fixed_post_5xx_count += 1
        except Exception:
            continue
    first_5xx_fixed_post_ts = None
    for e in post_window:
        rc = e.get("response_code")
        try:
            if int(rc) >= 500:
                ts = _parse_ts(e.get("timestamp"))
                if ts and (first_5xx_fixed_post_ts is None or ts < first_5xx_fixed_post_ts):
                    first_5xx_fixed_post_ts = ts
        except Exception:
            continue

    pre_window_seconds = float(n * 60)
    post_window_seconds = float(n * 60)
    pre_window_rate = _safe_rate(len(pre_window_fail), pre_window_seconds)
    post_window_rate = _safe_rate(len(post_window_fail), post_window_seconds)

    # Mode-level lift in fixed windows.
    pre_window_mode_counts: Dict[str, int] = {}
    post_window_mode_counts: Dict[str, int] = {}
    for e in pre_window_fail:
        mode = _failure_mode_from_event(e)
        pre_window_mode_counts[mode] = pre_window_mode_counts.get(mode, 0) + 1
    for e in post_window_fail:
        mode = _failure_mode_from_event(e)
        post_window_mode_counts[mode] = post_window_mode_counts.get(mode, 0) + 1
    win_mode_lifts: List[Dict[str, Any]] = []
    all_win_modes = sorted(set(pre_window_mode_counts.keys()) | set(post_window_mode_counts.keys()))
    for mode in all_win_modes:
        pre_c = pre_window_mode_counts.get(mode, 0)
        post_c = post_window_mode_counts.get(mode, 0)
        r_pre = _safe_rate(pre_c, pre_window_seconds)
        r_post = _safe_rate(post_c, post_window_seconds)
        win_mode_lifts.append(
            {
                "failure_mode": mode,
                "pre_count": pre_c,
                "post_count": post_c,
                "pre_rate_eps": round(r_pre, 6),
                "post_rate_eps": round(r_post, 6),
                "lift_ratio": _format_lift(r_pre, r_post),
            }
        )
    win_mode_lifts.sort(key=lambda x: x.get("post_count", 0), reverse=True)

    mode_lifts: List[Dict[str, Any]] = []
    all_modes = sorted(set(pre_mode_counts.keys()) | set(mode_counts.keys()))
    for mode in all_modes:
        pre_c = pre_mode_counts.get(mode, 0)
        post_c = mode_counts.get(mode, 0)
        r_pre = _safe_rate(pre_c, pre_seconds)
        r_post = _safe_rate(post_c, post_seconds)
        mode_lifts.append(
            {
                "failure_mode": mode,
                "pre_count": pre_c,
                "post_count": post_c,
                "pre_rate_eps": round(r_pre, 6),
                "post_rate_eps": round(r_post, 6),
                "lift_ratio": _format_lift(r_pre, r_post),
            }
        )
    mode_lifts.sort(key=lambda x: x.get("post_count", 0), reverse=True)

    first_5xx_delta = None
    if first_5xx_ts:
        first_5xx_delta = round((first_5xx_ts - t0).total_seconds(), 3)

    summary = (
        f"After root anomaly at {anomaly_onset.get('first_anomaly_timestamp')}, "
        f"{len(failure_events)} failure events were observed in-window; "
        f"first 5xx in incident window={first_5xx_ts.isoformat() if first_5xx_ts else 'not observed'}, "
        f"first 5xx in fixed +{n}m window={first_5xx_fixed_post_ts.isoformat() if first_5xx_fixed_post_ts else 'not observed'}."
    )

    return {
        "window_start": anomaly_onset.get("first_anomaly_timestamp"),
        "window_end": incident.get("end_time"),
        "events_after_anomaly": len(sliced),
        "failure_events_after_anomaly": len(failure_events),
        "first_5xx_timestamp": first_5xx_ts.isoformat() if first_5xx_ts else None,
        "first_5xx_delta_seconds": first_5xx_delta,
        "first_5xx_timestamp_fixed_post_window": (
            first_5xx_fixed_post_ts.isoformat() if first_5xx_fixed_post_ts else None
        ),
        "first_5xx_delta_seconds_fixed_post_window": (
            round((first_5xx_fixed_post_ts - t0).total_seconds(), 3) if first_5xx_fixed_post_ts else None
        ),
        "status_class_counts_after_anomaly": status_counts,
        "failure_domain_breakdown_after_anomaly": [{"failure_mode": k, "count": v} for k, v in top_modes],
        "component_failure_breakdown_after_anomaly": [
            {"component": k, "count": v} for k, v in top_components
        ],
        "service_failure_breakdown_after_anomaly": [{"service": k, "count": v} for k, v in top_services],
        "resource_failure_breakdown_after_anomaly": [{"resource": k, "count": v} for k, v in top_resources],
        "observed_dependency_impacts_after_anomaly": top_dep_edges,
        "primary_dependency_impact": primary_dep,
        "secondary_dependency_impacts": secondary_deps,
        "pre_vs_post_failure_lift": {
            "pre_window_seconds": round(pre_seconds, 3),
            "post_window_seconds": round(post_seconds, 3),
            "pre_failure_count": pre_fail,
            "post_failure_count": len(failure_events),
            "pre_failure_rate_eps": round(pre_rate, 6),
            "post_failure_rate_eps": round(post_rate, 6),
            "overall_lift_ratio": _format_lift(pre_rate, post_rate),
            "failure_mode_lifts": mode_lifts[:10],
            "fixed_window_minutes": n,
            "fixed_window_start_pre": pre_start.isoformat(),
            "fixed_window_end_post": post_end.isoformat(),
            "fixed_pre_failure_count": len(pre_window_fail),
            "fixed_post_failure_count": len(post_window_fail),
            "fixed_pre_5xx_count": fixed_pre_5xx_count,
            "fixed_post_5xx_count": fixed_post_5xx_count,
            "fixed_pre_failure_rate_eps": round(pre_window_rate, 6),
            "fixed_post_failure_rate_eps": round(post_window_rate, 6),
            "fixed_overall_lift_ratio": _format_lift(pre_window_rate, post_window_rate),
            "fixed_failure_mode_lifts": win_mode_lifts[:10],
        },
        "top_impacted_services": [{"service": k, "count": v} for k, v in top_services],
        "top_impacted_resources": [{"resource": k, "count": v} for k, v in top_resources],
        "summary": summary,
    }


def build_evidence_bundle(
    incidents_path: Path,
    candidates_path: Path,
    grounded_events_path: Path,
    graph_path: Path,
    report_path: Path,
    events_path: Path,
    output_path: Path,
) -> List[Dict[str, Any]]:
    incidents = _load_json(incidents_path)
    candidates = _load_json(candidates_path)
    grounded = _load_json(grounded_events_path)
    graphs = _load_json(graph_path)
    reports = _load_json(report_path)
    all_events = _load_jsonl(events_path)

    cand_map = _incident_map(candidates)
    grounded_map = _incident_map(grounded)
    graph_map = _incident_map(graphs)
    report_map = _incident_map(reports)

    bundles: List[Dict[str, Any]] = []

    for inc in incidents:
        iid = inc.get("incident_id")
        if not iid:
            continue

        cand_item = cand_map.get(iid, {})
        grounded_item = grounded_map.get(iid, {})
        graph_item = graph_map.get(iid, {})
        report_item = report_map.get(iid, {})

        candidate_list = cand_item.get("candidates", [])
        root_events = grounded_item.get("root_events", [])
        top = candidate_list[0] if candidate_list else {}
        top_cluster = top.get("cluster_id")

        node_map = _cluster_node_map(graph_item)
        primary_event = None
        for ev in root_events:
            if ev.get("reason") == "earliest_failure":
                primary_event = ev
                break
        if primary_event is None and root_events:
            primary_event = root_events[0]

        chain = _pick_chain_edges(top_cluster, graph_item) if top_cluster else []
        chain_refs = []
        for e in chain:
            chain_refs.append(
                {
                    "source_cluster_id": e.get("source"),
                    "target_cluster_id": e.get("target"),
                    "edge_score": e.get("score"),
                    "lag_seconds": e.get("lag_seconds"),
                    "semantic_links": e.get("semantic_links"),
                }
            )

        claim = {
            "claim_id": f"{iid}-CLM-ROOT-001",
            "type": "root_cause",
            "statement": f"Cluster {top_cluster} is the primary root cause candidate." if top_cluster else "No root cluster candidate available.",
            "confidence": (report_item.get("confidence") or {}).get("score"),
            "supports": {
                "candidate": {
                    "cluster_id": top.get("cluster_id"),
                    "candidate_score": top.get("candidate_score"),
                    "temporal_rank": top.get("temporal_rank"),
                    "out_strength": top.get("out_strength"),
                    "in_strength": top.get("in_strength"),
                    "failure_domain": top.get("failure_domain"),
                },
                "events": [
                    {
                        "event_id": ev.get("event_id"),
                        "cluster_id": ev.get("cluster_id"),
                        "timestamp": ev.get("timestamp"),
                        "response_code": ev.get("response_code"),
                        "actor": ev.get("actor"),
                        "resource": ev.get("resource"),
                        "reason": ev.get("reason"),
                    }
                    for ev in root_events[:10]
                ],
                "graph_edges": chain_refs,
            },
            "operator_view": {
                "operator_label": (report_item.get("root_cause_summary") or {}).get("type") or "Root Cause",
                "operator_summary": report_item.get("explanation"),
                "impact_window": report_item.get("incident_window") or {
                    "start_time": inc.get("start_time"),
                    "end_time": inc.get("end_time"),
                    "duration_seconds": inc.get("duration_seconds"),
                },
            },
            "technical_refs": {
                "incident_id": iid,
                "primary_cluster_id": top_cluster,
                "primary_event_id": (primary_event or {}).get("event_id"),
                "node_snapshot": node_map.get(top_cluster) if top_cluster else None,
            },
        }

        coverage = {
            "claims_total": 1,
            "claims_with_evidence": 1 if top_cluster and root_events else 0,
        }
        coverage["coverage_pct"] = round(
            100.0 * coverage["claims_with_evidence"] / max(1, coverage["claims_total"]), 2
        )
        anomaly_onset = _compute_anomaly_onset(root_events, primary_event)
        post_impacts = _compute_post_anomaly_impacts(anomaly_onset, inc, all_events, baseline_window_minutes=5)

        bundle = {
            "incident_id": iid,
            "bundle_version": "1.0",
            "incident_metadata": {
                "incident_version": inc.get("incident_version"),
                "episode_count": inc.get("episode_count"),
                "incident_class": inc.get("incident_class"),
                "declaration": inc.get("declaration"),
                "confidence": inc.get("confidence"),
                "policy_summary": inc.get("policy_summary"),
            },
            "lineage": {
                "incidents_path": str(incidents_path),
                "candidates_path": str(candidates_path),
                "grounded_events_path": str(grounded_events_path),
                "graph_path": str(graph_path),
                "report_path": str(report_path),
            },
            "claims": [claim],
            "coverage": coverage,
            "anomaly_onset": anomaly_onset,
            "post_anomaly_impacts": post_impacts,
            "chain_summary": {
                "top_cluster": top_cluster,
                "direct_downstream_edges": len(chain),
                "all_graph_edges": len(graph_item.get("edges", [])),
            },
        }
        bundles.append(bundle)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(bundles, f, ensure_ascii=False, indent=2)

    return bundles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build incident evidence bundle JSON from existing RCA artifacts."
    )
    parser.add_argument("--incidents", default="outputs/incidents.json")
    parser.add_argument("--candidates", default="outputs/incident_root_candidates.json")
    parser.add_argument("--grounded-events", default="outputs/incident_root_events.json")
    parser.add_argument("--graph", default="outputs/incident_causal_graph.json")
    parser.add_argument("--report", default="outputs/incident_rca_report.json")
    parser.add_argument("--events", default="outputs/events.jsonl")
    parser.add_argument("--output", default="outputs/incident_evidence_bundle.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = build_evidence_bundle(
        incidents_path=Path(args.incidents),
        candidates_path=Path(args.candidates),
        grounded_events_path=Path(args.grounded_events),
        graph_path=Path(args.graph),
        report_path=Path(args.report),
        events_path=Path(args.events),
        output_path=Path(args.output),
    )
    print(f"[evidence_bundle] incidents={len(out)} -> {args.output}")


if __name__ == "__main__":
    main()

