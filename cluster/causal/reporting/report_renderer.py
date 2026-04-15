# cluster/causal/reporting/report_renderer.py

from __future__ import annotations
from typing import List, Dict, Any

from cluster.causal.utils.io_utils import load_json
from cluster.causal.reporting.pattern_classifier import classify_failure_pattern
from cluster.causal.reporting.explanation_builder import build_explanation


def _symptom_summary(root_events: List[Dict[str, Any]]) -> Dict[str, Any]:
    codes: Dict[int, int] = {}
    actors: Dict[str, int] = {}
    resources: Dict[str, int] = {}
    for ev in root_events:
        rc = ev.get("response_code")
        if isinstance(rc, int):
            codes[rc] = codes.get(rc, 0) + 1
        a = ev.get("actor")
        if a:
            actors[a] = actors.get(a, 0) + 1
        r = ev.get("resource")
        if r:
            resources[r] = resources.get(r, 0) + 1
    top_codes = sorted(codes.items(), key=lambda x: x[1], reverse=True)[:3]
    top_actors = sorted(actors.items(), key=lambda x: x[1], reverse=True)[:3]
    top_resources = sorted(resources.items(), key=lambda x: x[1], reverse=True)[:3]
    return {
        "total_root_events": len(root_events),
        "top_codes": top_codes,
        "top_actors": top_actors,
        "top_resources": top_resources,
    }


def _suggest_log_targets(top_candidate: Dict[str, Any], root_events: List[Dict[str, Any]]) -> List[str]:
    targets: List[str] = []
    fd = str(top_candidate.get("failure_domain") or "unknown")
    if fd in {"rbac_authorization", "authz_failure"}:
        targets.append("kube-apiserver audit logs for authorization denials around incident start")
        targets.append("rbac object history: Role/ClusterRole and Binding changes for impacted service accounts")
    elif fd in {"resource_missing", "scheduler", "control_plane"}:
        targets.append("controller/operator logs for missing objects and reconcile retries")
        targets.append("kube-apiserver and controller-manager logs around object create/update failures")
    else:
        targets.append("service/component logs tied to top actor/resource in supporting failures")
        targets.append("control-plane logs around incident start for correlated retries/errors")

    if any((ev.get("response_code") or 0) >= 500 for ev in root_events if isinstance(ev.get("response_code"), int)):
        targets.append("upstream dependency and backend service health logs for 5xx interval")
    return targets


def _fallback_root_event(top: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "event_id": None,
        "timestamp": top.get("first_seen"),
        "actor": top.get("actor"),
        "resource": top.get("resource"),
        "response_code": None,
        "reason": "candidate_fallback",
        "failure_domain": top.get("failure_domain"),
    }


def render_report(
    incidents_path: str,
    candidates_path: str,
    grounded_events_path: str,
    output_path: str,
) -> None:

    incidents = load_json(incidents_path)
    candidates = load_json(candidates_path)
    grounded = load_json(grounded_events_path)

    candidate_map = {c["incident_id"]: c for c in candidates}
    grounded_map = {g["incident_id"]: g for g in grounded}

    lines: List[str] = []

    lines.append("# Semantic RCA Report\n")
    lines.append("---\n")

    for inc in incidents:
        iid = inc["incident_id"]

        candidate_list = candidate_map.get(iid, {}).get("candidates", [])
        root_events = grounded_map.get(iid, {}).get("root_events", [])

        if not candidate_list:
            lines.append(f"# Incident {iid}\nNo RCA data available.\n---\n")
            continue

        top = candidate_list[0]
        if not root_events:
            root_events = [_fallback_root_event(top)]

        pattern_info = classify_failure_pattern(root_events)

        explanation = build_explanation(
            top_candidate=top,
            root_events=root_events,
            pattern_info=pattern_info,
        )

        earliest = root_events[0]

        # -------------------------
        # Incident header
        # -------------------------
        lines.append(f"# Incident {iid}\n")

        # Accept both incident schemas:
        # 1) {"window": {...}} and 2) {"start_time", "end_time", "duration_seconds"}
        window = inc.get("window")
        if not window:
            window = {
                "start_time": inc.get("start_time"),
                "end_time": inc.get("end_time"),
                "duration_seconds": inc.get("duration_seconds"),
            }

        if window and (window.get("start_time") or window.get("end_time")):
            lines.append("## Incident Window\n")
            lines.append(
                f"{window.get('start_time')} → {window.get('end_time')} "
                f"({window.get('duration_seconds')}s)\n"
            )

        # -------------------------
        # Root Cause
        # -------------------------
        lines.append("## Root Cause\n")

        lines.append(
            f"- **Type:** {pattern_info['pattern'].replace('_', ' ').title()}\n"
        )
        lines.append(f"- **Cluster:** {top['cluster_id']}\n")
        lines.append(f"- **Primary Event:** {earliest['event_id']}\n")
        lines.append(f"- **Time:** {earliest['timestamp']}\n")
        lines.append(f"- **Actor:** {earliest.get('actor')}\n")
        lines.append(f"- **Service:** {earliest.get('service')}\n")
        lines.append(f"- **Component:** {earliest.get('component')}\n")
        lines.append(f"- **Resource:** {earliest.get('resource')}\n")
        lines.append(f"- **Response Code:** {earliest.get('response_code')}\n")

        # -------------------------
        # Engineering interpretation
        # -------------------------
        sym = _symptom_summary(root_events)
        lines.append("\n## What Broke (Symptoms)\n")
        lines.append(
            f"- Observed `{sym['total_root_events']}` grounded failure events in this incident window."
        )
        if sym["top_codes"]:
            lines.append(f"- Dominant response codes: `{sym['top_codes']}`")
        if sym["top_actors"]:
            lines.append(f"- Most affected actors: `{sym['top_actors']}`")
        if sym["top_resources"]:
            lines.append(f"- Most affected resources: `{sym['top_resources']}`")

        lines.append("\n## Likely Cause (RCA Hypothesis)\n")
        lines.append(
            f"- We hypothesize cluster `{top['cluster_id']}` as the source pattern "
            f"because it is earliest/highest-ranked with pattern `{pattern_info['pattern']}`."
        )
        lines.append(f"- Supporting explanation: {explanation['summary']}")

        lines.append("\n## Why This Is Not Just a Symptom\n")
        lines.append(
            f"- Candidate score `{round(top.get('candidate_score', 0.0), 3)}`, "
            f"out-degree `{top.get('out_degree')}`, in-degree `{top.get('in_degree')}`."
        )
        lines.append("- Earliest grounded failures align with this candidate's timeline.")

        lines.append("\n## Where Engineers Should Look Next\n")
        for t in _suggest_log_targets(top, root_events):
            lines.append(f"- {t}")

        # -------------------------
        # Evidence
        # -------------------------
        lines.append("\n## Supporting Evidence\n")

        for ev in root_events:
            lines.append(
                f"- [{ev['timestamp']}] "
                f"{ev.get('actor')} → {ev.get('resource')} "
                f"({ev.get('response_code')}) "
                f"[{ev.get('reason')}]"
            )

        # -------------------------
        # Top Candidates
        # -------------------------
        lines.append("\n## Top Candidate Clusters\n")

        for c in candidate_list[:5]:
            lines.append(
                f"- {c['cluster_id']} "
                f"(score={round(c['candidate_score'], 3)}, "
                f"out={c['out_degree']}, in={c['in_degree']})"
            )

        lines.append("\n---\n")

    with open(output_path, "w", encoding="utf-8") as f:
        f.write("\n".join(lines))

    print(f"[report_renderer] report written -> {output_path}")