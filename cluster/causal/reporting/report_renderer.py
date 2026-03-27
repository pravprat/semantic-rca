# cluster/causal/reporting/report_renderer.py

from __future__ import annotations
from typing import List, Dict, Any

from cluster.causal.utils.io_utils import load_json
from cluster.causal.reporting.pattern_classifier import classify_failure_pattern
from cluster.causal.reporting.explanation_builder import build_explanation


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

        if not candidate_list or not root_events:
            lines.append(f"# Incident {iid}\nNo RCA data available.\n---\n")
            continue

        top = candidate_list[0]

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
        lines.append(f"- **Resource:** {earliest.get('resource')}\n")
        lines.append(f"- **Response Code:** {earliest.get('response_code')}\n")

        # -------------------------
        # Explanation
        # -------------------------
        lines.append("\n## Explanation\n")
        lines.append(explanation["summary"] + "\n")

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