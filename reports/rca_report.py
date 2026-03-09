# semantic-rca/reports/rca_report.py
from __future__ import annotations

from typing import Dict, Any, List
from datetime import datetime


def render_rca_markdown(
    graph: Dict[str, Any],
    root_causes: List[Dict[str, Any]],
    clusters: Dict[str, Any],
    cluster_summaries: Dict[str, Any],
    incidents: List[Dict[str, Any]]
) -> str:
    now = datetime.utcnow().isoformat() + "Z"

    lines = []
    lines.append(f"# RCA Report")
    lines.append(f"_Generated: {now}_")
    lines.append("")
    lines.append("## Executive Summary")
    if root_causes:
        top = root_causes[0]
        cid = top["cluster_id"]
        summary = cluster_summaries.get(cid, {})
        lines.append(f"- Top candidate: **{cid}** (confidence: {top['confidence']:.2f}, size: {top['size']})")
        if summary.get("representative_text"):
            lines.append(f"- Representative evidence: `{_oneline(summary['representative_text'])}`")
    else:
        lines.append("- No root cause candidates identified (insufficient clustered signal).")

    lines.append("")
    lines.append("## Root Cause Candidates")
    for rc in root_causes:
        cid = rc["cluster_id"]
        lines.append(f"- **{cid}** confidence={rc['confidence']:.2f} size={rc['size']} "
                     f"out_precedes={rc['out_precedes_weight']} in_precedes={rc['in_precedes_weight']}")

    lines.append("")
    lines.append("## Cluster Summaries")
    for cid, s in cluster_summaries.items():
        lines.append(f"### {cid} (size={s.get('size')})")
        rt = s.get("representative_text", "")
        if rt:
            lines.append(f"- Representative: `{_oneline(rt)}`")
        lines.append("")

    lines.append("## Incidents (time-bucketed)")
    for inc in incidents[:20]:
        lines.append(f"- **{inc.get('incident_id')}** "
                     f"{inc.get('start_time')} → {inc.get('end_time')} "
                     f"clusters={len(inc.get('cluster_ids', []))} events={len(inc.get('event_ids', []))}")

    lines.append("")
    lines.append("## Graph Stats")
    lines.append(f"- Nodes: {len(graph.get('nodes', []))}")
    lines.append(f"- Edges: {len(graph.get('edges', []))}")

    return "\n".join(lines)


def _oneline(s: str) -> str:
    return " ".join(s.strip().split())[:300]