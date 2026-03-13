from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Optional
from datetime import datetime
import json
from tools.cluster_explainer import describe_cluster


# ==========================================================
# Utility helpers
# ==========================================================

def _safe(v):
    return v if v is not None else "unknown"


def _severity(error_count: int) -> str:

    if not isinstance(error_count, int):
        return "Unknown"

    if error_count < 50:
        return "Low"

    if error_count < 200:
        return "Medium"

    return "High"


def _parse_ts(ts):

    if not ts:
        return None

    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00"))
    except Exception:
        return None


# ==========================================================
# Event formatting
# ==========================================================

def _format_event(ev: Dict) -> str:

    if not isinstance(ev, dict):
        return "event unavailable"

    ts = ev.get("timestamp")
    text = ev.get("text")

    if not text:
        return "event unavailable"

    parts = text.split(",")

    actor = parts[2] if len(parts) > 2 else "unknown"
    verb = parts[3] if len(parts) > 3 else "unknown"
    resource = parts[4] if len(parts) > 4 else "unknown"
    response = parts[11] if len(parts) > 11 else "unknown"

    return (
        f"Timestamp: {ts}\n"
        f"Actor: {actor}\n"
        f"Action: {verb} {resource}\n"
        f"Response: HTTP {response}"
    )


# ==========================================================
# Root cause helpers
# ==========================================================

def _root_service(summary: Dict):

    top = summary.get("top_services")

    if not top:
        return "unknown"

    return top[0].get("service", "unknown")


def _incident_type(http_dist: Dict):

    if not http_dist:
        return "Unclassified anomaly"

    if http_dist.get("5xx", 0) > 0:
        return "Control-plane service failure"

    if http_dist.get("4xx", 0) > 20:
        return "Authorization / resource access anomaly"

    return "Operational anomaly"


# ==========================================================
# Timeline helpers
# ==========================================================

def _timeline_rows(timeline):

    rows = []

    for t in timeline[:10]:

        ts = t.get("timestamp")
        svc = t.get("service")
        rc = t.get("response_code")

        rows.append((ts, svc, rc))

    return rows


def _propagation_chain(timeline):

    services = []

    for t in timeline[:10]:

        svc = t.get("service")

        if svc and svc not in services:
            services.append(svc)

    return services


def _propagation_delays(timeline):

    delays = []

    rows = _timeline_rows(timeline)

    for i in range(len(rows) - 1):

        ts1, s1, _ = rows[i]
        ts2, s2, _ = rows[i + 1]

        t1 = _parse_ts(ts1)
        t2 = _parse_ts(ts2)

        if not t1 or not t2:
            continue

        delta = int((t2 - t1).total_seconds())

        delays.append(f"{s1} → {s2} : {delta}s")

    return delays


# ==========================================================
# Confidence reasoning
# ==========================================================

def _confidence_reasons(root, timeline):

    conf = root.get("confidence", {})
    reasons = conf.get("reasons")

    if reasons:
        return reasons

    reasons = []

    if root.get("signals", {}).get("trigger_score"):
        reasons.append("cluster exhibits highest trigger anomaly score")

    if root.get("signals", {}).get("error_count"):
        reasons.append("cluster contains largest burst of error responses")

    if timeline:
        reasons.append("events occur earliest in incident timeline")

    return reasons


# ==========================================================
# Incident graph relationships
# ==========================================================

def _incident_relationships(graph: Dict, incident_id: str) -> Dict:

    if not graph:
        return {"parents": [], "children": [], "is_downstream": False}

    for node in graph.get("nodes", []):
        if node.get("incident_id") == incident_id:
            return {
                "parents": node.get("parent_incidents", []),
                "children": node.get("child_incidents", []),
                "is_downstream": node.get("is_downstream_incident", False),
            }

    return {"parents": [], "children": [], "is_downstream": False}


# ==========================================================
# Renderer
# ==========================================================

def render_incident_markdown(incident: Dict, rel: Dict) -> str:

    root = incident.get("root_cause", {})
    component = root.get("component")
    failure_mode = root.get("failure_mode")
    status_class = root.get("status_class")
    behavior = root.get("cluster_behavior")

    trigger = root.get("representative_raw_text")

    blast_radius = len(root.get("downstream_neighbors", []))

    summary = root.get("cluster_summary", {})
    signals = root.get("signals", {})
    timeline = sorted(
        incident.get("timeline", []),
        key=lambda x: x.get("timestamp") or ""
    )

    service = component or _root_service(summary)

    error_count = signals.get("error_count") or 0
    trigger_score = signals.get("trigger_score")

    severity = _severity(error_count)

    http_dist = summary.get("http_class_counts", {})

    incident_type = _incident_type(http_dist)

    trigger_event = root.get("first_seen_event", {})
    rep_event = root.get("representative_event", {})

    propagation = _propagation_chain(timeline)
    delays = _propagation_delays(timeline)

    reasons = _confidence_reasons(root, timeline)
    cluster_behavior = describe_cluster(root)

    md = []

    md.append("# Semantic RCA Report\n\n")

    md.append("## Incident Window\n\n")

    win = incident.get("incident_window", {})

    md.append(f"{win.get('start_time')} → {win.get('end_time')}\n\n")
    md.append("")
    md.append(f"Incident Severity: **{severity}**\n")
    md.append("")
    md.append(f"Incident Type: **{incident_type}**\n\n")
    md.append("")

    md.append("---\n\n")

    md.append("## Incident Relationships\n\n")

    if rel.get("parents"):
        md.append(f"Parent Incidents: {', '.join(rel['parents'])}\n")
    md.append("")
    if rel.get("children"):
        md.append(f"Child Incidents: {', '.join(rel['children'])}\n")
    md.append("")
    if rel.get("is_downstream"):
        md.append("Classification: Downstream incident\n")
    else:
        md.append("Classification: Primary incident\n")
    md.append("")

    md.append("\n---\n\n")

    md.append("## Primary Trigger\n\n")
    md.append(_format_event(trigger_event) + "\n\n")

    md.append("---\n\n")

    md.append("## Root Cause Candidate\n\n")
    md.append("")
    md.append(f"Component: **{service}**\n\n")
    md.append("")
    if failure_mode:
        md.append(f"Failure Mode: **{failure_mode}**\n\n")
    md.append("")
    if status_class:
        md.append(f"Status Class: **{status_class}**\n\n")
    md.append("")
    if cluster_behavior:
        md.append("### Cluster Behavior\n\n")
        md.append(f"{cluster_behavior}\n\n")
    md.append("")

    md.append(
        f"Detected **{error_count} anomalous events** "
        f"(trigger_score={trigger_score}).\n\n"
    )
    md.append("")
    if trigger:
        md.append('### Trigger Explanation \n\n')
        md.append(f"{trigger}\n\n")

    md.append("")
    confidence = root.get("confidence", {})

    md.append(
        f"Confidence: **{confidence.get('label', 'unknown')}** "
        f"({confidence.get('value', 'unknown')})\n\n"
    )

    md.append("---\n\n")

    md.append("## Representative Failure\n\n")
    md.append(_format_event(rep_event) + "\n\n")

    md.append("---\n\n")

    md.append("## Error Distribution\n\n")

    for k, v in http_dist.items():
        md.append(f"{k}: {v}\n")

    md.append("\n---\n\n")

    md.append("## Propagation Chain\n\n")

    if propagation:
        md.append(" → ".join(propagation) + "\n\n")

    md.append("---\n\n")

    md.append("## Propagation Delays\n\n")

    if delays:
        for d in delays:
            md.append(d + "\n")
    else:
        md.append("Unable to compute propagation delay\n")

    md.append("\n---\n\n")

    md.append("## Failure Timeline\n\n")

    rows = _timeline_rows(timeline)

    for ts, svc, rc in rows:
        md.append(f"{ts} — {svc} (HTTP {rc})\n")

    md.append("\n---\n\n")

    md.append("## Confidence Reasoning\n\n")

    for r in reasons:
        md.append(f"- {r}\n")

    md.append("\n---\n\n")

    md.append("## Deterministic Conclusion\n\n")

    if rel.get("is_downstream"):
        parent_txt = ", ".join(rel.get("parents", [])) if rel.get("parents") else "an earlier incident"
        md.append(
            f"This incident is classified as a **downstream incident**. "
            f"The primary affected component in this phase is **{service}**, "
            f"described as: **{cluster_behavior}**. "
            f"The broader failure sequence appears to descend from **{parent_txt}**. "
            f"Temporal ordering and anomaly concentration show that this incident reflects "
            f"secondary operational fallout rather than the original initiating failure.\n"
        )
    else:
        md.append(
            f"The earliest anomaly burst originates from **{service}**, "
            f"described as: **{cluster_behavior}**. "
            f"Temporal ordering, error concentration, and trigger score "
            f"identify this component as the most probable origin of the "
            f"incident.\n"
        )

    return "".join(md)


# ==========================================================
# Incident Graph Loader
# ==========================================================

def _load_incident_graph(output_dir: Path) -> Dict:

    graph_path = output_dir.parent / "incident_graph" / "incident_graph.json"

    if not graph_path.exists():
        return {}

    try:
        return json.loads(graph_path.read_text(encoding="utf-8"))

    except Exception:
        return {}


# ==========================================================
# Summarizer driver
# ==========================================================

class LLMSummarizer:

    def __init__(self, provider=None, output_dir: Optional[Path] = None):

        self.provider = provider
        self.output_dir = output_dir or Path("outputs/llm")

        self.output_dir.mkdir(parents=True, exist_ok=True)

    def summarize_incidents(self, evidence_bundle: Dict):

        incidents = evidence_bundle.get("incidents")

        graph = _load_incident_graph(self.output_dir)

        if not incidents:
            raise RuntimeError("Evidence bundle missing incidents")

        paths = []

        for inc in incidents:
            rel = _incident_relationships(graph, inc.get("incident_id"))
            iid = inc.get("incident_id") or "UNKNOWN"
            md = render_incident_markdown(inc, rel)

            path = self.output_dir / f"incident_{iid}_summary.md"

            path.write_text(md, encoding="utf-8")

            paths.append(path)

        return paths