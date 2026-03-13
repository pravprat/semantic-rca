from __future__ import annotations

import json
import csv
import io
from pathlib import Path
from typing import Dict, Any, List

from tools.cluster_explainer import describe_cluster, short_cluster_label


# ------------------------------------------------------------
# Helpers
# ------------------------------------------------------------

def _parse_representative_event(raw: str) -> Dict[str, str]:
    """
    Parse representative_raw_text CSV audit log into readable fields
    """

    if not raw:
        return {}

    try:
        reader = csv.reader(io.StringIO(raw))
        row = next(reader)

        return {
            "timestamp": row[1] if len(row) > 1 else "",
            "user": row[2] if len(row) > 2 else "",
            "verb": row[3] if len(row) > 3 else "",
            "resource": row[4] if len(row) > 4 else "",
            "namespace": row[6] if len(row) > 6 else "",
            "endpoint": row[7] if len(row) > 7 else "",
            "status": row[11] if len(row) > 11 else "",
        }

    except Exception:
        return {}


def _human_description(event: Dict[str, str]) -> str:

    if not event:
        return "Unknown system behavior"

    user = event.get("user", "unknown user")
    verb = event.get("verb", "operation")
    resource = event.get("resource", "resource")
    endpoint = event.get("endpoint", "")
    status = event.get("status", "")

    return (
        f"{user} attempted to {verb} {resource} "
        f"via {endpoint} resulting in HTTP {status}"
    )

def _build_mermaid_graph(root_cluster: str, downstream_neighbors: List[Dict[str, Any]]) -> str:

    if not downstream_neighbors:
        return ""

    lines = []
    lines.append("```mermaid")
    lines.append("flowchart TD")

    root_label = root_cluster

    for n in sorted(downstream_neighbors, key=lambda x: x.get("cluster_id", "")):
        cid = n.get("cluster_id")

        if not cid:
            continue

        # try to derive readable label
        node_label = short_cluster_label(n)

        if not node_label or node_label.lower().startswith("unknown"):
            node_label = cid

        node_label = node_label.replace('"', "'")

        lines.append(f'{root_label}["{root_cluster}"] --> {cid}["{node_label}"]')

    lines.append("```")

    return "\n".join(lines)


def _blast_radius(root: Dict[str, Any]) -> int:
    downstream = root.get("downstream_neighbors", [])
    return len(downstream) if downstream else 0


def _lead_lag_sections(root: Dict[str, Any]) -> Dict[str, List[Dict[str, Any]]]:
    """
    Very light deterministic grouping:
    - lag: evidence neighbors
    - lead: downstream neighbors
    """
    return {
        "lag": root.get("evidence_neighbors", []) or [],
        "lead": root.get("downstream_neighbors", []) or [],
    }


# ------------------------------------------------------------
# Main report writer
# ------------------------------------------------------------

def write_incident_rca_report(
    *,
    incident_rca_path: Path,
    clusters_path: Path,
    out_path: Path
):

    if not Path(incident_rca_path).exists():
        raise FileNotFoundError(incident_rca_path)

    incident_rca = json.loads(Path(incident_rca_path).read_text())

    if isinstance(incident_rca, dict) and "incidents" in incident_rca:
        incident_rca = incident_rca["incidents"]

    # -------------------------------------------------
    # Normalize incident structure
    # -------------------------------------------------

    if isinstance(incident_rca, dict):

        # case: {"I1": {...}, "I2": {...}}
        if all(isinstance(v, dict) for v in incident_rca.values()):
            incident_rca = list(incident_rca.values())

        # case: {"incidents": [...]}
        elif "incidents" in incident_rca:
            incident_rca = incident_rca["incidents"]

    if not isinstance(incident_rca, list):
        raise ValueError("incident_root_causes.json must contain a list of incidents")

    clusters = {}
    if Path(clusters_path).exists():
        clusters = json.loads(Path(clusters_path).read_text())

    lines: List[str] = []

    lines.append("# Semantic RCA Report")
    lines.append("")

    for incident in incident_rca:

        incident_id = incident.get("incident_id", "unknown")
        start = incident.get("start_time", "")
        end = incident.get("end_time", "")

        lines.append("---")
        lines.append(f"# Incident {incident_id}")
        lines.append("")

        lines.append("## Incident Window")
        lines.append(f"{start} → {end}")
        lines.append("")

        candidates = (
            incident.get("root_cause_candidates")
            or incident.get("candidates")
            or []
        )

        if not candidates:
            lines.append("No root cause candidates detected.\n")
            continue

        root = candidates[0]
        others = candidates[1:]

        event = _parse_representative_event(root.get("representative_raw_text"))
        description = _human_description(event)
        cluster_behavior = describe_cluster(root)

        # --------------------------------------------------
        # Root cause section
        # --------------------------------------------------

        lines.append("## Root Cause")
        lines.append("")

        lines.append(f"Cluster: `{root.get('cluster_id')}`")
        lines.append(f"Score: {root.get('score', 0):.2f}")
        lines.append("")

        component = root.get("component")
        failure_mode = root.get("failure_mode")
        status_class = root.get("status_class")
        behavior = root.get("cluster_behavior")

        if component or failure_mode:
            lines.append("")
            lines.append(f"Component: {component}")
            lines.append(f"Failure Mode: {failure_mode}")
            lines.append(f"Status Class: {status_class}")

        if behavior:
            lines.append("")
            lines.append("Behavior:")
            lines.append(f"{behavior}")

        lines.append("")

        lines.append("### Cluster Behavior")
        lines.append(cluster_behavior)
        lines.append("")

        lines.append("### Trigger Explanation")
        lines.append(description)
        lines.append("")

        lines.append("### Key Signals")
        lines.append(f"- trigger_score: {root.get('trigger_score')}")
        lines.append(f"- error_count: {root.get('error_count')}")
        lines.append(f"- graph_out_weight: {root.get('out_weight')}")
        lines.append(f"- graph_in_weight: {root.get('in_weight')}")
        if root.get("confidence"):
            conf = root.get("confidence", {})
            lines.append(
                f"- confidence: {conf.get('label', 'unknown')} ({conf.get('value', 'unknown')})"
            )
        lines.append("")

        # --------------------------------------------------
        # Blast radius
        # --------------------------------------------------

        lines.append("### Blast Radius")
        lines.append(f"Affected downstream clusters: **{_blast_radius(root)}**")
        lines.append("")

        # --------------------------------------------------
        # Trigger / Lag / Lead
        # --------------------------------------------------

        sections = _lead_lag_sections(root)

        lines.append("### Trigger / Lag / Lead")
        lines.append("")
        lines.append(f"- Trigger: {cluster_behavior}")

        if sections["lag"]:
            lag_items = [short_cluster_label(x) for x in sections["lag"][:5]]
            lines.append(f"- Lag: {' ; '.join(lag_items)}")
        else:
            lines.append("- Lag: none detected")

        if sections["lead"]:
            lead_items = [short_cluster_label(x) for x in sections["lead"][:5]]
            lines.append(f"- Lead: {' ; '.join(lead_items)}")
        else:
            lines.append("- Lead: none detected")

        lines.append("")

        # --------------------------------------------------
        # Propagation graph
        # --------------------------------------------------

        lines.append("### Causal Propagation")

        mermaid = _build_mermaid_graph(
            root.get("cluster_id"),
            root.get("downstream_neighbors", [])
        )

        if mermaid:
            lines.append(mermaid)
        else:
            lines.append("No downstream propagation detected.")

        lines.append("")

        # --------------------------------------------------
        # Evidence
        # --------------------------------------------------

        lines.append("### Primary Evidence Event")

        lines.append("```")
        lines.append(root.get("representative_raw_text", ""))
        lines.append("```")
        lines.append("")

        # --------------------------------------------------
        # Additional candidates
        # --------------------------------------------------

        if others:

            lines.append("## Other Possible Contributors")
            lines.append("")
            lines.append("| Rank | Cluster | Behavior | Score | Errors |")
            lines.append("|------|--------|----------|------|------|")

            for i, o in enumerate(others, start=2):
                behavior = describe_cluster(o).replace("|", "/")
                lines.append(
                    f"| {i} | {o.get('cluster_id')} | "
                    f"{behavior} | "
                    f"{o.get('score', 0):.2f} | "
                    f"{o.get('error_count')} |"
                )

            lines.append("")

    Path(out_path).write_text("\n".join(lines), encoding="utf-8")

    print(f"[report] wrote -> {out_path}")