# semantic-rca/rca_explainer.py

from __future__ import annotations

import json
from pathlib import Path
from typing import Dict, Any, List


# -------------------------------------------------------------
# Build explanation lines for each RCA candidate
# -------------------------------------------------------------
def _explain_candidate(rc: Dict[str, Any]) -> List[str]:

    lines: List[str] = []

    size = rc.get("size", 0)
    err = rc.get("error_count", 0)
    prox = rc.get("trigger_proximity", 0.0)

    actor = rc.get("dominant_actor", "")
    operation = rc.get("dominant_operation", "")
    resource = rc.get("dominant_resource", "")
    status = rc.get("dominant_status", "")

    lines.append(f"Behavior: {actor} {operation} {resource} (HTTP {status})")
    lines.append(f"Cluster size: {size} events")

    if err:
        lines.append(f"Error burst detected: {err} anomalous events")

    if prox is not None:
        lines.append(f"Trigger proximity to incident start: {round(prox,3)}")

    neighbors = rc.get("downstream_neighbors") or []
    if neighbors:
        lines.append(f"Downstream clusters affected: {len(neighbors)}")

    return lines


# -------------------------------------------------------------
# Build explanations JSON
# -------------------------------------------------------------
def build_incident_explanations(
    incident_rca_path: Path,
    out_path: Path
) -> Dict[str, Any]:

    with incident_rca_path.open("r", encoding="utf-8") as f:
        data = json.load(f)

    output: Dict[str, Any] = {"incidents": []}

    for inc in data.get("incidents", []):

        explanations = []

        for rc in inc.get("root_cause_candidates", []):

            behavior = rc.get("cluster_behavior")

            explanation_lines = _explain_candidate(rc)

            explanations.append(
                {
                    "cluster_id": rc.get("cluster_id"),
                    "behavior": behavior,
                    "score": rc.get("score"),
                    "explanation": explanation_lines
                }
            )

        output["incidents"].append(
            {
                "incident_id": inc.get("incident_id"),
                "start_time": inc.get("start_time"),
                "end_time": inc.get("end_time"),
                "explanations": explanations
            }
        )

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(output, f, indent=2)

    return output


# -------------------------------------------------------------
# Write Markdown report
# -------------------------------------------------------------
def write_explanation_report(
    explanation_json: Path,
    out_md: Path
):

    with explanation_json.open("r", encoding="utf-8") as f:
        data = json.load(f)

    lines: List[str] = []

    lines.append("# Semantic RCA Explanation Report")
    lines.append("")

    for inc in data.get("incidents", []):

        lines.append("---")
        lines.append(f"# Incident {inc.get('incident_id')}")
        lines.append("")

        for rc in inc.get("explanations", []):

            lines.append(f"### Cluster {rc['cluster_id']}")
            lines.append(f"Behavior: {rc['behavior']}")
            lines.append(f"Score: {round(rc['score'],2)}")
            lines.append("")

            for l in rc["explanation"]:
                lines.append(f"- {l}")

            lines.append("")

    with out_md.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))