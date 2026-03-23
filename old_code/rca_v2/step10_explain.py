from __future__ import annotations

from pathlib import Path
from typing import Any, Dict, List

from .utils import load_json, write_json, write_text


# ---------------------------------------------------------
# Reasoning helpers
# ---------------------------------------------------------

def _build_root_cause_reasoning(top: Dict[str, Any]) -> List[str]:
    signals = top.get("signals", {}) or {}
    primary = top.get("primary_pattern", {}) or {}

    reasons: List[str] = []

    if signals.get("earliest_first_seen"):
        reasons.append("Earliest observed failure pattern in the incident window")

    if signals.get("actor_diversity", 0) >= 2:
        reasons.append("Affects multiple system actors (systemic impact)")

    if signals.get("control_plane_count", 0) >= 1:
        reasons.append("Impacts control-plane components")

    if primary.get("http_class") == "4xx":
        reasons.append("Consistent authorization/client error responses")

    if primary.get("http_class") == "5xx":
        reasons.append("Indicates service instability (5xx errors)")

    if signals.get("max_trigger_score", 0) > 1.0:
        reasons.append("High trigger intensity during incident window")

    return reasons

def _build_causal_chain(top: Dict[str, Any]) -> List[str]:
    patterns = top.get("supporting_patterns", []) or []

    if not patterns:
        return []

    ordered = sorted(
        [p for p in patterns if p.get("first_seen")],
        key=lambda x: x.get("first_seen")
    )

    if len(ordered) < 2:
        return []

    chain = []

    first = ordered[0]
    first_http = first.get("http_class")

    # Step 1: root cause
    chain.append(
        f"{first.get('failure_domain')} failure caused repeated {first_http} responses"
    )

    # Step 2: bridge (THIS IS THE KEY ADDITION)
    if first_http == "4xx":
        chain.append(
            "These authorization failures prevented successful resource access"
        )
    elif first_http == "5xx":
        chain.append(
            "Service instability propagated across dependent components"
        )

    # Step 3: downstream effects
    downstream = ordered[1:]

    affected_services = {
        p.get("service") for p in downstream if p.get("service")
    }

    if affected_services:
        services_str = ", ".join(sorted(affected_services))
        chain.append(
            f"This resulted in widespread failures across services: {services_str}"
        )

    return chain

def _build_causal_statement(top: Dict[str, Any]) -> str:
    primary = top.get("primary_pattern", {}) or {}

    verb = primary.get("verb")
    resource = primary.get("resource")
    service = primary.get("service")
    http_class = primary.get("http_class")

    return (
        f"The failure originates from `{service}` performing `{verb} {resource}` "
        f"operations resulting in `{http_class}` responses."
    )


# ---------------------------------------------------------
# Incident Summary (Investor-grade)
# ---------------------------------------------------------

def _incident_summary_md(incident: Dict[str, Any]) -> str:
    iid = incident.get("incident_id")
    start = incident.get("start_time")
    end = incident.get("end_time")
    candidates = incident.get("root_cause_candidates", []) or []

    lines: List[str] = []

    lines.append(f"# Incident {iid}")
    lines.append("")
    lines.append("## Incident Window")
    lines.append("")
    lines.append(f"{start} → {end}")
    lines.append("")

    if not candidates:
        lines.append("No root cause candidates detected.")
        return "\n".join(lines)

    top = candidates[0]
    primary = top.get("primary_pattern", {}) or {}
    confidence = top.get("confidence", {}) or {}
    signals = top.get("signals", {}) or {}

    # ---------------------------------------------------------
    # Primary Issue
    # ---------------------------------------------------------

    lines.append("## Root Cause")
    lines.append("")
    lines.append(f"**{top.get('title')}**")
    lines.append("")
    lines.append(top.get("summary", ""))
    lines.append("")

    # ---------------------------------------------------------
    # Causal reasoning
    # ---------------------------------------------------------

    lines.append("## Why this is the Root Cause")
    lines.append("")

    for r in _build_root_cause_reasoning(top):
        lines.append(f"- {r}")

    lines.append("")

    lines.append("## Causal Chain")
    lines.append("")

    chain = _build_causal_chain(top)

    if chain:
        for step in chain:
            lines.append(f"- {step}")
    else:
        lines.append(_build_causal_statement(top))

    lines.append("")

    lines.append("")

    # ---------------------------------------------------------
    # Signals
    # ---------------------------------------------------------

    lines.append("## Key Signals")
    lines.append("")
    lines.append(f"- Confidence: {confidence.get('label')} ({confidence.get('value')})")
    lines.append(f"- Event count: {signals.get('event_count', 0)}")
    lines.append(f"- Error count: {signals.get('error_count', 0)}")

    if signals.get("actor_diversity") is not None:
        lines.append(f"- Actor diversity: {signals.get('actor_diversity')}")

    if signals.get("control_plane_count") is not None:
        lines.append(f"- Control-plane impact: {signals.get('control_plane_count')} patterns")

    if signals.get("max_trigger_score") is not None:
        lines.append(f"- Trigger score: {signals.get('max_trigger_score')}")

    lines.append("")

    # ---------------------------------------------------------
    # Primary pattern
    # ---------------------------------------------------------

    lines.append("## Primary Pattern")
    lines.append("")
    lines.append(f"- Service: {primary.get('service')}")
    lines.append(f"- Operation: {primary.get('verb')} {primary.get('resource')}")
    lines.append(f"- Status: {primary.get('http_class')}")
    lines.append(f"- First seen: {primary.get('first_seen')}")
    lines.append(f"- Last seen: {primary.get('last_seen')}")
    lines.append("")

    # ---------------------------------------------------------
    # Evidence
    # ---------------------------------------------------------

    examples = primary.get("examples", []) or []
    if examples:
        lines.append("## Evidence")
        lines.append("")
        for ex in examples[:3]:
            lines.append("```")
            lines.append(ex)
            lines.append("```")
            lines.append("")

    # ---------------------------------------------------------
    # Other candidates
    # ---------------------------------------------------------

    if len(candidates) > 1:
        lines.append("## Alternative Hypotheses")
        lines.append("")
        for c in candidates[1:]:
            conf = c.get("confidence", {}) or {}
            lines.append(
                f"- {c.get('title')} (score={c.get('score')}, confidence={conf.get('label')})"
            )
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------
# Master report
# ---------------------------------------------------------

def _master_report_md(root_causes: Dict[str, Any]) -> str:
    lines: List[str] = []
    lines.append("# Semantic RCA Report")
    lines.append("")

    for inc in root_causes.get("incidents", []):
        iid = inc.get("incident_id")
        start = inc.get("start_time")
        end = inc.get("end_time")
        candidates = inc.get("root_cause_candidates", []) or []

        lines.append("---")
        lines.append(f"## Incident {iid}")
        lines.append("")
        lines.append(f"Window: {start} → {end}")
        lines.append("")

        if not candidates:
            lines.append("No root cause candidates detected.")
            lines.append("")
            continue

        top = candidates[0]
        conf = top.get("confidence", {}) or {}

        lines.append(f"Root Cause: **{top.get('title')}**")
        lines.append("")
        lines.append(top.get("summary", ""))
        lines.append("")
        lines.append(f"- Confidence: {conf.get('label')} ({conf.get('value')})")
        lines.append("")

    return "\n".join(lines)


# ---------------------------------------------------------
# Entry point
# ---------------------------------------------------------

def write_rca_outputs(
    artifacts_dir: Path,
    ranked_path: Path | None = None,
) -> Dict[str, Any]:

    artifacts_dir = Path(artifacts_dir)
    ranked = load_json(ranked_path or (artifacts_dir / "incident_root_causes.json"), default={}) or {}

    evidence_bundle = {
        "bundle_version": "v2.step10.final",
        "incidents": [],
    }

    for inc in ranked.get("incidents", []):
        iid = inc.get("incident_id")
        candidates = inc.get("root_cause_candidates", []) or []
        top = candidates[0] if candidates else None

        payload = {
            "incident_id": iid,
            "incident_window": {
                "start_time": inc.get("start_time"),
                "end_time": inc.get("end_time"),
            },
            "root_cause": top,
            "other_candidates": candidates[1:] if len(candidates) > 1 else [],
            "stats": {
                "candidate_count": len(candidates),
                "has_root_cause": top is not None,
            },
        }

        evidence_bundle["incidents"].append(payload)

        write_json(artifacts_dir / f"incident_{iid}.json", payload)
        write_text(artifacts_dir / f"incident_{iid}_summary.md", _incident_summary_md(inc))

    write_json(artifacts_dir / "evidence_bundle.json", evidence_bundle)
    write_text(artifacts_dir / "incident_rca_report.md", _master_report_md(ranked))

    return evidence_bundle


if __name__ == "__main__":
    write_rca_outputs(Path("outputs"))