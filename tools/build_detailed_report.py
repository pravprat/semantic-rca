#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _incident_map(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for item in items:
        iid = item.get("incident_id")
        if iid:
            out[iid] = item
    return out


def _complexity_label(total_edges: Any) -> str:
    try:
        n = int(total_edges)
    except Exception:
        return "unknown"
    if n >= 80:
        return "high"
    if n >= 25:
        return "medium"
    return "low"


def _recommended_actions(summary: Dict[str, Any]) -> List[str]:
    pattern = (summary.get("pattern") or "").lower()
    actions: List[str] = []

    if "authorization" in pattern or "rbac" in pattern:
        actions.append("Review recent RBAC and policy changes affecting the primary actor and namespace.")
        actions.append("Validate service account, role, and rolebinding permissions for impacted resources.")
    elif "resource" in pattern and "not_found" in pattern:
        actions.append("Validate object existence and reconcile deployment/controller references.")
        actions.append("Check rollout order and dependency creation timing.")
    elif "service" in pattern or "5xx" in pattern:
        actions.append("Check component health, restart events, and upstream dependency availability.")
        actions.append("Inspect error spikes around the incident start window.")
    else:
        actions.append("Validate recent config, policy, and deployment changes around incident start time.")
        actions.append("Correlate affected actors/resources with control-plane and admission events.")

    return actions


def build_detailed_report_json(
    base_report_path: Path,
    evidence_bundle_path: Path,
    output_json_path: Path,
) -> List[Dict[str, Any]]:
    base_reports = _load_json(base_report_path)
    bundles = _load_json(evidence_bundle_path)

    bundle_map = _incident_map(bundles)
    detailed: List[Dict[str, Any]] = []

    for report in base_reports:
        iid = report.get("incident_id")
        bundle = bundle_map.get(iid, {})

        entry = dict(report)
        entry["report_version"] = "detailed_v1"
        entry["evidence_bundle_ref"] = {
            "incident_id": iid,
            "bundle_version": bundle.get("bundle_version"),
            "coverage": bundle.get("coverage", {}),
        }
        entry["forensic_details"] = {
            "claims": bundle.get("claims", []),
            "chain_summary": bundle.get("chain_summary", {}),
            "anomaly_onset": bundle.get("anomaly_onset", {}),
            "post_anomaly_impacts": bundle.get("post_anomaly_impacts", {}),
            "lineage": bundle.get("lineage", {}),
        }
        detailed.append(entry)

    output_json_path.parent.mkdir(parents=True, exist_ok=True)
    with output_json_path.open("w", encoding="utf-8") as f:
        json.dump(detailed, f, ensure_ascii=False, indent=2)

    return detailed


def render_detailed_markdown(
    detailed_reports: List[Dict[str, Any]],
    output_md_path: Path,
) -> None:
    lines: List[str] = []
    lines.append("# Semantic RCA Detailed Report\n")
    lines.append("---\n")

    for rpt in detailed_reports:
        iid = rpt.get("incident_id")
        summary = rpt.get("root_cause_summary", {})
        conf = rpt.get("confidence", {})
        evidence_ref = rpt.get("evidence_bundle_ref", {})
        forensic = rpt.get("forensic_details", {})
        pattern = summary.get("pattern")

        lines.append(f"# Incident {iid}\n")

        window = rpt.get("incident_window", {})
        if window:
            lines.append("## Incident Window\n")
            lines.append(
                f"{window.get('start_time')} -> {window.get('end_time')} "
                f"({window.get('duration_seconds')}s)\n"
            )

        # ------------------------------------------------------------
        # Support-first narrative section
        # ------------------------------------------------------------
        lines.append("## Incident Narrative\n")
        lines.append(
            f"This incident is classified as **{summary.get('type')}**. "
            f"The earliest high-signal failure was observed for actor "
            f"`{summary.get('primary_actor')}` on resource `{summary.get('primary_resource')}` "
            f"with response code `{summary.get('primary_response_code')}`.\n"
        )
        if rpt.get("explanation"):
            lines.append(rpt.get("explanation") + "\n")

        lines.append("## Support Impact Summary\n")
        chain = forensic.get("chain_summary", {})
        complexity = _complexity_label(chain.get("all_graph_edges"))
        lines.append(f"- **Primary issue type:** {summary.get('type')}")
        lines.append(f"- **Likely immediate spread:** {chain.get('direct_downstream_edges')} directly connected failure patterns")
        lines.append(
            f"- **Incident complexity:** {complexity} "
            f"({chain.get('all_graph_edges')} causal links observed)"
        )
        lines.append(f"- **Confidence:** {conf.get('score')} ({conf.get('label')})\n")

        lines.append("## Detection Timeline\n")
        onset = forensic.get("anomaly_onset", {})
        lines.append(f"- **First anomaly observed at:** {onset.get('first_anomaly_timestamp')}")
        lines.append(f"- **First anomaly event ID:** {onset.get('first_anomaly_event_id')}")
        lines.append(f"- **Detection rule:** {onset.get('detection_rule')}")
        lines.append(f"- **Delta to primary root event:** {onset.get('delta_to_primary_seconds')} seconds\n")

        impacts = forensic.get("post_anomaly_impacts", {})
        lines.append("## Post-Root Impact Timeline\n")
        lines.append(f"- **Window start (t0):** {impacts.get('window_start')}")
        lines.append(f"- **Window end:** {impacts.get('window_end')}")
        lines.append(f"- **Events after anomaly:** {impacts.get('events_after_anomaly')}")
        lines.append(f"- **Failure events after anomaly:** {impacts.get('failure_events_after_anomaly')}")
        lines.append(f"- **First 5xx observed at:** {impacts.get('first_5xx_timestamp')}")
        lines.append(f"- **Delta t0 -> first 5xx:** {impacts.get('first_5xx_delta_seconds')} seconds")
        if impacts.get("summary"):
            lines.append(f"- **Summary:** {impacts.get('summary')}")
        status_counts = impacts.get("status_class_counts_after_anomaly", {})
        if isinstance(status_counts, dict) and status_counts:
            lines.append("- **Status class counts after anomaly:**")
            for k, v in sorted(status_counts.items(), key=lambda x: x[0]):
                lines.append(f"  - {k}: {v}")
        mode_breakdown = impacts.get("failure_domain_breakdown_after_anomaly", [])
        if isinstance(mode_breakdown, list) and mode_breakdown:
            lines.append("- **Failure domain breakdown (post-anomaly):**")
            for row in mode_breakdown:
                lines.append(f"  - {row.get('failure_mode')}: {row.get('count')}")
        comp_breakdown = impacts.get("component_failure_breakdown_after_anomaly", [])
        if isinstance(comp_breakdown, list) and comp_breakdown:
            lines.append("- **Component breakdown (post-anomaly):**")
            for row in comp_breakdown[:8]:
                lines.append(f"  - {row.get('component')}: {row.get('count')}")
        dep_edges = impacts.get("observed_dependency_impacts_after_anomaly", [])
        if isinstance(dep_edges, list) and dep_edges:
            lines.append("- **Observed downstream dependency impacts (what broke what):**")
            for row in dep_edges[:8]:
                src_meta = row.get("source_meta") or {}
                tgt_meta = row.get("target_meta") or {}
                lines.append(
                    f"  - {row.get('source_service')} -> {row.get('target_service')} "
                    f"(count={row.get('count')}, first_seen={row.get('first_seen')}, "
                    f"failure_location={row.get('failure_location')}, causal_confidence_tier={row.get('causal_confidence_tier')}, "
                    f"source_domain={src_meta.get('domain')}, target_domain={tgt_meta.get('domain')}, "
                    f"target_system={tgt_meta.get('system')}, target_owner_hint={tgt_meta.get('owner_hint')})"
                )
        lift = impacts.get("pre_vs_post_failure_lift", {})
        if isinstance(lift, dict) and lift:
            lines.append("- **Pre vs post anomaly failure degradation:**")
            lines.append(
                f"  - pre_count={lift.get('pre_failure_count')}, post_count={lift.get('post_failure_count')}, "
                f"pre_rate={lift.get('pre_failure_rate_eps')} eps, post_rate={lift.get('post_failure_rate_eps')} eps, "
                f"overall_lift={lift.get('overall_lift_ratio')}"
            )
            lines.append(
                f"  - fixed_window={lift.get('fixed_window_minutes')}m pre/post around t0: "
                f"pre_count={lift.get('fixed_pre_failure_count')}, post_count={lift.get('fixed_post_failure_count')}, "
                f"pre_rate={lift.get('fixed_pre_failure_rate_eps')} eps, post_rate={lift.get('fixed_post_failure_rate_eps')} eps, "
                f"overall_lift={lift.get('fixed_overall_lift_ratio')}"
            )
            mode_lifts = lift.get("failure_mode_lifts", [])
            if isinstance(mode_lifts, list) and mode_lifts:
                lines.append("  - top failure-mode lifts:")
                for row in mode_lifts[:5]:
                    lines.append(
                        f"    - {row.get('failure_mode')}: pre={row.get('pre_count')}, post={row.get('post_count')}, "
                        f"pre_rate={row.get('pre_rate_eps')} eps, post_rate={row.get('post_rate_eps')} eps, lift={row.get('lift_ratio')}"
                    )
            fixed_mode_lifts = lift.get("fixed_failure_mode_lifts", [])
            if isinstance(fixed_mode_lifts, list) and fixed_mode_lifts:
                lines.append("  - top failure-mode lifts (fixed pre/post window):")
                for row in fixed_mode_lifts[:5]:
                    lines.append(
                        f"    - {row.get('failure_mode')}: pre={row.get('pre_count')}, post={row.get('post_count')}, "
                        f"pre_rate={row.get('pre_rate_eps')} eps, post_rate={row.get('post_rate_eps')} eps, lift={row.get('lift_ratio')}"
                    )
        top_services = impacts.get("top_impacted_services", [])
        if isinstance(top_services, list) and top_services:
            lines.append("- **Top impacted services:**")
            for row in top_services:
                lines.append(f"  - {row.get('service')}: {row.get('count')}")
        top_resources = impacts.get("top_impacted_resources", [])
        if isinstance(top_resources, list) and top_resources:
            lines.append("- **Top impacted resources:**")
            for row in top_resources:
                lines.append(f"  - {row.get('resource')}: {row.get('count')}")
        lines.append("")

        lines.append("## Suggested Next Actions\n")
        for action in _recommended_actions(summary):
            lines.append(f"- {action}")
        lines.append("")

        lines.append("## Evidence Coverage\n")
        cov = evidence_ref.get("coverage", {})
        lines.append(
            f"- claims_with_evidence={cov.get('claims_with_evidence')} / "
            f"claims_total={cov.get('claims_total')} "
            f"({cov.get('coverage_pct')}%)\n"
        )

        lines.append("## Forensic Claims\n")
        claims = forensic.get("claims", [])
        if not claims:
            lines.append("- No forensic claims found.\n")
        else:
            for claim in claims:
                lines.append(f"- **{claim.get('claim_id')}**: {claim.get('statement')}")
                lines.append(
                    f"  - confidence={claim.get('confidence')}, "
                    f"cluster={((claim.get('supports') or {}).get('candidate') or {}).get('cluster_id')}"
                )

                op = claim.get("operator_view", {})
                lines.append(f"  - operator_label={op.get('operator_label')}")
                if op.get("operator_summary"):
                    lines.append(f"  - operator_summary={op.get('operator_summary')}")

        # ------------------------------------------------------------
        # Technical appendix (engineer-facing supplemental details)
        # ------------------------------------------------------------
        lines.append("## Technical Appendix\n")
        lines.append(f"- pattern={pattern}")
        lines.append(f"- primary_cluster_id={summary.get('primary_cluster_id')}")
        lines.append(f"- primary_event_id={summary.get('primary_event_id')}")
        lines.append(f"- direct_downstream_edges={chain.get('direct_downstream_edges')}")
        lines.append(f"- total_graph_edges={chain.get('all_graph_edges')}")
        lines.append(f"- first_anomaly_cluster_id={onset.get('first_anomaly_cluster_id')}")
        lines.append(f"- first_anomaly_response_code={onset.get('first_anomaly_response_code')}")
        lines.append(f"- confidence_label={conf.get('label')}\n")

        lines.append("---\n")

    output_md_path.parent.mkdir(parents=True, exist_ok=True)
    with output_md_path.open("w", encoding="utf-8") as f:
        f.write("\n".join(lines))


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Build detailed RCA report by merging base report and evidence bundle."
    )
    p.add_argument("--base-report", default="outputs/incident_rca_report.json")
    p.add_argument("--evidence-bundle", default="outputs/incident_evidence_bundle.json")
    p.add_argument("--output-json", default="outputs/incident_rca_report_detailed.json")
    p.add_argument("--output-md", default="outputs/incident_rca_report_detailed.md")
    p.add_argument("--skip-md", action="store_true", help="Only write detailed JSON report")
    return p.parse_args()


def main() -> None:
    args = parse_args()

    detailed = build_detailed_report_json(
        base_report_path=Path(args.base_report),
        evidence_bundle_path=Path(args.evidence_bundle),
        output_json_path=Path(args.output_json),
    )
    print(f"[detailed_report] incidents={len(detailed)} -> {args.output_json}")

    if not args.skip_md:
        render_detailed_markdown(
            detailed_reports=detailed,
            output_md_path=Path(args.output_md),
        )
        print(f"[detailed_report] markdown -> {args.output_md}")


if __name__ == "__main__":
    main()

