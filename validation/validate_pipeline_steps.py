#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import Counter
from pathlib import Path
from typing import Dict, List

from _common import (
    CheckResult,
    file_exists,
    load_json,
    load_jsonl,
    pass_fail,
    print_results,
)


def step1_validate(outputs: Path, raw_log: Path | None) -> bool:
    results: List[CheckResult] = []
    events_path = outputs / "events.jsonl"
    results.append(
        CheckResult(
            name="events.jsonl exists",
            compared=f"{events_path}",
            passed=file_exists(events_path),
            details="file present" if file_exists(events_path) else "missing",
        )
    )
    if not file_exists(events_path):
        return print_results("Step 1: Ingest", results)

    events = load_jsonl(events_path)
    results.append(
        CheckResult(
            name="events.jsonl parseable",
            compared="JSON decode for every line",
            passed=True,
            details=f"parsed lines={len(events)}",
        )
    )

    if raw_log and raw_log.exists() and raw_log.is_file():
        raw_lines = sum(1 for _ in raw_log.open("r", encoding="utf-8", errors="replace"))
        ratio = (len(events) / max(1, raw_lines)) * 100.0
        exact_match = len(events) == raw_lines
        results.append(
            CheckResult(
                name="Raw log vs events count comparison",
                compared="raw line count == events count",
                passed=exact_match,
                details=f"raw_lines={raw_lines}, events={len(events)}, ratio={ratio:.2f}%",
            )
        )
        results.append(
            CheckResult(
                name="Ingest non-empty output",
                compared="events count > 0",
                passed=len(events) > 0,
                details=f"events={len(events)}",
            )
        )
    return print_results("Step 1: Ingest", results)


def step2_validate(outputs: Path) -> bool:
    results: List[CheckResult] = []
    events_path = outputs / "events.jsonl"
    index_path = outputs / "event_index.json"
    emb_path = outputs / "event_embeddings.npy"

    for p in [events_path, index_path, emb_path]:
        results.append(
            CheckResult(
                name=f"{p.name} exists",
                compared=str(p),
                passed=file_exists(p),
                details="file present" if file_exists(p) else "missing",
            )
        )
    if not all(file_exists(p) for p in [events_path, index_path, emb_path]):
        return print_results("Step 2: Embed", results)

    events = load_jsonl(events_path)
    index = load_json(index_path)
    import numpy as np

    vec = np.load(emb_path)

    results.append(
        CheckResult(
            name="Row alignment (events/index/embeddings)",
            compared="len(events) == len(index) == embeddings.shape[0]",
            passed=(len(events) == len(index) == vec.shape[0]),
            details=f"events={len(events)}, index={len(index)}, vectors={vec.shape[0]}",
        )
    )

    fields = [
        "timestamp",
        "service",
        "severity",
        "actor",
        "verb",
        "resource",
        "response_code",
        "http_class",
        "status_family",
        "failure_hint",
        "path",
        "stage",
    ]
    mismatches: Dict[str, int] = {f: 0 for f in fields}
    for e, ix in zip(events, index):
        for f in fields:
            if e.get(f) != ix.get(f):
                mismatches[f] += 1
    bad = {k: v for k, v in mismatches.items() if v > 0}
    results.append(
        CheckResult(
            name="events.jsonl vs event_index field consistency",
            compared=f"fields={fields}",
            passed=len(bad) == 0,
            details=f"mismatches={bad if bad else 'none'}",
        )
    )
    return print_results("Step 2: Embed", results)


def _validate_exists_group(step_name: str, files: List[Path]) -> bool:
    results: List[CheckResult] = []
    for p in files:
        results.append(
            CheckResult(
                name=f"{p.name} exists",
                compared=str(p),
                passed=file_exists(p),
                details="file present" if file_exists(p) else "missing",
            )
        )
    return print_results(step_name, results)


def step3_validate(outputs: Path) -> bool:
    return _validate_exists_group(
        "Step 3: Cluster",
        [outputs / "clusters.json", outputs / "event_cluster_map.json", outputs / "clusters_stats.json"],
    )


def step4_validate(outputs: Path) -> bool:
    p = outputs / "cluster_trigger_stats.json"
    ok = _validate_exists_group("Step 4: Trigger Analysis", [p])
    if not file_exists(p):
        return ok
    tr = load_json(p)
    vals = list(tr.values())
    results = [
        CheckResult(
            name="Trigger stats non-empty",
            compared="len(cluster_trigger_stats)",
            passed=len(vals) > 0,
            details=f"clusters={len(vals)}",
        ),
        CheckResult(
            name="Candidate count visibility",
            compared="sum(is_candidate)",
            passed=True,
            details=f"candidates={sum(1 for x in vals if x.get('is_candidate'))}",
        ),
    ]
    return print_results("Step 4: Trigger Analysis (content)", results) and ok


def step5_validate(outputs: Path, compat_v142: bool = False) -> bool:
    status = outputs / "incident_detection_status.json"
    incidents = outputs / "incidents.json"
    required = [incidents] if compat_v142 else [status, incidents]
    ok = _validate_exists_group("Step 5: Incident Detection", required)
    if not ok or (not file_exists(status) and compat_v142):
        if compat_v142 and file_exists(incidents) and not file_exists(status):
            legacy_results = [
                CheckResult(
                    name="Legacy status compatibility",
                    compared="incident_detection_status.json optional in v1.4.2 mode",
                    passed=True,
                    details="status file missing but accepted in compat mode",
                )
            ]
            return print_results("Step 5: Incident Detection (legacy compat)", legacy_results)
        return False
    s = load_json(status)
    inc = load_json(incidents)
    results = [
        CheckResult(
            name="Incident status contract",
            compared="status in {incident_detected,no_incident}",
            passed=s.get("status") in {"incident_detected", "no_incident"},
            details=str(s),
        ),
        CheckResult(
            name="Incidents shape",
            compared="incidents.json is a list",
            passed=isinstance(inc, list),
            details=f"type={type(inc).__name__}, count={len(inc) if isinstance(inc, list) else 'n/a'}",
        ),
    ]
    return print_results("Step 5: Incident Detection (content)", results)


def _incident_mode(outputs: Path) -> str:
    status_path = outputs / "incident_detection_status.json"
    if not file_exists(status_path):
        return "legacy_or_unknown"
    status = load_json(status_path).get("status")
    return "no_incident" if status == "no_incident" else "incident"


def _build_detailed_stats(outputs: Path, raw_log: Path | None, compat_v142: bool) -> Dict[str, object]:
    stats: Dict[str, object] = {
        "compat_v142": compat_v142,
    }

    events_path = outputs / "events.jsonl"
    index_path = outputs / "event_index.json"
    emb_path = outputs / "event_embeddings.npy"
    trigger_path = outputs / "cluster_trigger_stats.json"
    status_path = outputs / "incident_detection_status.json"
    candidates_path = outputs / "incident_root_candidates.json"

    if file_exists(events_path):
        events = load_jsonl(events_path)
        total = len(events)
        fields = ["service", "actor", "verb", "resource", "path", "stage", "response_code", "http_class", "status_family"]
        null_counts: Dict[str, int] = {f: 0 for f in fields}
        codes = Counter()
        for e in events:
            for f in fields:
                if e.get(f) is None:
                    null_counts[f] += 1
            rc = e.get("response_code")
            if isinstance(rc, int):
                codes[rc] += 1
        null_pct = {f: round((null_counts[f] / total) * 100.0, 4) if total else 0.0 for f in fields}
        coverage_pct = {f: round(100.0 - null_pct[f], 4) for f in fields}
        ingest_stats: Dict[str, object] = {
            "events_count": total,
            "field_null_counts": null_counts,
            "field_null_percent": null_pct,
            "field_non_null_percent": coverage_pct,
            "response_code_counts": {
                "200": codes[200],
                "403": codes[403],
                "500": codes[500],
            },
        }
        if raw_log and raw_log.exists() and raw_log.is_file():
            raw_lines = sum(1 for _ in raw_log.open("r", encoding="utf-8", errors="replace"))
            ingest_stats["raw_log_lines"] = raw_lines
            ingest_stats["events_vs_raw_ratio_percent"] = round((total / max(1, raw_lines)) * 100.0, 4)
            ingest_stats["events_equals_raw_lines"] = total == raw_lines
        stats["ingest"] = ingest_stats

    if file_exists(index_path):
        idx = load_json(index_path)
        stats["embed"] = {"index_count": len(idx)}
        if file_exists(emb_path):
            import numpy as np

            vec = np.load(emb_path)
            stats["embed"]["embedding_rows"] = int(vec.shape[0])  # type: ignore[index]
            stats["embed"]["embedding_dims"] = int(vec.shape[1]) if len(vec.shape) > 1 else 0  # type: ignore[index]
            stats["embed"]["index_embedding_row_match"] = len(idx) == int(vec.shape[0])  # type: ignore[index]

    if file_exists(trigger_path):
        tr = load_json(trigger_path)
        vals = list(tr.values())
        candidates = [v for v in vals if v.get("is_candidate")]
        stats["trigger_analysis"] = {
            "cluster_count": len(vals),
            "candidate_count": len(candidates),
            "candidate_ratio_percent": round((len(candidates) / max(1, len(vals))) * 100.0, 4),
            "top_candidates": sorted(
                [
                    {
                        "cluster_id": cid,
                        "trigger_score": v.get("trigger_score"),
                        "error_count": v.get("error_count"),
                        "fallback_error_count": v.get("fallback_error_count"),
                        "event_count": v.get("event_count"),
                    }
                    for cid, v in tr.items()
                    if v.get("is_candidate")
                ],
                key=lambda x: (x.get("trigger_score") or 0.0),
                reverse=True,
            )[:5],
        }

    if file_exists(status_path):
        status = load_json(status_path)
        stats["incident_detection"] = status

    if file_exists(candidates_path):
        roots = load_json(candidates_path)
        top_roots = []
        if isinstance(roots, list):
            top_roots = roots[:5]
        elif isinstance(roots, dict):
            for inc, rows in roots.items():
                if isinstance(rows, list):
                    top_roots.extend(rows[:3])
                if len(top_roots) >= 5:
                    break
            top_roots = top_roots[:5]
        stats["root_candidates_preview"] = top_roots

    return stats


def step6_validate(outputs: Path, compat_v142: bool = False) -> bool:
    mode = _incident_mode(outputs)
    if mode == "no_incident":
        return print_results(
            "Step 6: Causal Analysis",
            [CheckResult("Skipped on no-incident path", "incident_detection_status.json", True, "not applicable")],
        )
    if mode == "legacy_or_unknown" and not compat_v142:
        return print_results(
            "Step 6: Causal Analysis",
            [CheckResult("Cannot decide path", "incident_detection_status.json", False, "missing")],
        )
    return _validate_exists_group("Step 6: Causal Analysis", [outputs / "incident_causal_graph.json", outputs / "incident_root_candidates.json", outputs / "incident_root_events.json"])


def step7_validate(outputs: Path, compat_v142: bool = False) -> bool:
    mode = _incident_mode(outputs)
    if mode == "no_incident":
        return print_results(
            "Step 7: RCA Report",
            [CheckResult("Skipped on no-incident path", "incident_detection_status.json", True, "not applicable")],
        )
    if mode == "legacy_or_unknown" and not compat_v142:
        return print_results(
            "Step 7: RCA Report",
            [CheckResult("Cannot decide path", "incident_detection_status.json", False, "missing")],
        )
    return _validate_exists_group("Step 7: RCA Report", [outputs / "incident_rca_report.json"])


def step8_validate(outputs: Path, compat_v142: bool = False) -> bool:
    mode = _incident_mode(outputs)
    if mode == "no_incident":
        return print_results(
            "Step 8: Evidence Bundle",
            [CheckResult("Skipped on no-incident path", "incident_detection_status.json", True, "not applicable")],
        )
    if mode == "legacy_or_unknown" and not compat_v142:
        return print_results(
            "Step 8: Evidence Bundle",
            [CheckResult("Cannot decide path", "incident_detection_status.json", False, "missing")],
        )
    return _validate_exists_group("Step 8: Evidence Bundle", [outputs / "incident_evidence_bundle.json"])


def step9_validate(outputs: Path, compat_v142: bool = False) -> bool:
    mode = _incident_mode(outputs)
    if mode == "no_incident":
        return print_results(
            "Step 9: Detailed Report",
            [CheckResult("Skipped on no-incident path", "incident_detection_status.json", True, "not applicable")],
        )
    if mode == "legacy_or_unknown" and not compat_v142:
        return print_results(
            "Step 9: Detailed Report",
            [CheckResult("Cannot decide path", "incident_detection_status.json", False, "missing")],
        )
    return _validate_exists_group("Step 9: Detailed Report", [outputs / "incident_rca_report_detailed.json"])


def step10_validate(outputs: Path, compat_v142: bool = False) -> bool:
    mode = _incident_mode(outputs)
    if mode == "no_incident":
        return print_results(
            "Step 10: Incident Assertions",
            [CheckResult("Skipped on no-incident path", "incident_detection_status.json", True, "not applicable")],
        )
    if mode == "legacy_or_unknown" and not compat_v142:
        return print_results(
            "Step 10: Incident Assertions",
            [CheckResult("Cannot decide path", "incident_detection_status.json", False, "missing")],
        )
    return _validate_exists_group("Step 10: Incident Assertions", [outputs / "incident_assertions.json"])


def step11_validate(outputs: Path, compat_v142: bool = False) -> bool:
    mode = _incident_mode(outputs)
    if mode == "no_incident":
        return _validate_exists_group(
            "Step 11: No-Incident Diagnostics",
            [outputs / "preincident_diagnostics.json", outputs / "preincident_diagnostics.md"],
        )
    if mode == "legacy_or_unknown":
        if compat_v142:
            return print_results(
                "Step 11: Incident Timeline",
                [CheckResult("Legacy timeline optional", "incident_timeline_summary.json", True, "optional in compat mode")],
            )
        return print_results(
            "Step 11: Incident Timeline",
            [CheckResult("Cannot decide path", "incident_detection_status.json", False, "missing")],
        )
    return _validate_exists_group("Step 11: Incident Timeline", [outputs / "incident_timeline_summary.json"])


def main() -> None:
    p = argparse.ArgumentParser(description="Validate SCRCA outputs step-by-step for QA.")
    p.add_argument("--outputs-dir", default="outputs")
    p.add_argument("--raw-log", default=None, help="Optional raw log file path for ingest volume sanity check")
    p.add_argument(
        "--compat-v142",
        action="store_true",
        help="Compatibility mode for v1.4.2-style outputs (status/timeline optional).",
    )
    p.add_argument("--report-json", default=None, help="Optional path to write validation JSON report.")
    p.add_argument("--report-md", default=None, help="Optional path to write validation Markdown report.")
    p.add_argument(
        "--require-step11",
        action="store_true",
        help="If set, include Step 11 in final PASS/FAIL gating.",
    )
    args = p.parse_args()

    outputs = Path(args.outputs_dir)
    raw_log = Path(args.raw_log) if args.raw_log else None

    step_status = {
        "step1_ingest": step1_validate(outputs, raw_log),
        "step2_embed": step2_validate(outputs),
        "step3_cluster": step3_validate(outputs),
        "step4_trigger_analysis": step4_validate(outputs),
        "step5_incident_detection": step5_validate(outputs, compat_v142=args.compat_v142),
        "step6_causal_analysis": step6_validate(outputs, compat_v142=args.compat_v142),
        "step7_rca_report": step7_validate(outputs, compat_v142=args.compat_v142),
        "step8_evidence_bundle": step8_validate(outputs, compat_v142=args.compat_v142),
        "step9_detailed_report": step9_validate(outputs, compat_v142=args.compat_v142),
        "step10_incident_assertions": step10_validate(outputs, compat_v142=args.compat_v142),
        "step11_timeline_or_diagnostics": step11_validate(outputs, compat_v142=args.compat_v142),
    }
    required_steps = [k for k in step_status.keys() if k != "step11_timeline_or_diagnostics"]
    if args.require_step11:
        required_steps.append("step11_timeline_or_diagnostics")
    overall = all(step_status[k] for k in required_steps)

    print("\n=== FINAL QA SUMMARY ===")
    print(f"Overall status: {pass_fail(overall)}")

    if args.report_json or args.report_md:
        detailed_stats = _build_detailed_stats(outputs, raw_log, args.compat_v142)
        report = {
            "outputs_dir": str(outputs),
            "raw_log": str(raw_log) if raw_log else None,
            "compat_v142": args.compat_v142,
            "require_step11": args.require_step11,
            "required_steps_for_overall": required_steps,
            "steps": step_status,
            "overall_status": "PASS" if overall else "FAIL",
            "stats": detailed_stats,
        }
        if args.report_json:
            out_json = Path(args.report_json)
            out_json.parent.mkdir(parents=True, exist_ok=True)
            out_json.write_text(json.dumps(report, ensure_ascii=False, indent=2), encoding="utf-8")
            print(f"[VALIDATION] JSON report -> {out_json}")
        if args.report_md:
            out_md = Path(args.report_md)
            out_md.parent.mkdir(parents=True, exist_ok=True)
            lines = [
                "# Validation Report",
                "",
                f"- Outputs directory: `{report['outputs_dir']}`",
                f"- Raw log: `{report['raw_log']}`" if report["raw_log"] else "- Raw log: `none`",
                f"- Compatibility mode (v1.4.2): `{args.compat_v142}`",
                f"- Require Step 11 for overall: `{args.require_step11}`",
                "",
                "## Step Results",
                "",
            ]
            for k, v in step_status.items():
                lines.append(f"- `{k}`: {'PASS' if v else 'FAIL'}")
            lines.extend(["", f"## Overall: `{report['overall_status']}`", ""])

            ingest = detailed_stats.get("ingest")
            if isinstance(ingest, dict):
                lines.extend(
                    [
                        "## Ingest Stats",
                        "",
                        f"- Events parsed: `{ingest.get('events_count')}`",
                        f"- Raw log lines: `{ingest.get('raw_log_lines')}`" if "raw_log_lines" in ingest else "- Raw log lines: `n/a`",
                        f"- Events/raw ratio: `{ingest.get('events_vs_raw_ratio_percent', 'n/a')}%`",
                        f"- Exact raw==events: `{ingest.get('events_equals_raw_lines', 'n/a')}`",
                        f"- Response codes: `200={ingest.get('response_code_counts', {}).get('200', 0)}` `403={ingest.get('response_code_counts', {}).get('403', 0)}` `500={ingest.get('response_code_counts', {}).get('500', 0)}`",
                        "",
                        "### Non-Null Coverage (%)",
                        "",
                    ]
                )
                cov = ingest.get("field_non_null_percent", {})
                if isinstance(cov, dict):
                    for f in ["service", "actor", "verb", "resource", "path", "stage", "response_code", "http_class", "status_family"]:
                        lines.append(f"- `{f}`: `{cov.get(f, 'n/a')}%`")
                lines.append("")

            emb = detailed_stats.get("embed")
            if isinstance(emb, dict):
                lines.extend(
                    [
                        "## Embedding Stats",
                        "",
                        f"- Index rows: `{emb.get('index_count', 'n/a')}`",
                        f"- Embedding rows: `{emb.get('embedding_rows', 'n/a')}`",
                        f"- Embedding dimensions: `{emb.get('embedding_dims', 'n/a')}`",
                        f"- Index/embedding row match: `{emb.get('index_embedding_row_match', 'n/a')}`",
                        "",
                    ]
                )

            trig = detailed_stats.get("trigger_analysis")
            if isinstance(trig, dict):
                lines.extend(
                    [
                        "## Trigger and Incident Reasoning",
                        "",
                        f"- Clusters analyzed: `{trig.get('cluster_count', 'n/a')}`",
                        f"- Candidate clusters: `{trig.get('candidate_count', 'n/a')}`",
                        f"- Candidate ratio: `{trig.get('candidate_ratio_percent', 'n/a')}%`",
                    ]
                )
                inc = detailed_stats.get("incident_detection")
                if isinstance(inc, dict):
                    lines.append(
                        f"- Incident decision: `status={inc.get('status')}`, `reason={inc.get('reason')}`, `incidents_count={inc.get('incidents_count')}`"
                    )
                top = trig.get("top_candidates")
                if isinstance(top, list) and top:
                    lines.extend(["", "### Top Candidate Clusters", ""])
                    for row in top:
                        if isinstance(row, dict):
                            lines.append(
                                f"- `{row.get('cluster_id')}`: trigger_score={row.get('trigger_score')}, error_count={row.get('error_count')}, fallback_error_count={row.get('fallback_error_count')}, event_count={row.get('event_count')}"
                            )
                roots = detailed_stats.get("root_candidates_preview")
                if isinstance(roots, list) and roots:
                    lines.extend(["", "### Root Candidate Preview", ""])
                    for r in roots[:5]:
                        lines.append(f"- `{r}`")
                lines.append("")

            out_md.write_text("\n".join(lines), encoding="utf-8")
            print(f"[VALIDATION] Markdown report -> {out_md}")


if __name__ == "__main__":
    main()

