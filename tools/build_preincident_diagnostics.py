#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List
from event_io import load_events


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    return load_events(path)


def build_preincident_diagnostics(outputs_dir: Path) -> Dict[str, Any]:
    events_path = outputs_dir / "events.parquet"
    if not events_path.exists():
        events_path = outputs_dir / "events.jsonl"
    events = _load_jsonl(events_path)
    trigger = _load_json(outputs_dir / "cluster_trigger_stats.json")
    cstats = _load_json(outputs_dir / "clusters_stats.json")

    n = len(events)
    sev_non_null = sum(1 for e in events if e.get("severity") not in (None, ""))
    rc_non_null = sum(1 for e in events if e.get("response_code") is not None)
    hc_non_null = sum(1 for e in events if e.get("http_class") not in (None, ""))
    verb_non_null = sum(1 for e in events if e.get("verb") not in (None, ""))
    resource_non_null = sum(1 for e in events if e.get("resource") not in (None, ""))

    err = sum(1 for e in events if str(e.get("severity") or "").upper() == "ERROR")
    warn = sum(1 for e in events if str(e.get("severity") or "").upper() == "WARN")
    fatal = sum(1 for e in events if str(e.get("severity") or "").upper() == "FATAL")
    http_bad = sum(1 for e in events if isinstance(e.get("response_code"), int) and int(e["response_code"]) >= 400)
    http_ok = sum(1 for e in events if isinstance(e.get("response_code"), int) and 200 <= int(e["response_code"]) < 300)

    vals = list(trigger.values())
    candidate_count = sum(1 for x in vals if x.get("is_candidate"))
    clusters_err = sum(1 for x in vals if (x.get("error_count") or 0) > 0)
    clusters_trigger = sum(1 for x in vals if (x.get("trigger_score") or 0) > 0)
    max_trigger = max((x.get("trigger_score") or 0.0) for x in vals) if vals else 0.0
    mean_trigger = sum((x.get("trigger_score") or 0.0) for x in vals) / max(1, len(vals))

    reasons: List[str] = []
    if candidate_count == 0:
        reasons.append("no_trigger_candidates")
    if http_bad == 0:
        reasons.append("no_http_4xx_5xx_signal")
    if err + warn + fatal > 0 and candidate_count == 0:
        reasons.append("severity_present_but_not_promoted")

    if candidate_count > 0:
        status = "detectable"
        action = "Run incident detection and full RCA stages."
    elif (err + warn + fatal) > 0:
        status = "weak_signal"
        action = "No trigger candidates; inspect trigger thresholds or severity fallback policy."
    else:
        status = "no_incident_signal"
        action = "No failure-like signal detected in current dataset."

    top = sorted(
        (
            {
                "cluster_id": cid,
                "trigger_score": s.get("trigger_score", 0.0),
                "error_count": s.get("error_count", 0),
                "error_rate": s.get("error_rate", 0.0),
                "event_count": s.get("event_count", 0),
                "actor": s.get("actor"),
                "resource": s.get("resource"),
                "is_candidate": s.get("is_candidate", False),
            }
            for cid, s in trigger.items()
        ),
        key=lambda x: x["trigger_score"],
        reverse=True,
    )[:10]

    return {
        "version": "1.0",
        "run_context": {
            "events_count": n,
            "clusters_count": int(cstats.get("cluster_count", 0)),
            "cluster_coverage_pct": float(cstats.get("cluster_coverage_pct", 0.0)),
        },
        "field_coverage": {
            "severity_non_null_pct": round((sev_non_null / max(1, n)) * 100.0, 4),
            "response_code_non_null_pct": round((rc_non_null / max(1, n)) * 100.0, 4),
            "http_class_non_null_pct": round((hc_non_null / max(1, n)) * 100.0, 4),
            "verb_non_null_pct": round((verb_non_null / max(1, n)) * 100.0, 4),
            "resource_non_null_pct": round((resource_non_null / max(1, n)) * 100.0, 4),
        },
        "failure_signal": {
            "events_error_count": err,
            "events_warn_count": warn,
            "events_fatal_count": fatal,
            "events_http_4xx_5xx_count": http_bad,
            "events_http_2xx_count": http_ok,
        },
        "trigger_stage": {
            "clusters_with_error_count_gt0": clusters_err,
            "clusters_with_trigger_score_gt0": clusters_trigger,
            "candidate_clusters_count": candidate_count,
            "max_trigger_score": round(max_trigger, 6),
            "mean_trigger_score": round(mean_trigger, 6),
        },
        "candidate_preview": top,
        "detectability_assessment": {
            "status": status,
            "reasons": reasons,
            "recommended_next_action": action,
        },
    }


def render_markdown(diag: Dict[str, Any]) -> str:
    out = []
    out.append("# Pre-Incident Diagnostics")
    out.append("")
    out.append(f"- Status: `{diag['detectability_assessment']['status']}`")
    out.append(f"- Events: `{diag['run_context']['events_count']}`")
    out.append(f"- Clusters: `{diag['run_context']['clusters_count']}`")
    out.append(f"- Coverage: `{diag['run_context']['cluster_coverage_pct']}`")
    out.append("")
    out.append("## Trigger Summary")
    ts = diag["trigger_stage"]
    out.append(f"- Candidate clusters: `{ts['candidate_clusters_count']}`")
    out.append(f"- Max trigger score: `{ts['max_trigger_score']}`")
    out.append(f"- Mean trigger score: `{ts['mean_trigger_score']}`")
    out.append("")
    out.append("## Reasons")
    for r in diag["detectability_assessment"]["reasons"]:
        out.append(f"- {r}")
    if not diag["detectability_assessment"]["reasons"]:
        out.append("- none")
    out.append("")
    out.append("## Recommended Action")
    out.append(diag["detectability_assessment"]["recommended_next_action"])
    return "\n".join(out) + "\n"


def main() -> None:
    p = argparse.ArgumentParser(description="Build pre-incident diagnostics from stage 1-4 artifacts.")
    p.add_argument("--outputs-dir", default="outputs")
    p.add_argument("--json", default="outputs/preincident_diagnostics.json")
    p.add_argument("--md", default="outputs/preincident_diagnostics.md")
    args = p.parse_args()

    diag = build_preincident_diagnostics(Path(args.outputs_dir))
    Path(args.json).write_text(json.dumps(diag, ensure_ascii=False, indent=2), encoding="utf-8")
    Path(args.md).write_text(render_markdown(diag), encoding="utf-8")
    print(f"[preincident] -> {args.json}")
    print(f"[preincident] -> {args.md}")


if __name__ == "__main__":
    main()

