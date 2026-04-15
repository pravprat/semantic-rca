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


def _status_from_threshold(
    observed: float,
    threshold: float,
    op: str = ">=",
) -> str:
    if op == ">=":
        return "pass" if observed >= threshold else "fail"
    if op == ">":
        return "pass" if observed > threshold else "fail"
    if op == "==":
        return "pass" if observed == threshold else "fail"
    return "inconclusive"


def _is_failure_event(ev: Dict[str, Any]) -> bool:
    rc = ev.get("response_code")
    try:
        if rc is not None and int(rc) >= 400:
            return True
    except Exception:
        pass
    if str(ev.get("status_family") or "").lower() == "failure":
        return True
    if ev.get("failure_hint"):
        return True
    sem = ev.get("semantic") if isinstance(ev.get("semantic"), dict) else {}
    mode = str((sem or {}).get("failure_mode") or "").lower()
    return bool(mode and mode not in {"normal", "unknown"})


def build_assertions(
    incidents_path: Path,
    candidates_path: Path,
    roots_path: Path,
    evidence_bundle_path: Path,
    output_path: Path,
) -> List[Dict[str, Any]]:
    incidents = _load_json(incidents_path)
    candidates = _load_json(candidates_path)
    roots = _load_json(roots_path)
    bundles = _load_json(evidence_bundle_path)

    cand_map = _incident_map(candidates)
    roots_map = _incident_map(roots)
    bundle_map = _incident_map(bundles)

    results: List[Dict[str, Any]] = []

    for inc in incidents:
        iid = inc.get("incident_id")
        if not iid:
            continue

        cand_item = cand_map.get(iid, {})
        root_item = roots_map.get(iid, {})
        bundle_item = bundle_map.get(iid, {})
        declaration = str(inc.get("declaration") or "incident")
        confidence_score = float(((inc.get("confidence") or {}).get("score") or 0.0))

        cand_list = cand_item.get("candidates", [])
        top = cand_list[0] if cand_list else {}
        second = cand_list[1] if len(cand_list) > 1 else {}
        root_events = root_item.get("root_events", [])

        assertions: List[Dict[str, Any]] = []

        # A1: Top candidate has positive net influence.
        out_strength = float(top.get("out_strength", 0.0) or 0.0)
        in_strength = float(top.get("in_strength", 0.0) or 0.0)
        net = out_strength - in_strength
        assertions.append(
            {
                "assertion_id": "A1_root_net_influence_positive",
                "status": _status_from_threshold(net, 0.0, op=">"),
                "rule": "top.out_strength - top.in_strength > 0",
                "observed": {"out_strength": out_strength, "in_strength": in_strength, "net": round(net, 6)},
                "threshold": {"net_gt": 0.0},
                "severity": "high",
                "impact_on_confidence": 0.12,
            }
        )

        # A6: Provenance should include observed log evidence for high-trust claims.
        prov = bundle_item.get("provenance", {}) if isinstance(bundle_item.get("provenance"), dict) else {}
        observed_events = int(prov.get("observed_log_evidence_events", 0) or 0)
        source = str(prov.get("root_cause_source") or "unknown")
        assertions.append(
            {
                "assertion_id": "A6_provenance_observed_logs_present",
                "status": "pass" if observed_events > 0 and source in {"logs_only", "logs_plus_external"} else "fail",
                "rule": "provenance has observed log evidence and logs-based source",
                "observed": {
                    "root_cause_source": source,
                    "observed_log_evidence_events": observed_events,
                },
                "threshold": {
                    "observed_log_evidence_events_gt": 0,
                    "allowed_sources": ["logs_only", "logs_plus_external"],
                },
                "severity": "high",
                "impact_on_confidence": 0.08,
            }
        )

        # A0: Step 5 policy declaration consistency.
        allowed_decl = {"incident", "possible_incident"}
        assertions.append(
            {
                "assertion_id": "A0_step5_declaration_allowed",
                "status": "pass" if declaration in allowed_decl else "fail",
                "rule": "incident.declaration in {'incident','possible_incident'}",
                "observed": {
                    "declaration": declaration,
                    "incident_class": inc.get("incident_class"),
                    "incident_confidence_score": confidence_score,
                },
                "threshold": {"allowed_declarations": sorted(allowed_decl)},
                "severity": "high",
                "impact_on_confidence": 0.08,
            }
        )

        # A2: Candidate score gap from second candidate is meaningful.
        top_score = float(top.get("candidate_score", 0.0) or 0.0)
        second_score = float(second.get("candidate_score", 0.0) or 0.0)
        gap = top_score - second_score
        assertions.append(
            {
                "assertion_id": "A2_root_score_gap_significant",
                "status": _status_from_threshold(gap, 0.10, op=">="),
                "rule": "top.candidate_score - second.candidate_score >= 0.10",
                "observed": {
                    "top_candidate_score": round(top_score, 6),
                    "second_candidate_score": round(second_score, 6),
                    "gap": round(gap, 6),
                },
                "threshold": {"gap_gte": 0.10},
                "severity": "medium",
                "impact_on_confidence": 0.08,
            }
        )

        # A5: Low declaration should not claim high confidence.
        if declaration == "possible_incident":
            status = "pass" if confidence_score < 0.70 else "fail"
            rule = "possible_incident => incident.confidence.score < 0.70"
            threshold = {"max_confidence_for_possible_incident": 0.70}
        else:
            status = "inconclusive"
            rule = "applies only to possible_incident declarations"
            threshold = {}
        assertions.append(
            {
                "assertion_id": "A5_possible_incident_confidence_bound",
                "status": status,
                "rule": rule,
                "observed": {
                    "declaration": declaration,
                    "incident_confidence_score": round(confidence_score, 6),
                },
                "threshold": threshold,
                "severity": "medium",
                "impact_on_confidence": 0.04,
            }
        )

        # A3: Root events should be failure-class events.
        root_total = len(root_events)
        failure_like = 0
        for ev in root_events:
            if _is_failure_event(ev):
                failure_like += 1
        ratio = (failure_like / root_total) if root_total else 0.0
        assertions.append(
            {
                "assertion_id": "A3_root_events_failure_class",
                "status": _status_from_threshold(ratio, 1.0, op="=="),
                "rule": "all root_events are failure-class by multi-signal policy",
                "observed": {
                    "root_events_total": root_total,
                    "failure_class_events": failure_like,
                    "failure_ratio": round(ratio, 6),
                },
                "threshold": {"failure_ratio_eq": 1.0},
                "severity": "high",
                "impact_on_confidence": 0.1,
            }
        )

        # A4: Evidence bundle claim coverage should be complete.
        cov = bundle_item.get("coverage", {})
        coverage_pct = float(cov.get("coverage_pct", 0.0) or 0.0)
        assertions.append(
            {
                "assertion_id": "A4_evidence_claim_coverage_complete",
                "status": _status_from_threshold(coverage_pct, 100.0, op="=="),
                "rule": "evidence.coverage_pct == 100",
                "observed": {
                    "claims_total": cov.get("claims_total", 0),
                    "claims_with_evidence": cov.get("claims_with_evidence", 0),
                    "coverage_pct": coverage_pct,
                },
                "threshold": {"coverage_pct_eq": 100.0},
                "severity": "medium",
                "impact_on_confidence": 0.06,
            }
        )

        summary = {"pass": 0, "fail": 0, "inconclusive": 0}
        for a in assertions:
            summary[a["status"]] = summary.get(a["status"], 0) + 1

        results.append(
            {
                "incident_id": iid,
                "assertion_version": "1.1",
                "incident_metadata": {
                    "incident_class": inc.get("incident_class"),
                    "declaration": declaration,
                    "episode_count": inc.get("episode_count"),
                    "confidence": inc.get("confidence"),
                },
                "assertions": assertions,
                "summary": summary,
            }
        )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return results


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Build incident assertions JSON from RCA outputs.")
    p.add_argument("--incidents", default="outputs/incidents.json")
    p.add_argument("--candidates", default="outputs/incident_root_candidates.json")
    p.add_argument("--roots", default="outputs/incident_root_events.json")
    p.add_argument("--evidence-bundle", default="outputs/incident_evidence_bundle.json")
    p.add_argument("--output", default="outputs/incident_assertions.json")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    out = build_assertions(
        incidents_path=Path(args.incidents),
        candidates_path=Path(args.candidates),
        roots_path=Path(args.roots),
        evidence_bundle_path=Path(args.evidence_bundle),
        output_path=Path(args.output),
    )
    print(f"[incident_assertions] incidents={len(out)} -> {args.output}")


if __name__ == "__main__":
    main()

