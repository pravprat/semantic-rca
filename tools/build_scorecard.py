#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    items: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            items.append(json.loads(line))
    return items


def _incident_ids(items: List[Dict[str, Any]]) -> set[str]:
    return {x.get("incident_id") for x in items if x.get("incident_id")}


def compute_scorecard(outputs_dir: Path) -> Dict[str, Any]:
    events = _load_jsonl(outputs_dir / "events.jsonl")
    idx = _load_json(outputs_dir / "event_index.json")
    ecm = _load_json(outputs_dir / "event_cluster_map.json")
    clusters = _load_json(outputs_dir / "clusters.json")
    cstats = _load_json(outputs_dir / "clusters_stats.json")
    trig = _load_json(outputs_dir / "cluster_trigger_stats.json")
    incidents = _load_json(outputs_dir / "incidents.json")
    graph = _load_json(outputs_dir / "incident_causal_graph.json")
    candidates = _load_json(outputs_dir / "incident_root_candidates.json")
    roots = _load_json(outputs_dir / "incident_root_events.json")
    report = _load_json(outputs_dir / "incident_rca_report.json")
    bundle = _load_json(outputs_dir / "incident_evidence_bundle.json")
    detailed = _load_json(outputs_dir / "incident_rca_report_detailed.json")

    idx_set = {e.get("event_id") for e in idx}
    map_ids = set(ecm.keys())
    cluster_ids = set(clusters.keys())

    # Metrics
    missing_semantic = sum(1 for e in events if not e.get("semantic"))
    missing_signature = sum(1 for e in events if not e.get("signature"))
    invalid_map_targets = sum(1 for _, cid in ecm.items() if cid not in cluster_ids)
    index_only_events = len(idx_set - map_ids)
    root_non_failure = 0
    for r in roots:
        for ev in r.get("root_events", []):
            try:
                rc = ev.get("response_code")
                if rc is not None and int(rc) < 400:
                    root_non_failure += 1
            except Exception:
                pass

    node_null_actor = 0
    node_null_resource = 0
    node_count = 0
    for g in graph:
        for n in g.get("nodes", []):
            node_count += 1
            if n.get("actor") in (None, ""):
                node_null_actor += 1
            if n.get("resource") in (None, ""):
                node_null_resource += 1

    inc_ids = _incident_ids(incidents)
    align_missing = (
        len(inc_ids - _incident_ids(graph))
        + len(inc_ids - _incident_ids(candidates))
        + len(inc_ids - _incident_ids(roots))
        + len(inc_ids - _incident_ids(report))
        + len(inc_ids - _incident_ids(bundle))
        + len(inc_ids - _incident_ids(detailed))
    )

    onset_missing = 0
    for b in bundle:
        ao = b.get("anomaly_onset", {})
        if not ao.get("first_anomaly_timestamp") or not ao.get("first_anomaly_event_id"):
            onset_missing += 1

    coverage_pct = float(cstats.get("cluster_coverage_pct", 0.0))

    # Scores (same rubric used in manual scoring)
    ingest = 14 if missing_semantic == 0 and missing_signature == 0 else 12
    semantic = 13 if missing_semantic == 0 and missing_signature == 0 else 10
    embedding_index = 9 if len(idx) == len(idx_set) else 7

    if coverage_pct >= 96:
        clustering = 15
    elif coverage_pct >= 94:
        clustering = 14
    elif coverage_pct >= 92:
        clustering = 13
    else:
        clustering = 11

    trigger_incident = 13 if align_missing == 0 else 10
    causal = 18 if root_non_failure == 0 and node_null_actor == 0 and node_null_resource == 0 else 14
    reporting = 10 if onset_missing == 0 else 8

    total = ingest + semantic + embedding_index + clustering + trigger_incident + causal + reporting

    return {
        "scorecard_version": "1.0",
        "scores": {
            "ingest_correctness": {"score": ingest, "out_of": 15},
            "semantic_enrichment": {"score": semantic, "out_of": 15},
            "embedding_index_alignment": {"score": embedding_index, "out_of": 10},
            "clustering_sanity_coverage": {"score": clustering, "out_of": 15},
            "trigger_incident_detection": {"score": trigger_incident, "out_of": 15},
            "causal_analysis_quality": {"score": causal, "out_of": 20},
            "reporting_fidelity": {"score": reporting, "out_of": 10},
        },
        "overall": {"score": total, "out_of": 100},
        "key_metrics": {
            "events_count": len(events),
            "index_count": len(idx),
            "mapped_events": len(ecm),
            "unmapped_events": index_only_events,
            "cluster_coverage_pct": coverage_pct,
            "clusters_count": len(clusters),
            "trigger_candidates": sum(1 for s in trig.values() if s.get("is_candidate")),
            "incidents_count": len(incidents),
            "graph_nodes": node_count,
            "invalid_map_targets": invalid_map_targets,
            "root_events_non_failure": root_non_failure,
            "graph_null_actor": node_null_actor,
            "graph_null_resource": node_null_resource,
            "incident_alignment_missing_total": align_missing,
            "bundle_onset_missing": onset_missing,
        },
    }


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Generate RCA run scorecard JSON.")
    p.add_argument("--outputs-dir", default="outputs")
    p.add_argument("--output", default="outputs/scorecard.json")
    return p.parse_args()


def main() -> None:
    args = parse_args()
    scorecard = compute_scorecard(Path(args.outputs_dir))
    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with out_path.open("w", encoding="utf-8") as f:
        json.dump(scorecard, f, ensure_ascii=False, indent=2)
    print(f"[scorecard] -> {out_path} (overall={scorecard['overall']['score']}/100)")


if __name__ == "__main__":
    main()

