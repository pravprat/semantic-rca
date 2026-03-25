#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from pathlib import Path
from typing import Any, Dict, List, Tuple


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _incident_map(items: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    return {x.get("incident_id"): x for x in items if x.get("incident_id")}


def _cluster_node_map(graph_item: Dict[str, Any]) -> Dict[str, Dict[str, Any]]:
    return {n.get("cluster_id"): n for n in graph_item.get("nodes", []) if n.get("cluster_id")}


def _cluster_edge_index(graph_item: Dict[str, Any]) -> Dict[Tuple[str, str], Dict[str, Any]]:
    idx: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for e in graph_item.get("edges", []):
        src = e.get("source")
        dst = e.get("target")
        if src and dst:
            idx[(src, dst)] = e
    return idx


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def _pick_chain_edges(top_cluster: str, graph_item: Dict[str, Any], max_edges: int = 5) -> List[Dict[str, Any]]:
    edges = [e for e in graph_item.get("edges", []) if e.get("source") == top_cluster]
    edges.sort(key=lambda x: (_safe_float(x.get("lag_seconds"), 1e9), -_safe_float(x.get("score"), 0.0)))
    return edges[:max_edges]


def build_evidence_bundle(
    incidents_path: Path,
    candidates_path: Path,
    grounded_events_path: Path,
    graph_path: Path,
    report_path: Path,
    output_path: Path,
) -> List[Dict[str, Any]]:
    incidents = _load_json(incidents_path)
    candidates = _load_json(candidates_path)
    grounded = _load_json(grounded_events_path)
    graphs = _load_json(graph_path)
    reports = _load_json(report_path)

    cand_map = _incident_map(candidates)
    grounded_map = _incident_map(grounded)
    graph_map = _incident_map(graphs)
    report_map = _incident_map(reports)

    bundles: List[Dict[str, Any]] = []

    for inc in incidents:
        iid = inc.get("incident_id")
        if not iid:
            continue

        cand_item = cand_map.get(iid, {})
        grounded_item = grounded_map.get(iid, {})
        graph_item = graph_map.get(iid, {})
        report_item = report_map.get(iid, {})

        candidate_list = cand_item.get("candidates", [])
        root_events = grounded_item.get("root_events", [])
        top = candidate_list[0] if candidate_list else {}
        top_cluster = top.get("cluster_id")

        node_map = _cluster_node_map(graph_item)
        edge_idx = _cluster_edge_index(graph_item)

        primary_event = None
        for ev in root_events:
            if ev.get("reason") == "earliest_failure":
                primary_event = ev
                break
        if primary_event is None and root_events:
            primary_event = root_events[0]

        chain = _pick_chain_edges(top_cluster, graph_item) if top_cluster else []
        chain_refs = []
        for e in chain:
            chain_refs.append(
                {
                    "source_cluster_id": e.get("source"),
                    "target_cluster_id": e.get("target"),
                    "edge_score": e.get("score"),
                    "lag_seconds": e.get("lag_seconds"),
                    "semantic_links": e.get("semantic_links"),
                }
            )

        claim = {
            "claim_id": f"{iid}-CLM-ROOT-001",
            "type": "root_cause",
            "statement": f"Cluster {top_cluster} is the primary root cause candidate." if top_cluster else "No root cluster candidate available.",
            "confidence": (report_item.get("confidence") or {}).get("score"),
            "supports": {
                "candidate": {
                    "cluster_id": top.get("cluster_id"),
                    "candidate_score": top.get("candidate_score"),
                    "temporal_rank": top.get("temporal_rank"),
                    "out_strength": top.get("out_strength"),
                    "in_strength": top.get("in_strength"),
                    "failure_domain": top.get("failure_domain"),
                },
                "events": [
                    {
                        "event_id": ev.get("event_id"),
                        "cluster_id": ev.get("cluster_id"),
                        "timestamp": ev.get("timestamp"),
                        "response_code": ev.get("response_code"),
                        "actor": ev.get("actor"),
                        "resource": ev.get("resource"),
                        "reason": ev.get("reason"),
                    }
                    for ev in root_events[:10]
                ],
                "graph_edges": chain_refs,
            },
            "operator_view": {
                "operator_label": (report_item.get("root_cause_summary") or {}).get("type") or "Root Cause",
                "operator_summary": report_item.get("explanation"),
                "impact_window": report_item.get("incident_window") or {
                    "start_time": inc.get("start_time"),
                    "end_time": inc.get("end_time"),
                    "duration_seconds": inc.get("duration_seconds"),
                },
            },
            "technical_refs": {
                "incident_id": iid,
                "primary_cluster_id": top_cluster,
                "primary_event_id": (primary_event or {}).get("event_id"),
                "node_snapshot": node_map.get(top_cluster) if top_cluster else None,
            },
        }

        coverage = {
            "claims_total": 1,
            "claims_with_evidence": 1 if top_cluster and root_events else 0,
        }
        coverage["coverage_pct"] = round(
            100.0 * coverage["claims_with_evidence"] / max(1, coverage["claims_total"]), 2
        )

        bundle = {
            "incident_id": iid,
            "bundle_version": "1.0",
            "lineage": {
                "incidents_path": str(incidents_path),
                "candidates_path": str(candidates_path),
                "grounded_events_path": str(grounded_events_path),
                "graph_path": str(graph_path),
                "report_path": str(report_path),
            },
            "claims": [claim],
            "coverage": coverage,
            "chain_summary": {
                "top_cluster": top_cluster,
                "direct_downstream_edges": len(chain),
                "all_graph_edges": len(graph_item.get("edges", [])),
            },
        }
        bundles.append(bundle)

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with output_path.open("w", encoding="utf-8") as f:
        json.dump(bundles, f, ensure_ascii=False, indent=2)

    return bundles


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Build incident evidence bundle JSON from existing RCA artifacts."
    )
    parser.add_argument("--incidents", default="outputs/incidents.json")
    parser.add_argument("--candidates", default="outputs/incident_root_candidates.json")
    parser.add_argument("--grounded-events", default="outputs/incident_root_events.json")
    parser.add_argument("--graph", default="outputs/incident_causal_graph.json")
    parser.add_argument("--report", default="outputs/incident_rca_report.json")
    parser.add_argument("--output", default="outputs/incident_evidence_bundle.json")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    out = build_evidence_bundle(
        incidents_path=Path(args.incidents),
        candidates_path=Path(args.candidates),
        grounded_events_path=Path(args.grounded_events),
        graph_path=Path(args.graph),
        report_path=Path(args.report),
        output_path=Path(args.output),
    )
    print(f"[evidence_bundle] incidents={len(out)} -> {args.output}")


if __name__ == "__main__":
    main()

