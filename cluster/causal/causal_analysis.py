#causal_analysis.py
from __future__ import annotations

from typing import Any, Dict, List
from datetime import datetime

from cluster.causal.core.candidate_extractor import extract_candidates
from cluster.causal.core.graph_builder import infer_edges
from cluster.causal.features.cluster_profile import build_cluster_profiles

from cluster.causal.utils.io_utils import load_json, load_jsonl, write_json

from cluster.causal.validation.candidate_checks import validate_candidates
from cluster.causal.validation.event_checks import validate_grounded_events
from cluster.causal.validation.graph_checks import validate_graph
from semantic.component_registry import resolve_component


# ============================================================
# 🔷 Cluster-Level Causal Analyzer (Step 6)
# ============================================================

class ClusterCausalAnalyzer:

    def __init__(self, stats, events, event_cluster_map):
        self.stats = stats
        self.events = events
        self.event_cluster_map = event_cluster_map

    def analyze(self, incident: Dict[str, Any]):
        profiles = build_cluster_profiles(
            incident=incident,
            cluster_trigger_stats=self.stats,
            events=self.events,
            event_cluster_map=self.event_cluster_map
        )

        edges = infer_edges(profiles)

        candidates = extract_candidates(profiles, edges)

        validate_graph(profiles, edges)
        validate_candidates(candidates, profiles)

        return profiles, edges, candidates


# ============================================================
# 🔷 Event Resolver (Step 7)
# ============================================================

class EventResolver:

    def __init__(
        self,
        event_cluster_map: Dict[str, str],
        events: List[Dict[str, Any]],
    ):
        self.event_cluster_map = event_cluster_map
        self.events = events

    @staticmethod
    def _parse_ts(ts: Any) -> datetime | None:
        if not isinstance(ts, str) or not ts:
            return None
        try:
            return datetime.fromisoformat(ts.replace("Z", "+00:00"))
        except Exception:
            return None

    @staticmethod
    def _is_failure_event(ev: Dict[str, Any]) -> bool:
        rc = ev.get("response_code")
        try:
            if rc is not None and int(rc) >= 400:
                return True
        except Exception:
            pass
        status_family = str(ev.get("status_family") or "").lower()
        if status_family == "failure":
            return True
        sev = str(ev.get("severity") or "").upper()
        if sev in {"ERROR", "FATAL"}:
            return True
        if ev.get("failure_hint"):
            return True
        sem = ev.get("semantic") if isinstance(ev.get("semantic"), dict) else {}
        mode = str((sem or {}).get("failure_mode") or "").strip().lower()
        if mode and mode not in {"normal", "unknown"}:
            return True
        txt = str(
            ev.get("message")
            or ev.get("msg")
            or ev.get("raw_text")
            or ev.get("normalized_text")
            or ""
        ).lower()
        timeout_tokens = ("timeout", "timed out", "deadline exceeded", "connection refused", "connection reset", "exception", "failed")
        return any(tok in txt for tok in timeout_tokens)

    def resolve(
        self,
        incident: Dict[str, Any],
        candidate_clusters: List[str],
        top_k_per_cluster: int = 3,
    ):
        # 🔥 inline logic (no external dependency confusion)

        cluster_to_events: Dict[str, List[Dict[str, Any]]] = {}

        for e in self.events:
            eid = e.get("event_id")
            cid = self.event_cluster_map.get(eid)

            if cid in candidate_clusters and self._is_failure_event(e):
                cluster_to_events.setdefault(cid, []).append(e)

        results = []

        for cid in candidate_clusters:
            evs = cluster_to_events.get(cid, [])
            if not evs:
                continue

            # sort by parsed timestamp safely; non-parseable timestamps sink last
            evs_sorted = sorted(
                evs,
                key=lambda x: (
                    self._parse_ts(x.get("timestamp")) is None,
                    self._parse_ts(x.get("timestamp")) or datetime.max,
                ),
            )

            for i, ev in enumerate(evs_sorted[:top_k_per_cluster]):
                service = ev.get("service") or ev.get("actor")
                sem = ev.get("semantic") if isinstance(ev.get("semantic"), dict) else {}
                component = (sem or {}).get("component")
                comp_domain = (sem or {}).get("domain")
                if not component:
                    comp, dom = resolve_component(str(service or ""), str(ev.get("raw_text") or ""))
                    component = comp
                    comp_domain = comp_domain or dom
                results.append({
                    "cluster_id": cid,
                    "event_id": ev.get("event_id"),
                    "timestamp": ev.get("timestamp"),
                    "response_code": ev.get("response_code"),
                    "status_family": ev.get("status_family"),
                    "severity": ev.get("severity"),
                    "failure_hint": ev.get("failure_hint"),
                    "service": service,
                    "component": component,
                    "component_domain": comp_domain,
                    "actor": ev.get("actor"),
                    "resource": ev.get("resource"),
                    "reason": "earliest_failure" if i == 0 else "supporting_failure",
                })

        validate_grounded_events(results)

        return results


# ============================================================
# 🔷 MAIN ENTRY
# ============================================================

def run_causal_analysis(
    incidents_path: str,
    cluster_trigger_stats_path: str,
    graph_output_path: str,
    candidates_output_path: str,
    event_cluster_map_path: str | None = None,
    events_path: str | None = None,
    grounded_events_output_path: str | None = None,
) -> None:

    incidents: List[Dict[str, Any]] = load_json(incidents_path)
    stats: Dict[str, Any] = load_json(cluster_trigger_stats_path)

    enable_grounding = bool(
        event_cluster_map_path and events_path and grounded_events_output_path
    )

    if enable_grounding:
        event_cluster_map = load_json(event_cluster_map_path)
        events = load_jsonl(events_path)

        analyzer = ClusterCausalAnalyzer(
            stats=stats,
            events=events,
            event_cluster_map=event_cluster_map,
        )

        resolver = EventResolver(event_cluster_map, events)

    else:
        raise ValueError("Step 6 now requires events + event_cluster_map for domain inference")

    graph_output: List[Dict[str, Any]] = []
    candidate_output: List[Dict[str, Any]] = []
    grounded_output: List[Dict[str, Any]] = []

    for inc in incidents:

        # -------------------------
        # Step 6: Cluster reasoning
        # -------------------------

        profiles, edges, candidates = analyzer.analyze(inc)

        graph_output.append({
            "incident_id": inc["incident_id"],
            "causal_graph_version": "1.1",
            "incident_metadata": {
                "incident_version": inc.get("incident_version"),
                "episode_count": inc.get("episode_count"),
                "incident_class": inc.get("incident_class"),
                "declaration": inc.get("declaration"),
                "confidence": inc.get("confidence"),
            },
            "nodes": [
                {
                    "cluster_id": p.cluster_id,
                    "first_seen": p.first_seen.isoformat(),
                    "last_seen": p.last_seen.isoformat(),
                    "trigger_score": p.trigger_score,
                    "error_count": p.error_count,
                    "error_rate": p.error_rate,
                    "severity": p.severity,
                    "systemic_spread": p.systemic_spread,
                    "actor": p.actor,
                    "resource": p.resource,
                    "failure_domain": p.failure_domain,
                }
                for p in profiles.values()
            ],
            "edges": [
                {
                    "source": e.source,
                    "target": e.target,
                    "score": e.score,
                    "lag_seconds": e.lag_seconds,
                    "semantic_links": e.semantic_links,
                }
                for e in edges
            ],
        })

        candidate_output.append({
            "incident_id": inc["incident_id"],
            "root_candidate_version": "1.1",
            "incident_metadata": {
                "incident_class": inc.get("incident_class"),
                "declaration": inc.get("declaration"),
                "incident_confidence": inc.get("confidence"),
            },
            "candidates": [
                {
                    "cluster_id": c.cluster_id,
                    "candidate_score": c.candidate_score,
                    "temporal_rank": c.temporal_rank,
                    "out_degree": c.out_degree,
                    "in_degree": c.in_degree,
                    "out_strength": c.out_strength,
                    "in_strength": c.in_strength,
                    "failure_domain": c.failure_domain,
                }
                for c in candidates
            ],
        })

        # -------------------------
        # Step 7: Event resolution
        # -------------------------

        if enable_grounding:
            top_k = 3
            if str(inc.get("declaration") or "") == "possible_incident":
                top_k = 2
            top_clusters = [c.cluster_id for c in candidates[:top_k]]

            root_events = resolver.resolve(
                incident=inc,
                candidate_clusters=top_clusters,
            )

            grounded_output.append({
                "incident_id": inc["incident_id"],
                "incident_metadata": {
                    "incident_class": inc.get("incident_class"),
                    "declaration": inc.get("declaration"),
                },
                "root_events": root_events,
            })

    write_json(graph_output_path, graph_output)
    write_json(candidates_output_path, candidate_output)

    if enable_grounding:
        write_json(grounded_events_output_path, grounded_output)

    print(
        f"[causal_analysis] incidents={len(graph_output)} "
        f"-> {graph_output_path}, {candidates_output_path}"
        + (f", {grounded_events_output_path}" if enable_grounding else "")
    )