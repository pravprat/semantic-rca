# cluster/causal/features/cluster_profile.py

from __future__ import annotations

from typing import Dict, Any, List
from collections import Counter

from cluster.causal.models.cluster_profile_model import ClusterProfile
from cluster.causal.utils.time_utils import parse_ts
from cluster.causal.domain.failure_domain_inferer import infer_failure_domain


def _fallback_resource(ev: Dict[str, Any]) -> str | None:
    resource = ev.get("resource")
    if resource:
        return resource

    sem = ev.get("semantic") or {}
    if sem.get("resource"):
        return sem.get("resource")

    structured = ev.get("structured_fields") or {}
    if structured.get("resource"):
        return structured.get("resource")

    path = ev.get("path")
    if isinstance(path, str) and path:
        parts = [p for p in path.split("/") if p]
        if parts:
            return parts[-1]

    return None


def _fallback_actor(ev: Dict[str, Any]) -> str | None:
    actor = ev.get("actor") or ev.get("service")
    if actor:
        return actor
    sem = ev.get("semantic") or {}
    return sem.get("actor")


def build_cluster_profiles(
    incident: Dict[str, Any],
    cluster_trigger_stats: Dict[str, Any],
    events: List[Dict[str, Any]],                    # ✅ ADD
    event_cluster_map: Dict[str, str],               # ✅ ADD
) -> Dict[str, ClusterProfile]:

    profiles: Dict[str, ClusterProfile] = {}

    for cid in incident.get("trigger_clusters", []):
        s = cluster_trigger_stats.get(cid)
        if not s:
            continue

        first_seen = parse_ts(s.get("first_seen"))
        last_seen = parse_ts(s.get("last_seen"))
        if not first_seen or not last_seen:
            continue

        # ------------------------------------------------------
        # Step 8A: collect cluster events
        # ------------------------------------------------------
        cluster_events = [
            e for e in events
            if event_cluster_map.get(e.get("event_id")) == cid
        ]

        # ------------------------------------------------------
        # Step 8A: infer failure domain
        # ------------------------------------------------------
        failure_domain = infer_failure_domain(cluster_events)

        # ------------------------------------------------------
        # Build profile
        # ------------------------------------------------------
        actor = s.get("actor")
        resource = s.get("resource")

        if not actor or not resource:
            actor_counts = Counter()
            resource_counts = Counter()
            for ev in cluster_events:
                a = _fallback_actor(ev)
                r = _fallback_resource(ev)
                if a:
                    actor_counts[a] += 1
                if r:
                    resource_counts[r] += 1

            if not actor and actor_counts:
                actor = actor_counts.most_common(1)[0][0]
            if not resource and resource_counts:
                resource = resource_counts.most_common(1)[0][0]

        if not actor:
            actor = "unknown_actor"
        if not resource:
            resource = "unknown_resource"

        profiles[cid] = ClusterProfile(
            cluster_id=cid,
            first_seen=first_seen,
            last_seen=last_seen,
            trigger_score=float(s.get("trigger_score", 0.0)),
            error_count=int(s.get("error_count", 0)),
            error_rate=float(s.get("error_rate", 0.0)),
            severity=float(s.get("severity", 0.0)),
            systemic_spread=float(s.get("systemic_spread", 0.0)),
            actor=actor,
            resource=resource,
            failure_domain=failure_domain,   # ✅ FIXED
        )

    return profiles