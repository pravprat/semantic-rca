# cluster/causal/validation/graph_checks.py

from __future__ import annotations

from typing import Dict, List

from cluster.causal.models.cluster_profile_model import ClusterProfile
from cluster.causal.models.edge_model import Edge


def validate_graph(
    profiles: Dict[str, ClusterProfile],
    edges: List[Edge],
) -> None:
    ids = set(profiles.keys())

    for e in edges:
        if e.source == e.target:
            raise RuntimeError("[causal_analysis] self-loop detected")
        if e.source not in ids or e.target not in ids:
            raise RuntimeError("[causal_analysis] edge references unknown node")
        if not (0.0 <= e.score <= 1.0):
            raise RuntimeError("[causal_analysis] edge score out of range")
        if profiles[e.source].first_seen > profiles[e.target].first_seen:
            raise RuntimeError("[causal_analysis] backward edge detected")