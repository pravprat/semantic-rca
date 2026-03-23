# cluster/causal/core/graph_builder.py

from __future__ import annotations

from typing import Dict, List

from cluster.causal.core.scoring import edge_score
from cluster.causal.models.cluster_profile_model import ClusterProfile
from cluster.causal.models.edge_model import Edge
from cluster.causal.utils.time_utils import seconds_between


def infer_edges(
    profiles: Dict[str, ClusterProfile],
    max_gap_seconds: int = 30,
) -> List[Edge]:
    """
    Build causal edges using temporal precedence first.

    Rules:
    - forward in time only
    - allow zero-delta edges for near-simultaneous clusters
    - add fallback sequential edges if graph would otherwise be empty
    """

    sorted_profiles = sorted(
        profiles.values(),
        key=lambda p: p.first_seen.timestamp(),
    )

    edges: List[Edge] = []

    # ----------------------------------
    # Primary temporal edge construction
    # ----------------------------------
    for i in range(len(sorted_profiles)):
        a = sorted_profiles[i]

        for j in range(i + 1, len(sorted_profiles)):
            b = sorted_profiles[j]

            delta = (b.first_seen - a.first_seen).total_seconds()

            # FIX 1: allow zero-delta edges

            if 0 <= delta <= max_gap_seconds:
                # prevent edge explosion for far-but-valid pairs
                if delta > 5 and j > i + 5:
                    continue

                semantic_links = 0

                if a.actor and b.actor and a.actor == b.actor:
                    semantic_links += 1
                if a.resource and b.resource and a.resource == b.resource:
                    semantic_links += 1
                if (
                        a.failure_domain
                        and b.failure_domain
                        and a.failure_domain == b.failure_domain
                ):
                    semantic_links += 1

                from cluster.causal.config.scoring_config import EDGE_SCORING

                base_score = EDGE_SCORING["base_temporal_weight"]
                semantic_bonus = EDGE_SCORING["semantic_weight_per_match"] * semantic_links

                score = min(base_score + semantic_bonus, EDGE_SCORING["max_score"])

                edges.append(
                    Edge(
                        source=a.cluster_id,
                        target=b.cluster_id,
                        score=round(score, 6),
                        lag_seconds=int(delta),
                        semantic_links=semantic_links,
                    )
                )

            if delta > max_gap_seconds:
                break

    # ----------------------------------
    # FIX 2: fallback graph must not be empty
    # ----------------------------------
    if not edges and len(sorted_profiles) > 1:
        for i in range(len(sorted_profiles) - 1):
            a = sorted_profiles[i]
            b = sorted_profiles[i + 1]

            delta = (b.first_seen - a.first_seen).total_seconds()
            if delta < 0:
                continue

            edges.append(
                Edge(
                    source=a.cluster_id,
                    target=b.cluster_id,
                    score=0.6,
                    lag_seconds=int(delta),
                    semantic_links=0,
                )
            )

    print(f"[infer_edges] nodes={len(sorted_profiles)} edges={len(edges)}")
    return edges