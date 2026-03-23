# cluster/causal/core/scoring.py

from __future__ import annotations

from cluster.causal.models.cluster_profile_model import ClusterProfile


def edge_score(
    a: ClusterProfile,
    b: ClusterProfile,
    lag_seconds: float,
    semantic_links: int,
) -> float:
    score = 0.0

    # temporal closeness
    score += max(0.0, 1.0 - (lag_seconds / 120.0))

    # semantic strength
    score += 0.2 * semantic_links

    # upstream should not be dramatically weaker
    if a.trigger_score >= b.trigger_score:
        score += 0.1

    return round(min(1.0, score), 6)


# cluster/causal/core/scoring.py

def candidate_score(
    trigger_score: float,
    temporal_rank: int,
    out_strength: float,
    in_strength: float,
) -> float:
    """
    Balanced root-cause scoring:
    - Strongly favors early clusters (causal origin)
    - Uses trigger_score as signal strength
    - Uses graph structure but prevents domination by fan-out
    """

    # --- Temporal dominance (earlier = higher) ---
    temporal_score = 1.0 / (1.0 + temporal_rank)

    # --- Controlled graph influence ---
    total_edges = out_strength + in_strength
    if total_edges > 0:
        graph_score = (out_strength - in_strength) / (1.0 + total_edges)
    else:
        graph_score = 0.0

    # --- Final weighted score ---
    score = (
        0.5 * temporal_score +
        0.3 * trigger_score +
        0.2 * graph_score
    )

    return round(score, 6)