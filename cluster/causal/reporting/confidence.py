# 7C. cluster/causal/reporting/confidence.py

from __future__ import annotations

from typing import Dict, Any, List


def compute_confidence(
    top_candidate: Dict[str, Any],
    all_candidates: List[Dict[str, Any]],
    root_events: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Deterministic confidence score from:
    - candidate score strength
    - gap vs next candidate
    - temporal dominance
    - graph support
    - event support count
    """

    top_score = float(top_candidate.get("candidate_score", 0.0))
    temporal_rank = int(top_candidate.get("temporal_rank", 999))
    out_degree = int(top_candidate.get("out_degree", 0))
    in_degree = int(top_candidate.get("in_degree", 0))

    second_score = 0.0
    if len(all_candidates) > 1:
        second_score = float(all_candidates[1].get("candidate_score", 0.0))

    score_gap = max(0.0, top_score - second_score)

    # ---- Normalize component scores ---------------------------------
    candidate_strength = min(1.0, top_score)
    gap_strength = min(1.0, score_gap / 0.25)  # 0.25 gap = strong
    temporal_strength = 1.0 / (1.0 + temporal_rank)

    if out_degree + in_degree > 0:
        graph_strength = out_degree / (1.0 + out_degree + in_degree)
    else:
        graph_strength = 0.0

    event_strength = min(1.0, len(root_events) / 5.0)

    confidence = (
        0.35 * candidate_strength +
        0.20 * gap_strength +
        0.20 * temporal_strength +
        0.15 * graph_strength +
        0.10 * event_strength
    )

    confidence = round(confidence, 6)

    if confidence >= 0.80:
        label = "high"
    elif confidence >= 0.55:
        label = "medium"
    else:
        label = "low"

    return {
        "score": confidence,
        "label": label,
        "signals": {
            "candidate_strength": round(candidate_strength, 6),
            "gap_strength": round(gap_strength, 6),
            "temporal_strength": round(temporal_strength, 6),
            "graph_strength": round(graph_strength, 6),
            "event_strength": round(event_strength, 6),
        },
    }