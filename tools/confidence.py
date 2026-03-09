from __future__ import annotations

from typing import Dict


def compute_rca_confidence(
    *,
    trigger_score: float,
    error_count: int,
    out_weight: float,
    in_weight: float,
    trigger_proximity: float,
    temporal_consistent: bool,
    churn_penalty: float,
) -> Dict:
    """
    Deterministic confidence score in [0, 1].
    """

    score = 0.0
    reasons = []

    if trigger_score >= 3.0:
        score += 0.22
        reasons.append("strong trigger anomaly")
    elif trigger_score >= 1.5:
        score += 0.12
        reasons.append("moderate trigger anomaly")

    if error_count >= 100:
        score += 0.22
        reasons.append("large error burst")
    elif error_count >= 20:
        score += 0.12
        reasons.append("meaningful error burst")

    influence = max(out_weight - in_weight, 0.0)
    if influence >= 5:
        score += 0.20
        reasons.append("strong downstream influence")
    elif influence >= 1:
        score += 0.10
        reasons.append("some downstream influence")

    if trigger_proximity >= 0.85:
        score += 0.18
        reasons.append("appears near incident onset")
    elif trigger_proximity >= 0.50:
        score += 0.08
        reasons.append("appears reasonably early")

    if temporal_consistent:
        score += 0.12
        reasons.append("temporal ordering is consistent with causality")
    else:
        score -= 0.10
        reasons.append("temporal ordering weakens causality confidence")

    if churn_penalty >= 6:
        score -= 0.16
        reasons.append("cluster resembles expected operational churn")
    elif churn_penalty > 0:
        score -= 0.08
        reasons.append("cluster contains partial expected churn")

    score = max(0.0, min(1.0, score))

    if score >= 0.75:
        label = "high"
    elif score >= 0.45:
        label = "medium"
    else:
        label = "low"

    return {
        "value": round(score, 3),
        "label": label,
        "reasons": reasons,
    }