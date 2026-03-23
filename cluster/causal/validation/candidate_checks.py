# cluster/causal/validation/candidate_checks.py

from __future__ import annotations

from typing import List

from cluster.causal.models.candidate_model import Candidate
from cluster.causal.models.cluster_profile_model import ClusterProfile


def validate_candidates(
    candidates: List[Candidate],
    profiles: dict[str, ClusterProfile],
) -> None:
    if not candidates:
        raise RuntimeError("[causal_analysis] no candidates produced")

    top = candidates[0]
    p = profiles[top.cluster_id]

    if p.error_count <= 0:
        raise RuntimeError("[causal_analysis] top candidate has no failure signal")