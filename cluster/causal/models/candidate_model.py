# cluster/causal/models/candidate_model.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Optional


@dataclass(frozen=True)
class Candidate:
    cluster_id: str
    candidate_score: float
    temporal_rank: int
    out_degree: int
    in_degree: int
    out_strength: float
    in_strength: float
    failure_domain: Optional[str]