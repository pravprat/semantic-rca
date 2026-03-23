# cluster/causal/models/cluster_profile_model.py

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass(frozen=True)
class ClusterProfile:
    cluster_id: str
    first_seen: datetime
    last_seen: datetime
    trigger_score: float
    error_count: int
    error_rate: float
    severity: float
    systemic_spread: float
    actor: Optional[str]
    resource: Optional[str]
    failure_domain: Optional[str]