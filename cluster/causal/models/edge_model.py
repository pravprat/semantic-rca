# cluster/causal/models/edge_model.py

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Edge:
    source: str
    target: str
    score: float
    lag_seconds: int
    semantic_links: int