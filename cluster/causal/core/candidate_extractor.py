# cluster/causal/core/candidate_extractor.py

from __future__ import annotations

from collections import defaultdict
from typing import Dict, List

from cluster.causal.core.scoring import candidate_score
from cluster.causal.models.candidate_model import Candidate
from cluster.causal.models.cluster_profile_model import ClusterProfile
from cluster.causal.models.edge_model import Edge


def extract_candidates(
    profiles: Dict[str, ClusterProfile],
    edges: List[Edge],
) -> List[Candidate]:
    incoming = defaultdict(list)
    outgoing = defaultdict(list)

    for e in edges:
        outgoing[e.source].append(e)
        incoming[e.target].append(e)

    ordered = sorted(profiles.values(), key=lambda p: p.first_seen)

    out: List[Candidate] = []

    for rank, p in enumerate(ordered):
        out_strength = sum(e.score for e in outgoing[p.cluster_id])
        in_strength = sum(e.score for e in incoming[p.cluster_id])

        # ----------------------------------
        # Normalize strengths (critical fix)
        # ----------------------------------
        max_out = max((sum(e.score for e in outgoing[c.cluster_id]) for c in ordered), default=1.0)
        max_in = max((sum(e.score for e in incoming[c.cluster_id]) for c in ordered), default=1.0)

        out_strength_norm = out_strength / max_out if max_out > 0 else 0.0
        in_strength_norm = in_strength / max_in if max_in > 0 else 0.0

        score = candidate_score(
            trigger_score=p.trigger_score,
            temporal_rank=rank,
            out_strength=out_strength_norm,
            in_strength=in_strength_norm,
        )

        out.append(
            Candidate(
                cluster_id=p.cluster_id,
                candidate_score=score,
                temporal_rank=rank,
                out_degree=len(outgoing[p.cluster_id]),
                in_degree=len(incoming[p.cluster_id]),
                out_strength=round(out_strength, 6),
                in_strength=round(in_strength, 6),
                failure_domain=p.failure_domain,
            )
        )

    out.sort(key=lambda x: x.candidate_score, reverse=True)
    return out