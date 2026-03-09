# semantic-rca/cluster/pattern_cluster.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Any
import numpy as np


@dataclass
class PatternCluster:
    cluster_id: str
    member_indices: List[int]
    size: int
    representative_index: int


def cluster_patterns(vectors: np.ndarray, min_cluster_size: int = 15) -> Dict[str, PatternCluster]:
    """
    MVP pattern clustering:
      - Prefer HDBSCAN if installed (best for unknown cluster counts)
      - Fallback: Agglomerative clustering with a cosine distance threshold
    Returns dict cluster_id -> PatternCluster
    """
    labels = None

    # Try HDBSCAN first
    try:
        import hdbscan  # type: ignore
        clusterer = hdbscan.HDBSCAN(min_cluster_size=min_cluster_size, metric="euclidean")
        labels = clusterer.fit_predict(vectors)
    except Exception:
        labels = None

    if labels is None:
        # Fallback: Agglomerative clustering
        try:
            from sklearn.cluster import AgglomerativeClustering
        except Exception as e:
            raise RuntimeError("Need hdbscan or scikit-learn for clustering.") from e

        # cosine distance ~ (1 - cosine similarity); vectors are normalized
        # Choose a moderate threshold; tune later
        model = AgglomerativeClustering(
            n_clusters=None,
            metric="cosine",
            linkage="average",
            distance_threshold=0.35
        )
        labels = model.fit_predict(vectors)

    clusters: Dict[int, List[int]] = {}
    for idx, lab in enumerate(labels):
        if lab == -1:
            continue
        clusters.setdefault(int(lab), []).append(idx)

    out: Dict[str, PatternCluster] = {}
    for lab, members in clusters.items():
        if len(members) < min_cluster_size:
            continue
        rep = _choose_representative(vectors, members)
        cid = f"C{lab}"
        out[cid] = PatternCluster(cluster_id=cid, member_indices=members, size=len(members), representative_index=rep)

    return out

def _choose_representative(vectors: np.ndarray, members: List[int]) -> int:
    """
    Choose the member whose vector is closest to the centroid (cosine).
    """
    M = vectors[members]
    centroid = M.mean(axis=0, keepdims=True)
    # normalize centroid
    centroid = centroid / (np.linalg.norm(centroid) + 1e-12)
    sims = (M @ centroid.T).reshape(-1)
    best = int(np.argmax(sims))
    return members[best]