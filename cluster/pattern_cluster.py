# semantic-rca/cluster/pattern_cluster.py

from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List
import numpy as np


@dataclass
class PatternCluster:
    cluster_id: str
    member_indices: List[int]
    size: int
    representative_index: int


def cluster_patterns(vectors: np.ndarray, min_cluster_size: int = 15) -> Dict[str, PatternCluster]:

    # ------------------------------------------------
    # Memory safety
    # ------------------------------------------------

    vectors = vectors.astype(np.float32, copy=False)

    MAX_CLUSTER_EVENTS = 120000

    if len(vectors) > MAX_CLUSTER_EVENTS:
        raise RuntimeError(
            f"Too many events ({len(vectors)}) for local clustering. "
            f"Limit is {MAX_CLUSTER_EVENTS}."
        )

    labels = None

    # ------------------------------------------------
    # Prefer HDBSCAN
    # ------------------------------------------------

    try:
        import hdbscan

        clusterer = hdbscan.HDBSCAN(
            min_cluster_size=min_cluster_size,
            metric="euclidean",
            core_dist_n_jobs=4,
            approx_min_span_tree=True
        )

        labels = clusterer.fit_predict(vectors)

    except Exception:
        labels = None

    # ------------------------------------------------
    # Fallback clustering (small datasets only)
    # ------------------------------------------------

    if labels is None:

        if len(vectors) > 40000:
            raise RuntimeError(
                "Agglomerative fallback disabled for datasets >40k. "
                "Install hdbscan to cluster large logs."
            )

        try:
            from sklearn.cluster import AgglomerativeClustering
        except Exception as e:
            raise RuntimeError(
                "Need hdbscan or scikit-learn for clustering."
            ) from e

        model = AgglomerativeClustering(
            n_clusters=None,
            metric="cosine",
            linkage="average",
            distance_threshold=0.35
        )

        labels = model.fit_predict(vectors)

    # ------------------------------------------------
    # Build cluster membership
    # ------------------------------------------------

    clusters: Dict[int, List[int]] = {}

    for idx, lab in enumerate(labels):

        if lab == -1:
            continue

        clusters.setdefault(int(lab), []).append(idx)

    # ------------------------------------------------
    # Convert to PatternCluster objects
    # ------------------------------------------------

    out: Dict[str, PatternCluster] = {}

    for lab, members in clusters.items():

        if len(members) < min_cluster_size:
            continue

        rep = _choose_representative(vectors, members)

        cid = f"C{lab}"

        out[cid] = PatternCluster(
            cluster_id=cid,
            member_indices=members,
            size=len(members),
            representative_index=rep
        )

    return out


def _choose_representative(vectors: np.ndarray, members: List[int]) -> int:
    """
    Choose cluster member closest to centroid.
    """

    M = vectors[members]

    centroid = np.mean(M, axis=0)
    centroid = centroid / (np.linalg.norm(centroid) + 1e-12)

    sims = M @ centroid

    best = int(np.argmax(sims))

    return members[best]