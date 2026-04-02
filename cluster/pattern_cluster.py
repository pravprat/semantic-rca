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


def _cluster_patterns_hdbscan_or_fallback(vectors: np.ndarray, min_cluster_size: int = 15) -> Dict[str, PatternCluster]:

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


def _cluster_patterns_fast_kmeans(
    vectors: np.ndarray,
    min_cluster_size: int = 15,
    max_clusters: int = 300,
) -> Dict[str, PatternCluster]:
    """
    Fast clustering mode for large datasets.
    Uses MiniBatchKMeans for bounded runtime, then filters small clusters.
    """
    try:
        from sklearn.cluster import MiniBatchKMeans
    except Exception as e:
        raise RuntimeError("Need scikit-learn for fast clustering mode.") from e

    n = len(vectors)
    # Keep cluster count bounded and tied to dataset size.
    # This prioritizes runtime stability over fine-grained cluster purity.
    n_clusters = max(8, min(max_clusters, n // max(min_cluster_size * 6, 60)))
    n_clusters = min(n_clusters, max(1, n))

    model = MiniBatchKMeans(
        n_clusters=n_clusters,
        random_state=42,
        batch_size=4096,
        n_init="auto",
        max_iter=120,
    )
    labels = model.fit_predict(vectors)

    clusters: Dict[int, List[int]] = {}
    for idx, lab in enumerate(labels):
        clusters.setdefault(int(lab), []).append(idx)

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
            representative_index=rep,
        )
    return out


def cluster_patterns(
    vectors: np.ndarray,
    min_cluster_size: int = 15,
    mode: str = "standard",
) -> Dict[str, PatternCluster]:

    if mode == "fast":
        return _cluster_patterns_fast_kmeans(vectors, min_cluster_size=min_cluster_size)
    return _cluster_patterns_hdbscan_or_fallback(vectors, min_cluster_size=min_cluster_size)


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