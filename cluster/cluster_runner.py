#cluster_runner.py
# cluster/cluster_runner.py

from __future__ import annotations

import json
import os
from typing import List, Dict, Any

import numpy as np
from sklearn.decomposition import PCA

from cluster.pattern_cluster import cluster_patterns


def tag_cluster_type(cluster_size: int, total_events: int) -> str:
    fraction = cluster_size / max(total_events, 1)
    if fraction >= 0.20:
        return "baseline"
    elif fraction <= 0.05:
        # Structural size class only; trigger stage decides candidates.
        return "minor_pattern"
    else:
        return "contextual"


def load_events(events_path: str) -> List[Dict[str, Any]]:
    events = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    return events


def run_clustering(
    events_path: str,
    embeddings_path: str,
    clusters_output_path: str,
    event_cluster_map_output_path: str,
    min_cluster_size: int,
    pca_dims: int | None,
) -> None:

    events = load_events(events_path)

    vectors = np.load(embeddings_path)

    print(f"[cluster] original vectors shape: {vectors.shape}")

    # ---- PCA ---------------------------------------------------------
    if pca_dims and pca_dims < vectors.shape[1]:
        print(f"[cluster] applying PCA: {vectors.shape[1]} → {pca_dims}")

        vectors = PCA(
            n_components=pca_dims,
            random_state=42,
            svd_solver="randomized",
        ).fit_transform(vectors)

        print(f"[cluster] PCA complete. New shape: {vectors.shape}")

        assert vectors.ndim == 2 and vectors.shape[1] == pca_dims

        vectors = vectors.astype(np.float32, copy=False)

        # Optional debug artifact
        np.save(
            os.path.join(os.path.dirname(embeddings_path), "event_embeddings_pca.npy"),
            vectors,
        )
    else:
        print("[cluster] PCA skipped")
        vectors = vectors.astype(np.float32, copy=False)

    # ---- Clustering --------------------------------------------------
    clusters = cluster_patterns(
        vectors,
        min_cluster_size=min_cluster_size,
    )

    # ---- Event → Cluster mapping -------------------------------------
    event_cluster_map: Dict[str, str] = {}

    for cid, c in clusters.items():
        for idx in c.member_indices:
            if 0 <= idx < len(events):
                event_id = events[idx].get("event_id")
                if event_id:
                    event_cluster_map[event_id] = cid

    # ---- Build cluster output ----------------------------------------
    total_events = len(events)
    clusters_out: Dict[str, Any] = {}

    event_times = [e.get("timestamp") for e in events]

    for cid, c in clusters.items():
        member_times = [
            event_times[idx]
            for idx in c.member_indices
            if idx < len(event_times) and event_times[idx] is not None
        ]

        clusters_out[cid] = {
            "cluster_id": cid,
            "member_indices": c.member_indices,
            "size": c.size,
            "representative_index": c.representative_index,
            "cluster_type": tag_cluster_type(c.size, total_events),
            "first_seen_ts": min(member_times) if member_times else None,
            "last_seen_ts": max(member_times) if member_times else None,
            "event_count": len(member_times),
        }

    # ---- Write outputs -----------------------------------------------
    with open(clusters_output_path, "w", encoding="utf-8") as f:
        json.dump(clusters_out, f, ensure_ascii=False, indent=2)

    with open(event_cluster_map_output_path, "w", encoding="utf-8") as f:
        json.dump(event_cluster_map, f, ensure_ascii=False, indent=2)

    mapped_events = len(event_cluster_map)
    unmapped_events = max(0, total_events - mapped_events)
    coverage = (mapped_events / max(total_events, 1)) * 100.0

    clustering_stats = {
        "total_events": total_events,
        "clustered_events": mapped_events,
        "unmapped_events": unmapped_events,
        "cluster_coverage_pct": round(coverage, 4),
        "cluster_count": len(clusters_out),
    }
    stats_output_path = clusters_output_path.replace(".json", "_stats.json")
    with open(stats_output_path, "w", encoding="utf-8") as f:
        json.dump(clustering_stats, f, ensure_ascii=False, indent=2)

    print(f"[cluster] clusters={len(clusters_out)} -> {clusters_output_path}")
    print(f"[cluster] event_cluster_map size={len(event_cluster_map)} -> {event_cluster_map_output_path}")
    print(
        f"[cluster] clustered={mapped_events}/{total_events} "
        f"({coverage:.2f}%), unmapped={unmapped_events} -> {stats_output_path}"
    )