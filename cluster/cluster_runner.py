#cluster_runner.py
# cluster/cluster_runner.py

from __future__ import annotations

import json
import os
from typing import List, Dict, Any

import numpy as np
from sklearn.decomposition import PCA

from cluster.pattern_cluster import cluster_patterns

MAX_LOCAL_CLUSTER_EVENTS = 120000


def _l2_normalize(x: np.ndarray) -> np.ndarray:
    n = np.linalg.norm(x, axis=1, keepdims=True) + 1e-12
    return x / n


def _to_float32(x: np.ndarray) -> np.ndarray:
    if x.dtype == np.float32:
        return x
    return x.astype(np.float32, copy=False)


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
    max_cluster_events: int = MAX_LOCAL_CLUSTER_EVENTS,
    cluster_overflow_mode: str = "downsample",
    cluster_mode: str = "standard",
) -> None:

    events = load_events(events_path)

    vectors = np.load(embeddings_path, mmap_mode="r")

    print(f"[cluster] original vectors shape: {vectors.shape}")

    # ---- Clustering --------------------------------------------------
    sampled_indices = None
    effective_cluster_mode = cluster_mode
    clustering_input = vectors
    pca_model = None
    if len(vectors) > max_cluster_events:
        if cluster_overflow_mode == "fail":
            raise RuntimeError(
                f"Too many events ({len(vectors)}) for local clustering. "
                f"Limit is {max_cluster_events}. "
                f"Use --cluster-overflow-mode downsample to continue safely."
            )
        # Graceful fallback: keep pipeline running with bounded local workload.
        sampled_indices = np.linspace(
            0,
            len(vectors) - 1,
            num=max_cluster_events,
            dtype=np.int64,
        )
        clustering_input = vectors[sampled_indices]
        print(
            f"[cluster] large input detected ({len(vectors)} events). "
            f"Downsampling to {len(clustering_input)} for local clustering."
        )

    clustering_input = _to_float32(np.asarray(clustering_input))

    # ---- PCA (fit only on clustering input) -------------------------
    if pca_dims and pca_dims < clustering_input.shape[1]:
        print(
            f"[cluster] applying PCA on clustering input: "
            f"{clustering_input.shape[1]} → {pca_dims}"
        )
        pca_model = PCA(
            n_components=pca_dims,
            random_state=42,
            svd_solver="randomized",
        )
        clustering_vectors = _to_float32(pca_model.fit_transform(clustering_input))
        print(f"[cluster] PCA complete. clustering shape: {clustering_vectors.shape}")

        # Optional debug artifact (sample/input-side PCA only)
        np.save(
            os.path.join(os.path.dirname(embeddings_path), "event_embeddings_pca.npy"),
            clustering_vectors,
        )
    else:
        print("[cluster] PCA skipped")
        clustering_vectors = clustering_input
    # Optional auto-fast mode to avoid long waits on large runs.
    if cluster_mode == "auto" and len(clustering_vectors) > 60000:
        effective_cluster_mode = "fast"
        print(
            f"[cluster] auto mode enabled: switching to fast clustering "
            f"for {len(clustering_vectors)} vectors."
        )
    elif cluster_mode == "auto":
        effective_cluster_mode = "standard"

    clusters = cluster_patterns(
        clustering_vectors,
        min_cluster_size=min_cluster_size,
        mode=effective_cluster_mode,
    )

    # ---- Event → Cluster mapping -------------------------------------
    event_cluster_map: Dict[str, str] = {}
    assigned_indices_by_cluster: Dict[str, List[int]] = {}
    assignment_mode = "direct_cluster_members"

    if sampled_indices is not None and clusters:
        # Downsample mode: cluster sampled vectors, then project all events to nearest cluster centroid.
        # This keeps runtime bounded while restoring high mapping coverage.
        assignment_mode = "downsample_centroid_projection"
        cluster_ids = list(clusters.keys())
        centroids: List[np.ndarray] = []
        for cid in cluster_ids:
            members = clusters[cid].member_indices
            m = clustering_vectors[members]
            cvec = np.mean(m, axis=0, dtype=np.float32)
            centroids.append(cvec)
        centroid_mat = _l2_normalize(np.vstack(centroids).astype(np.float32, copy=False))

        assign_batch_size = 50000
        for start in range(0, len(vectors), assign_batch_size):
            end = min(len(vectors), start + assign_batch_size)
            chunk = _to_float32(np.asarray(vectors[start:end]))
            if pca_model is not None:
                chunk = _to_float32(pca_model.transform(chunk))
            chunk = _l2_normalize(chunk)
            sims = chunk @ centroid_mat.T
            best = np.argmax(sims, axis=1)

            for offset, b in enumerate(best):
                real_idx = start + int(offset)
                cid = cluster_ids[int(b)]
                assigned_indices_by_cluster.setdefault(cid, []).append(real_idx)
                event_id = events[real_idx].get("event_id")
                if event_id:
                    event_cluster_map[event_id] = cid
    else:
        for cid, c in clusters.items():
            for idx in c.member_indices:
                real_idx = int(sampled_indices[idx]) if sampled_indices is not None else idx
                if 0 <= real_idx < len(events):
                    assigned_indices_by_cluster.setdefault(cid, []).append(int(real_idx))
                    event_id = events[real_idx].get("event_id")
                    if event_id:
                        event_cluster_map[event_id] = cid

    # ---- Build cluster output ----------------------------------------
    total_events = len(events)
    clusters_out: Dict[str, Any] = {}

    event_times = [e.get("timestamp") for e in events]

    for cid, c in clusters.items():
        mapped_member_indices = assigned_indices_by_cluster.get(
            cid,
            [
                int(sampled_indices[idx]) if sampled_indices is not None else idx
                for idx in c.member_indices
            ],
        )
        member_times = [
            event_times[idx]
            for idx in mapped_member_indices
            if idx < len(event_times) and event_times[idx] is not None
        ]

        clusters_out[cid] = {
            "cluster_id": cid,
            "member_indices": mapped_member_indices,
            "size": len(mapped_member_indices),
            "representative_index": (
                int(sampled_indices[c.representative_index])
                if sampled_indices is not None
                else c.representative_index
            ),
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
        "clustering_downsampled": sampled_indices is not None,
        "clustering_input_events": int(len(clustering_vectors)),
        "clustering_original_events": int(len(vectors)),
        "pca_applied": bool(pca_model is not None),
        "pca_dims": int(pca_dims) if pca_model is not None else None,
        "cluster_mode_requested": cluster_mode,
        "cluster_mode_effective": effective_cluster_mode,
        "max_cluster_events": int(max_cluster_events),
        "cluster_overflow_mode": cluster_overflow_mode,
        "cluster_assignment_mode": assignment_mode,
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