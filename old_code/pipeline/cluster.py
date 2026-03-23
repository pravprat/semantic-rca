from __future__ import annotations

import json
import os
from pathlib import Path
import numpy as np
from sklearn.decomposition import PCA

## Imports from the Pipeline
from old_code.pipeline.cluster import cluster_patterns

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"

def ensure_outputs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


#################################################
# Step 3: Clustering
#################################################

def cmd_cluster(args):
    ensure_outputs()
    events = load_events()

    vectors = np.load(os.path.join(OUTPUT_DIR, "event_embeddings.npy"))

    # ---- PCA for clustering speedup ----------------------------------
    print(f"[cluster] original vectors shape: {vectors.shape}")

    pca_dims = getattr(args, "pca_dims", 256)
    if pca_dims and pca_dims < vectors.shape[1]:
        print(f"[cluster] applying PCA: {vectors.shape[1]} → {pca_dims}")
        vectors = PCA(
            n_components=pca_dims,
            random_state=42,
            svd_solver="randomized"
        ).fit_transform(vectors)

        print(f"[cluster] PCA complete. New shape: {vectors.shape}")

        assert vectors.ndim == 2 and vectors.shape[1] == pca_dims, \
            f"PCA failed: got shape {vectors.shape}, expected (*, {pca_dims})"

        vectors = vectors.astype(np.float32, copy=False)
        assert vectors.dtype == np.float32

        # Save PCA vectors
        np.save(
            os.path.join(OUTPUT_DIR, "event_embeddings_pca.npy"),
            vectors
        )
    else:
        print("[cluster] PCA skipped")
        vectors = vectors.astype(np.float32, copy=False)

    # ---- Clustering --------------------------------------------------
    clusters = cluster_patterns(
        vectors,
        min_cluster_size=args.min_cluster_size
    )

    # ---- Build event_id -> cluster_id map ----------------------------
    event_cluster_map = {}

    for cid, c in clusters.items():
        for idx in c.member_indices:

            if idx < 0 or idx >= len(events):
                continue

            event_id = events[idx].get("event_id")
            if event_id:
                event_cluster_map[event_id] = cid

    # ---- Persist clusters --------------------------------------------
    total_events = len(events)

    clusters_out = {}

    # Pre-extract timestamps
    event_times = [
        e.get("timestamp") for e in events
    ]

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

    clusters_path = os.path.join(OUTPUT_DIR, "clusters.json")
    with open(clusters_path, "w", encoding="utf-8") as f:
        json.dump(clusters_out, f, ensure_ascii=False, indent=2)

    # ---- Persist event_cluster_map -----------------------------------
    map_path = os.path.join(OUTPUT_DIR, "event_cluster_map.json")
    with open(map_path, "w", encoding="utf-8") as f:
        json.dump(event_cluster_map, f, ensure_ascii=False, indent=2)

    print(f"[cluster] clusters={len(clusters_out)} -> {clusters_path}")
    print(f"[cluster] event_cluster_map size={len(event_cluster_map)} -> {map_path}")