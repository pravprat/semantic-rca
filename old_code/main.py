# semantic-rca/main.py
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
from sklearn.decomposition import PCA

## Imports from the Pipeline
from parsers.log_reader import LogReader, iter_records_from_path
from parsers.eventizer import Eventizer

from semantic.enrichment import enrich_event

from embeddings.embedder import Embedder

from cluster.pattern_cluster import cluster_patterns
from cluster.trigger_analysis import run_trigger_analysis
from cluster.incident_detection import run_incident_detection

from old_code.rca_v2 import build_semantic_graph_from_incidents

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"

def ensure_outputs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def tag_cluster_type(cluster_size: int, total_events: int) -> str:
    """""""""
    Classifies a cluster as baseline, contextual, or candidate
    based on its relative size in the dataset
    """""""""

    fraction = cluster_size / max(total_events, 1)
    if fraction >= 0.20:
        return "baseline"
    elif fraction <= 0.05:
        return "candidate"
    else:
        return "contextual"

#############################################
# Step 1: Data ingestion from Logs
#Raw logs -> LogReader -> Eventizer (structure) -> Semantic Enrichment (meaning)
# -> events.jsonl (semantic events) -> Embedding / Clustering / RCA
#############################################

def cmd_ingest(args):

    ensure_outputs()

    reader = LogReader()
    eventizer = Eventizer()

    events_path = os.path.join(OUTPUT_DIR, "events.jsonl")

    count = 0
    batch = []
    BATCH_SIZE = 2000

    with open(events_path, "w", encoding="utf-8") as out:

        for record in iter_records_from_path(reader, args.logfile):

            batch.append(record)

            if len(batch) >= BATCH_SIZE:

                for ev in eventizer.iter_events(batch):
                    ev_dict = ev.to_dict()
                    ev_dict = enrich_event(ev_dict)
                    out.write(json.dumps(ev_dict, ensure_ascii=False) + "\n")
                    count += 1

                batch.clear()

        # flush remainder
        if batch:
            for ev in eventizer.iter_events(batch):
                ev_dict = ev.to_dict()
                ev_dict = enrich_event(ev_dict)
                out.write(json.dumps(ev_dict, ensure_ascii=False) + "\n")
                count += 1

    print(f"[ingest] wrote {count} events -> {events_path}")

def load_events() -> List[Dict[str, Any]]:
    events_path = os.path.join(OUTPUT_DIR, "events.jsonl")
    events = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    return events

#############################################
# Step 2: Generate Embeddings
#############################################

def cmd_embed(args):
    ensure_outputs()

    events = load_events()
    texts = [e["embedding_text"] for e in events]

    emb = Embedder()
    res = emb.fit_transform(texts)

    vec_path = os.path.join(OUTPUT_DIR, "event_embeddings.npy")
    np.save(vec_path, res.vectors)

    index_path = os.path.join(OUTPUT_DIR, "event_index.json")

    meta = []

    for e, text in zip(events, texts):
        entry = {
            # core identity
            "event_id": e.get("event_id"),
            "timestamp": e.get("timestamp"),
            "service": e.get("service"),
            "severity": e.get("severity"),

            # causal / behavioral fields
            "actor": e.get("actor"),
            "verb": e.get("verb"),
            "resource": e.get("resource"),
            "response_code": e.get("response_code"),
            "http_class": e.get("http_class"),

            # structured signal (low-noise)
            "stage": e.get("stage"),

            # semantic enrichment (critical)
            "semantic": e.get("semantic"),
            "signature": e.get("signature"),

            # embedding reference
            "embedding_text": text,
        }

        meta.append(entry)

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[embed] vectors={res.vectors.shape} -> {vec_path}")
    print(f"[embed] index -> {index_path}")

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

###################################################
#Step 4: Trigger Analysis
###################################################
def cmd_trigger_analysis(args):

    run_trigger_analysis(
        events_path=os.path.join(OUTPUT_DIR, "events.jsonl"),
        clusters_path=os.path.join(OUTPUT_DIR, "clusters.json"),
        event_cluster_map_path=os.path.join(
            OUTPUT_DIR,
            "event_cluster_map.json"
        ),
        output_path=os.path.join(
            OUTPUT_DIR,
            "cluster_trigger_stats.json"
        )
    )

#####################################################
#Step 5 Incident Detection
#####################################################
def cmd_incident_detection(args):
    run_incident_detection(
        cluster_trigger_stats_path=os.path.join(
            OUTPUT_DIR, "cluster_trigger_stats.json"
        ),
        output_path=os.path.join(
            OUTPUT_DIR, "incidents.json"
        ),
        gap_seconds=getattr(args, "gap_seconds", 30),
        max_seeds=getattr(args, "max_seeds", 3),
    )


#################################################
# Step 6: Semantic Graph
#################################################

from cluster.causal.causal_analysis import run_causal_analysis

def cmd_causal_analysis(args):
    run_causal_analysis(
        incidents_path=os.path.join(OUTPUT_DIR, "incidents.json"),
        cluster_trigger_stats_path=os.path.join(OUTPUT_DIR, "cluster_trigger_stats.json"),
        graph_output_path=os.path.join(OUTPUT_DIR, "incident_causal_graph.json"),
        candidates_output_path=os.path.join(OUTPUT_DIR, "incident_root_candidates.json"),
        event_cluster_map_path=os.path.join(OUTPUT_DIR, "event_cluster_map.json"),
        events_path=os.path.join(OUTPUT_DIR, "events.jsonl"),
        grounded_events_output_path=os.path.join(OUTPUT_DIR, "incident_root_events.json"),
    )


def cmd_graph(args):
    ensure_outputs()

    # --------------------------------------------------
    # Inputs
    # --------------------------------------------------

    clusters_path = os.path.join(OUTPUT_DIR, "clusters.json")
    incidents_path = os.path.join(OUTPUT_DIR, "incidents.json")
    events_path = os.path.join(OUTPUT_DIR, "events.jsonl")
    event_cluster_map_path = os.path.join(OUTPUT_DIR, "event_cluster_map.json")

    # Load clusters
    with open(clusters_path, "r", encoding="utf-8") as f:
        clusters = json.load(f)

    # Load incidents (from incident_detection step)
    with open(incidents_path, "r", encoding="utf-8") as f:
        incidents = json.load(f)

    # Load event -> cluster mapping (from clustering step)
    with open(event_cluster_map_path, "r", encoding="utf-8") as f:
        event_cluster_map = json.load(f)

    # --------------------------------------------------
    # Build semantic graph
    # --------------------------------------------------

    graph = build_semantic_graph_from_incidents(
        clusters=clusters,
        incidents=incidents,
        events_path=events_path,
        event_cluster_map=event_cluster_map,
        bucket_seconds=int(getattr(args, "bucket_seconds", 10)),
        lookahead_buckets=int(getattr(args, "lookahead_buckets", 3)),
    )

    # --------------------------------------------------
    # Output
    # --------------------------------------------------

    graph_path = os.path.join(OUTPUT_DIR, "graph.json")

    with open(graph_path, "w", encoding="utf-8") as f:
        json.dump(graph, f, ensure_ascii=False, indent=2)

    print(
        f"[graph] nodes={len(graph.get('nodes', []))} "
        f"edges={len(graph.get('edges', []))} -> {graph_path}"
    )

###### Steps 7, 8, 9, 10, 11 ######################
def cmd_patterns(args):
    from old_code.rca_v2.step7_patterns import build_incident_patterns

    out_path = OUTPUT_DIR / "incident_patterns.json"
    build_incident_patterns(OUTPUT_DIR)

    print(f"[patterns] -> {out_path}")

def cmd_candidates(args):
    from old_code.rca_v2.step8_candidates import build_incident_candidates

    out_path = OUTPUT_DIR / "incident_candidates.json"
    build_incident_candidates(OUTPUT_DIR)

    print(f"[candidates] -> {out_path}")

def cmd_root_causes(args):
    from old_code.rca_v2.step9_rank import build_ranked_root_causes

    out_path = OUTPUT_DIR / "incident_root_causes.json"
    build_ranked_root_causes(OUTPUT_DIR)

    print(f"[root_causes] -> {out_path}")


def cmd_rca_outputs(args):
    from old_code.rca_v2.step10_explain import write_rca_outputs

    write_rca_outputs(OUTPUT_DIR)

    print(f"[rca_outputs] report -> {OUTPUT_DIR / 'incident_rca_report.md'}")
    print(f"[rca_outputs] summaries -> incident_I*_summary.md")

def cmd_causal_chain(args):
    from old_code.rca_v2.step11_causal_chain import build_incident_causal_chains

    out_path = OUTPUT_DIR / "incident_causal_chains.json"
    build_incident_causal_chains(OUTPUT_DIR)

    print(f"[causal_chain] -> {out_path}")

#############################################
# Run through the pipeline steps: 1 through 12
#############################################

def cmd_all(args):

    if args.logfile:

        print("\n[STEP 1] ingest")
        cmd_ingest(args)

        print("\n[STEP 2] embed")
        cmd_embed(args)

        print("\n[STEP 3] cluster")
        cmd_cluster(args)

        print("\n[STEP 4] trigger_analysis")
        cmd_trigger_analysis(args)

        print("\n[STEP 5] incident_detection")
        cmd_incident_detection(args)

        print("\n[STEP 6] graph")
        cmd_graph(args)

        print("\n[STEP 7] patterns")
        cmd_patterns(args)

        print("\n[STEP 8] candidates")
        cmd_candidates(args)

        print("\n[STEP 9] rank")
        cmd_root_causes(args)

        print("\n[STEP 10] explain_summary")
        cmd_rca_outputs(args)

        print("\n[STEP 11] causal_chain")
        cmd_causal_chain(args)

def build_parser():
    p = argparse.ArgumentParser(prog="semantic-rca", description="Semantic RCA prototype (logs -> clusters -> graph -> RCA report)")
    sub = p.add_subparsers(dest="cmd", required=True)

    ingest = sub.add_parser("ingest", help="Parse raw log into outputs/events.jsonl")
    ingest.add_argument("logfile")
    ingest.set_defaults(func=cmd_ingest)

    embed = sub.add_parser("embed", help="Create embeddings from outputs/events.jsonl")
    embed.add_argument("--logfile", default=None, help="Unused; events.jsonl must exist")
    embed.set_defaults(func=cmd_embed)

    cluster = sub.add_parser("cluster", help="Cluster embeddings into semantic event patterns")
    cluster.add_argument("--min-cluster-size", type=int, default=15)
    cluster.add_argument("--max-gap-seconds", type=int, default=120)
    cluster.add_argument(
        "--pca-dims", type=int, default=256,
        help="Reduce embedding dimensions before clustering (0 or None to disable)"
    )
    cluster.set_defaults(func=cmd_cluster)

    trigger = sub.add_parser("trigger_analysis",help="Compute per-cluster trigger statistics (writes outputs/cluster_trigger_stats.json)")
    trigger.set_defaults(func=cmd_trigger_analysis)

    detection = sub.add_parser("incident_detection", help="Detect behavior-driven incidents from trigger waves")
    detection.add_argument("--bucket-seconds", type=int, default=10)
    detection.add_argument("--seed-trigger-threshold", type=float, default=0.20)
    detection.add_argument("--bucket-anomaly-threshold", type=float, default=1.0)
    detection.add_argument("--cooldown-buckets", type=int, default=6)
    detection.add_argument("--max-incident-seconds", type=int, default=1800)
    detection.add_argument("--max-seeds", type=int, default=3)
    detection.add_argument("--signal-trigger-threshold",type=float,default=0.18, help="Ignore clusters with trigger_score below this threshold")
    detection.set_defaults(func=cmd_incident_detection)

    graph = sub.add_parser("graph", help="Build semantic cluster graph from incidents")
    graph.add_argument("--top-k", type=int, default=10)
    graph.set_defaults(func=cmd_graph)

    patterns = sub.add_parser("patterns")
    patterns.set_defaults(func=cmd_patterns)

    candidates = sub.add_parser("candidates")
    candidates.set_defaults(func=cmd_candidates)

    rank = sub.add_parser("root_causes")
    rank.set_defaults(func=cmd_root_causes)

    explain = sub.add_parser("rca_outputs")
    explain.set_defaults(func=cmd_rca_outputs)

    causal = sub.add_parser("causal_chain", help="Build causal propagation chains from RCA outputs")
    causal.set_defaults(func=cmd_causal_chain)

    allp = sub.add_parser("all", help="Run full pipeline: ingest -> embed -> cluster -> trigger -> incident_detection -> graph -> patterns -> candidates -> rank root causes ->  summary")
    allp.add_argument("logfile", nargs="?", help="Path to raw logfile")
    allp.add_argument("--min-cluster-size", type=int, default=15)
    allp.add_argument("--max-gap-seconds", type=int, default=120)
    allp.add_argument("--top-k", type=int, default=10)
    allp.add_argument(
        "--pca-dims",
        type=int,
        default=256,
        help="Reduce embedding dimensions before clustering (0 or None to disable)"
    )
    allp.set_defaults(func=cmd_all)
    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()