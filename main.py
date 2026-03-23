# semantic-rca/main.py
from __future__ import annotations

import argparse
import shutil
from pathlib import Path

# ------------------------------------------------------------
# Project paths
# ------------------------------------------------------------

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"


def clean_outputs():
    if OUTPUT_DIR.exists():
        print(f"\n🧹 Cleaning outputs directory: {OUTPUT_DIR}")
        shutil.rmtree(OUTPUT_DIR)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    print("✅ outputs/ cleaned")


def ensure_outputs() -> None:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


# Centralized paths (single source of truth)
PATHS = {
    "events": OUTPUT_DIR / "events.jsonl",
    "embeddings": OUTPUT_DIR / "event_embeddings.npy",
    "index": OUTPUT_DIR / "event_index.json",
    "clusters": OUTPUT_DIR / "clusters.json",
    "event_cluster_map": OUTPUT_DIR / "event_cluster_map.json",
    "trigger_stats": OUTPUT_DIR / "cluster_trigger_stats.json",
    "incidents": OUTPUT_DIR / "incidents.json",
    "graph": OUTPUT_DIR / "incident_causal_graph.json",
    "candidates": OUTPUT_DIR / "incident_root_candidates.json",
    "root_events": OUTPUT_DIR / "incident_root_events.json",
    "rca_report": OUTPUT_DIR / "incident_rca_report.json",
}

# ------------------------------------------------------------
# Step 1: Ingest
# ------------------------------------------------------------

from parsers.ingest_runner import run_ingest


def cmd_ingest(args):
    run_ingest(
        logfile=args.logfile,
        output_path=str(PATHS["events"]),
    )

# ------------------------------------------------------------
# Step 2: Embeddings
# ------------------------------------------------------------

from embeddings.embed_runner import run_embedding

def cmd_embed(args):
    run_embedding(
        events_path=str(PATHS["events"]),
        output_vectors_path=str(PATHS["embeddings"]),
        output_index_path=str(PATHS["index"]),
    )

# ------------------------------------------------------------
# Step 3: Clustering
# ------------------------------------------------------------

from cluster.cluster_runner import run_clustering

def cmd_cluster(args):
    run_clustering(
        events_path=str(PATHS["events"]),
        embeddings_path=str(PATHS["embeddings"]),
        clusters_output_path=str(PATHS["clusters"]),
        event_cluster_map_output_path=str(PATHS["event_cluster_map"]),
        min_cluster_size=args.min_cluster_size,
        pca_dims=args.pca_dims,
    )

# ------------------------------------------------------------
# Step 4: Trigger Analysis
# ------------------------------------------------------------

from cluster.trigger_analysis import run_trigger_analysis

def cmd_trigger_analysis(args):
    run_trigger_analysis(
        events_path=str(PATHS["events"]),
        clusters_path=str(PATHS["clusters"]),
        event_cluster_map_path=str(PATHS["event_cluster_map"]),
        output_path=str(PATHS["trigger_stats"]),
    )

# ------------------------------------------------------------
# Step 5: Incident Detection
# ------------------------------------------------------------

from cluster.incident_detection import run_incident_detection


def cmd_incident_detection(args):
    run_incident_detection(
        cluster_trigger_stats_path=str(PATHS["trigger_stats"]),
        output_path=str(PATHS["incidents"]),
        gap_seconds=getattr(args, "gap_seconds", 30),
        max_seeds=getattr(args, "max_seeds", 3),
    )

# ------------------------------------------------------------
# Step 6: Causal Inference + Root Identification
# ------------------------------------------------------------

from cluster.causal.causal_analysis import run_causal_analysis

def cmd_causal_analysis(args):
    """
    Step 6:
    - Cluster-level causal graph (inference)
    - Root candidate extraction
    - Event-level grounding (evidence)
    """
    run_causal_analysis(
        incidents_path=str(PATHS["incidents"]),
        cluster_trigger_stats_path=str(PATHS["trigger_stats"]),
        graph_output_path=str(PATHS["graph"]),
        candidates_output_path=str(PATHS["candidates"]),
        event_cluster_map_path=str(PATHS["event_cluster_map"]),
        events_path=str(PATHS["events"]),
        grounded_events_output_path=str(PATHS["root_events"]),
    )

# ------------------------------------------------------------
# Step 7: Reporting
# ------------------------------------------------------------

from cluster.causal.reporting.rca_report_builder import build_rca_report

def cmd_rca_report(args):
    build_rca_report(
        incidents_path=str(PATHS["incidents"]),
        candidates_path=str(PATHS["candidates"]),
        root_events_path=str(PATHS["root_events"]),
        output_path=str(PATHS["rca_report"]),
    )


# ------------------------------------------------------------
# Full pipeline runner
# ------------------------------------------------------------

PIPELINE_STEPS = [
    ("STEP 1", "ingest", cmd_ingest),
    ("STEP 2", "embed", cmd_embed),
    ("STEP 3", "cluster", cmd_cluster),
    ("STEP 4", "trigger_analysis", cmd_trigger_analysis),
    ("STEP 5", "incident_detection", cmd_incident_detection),
    ("STEP 6", "causal_analysis", cmd_causal_analysis),
    ("STEP 7", "rca_report", cmd_rca_report),
]


def cmd_all(args):
    import time

    if not args.logfile:
        raise ValueError("logfile is required for 'all' command")

    if getattr(args, "clean", False):
        clean_outputs()

    ensure_outputs()

    for step_name, label, fn in PIPELINE_STEPS:
        print(f"\n[{step_name}] {label}")
        start = time.time()

        fn(args)

        duration = round(time.time() - start, 2)
        print(f"[{step_name}] completed in {duration}s")

    print("\n🎉 Pipeline complete")
    print(f"Outputs available at: {OUTPUT_DIR}")


# ------------------------------------------------------------
# CLI
# ------------------------------------------------------------

def build_parser():
    p = argparse.ArgumentParser(
        prog="semantic-rca",
        description="Semantic RCA pipeline (logs → clusters → causal graph → root causes)"
    )

    sub = p.add_subparsers(dest="cmd", required=True)

    # ingest
    ingest = sub.add_parser("ingest", help="Parse raw logs into events.jsonl")
    ingest.add_argument("logfile")
    ingest.set_defaults(func=cmd_ingest)

    # embed
    embed = sub.add_parser("embed", help="Generate embeddings")
    embed.set_defaults(func=cmd_embed)

    # cluster
    cluster = sub.add_parser("cluster", help="Cluster events into patterns")
    cluster.add_argument("--min-cluster-size", type=int, default=15)
    cluster.add_argument("--pca-dims", type=int, default=256)
    cluster.set_defaults(func=cmd_cluster)

    # trigger analysis
    trigger = sub.add_parser("trigger_analysis", help="Compute trigger stats")
    trigger.set_defaults(func=cmd_trigger_analysis)

    # incident detection
    detection = sub.add_parser("incident_detection", help="Detect incidents")
    detection.add_argument("--gap-seconds", type=int, default=30)
    detection.add_argument("--max-seeds", type=int, default=3)
    detection.set_defaults(func=cmd_incident_detection)

    # causal analysis (Step 6)
    causal = sub.add_parser(
        "causal_analysis",
        help="Step 6: causal graph + root candidates + event grounding"
    )
    causal.set_defaults(func=cmd_causal_analysis)

    report = sub.add_parser("rca_report", help="Step 7: build RCA report")
    report.set_defaults(func=cmd_rca_report)

    # full pipeline
    allp = sub.add_parser(
        "all",
        help="Run full pipeline end-to-end"
    )
    allp.add_argument("logfile", nargs="?", help="Path to raw logfile")
    allp.add_argument("--min-cluster-size", type=int, default=15)
    allp.add_argument("--pca-dims", type=int, default=256)
    allp.add_argument("--gap-seconds", type=int, default=30)
    allp.add_argument("--max-seeds", type=int, default=3)
    allp.set_defaults(func=cmd_all)
    allp.add_argument(
        "--clean",
        action="store_true",
        help="Clean outputs directory before running pipeline"
    )

    return p

def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)

if __name__ == "__main__":
    main()