# semantic-rca/main.py
from __future__ import annotations

import argparse
import json
import os
import sys
from pathlib import Path
from typing import List, Dict, Any
import numpy as np
from sklearn.decomposition import PCA

## Imports from the Pipeline
from parsers.log_reader import LogReader, iter_records_from_path
from parsers.eventizer import Eventizer
from embeddings.embedder import Embedder
from embeddings.vector_store import VectorStore
from cluster.pattern_cluster import cluster_patterns
from cluster.incident_cluster import build_incidents
from cluster.trigger_analysis import run_trigger_analysis
from cluster.incident_detection import run_incident_detection

from graph.incident_rca import build_incident_root_causes
from graph.build_graph import build_semantic_graph_from_incidents

from reports.incident_rca_report import write_incident_rca_report
from reports.rca_explainer import build_incident_explanations, write_explanation_report

from tools.evidence_bundle import build_evidence_bundle
from tools.llm_summarizer import LLMSummarizer
from tools.incident_graph import write_incident_graph


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
                    out.write(json.dumps(ev.to_dict(), ensure_ascii=False) + "\n")
                    count += 1

                batch.clear()

        # flush remainder
        if batch:
            for ev in eventizer.iter_events(batch):
                out.write(json.dumps(ev.to_dict(), ensure_ascii=False) + "\n")
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
            "event_id": e.get("event_id"),
            "timestamp": e.get("timestamp"),
            "service": e.get("service"),
            "severity": e.get("severity"),

            # useful causal metadata
            "actor": e.get("actor"),
            "verb": e.get("verb"),
            "resource": e.get("resource"),
            "response_code": e.get("response_code"),

            # embedding text for debugging / clustering inspection
            "embedding_text": text
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
        events_path=os.path.join(OUTPUT_DIR, "events.jsonl"),
        event_cluster_map_path=os.path.join(OUTPUT_DIR, "event_cluster_map.json"),
        cluster_trigger_stats_path=os.path.join(OUTPUT_DIR, "cluster_trigger_stats.json"),
        output_path=os.path.join(OUTPUT_DIR, "incidents.json"),
        bucket_seconds=getattr(args, "bucket_seconds", 10),
        seed_trigger_threshold=getattr(args, "seed_trigger_threshold", 0.20),
        signal_trigger_threshold=getattr(args, "signal_trigger_threshold", 0.18),
        bucket_anomaly_threshold=getattr(args, "bucket_anomaly_threshold", 1.0),
        cooldown_buckets=getattr(args, "cooldown_buckets", 6),
        max_incident_seconds=getattr(args, "max_incident_seconds", 1800),
        max_seeds=getattr(args, "max_seeds", 3),
    )


#################################################
# Step 6: Semantic Graph
#################################################

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


#############################################
# Step 7: Incident-Scoped Root Cause Analysis
#############################################
def cmd_incident_rca(args):
    ensure_outputs()

    inc_root_causes = build_incident_root_causes(
        outputs_dir=OUTPUT_DIR,
        incidents_path=OUTPUT_DIR / "incidents.json",
        top_k_per_incident=int(getattr(args, "top_k", 5)),
    )

    out_path = OUTPUT_DIR / "incident_root_causes.json"

    with out_path.open("w", encoding="utf-8") as f:
        json.dump(inc_root_causes, f, ensure_ascii=False, indent=2)

    write_incident_rca_report(
        incident_rca_path=out_path,
        clusters_path=OUTPUT_DIR / "clusters.json",
        out_path=OUTPUT_DIR / "incident_rca_report.md",
    )

    print(f"[incident_rca] wrote -> {out_path}")


#############################################
# Step 8: RCA Report
#############################################

def cmd_report(args):

    ensure_outputs()

    incident_rca_path = OUTPUT_DIR / "incident_root_causes.json"
    clusters_path = OUTPUT_DIR / "clusters.json"
    report_path = OUTPUT_DIR / "incident_rca_report.md"

    write_incident_rca_report(
        incident_rca_path=incident_rca_path,
        clusters_path=clusters_path,
        out_path=report_path
    )

#######################################
### Step 9: RCA Explain
#######################################
def cmd_rca_explain(args):

    ensure_outputs()

    expl_json = OUTPUT_DIR / "incident_explanations.json"

    build_incident_explanations(
        incident_rca_path=OUTPUT_DIR / "incident_root_causes.json",
        out_path=expl_json,
    )

    write_explanation_report(
        explanation_json=expl_json,
        out_md=OUTPUT_DIR / "incident_explanations.md",
    )

    print(f"[rca_explain] wrote -> {expl_json}")

#############################################
# Step 10: Evidence Bundle Generation
#############################################

def cmd_evidence(args):
    """
    Step 9 — Evidence Bundle

    Build and persist explainability artifacts from RCA outputs.

    Writes:
        outputs/evidence/evidence_bundle.json
        outputs/evidence/incident_<incident_id>.json
    """

    ensure_outputs()

    top_n = int(getattr(args, "top_n", 5))

    evidence_dir = OUTPUT_DIR / "evidence"
    evidence_dir.mkdir(parents=True, exist_ok=True)

    # Build bundle (do NOT let builder write files)
    bundle = build_evidence_bundle(
        outputs_dir=OUTPUT_DIR,
        write_json=False,
    )

    # -----------------------------
    # Write full evidence bundle
    # -----------------------------
    bundle_path = evidence_dir / "evidence_bundle.json"

    bundle_path.write_text(
        json.dumps(bundle, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8"
    )

    print(f"[evidence] bundle written → {bundle_path}")

    # -----------------------------
    # Write per-incident evidence
    # -----------------------------
    incidents = bundle.get("incidents", [])

    written = 0

    if isinstance(incidents, list):
        for inc in incidents:

            inc_id = inc.get("incident_id") or f"unknown_{written}"

            inc_file = evidence_dir / f"incident_{inc_id}.json"

            inc_file.write_text(
                json.dumps(inc, ensure_ascii=False, indent=2) + "\n",
                encoding="utf-8"
            )

            print(f"[evidence] incident written → {inc_file}")

            written += 1

    print(
        f"[evidence] complete: {written} incident(s) written (top_n={top_n})"
    )

##########################################
#Step 11 Incident Graph
#########################################
def cmd_incident_graph(args):
    """
    Step 9.5 — Build cross-incident causality graph from evidence bundle.
    """
    ensure_outputs()

    max_gap_seconds = int(getattr(args, "max_gap_seconds", 300))

    out_path = write_incident_graph(
        outputs_dir=OUTPUT_DIR,
        max_gap_seconds=max_gap_seconds,
    )

    print(f"[incident_graph] wrote → {out_path}")

#############################################
# Step 12: generate LLM based summary
#############################################
def cmd_llm_summary(args):
    """
    Step 10 — Deterministic narrative summaries
    """

    ensure_outputs()

    evidence_path = OUTPUT_DIR / "evidence" / "evidence_bundle.json"

    if not evidence_path.exists():
        raise RuntimeError(
            "Evidence bundle not found. Run step 9 (evidence) first."
        )

    with evidence_path.open("r", encoding="utf-8") as f:
        evidence_bundle = json.load(f)

    summarizer = LLMSummarizer(
        output_dir=OUTPUT_DIR / "llm"
    )

    written = summarizer.summarize_incidents(
        evidence_bundle=evidence_bundle
    )

    print("\n[llm] summaries written:")

    for p in written:
        print(f"  - {p}")

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

        print("\n[STEP 7] incident_rca")
        cmd_incident_rca(args)

        print("\n[STEP 8] report")
        cmd_report(args)

        print("\n[STEP 9] rca_explain")
        cmd_report(args)

        print("\n[STEP 10] evidence")
        cmd_evidence(args)

        print("\n[STEP 11] incident_graph")
        cmd_incident_graph(args)

    else:
        print("[All] No log file provided - assuming outputs already present")

    if getattr(args, "llm_summary", False):

        print("\n[STEP 12] llm_summary")
        cmd_llm_summary(args)


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

    incident_rca = sub.add_parser("incident_rca", help="Incident-scoped RCA ranking")
    incident_rca.set_defaults(func=cmd_incident_rca)

    report = sub.add_parser("report", help="Generate RCA markdown report")
    report.add_argument("--logfile", default=None, help="Unused; artifacts must exist")
    report.set_defaults(func=cmd_report)

    rca_explain = sub.add_parser("rca_explain", help="Explain RCA")
    rca_explain.set_defaults(func=cmd_rca_explain)

    evidence = sub.add_parser("evidence", help="Generate deterministic evidence bundle JSON from existing outputs")
    evidence.add_argument("--top-n", type=int, default=5, help="Top N ranked candidates per incident to include in evidence")
    evidence.set_defaults(func=cmd_evidence)

    incident_graph = sub.add_parser("incident_graph",help="Build cross-incident causality graph")
    incident_graph.add_argument("--max-gap-seconds", type=int, default=300, help="Maximum allowed gap between incidents for parent-child linking")
    incident_graph.set_defaults(func=cmd_incident_graph)

    llm = sub.add_parser("llm_summary", help="Generate narrative summaries from evidence bundle")
    #llm.add_argument("--model", default=None, help="Ollama model name")
    #llm.add_argument("--ollama-model", default=None, help="Alias for --model")
    #llm.add_argument("--use-openai", action="store_true", help="Use OpenAI provider")
    #llm.add_argument("--use-ollama", action="store_true", help="Use Ollama provider")
    #llm.add_argument("--openai-model", default=None, help="OpenAI model name")
    llm.set_defaults(func=cmd_llm_summary)

    allp = sub.add_parser("all", help="Run full pipeline: ingest -> embed -> cluster -> trigger -> incident_detection -> graph -> incident_rca -> report -> evidence -> llm summary")
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
    allp.add_argument(
        "--llm-summary",
        action="store_true",
        help="Generate LLM-based narrative summaries after reports"
    )
    allp.set_defaults(func=cmd_all)

    return p


def main():
    parser = build_parser()
    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()