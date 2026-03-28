# semantic-rca/main.py
from __future__ import annotations

import argparse
import json
import subprocess
import shutil
import sys
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
    "report_md": OUTPUT_DIR / "incident_rca_report.md",
    "evidence_bundle": OUTPUT_DIR / "incident_evidence_bundle.json",
    "detailed_report_json": OUTPUT_DIR / "incident_rca_report_detailed.json",
    "detailed_report_md": OUTPUT_DIR / "incident_rca_report_detailed.md",
    "assertions": OUTPUT_DIR / "incident_assertions.json",
    "timeline_summary": OUTPUT_DIR / "incident_timeline_summary.json",
    "incident_detection_status": OUTPUT_DIR / "incident_detection_status.json",
    "preincident_json": OUTPUT_DIR / "preincident_diagnostics.json",
    "preincident_md": OUTPUT_DIR / "preincident_diagnostics.md",
    "validation_json": OUTPUT_DIR / "validation_report.json",
    "validation_md": OUTPUT_DIR / "validation_report.md",
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
from tools.build_preincident_diagnostics import build_preincident_diagnostics, render_markdown


def cmd_incident_detection(args):
    out = run_incident_detection(
        cluster_trigger_stats_path=str(PATHS["trigger_stats"]),
        output_path=str(PATHS["incidents"]),
        gap_seconds=getattr(args, "gap_seconds", 30),
        max_seeds=getattr(args, "max_seeds", 3),
        status_output_path=str(PATHS["incident_detection_status"]),
    )
    return out


def cmd_validate_outputs(args) -> bool:
    """Run external validation script and write reports."""
    validation_script = PROJECT_ROOT / "validation" / "validate_pipeline_steps.py"
    outputs_dir = Path(getattr(args, "outputs_dir", OUTPUT_DIR))
    raw_log_arg = getattr(args, "raw_log", None)
    cmd = [sys.executable, str(validation_script), "--outputs-dir", str(outputs_dir)]
    if raw_log_arg:
        cmd.extend(["--raw-log", str(raw_log_arg)])
    if getattr(args, "compat_v142", False):
        cmd.append("--compat-v142")
    cmd.extend(["--report-json", str(PATHS["validation_json"]), "--report-md", str(PATHS["validation_md"])])
    rc = subprocess.run(cmd, check=False).returncode
    return rc == 0

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
# Step 7: Reporting (JSON + Markdown)
# ------------------------------------------------------------

from cluster.causal.reporting.rca_report_builder import build_rca_report
from cluster.causal.reporting.report_renderer import render_report


def cmd_report(args):
    """
    Step 7:
    - Build structured RCA report (JSON)
    - Render human-readable report (Markdown)
    """

    print("\n[STEP 7] building RCA JSON report")

    build_rca_report(
        incidents_path=str(PATHS["incidents"]),
        candidates_path=str(PATHS["candidates"]),
        root_events_path=str(PATHS["root_events"]),
        output_path=str(PATHS["rca_report"]),
    )

    print("[STEP 7] rendering Markdown report")

    render_report(
        incidents_path=str(PATHS["incidents"]),
        candidates_path=str(PATHS["candidates"]),
        grounded_events_path=str(PATHS["root_events"]),
        output_path=str(PATHS["report_md"]),
    )

    print("[STEP 7] reporting complete")

##########################################################
##Step 8: Build evidence bundle
#########################################################

from tools.build_evidence_bundle import build_evidence_bundle

def cmd_evidence_bundle(args):
    """
    Step 8:
    - Build forensic evidence bundle JSON from existing RCA artifacts.
    """
    print("\n[STEP 8] building evidence bundle")

    build_evidence_bundle(
        incidents_path=PATHS["incidents"],
        candidates_path=PATHS["candidates"],
        grounded_events_path=PATHS["root_events"],
        graph_path=PATHS["graph"],
        report_path=PATHS["rca_report"],
        events_path=PATHS["events"],
        output_path=OUTPUT_DIR / "incident_evidence_bundle.json",
    )

    print("[STEP 8] evidence bundle complete")

# ------------------------------------------------------------
# Step 9: Detailed reporting (evidence-aware JSON + Markdown)
# ------------------------------------------------------------

from tools.build_detailed_report import build_detailed_report_json, render_detailed_markdown

def cmd_detailed_report(args):
    """
    Step 9:
    - Build detailed RCA JSON by merging base report and evidence bundle
    - Render support-first detailed Markdown report
    """
    print("\n[STEP 9] building detailed RCA JSON report")

    detailed_reports = build_detailed_report_json(
        base_report_path=PATHS["rca_report"],
        evidence_bundle_path=PATHS["evidence_bundle"],
        output_json_path=PATHS["detailed_report_json"],
    )

    print("[STEP 9] rendering detailed Markdown report")
    render_detailed_markdown(
        detailed_reports=detailed_reports,
        output_md_path=PATHS["detailed_report_md"],
    )

    print("[STEP 9] detailed reporting complete")

# ------------------------------------------------------------
# Step 10: Incident assertions
# ------------------------------------------------------------

from tools.build_incident_assertions import build_assertions

def cmd_incident_assertions(args):
    """
    Step 10:
    - Build machine-checkable incident assertions from RCA artifacts
    """
    print("\n[STEP 10] building incident assertions")
    out = build_assertions(
        incidents_path=PATHS["incidents"],
        candidates_path=PATHS["candidates"],
        roots_path=PATHS["root_events"],
        evidence_bundle_path=PATHS["evidence_bundle"],
        output_path=PATHS["assertions"],
    )
    print(f"[STEP 10] incident assertions complete (incidents={len(out)})")

# ------------------------------------------------------------
# Diagnostics (no-incident helper)
# ------------------------------------------------------------

def cmd_preincident_diagnostics(args):
    diag = build_preincident_diagnostics(OUTPUT_DIR)
    PATHS["preincident_json"].write_text(json.dumps(diag, ensure_ascii=False, indent=2), encoding="utf-8")
    PATHS["preincident_md"].write_text(render_markdown(diag), encoding="utf-8")
    print(f"[PREINCIDENT] -> {PATHS['preincident_json']}")
    print(f"[PREINCIDENT] -> {PATHS['preincident_md']}")

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
    ("STEP 7", "rca_report", cmd_report),
    ("STEP 8", "evidence_bundle", cmd_evidence_bundle),
    ("STEP 9", "detailed_report", cmd_detailed_report),
    ("STEP 10", "incident_assertions", cmd_incident_assertions),
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

        # If no incidents are detected, emit diagnostics and stop gracefully.
        if label == "incident_detection":
            incidents = []
            if PATHS["incidents"].exists():
                incidents = json.loads(PATHS["incidents"].read_text(encoding="utf-8"))
            if not incidents:
                print("\n[PIPELINE] no incidents detected, generating pre-incident diagnostics")
                cmd_preincident_diagnostics(args)
                print("\n[PIPELINE] running post-run validation")
                args.outputs_dir = OUTPUT_DIR
                args.raw_log = args.logfile
                cmd_validate_outputs(args)
                print("[PIPELINE] stopping after diagnostics (no incident path)")
                print(f"Outputs available at: {OUTPUT_DIR}")
                return

    print("\n🎉 Pipeline complete")
    print(f"Outputs available at: {OUTPUT_DIR}")
    print("\n[PIPELINE] running post-run validation")
    args.outputs_dir = OUTPUT_DIR
    args.raw_log = args.logfile
    cmd_validate_outputs(args)


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

    report = sub.add_parser("report", help="Step 7: Generate RCA report (JSON + Markdown)")
    report.set_defaults(func=cmd_report)

    evidence = sub.add_parser(
        "evidence_bundle",help="Step 8: Build incident evidence bundle JSON")
    evidence.set_defaults(func=cmd_evidence_bundle)

    detailed = sub.add_parser(
        "detailed_report",
        help="Step 9: Generate detailed RCA report (JSON + Markdown)"
    )
    detailed.set_defaults(func=cmd_detailed_report)

    assertions = sub.add_parser(
        "incident_assertions",
        help="Step 10: Generate incident assertions JSON"
    )
    assertions.set_defaults(func=cmd_incident_assertions)

    prediag = sub.add_parser(
        "preincident_diagnostics",
        help="Build diagnostics when incidents are not detected"
    )
    prediag.set_defaults(func=cmd_preincident_diagnostics)

    validate = sub.add_parser(
        "validate",
        help="Run QA validation checks on output artifacts"
    )
    validate.add_argument("--outputs-dir", default=str(OUTPUT_DIR))
    validate.add_argument("--raw-log", default=None)
    validate.add_argument(
        "--compat-v142",
        action="store_true",
        help="Compatibility mode for v1.4.2-style outputs (status/timeline optional).",
    )
    validate.set_defaults(func=cmd_validate_outputs)

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