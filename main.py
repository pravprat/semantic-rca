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
DEFAULT_OUTPUTS_ROOT = PROJECT_ROOT / "outputs"


def clean_outputs():
    if DEFAULT_OUTPUTS_ROOT.exists():
        print(f"\n🧹 Cleaning outputs directory: {DEFAULT_OUTPUTS_ROOT}")
        shutil.rmtree(DEFAULT_OUTPUTS_ROOT)
    DEFAULT_OUTPUTS_ROOT.mkdir(parents=True, exist_ok=True)
    print("✅ outputs/ cleaned")


def ensure_outputs() -> None:
    DEFAULT_OUTPUTS_ROOT.mkdir(parents=True, exist_ok=True)


def get_output_dir(args) -> Path:
    root = Path(getattr(args, "outputs_root", DEFAULT_OUTPUTS_ROOT))
    profile = getattr(args, "pipeline_profile", "v1")
    return root / profile


def get_paths(args) -> dict[str, Path]:
    output_dir = get_output_dir(args)
    profile = getattr(args, "pipeline_profile", "v1")
    events_file = "events.parquet" if profile == "v2" else "events.jsonl"
    index_file = "event_index.parquet" if profile == "v2" else "event_index.json"
    return {
        "output_dir": output_dir,
        "events": output_dir / events_file,
        "embeddings": output_dir / "event_embeddings.npy",
        "index": output_dir / index_file,
        "clusters": output_dir / "clusters.json",
        "event_cluster_map": output_dir / "event_cluster_map.json",
        "trigger_stats": output_dir / "cluster_trigger_stats.json",
        "incidents": output_dir / "incidents.json",
        "graph": output_dir / "incident_causal_graph.json",
        "candidates": output_dir / "incident_root_candidates.json",
        "root_events": output_dir / "incident_root_events.json",
        "rca_report": output_dir / "incident_rca_report.json",
        "report_md": output_dir / "incident_rca_report.md",
        "evidence_bundle": output_dir / "incident_evidence_bundle.json",
        "detailed_report_json": output_dir / "incident_rca_report_detailed.json",
        "detailed_report_md": output_dir / "incident_rca_report_detailed.md",
        "assertions": output_dir / "incident_assertions.json",
        "timeline_summary": output_dir / "incident_timeline_summary.json",
        "incident_detection_status": output_dir / "incident_detection_status.json",
        "preincident_json": output_dir / "preincident_diagnostics.json",
        "preincident_md": output_dir / "preincident_diagnostics.md",
        "validation_json": output_dir / "validation_report.json",
        "validation_md": output_dir / "validation_report.md",
    }

# ------------------------------------------------------------
# Step 1: Ingest
# ------------------------------------------------------------

from parsers.ingest_runner import run_ingest
from tools.build_log_triage_slices import build_triage


def _resolve_ingest_logfile_list(args, output_dir: Path) -> tuple[str | None, Path | None]:
    """
    Returns (logfile_list_path or None, triage_output_dir or None).
    If --triage, runs failure-signal triage and returns path to selected_log_files.txt.
    """
    logfile_list = getattr(args, "logfile_list", None)
    if not getattr(args, "triage", False):
        return logfile_list, None

    if logfile_list:
        raise ValueError("Use either --triage or --logfile-list, not both.")

    input_dir = getattr(args, "triage_input_dir", None) or args.logfile
    if not input_dir:
        raise ValueError("--triage requires logfile (as a directory) or --triage-input-dir")

    root = Path(input_dir)
    if not root.is_dir():
        raise ValueError(f"--triage requires a directory path; got: {input_dir}")

    tout = getattr(args, "triage_output_dir", None)
    triage_out = Path(tout) if tout else (output_dir / "log_triage")
    manifest = build_triage(
        input_dir=root,
        output_dir=triage_out,
        top_n=getattr(args, "triage_top_n", 40),
        min_weighted_score=getattr(args, "triage_min_weighted_score", 10),
        max_lines_per_file=getattr(args, "triage_max_lines_per_file", 0),
    )
    selected = triage_out / "selected_log_files.txt"
    print(
        f"[ingest] triage: selected={manifest['files_selected']} "
        f"eligible={manifest['files_meeting_threshold']} -> {selected}"
    )
    return str(selected), triage_out


def cmd_ingest(args):
    paths = get_paths(args)
    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    logfile_list, _ = _resolve_ingest_logfile_list(args, paths["output_dir"])
    logfile = args.logfile if args.logfile else "."
    run_ingest(
        logfile=logfile,
        output_path=str(paths["events"]),
        file_batch_size=getattr(args, "ingest_file_batch_size", 20),
        batch_size=getattr(args, "ingest_event_batch_size", 5000),
        logfile_list=logfile_list,
    )

# ------------------------------------------------------------
# Step 2: Embeddings
# ------------------------------------------------------------

from embeddings.embed_runner import run_embedding

def cmd_embed(args):
    paths = get_paths(args)
    paths["output_dir"].mkdir(parents=True, exist_ok=True)
    run_embedding(
        events_path=str(paths["events"]),
        output_vectors_path=str(paths["embeddings"]),
        output_index_path=str(paths["index"]),
        embed_chunk_size=getattr(args, "embed_chunk_size", 12000),
        embed_batch_size=getattr(args, "embed_batch_size", 64),
        embed_device=getattr(args, "embed_device", "mps"),
    )

# ------------------------------------------------------------
# Step 3: Clustering
# ------------------------------------------------------------

from cluster.cluster_runner import run_clustering

def cmd_cluster(args):
    paths = get_paths(args)
    run_clustering(
        events_path=str(paths["events"]),
        embeddings_path=str(paths["embeddings"]),
        clusters_output_path=str(paths["clusters"]),
        event_cluster_map_output_path=str(paths["event_cluster_map"]),
        min_cluster_size=args.min_cluster_size,
        pca_dims=args.pca_dims,
        max_cluster_events=getattr(args, "max_cluster_events", 120000),
        cluster_overflow_mode=getattr(args, "cluster_overflow_mode", "downsample"),
        cluster_mode=getattr(args, "cluster_mode", "standard"),
    )

# ------------------------------------------------------------
# Step 4: Trigger Analysis
# ------------------------------------------------------------

from cluster.trigger_analysis import run_trigger_analysis

def cmd_trigger_analysis(args):
    paths = get_paths(args)
    run_trigger_analysis(
        events_path=str(paths["events"]),
        clusters_path=str(paths["clusters"]),
        event_cluster_map_path=str(paths["event_cluster_map"]),
        output_path=str(paths["trigger_stats"]),
    )

# ------------------------------------------------------------
# Step 5: Incident Detection
# ------------------------------------------------------------

from cluster.incident_detection import run_incident_detection
from cluster.incident_detection_v2 import run_incident_detection_v2
from tools.build_preincident_diagnostics import build_preincident_diagnostics, render_markdown


def cmd_incident_detection(args):
    paths = get_paths(args)
    incident_mode = getattr(args, "incident_mode", "v2")
    if incident_mode == "v2":
        out = run_incident_detection_v2(
            cluster_trigger_stats_path=str(paths["trigger_stats"]),
            output_path=str(paths["incidents"]),
            gap_seconds=getattr(args, "gap_seconds", 30),
            max_seeds=getattr(args, "max_seeds", 3),
            cluster_window_cap_seconds=getattr(args, "cluster_window_cap_seconds", 900),
            max_incident_duration_seconds=getattr(args, "max_incident_duration_seconds", 14400),
            episode_gap_seconds=getattr(args, "episode_gap_seconds", 120),
            max_episode_duration_seconds=getattr(args, "max_episode_duration_seconds", 1200),
            semantic_jaccard_threshold=getattr(args, "semantic_jaccard_threshold", 0.3),
            status_output_path=str(paths["incident_detection_status"]),
        )
    else:
        out = run_incident_detection(
            cluster_trigger_stats_path=str(paths["trigger_stats"]),
            output_path=str(paths["incidents"]),
            gap_seconds=getattr(args, "gap_seconds", 30),
            max_seeds=getattr(args, "max_seeds", 3),
            cluster_window_cap_seconds=getattr(args, "cluster_window_cap_seconds", 900),
            max_incident_duration_seconds=getattr(args, "max_incident_duration_seconds", 3600),
            status_output_path=str(paths["incident_detection_status"]),
        )
    return out


def cmd_validate_outputs(args) -> bool:
    """Run external validation script and write reports."""
    validation_script = PROJECT_ROOT / "validation" / "validate_pipeline_steps.py"
    outputs_dir_arg = getattr(args, "outputs_dir", None)
    outputs_dir = Path(outputs_dir_arg) if outputs_dir_arg else get_output_dir(args)
    raw_log_arg = getattr(args, "raw_log", None)
    cmd = [sys.executable, str(validation_script), "--outputs-dir", str(outputs_dir)]
    if raw_log_arg:
        cmd.extend(["--raw-log", str(raw_log_arg)])
    if getattr(args, "compat_v142", False):
        cmd.append("--compat-v142")
    paths = get_paths(args)
    cmd.extend(["--report-json", str(paths["validation_json"]), "--report-md", str(paths["validation_md"])])
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
    paths = get_paths(args)
    run_causal_analysis(
        incidents_path=str(paths["incidents"]),
        cluster_trigger_stats_path=str(paths["trigger_stats"]),
        graph_output_path=str(paths["graph"]),
        candidates_output_path=str(paths["candidates"]),
        event_cluster_map_path=str(paths["event_cluster_map"]),
        events_path=str(paths["events"]),
        grounded_events_output_path=str(paths["root_events"]),
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

    paths = get_paths(args)
    print("\n[STEP 7] building RCA JSON report")

    build_rca_report(
        incidents_path=str(paths["incidents"]),
        candidates_path=str(paths["candidates"]),
        root_events_path=str(paths["root_events"]),
        output_path=str(paths["rca_report"]),
    )

    print("[STEP 7] rendering Markdown report")

    render_report(
        incidents_path=str(paths["incidents"]),
        candidates_path=str(paths["candidates"]),
        grounded_events_path=str(paths["root_events"]),
        output_path=str(paths["report_md"]),
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
    paths = get_paths(args)
    print("\n[STEP 8] building evidence bundle")

    build_evidence_bundle(
        incidents_path=paths["incidents"],
        candidates_path=paths["candidates"],
        grounded_events_path=paths["root_events"],
        graph_path=paths["graph"],
        report_path=paths["rca_report"],
        events_path=paths["events"],
        output_path=paths["evidence_bundle"],
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
    paths = get_paths(args)
    print("\n[STEP 9] building detailed RCA JSON report")

    detailed_reports = build_detailed_report_json(
        base_report_path=paths["rca_report"],
        evidence_bundle_path=paths["evidence_bundle"],
        output_json_path=paths["detailed_report_json"],
    )

    print("[STEP 9] rendering detailed Markdown report")
    render_detailed_markdown(
        detailed_reports=detailed_reports,
        output_md_path=paths["detailed_report_md"],
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
    paths = get_paths(args)
    print("\n[STEP 10] building incident assertions")
    out = build_assertions(
        incidents_path=paths["incidents"],
        candidates_path=paths["candidates"],
        roots_path=paths["root_events"],
        evidence_bundle_path=paths["evidence_bundle"],
        output_path=paths["assertions"],
    )
    print(f"[STEP 10] incident assertions complete (incidents={len(out)})")

# ------------------------------------------------------------
# Diagnostics (no-incident helper)
# ------------------------------------------------------------

def cmd_preincident_diagnostics(args):
    paths = get_paths(args)
    diag = build_preincident_diagnostics(paths["output_dir"])
    paths["preincident_json"].write_text(json.dumps(diag, ensure_ascii=False, indent=2), encoding="utf-8")
    paths["preincident_md"].write_text(render_markdown(diag), encoding="utf-8")
    print(f"[PREINCIDENT] -> {paths['preincident_json']}")
    print(f"[PREINCIDENT] -> {paths['preincident_md']}")

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

    triage_ok = getattr(args, "triage", False) and (
        args.logfile or getattr(args, "triage_input_dir", None)
    )
    if (
        not args.logfile
        and not getattr(args, "logfile_list", None)
        and not triage_ok
    ):
        raise ValueError(
            "Either logfile, --logfile-list, or --triage with logfile (dir) / --triage-input-dir "
            "is required for 'all' command"
        )

    if getattr(args, "clean", False):
        clean_outputs()

    ensure_outputs()
    paths = get_paths(args)
    paths["output_dir"].mkdir(parents=True, exist_ok=True)

    for step_name, label, fn in PIPELINE_STEPS:
        print(f"\n[{step_name}] {label}")
        start = time.time()

        fn(args)

        duration = round(time.time() - start, 2)
        print(f"[{step_name}] completed in {duration}s")

        # If no incidents are detected, emit diagnostics and stop gracefully.
        if label == "incident_detection":
            incidents = []
            if paths["incidents"].exists():
                incidents = json.loads(paths["incidents"].read_text(encoding="utf-8"))
            if not incidents:
                print("\n[PIPELINE] no incidents detected, generating pre-incident diagnostics")
                cmd_preincident_diagnostics(args)
                print("\n[PIPELINE] running post-run validation")
                args.outputs_dir = paths["output_dir"]
                args.raw_log = args.logfile or getattr(args, "triage_input_dir", None)
                cmd_validate_outputs(args)
                print("[PIPELINE] stopping after diagnostics (no incident path)")
                print(f"Outputs available at: {paths['output_dir']}")
                return

    print("\n🎉 Pipeline complete")
    print(f"Outputs available at: {paths['output_dir']}")
    print("\n[PIPELINE] running post-run validation")
    args.outputs_dir = paths["output_dir"]
    args.raw_log = args.logfile or getattr(args, "triage_input_dir", None)
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

    def add_profile_args(sp):
        sp.add_argument("--pipeline-profile", choices=["v1", "v2"], default="v1")
        sp.add_argument("--outputs-root", default=str(DEFAULT_OUTPUTS_ROOT))

    def add_triage_args(sp):
        sp.add_argument(
            "--triage",
            action="store_true",
            help="Pre-scan logs with weighted failure-regex triage; ingest only selected files",
        )
        sp.add_argument(
            "--triage-input-dir",
            default=None,
            help="Directory to scan (default: logfile when it is a directory)",
        )
        sp.add_argument(
            "--triage-output-dir",
            default=None,
            help="Where to write selected_log_files.txt and triage_manifest.json "
            "(default: <profile output>/log_triage)",
        )
        sp.add_argument("--triage-top-n", type=int, default=40, help="Max files to keep after ranking")
        sp.add_argument(
            "--triage-min-weighted-score",
            type=int,
            default=10,
            help="Minimum triage score for a file to be eligible",
        )
        sp.add_argument(
            "--triage-max-lines-per-file",
            type=int,
            default=0,
            help="Cap lines read per file during triage (0 = full file)",
        )

    # ingest
    ingest = sub.add_parser("ingest", help="Parse raw logs into events.jsonl")
    add_profile_args(ingest)
    add_triage_args(ingest)
    ingest.add_argument("logfile", nargs="?", help="Path to raw logfile or directory")
    ingest.add_argument("--logfile-list", default=None, help="Path to newline-delimited file list")
    ingest.add_argument("--ingest-file-batch-size", type=int, default=20)
    ingest.add_argument("--ingest-event-batch-size", type=int, default=5000)
    ingest.set_defaults(func=cmd_ingest)

    # embed
    embed = sub.add_parser("embed", help="Generate embeddings")
    add_profile_args(embed)
    embed.add_argument("--embed-chunk-size", type=int, default=12000)
    embed.add_argument("--embed-batch-size", type=int, default=64)
    embed.add_argument("--embed-device", choices=["mps", "cpu"], default="mps")
    embed.set_defaults(func=cmd_embed)

    # cluster
    cluster = sub.add_parser("cluster", help="Cluster events into patterns")
    add_profile_args(cluster)
    cluster.add_argument("--min-cluster-size", type=int, default=15)
    cluster.add_argument("--pca-dims", type=int, default=256)
    cluster.add_argument("--max-cluster-events", type=int, default=120000)
    cluster.add_argument(
        "--cluster-overflow-mode",
        choices=["fail", "downsample"],
        default="downsample",
    )
    cluster.add_argument(
        "--cluster-mode",
        choices=["standard", "fast", "auto"],
        default="standard",
        help="standard=hdbscan/fallback, fast=minibatch kmeans, auto=fast on large runs",
    )
    cluster.set_defaults(func=cmd_cluster)

    # trigger analysis
    trigger = sub.add_parser("trigger_analysis", help="Compute trigger stats")
    add_profile_args(trigger)
    trigger.set_defaults(func=cmd_trigger_analysis)

    # incident detection
    detection = sub.add_parser("incident_detection", help="Detect incidents")
    add_profile_args(detection)
    detection.add_argument("--incident-mode", choices=["v1", "v2"], default="v2")
    detection.add_argument("--gap-seconds", type=int, default=30)
    detection.add_argument("--max-seeds", type=int, default=3)
    detection.add_argument("--semantic-jaccard-threshold", type=float, default=0.3)
    detection.add_argument("--cluster-window-cap-seconds", type=int, default=900)
    detection.add_argument("--episode-gap-seconds", type=int, default=120)
    detection.add_argument("--max-episode-duration-seconds", type=int, default=1200)
    detection.add_argument("--max-incident-duration-seconds", type=int, default=14400)
    detection.set_defaults(func=cmd_incident_detection)

    # causal analysis (Step 6)
    causal = sub.add_parser(
        "causal_analysis",
        help="Step 6: causal graph + root candidates + event grounding"
    )
    add_profile_args(causal)
    causal.set_defaults(func=cmd_causal_analysis)

    report = sub.add_parser("report", help="Step 7: Generate RCA report (JSON + Markdown)")
    add_profile_args(report)
    report.set_defaults(func=cmd_report)

    evidence = sub.add_parser(
        "evidence_bundle",help="Step 8: Build incident evidence bundle JSON")
    add_profile_args(evidence)
    evidence.set_defaults(func=cmd_evidence_bundle)

    detailed = sub.add_parser(
        "detailed_report",
        help="Step 9: Generate detailed RCA report (JSON + Markdown)"
    )
    add_profile_args(detailed)
    detailed.set_defaults(func=cmd_detailed_report)

    assertions = sub.add_parser(
        "incident_assertions",
        help="Step 10: Generate incident assertions JSON"
    )
    add_profile_args(assertions)
    assertions.set_defaults(func=cmd_incident_assertions)

    prediag = sub.add_parser(
        "preincident_diagnostics",
        help="Build diagnostics when incidents are not detected"
    )
    add_profile_args(prediag)
    prediag.set_defaults(func=cmd_preincident_diagnostics)

    validate = sub.add_parser(
        "validate",
        help="Run QA validation checks on output artifacts"
    )
    add_profile_args(validate)
    validate.add_argument("--outputs-dir", default=None)
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
    add_profile_args(allp)
    add_triage_args(allp)
    allp.add_argument("logfile", nargs="?", help="Path to raw logfile or directory")
    allp.add_argument("--logfile-list", default=None, help="Path to newline-delimited file list for ingest")
    allp.add_argument("--min-cluster-size", type=int, default=15)
    allp.add_argument("--pca-dims", type=int, default=256)
    allp.add_argument("--max-cluster-events", type=int, default=120000)
    allp.add_argument(
        "--cluster-overflow-mode",
        choices=["fail", "downsample"],
        default="downsample",
    )
    allp.add_argument(
        "--cluster-mode",
        choices=["standard", "fast", "auto"],
        default="standard",
        help="standard=hdbscan/fallback, fast=minibatch kmeans, auto=fast on large runs",
    )
    allp.add_argument("--gap-seconds", type=int, default=30)
    allp.add_argument("--max-seeds", type=int, default=3)
    allp.add_argument("--incident-mode", choices=["v1", "v2"], default="v2")
    allp.add_argument("--semantic-jaccard-threshold", type=float, default=0.3)
    allp.add_argument("--cluster-window-cap-seconds", type=int, default=900)
    allp.add_argument("--episode-gap-seconds", type=int, default=120)
    allp.add_argument("--max-episode-duration-seconds", type=int, default=1200)
    allp.add_argument("--max-incident-duration-seconds", type=int, default=14400)
    allp.add_argument("--ingest-file-batch-size", type=int, default=20)
    allp.add_argument("--ingest-event-batch-size", type=int, default=5000)
    allp.add_argument("--embed-chunk-size", type=int, default=12000)
    allp.add_argument("--embed-batch-size", type=int, default=64)
    allp.add_argument("--embed-device", choices=["mps", "cpu"], default="mps")
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