#!/usr/bin/env python3
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path


PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUTS_DIR = PROJECT_ROOT / "outputs"


def run(cmd: list[str], step_name: str) -> None:
    print(f"\n========== {step_name} ==========")
    print("Running:", " ".join(cmd))
    result = subprocess.run(cmd, cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print(f"❌ {step_name} FAILED")
        sys.exit(result.returncode)
    print(f"✅ {step_name} COMPLETE")


def clean_outputs() -> None:
    if OUTPUTS_DIR.exists():
        print(f"\n🧹 Cleaning outputs directory: {OUTPUTS_DIR}")
        shutil.rmtree(OUTPUTS_DIR)
    OUTPUTS_DIR.mkdir(parents=True, exist_ok=True)
    print("✅ outputs/ cleaned")


def main() -> None:
    print("\n=== Semantic RCA Full Pipeline ===")

    clean_outputs()

    run(["python", "main.py", "ingest", "log/"], "INGEST")
    run(["python", "main.py", "embed"], "EMBED")
    run(
        [
            "python",
            "main.py",
            "cluster",
            "--pca-dims", "128",
            "--min-cluster-size", "3",
        ],
        "CLUSTER",
    )
    run(["python", "main.py", "trigger_analysis"], "TRIGGER ANALYSIS")
    run(["python", "main.py", "incident_detection"], "INCIDENT DETECTION")
    run(["python", "main.py", "graph"], "GRAPH")
    run(["python", "main.py", "incident_rca"], "INCIDENT RCA")
    run(["python", "main.py", "report"], "REPORT")
    run(["python", "main.py", "rca_explain"], "RCA EXPLAIN")
    run(["python", "main.py", "evidence"], "EVIDENCE")
    run(["python", "main.py", "incident_graph"], "INCIDENT GRAPH")
    run(["python", "main.py", "llm_summary"], "LLM SUMMARY")

    print("\n🎉 PIPELINE COMPLETE")
    print("Artifacts in:", OUTPUTS_DIR)

    print("\n========== PUBLISH RUN ==========")
    result = subprocess.run(["python", "tools/publish_run.py"], cwd=PROJECT_ROOT)
    if result.returncode != 0:
        print("❌ PUBLISH RUN FAILED")
        sys.exit(result.returncode)
    print("✅ PUBLISH RUN COMPLETE")


if __name__ == "__main__":
    main()