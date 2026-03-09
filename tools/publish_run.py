import os
import shutil
import subprocess
from datetime import datetime
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OUTPUTS = ROOT / "outputs"

RESULT_REPO = Path.home() / "PycharmProjects" / "semantic-rca-runs"


def run(cmd, cwd=None):
    subprocess.run(cmd, cwd=cwd, check=True)


def publish():

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")

    run_dir = RESULT_REPO / f"run_{ts}"

    run_dir.mkdir(parents=True, exist_ok=True)

    print(f"Exporting run → {run_dir}")

    for f in OUTPUTS.glob("**/*.json"):
        shutil.copy(f, run_dir / f.name)

    for f in OUTPUTS.glob("**/*.md"):
        shutil.copy(f, run_dir / f.name)

    run(["git", "add", "."], cwd=RESULT_REPO)

    run(["git", "commit", "-m", f"SCRCA run {ts}"], cwd=RESULT_REPO)

    run(["git", "push", "origin", "main"], cwd=RESULT_REPO)

    print("Run published successfully")


if __name__ == "__main__":
    publish()