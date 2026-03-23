#publish_run.py

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

    if not OUTPUTS.exists():
        raise RuntimeError(f"Outputs directory does not exist: {OUTPUTS}")

    json_files = list(OUTPUTS.glob("**/*.json"))
    md_files = list(OUTPUTS.glob("**/*.md"))

    if not json_files and not md_files:
        raise RuntimeError("No JSON/MD artifacts found in outputs/ to publish")

    ts = datetime.utcnow().strftime("%Y%m%d_%H%M%S")
    run_dir = RESULT_REPO / f"run_{ts}"
    run_dir.mkdir(parents=True, exist_ok=False)

    print(f"Exporting run → {run_dir}")

    for f in json_files:
        shutil.copy2(f, run_dir / f.name)

    for f in md_files:
        shutil.copy2(f, run_dir / f.name)

    run(["git", "add", "."], cwd=RESULT_REPO)
    run(["git", "commit", "-m", f"SCRCA run {ts}"], cwd=RESULT_REPO)
    run(["git", "push", "origin", "main"], cwd=RESULT_REPO)

    print("Run published successfully")


if __name__ == "__main__":
    publish()