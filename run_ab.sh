#!/usr/bin/env bash
set -euo pipefail

# Usage:
#   ./run_ab.sh "/path/to/k8s/logs" "/path/to/sb/folder" [outputs_root]
#
# Example:
#   ./run_ab.sh "/Users/me/data/k8s" "/Users/me/data/sb.CONTAP-670133" "outputs"

K8S_LOG_PATH="${1:-}"
SB_LOG_PATH="${2:-}"
OUTPUTS_ROOT="${3:-outputs}"

if [[ -z "${K8S_LOG_PATH}" || -z "${SB_LOG_PATH}" ]]; then
  echo "Usage: $0 <k8s_log_path> <sb_log_path> [outputs_root]"
  exit 1
fi

run_pipeline() {
  local profile="$1"
  local log_path="$2"

  echo ""
  echo "============================================================"
  echo "Running profile=${profile} log_path=${log_path}"
  echo "============================================================"

  local start_ts end_ts elapsed
  start_ts="$(date +%s)"

  python3 main.py all "${log_path}" \
    --pipeline-profile "${profile}" \
    --outputs-root "${OUTPUTS_ROOT}" \
    --incident-mode v2 \
    --cluster-mode auto \
    --embed-device mps

  python3 main.py validate \
    --pipeline-profile "${profile}" \
    --outputs-root "${OUTPUTS_ROOT}" \
    --raw-log "${log_path}"

  end_ts="$(date +%s)"
  elapsed="$((end_ts - start_ts))"
  echo "[${profile}] elapsed_seconds=${elapsed}"
}

run_pipeline "v1" "${K8S_LOG_PATH}"
run_pipeline "v2" "${SB_LOG_PATH}"

echo ""
echo "============================================================"
echo "A/B quick summary"
echo "============================================================"

python3 - <<'PY'
import json
from pathlib import Path

root = Path("outputs")
for profile in ("v1", "v2"):
    base = root / profile
    validation = base / "validation_report.json"
    incidents = base / "incidents.json"
    status = "MISSING"
    incidents_count = "n/a"
    overall = "n/a"
    if validation.exists():
        data = json.loads(validation.read_text(encoding="utf-8"))
        overall = data.get("overall_status", "n/a")
        status = "OK"
    if incidents.exists():
        items = json.loads(incidents.read_text(encoding="utf-8"))
        incidents_count = len(items) if isinstance(items, list) else "n/a"
    print(f"{profile}: validation={status} overall={overall} incidents={incidents_count} dir={base}")
PY

echo ""
echo "Done."
