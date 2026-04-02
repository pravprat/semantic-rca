from __future__ import annotations

import json
import sys
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List

ROOT_DIR = Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from event_io import load_events, is_parquet_path


@dataclass
class CheckResult:
    name: str
    compared: str
    passed: bool
    details: str


def load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> List[dict]:
    return load_events(path)


def load_index(path: Path) -> List[dict]:
    if is_parquet_path(path):
        return load_events(path)
    return load_json(path)


def file_exists(path: Path) -> bool:
    return path.exists() and path.is_file()


def pass_fail(flag: bool) -> str:
    return "PASS" if flag else "FAIL"


def print_results(step_name: str, results: Iterable[CheckResult]) -> bool:
    print(f"\n=== {step_name} ===")
    ok = True
    for r in results:
        status = pass_fail(r.passed)
        print(f"[{status}] {r.name}")
        print(f"  Compared: {r.compared}")
        print(f"  Result  : {r.details}")
        ok = ok and r.passed
    print(f"Step status: {pass_fail(ok)}")
    return ok

