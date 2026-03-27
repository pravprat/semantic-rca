from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Any, Iterable, List


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
    out: List[dict] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                out.append(json.loads(line))
    return out


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

