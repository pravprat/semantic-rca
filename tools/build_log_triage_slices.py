#!/usr/bin/env python3
from __future__ import annotations

import argparse
import gzip
import json
import re
from dataclasses import dataclass, asdict
from pathlib import Path
from typing import Dict, Iterable, List, Tuple


FAILURE_PATTERNS: List[Tuple[re.Pattern[str], int, str]] = [
    (re.compile(r"\bforbidden\b", re.IGNORECASE), 6, "forbidden"),
    (re.compile(r"\bunauthorized\b", re.IGNORECASE), 6, "unauthorized"),
    (re.compile(r"\bpermission denied\b", re.IGNORECASE), 6, "permission_denied"),
    (re.compile(r"\btimeout\b|\bdeadline exceeded\b|\btimed out\b", re.IGNORECASE), 5, "timeout"),
    (re.compile(r"\bconnection refused\b|\bconnection reset\b", re.IGNORECASE), 5, "connection"),
    (re.compile(r"\bexception\b|\bpanic\b|\btraceback\b", re.IGNORECASE), 5, "exception"),
    (re.compile(r"\berror\b|\bfatal\b|\bfailed\b", re.IGNORECASE), 4, "error"),
    (re.compile(r"\bstatus(?:_code)?[\"'=:\s]+([45]\d\d)\b", re.IGNORECASE), 5, "status_code_text"),
    (re.compile(r"\bresponse.*\b([45]\d\d)\b", re.IGNORECASE), 4, "response_code_text"),
    (re.compile(r"\"code\"\s*:\s*([45]\d\d)\b"), 5, "json_code"),
    (re.compile(r"\"status\"\s*:\s*\"Failure\""), 4, "json_failure_status"),
]


TEXT_EXTS = {".log", ".txt", ".json", ".jsonl", ".out", ".gz"}


@dataclass
class FileScore:
    path: str
    lines: int
    matched_lines: int
    weighted_score: int
    density: float
    top_signals: List[Dict[str, int]]


def _iter_files(root: Path) -> Iterable[Path]:
    for p in root.rglob("*"):
        if not p.is_file():
            continue
        if p.suffix.lower() in TEXT_EXTS or p.name.endswith(".log.gz"):
            yield p


def _update_signal_counts(text: str, signal_counts: Dict[str, int]) -> Tuple[int, bool]:
    line_score = 0
    matched = False
    for pattern, weight, label in FAILURE_PATTERNS:
        if pattern.search(text):
            line_score += weight
            signal_counts[label] = signal_counts.get(label, 0) + 1
            matched = True
    return line_score, matched


def _score_file(path: Path, max_lines_per_file: int) -> FileScore:
    line_count = 0
    matched_lines = 0
    weighted_score = 0
    signal_counts: Dict[str, int] = {}

    if path.suffix.lower() == ".gz":
        fobj = gzip.open(path, "rt", encoding="utf-8", errors="replace")
    else:
        fobj = path.open("r", encoding="utf-8", errors="replace")

    with fobj as f:
        for line in f:
            line_count += 1
            if max_lines_per_file > 0 and line_count > max_lines_per_file:
                break

            ls, matched = _update_signal_counts(line, signal_counts)
            weighted_score += ls
            if matched:
                matched_lines += 1

    density = (matched_lines / line_count) if line_count else 0.0
    top_signals = [
        {"signal": k, "count": v}
        for k, v in sorted(signal_counts.items(), key=lambda kv: kv[1], reverse=True)[:8]
    ]
    return FileScore(
        path=str(path),
        lines=line_count,
        matched_lines=matched_lines,
        weighted_score=weighted_score,
        density=round(density, 6),
        top_signals=top_signals,
    )


def _rank_scores(scores: List[FileScore]) -> List[FileScore]:
    return sorted(
        scores,
        key=lambda s: (s.weighted_score, s.matched_lines, s.density),
        reverse=True,
    )


def build_triage(
    input_dir: Path,
    output_dir: Path,
    top_n: int,
    min_weighted_score: int,
    max_lines_per_file: int,
) -> Dict[str, object]:
    scores: List[FileScore] = []
    for p in _iter_files(input_dir):
        sc = _score_file(p, max_lines_per_file=max_lines_per_file)
        if sc.weighted_score >= min_weighted_score:
            scores.append(sc)

    ranked = _rank_scores(scores)
    selected = ranked[: max(0, top_n)]

    output_dir.mkdir(parents=True, exist_ok=True)
    selected_paths_file = output_dir / "selected_log_files.txt"
    manifest_file = output_dir / "triage_manifest.json"

    with selected_paths_file.open("w", encoding="utf-8") as f:
        for row in selected:
            f.write(f"{row.path}\n")

    manifest = {
        "input_dir": str(input_dir),
        "files_scanned": len(list(_iter_files(input_dir))),
        "files_meeting_threshold": len(ranked),
        "files_selected": len(selected),
        "selection_criteria": {
            "top_n": top_n,
            "min_weighted_score": min_weighted_score,
            "max_lines_per_file": max_lines_per_file,
        },
        "selected_files": [asdict(r) for r in selected],
    }
    manifest_file.write_text(json.dumps(manifest, ensure_ascii=False, indent=2), encoding="utf-8")
    return manifest


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Identify failure-signal-heavy log files and output selected filenames."
    )
    p.add_argument("--input-dir", required=True, help="Root directory containing raw logs")
    p.add_argument(
        "--output-dir",
        default="reports/log_triage",
        help="Output directory for selected filenames and manifest",
    )
    p.add_argument("--top-n", type=int, default=40, help="Number of files to select")
    p.add_argument(
        "--min-weighted-score",
        type=int,
        default=10,
        help="Minimum score for file to be eligible",
    )
    p.add_argument(
        "--max-lines-per-file",
        type=int,
        default=0,
        help="Optional cap per file (0 means full file scan)",
    )
    return p.parse_args()


def main() -> None:
    args = parse_args()
    manifest = build_triage(
        input_dir=Path(args.input_dir),
        output_dir=Path(args.output_dir),
        top_n=args.top_n,
        min_weighted_score=args.min_weighted_score,
        max_lines_per_file=args.max_lines_per_file,
    )
    print(
        f"[log_triage] selected={manifest['files_selected']} "
        f"from eligible={manifest['files_meeting_threshold']}"
    )
    print(f"[log_triage] paths -> {Path(args.output_dir) / 'selected_log_files.txt'}")
    print(f"[log_triage] manifest -> {Path(args.output_dir) / 'triage_manifest.json'}")


if __name__ == "__main__":
    main()

