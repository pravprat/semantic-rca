#log_reader

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional, Iterable, Union, List
from pathlib import Path
import json
import gzip
import re


# ---------------------------------------------------------
# Data Model
# ---------------------------------------------------------

@dataclass(frozen=True)
class RawRecord:
    raw: str
    json_obj: Optional[dict]
    source_file: Optional[str] = None


# ---------------------------------------------------------
# Timestamp Detection (FIXED: removed ^ anchor)
# ---------------------------------------------------------

TIMESTAMP_PATTERN = re.compile(
    r"""
    (
        \d{4}-\d{2}-\d{2}[\sT]\d{2}:\d{2}:\d{2}      # ISO logs
        |
        \d{6}\s+\d{6}                                # HDFS logs
        |
        -\s+\d+\s+\d{4}\.\d{2}\.\d{2}                 # BGL logs
    )
    """,
    re.VERBOSE
)


# ---------------------------------------------------------
# Log Reader
# ---------------------------------------------------------

class LogReader:

    # ---------- Public API ----------

    def iter_records(
        self,
        file_path: Union[str, Path],
        encoding: str = "utf-8",
        errors: str = "replace",
    ) -> Iterator[RawRecord]:
        with open(file_path, "r", encoding=encoding, errors=errors) as f:
            yield from self._iter_lines(f, Path(file_path).name)

    def iter_records_from_text(
        self,
        text: str,
        source_file: Optional[str] = None,
    ) -> Iterator[RawRecord]:
        yield from self._iter_lines(text.splitlines(), source_file)

    # ---------- Core Logic ----------

    def _iter_lines(
        self,
        lines: Iterable[str],
        source_file: Optional[str],
    ) -> Iterator[RawRecord]:

        buffer = []

        def flush():
            nonlocal buffer
            if not buffer:
                return None

            joined = "\n".join(buffer)
            buffer = []

            return RawRecord(
                raw=joined,
                json_obj=self._try_parse_json(joined),
                source_file=source_file,
            )

        for line in lines:
            line = line.rstrip("\n")

            if not line:
                continue

            # -------------------------------------------------
            # 1. JSON logs (highest priority)
            # -------------------------------------------------
            json_obj = self._try_parse_json(line)
            if json_obj is not None:
                rec = flush()
                if rec:
                    yield rec

                yield RawRecord(
                    raw=line,
                    json_obj=json_obj,
                    source_file=source_file,
                )
                continue

            # -------------------------------------------------
            # 2. K8s Audit CSV (CRITICAL FIX)
            # -------------------------------------------------
            if self._looks_like_k8s_audit(line):
                rec = flush()
                if rec:
                    yield rec

                yield RawRecord(
                    raw=line,
                    json_obj=None,
                    source_file=source_file,
                )
                continue

            # -------------------------------------------------
            # 3. Timestamp-based logs (multi-line support)
            # -------------------------------------------------
            if TIMESTAMP_PATTERN.search(line):
                rec = flush()
                if rec:
                    yield rec
                buffer = [line]
                continue

            # -------------------------------------------------
            # 4. Continuation lines
            # -------------------------------------------------
            if buffer:
                buffer.append(line)
            else:
                buffer = [line]

        # flush remaining buffer
        rec = flush()
        if rec:
            yield rec

    # ---------------------------------------------------------
    # Helpers
    # ---------------------------------------------------------

    def _looks_like_k8s_audit(self, line: str) -> bool:
        """
        Detect Kubernetes audit CSV logs.

        These logs:
        - have many comma-separated fields
        - contain 'ResponseStarted' or 'ResponseComplete'
        - contain 'system:' actor
        """
        return (
            line.count(",") >= 10
            and ("ResponseStarted" in line or "ResponseComplete" in line)
            and "system:" in line
        )

    @staticmethod
    def _try_parse_json(text: str) -> Optional[dict]:
        text = text.strip()
        if not text:
            return None

        # direct JSON
        if text.startswith("{") and text.endswith("}"):
            try:
                return json.loads(text)
            except Exception:
                return None

        # embedded JSON
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except Exception:
                pass

        return None


# ---------------------------------------------------------
# Directory / File Loader
# ---------------------------------------------------------

def iter_records_from_path(
    reader: LogReader,
    path: Union[str, Path],
) -> Iterator[RawRecord]:

    path = Path(path)

    if path.is_file():
        if path.suffixes[-2:] == [".log", ".gz"] or path.suffix == ".gz":
            with gzip.open(path, "rt", encoding="utf-8", errors="replace") as f:
                yield from reader._iter_lines(f, source_file=path.name)
        else:
            yield from reader.iter_records(path)
        return

    for file in iter_log_files(path):
        if file.suffixes[-2:] == [".log", ".gz"] or file.suffix == ".gz":
            with gzip.open(file, "rt", encoding="utf-8", errors="replace") as f:
                yield from reader._iter_lines(f, source_file=file.name)
        else:
            yield from reader.iter_records(file)


def iter_log_files(path: Union[str, Path]) -> List[Path]:
    root = Path(path)
    if root.is_file():
        return [root]

    files: List[Path] = []
    for file in root.rglob("*"):
        if not file.is_file():
            continue
        if file.name.startswith("."):
            continue
        # Accept common plain and gzipped log formats.
        if file.suffix in {".log", ".txt", ".json", ".out"}:
            files.append(file)
            continue
        if file.suffix == ".gz":
            files.append(file)
    return sorted(files)