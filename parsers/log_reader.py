# semantic-rca/parsers/log_reader.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional, List, Iterable, Union
from pathlib import Path
import json
import gzip


# =========================
# Data model
# =========================

@dataclass(frozen=True)
class RawRecord:
    """
    A raw record read from the log.
    'raw' is the original line(s) as a single string.
    If the record is valid JSON, 'json_obj' contains the parsed object.
    """
    raw: str
    json_obj: Optional[dict]
    source_file: Optional[str] = None


# =========================
# Core LogReader
# =========================

class LogReader:
    """
    Streams a log source and yields RawRecord items.

    Supports:
      - JSON-per-line logs (common in k8s)
      - Multiline records (stack traces) by grouping continuation lines
    """

    def __init__(self, continuation_prefixes: Optional[List[str]] = None):
        self.continuation_prefixes = continuation_prefixes or [
            "\t",
            "    ",
            "at ",
            "Caused by:",
            "Traceback",
        ]

    # ---------- Public APIs ----------

    def iter_records(
        self,
        file_path: Union[str, Path],
        encoding: str = "utf-8",
        errors: str = "replace",
    ) -> Iterator[RawRecord]:
        """
        Iterate RawRecords from a single .log file.
        """
        with open(file_path, "r", encoding=encoding, errors=errors) as f:
            yield from self._iter_lines(
                f,
                source_file=Path(file_path).name,
            )

    def iter_records_from_text(
        self,
        text: str,
        source_file: Optional[str] = None,
    ) -> Iterator[RawRecord]:
        """
        Iterate RawRecords from in-memory text.
        Used for .log.gz files.
        """
        lines = text.splitlines()
        yield from self._iter_lines(lines, source_file=source_file)

    # ---------- Internal parsing logic ----------

    def _iter_lines(
        self,
        lines: Iterable[str],
        source_file: Optional[str],
    ) -> Iterator[RawRecord]:
        buffer_lines: List[str] = []

        def flush_buffer() -> Optional[RawRecord]:
            nonlocal buffer_lines
            if not buffer_lines:
                return None
            joined = "\n".join(buffer_lines)
            buffer_lines = []
            return RawRecord(
                raw=joined,
                json_obj=self._try_parse_json(joined),
                source_file=source_file,
            )

        for line in lines:
            line = line.rstrip("\n")

            if not line:
                rec = flush_buffer()
                if rec:
                    yield rec
                continue

            obj = self._try_parse_json(line)
            if obj is not None:
                rec = flush_buffer()
                if rec:
                    yield rec
                yield RawRecord(
                    raw=line,
                    json_obj=obj,
                    source_file=source_file,
                )
                continue

            if buffer_lines and self._is_continuation(line):
                buffer_lines.append(line)
            else:
                rec = flush_buffer()
                if rec:
                    yield rec
                buffer_lines = [line]

        rec = flush_buffer()
        if rec:
            yield rec

    # ---------- Helpers ----------

    @staticmethod
    def _try_parse_json(text: str) -> Optional[dict]:
        text = text.strip()
        if not text:
            return None
        if not (text.startswith("{") and text.endswith("}")):
            return None
        try:
            return json.loads(text)
        except Exception:
            return None

    def _is_continuation(self, line: str) -> bool:
        s = line.lstrip()
        for p in self.continuation_prefixes:
            if s.startswith(p) or line.startswith(p):
                return True
        return False


# =========================
# Directory / gzip wrapper
# =========================

def iter_records_from_path(
    reader: LogReader,
    path: Union[str, Path],
    encoding: str = "utf-8",
    errors: str = "replace",
) -> Iterator[RawRecord]:
    """
    Iterate RawRecords from:
      - a single .log file
      - a single .log.gz file
      - a directory containing .log / .log.gz files
    """
    path = Path(path)

    if path.is_file():
        yield from _iter_file(reader, path, encoding, errors)
        return

    if not path.is_dir():
        raise ValueError(f"Invalid log source: {path}")

    for file in sorted(path.iterdir()):
        if file.suffix == ".log":
            yield from _iter_file(reader, file, encoding, errors)
        elif file.suffixes[-2:] == [".log", ".gz"]:
            yield from _iter_gz(reader, file, encoding, errors)


def _iter_file(
    reader: LogReader,
    file_path: Path,
    encoding: str,
    errors: str,
) -> Iterator[RawRecord]:
    yield from reader.iter_records(file_path, encoding=encoding, errors=errors)


def _iter_gz(
    reader: LogReader,
    file_path: Path,
    encoding: str,
    errors: str,
) -> Iterator[RawRecord]:
    with gzip.open(file_path, "rt", encoding=encoding, errors=errors) as f:
        text = f.read()
        yield from reader.iter_records_from_text(
            text,
            source_file=file_path.name,
        )