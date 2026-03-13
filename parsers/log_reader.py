from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator, Optional, Iterable, Union
from pathlib import Path
import json
import gzip
import re


@dataclass(frozen=True)
class RawRecord:
    raw: str
    json_obj: Optional[dict]
    source_file: Optional[str] = None


TIMESTAMP_PATTERN = re.compile(
    r"""
    ^
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

class LogReader:
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

            if TIMESTAMP_PATTERN.search(line):
                rec = flush()
                if rec:
                    yield rec
                buffer = [line]
                continue

            if buffer:
                buffer.append(line)
            else:
                buffer = [line]

        rec = flush()
        if rec:
            yield rec

    @staticmethod
    def _try_parse_json(text: str) -> Optional[dict]:
        text = text.strip()
        if not text:
            return None

        if text.startswith("{") and text.endswith("}"):
            try:
                return json.loads(text)
            except Exception:
                return None

        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            try:
                return json.loads(text[start:end + 1])
            except Exception:
                pass

        return None


def iter_records_from_path(
    reader: LogReader,
    path: Union[str, Path],
) -> Iterator[RawRecord]:
    path = Path(path)

    if path.is_file():
        yield from reader.iter_records(path)
        return

    for file in sorted(path.iterdir()):
        if file.suffix == ".log":
            yield from reader.iter_records(file)

        elif file.suffixes[-2:] == [".log", ".gz"]:
            with gzip.open(file, "rt", encoding="utf-8", errors="replace") as f:
                yield from reader._iter_lines(
                    f,
                    source_file=file.name,
                )