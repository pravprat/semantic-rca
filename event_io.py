from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Dict, Iterator, List


NORMALIZED_FIELDS = [
    "event_id",
    "timestamp",
    "service",
    "severity",
    "actor",
    "verb",
    "resource",
    "response_code",
    "http_class",
    "status_family",
    "failure_hint",
    "path",
    "stage",
    "semantic",
    "signature",
    "structured_fields",
    "raw_text",
]


def normalize_event_row(row: Dict[str, Any]) -> Dict[str, Any]:
    out = dict(row)
    for k in NORMALIZED_FIELDS:
        out.setdefault(k, None)
    return out


def _load_pyarrow():
    try:
        import pyarrow as pa  # type: ignore
        import pyarrow.parquet as pq  # type: ignore
    except Exception as exc:
        raise RuntimeError(
            "Parquet support requires pyarrow. Install with: pip install pyarrow"
        ) from exc
    return pa, pq


def is_parquet_path(path: str | Path) -> bool:
    return str(path).lower().endswith(".parquet")


def count_events(path: str | Path) -> int:
    p = Path(path)
    if is_parquet_path(p):
        _, pq = _load_pyarrow()
        pf = pq.ParquetFile(str(p))
        return int(pf.metadata.num_rows) if pf.metadata else 0
    total = 0
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                total += 1
    return total


def iter_events(path: str | Path, chunk_size: int = 10000) -> Iterator[List[Dict[str, Any]]]:
    p = Path(path)
    chunk_size = max(1, int(chunk_size))
    if is_parquet_path(p):
        _, pq = _load_pyarrow()
        pf = pq.ParquetFile(str(p))
        for batch in pf.iter_batches(batch_size=chunk_size):
            rows = batch.to_pylist()
            if rows:
                yield [normalize_event_row(r) for r in rows]
        return

    chunk: List[Dict[str, Any]] = []
    with p.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            chunk.append(normalize_event_row(json.loads(line)))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
    if chunk:
        yield chunk


def load_events(path: str | Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    for chunk in iter_events(path, chunk_size=50000):
        rows.extend(chunk)
    return rows


class EventParquetBatchWriter:
    def __init__(self, output_path: str | Path):
        self.output_path = Path(output_path)
        self._writer = None
        self._pa = None

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        pa, pq = _load_pyarrow()
        if self._writer is None:
            self._pa = pa
            table = pa.Table.from_pylist(rows)
            self._writer = pq.ParquetWriter(str(self.output_path), table.schema)
            self._writer.write_table(table)
            return
        table = self._pa.Table.from_pylist(rows)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
