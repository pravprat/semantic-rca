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


_JSON_PARQUET_FIELDS = ("structured_fields", "semantic", "redactions")


def _coerce_response_code(v: Any) -> int | None:
    if v in (None, "", "null"):
        return None
    try:
        return int(v)
    except Exception:
        return None


def _prepare_row_for_parquet(row: Dict[str, Any]) -> Dict[str, Any]:
    """
    Make parquet rows schema-stable across batches.
    - Keep top-level scalar columns consistently typed.
    - Store nested variable dicts as JSON strings to avoid struct drift.
    """
    out = normalize_event_row(dict(row))
    out["response_code"] = _coerce_response_code(out.get("response_code"))
    hc = out.get("http_class")
    out["http_class"] = str(hc) if hc not in (None, "") else None
    for fld in _JSON_PARQUET_FIELDS:
        val = out.get(fld)
        if isinstance(val, (dict, list)):
            out[fld] = json.dumps(val, ensure_ascii=False, sort_keys=True)
        elif val is None:
            out[fld] = None
        else:
            out[fld] = str(val)
    return out


def _restore_row_from_parquet(row: Dict[str, Any]) -> Dict[str, Any]:
    out = normalize_event_row(dict(row))
    out["response_code"] = _coerce_response_code(out.get("response_code"))
    for fld in _JSON_PARQUET_FIELDS:
        val = out.get(fld)
        if isinstance(val, str) and val:
            try:
                out[fld] = json.loads(val)
            except Exception:
                # Keep raw text if value is not valid JSON.
                pass
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
                yield [_restore_row_from_parquet(r) for r in rows]
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
        self._schema = None

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        pa, pq = _load_pyarrow()
        prepared = [_prepare_row_for_parquet(r) for r in rows]
        if self._writer is None:
            self._pa = pa
            # Use an explicit schema so null-only first batches do not lock
            # response_code/http_class to null type.
            self._schema = pa.schema([
                pa.field("event_id", pa.string()),
                pa.field("source_type", pa.string()),
                pa.field("timestamp", pa.string()),
                pa.field("severity", pa.string()),
                pa.field("service", pa.string()),
                pa.field("actor", pa.string()),
                pa.field("verb", pa.string()),
                pa.field("resource", pa.string()),
                pa.field("path", pa.string()),
                pa.field("stage", pa.string()),
                pa.field("response_code", pa.int64()),
                pa.field("http_class", pa.string()),
                pa.field("status_family", pa.string()),
                pa.field("failure_hint", pa.string()),
                pa.field("raw_text", pa.string()),
                pa.field("normalized_text", pa.string()),
                pa.field("embedding_text", pa.string()),
                pa.field("structured_fields", pa.string()),
                pa.field("redactions", pa.string()),
                pa.field("semantic", pa.string()),
                pa.field("signature", pa.string()),
            ])
            table = pa.Table.from_pylist(prepared, schema=self._schema)
            self._writer = pq.ParquetWriter(str(self.output_path), self._schema)
            self._writer.write_table(table)
            return
        table = self._pa.Table.from_pylist(prepared, schema=self._schema)
        self._writer.write_table(table)

    def close(self) -> None:
        if self._writer is not None:
            self._writer.close()
            self._writer = None
