# embeddings/embed_runner.py

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Iterator

import numpy as np

from embeddings.embedder import Embedder
from event_io import count_events, iter_events, is_parquet_path


class _IndexWriter:
    def __init__(self, output_index_path: str):
        self.path = output_index_path
        self.as_parquet = is_parquet_path(output_index_path)
        self._json_f = None
        self._first = True
        self._pq_writer = None
        self._pa = None
        self._schema = None

    @staticmethod
    def _prepare_row(row: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(row)
        # Keep parquet schema stable across batches.
        rc = out.get("response_code")
        try:
            out["response_code"] = int(rc) if rc not in (None, "", "null") else None
        except Exception:
            out["response_code"] = None
        hc = out.get("http_class")
        out["http_class"] = str(hc) if hc not in (None, "") else None
        sem = out.get("semantic")
        if isinstance(sem, (dict, list)):
            out["semantic"] = json.dumps(sem, ensure_ascii=False, sort_keys=True)
        elif sem is None:
            out["semantic"] = None
        else:
            out["semantic"] = str(sem)
        return out

    def __enter__(self):
        if self.as_parquet:
            return self
        self._json_f = open(self.path, "w", encoding="utf-8")
        self._json_f.write("[\n")
        return self

    def write_rows(self, rows: List[Dict[str, Any]]) -> None:
        if not rows:
            return
        if self.as_parquet:
            prepared = [self._prepare_row(r) for r in rows]
            if self._pq_writer is None:
                import pyarrow as pa  # type: ignore
                import pyarrow.parquet as pq  # type: ignore
                self._pa = pa
                self._schema = pa.schema([
                    pa.field("event_id", pa.string()),
                    pa.field("timestamp", pa.string()),
                    pa.field("service", pa.string()),
                    pa.field("severity", pa.string()),
                    pa.field("actor", pa.string()),
                    pa.field("verb", pa.string()),
                    pa.field("resource", pa.string()),
                    pa.field("response_code", pa.int64()),
                    pa.field("http_class", pa.string()),
                    pa.field("status_family", pa.string()),
                    pa.field("failure_hint", pa.string()),
                    pa.field("path", pa.string()),
                    pa.field("stage", pa.string()),
                    pa.field("semantic", pa.string()),
                    pa.field("signature", pa.string()),
                    pa.field("embedding_text", pa.string()),
                ])
                table = pa.Table.from_pylist(prepared, schema=self._schema)
                self._pq_writer = pq.ParquetWriter(self.path, self._schema)
                self._pq_writer.write_table(table)
            else:
                self._pq_writer.write_table(self._pa.Table.from_pylist(prepared, schema=self._schema))
            return
        for row in rows:
            if not self._first:
                self._json_f.write(",\n")
            self._json_f.write(json.dumps(row, ensure_ascii=False))
            self._first = False

    def __exit__(self, exc_type, exc, tb):
        if self._pq_writer is not None:
            self._pq_writer.close()
            self._pq_writer = None
        if self._json_f is not None:
            self._json_f.write("\n]\n")
            self._json_f.close()
            self._json_f = None


def build_index_entry(e: Dict[str, Any], text: str) -> Dict[str, Any]:
    return {
        # identity
        "event_id": e.get("event_id"),
        "timestamp": e.get("timestamp"),
        "service": e.get("service"),
        "severity": e.get("severity"),
        # causal fields
        "actor": e.get("actor"),
        "verb": e.get("verb"),
        "resource": e.get("resource"),
        "response_code": e.get("response_code"),
        "http_class": e.get("http_class"),
        "status_family": e.get("status_family"),
        "failure_hint": e.get("failure_hint"),
        "path": e.get("path"),
        # structured signal
        "stage": e.get("stage"),
        # semantic enrichment
        "semantic": e.get("semantic"),
        "signature": e.get("signature"),
        # embedding reference
        "embedding_text": text,
    }


def run_embedding(
    events_path: str,
    output_vectors_path: str,
    output_index_path: str,
    embed_chunk_size: int = 10000,
    embed_batch_size: int = 64,
    embed_device: str = "mps",
) -> None:
    total_events = count_events(events_path)
    if total_events == 0:
        np.save(output_vectors_path, np.zeros((0, 0), dtype=np.float32))
        with open(output_index_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        print(f"[embed] no events found -> {events_path}")
        return

    vectors_tmp_path = str(Path(output_vectors_path).with_suffix(".mmap"))
    emb = Embedder(
        encode_batch_size=embed_batch_size,
        device=embed_device,
    )
    memmap: np.memmap | None = None
    vector_dim = 0
    write_row = 0
    chunk_id = 0
    index_written = 0

    with _IndexWriter(output_index_path) as index_writer:
        for events in iter_events(events_path, chunk_size=max(1, embed_chunk_size)):
            chunk_id += 1
            texts = [e.get("embedding_text", "") for e in events]
            res = emb.fit_transform(texts)

            if memmap is None:
                vector_dim = int(res.dim)
                memmap = np.memmap(
                    vectors_tmp_path,
                    mode="w+",
                    dtype=np.float32,
                    shape=(total_events, vector_dim),
                )

            chunk_rows = res.vectors.shape[0]
            memmap[write_row:write_row + chunk_rows] = res.vectors
            write_row += chunk_rows

            index_rows = []
            for e, text in zip(events, texts):
                index_rows.append(build_index_entry(e, text))
            index_writer.write_rows(index_rows)
            index_written += len(index_rows)

            print(
                f"[embed] chunk={chunk_id} rows={chunk_rows} "
                f"processed={write_row}/{total_events}"
            )

    if memmap is None:
        raise RuntimeError("[embed] failed to initialize vector memmap")

    memmap.flush()
    vectors = np.memmap(
        vectors_tmp_path,
        mode="r",
        dtype=np.float32,
        shape=(total_events, vector_dim),
    )
    np.save(output_vectors_path, vectors)
    del vectors
    os.remove(vectors_tmp_path)

    print(f"[embed] vectors=({total_events}, {vector_dim}) -> {output_vectors_path}")
    print(f"[embed] index -> {output_index_path}")
    print(f"[embed] index_rows={index_written}")