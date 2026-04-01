# embeddings/embed_runner.py

from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict, Any, Iterator

import numpy as np

from embeddings.embedder import Embedder


def iter_event_chunks(events_path: str, chunk_size: int) -> Iterator[List[Dict[str, Any]]]:
    chunk: List[Dict[str, Any]] = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            chunk.append(json.loads(line))
            if len(chunk) >= chunk_size:
                yield chunk
                chunk = []
    if chunk:
        yield chunk


def count_events(events_path: str) -> int:
    count = 0
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            if line.strip():
                count += 1
    return count


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
) -> None:
    total_events = count_events(events_path)
    if total_events == 0:
        np.save(output_vectors_path, np.zeros((0, 0), dtype=np.float32))
        with open(output_index_path, "w", encoding="utf-8") as f:
            json.dump([], f, ensure_ascii=False, indent=2)
        print(f"[embed] no events found -> {events_path}")
        return

    vectors_tmp_path = str(Path(output_vectors_path).with_suffix(".mmap"))
    emb = Embedder()
    memmap: np.memmap | None = None
    vector_dim = 0
    write_row = 0
    chunk_id = 0
    index_written = 0

    with open(output_index_path, "w", encoding="utf-8") as index_f:
        index_f.write("[\n")
        first_index_entry = True

        for events in iter_event_chunks(events_path, chunk_size=max(1, embed_chunk_size)):
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

            for e, text in zip(events, texts):
                entry_json = json.dumps(build_index_entry(e, text), ensure_ascii=False)
                if not first_index_entry:
                    index_f.write(",\n")
                index_f.write(entry_json)
                first_index_entry = False
                index_written += 1

            print(
                f"[embed] chunk={chunk_id} rows={chunk_rows} "
                f"processed={write_row}/{total_events}"
            )

        index_f.write("\n]\n")

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