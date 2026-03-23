# embeddings/embed_runner.py

from __future__ import annotations

import json
from typing import List, Dict, Any

import numpy as np

from embeddings.embedder import Embedder


def load_events(events_path: str) -> List[Dict[str, Any]]:
    events = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    return events


def run_embedding(
    events_path: str,
    output_vectors_path: str,
    output_index_path: str,
) -> None:

    events = load_events(events_path)

    texts = [e["embedding_text"] for e in events]

    emb = Embedder()
    res = emb.fit_transform(texts)

    # ---- Save vectors ------------------------------------------------
    np.save(output_vectors_path, res.vectors)

    # ---- Build index -------------------------------------------------
    meta: List[Dict[str, Any]] = []

    for e, text in zip(events, texts):
        entry = {
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

            # structured signal
            "stage": e.get("stage"),

            # semantic enrichment
            "semantic": e.get("semantic"),
            "signature": e.get("signature"),

            # embedding reference
            "embedding_text": text,
        }

        meta.append(entry)

    # ---- Save index --------------------------------------------------
    with open(output_index_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[embed] vectors={res.vectors.shape} -> {output_vectors_path}")
    print(f"[embed] index -> {output_index_path}")