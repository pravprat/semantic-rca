#############################################
# Step 2: Generate Embeddings
#############################################

from __future__ import annotations

import json
import os
from pathlib import Path
import numpy as np

## Imports from the Pipeline
from embeddings.embedder import Embedder
from embeddings.vector_store import VectorStore

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"

def ensure_outputs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def cmd_embed(args):
    ensure_outputs()
    events = load_events()
    texts = [e["embedding_text"] for e in events]

    emb = Embedder()
    res = emb.fit_transform(texts)

    vec_path = os.path.join(OUTPUT_DIR, "event_embeddings.npy")
    np.save(vec_path, res.vectors)

    index_path = os.path.join(OUTPUT_DIR, "event_index.json")

    meta = []

    for e, text in zip(events, texts):
        entry = {
            "event_id": e.get("event_id"),
            "timestamp": e.get("timestamp"),
            "service": e.get("service"),
            "severity": e.get("severity"),

            # useful causal metadata
            "actor": e.get("actor"),
            "verb": e.get("verb"),
            "resource": e.get("resource"),
            "response_code": e.get("response_code"),

            # embedding text for debugging / clustering inspection
            "embedding_text": text
        }

        meta.append(entry)

    with open(index_path, "w", encoding="utf-8") as f:
        json.dump(meta, f, ensure_ascii=False, indent=2)

    print(f"[embed] vectors={res.vectors.shape} -> {vec_path}")
    print(f"[embed] index -> {index_path}")
