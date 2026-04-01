# semantic-rca/embeddings/embedder.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List
import numpy as np

import os
os.environ["HF_HOME"] = "./hf_cache"

@dataclass(frozen=True)
class EmbeddingResult:
    vectors: np.ndarray
    dim: int


class Embedder:
    """
    Semantic embedder using sentence-transformers.

    Model:
        sentence-transformers/all-MiniLM-L6-v2

    Produces 384-dim embeddings suitable for:
        - clustering
        - similarity search
        - graph construction
    """


    def __init__(
        self,
        model_name: str = "sentence-transformers/all-MiniLM-L6-v2",
        encode_batch_size: int = 32,
        device: str = "mps",
    ):
        try:

            from sentence_transformers import SentenceTransformer

        except Exception as e:
            raise RuntimeError(
                "sentence-transformers is required. Install with:\n"
                "pip install sentence-transformers"
            ) from e

        self.model = SentenceTransformer(model_name)
        self.encode_batch_size = max(1, int(encode_batch_size))
        self.device = device

    def fit_transform(self, texts: List[str]) -> EmbeddingResult:
        """
        For compatibility with the previous interface.
        SentenceTransformer does not need fitting.
        """
        return self._encode(texts)

    def transform(self, texts: List[str]) -> EmbeddingResult:
        return self._encode(texts)

    def _encode(self, texts: List[str]) -> EmbeddingResult:

        if not texts:
            return EmbeddingResult(vectors=np.zeros((0, 0), dtype=np.float32), dim=0)

        CHUNK = 5000
        all_vecs = []

        for i in range(0, len(texts), CHUNK):
            batch = texts[i:i + CHUNK]

            vecs = self.model.encode(
                batch,
                batch_size=self.encode_batch_size,
                device=self.device,
                convert_to_numpy=True,
                show_progress_bar=False,
                normalize_embeddings=True
            ).astype(np.float32)

            all_vecs.append(vecs)

        vectors = np.vstack(all_vecs)

        return EmbeddingResult(
            vectors=vectors,
            dim=vectors.shape[1]
        )