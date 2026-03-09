# semantic-rca/embeddings/vector_store.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Tuple, Optional
import numpy as np


@dataclass
class Neighbor:
    index: int
    score: float  # cosine similarity


class VectorStore:
    """
    Optional FAISS; falls back to sklearn NearestNeighbors.
    Stores vectors in-memory; persists separately via numpy save.
    """

    def __init__(self, use_faiss_if_available: bool = True):
        self.use_faiss_if_available = use_faiss_if_available
        self._vectors: Optional[np.ndarray] = None
        self._faiss_index = None
        self._nn = None

    def build(self, vectors: np.ndarray):
        self._vectors = vectors.astype(np.float32)

        if self.use_faiss_if_available:
            try:
                import faiss  # type: ignore
                dim = self._vectors.shape[1]
                index = faiss.IndexFlatIP(dim)  # cosine if vectors normalized
                index.add(self._vectors)
                self._faiss_index = index
                self._nn = None
                return
            except Exception:
                self._faiss_index = None

        # fallback
        try:
            from sklearn.neighbors import NearestNeighbors
        except Exception as e:
            raise RuntimeError("Need either faiss or scikit-learn for nearest neighbor search.") from e

        nn = NearestNeighbors(metric="cosine", algorithm="auto")
        nn.fit(self._vectors)
        self._nn = nn

    def query(self, vector: np.ndarray, top_k: int = 10) -> List[Neighbor]:
        if self._vectors is None:
            raise RuntimeError("VectorStore not built")

        v = vector.astype(np.float32).reshape(1, -1)

        if self._faiss_index is not None:
            # inner product similarity for normalized vectors == cosine similarity
            scores, idxs = self._faiss_index.search(v, top_k)
            out: List[Neighbor] = []
            for i, s in zip(idxs[0].tolist(), scores[0].tolist()):
                out.append(Neighbor(index=int(i), score=float(s)))
            return out

        if self._nn is None:
            raise RuntimeError("No index available")

        # sklearn cosine distance -> convert to similarity
        distances, indices = self._nn.kneighbors(v, n_neighbors=top_k)
        out = []
        for i, d in zip(indices[0].tolist(), distances[0].tolist()):
            out.append(Neighbor(index=int(i), score=float(1.0 - d)))
        return out