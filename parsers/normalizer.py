#/parsers/normalizer.py

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Any, Optional


UUID_RE = re.compile(
    r"\b[0-9a-fA-F]{8}-[0-9a-fA-F\-]{27}\b"
)
IP_RE = re.compile(r"\b(\d{1,3}\.){3}\d{1,3}\b")
HEX_RE = re.compile(r"\b0x[0-9a-fA-F]+\b")
LONG_NUM_RE = re.compile(r"\b\d{6,}\b")
PATH_RE = re.compile(r"(/[\w\-.]+)+")


@dataclass(frozen=True)
class NormalizedText:
    normalized: str
    redactions: Dict[str, int]


class Normalizer:
    def normalize_text(self, text: str) -> NormalizedText:
        redactions: Dict[str, int] = {}

        def apply(pattern, repl, key, t):
            matches = pattern.findall(t)
            if matches:
                redactions[key] = len(matches)
            return pattern.sub(repl, t)

        t = text
        t = apply(UUID_RE, "<uuid>", "uuid", t)
        t = apply(IP_RE, "<ip>", "ip", t)
        t = apply(HEX_RE, "<hex>", "hex", t)
        t = apply(LONG_NUM_RE, "<num>", "num", t)
        t = apply(PATH_RE, "<path>", "path", t)

        t = t.replace("\r", " ")
        t = re.sub(r"\s+", " ", t)
        t = t.strip().lower()

        return NormalizedText(t, redactions)

    def normalize_fields(self, fields: Dict[str, Any]):
        out = {}
        for k, v in fields.items():
            if isinstance(v, str):
                out[k] = self.normalize_text(v).normalized
            else:
                out[k] = v
        return out

    @staticmethod
    @staticmethod
    def build_embedding_text(
            normalized_text: str,
            service: Optional[str],
            severity: Optional[str],
            key_fields: Optional[Dict[str, Any]] = None,
    ):
        parts = []

        if service:
            parts.append(f"service: {service}")

        if severity:
            parts.append(f"severity: {severity}")

        # ONLY include stable behavioral fields
        if key_fields:
            for k in ["verb", "resource", "http_class"]:
                v = key_fields.get(k)
                if v is not None:
                    parts.append(f"{k}: {v}")

        # IMPORTANT: only include message if explicitly meaningful
        if normalized_text:
            parts.append(f"message: {normalized_text}")

        return " | ".join(parts)