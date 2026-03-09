# semantic-rca/parsers/normalizer.py
from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Dict, Any, Optional, Tuple


_UUID_RE = re.compile(r"\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[1-5][0-9a-fA-F]{3}-[89abAB][0-9a-fA-F]{3}-[0-9a-fA-F]{12}\b")
_HEX_RE = re.compile(r"\b0x[0-9a-fA-F]+\b")
_IP_RE = re.compile(r"\b(\d{1,3}\.){3}\d{1,3}\b")
_LONG_NUM_RE = re.compile(r"\b\d{6,}\b")
_TS_ISO_RE = re.compile(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b")
_PATH_RE = re.compile(r"(/[\w\-.]+)+")
_POD_HASH_RE = re.compile(r"\b([a-z0-9-]+)-[a-f0-9]{8,}\b")


@dataclass(frozen=True)
class NormalizedText:
    normalized: str
    redactions: Dict[str, int]


class Normalizer:
    """
    Masks high-cardinality tokens and canonicalizes text to reduce false uniqueness.
    """

    def normalize_text(self, text: str) -> NormalizedText:
        redactions: Dict[str, int] = {}

        def sub(pattern: re.Pattern, repl: str, t: str, key: str) -> str:
            matches = pattern.findall(t)
            if matches:
                redactions[key] = redactions.get(key, 0) + (len(matches) if isinstance(matches, list) else 1)
            return pattern.sub(repl, t)

        t = text
        t = sub(_TS_ISO_RE, "<TS>", t, "ts")
        t = sub(_UUID_RE, "<UUID>", t, "uuid")
        t = sub(_IP_RE, "<IP>", t, "ip")
        t = sub(_HEX_RE, "<HEX>", t, "hex")
        t = sub(_LONG_NUM_RE, "<NUM>", t, "num")
        t = sub(_POD_HASH_RE, r"\1-<HASH>", t, "hash")
        t = sub(_PATH_RE, "<PATH>", t, "path")

        # Light canonicalization
        t = t.replace("\r", " ")
        t = re.sub(r"\s+", " ", t).strip().lower()

        return NormalizedText(normalized=t, redactions=redactions)

    def normalize_fields(self, fields: Dict[str, Any]) -> Dict[str, Any]:
        """
        Best-effort normalization of structured fields (strings only).
        """
        out: Dict[str, Any] = {}
        for k, v in fields.items():
            if isinstance(v, str):
                out[k] = self.normalize_text(v).normalized
            else:
                out[k] = v
        return out

    @staticmethod
    def build_embedding_text(
        normalized_text: str,
        service: Optional[str],
        severity: Optional[str],
        key_fields: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Creates the exact input string that will be embedded.
        """
        parts = []
        if service:
            parts.append(f"service: {service}")
        if severity:
            parts.append(f"severity: {severity}")
        if key_fields:
            # Keep it small and stable
            for kk in sorted(key_fields.keys()):
                vv = key_fields[kk]
                if vv is None:
                    continue
                if isinstance(vv, (str, int, float, bool)):
                    parts.append(f"{kk}: {vv}")
        parts.append(f"message: {normalized_text}")
        return " | ".join(parts)