#eventizer

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, Iterator, List
import uuid
import re
import csv
from io import StringIO
from datetime import datetime, timezone

from parsers.log_reader import RawRecord
from parsers.normalizer import Normalizer


@dataclass
class SemanticEvent:
    event_id: str
    source_type: str

    timestamp: Optional[str]
    severity: Optional[str]

    service: Optional[str]
    actor: Optional[str]

    verb: Optional[str]
    resource: Optional[str]
    path: Optional[str]
    stage: Optional[str]

    response_code: Optional[int]
    http_class: Optional[str]

    raw_text: str
    normalized_text: str
    embedding_text: str

    structured_fields: Dict[str, Any]
    redactions: Dict[str, int]

    def to_dict(self):
        return asdict(self)


SEVERITY_TOKEN_RE = re.compile(r"\b(INFO|WARN|ERROR|DEBUG|TRACE|FATAL)\b")
STATUS_RE = re.compile(r"status[:=]\s*(\d{3})")


class Eventizer:
    def __init__(self, normalizer: Optional[Normalizer] = None):
        self.normalizer = normalizer or Normalizer()

    def iter_events(self, records: Iterator[RawRecord]) -> Iterator[SemanticEvent]:
        for r in records:
            yield self._record_to_event(r)

    # -------------------------------
    # TIMESTAMP EXTRACTION (unchanged)
    # -------------------------------
    def _extract_timestamp(self, text: str) -> Optional[str]:
        m = re.search(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b", text)
        if m:
            return m.group(0)

        return None

    # -------------------------------
    # K8S JSON PARSER
    # -------------------------------
    def parse_k8s_audit_json(self, obj: Dict[str, Any]) -> Dict[str, Any]:
        if not obj:
            return {}

        out: Dict[str, Any] = {}

        # timestamp
        ts = obj.get("requestReceivedTimestamp")
        if ts:
            out["timestamp"] = ts

        # actor
        actor = obj.get("user", {}).get("username")
        if actor:
            out["actor"] = actor
            out["service"] = actor  # keep same behavior as CSV

        # verb
        out["verb"] = obj.get("verb")

        # resource
        ref = obj.get("objectRef", {})
        out["resource"] = ref.get("resource")

        # path
        out["path"] = obj.get("requestURI")

        # stage
        stage = obj.get("stage")
        if stage:
            out["stage"] = stage.lower()

        # response code
        code = obj.get("responseStatus", {}).get("code")
        if isinstance(code, int):
            out["response_code"] = code
            out["http_class"] = f"{code // 100}xx"

        return out

    # -------------------------------
    # K8S AUDIT CSV PARSER
    # -------------------------------
    @staticmethod
    def parse_k8s_audit_csv(raw: str) -> Dict[str, Any]:
        raw = (raw or "").strip()
        if not raw:
            return {}

        try:
            parts: List[str] = next(csv.reader(StringIO(raw)))
        except Exception:
            return {}

        if len(parts) < 12:
            return {}

        out: Dict[str, Any] = {}

        ts = (parts[1] or "").strip()
        if "T" in ts and ts.endswith("Z"):
            out["timestamp"] = ts

        actor = (parts[2] or "").strip()
        if actor:
            out["actor"] = actor
            out["service"] = actor  # preserve raw identity

        if len(parts) > 3:
            out["verb"] = (parts[3] or "").strip()

        if len(parts) > 4:
            out["resource"] = (parts[4] or "").strip()

        if len(parts) > 8:
            out["path"] = (parts[8] or "").strip()

        if len(parts) > 10:
            out["stage"] = (parts[10] or "").strip().lower()

        code = None
        if len(parts) > 11:
            v = (parts[11] or "").strip()
            if v:
                try:
                    code = int(v)
                except Exception:
                    pass

        out["response_code"] = code
        out["http_class"] = f"{code // 100}xx" if isinstance(code, int) else None

        return out

    # -------------------------------
    # SEVERITY INFERENCE
    # -------------------------------
    def infer_severity(self, response_code: Optional[int], text: str) -> Optional[str]:
        if isinstance(response_code, int):
            if response_code >= 500:
                return "ERROR"
            elif response_code in (401, 403):
                return "ERROR"
            elif response_code >= 400:
                return "WARN"
            else:
                return "INFO"

        t = text.lower()
        if "error" in t or "failed" in t:
            return "ERROR"
        if "warn" in t:
            return "WARN"
        if "info" in t:
            return "INFO"

        return None

    # -------------------------------
    # MAIN EVENT BUILDER
    # -------------------------------
    def _record_to_event(self, r: RawRecord) -> SemanticEvent:
        event_id = str(uuid.uuid4())

        raw_text = r.raw or ""

        timestamp = self._extract_timestamp(raw_text)

        service = None
        actor = None
        verb = None
        resource = None
        path = None
        stage = None
        response_code = None
        http_class = None

        structured: Dict[str, Any] = {}

        fields = {}

        # ---- JSON audit log (NEW) ----
        if r.json_obj:
            fields = self.parse_k8s_audit_json(r.json_obj)

        # ---- fallback: CSV audit ----
        if not fields:
            fields = self.parse_k8s_audit_csv(raw_text)

        if fields:
            timestamp = fields.get("timestamp") or timestamp
            service = fields.get("service")
            actor = fields.get("actor")
            verb = fields.get("verb")
            resource = fields.get("resource")
            path = fields.get("path")
            stage = fields.get("stage")
            response_code = fields.get("response_code")
            http_class = fields.get("http_class")

            structured.update(fields)

        # ---- SEVERITY ----
        severity = self.infer_severity(response_code, raw_text)

        # ---- NORMALIZATION ----
        norm = self.normalizer.normalize_text(raw_text)
        normalized_text = norm.normalized

        # ------------------------------------
        # 🔥 CRITICAL FIX: STABLE EMBEDDING ONLY
        # ------------------------------------
        key_fields: Dict[str, Any] = {}

        if verb:
            key_fields["verb"] = verb

        if resource:
            key_fields["resource"] = resource

        if http_class:
            key_fields["http_class"] = http_class

        # NO path
        # NO stage
        # NO component
        # NO message leakage

        embedding_text = self.normalizer.build_embedding_text(
            normalized_text=None,  # 🔥 disable message entirely
            service=service,
            severity=(severity.lower() if isinstance(severity, str) else severity),
            key_fields=key_fields,
        )

        return SemanticEvent(
            event_id=event_id,
            source_type="log",
            timestamp=timestamp,
            severity=severity,
            service=service,
            actor=actor,
            verb=verb,
            resource=resource,
            path=path,
            stage=stage,
            response_code=response_code,
            http_class=http_class,
            raw_text=raw_text,
            normalized_text=normalized_text,
            embedding_text=embedding_text,
            structured_fields=structured,
            redactions=norm.redactions,
        )