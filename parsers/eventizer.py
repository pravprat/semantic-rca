# semantic-rca/parsers/eventizer.py
from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, Iterator, List
import uuid
import re
from datetime import datetime, timezone
import csv
from io import StringIO

from parsers.log_reader import RawRecord
from parsers.normalizer import Normalizer


@dataclass
class SemanticEvent:
    event_id: str
    source_type: str  # "log"
    timestamp: Optional[str]
    severity: Optional[str]
    service: Optional[str]

    # ✅ Canonical structured fields that downstream expects TOP-LEVEL
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

    def to_dict(self) -> Dict[str, Any]:
        return asdict(self)


class Eventizer:
    def __init__(self, normalizer: Optional[Normalizer] = None):
        self.normalizer = normalizer or Normalizer()

    def iter_events(self, records: Iterator[RawRecord]) -> Iterator[SemanticEvent]:
        for r in records:
            yield self._record_to_event(r)

    def _extract_timestamp(self, r: RawRecord) -> Optional[str]:
        # Prefer JSON obj timestamps
        if r.json_obj is not None:
            obj = r.json_obj
            ts = obj.get("time") or obj.get("timestamp") or obj.get("@timestamp")
            if isinstance(ts, str) and ts.strip():
                return ts.strip()

            t_field = obj.get("t")
            if isinstance(t_field, dict):
                d = t_field.get("$date")
                if isinstance(d, str) and d.strip():
                    return d.strip()

        raw = (r.raw or "").strip()

        # ISO timestamp in CSV logs
        m = re.search(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b", raw)
        if m:
            return m.group(0)

        # "time":"...Z"
        m = re.search(r'"time"\s*:\s*"([^"]+)"', raw)
        if m:
            return m.group(1)

        # "$date":"..."
        m = re.search(r'"\$date"\s*:\s*"([^"]+)"', raw)
        if m:
            return m.group(1)

        # epoch float inside bracket: [1771372499.88, {...}]
        m = re.search(r'\[(\d+(?:\.\d+)?),', raw)
        if m:
            try:
                epoch = float(m.group(1))
                dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except Exception:
                return None

        return None

    @staticmethod
    def parse_k8s_audit_csv(raw: str) -> Dict[str, Any]:
        """
        Parses lines like:
        "<ua_json>",2023-...Z,system:serviceaccount:ns:sa,get,...,/path,...,ResponseComplete,200,,,
        Uses csv.reader to respect quoted commas.
        """
        raw = (raw or "").strip()
        if not raw:
            return {}

        # Try robust CSV parse first
        parts: List[str]
        try:
            reader = csv.reader(StringIO(raw))
            parts = next(reader)
        except Exception:
            # Fallback: naive split (last resort)
            parts = [p.strip() for p in raw.split(",")]

        # Need at least: [ua, ts, actor]
        if len(parts) < 3:
            return {}

        out: Dict[str, Any] = {}

        # timestamp at index 1
        ts = (parts[1] or "").strip()
        if "T" in ts and ts.endswith("Z"):
            out["timestamp"] = ts

        # actor at index 2
        actor = (parts[2] or "").strip()
        if actor:
            out["actor"] = actor

            # service derivation
            if actor.startswith("system:serviceaccount:"):
                toks = actor.split(":")
                if len(toks) >= 4:
                    out["service"] = f"{toks[2]}/{toks[3]}"
            elif actor.startswith("system:node:"):
                # ✅ your screenshot shows these, keep as service too
                out["service"] = actor

        # verb index 3
        if len(parts) > 3 and (parts[3] or "").strip():
            out["verb"] = parts[3].strip()

        # resource index 4
        if len(parts) > 4 and (parts[4] or "").strip():
            out["resource"] = parts[4].strip()

        # request path index 8
        if len(parts) > 8 and (parts[8] or "").strip():
            out["path"] = parts[8].strip()

        # stage index 10
        if len(parts) > 10 and (parts[10] or "").strip():
            out["stage"] = parts[10].strip().lower()

        # response code index 11
        code: Optional[int] = None
        if len(parts) > 11:
            v = (parts[11] or "").strip()
            if v:
                try:
                    code = int(v)
                except Exception:
                    code = None
        out["response_code"] = code
        out["http_class"] = f"{code // 100}xx" if isinstance(code, int) else None

        return out

    def _record_to_event(self, r: RawRecord) -> SemanticEvent:
        event_id = str(uuid.uuid4())

        timestamp = self._extract_timestamp(r)
        severity: Optional[str] = None
        service: Optional[str] = None

        # ✅ canonical top-level fields
        actor: Optional[str] = None
        verb: Optional[str] = None
        resource: Optional[str] = None
        path: Optional[str] = None
        stage: Optional[str] = None
        response_code: Optional[int] = None
        http_class: Optional[str] = None

        structured: Dict[str, Any] = {}

        # --------------------------
        # JSON log case
        # --------------------------
        if r.json_obj is not None:
            obj = r.json_obj
            timestamp = (obj.get("time") or obj.get("timestamp") or timestamp)
            severity = (obj.get("level") or obj.get("severity") or obj.get("lvl"))
            msg = obj.get("msg") or obj.get("message") or obj.get("log") or r.raw

            k8s = obj.get("kubernetes") or {}
            if isinstance(k8s, dict):
                service = k8s.get("container_name") or k8s.get("pod_name") or obj.get("service")

            structured = self._extract_structured(obj)
            raw_text = msg if isinstance(msg, str) else (r.raw or "")

        # --------------------------
        # Plain-text (audit CSV) case
        # --------------------------
        else:
            raw_text = r.raw or ""
            fields = self.parse_k8s_audit_csv(raw_text)

            if fields:
                timestamp = timestamp or fields.get("timestamp")
                service = service or fields.get("service") or fields.get("actor")

                actor = fields.get("actor")
                verb = fields.get("verb")
                resource = fields.get("resource")
                path = fields.get("path")
                stage = fields.get("stage")
                response_code = fields.get("response_code")
                http_class = fields.get("http_class")

                # Mirror into structured for future use, but do NOT rely on it downstream
                for k, v in fields.items():
                    if v is not None and v != "":
                        structured[k] = v

                # Severity from HTTP code
                if isinstance(response_code, int):
                    if response_code >= 500:
                        severity = "ERROR"
                    elif response_code >= 400:
                        severity = "WARN"
                    else:
                        severity = "INFO"

        norm = self.normalizer.normalize_text(raw_text)
        normalized_text = norm.normalized

        key_fields = {}
        for k in [
            "verb",
            "resource",
            "path",
            "stage",
            "http_class",
            "error",
            "exception",
            "code",
            "status",
            "reason",
            "component",
            "namespace",
            "container"
        ]:
            if k in structured:
                key_fields[k] = structured[k]

        # Use message only if we don't have meaningful structured fields
        use_message = not any(
            k in structured
            for k in ["verb", "resource", "http_class", "stage"]
        )

        embedding_text = self.normalizer.build_embedding_text(
            normalized_text=(normalized_text if use_message else ""),
            service=service,
            severity=(severity.lower() if isinstance(severity, str) else severity),
            key_fields=key_fields
        )

        return SemanticEvent(
            event_id=event_id,
            source_type="log",
            timestamp=timestamp,
            severity=(severity.upper() if isinstance(severity, str) else severity),
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
            redactions=norm.redactions
        )

    @staticmethod
    def _extract_structured(obj: Dict[str, Any]) -> Dict[str, Any]:
        structured: Dict[str, Any] = {}
        for k in ["host", "logger", "component", "reason", "code", "status", "error", "exception"]:
            if k in obj and isinstance(obj[k], (str, int, float, bool)):
                structured[k] = obj[k]

        k8s = obj.get("kubernetes")
        if isinstance(k8s, dict):
            for k in ["namespace_name", "container_name", "pod_name", "node_name"]:
                if k in k8s and isinstance(k8s[k], str):
                    structured[k.replace("_name", "")] = k8s[k]
        return structured