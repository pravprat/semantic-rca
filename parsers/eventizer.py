#eventizer

from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import Dict, Any, Optional, Iterator, List
import uuid
import re
import csv
from io import StringIO
from datetime import datetime, timezone
import json

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
    status_family: Optional[str]
    failure_hint: Optional[str]

    raw_text: str
    normalized_text: str
    embedding_text: str

    structured_fields: Dict[str, Any]
    redactions: Dict[str, int]

    def to_dict(self):
        return asdict(self)


SEVERITY_TOKEN_RE = re.compile(r"\b(INFO|WARN|ERROR|DEBUG|TRACE|FATAL)\b")
STATUS_RE = re.compile(r"status[:=]\s*(\d{3})")
ASUP_STATUS_RE = re.compile(r"(?:unexpected\s+status|status)[:=\s]+(\d{3})", re.IGNORECASE)
HTTP_STATUS_RE = re.compile(r"\b(?:status|code|response)\s*[=:]?\s*(\d{3})\b", re.IGNORECASE)
URI_RE = re.compile(r"(https?://[^\s\"']+|/[A-Za-z0-9_\-./?=&%]+)")
VERB_HINT_RE = re.compile(
    r"\b(fetch|send|connect|watch|list|create|delete|update|retry|read|write|process)\b",
    re.IGNORECASE,
)
FAILURE_HINT_PATTERNS = [
    (re.compile(r"\bdeadline\s+exceeded\b", re.IGNORECASE), "timeout"),
    (re.compile(r"\bcontext\s+deadline\s+exceeded\b", re.IGNORECASE), "timeout"),
    (re.compile(r"\bconnection\s+refused\b", re.IGNORECASE), "connection_refused"),
    (re.compile(r"\bconnection\s+reset\b", re.IGNORECASE), "connection_reset"),
    (re.compile(r"\bnetwork\s+unreachable\b", re.IGNORECASE), "network_unreachable"),
    (re.compile(r"\btimeout\b", re.IGNORECASE), "timeout"),
    (re.compile(r"\brpc\s+error\b", re.IGNORECASE), "rpc_error"),
    (re.compile(r"\bthreshold\s+exceeded\b", re.IGNORECASE), "threshold_exceeded"),
    (re.compile(r"\bexceeded\b", re.IGNORECASE), "threshold_exceeded"),
    (re.compile(r"\bexception\b", re.IGNORECASE), "exception"),
    (re.compile(r"\bpanic\b", re.IGNORECASE), "panic"),
    (re.compile(r"\bforbidden\b", re.IGNORECASE), "forbidden"),
    (re.compile(r"\bunauthorized\b", re.IGNORECASE), "unauthorized"),
    (re.compile(r"\baccess\s+denied\b", re.IGNORECASE), "access_denied"),
    (re.compile(r"\bpermission\s+denied\b", re.IGNORECASE), "permission_denied"),
    (re.compile(r"\bfailed\b", re.IGNORECASE), "failed"),
]


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

    @staticmethod
    def _iso_from_epoch(value: Any) -> Optional[str]:
        try:
            ts = float(value)
            return datetime.fromtimestamp(ts, tz=timezone.utc).isoformat()
        except Exception:
            return None

    @staticmethod
    def _detect_inner_type(payload: Dict[str, Any]) -> str:
        if "statusCode" in payload and "method" in payload:
            return "http_probe_or_access"
        if "caller" in payload and "msg" in payload:
            return "caller_msg_structured"
        if "t" in payload and isinstance(payload.get("t"), dict) and "$date" in payload.get("t", {}):
            return "mongodb_structured"
        if "log" in payload:
            return "log_field_generic"
        return "other_json"

    @staticmethod
    def _extract_status_from_text(text: str) -> Optional[int]:
        if not text:
            return None
        m = ASUP_STATUS_RE.search(text) or HTTP_STATUS_RE.search(text)
        if not m:
            return None
        try:
            code = int(m.group(1))
            if 100 <= code <= 599:
                return code
        except Exception:
            pass
        return None

    @staticmethod
    def _coerce_http_code(value: Any) -> Optional[int]:
        if value is None:
            return None
        try:
            code = int(str(value).strip())
            if 100 <= code <= 599:
                return code
        except Exception:
            return None
        return None

    @staticmethod
    def _extract_path_from_text(text: str) -> Optional[str]:
        if not text:
            return None
        m = URI_RE.search(text)
        return m.group(1) if m else None

    @staticmethod
    def _extract_verb_from_text(text: str) -> Optional[str]:
        if not text:
            return None
        m = VERB_HINT_RE.search(text)
        return m.group(1).lower() if m else None

    @staticmethod
    def _extract_failure_hint(text: str) -> Optional[str]:
        if not text:
            return None
        for pat, hint in FAILURE_HINT_PATTERNS:
            if pat.search(text):
                return hint
        return None

    @staticmethod
    def _parse_wrapped_outer(raw: str) -> Optional[Dict[str, Any]]:
        s = (raw or "").strip()
        prefix = None
        if s.startswith("all.logs:"):
            prefix = "all.logs"
        elif s.startswith("asup.error.logs:"):
            prefix = "asup.error.logs"
        if not prefix:
            return None
        i = s.find("[")
        if i < 0:
            return None
        try:
            arr = json.loads(s[i:])
        except Exception:
            return None
        if not isinstance(arr, list) or len(arr) < 2 or not isinstance(arr[1], dict):
            return None
        return {"collector_ts": arr[0], "payload": arr[1], "source_subtype": prefix}

    @staticmethod
    def parse_all_logs_record(raw: str, obj: Dict[str, Any]) -> Dict[str, Any]:
        outer = Eventizer._parse_wrapped_outer(raw)
        # Critical guard: only map wrapped formats here.
        # Plain JSON audit records must flow to parse_k8s_audit_json().
        if not outer:
            return {}
        payload = outer.get("payload") or {}
        if not isinstance(payload, dict):
            return {}

        out: Dict[str, Any] = {}

        ts = payload.get("time")
        if isinstance(ts, str) and ts:
            out["timestamp"] = ts
        else:
            t_date = ((payload.get("t") or {}).get("$date")) if isinstance(payload.get("t"), dict) else None
            if isinstance(t_date, str) and t_date:
                out["timestamp"] = t_date
            else:
                collector_iso = Eventizer._iso_from_epoch((outer or {}).get("collector_ts"))
                if collector_iso:
                    out["timestamp"] = collector_iso

        kube = payload.get("kubernetes") if isinstance(payload.get("kubernetes"), dict) else {}
        labels = kube.get("labels") if isinstance(kube.get("labels"), dict) else {}
        attr = payload.get("attr") if isinstance(payload.get("attr"), dict) else {}

        service = (
            labels.get("app.kubernetes.io/name")
            or labels.get("app")
            or kube.get("container_name")
            or kube.get("pod_name")
        )
        if service:
            out["service"] = str(service)

        actor = (
            payload.get("user_name")
            or payload.get("caller")
            or payload.get("ctx")
            or kube.get("container_name")
        )
        if actor:
            out["actor"] = str(actor)

        method = payload.get("method")
        if method:
            out["verb"] = str(method).lower()
        else:
            text_for_verb = f"{payload.get('msg') or ''} {payload.get('log') or ''}"
            verb = Eventizer._extract_verb_from_text(text_for_verb)
            if verb:
                out["verb"] = verb

        resource = (
            attr.get("collection")
            or attr.get("db")
            or attr.get("topic")
            or attr.get("index")
            or kube.get("pod_name")
            or kube.get("namespace_name")
            or payload.get("c")
        )
        if resource:
            out["resource"] = str(resource)

        path = payload.get("path") or payload.get("uri")
        if not path:
            path = Eventizer._extract_path_from_text(str(payload.get("log") or payload.get("msg") or ""))
        if path:
            out["path"] = str(path)

        stream = payload.get("stream")
        if stream:
            out["stage"] = str(stream).lower()

        code = (
            Eventizer._coerce_http_code(payload.get("statusCode"))
            or Eventizer._coerce_http_code(payload.get("status"))
            or Eventizer._coerce_http_code(payload.get("status_code"))
            or Eventizer._coerce_http_code(payload.get("code"))
        )
        if code is None and isinstance(payload.get("responseStatus"), dict):
            code = Eventizer._coerce_http_code(payload.get("responseStatus", {}).get("code"))
        if code is None and isinstance(payload.get("response"), dict):
            code = (
                Eventizer._coerce_http_code(payload.get("response", {}).get("status"))
                or Eventizer._coerce_http_code(payload.get("response", {}).get("code"))
            )
        if code is None:
            # ASUP errors sometimes include status only in free-text error details.
            err_text = f"{payload.get('error') or ''} {payload.get('msg') or ''} {payload.get('log') or ''}"
            code = Eventizer._extract_status_from_text(err_text)
        if code is not None:
            out["response_code"] = code
            out["http_class"] = f"{code // 100}xx"

        # Non-HTTP fallback signal for incident detection in text-heavy logs.
        msg_text = f"{payload.get('error') or ''} {payload.get('msg') or ''} {payload.get('log') or ''}"
        failure_hint = Eventizer._extract_failure_hint(msg_text)
        if failure_hint:
            out["failure_hint"] = failure_hint

        sev = payload.get("s")
        if isinstance(sev, str):
            mapped = {
                "F": "FATAL",
                "E": "ERROR",
                "W": "WARN",
                "I": "INFO",
                "D": "DEBUG",
                "T": "TRACE",
            }.get(sev.upper())
            if mapped:
                out["severity_hint"] = mapped
        elif isinstance(payload.get("level"), str):
            lvl = str(payload.get("level")).upper()
            if lvl in {"FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"}:
                out["severity_hint"] = lvl

        if out.get("http_class"):
            out["status_family"] = "failure" if out["http_class"] in {"4xx", "5xx"} else "normal"
        else:
            sev_hint = out.get("severity_hint")
            if sev_hint in {"ERROR", "FATAL"} or failure_hint:
                out["status_family"] = "failure"
            elif sev_hint == "WARN":
                out["status_family"] = "warning"
            else:
                out["status_family"] = "unknown"

        out["source_subtype"] = (outer or {}).get("source_subtype") or "wrapped.logs"
        out["inner_type"] = Eventizer._detect_inner_type(payload)
        collector_ts = (outer or {}).get("collector_ts")
        if collector_ts is not None:
            out["collector_ts"] = collector_ts
        return out

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
        status_family = None
        failure_hint = None

        structured: Dict[str, Any] = {}

        fields = {}

        # ---- all.logs wrapper mapper (ASUP/support bundle) ----
        if r.json_obj:
            fields = self.parse_all_logs_record(raw_text, r.json_obj)

        # ---- JSON audit log fallback ----
        if r.json_obj:
            if not fields:
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
            status_family = fields.get("status_family")
            failure_hint = fields.get("failure_hint")

            structured.update(fields)

        # ---- SEVERITY ----
        severity = fields.get("severity_hint") if isinstance(fields, dict) else None
        if not severity:
            severity = self.infer_severity(response_code, raw_text)
        if not status_family:
            if isinstance(response_code, int):
                status_family = "failure" if response_code >= 400 else "normal"
            elif severity in {"ERROR", "FATAL"}:
                status_family = "failure"
            elif severity == "WARN":
                status_family = "warning"
            else:
                status_family = "unknown"

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
            status_family=status_family,
            failure_hint=failure_hint,
            raw_text=raw_text,
            normalized_text=normalized_text,
            embedding_text=embedding_text,
            structured_fields=structured,
            redactions=norm.redactions,
        )