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
STRICT_PATH_RE = re.compile(r"(/(?:api|apis|v1|v2|metrics|healthz|readyz|livez)[A-Za-z0-9_\-./?=&%]*)", re.IGNORECASE)
VERB_HINT_RE = re.compile(
    r"\b(fetch|send|connect|watch|list|create|delete|update|retry|read|write|process)\b",
    re.IGNORECASE,
)
HTTP_METHOD_RE = re.compile(r"\b(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b", re.IGNORECASE)
METHOD_EQ_RE = re.compile(r"\bmethod\s*=\s*(GET|POST|PUT|PATCH|DELETE|HEAD|OPTIONS)\b", re.IGNORECASE)
SEVERITY_HINT_RE = re.compile(
    r"\b(?:level|severity)\s*[:=]\s*(fatal|error|warn|warning|info|debug|trace)\b|\[(fatal|error|warn|warning|info|debug|trace)\]",
    re.IGNORECASE,
)
SVC_FQDN_RE = re.compile(
    r"https?://([a-z0-9-]+(?:\.[a-z0-9-]+){1,6}\.svc)(?::\d+)?",
    re.IGNORECASE,
)
FAILURE_HINT_PATTERNS = [
    (re.compile(r"\bdeadline\s+exceeded\b", re.IGNORECASE), "timeout"),
    (re.compile(r"\bcontext\s+deadline\s+exceeded\b", re.IGNORECASE), "timeout"),
    (re.compile(r"\bconnection\s+refused\b", re.IGNORECASE), "connection_refused"),
    (re.compile(r"\bconnection\s+reset\b", re.IGNORECASE), "connection_reset"),
    (re.compile(r"\btls\s+handshake\b", re.IGNORECASE), "tls_handshake"),
    (re.compile(r"\bcertificate\b", re.IGNORECASE), "tls_certificate"),
    (re.compile(r"\bdns\b", re.IGNORECASE), "dns_failure"),
    (re.compile(r"\bno\s+such\s+host\b", re.IGNORECASE), "dns_failure"),
    (re.compile(r"\bnetwork\s+unreachable\b", re.IGNORECASE), "network_unreachable"),
    (re.compile(r"\btimeout\b", re.IGNORECASE), "timeout"),
    (re.compile(r"\brpc\s+error\b", re.IGNORECASE), "rpc_error"),
    (re.compile(r"\bleader\s+election\b", re.IGNORECASE), "leader_election_failure"),
    (re.compile(r"\bcrashloopbackoff\b", re.IGNORECASE), "crash_loop"),
    (re.compile(r"\boomkilled\b", re.IGNORECASE), "oom_killed"),
    (re.compile(r"\bout\s+of\s+memory\b", re.IGNORECASE), "oom"),
    (re.compile(r"\breplica\s+set\b", re.IGNORECASE), "replica_instability"),
    (re.compile(r"\bprimary\s+not\s+found\b", re.IGNORECASE), "replica_instability"),
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
    def _looks_code_identity(value: str) -> bool:
        v = (value or "").strip()
        if not v:
            return False
        # Common noisy identities from app logs (func path, file:line, serialized dict payloads).
        if v.startswith("{") and ("func" in v or "source" in v):
            return True
        if ".go:" in v or ".py:" in v or ".java:" in v:
            return True
        if "/" in v and ":" in v and ("trace.go" in v or "interceptor.go" in v):
            return True
        return False

    @staticmethod
    def _sanitize_identity(value: Any) -> Optional[str]:
        if value is None:
            return None
        v = str(value).strip()
        if not v:
            return None
        if Eventizer._looks_code_identity(v):
            return None
        return v

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
        # Safety-first: prefer stricter API/health style paths before generic URL match.
        m = STRICT_PATH_RE.search(text) or URI_RE.search(text)
        return m.group(1) if m else None

    @staticmethod
    def _extract_verb_from_text(text: str) -> Optional[str]:
        if not text:
            return None
        m = METHOD_EQ_RE.search(text) or HTTP_METHOD_RE.search(text) or VERB_HINT_RE.search(text)
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
    def _extract_severity_from_text(text: str) -> Optional[str]:
        if not text:
            return None
        m = SEVERITY_HINT_RE.search(text)
        if not m:
            return None
        token = m.group(1) or m.group(2)
        if not token:
            return None
        token = token.upper()
        if token == "WARNING":
            token = "WARN"
        return token

    @staticmethod
    def _extract_dependency_target(text: str) -> Optional[Dict[str, str]]:
        if not text:
            return None
        m = SVC_FQDN_RE.search(text)
        if not m:
            return None
        fqdn = m.group(1).lower()
        service = fqdn.split(".")[0]
        return {"target_dependency_service": service, "target_dependency_fqdn": fqdn}

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
        field_source: Dict[str, str] = {}

        ts = payload.get("time")
        if isinstance(ts, str) and ts:
            out["timestamp"] = ts
            field_source["timestamp"] = "payload.time"
        else:
            t_date = ((payload.get("t") or {}).get("$date")) if isinstance(payload.get("t"), dict) else None
            if isinstance(t_date, str) and t_date:
                out["timestamp"] = t_date
                field_source["timestamp"] = "payload.t.$date"
            else:
                collector_iso = Eventizer._iso_from_epoch((outer or {}).get("collector_ts"))
                if collector_iso:
                    out["timestamp"] = collector_iso
                    field_source["timestamp"] = "outer.collector_ts"

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
            out["source_service"] = str(service)
            field_source["service"] = "kubernetes.labels_or_container"
            field_source["source_service"] = "derived.from_service"

        actor = (
            payload.get("user_name")
            or payload.get("caller")
            or payload.get("ctx")
            or kube.get("container_name")
        )
        actor_clean = Eventizer._sanitize_identity(actor)
        if not actor_clean and service:
            # Keep actor semantically usable when payload actor is code-path noise.
            actor_clean = Eventizer._sanitize_identity(service)
        if actor_clean:
            out["actor"] = actor_clean
            field_source["actor"] = "payload.user_name_or_caller_or_ctx"

        method = payload.get("method")
        if method:
            out["verb"] = str(method).lower()
            field_source["verb"] = "payload.method"
        else:
            text_for_verb = f"{payload.get('msg') or ''} {payload.get('log') or ''}"
            verb = Eventizer._extract_verb_from_text(text_for_verb)
            if verb:
                out["verb"] = verb
                field_source["verb"] = "derived.regex_text"

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
            field_source["resource"] = "payload.attr_or_kubernetes"

        path = payload.get("path") or payload.get("uri")
        if not path:
            path = Eventizer._extract_path_from_text(str(payload.get("log") or payload.get("msg") or ""))
        if path:
            out["path"] = str(path)
            field_source["path"] = "payload.path_or_uri_or_regex"

        stream = payload.get("stream")
        if stream:
            out["stage"] = str(stream).lower()
            field_source["stage"] = "payload.stream"

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
            field_source["response_code"] = "payload_or_nested_or_regex"
            field_source["http_class"] = "derived.from_response_code"

        # Non-HTTP fallback signal for incident detection in text-heavy logs.
        msg_text = f"{payload.get('error') or ''} {payload.get('msg') or ''} {payload.get('log') or ''}"
        failure_hint = Eventizer._extract_failure_hint(msg_text)
        if failure_hint:
            out["failure_hint"] = failure_hint

        dep = Eventizer._extract_dependency_target(msg_text)
        if dep:
            out.update(dep)
            field_source["target_dependency_service"] = "derived.from_text_fqdn"
            field_source["target_dependency_fqdn"] = "derived.from_text_fqdn"

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
                field_source["severity_hint"] = "payload.s"
        elif isinstance(payload.get("level"), str):
            lvl = str(payload.get("level")).upper()
            if lvl in {"FATAL", "ERROR", "WARN", "INFO", "DEBUG", "TRACE"}:
                out["severity_hint"] = lvl
                field_source["severity_hint"] = "payload.level"
        if "severity_hint" not in out:
            sev_txt = Eventizer._extract_severity_from_text(f"{payload.get('msg') or ''} {payload.get('log') or ''}")
            if sev_txt:
                out["severity_hint"] = sev_txt
                field_source["severity_hint"] = "derived.regex_text"

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

        # Explicit causality role fields to avoid source/target ambiguity in support workflows.
        if out.get("target_dependency_service"):
            out["failure_location"] = "dependency_target"
            out["causal_confidence_tier"] = "observed"
            field_source["failure_location"] = "derived.target_dependency_present"
            field_source["causal_confidence_tier"] = "derived.target_dependency_present"
        elif out.get("status_family") == "failure":
            out["failure_location"] = "source_service"
            out["causal_confidence_tier"] = "likely"
            field_source["failure_location"] = "derived.failure_without_target"
            field_source["causal_confidence_tier"] = "derived.failure_without_target"

        out["source_subtype"] = (outer or {}).get("source_subtype") or "wrapped.logs"
        out["inner_type"] = Eventizer._detect_inner_type(payload)
        out["field_source"] = field_source
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
        field_source: Dict[str, str] = {}

        # timestamp
        ts = obj.get("requestReceivedTimestamp")
        if ts:
            out["timestamp"] = ts
            field_source["timestamp"] = "requestReceivedTimestamp"

        # actor
        actor = obj.get("user", {}).get("username")
        actor_clean = Eventizer._sanitize_identity(actor)
        if actor_clean:
            out["actor"] = actor_clean
            out["service"] = actor_clean  # keep same behavior as CSV
            field_source["actor"] = "user.username"
            field_source["service"] = "derived.from_actor"

        # verb
        out["verb"] = obj.get("verb")
        if out.get("verb"):
            field_source["verb"] = "verb"

        # resource
        ref = obj.get("objectRef", {})
        out["resource"] = ref.get("resource")
        if out.get("resource"):
            field_source["resource"] = "objectRef.resource"

        # path
        out["path"] = obj.get("requestURI")
        if out.get("path"):
            field_source["path"] = "requestURI"

        # stage
        stage = obj.get("stage")
        if stage:
            out["stage"] = stage.lower()
            field_source["stage"] = "stage"

        # response code
        code = obj.get("responseStatus", {}).get("code")
        if isinstance(code, int):
            out["response_code"] = code
            out["http_class"] = f"{code // 100}xx"
            field_source["response_code"] = "responseStatus.code"
            field_source["http_class"] = "derived.from_response_code"

        out["field_source"] = field_source

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
        field_source: Dict[str, str] = {}

        ts = (parts[1] or "").strip()
        if "T" in ts and ts.endswith("Z"):
            out["timestamp"] = ts
            field_source["timestamp"] = "csv.col[1]"

        actor = (parts[2] or "").strip()
        actor_clean = Eventizer._sanitize_identity(actor)
        if actor_clean:
            out["actor"] = actor_clean
            out["service"] = actor_clean  # preserve raw identity
            field_source["actor"] = "csv.col[2]"
            field_source["service"] = "derived.from_actor"

        if len(parts) > 3:
            out["verb"] = (parts[3] or "").strip()
            if out["verb"]:
                field_source["verb"] = "csv.col[3]"

        if len(parts) > 4:
            out["resource"] = (parts[4] or "").strip()
            if out["resource"]:
                field_source["resource"] = "csv.col[4]"

        if len(parts) > 8:
            out["path"] = (parts[8] or "").strip()
            if out["path"]:
                field_source["path"] = "csv.col[8]"

        if len(parts) > 10:
            out["stage"] = (parts[10] or "").strip().lower()
            if out["stage"]:
                field_source["stage"] = "csv.col[10]"

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
        if isinstance(code, int):
            field_source["response_code"] = "csv.col[11]"
            field_source["http_class"] = "derived.from_response_code"
        out["field_source"] = field_source

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