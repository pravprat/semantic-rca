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
HTTP_RE = re.compile(r'"(GET|POST|PUT|DELETE|PATCH|HEAD|OPTIONS)\s+([^\s"]+)')
STATUS_RE = re.compile(r"status[:=]\s*(\d{3})")
JAVA_COMPONENT_RE = re.compile(r"\b([a-z]+\.[a-z0-9_.]+\.[a-z0-9_.]+)\b")


class Eventizer:
    def __init__(self, normalizer: Optional[Normalizer] = None):
        self.normalizer = normalizer or Normalizer()

    def iter_events(self, records: Iterator[RawRecord]) -> Iterator[SemanticEvent]:
        for r in records:
            yield self._record_to_event(r)

    def _extract_timestamp(self, text: str) -> Optional[str]:
        # ISO / k8s / json-style
        m = re.search(r"\b\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?Z\b", text)
        if m:
            return m.group(0)

        # OpenStack / JVM style with millis
        m = re.search(r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\.\d+\b", text)
        if m:
            try:
                dt = datetime.strptime(m.group(0), "%Y-%m-%d %H:%M:%S.%f")
                dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except Exception:
                pass

        # BGL timestamp: 2005-06-03-15.22.50.675872
        m = re.search(r"\b\d{4}-\d{2}-\d{2}-\d{2}\.\d{2}\.\d{2}\.\d+\b", text)
        if m:
            try:
                dt = datetime.strptime(m.group(0), "%Y-%m-%d-%H.%M.%S.%f")
                dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except Exception:
                pass

        # OpenStack / JVM style without millis
        m = re.search(r"\b\d{4}-\d{2}-\d{2} \d{2}:\d{2}:\d{2}\b", text)
        if m:
            try:
                dt = datetime.strptime(m.group(0), "%Y-%m-%d %H:%M:%S")
                dt = dt.replace(tzinfo=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except Exception:
                pass

        # epoch float inside bracket
        m = re.search(r'\[(\d+(?:\.\d+)?),', text)
        if m:
            try:
                epoch = float(m.group(1))
                dt = datetime.fromtimestamp(epoch, tz=timezone.utc)
                return dt.isoformat().replace("+00:00", "Z")
            except Exception:
                pass

        return None

    @staticmethod
    def parse_k8s_audit_csv(raw: str) -> Dict[str, Any]:
        raw = (raw or "").strip()
        if not raw:
            return {}

        try:
            parts: List[str] = next(csv.reader(StringIO(raw)))
        except Exception:
            return {}

        # Need enough columns to look like audit csv
        if len(parts) < 12:
            return {}

        out: Dict[str, Any] = {}

        ts = (parts[1] or "").strip()
        if "T" in ts and ts.endswith("Z"):
            out["timestamp"] = ts

        actor = (parts[2] or "").strip()
        if actor:
            out["actor"] = actor
            if actor.startswith("system:serviceaccount:"):
                toks = actor.split(":")
                if len(toks) >= 4:
                    out["service"] = f"{toks[2]}/{toks[3]}"
            elif actor.startswith("system:node:"):
                out["service"] = actor

        if len(parts) > 3 and (parts[3] or "").strip():
            out["verb"] = parts[3].strip()

        if len(parts) > 4 and (parts[4] or "").strip():
            out["resource"] = parts[4].strip()

        if len(parts) > 8 and (parts[8] or "").strip():
            out["path"] = parts[8].strip()

        if len(parts) > 10 and (parts[10] or "").strip():
            out["stage"] = parts[10].strip().lower()

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

    def infer_severity_from_message(self, text: str) -> Optional[str]:
        t = text.strip().lower()

        # explicit token first
        m = SEVERITY_TOKEN_RE.search(text)
        if m:
            return m.group(1)

        # k8s short prefixes
        if re.match(r"^e\d{4}", t):
            return "ERROR"
        if re.match(r"^w\d{4}", t):
            return "WARN"
        if re.match(r"^i\d{4}", t):
            return "INFO"

        # keyword fallback
        if "panic" in t or "fatal" in t:
            return "ERROR"
        if "error" in t or "failed" in t:
            return "ERROR"
        if "warn" in t or "warning" in t:
            return "WARN"
        if "info" in t:
            return "INFO"

        return None

    def infer_http_code_from_message(self, text: str) -> Optional[int]:
        t = text.lower()

        m = re.search(r"code\s+([45]\d\d)", t)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass

        m = re.search(r"\b([45]\d\d)\s+(bad gateway|internal server error|service unavailable)", t)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass

        m = STATUS_RE.search(text)
        if m:
            try:
                return int(m.group(1))
            except Exception:
                pass

        return None

    def infer_k8s_component(self, obj: Dict[str, Any]) -> Optional[str]:
        k8s = obj.get("kubernetes")
        if not isinstance(k8s, dict):
            return None

        labels = k8s.get("labels")
        if isinstance(labels, dict) and labels.get("component"):
            return labels["component"]

        return None

    def infer_component_from_text(self, text: str) -> Optional[str]:
        m = re.search(r'"labels"\s*:\s*\{[^}]*"component"\s*:\s*"([^"]+)"', text)
        if m:
            return m.group(1)
        return None

    def infer_service_from_text(self, text: str) -> Optional[str]:
        # enterprise pod banner
        m = re.search(r"====\s*([a-z0-9\-]+)-[a-z0-9\-]+\s*::", text)
        if m:
            return m.group(1)

        # java/openstack component; skip filename-like matches
        for match in JAVA_COMPONENT_RE.findall(text):
            if ".log." in match:
                continue
            return match

        return None

    def infer_openstack_http_fields(self, text: str) -> Dict[str, Any]:
        out: Dict[str, Any] = {}

        m = HTTP_RE.search(text)
        if m:
            out["verb"] = m.group(1)
            out["path"] = m.group(2)

            parts = [p for p in m.group(2).split("/") if p]
            if parts:
                out["resource"] = parts[-1]

        m = STATUS_RE.search(text)
        if m:
            try:
                code = int(m.group(1))
                out["response_code"] = code
                out["http_class"] = f"{code // 100}xx"
            except Exception:
                pass

        return out

    def _record_to_event(self, r: RawRecord) -> SemanticEvent:
        event_id = str(uuid.uuid4())

        raw_text = r.raw or ""
        timestamp = self._extract_timestamp(raw_text)
        severity: Optional[str] = None
        service: Optional[str] = None
        actor: Optional[str] = None
        verb: Optional[str] = None
        resource: Optional[str] = None
        path: Optional[str] = None
        stage: Optional[str] = None
        response_code: Optional[int] = None
        http_class: Optional[str] = None
        structured: Dict[str, Any] = {}

        # 1) JSON / wrapped JSON path
        if r.json_obj is not None:
            obj = r.json_obj

            timestamp = (
                obj.get("time")
                or obj.get("timestamp")
                or obj.get("@timestamp")
                or timestamp
            )
            severity = obj.get("level") or obj.get("severity") or obj.get("lvl")

            msg = obj.get("msg") or obj.get("message") or obj.get("log") or raw_text
            raw_text = msg if isinstance(msg, str) else raw_text

            k8s = obj.get("kubernetes") or {}
            if isinstance(k8s, dict):
                service = (
                    k8s.get("container_name")
                    or k8s.get("pod_name")
                    or obj.get("service")
                )

                if isinstance(service, str) and (
                    service.startswith("stream") or service in ["stderr", "stdout"]
                ):
                    service = None

                labels = k8s.get("labels")
                if isinstance(labels, dict) and labels.get("component"):
                    actor = labels.get("component")

            structured = self._extract_structured(obj)

        # 2) K8s audit CSV path
        else:
            fields = self.parse_k8s_audit_csv(raw_text)

            if fields:
                timestamp = timestamp or fields.get("timestamp")
                service = fields.get("service") or fields.get("actor")
                actor = fields.get("actor")
                verb = fields.get("verb")
                resource = fields.get("resource")
                path = fields.get("path")
                stage = fields.get("stage")
                response_code = fields.get("response_code")
                http_class = fields.get("http_class")

                for k, v in fields.items():
                    if v is not None and v != "":
                        structured[k] = v

                if isinstance(response_code, int):

                    if response_code >= 500:
                        severity = "ERROR"

                    # authentication / authorization failures
                    elif response_code in (401, 403):
                        severity = "ERROR"

                    elif response_code >= 400:
                        severity = "WARN"

                    else:
                        severity = "INFO"

        # 3) Generic enrichment
        if severity is None:
            severity = self.infer_severity_from_message(raw_text)

        if response_code is None:
            code = self.infer_http_code_from_message(raw_text)
            if code is not None:
                response_code = code
                http_class = f"{code // 100}xx"

        comp = None
        if r.json_obj is not None:
            comp = self.infer_k8s_component(r.json_obj)

        if not comp:
            comp = self.infer_component_from_text(raw_text)

        if comp and actor is None:
            actor = comp
            structured["component"] = comp

        if service is None and actor:
            service = actor

        if service is None:
            inferred = self.infer_service_from_text(raw_text)
            if inferred:
                service = inferred

        http_fields = self.infer_openstack_http_fields(raw_text)
        if http_fields:
            if verb is None:
                verb = http_fields.get("verb")
            if path is None:
                path = http_fields.get("path")
            if resource is None:
                resource = http_fields.get("resource")
            if response_code is None:
                response_code = http_fields.get("response_code")
                http_class = http_fields.get("http_class")

        norm = self.normalizer.normalize_text(raw_text)
        normalized_text = norm.normalized

        key_fields: Dict[str, Any] = {}
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
            "container",
        ]:
            if k in structured:
                key_fields[k] = structured[k]

        # include extracted top-level fields too
        if verb is not None:
            key_fields["verb"] = verb
        if resource is not None:
            key_fields["resource"] = resource
        if path is not None:
            key_fields["path"] = path
        if stage is not None:
            key_fields["stage"] = stage
        if http_class is not None:
            key_fields["http_class"] = http_class

        if actor and "component" not in key_fields:
            key_fields["component"] = actor

        embedding_text = self.normalizer.build_embedding_text(
            normalized_text=normalized_text,
            service=service,
            severity=(severity.lower() if isinstance(severity, str) else severity),
            key_fields=key_fields,
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
            redactions=norm.redactions,
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