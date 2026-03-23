from __future__ import annotations

import csv
import io
from typing import Dict, Any


def _safe(v, default: str = "") -> str:
    if v is None:
        return default
    return str(v)


def _http_outcome(code: str) -> str:
    code = _safe(code).strip()

    if not code:
        return "unknown outcome"

    if code.startswith("5"):
        return "server failures"
    if code.startswith("4"):
        return "authorization/client errors"
    if code.startswith("2"):
        return "successful operations"

    return "unknown outcome"


def _looks_like_actor(v: str) -> bool:
    if not v:
        return False

    prefixes = (
        "system:",
        "kube-",
        "gatekeeper-",
        "replicaset-controller",
        "deployment-controller",
        "daemon-set-controller",
        "job-controller",
        "cronjob-controller",
    )
    return v.startswith(prefixes)


def _looks_like_verb(v: str) -> bool:
    return v in {
        "get",
        "list",
        "watch",
        "create",
        "delete",
        "patch",
        "update",
        "connect",
        "proxy",
    }


def _looks_like_http_code(v: str) -> bool:
    return v.isdigit() and len(v) == 3


def _parse_representative_raw_text(raw: str) -> Dict[str, str]:
    """
    Parse representative_raw_text from Kubernetes audit CSV rows.

    Preferred extraction uses known column offsets from your dataset:
      row[1]  timestamp
      row[2]  actor / user
      row[3]  verb
      row[4]  resource
      row[6]  namespace
      row[7]  endpoint-ish field
      row[11] response code / status

    If those fail, fall back to dynamic detection.
    """

    if not raw:
        return {}

    try:
        reader = csv.reader(io.StringIO(raw))
        row = next(reader)
    except Exception:
        return {}

    if not row:
        return {}

    # ---------------------------------------------------------
    # First try fixed offsets (best for your audit dataset)
    # ---------------------------------------------------------
    timestamp = row[1].strip() if len(row) > 1 and row[1] else ""
    actor = row[2].strip() if len(row) > 2 and row[2] else ""
    verb = row[3].strip() if len(row) > 3 and row[3] else ""
    resource = row[4].strip() if len(row) > 4 and row[4] else ""
    namespace = row[6].strip() if len(row) > 6 and row[6] else ""
    endpoint = row[7].strip() if len(row) > 7 and row[7] else ""
    response_code = row[11].strip() if len(row) > 11 and row[11] else ""

    # ---------------------------------------------------------
    # Dynamic fallback only for missing fields
    # ---------------------------------------------------------
    if not actor:
        for v in row:
            vv = v.strip()
            if _looks_like_actor(vv):
                actor = vv
                break

    if not verb:
        for v in row:
            vv = v.strip()
            if _looks_like_verb(vv):
                verb = vv
                break

    if not resource:
        # Prefer token after verb if possible
        for i, v in enumerate(row):
            vv = v.strip()
            if _looks_like_verb(vv) and i + 1 < len(row):
                candidate = row[i + 1].strip()
                if candidate:
                    resource = candidate
                    break

    if not response_code:
        for v in row:
            vv = v.strip()
            if _looks_like_http_code(vv):
                response_code = vv
                break

    return {
        "timestamp": timestamp,
        "actor": actor,
        "user": actor,
        "verb": verb,
        "resource": resource,
        "response_code": response_code,
        "status": response_code,
        "endpoint": endpoint,
        "namespace": namespace,
    }


def _normalize_event_like(obj: Any) -> Dict[str, str]:
    """
    Accept any of:
      - representative_event dict
      - first_seen_event dict
      - root cause wrapper dict containing representative_event / representative_raw_text
      - direct event dict
      - cluster-like dict with representative_raw_text

    Returns canonical keys:
      timestamp, actor, verb, resource, response_code, endpoint, namespace
    """

    if not isinstance(obj, dict):
        return {}

    # ---------------------------------------------------------
    # 1. representative_event (preferred)
    # ---------------------------------------------------------
    rep_event = obj.get("representative_event")
    if isinstance(rep_event, dict) and rep_event:
        actor = (
            rep_event.get("actor")
            or rep_event.get("user")
            or rep_event.get("service")
            or ""
        )
        return {
            "timestamp": _safe(rep_event.get("timestamp")),
            "actor": _safe(actor),
            "verb": _safe(rep_event.get("verb")),
            "resource": _safe(rep_event.get("resource")),
            "response_code": _safe(
                rep_event.get("response_code")
                or rep_event.get("status")
                or rep_event.get("code")
            ),
            "endpoint": _safe(rep_event.get("endpoint") or rep_event.get("path")),
            "namespace": _safe(rep_event.get("namespace")),
        }

    # ---------------------------------------------------------
    # 2. first_seen_event fallback
    # ---------------------------------------------------------
    first_seen_event = obj.get("first_seen_event")
    if isinstance(first_seen_event, dict) and first_seen_event:
        actor = (
            first_seen_event.get("actor")
            or first_seen_event.get("user")
            or first_seen_event.get("service")
            or ""
        )
        return {
            "timestamp": _safe(first_seen_event.get("timestamp")),
            "actor": _safe(actor),
            "verb": _safe(first_seen_event.get("verb")),
            "resource": _safe(first_seen_event.get("resource")),
            "response_code": _safe(
                first_seen_event.get("response_code")
                or first_seen_event.get("status")
                or first_seen_event.get("code")
            ),
            "endpoint": _safe(first_seen_event.get("endpoint") or first_seen_event.get("path")),
            "namespace": _safe(first_seen_event.get("namespace")),
        }

    # ---------------------------------------------------------
    # 3. object itself looks like an event
    # ---------------------------------------------------------
    actor = obj.get("actor") or obj.get("user") or obj.get("service")
    if actor or obj.get("verb") or obj.get("resource"):
        return {
            "timestamp": _safe(obj.get("timestamp")),
            "actor": _safe(actor),
            "verb": _safe(obj.get("verb")),
            "resource": _safe(obj.get("resource")),
            "response_code": _safe(
                obj.get("response_code") or obj.get("status") or obj.get("code")
            ),
            "endpoint": _safe(obj.get("endpoint") or obj.get("path")),
            "namespace": _safe(obj.get("namespace")),
        }

    # ---------------------------------------------------------
    # 4. representative_raw_text on wrapper/cluster object
    # ---------------------------------------------------------
    raw = obj.get("representative_raw_text")
    if raw:
        parsed = _parse_representative_raw_text(raw)
        if parsed:
            return {
                "timestamp": _safe(parsed.get("timestamp")),
                "actor": _safe(parsed.get("actor") or parsed.get("user")),
                "verb": _safe(parsed.get("verb")),
                "resource": _safe(parsed.get("resource")),
                "response_code": _safe(parsed.get("response_code") or parsed.get("status")),
                "endpoint": _safe(parsed.get("endpoint")),
                "namespace": _safe(parsed.get("namespace")),
            }

    return {}


def describe_cluster(obj: Any) -> str:
    """
    Human-readable cluster behavior description.
    Works for Step 8 and Step 10 inputs.
    """
    ev = _normalize_event_like(obj)

    if not ev:
        return "Unknown cluster behavior"

    actor = ev.get("actor") or "unknown actor"
    verb = ev.get("verb") or "operation"
    resource = ev.get("resource") or "resource"
    code = ev.get("response_code") or ""
    endpoint = ev.get("endpoint") or ""
    namespace = ev.get("namespace") or ""
    outcome = _http_outcome(code)

    parts = [f"{actor} {verb} {resource}"]

    if namespace:
        parts.append(f"in namespace {namespace}")

    if endpoint:
        parts.append(f"via {endpoint}")

    if code:
        parts.append(f"→ HTTP {code} ({outcome})")
    else:
        parts.append(f"({outcome})")

    return " ".join(parts)


def short_cluster_label(obj: Any) -> str:
    """
    Compact label for tables / Mermaid nodes.
    """
    ev = _normalize_event_like(obj)

    if not ev:
        return "unknown cluster"

    actor = ev.get("actor") or "unknown"
    verb = ev.get("verb") or "op"
    resource = ev.get("resource") or "resource"
    code = ev.get("response_code") or "?"

    return f"{actor} {verb} {resource} → {code}"