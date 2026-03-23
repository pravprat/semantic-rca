from __future__ import annotations

import json
from collections import Counter
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional


def load_json(path: Path, default: Any = None) -> Any:
    if not path.exists():
        return default
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def load_jsonl(path: Path) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    if not path.exists():
        return out
    with path.open("r", encoding="utf-8") as f:
        for i, line in enumerate(f):
            line = line.strip()
            if not line:
                continue
            obj = json.loads(line)
            if "index" not in obj:
                obj["index"] = i
            out.append(obj)
    return out


def write_json(path: Path, obj: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)


def write_text(path: Path, text: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text, encoding="utf-8")


def parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    try:
        return datetime.fromisoformat(ts.replace("Z", "+00:00")).astimezone(timezone.utc)
    except Exception:
        return None


def iso(dt: Optional[datetime]) -> Optional[str]:
    if not dt:
        return None
    return dt.astimezone(timezone.utc).isoformat().replace("+00:00", "Z")


def incident_cluster_ids(incident: Dict[str, Any]) -> List[str]:
    for key in ("clusters", "cluster_ids", "cluster_id_list"):
        v = incident.get(key)
        if isinstance(v, list):
            return [str(x) for x in v]
    seed = incident.get("seed_cluster")
    return [str(seed)] if seed else []


def cluster_member_indices(cluster_obj: Dict[str, Any]) -> List[int]:
    vals = cluster_obj.get("member_indices") or cluster_obj.get("event_indices") or []
    out: List[int] = []
    if not isinstance(vals, list):
        return out
    for v in vals:
        try:
            out.append(int(v))
        except Exception:
            continue
    return out


def get_event(events: List[Dict[str, Any]], idx: int) -> Optional[Dict[str, Any]]:
    if 0 <= idx < len(events):
        return events[idx]
    return None


def event_text(ev: Dict[str, Any]) -> str:
    for k in ("raw_text", "message", "msg", "text", "log"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    return ""


def event_service(ev: Dict[str, Any]) -> Optional[str]:
    for k in ("service", "actor"):
        v = ev.get(k)
        if isinstance(v, str) and v.strip():
            return v.strip()
    sf = ev.get("structured_fields")
    if isinstance(sf, dict):
        for k in ("service", "actor"):
            v = sf.get(k)
            if isinstance(v, str) and v.strip():
                return v.strip()
    return None


def event_actor(ev: Dict[str, Any]) -> Optional[str]:
    v = ev.get("actor")
    return v.strip() if isinstance(v, str) and v.strip() else None


def event_verb(ev: Dict[str, Any]) -> Optional[str]:
    v = ev.get("verb")
    return v.strip() if isinstance(v, str) and v.strip() else None


def event_resource(ev: Dict[str, Any]) -> Optional[str]:
    v = ev.get("resource")
    return v.strip() if isinstance(v, str) and v.strip() else None


def event_response_code(ev: Dict[str, Any]) -> Optional[int]:
    for k in ("response_code", "status", "code"):
        v = ev.get(k)
        if isinstance(v, int):
            return v
        if isinstance(v, str):
            try:
                return int(v.strip())
            except Exception:
                pass
    return None


def event_http_class(ev: Dict[str, Any]) -> Optional[str]:
    v = ev.get("http_class")
    if isinstance(v, str) and v.strip():
        return v.strip()
    code = event_response_code(ev)
    if isinstance(code, int):
        return f"{code // 100}xx"
    return None


def event_severity(ev: Dict[str, Any]) -> str:
    v = ev.get("severity") or ev.get("level")
    if isinstance(v, str) and v.strip():
        s = v.strip().upper()
        return "WARN" if s == "WARNING" else s
    code = event_response_code(ev)
    if isinstance(code, int):
        if code >= 500:
            return "ERROR"
        if code in (401, 403):
            return "ERROR"
        if code >= 400:
            return "WARN"
        return "INFO"
    return "UNKNOWN"


def first_and_last_seen(member_indices: List[int], events: List[Dict[str, Any]]) -> tuple[Optional[str], Optional[str]]:
    dts = []
    for idx in member_indices:
        ev = get_event(events, idx)
        if not ev:
            continue
        dt = parse_ts(ev.get("timestamp"))
        if dt:
            dts.append(dt)
    if not dts:
        return None, None
    return iso(min(dts)), iso(max(dts))


def representative_event(member_indices: List[int], events: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    for idx in member_indices:
        ev = get_event(events, idx)
        if ev:
            return ev
    return None


def top_counter_value(counter: Counter) -> Optional[str]:
    if not counter:
        return None
    return counter.most_common(1)[0][0]


def confidence_label(score: float) -> str:
    if score >= 0.80:
        return "high"
    if score >= 0.55:
        return "medium"
    return "low"