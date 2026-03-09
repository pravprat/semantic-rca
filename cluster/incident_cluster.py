# semantic-rca/cluster/incident_cluster.py
from __future__ import annotations

from dataclasses import dataclass
from typing import Dict, List, Any, Optional, Tuple
from datetime import datetime, timedelta


@dataclass
class Incident:
    incident_id: str
    start_time: Optional[str]
    end_time: Optional[str]
    cluster_ids: List[str]
    event_ids: List[str]


def build_incidents(
    events: List[Dict[str, Any]],
    event_cluster_map: Dict[str, str],
    max_gap_seconds: int = 120
) -> List[Incident]:
    """
    MVP incident builder:
      - Sort events by timestamp (if available)
      - Group into incidents when consecutive events are within a time gap
      - Incident membership driven by cluster ids (pattern clusters)
    """
    parsed = []
    for e in events:
        ts = _parse_ts(e.get("timestamp"))
        parsed.append((ts, e))

    parsed.sort(key=lambda x: (x[0] is None, x[0] or datetime.min))

    incidents: List[Incident] = []
    current_events: List[Dict[str, Any]] = []
    current_start: Optional[datetime] = None
    current_end: Optional[datetime] = None

    def flush():
        nonlocal current_events, current_start, current_end
        if not current_events:
            return
        clusters = []
        ev_ids = []
        for ev in current_events:
            eid = ev["event_id"]
            ev_ids.append(eid)
            cid = event_cluster_map.get(eid)
            if cid:
                clusters.append(cid)
        uniq_clusters = sorted(list(set(clusters)))
        inc_id = f"I{len(incidents)+1}"
        incidents.append(Incident(
            incident_id=inc_id,
            start_time=current_start.isoformat() if current_start else None,
            end_time=current_end.isoformat() if current_end else None,
            cluster_ids=uniq_clusters,
            event_ids=ev_ids
        ))
        current_events = []
        current_start = None
        current_end = None

    for ts, ev in parsed:
        if ts is None:
            # If no timestamps, just treat as one giant incident block at end
            current_events.append(ev)
            continue

        if not current_events:
            current_events = [ev]
            current_start = ts
            current_end = ts
            continue

        gap = (ts - (current_end or ts)).total_seconds()
        if gap <= max_gap_seconds:
            current_events.append(ev)
            current_end = ts
        else:
            flush()
            current_events = [ev]
            current_start = ts
            current_end = ts

    flush()
    return incidents


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None
    # Accept ISO-ish; if Z suffix, convert
    try:
        if ts.endswith("Z"):
            ts = ts[:-1] + "+00:00"
        return datetime.fromisoformat(ts)
    except Exception:
        return None