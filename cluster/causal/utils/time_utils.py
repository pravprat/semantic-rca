# cluster/causal/utils/time_utils.py

from __future__ import annotations

from datetime import datetime, timezone
from typing import Optional


def parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts:
        return None
    try:
        s = ts.replace("Z", "+00:00")
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def seconds_between(a: datetime, b: datetime) -> float:
    return (b - a).total_seconds()