from __future__ import annotations

import math
from dataclasses import dataclass
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple


# -----------------------------
# Time parsing (robust)
# -----------------------------
def parse_ts(value: Any) -> Optional[datetime]:
    """
    Accepts:
      - ISO8601 strings with Z
      - epoch seconds (int/float)
      - datetime
    Returns UTC-aware datetime or None.
    """
    if value is None:
        return None
    if isinstance(value, datetime):
        return value if value.tzinfo else value.replace(tzinfo=timezone.utc)
    if isinstance(value, (int, float)):
        try:
            return datetime.fromtimestamp(float(value), tz=timezone.utc)
        except Exception:
            return None
    if isinstance(value, str):
        s = value.strip()
        try:
            if s.endswith("Z"):
                s = s[:-1] + "+00:00"
            return datetime.fromisoformat(s).astimezone(timezone.utc)
        except Exception:
            return None
    return None


def floor_to_bucket(ts: datetime, bucket_seconds: int) -> datetime:
    epoch = int(ts.timestamp())
    floored = epoch - (epoch % bucket_seconds)
    return datetime.fromtimestamp(floored, tz=timezone.utc)


# -----------------------------
# Event access helpers
# -----------------------------
def get_event_ts(ev) -> Optional[datetime]:
    if hasattr(ev, "timestamp"):
        dt = parse_ts(getattr(ev, "timestamp"))
        if dt:
            return dt

    if isinstance(ev, dict):
        for k in ("ts", "time", "timestamp", "@timestamp"):
            if k in ev:
                dt = parse_ts(ev.get(k))
                if dt:
                    return dt
    return None


# -----------------------------
# Core temporal features
# -----------------------------
@dataclass
class ClusterTemporalStats:
    cluster_id: str
    first_seen: datetime
    last_seen: datetime
    total_events: int
    bucket_seconds: int
    series_start: datetime
    series_end: datetime
    counts: List[int]
    peak_count: int
    peak_bucket_start: datetime
    burst_onset_bucket_start: datetime
    accel_score: float
    earliness_score: float


def build_count_series(
    ts_list: List[datetime],
    bucket_seconds: int,
    window_start: datetime,
    window_end: datetime,
) -> Tuple[datetime, datetime, List[int]]:
    start = floor_to_bucket(window_start, bucket_seconds)
    end = floor_to_bucket(window_end, bucket_seconds)
    if end < start:
        end = start

    n = int((end.timestamp() - start.timestamp()) // bucket_seconds) + 1
    counts = [0] * n

    for ts in ts_list:
        if ts < window_start or ts > window_end:
            continue
        b = floor_to_bucket(ts, bucket_seconds)
        idx = int((b.timestamp() - start.timestamp()) // bucket_seconds)
        if 0 <= idx < n:
            counts[idx] += 1

    return start, end, counts


def burst_onset(counts: List[int], peak: int) -> int:
    if not counts:
        return 0
    thresh = max(2, int(math.ceil(0.2 * peak)))
    for i, c in enumerate(counts):
        if c >= thresh:
            return i
    return 0


def accel_heuristic(counts: List[int]) -> float:
    if len(counts) < 2:
        return 0.0
    diffs = [counts[i] - counts[i - 1] for i in range(1, len(counts))]
    mx = max(diffs) if diffs else 0
    total = max(1, sum(counts))
    return float(mx) / float(total)


def compute_cluster_temporal_stats(
    cluster_id: str,
    ts_list: List[datetime],
    bucket_seconds: int,
    incident_start: datetime,
    incident_end: datetime,
) -> ClusterTemporalStats:
    ts_list = sorted(ts_list)
    first_seen = ts_list[0]
    last_seen = ts_list[-1]

    series_start, series_end, counts = build_count_series(
        ts_list=ts_list,
        bucket_seconds=bucket_seconds,
        window_start=incident_start,
        window_end=incident_end,
    )

    peak_count = max(counts) if counts else 0
    peak_idx = counts.index(peak_count) if counts else 0
    peak_bucket_start = series_start + timedelta(seconds=bucket_seconds * peak_idx)

    onset_idx = burst_onset(counts, peak_count)
    burst_onset_bucket_start = series_start + timedelta(seconds=bucket_seconds * onset_idx)

    accel_score = accel_heuristic(counts)

    span = max(1.0, (incident_end - incident_start).total_seconds())
    earliness = 1.0 - ((first_seen - incident_start).total_seconds() / span)
    earliness = max(0.0, min(1.0, earliness))

    return ClusterTemporalStats(
        cluster_id=cluster_id,
        first_seen=first_seen,
        last_seen=last_seen,
        total_events=len(ts_list),
        bucket_seconds=bucket_seconds,
        series_start=series_start,
        series_end=series_end,
        counts=counts,
        peak_count=peak_count,
        peak_bucket_start=peak_bucket_start,
        burst_onset_bucket_start=burst_onset_bucket_start,
        accel_score=accel_score,
        earliness_score=earliness,
    )


def pearson_corr(a: List[int], b: List[int]) -> float:
    if len(a) != len(b) or not a:
        return 0.0
    n = len(a)
    ma = sum(a) / n
    mb = sum(b) / n
    num = sum((a[i] - ma) * (b[i] - mb) for i in range(n))
    da = sum((a[i] - ma) ** 2 for i in range(n))
    db = sum((b[i] - mb) ** 2 for i in range(n))
    den = math.sqrt(da * db) if da > 0 and db > 0 else 0.0
    return float(num / den) if den else 0.0


def best_lag_corr(a: List[int], b: List[int], max_lag_buckets: int) -> Tuple[int, float]:
    """
    lag > 0 means a leads b by lag buckets.
    """
    best_lag = 0
    best_corr = -1.0

    n = len(a)
    if n == 0 or len(b) != n:
        return (0, 0.0)

    for lag in range(1, max_lag_buckets + 1):
        aa = a[: n - lag]
        bb = b[lag:]
        if len(aa) < 3:
            break
        c = pearson_corr(aa, bb)
        if c > best_corr:
            best_corr = c
            best_lag = lag

    return best_lag, best_corr


def normalize01(x: float, lo: float, hi: float) -> float:
    if hi <= lo:
        return 0.0
    return max(0.0, min(1.0, (x - lo) / (hi - lo)))


def compute_trigger_scores(
    temporal_stats: Dict[str, ClusterTemporalStats],
    influence_ratio_by_cluster: Dict[str, float],
) -> Dict[str, float]:
    accel_vals = [s.accel_score for s in temporal_stats.values()]
    infl_vals = [influence_ratio_by_cluster.get(cid, 0.0) for cid in temporal_stats.keys()]

    accel_lo, accel_hi = (min(accel_vals), max(accel_vals)) if accel_vals else (0.0, 1.0)
    infl_lo, infl_hi = (min(infl_vals), max(infl_vals)) if infl_vals else (0.0, 1.0)

    scores: Dict[str, float] = {}
    for cid, s in temporal_stats.items():
        e = s.earliness_score
        a = normalize01(s.accel_score, accel_lo, accel_hi)
        infl = normalize01(influence_ratio_by_cluster.get(cid, 0.0), infl_lo, infl_hi)
        score = 0.50 * e + 0.30 * a + 0.20 * infl
        scores[cid] = float(score)

    return scores


def infer_cascade_edges(
    temporal_stats: Dict[str, ClusterTemporalStats],
    trigger_scores: Dict[str, float],
    max_lag_buckets: int = 10,
    min_corr: float = 0.25,
    top_k: int = 15,
) -> List[Dict[str, Any]]:
    clusters_sorted = sorted(trigger_scores.items(), key=lambda x: x[1], reverse=True)
    candidates = [cid for cid, _ in clusters_sorted[:top_k]]

    edges: List[Dict[str, Any]] = []
    for a in candidates:
        for b in candidates:
            if a == b:
                continue
            sa = temporal_stats.get(a)
            sb = temporal_stats.get(b)
            if not sa or not sb:
                continue
            if len(sa.counts) != len(sb.counts):
                continue

            lag, corr = best_lag_corr(sa.counts, sb.counts, max_lag_buckets=max_lag_buckets)
            if lag <= 0:
                continue
            if corr < min_corr:
                continue

            edges.append({
                "from": a,
                "to": b,
                "relation": "precedes",
                "weight": float(corr),
                "lag_buckets": int(lag),
                "lag_seconds": int(lag * sa.bucket_seconds),
                "evidence": "burst_correlation",
                "from_trigger_score": float(trigger_scores.get(a, 0.0)),
                "to_trigger_score": float(trigger_scores.get(b, 0.0)),
            })

    edges.sort(key=lambda e: (e["weight"], e["from_trigger_score"]), reverse=True)
    return edges


def temporal_semantic_inference(
    *,
    events: List[Dict[str, Any]],
    event_to_cluster: Dict[str, str],
    influence_ratio_by_cluster: Dict[str, float],
    bucket_seconds: int = 60,
) -> Dict[str, Any]:
    ts_all: List[datetime] = []
    for ev in events:
        ts = get_event_ts(ev)
        if ts:
            ts_all.append(ts)
    if not ts_all:
        return {"error": "No timestamps available for temporal inference."}

    incident_start = min(ts_all)
    incident_end = max(ts_all)

    cluster_ts: Dict[str, List[datetime]] = {}
    for ev in events:
        ev_id = ev.get("event_id") or ev.get("id") or ev.get("uid")
        if not ev_id:
            continue
        cid = event_to_cluster.get(str(ev_id))
        if not cid:
            continue
        ts = get_event_ts(ev)
        if not ts:
            continue
        cluster_ts.setdefault(cid, []).append(ts)

    temporal_stats: Dict[str, ClusterTemporalStats] = {}
    for cid, ts_list in cluster_ts.items():
        if len(ts_list) < 2:
            continue
        temporal_stats[cid] = compute_cluster_temporal_stats(
            cluster_id=cid,
            ts_list=ts_list,
            bucket_seconds=bucket_seconds,
            incident_start=incident_start,
            incident_end=incident_end,
        )

    trigger_scores = compute_trigger_scores(temporal_stats, influence_ratio_by_cluster)
    edges = infer_cascade_edges(temporal_stats, trigger_scores)

    stats_out = {}
    for cid, s in temporal_stats.items():
        stats_out[cid] = {
            "first_seen": s.first_seen.isoformat().replace("+00:00", "Z"),
            "last_seen": s.last_seen.isoformat().replace("+00:00", "Z"),
            "total_events": s.total_events,
            "bucket_seconds": s.bucket_seconds,
            "peak_count_per_bucket": s.peak_count,
            "peak_bucket_start": s.peak_bucket_start.isoformat().replace("+00:00", "Z"),
            "burst_onset_bucket_start": s.burst_onset_bucket_start.isoformat().replace("+00:00", "Z"),
            "accel_score": s.accel_score,
            "earliness_score": s.earliness_score,
            "trigger_score": trigger_scores.get(cid, 0.0),
        }

    return {
        "incident_start": incident_start.isoformat().replace("+00:00", "Z"),
        "incident_end": incident_end.isoformat().replace("+00:00", "Z"),
        "bucket_seconds": bucket_seconds,
        "cluster_temporal_stats": stats_out,
        "cascade_edges": edges,
    }