# /cluster/incident_detection.py
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from typing import Dict, Any, List, Optional


RETRY_KEYWORDS = [
    "retry",
    "retrying",
    "failed to connect",
    "connection refused",
    "timeout",
    "timed out",
    "unable to connect",
    "reinitializing",
    "backoff",
]


def _parse_ts(ts: Optional[str]) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None

    s = ts.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)
    except Exception:
        return None


def _load_events(events_path: str) -> List[Dict[str, Any]]:
    events: List[Dict[str, Any]] = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                events.append(json.loads(line))
    return events


def detect_service_retry_loop_incidents(
    events: List[Dict[str, Any]],
    event_cluster_map: Dict[str, str],
    min_events: int = 3,
    max_window_seconds: int = 900,
    min_error_events: int = 2,
    min_retry_hits: int = 2,
) -> List[Dict[str, Any]]:
    """
    Fallback detector for single-service retry-loop incidents.

    This is intentionally simple and conservative:
      - group events by service
      - require repeated events in a bounded window
      - require repeated failure/retry semantics
      - produce one incident per affected service
    """

    service_events: Dict[str, List[Dict[str, Any]]] = defaultdict(list)

    for e in events:
        svc = e.get("service") or e.get("actor")
        if svc:
            service_events[svc].append(e)

    out: List[Dict[str, Any]] = []

    for service, evs in service_events.items():
        if len(evs) < min_events:
            continue

        ts_list: List[datetime] = []
        error_count = 0
        warn_count = 0
        http5xx = 0
        retry_hits = 0
        clusters = set()

        for ev in evs:
            ts = _parse_ts(ev.get("timestamp"))
            if ts:
                ts_list.append(ts)

            sev = (ev.get("severity") or "").upper()
            if sev == "ERROR":
                error_count += 1
            elif sev == "WARN":
                warn_count += 1

            if ev.get("http_class") == "5xx":
                http5xx += 1

            txt = (ev.get("normalized_text") or ev.get("raw_text") or "").lower()
            if any(k in txt for k in RETRY_KEYWORDS):
                retry_hits += 1

            eid = ev.get("event_id")
            cid = event_cluster_map.get(eid)
            if cid:
                clusters.add(cid)

        if not ts_list:
            continue

        start_ts = min(ts_list)
        end_ts = max(ts_list)

        if (end_ts - start_ts).total_seconds() > max_window_seconds:
            continue

        strong_failure = (
            error_count >= min_error_events
            or warn_count >= 3
            or http5xx >= 1
        )
        retry_pattern = retry_hits >= min_retry_hits

        if not (strong_failure and retry_pattern):
            continue

        out.append(
            {
                "service": service,
                "start_time": start_ts,
                "end_time": end_ts,
                "clusters": sorted(clusters),
            }
        )

    return out


def run_incident_detection(
    events_path,
    event_cluster_map_path,
    cluster_trigger_stats_path,
    output_path,
    bucket_seconds=10,
    seed_trigger_threshold=0.20,
    signal_trigger_threshold=0.18,
    bucket_anomaly_threshold=1.0,
    cooldown_buckets=6,
    max_incident_seconds=1800,
    max_seeds=3,
):
    """
    Deterministic incident detection using trigger waves.

    Primary path:
      - Build time buckets
      - Compute bucket anomaly score from trigger_score(cluster)
      - Segment contiguous anomalous bucket regions into incidents

    Fallback path:
      - If no incidents are formed, detect single-service retry-loop incidents
    """

    events = _load_events(events_path)

    with open(event_cluster_map_path, "r", encoding="utf-8") as f:
        event_cluster_map = json.load(f)

    with open(cluster_trigger_stats_path, "r", encoding="utf-8") as f:
        trig = json.load(f)

    # --------------------------------------------------
    # Prepare ordered (timestamp, cluster_id) tuples
    # --------------------------------------------------
    tuples = []
    for e in events:
        ts = e.get("timestamp")
        eid = e.get("event_id")

        if not ts or not eid:
            continue

        cid = event_cluster_map.get(eid)
        if not cid:
            continue

        t = _parse_ts(ts)
        if not t:
            continue

        tuples.append((t, cid))

    if not tuples:
        raise RuntimeError("[incident_detection] No usable (timestamp, cluster) events found.")

    tuples.sort(key=lambda x: x[0])
    t0 = tuples[0][0]

    # --------------------------------------------------
    # Build time buckets
    # --------------------------------------------------
    bucket_clusters = defaultdict(set)
    bucket_event_counts = defaultdict(int)

    for t, cid in tuples:
        b = int((t - t0).total_seconds() // bucket_seconds)
        bucket_clusters[b].add(cid)
        bucket_event_counts[b] += 1

    # --------------------------------------------------
    # Compute bucket anomaly score
    # --------------------------------------------------
    bucket_score = {}

    for b, cids in bucket_clusters.items():
        scores = []

        for cid in cids:
            cs = trig.get(cid, {})
            trigger = float(cs.get("trigger_score", 0.0))

            if trigger < signal_trigger_threshold:
                continue

            error_count = int(cs.get("error_count", 1))
            scores.append(trigger * min(3, error_count))

        bucket_score[b] = max(scores) if scores else 0.0

    # --------------------------------------------------
    # Segment anomalous bucket waves into incidents
    # --------------------------------------------------
    # --------------------------------------------------
    # Segment anomalous bucket waves into incidents
    # (Causal continuity aware)
    # --------------------------------------------------
    buckets = sorted(bucket_score.keys())
    incidents = []

    in_incident = False
    start_b = None
    last_active_b = None
    below_run = 0

    def bucket_start_time(b):
        return t0 + timedelta(seconds=b * bucket_seconds)

    def bucket_end_time(b):
        return t0 + timedelta(seconds=(b + 1) * bucket_seconds)

    for b in buckets:
        s = bucket_score[b]
        prev = bucket_score.get(b - 1, 0.0)
        delta = s - prev

        # --------------------------------------------------
        # Strong anomaly trigger (same as before)
        # --------------------------------------------------
        strong_signal = (
            s >= bucket_anomaly_threshold
            and delta >= 0.05
        )

        # --------------------------------------------------
        # NEW: weak-but-present signal (continuation)
        #
        # Rationale:
        # - Incidents degrade gradually
        # - Not all buckets will stay above anomaly threshold
        # - We treat mid-level signal as continuation, not termination
        #
        # Not hardcoded:
        # - relative to threshold (0.5x)
        # --------------------------------------------------
        weak_signal = s >= (0.5 * bucket_anomaly_threshold)

        if strong_signal:
            if not in_incident:
                in_incident = True
                start_b = b
                below_run = 0

            last_active_b = b
            below_run = 0

        elif in_incident and weak_signal:
            # --------------------------------------------------
            # Continuation zone
            # --------------------------------------------------
            last_active_b = b
            below_run = 0

        else:
            if in_incident:
                below_run += 1

                if below_run >= cooldown_buckets:
                    # --------------------------------------------------
                    # Final check before closing:
                    # ensure signal truly disappeared
                    #
                    # Rationale:
                    # - Prevent splitting same causal incident
                    # - Uses recent window (no hardcoding)
                    # --------------------------------------------------
                    recent_window = range(max(start_b, b - cooldown_buckets), b + 1)

                    sustained_signal = any(
                        bucket_score.get(rb, 0.0) >= (0.5 * bucket_anomaly_threshold)
                        for rb in recent_window
                    )

                    if sustained_signal:
                        # Continue incident
                        below_run = 0
                        continue

                    # True recovery → close incident
                    end_b = last_active_b
                    incidents.append((start_b, end_b))

                    in_incident = False
                    start_b = None
                    last_active_b = None
                    below_run = 0

    # Close open incident
    if in_incident and start_b is not None and last_active_b is not None:
        incidents.append((start_b, last_active_b))

    # --------------------------------------------------
    # Build incident objects from trigger-wave incidents
    # --------------------------------------------------
    out = []

    for i, (sb, eb) in enumerate(incidents, start=1):
        start_t = bucket_start_time(sb)
        end_t = bucket_end_time(eb)

        if (end_t - start_t).total_seconds() > max_incident_seconds:
            end_t = start_t + timedelta(seconds=max_incident_seconds)

        clusters_in_inc = set()
        for b in range(sb, eb + 1):
            clusters_in_inc.update(bucket_clusters.get(b, set()))

        scored = []
        for cid in clusters_in_inc:
            ts = trig.get(cid, {})
            score = float(ts.get("trigger_score", 0.0))
            if score >= seed_trigger_threshold:
                scored.append((score, cid))

        scored.sort(reverse=True)
        seed_clusters = [cid for _, cid in scored[:max_seeds]]

        if not seed_clusters:
            all_scored = [
                (float(trig.get(cid, {}).get("trigger_score", 0.0)), cid)
                for cid in clusters_in_inc
            ]
            all_scored.sort(reverse=True)
            seed_clusters = [all_scored[0][1]] if all_scored else []

        out.append(
            {
                "incident_id": f"I{i}",
                "start_time": start_t.isoformat(),
                "end_time": end_t.isoformat(),
                "bucket_seconds": bucket_seconds,
                "seed_clusters": seed_clusters,
                "clusters": sorted(clusters_in_inc),
                "bucket_range": [sb, eb],
                "bucket_anomaly_threshold": bucket_anomaly_threshold,
            }
        )

    # --------------------------------------------------
    # Fallback: single-service retry-loop incidents
    # --------------------------------------------------
    if not out:
        retry_incidents = detect_service_retry_loop_incidents(
            events,
            event_cluster_map,
            min_events=3,
            max_window_seconds=900,
            min_error_events=2,
            min_retry_hits=2,
        )

        for i, r in enumerate(retry_incidents, start=1):
            seed_clusters = r["clusters"][:max_seeds] if r["clusters"] else []

            out.append(
                {
                    "incident_id": f"I{i}",
                    "start_time": r["start_time"].isoformat(),
                    "end_time": r["end_time"].isoformat(),
                    "incident_type": "single_service_retry_loop",
                    "seed_clusters": seed_clusters,
                    "clusters": r["clusters"],
                    "service": r["service"],
                    "detection_method": "retry_loop_fallback",
                }
            )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"[incident_detection] incidents={len(out)} -> {output_path}")