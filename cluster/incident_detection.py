import json
from collections import defaultdict
from datetime import datetime, timedelta


def _parse_ts(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def _load_events(events_path):
    events = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    return events

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
    Deterministic incident detection using 'trigger waves'.

    - Build time buckets.
    - Bucket anomaly score = sum(trigger_score(cluster)) for clusters active in bucket
      (weighted by error_count influence implicitly captured in trigger_score/severity).
    - Incident = contiguous region of buckets where anomaly score >= threshold,
      ending after cooldown_buckets below threshold.

    Produces incidents.json (canonical).
    """

    events = _load_events(events_path)

    with open(event_cluster_map_path, "r", encoding="utf-8") as f:
        event_cluster_map = json.load(f)

    with open(cluster_trigger_stats_path, "r", encoding="utf-8") as f:
        trig = json.load(f)

    # Prepare event tuples (ts, cid)
    tuples = []
    for e in events:
        ts = e.get("timestamp")
        eid = e.get("event_id")
        if not ts or not eid:
            continue
        cid = event_cluster_map.get(eid)
        if not cid:
            continue
        try:
            t = _parse_ts(ts)
        except Exception:
            continue
        tuples.append((t, cid))

    if not tuples:
        raise RuntimeError("[incident_detection] No usable (timestamp, cluster) events found.")

    tuples.sort(key=lambda x: x[0])
    t0 = tuples[0][0]

    # Build buckets: bucket_index -> set(cluster_ids)
    bucket_clusters = defaultdict(set)
    bucket_event_counts = defaultdict(int)

    for t, cid in tuples:
        b = int((t - t0).total_seconds() // bucket_seconds)
        bucket_clusters[b].add(cid)
        bucket_event_counts[b] += 1

    # Compute bucket anomaly score
    bucket_score = {}
    for b, cids in bucket_clusters.items():
        scores = []

        for cid in cids:
            cs = trig.get(cid, {})
            trigger = float(cs.get("trigger_score", 0.0))

            if trigger < signal_trigger_threshold:
                continue

            error_count = cs.get("error_count", 1)
            scores.append(trigger * min(3, error_count))

        bucket_score[b] = max(scores) if scores else 0.0

    # Segment into incidents
    buckets = sorted(bucket_score.keys())
    incidents = []

    in_incident = False
    start_b = None
    last_above_b = None
    below_run = 0

    def bucket_start_time(b):  # inclusive
        return t0 + timedelta(seconds=b * bucket_seconds)

    def bucket_end_time(b):  # exclusive-ish
        return t0 + timedelta(seconds=(b + 1) * bucket_seconds)

    for b in buckets:
        s = bucket_score[b]
        prev = bucket_score.get(b - 1, 0.0)

        delta = s - prev

        above = (
                s >= bucket_anomaly_threshold
                and delta >= 0.05
        )

        if above:
            if not in_incident:
                in_incident = True
                start_b = b
                below_run = 0
            last_above_b = b
            below_run = 0
        else:
            if in_incident:
                below_run += 1
                # close after cooldown
                if below_run >= cooldown_buckets:
                    end_b = last_above_b
                    incidents.append((start_b, end_b))
                    in_incident = False
                    start_b = None
                    last_above_b = None
                    below_run = 0

    # close trailing incident
    if in_incident and start_b is not None and last_above_b is not None:
        incidents.append((start_b, last_above_b))

    # Build incident objects
    out = []
    for i, (sb, eb) in enumerate(incidents, start=1):
        start_t = bucket_start_time(sb)
        end_t = bucket_end_time(eb)

        # guardrail: max incident length
        if (end_t - start_t).total_seconds() > max_incident_seconds:
            # truncate deterministically
            end_t = start_t + timedelta(seconds=max_incident_seconds)

        # collect clusters involved (from buckets within [sb, eb])
        clusters_in_inc = set()
        for b in range(sb, eb + 1):
            clusters_in_inc.update(bucket_clusters.get(b, set()))

        # seed clusters: highest trigger_score among clusters_in_inc above threshold
        scored = []
        for cid in clusters_in_inc:
            ts = trig.get(cid, {})
            score = float(ts.get("trigger_score", 0.0))
            if score >= seed_trigger_threshold:
                scored.append((score, cid))
        scored.sort(reverse=True)
        seed_clusters = [cid for _, cid in scored[:max_seeds]]

        # if nothing qualifies, still pick top 1 by trigger_score to avoid empty seeds
        if not seed_clusters:
            all_scored = [(float(trig.get(cid, {}).get("trigger_score", 0.0)), cid) for cid in clusters_in_inc]
            all_scored.sort(reverse=True)
            seed_clusters = [all_scored[0][1]] if all_scored else []

        incident_id = f"I{i}"
        out.append({
            "incident_id": incident_id,
            "start_time": start_t.isoformat(),
            "end_time": end_t.isoformat(),
            "bucket_seconds": bucket_seconds,
            "seed_clusters": seed_clusters,
            "clusters": sorted(clusters_in_inc),
            "bucket_range": [sb, eb],
            "bucket_anomaly_threshold": bucket_anomaly_threshold,
        })

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(out, f, indent=2)

    print(f"[incident_detection] incidents={len(out)} -> {output_path}")