import json
import math
from collections import defaultdict
from collections import Counter
from datetime import datetime


def _parse_ts(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


def load_events(path):
    events = []
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    return events


def run_trigger_analysis(
    events_path,
    clusters_path,
    event_cluster_map_path,
    output_path
):

    events = load_events(events_path)

    with open(clusters_path, "r", encoding="utf-8") as f:
        clusters = json.load(f)

    with open(event_cluster_map_path, "r", encoding="utf-8") as f:
        event_cluster_map = json.load(f)

    # -------------------------------------
    # Global baseline window
    # -------------------------------------

    timestamps = []

    for e in events:
        ts = e.get("timestamp")
        if ts:
            try:
                timestamps.append(_parse_ts(ts))
            except Exception:
                pass

    if not timestamps:
        raise RuntimeError("No timestamps found in events")

    global_start = min(timestamps)
    global_end = max(timestamps)

    total_duration = max(1.0, (global_end - global_start).total_seconds())
    total_events = len(events)

    global_rate = total_events / total_duration

    # -------------------------------------
    # Collect cluster stats
    # -------------------------------------

    cluster_event_count = defaultdict(int)
    cluster_error_count = defaultdict(int)
    cluster_times = defaultdict(list)

    # ✅ NEW
    cluster_actors = defaultdict(Counter)
    cluster_resources = defaultdict(Counter)

    for e in events:

        event_id = e.get("event_id")
        cid = event_cluster_map.get(event_id)

        if not cid:
            continue

        cluster_event_count[cid] += 1

        # ------------------------
        # Response code
        # ------------------------
        rc = (
            e.get("response_code")
            or e.get("status_code")
            or e.get("code")
        )

        if rc is None:
            resp = e.get("responseStatus") or {}
            rc = resp.get("code")

        try:
            rc = int(rc)
        except Exception:
            rc = 0

        if rc >= 400:
            cluster_error_count[cid] += 1

        # ------------------------
        # Timestamp
        # ------------------------
        ts = e.get("timestamp")
        if ts:
            try:
                cluster_times[cid].append(_parse_ts(ts))
            except Exception:
                pass

        # ------------------------
        # ✅ NEW: Actor tracking
        # ------------------------
        actor = e.get("actor") or e.get("service")
        if actor:
            cluster_actors[cid][actor] += 1

        resource = e.get("resource")
        if resource:
            cluster_resources[cid][resource] += 1

    # -------------------------------------
    # Compute trigger metrics
    # -------------------------------------

    results = {}

    for cid in clusters.keys():

        n = cluster_event_count.get(cid, 0)

        if n == 0:
            continue

        times = cluster_times[cid]

        if times:
            first = min(times)
            last = max(times)
            duration = max(1.0, (last - first).total_seconds())
        else:
            first = None
            last = None
            duration = total_duration

        # -------------------------------------
        # Core signals
        # -------------------------------------

        cluster_rate = n / duration
        burst_factor = cluster_rate / max(global_rate, 1e-9)

        errors = cluster_error_count.get(cid, 0)
        error_rate = errors / n if n > 0 else 0.0

        # -------------------------------------
        # Severity weighting
        # -------------------------------------

        if error_rate >= 0.8:
            severity = 3.0
        elif error_rate >= 0.4:
            severity = 2.0
        elif error_rate >= 0.1:
            severity = 1.0
        else:
            severity = 0.2

        # -------------------------------------
        # Base trigger score (existing logic)
        # -------------------------------------

        trigger_score = severity * (1 + math.log1p(burst_factor)) * error_rate

        # -------------------------------------
        # ✅ NEW: scale (event_count influence)
        # -------------------------------------

        scale = min(1.0, math.log1p(n) / 5.0)

        # -------------------------------------
        # ✅ NEW: systemic spread (actor diversity)
        # -------------------------------------

        actor_div = len(cluster_actors[cid].keys())
        spread = min(1.0, actor_div / 3.0)

        dominant_actor = None
        if cluster_actors[cid]:
            dominant_actor = cluster_actors[cid].most_common(1)[0][0]

        dominant_resource = None
        if cluster_resources[cid]:
            dominant_resource = cluster_resources[cid].most_common(1)[0][0]

        # -------------------------------------
        # ✅ Adjust trigger score (light modulation)
        # -------------------------------------

        adjusted_trigger = trigger_score
        adjusted_trigger *= (0.5 + 0.5 * scale)
        adjusted_trigger *= (0.7 + 0.3 * spread)

        adjusted_trigger = adjusted_trigger / (1 + adjusted_trigger)
        adjusted_trigger = round(adjusted_trigger, 6)

        # -------------------------------------
        # Output
        # -------------------------------------

        results[cid] = {

            "first_seen": first.isoformat() if first else None,
            "last_seen": last.isoformat() if last else None,

            "event_count": n,
            "error_count": errors,

            "duration_seconds": duration,

            "error_rate": round(error_rate, 6),
            "severity": severity,

            "cluster_rate_eps": round(cluster_rate, 6),
            "global_rate_eps": round(global_rate, 6),

            "burst_factor": round(burst_factor, 6),

            # Backward compatibility
            "trigger_score_raw": round(trigger_score, 6),

            # Final score
            "trigger_score": adjusted_trigger,

            # ✅ New signals
            "scale": round(scale, 6),
            "actor_diversity": actor_div,
            "systemic_spread": round(spread, 6),
            "actor": dominant_actor,
            "resource": dominant_resource,

        }

    # -------------------------------------
    # Candidate selection (STABLE)
    # -------------------------------------

    if not results:
        raise RuntimeError("[trigger_analysis] No clusters produced")

    for cid, s in results.items():
        s["is_candidate"] = (
                s["trigger_score"] >= 0.15
                or s["error_count"] >= 3
        )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(
        f"[trigger_analysis] clusters={len(results)} "
        f"global_rate_eps={global_rate:.4f} -> {output_path}"
    )