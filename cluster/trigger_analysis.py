import json
import math
from collections import defaultdict
from collections import Counter
from datetime import datetime
from event_io import load_events


def _parse_ts(ts: str):
    return datetime.fromisoformat(ts.replace("Z", "+00:00"))


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
    cluster_fallback_error_count = defaultdict(int)
    cluster_times = defaultdict(list)
    cluster_failure_hints = defaultdict(Counter)
    cluster_failure_modes = defaultdict(Counter)
    cluster_response_classes = defaultdict(Counter)
    cluster_dep_targets = defaultdict(Counter)
    cluster_field_coverage = defaultdict(lambda: Counter())
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

        used_http_failure = False
        rc_class = "unknown"
        if rc >= 500:
            rc_class = "5xx"
        elif rc >= 400:
            rc_class = "4xx"
        elif rc >= 200:
            rc_class = "2xx"
        cluster_response_classes[cid][rc_class] += 1

        if rc >= 400:
            cluster_error_count[cid] += 1
            used_http_failure = True

        # Non-HTTP fallback signal for text-heavy logs.
        if not used_http_failure:
            sev = str(e.get("severity") or "").upper()
            status_family = str(e.get("status_family") or "").lower()
            failure_hint = e.get("failure_hint")
            if sev in {"ERROR", "FATAL"} or status_family == "failure" or failure_hint:
                cluster_error_count[cid] += 1
                cluster_fallback_error_count[cid] += 1
                if failure_hint:
                    cluster_failure_hints[cid][str(failure_hint)] += 1

        # failure mode aggregation
        sem = e.get("semantic") if isinstance(e.get("semantic"), dict) else {}
        mode = str((sem or {}).get("failure_mode") or "").strip()
        if not mode:
            if rc >= 500:
                mode = "service_failure"
            elif rc in {401, 403}:
                mode = "authz_failure"
            elif rc == 404:
                mode = "resource_not_found"
            elif rc == 409:
                mode = "conflict"
            elif rc >= 400:
                mode = "client_failure"
            elif e.get("failure_hint"):
                mode = str(e.get("failure_hint"))
            else:
                mode = "unknown"
        cluster_failure_modes[cid][mode] += 1

        # dependency target aggregate (if present)
        sf = e.get("structured_fields") if isinstance(e.get("structured_fields"), dict) else {}
        dep_target = sf.get("target_dependency_service")
        if dep_target:
            cluster_dep_targets[cid][str(dep_target)] += 1

        # signal-quality coverage for step5 stats-only use
        for fld in ("actor", "resource", "verb", "response_code", "failure_hint"):
            if e.get(fld) not in (None, "", []):
                cluster_field_coverage[cid][fld] += 1

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

        # activity shape from per-minute buckets
        minute_ctr = Counter()
        for t in times:
            minute_ctr[t.replace(second=0, microsecond=0)] += 1
        minute_counts = sorted(minute_ctr.values())
        if minute_counts:
            p50 = minute_counts[len(minute_counts) // 2]
            p95 = minute_counts[min(len(minute_counts) - 1, int(len(minute_counts) * 0.95))]
        else:
            p50 = 0
            p95 = 0

        # signal quality score (0..1) from key field presence
        cov = cluster_field_coverage[cid]
        key_fields = ("actor", "resource", "verb", "response_code", "failure_hint")
        field_cov_ratios = {}
        for fld in key_fields:
            field_cov_ratios[fld] = (cov.get(fld, 0) / n) if n > 0 else 0.0
        coverage_score = sum(field_cov_ratios.values()) / len(key_fields)
        low_flags = []
        if field_cov_ratios["actor"] < 0.2:
            low_flags.append("low_actor_coverage")
        if field_cov_ratios["resource"] < 0.2:
            low_flags.append("low_resource_coverage")
        if field_cov_ratios["response_code"] < 0.1:
            low_flags.append("low_response_code_coverage")

        # -------------------------------------
        # Output
        # -------------------------------------

        results[cid] = {

            "first_seen": first.isoformat() if first else None,
            "last_seen": last.isoformat() if last else None,

            "event_count": n,
            "error_count": errors,
            "fallback_error_count": cluster_fallback_error_count.get(cid, 0),
            "failure_hint_diversity": len(cluster_failure_hints[cid]),
            "top_failure_hints": [k for k, _ in cluster_failure_hints[cid].most_common(3)],
            "response_class_counts": {
                "2xx": int(cluster_response_classes[cid].get("2xx", 0)),
                "4xx": int(cluster_response_classes[cid].get("4xx", 0)),
                "5xx": int(cluster_response_classes[cid].get("5xx", 0)),
                "unknown": int(cluster_response_classes[cid].get("unknown", 0)),
            },
            "top_failure_modes": [
                {
                    "mode": m,
                    "count": int(c),
                    "ratio": round((c / n), 6) if n > 0 else 0.0,
                }
                for m, c in cluster_failure_modes[cid].most_common(5)
            ],

            "duration_seconds": duration,
            "activity_shape": {
                "active_span_seconds": duration,
                "events_per_minute_p50": int(p50),
                "events_per_minute_p95": int(p95),
            },

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
            "top_actors": [
                {
                    "actor": a,
                    "count": int(c),
                    "ratio": round((c / n), 6) if n > 0 else 0.0,
                }
                for a, c in cluster_actors[cid].most_common(3)
            ],
            "top_resources": [
                {
                    "resource": r,
                    "count": int(c),
                    "ratio": round((c / n), 6) if n > 0 else 0.0,
                }
                for r, c in cluster_resources[cid].most_common(3)
            ],
            "dependency_targets": [
                {"service": s, "count": int(c)}
                for s, c in cluster_dep_targets[cid].most_common(5)
            ],
            "signal_quality": {
                "coverage_score": round(coverage_score, 6),
                "field_coverage": {k: round(v, 6) for k, v in field_cov_ratios.items()},
                "low_confidence_flags": low_flags,
            },

        }

    # -------------------------------------
    # Candidate selection (STABLE)
    # -------------------------------------

    if not results:
        raise RuntimeError("[trigger_analysis] No clusters produced")

    for cid, s in results.items():
        hint_div = int(s.get("failure_hint_diversity") or 0)
        fallback_errors = int(s.get("fallback_error_count") or 0)
        s["is_candidate"] = (
                s["trigger_score"] >= 0.15
                or s["error_count"] >= 3
                or fallback_errors >= 5
                or (fallback_errors >= 3 and hint_div >= 2)
        )

    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(results, f, indent=2)

    print(
        f"[trigger_analysis] clusters={len(results)} "
        f"global_rate_eps={global_rate:.4f} -> {output_path}"
    )