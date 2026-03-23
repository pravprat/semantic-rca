from __future__ import annotations

from collections import Counter
from pathlib import Path
from typing import Any, Dict, List, Tuple

from .utils import (
    load_json,
    load_jsonl,
    write_json,
    incident_cluster_ids,
    cluster_member_indices,
    get_event,
    event_text,
    event_service,
    event_actor,
    event_verb,
    event_resource,
    event_response_code,
    event_http_class,
    event_severity,
    first_and_last_seen,
)


# -----------------------------
# Domain + System Intelligence
# -----------------------------

def infer_failure_domain(http_class: str, codes: Counter) -> str:
    codes_set = set(codes.keys())

    if http_class == "4xx" and ("401" in codes_set or "403" in codes_set):
        return "authz"

    if http_class == "5xx":
        return "service_failure"

    return "other"


def is_control_plane(service: str | None) -> bool:
    if not service:
        return False
    return service.startswith("system:")


# -----------------------------
# Core Step 7
# -----------------------------

def build_incident_patterns(
    artifacts_dir: Path,
    out_path: Path | None = None,
) -> Dict[str, Any]:

    artifacts_dir = Path(artifacts_dir)

    incidents = load_json(artifacts_dir / "incidents.json", default=[])
    clusters = load_json(artifacts_dir / "clusters.json", default={})
    events = load_jsonl(artifacts_dir / "events.jsonl")
    trigger_stats = load_json(artifacts_dir / "cluster_trigger_stats.json", default={}) or {}

    output: Dict[str, Any] = {
        "pattern_version": "v2.step7.final",
        "incidents": [],
    }

    for incident in incidents:
        incident_id = str(incident.get("incident_id") or "UNKNOWN")
        cluster_ids = incident_cluster_ids(incident)

        pattern_map: Dict[Tuple, Dict[str, Any]] = {}
        pattern_counter = 0

        for cid in cluster_ids:
            cluster = clusters.get(cid, {})
            member_indices = cluster_member_indices(cluster)

            if not member_indices:
                continue

            # --- aggregators ---
            svc_ctr = Counter()
            actor_ctr = Counter()
            verb_ctr = Counter()
            res_ctr = Counter()
            http_ctr = Counter()
            code_ctr = Counter()
            sev_ctr = Counter()

            examples: List[str] = []

            for idx in member_indices:
                ev = get_event(events, idx)
                if not ev:
                    continue

                service = event_service(ev) or "unknown"
                actor = event_actor(ev) or service
                verb = event_verb(ev) or "unknown"
                resource = event_resource(ev) or "unknown"
                http_class = event_http_class(ev) or "unknown"
                code = event_response_code(ev)
                sev = event_severity(ev)

                svc_ctr[service] += 1
                actor_ctr[actor] += 1
                verb_ctr[verb] += 1
                res_ctr[resource] += 1
                http_ctr[http_class] += 1
                sev_ctr[sev] += 1

                if code is not None:
                    code_ctr[str(code)] += 1

                txt = event_text(ev)
                if txt and txt not in examples and len(examples) < 5:
                    examples.append(txt)

            # --- representative values ---
            service = svc_ctr.most_common(1)[0][0] if svc_ctr else "unknown"
            actor = actor_ctr.most_common(1)[0][0] if actor_ctr else service
            verb = verb_ctr.most_common(1)[0][0] if verb_ctr else "unknown"
            resource = res_ctr.most_common(1)[0][0] if res_ctr else "unknown"
            http_class = http_ctr.most_common(1)[0][0] if http_ctr else "unknown"

            key = (service, verb, resource, http_class)

            if key not in pattern_map:
                pattern_counter += 1

                pattern_map[key] = {
                    "pattern_id": f"P{pattern_counter}",

                    # identity
                    "service": service,
                    "actor": actor,
                    "verb": verb,
                    "resource": resource,
                    "http_class": http_class,

                    # NEW: system intelligence
                    "failure_domain": infer_failure_domain(http_class, code_ctr),
                    "is_control_plane": is_control_plane(service),

                    # aggregation
                    "cluster_ids": [],
                    "event_count": 0,
                    "error_count": 0,
                    "severity_counts": {"ERROR": 0, "WARN": 0, "INFO": 0, "UNKNOWN": 0},
                    "response_codes": Counter(),

                    # temporal
                    "first_seen": None,
                    "last_seen": None,

                    # trigger signal
                    "max_trigger_score": 0.0,

                    # actor spread (CRITICAL for RCA)
                    "unique_actors": set(),

                    # examples
                    "examples": [],
                }

            p = pattern_map[key]

            p["cluster_ids"].append(cid)
            p["event_count"] += len(member_indices)
            p["error_count"] += sev_ctr.get("ERROR", 0)

            for sev, cnt in sev_ctr.items():
                p["severity_counts"][sev] = p["severity_counts"].get(sev, 0) + cnt

            for code, cnt in code_ctr.items():
                p["response_codes"][code] += cnt

            p["max_trigger_score"] = max(
                p["max_trigger_score"],
                float((trigger_stats.get(cid) or {}).get("trigger_score", 0.0)),
            )

            # temporal
            fs, ls = first_and_last_seen(member_indices, events)

            if fs and (p["first_seen"] is None or fs < p["first_seen"]):
                p["first_seen"] = fs

            if ls and (p["last_seen"] is None or ls > p["last_seen"]):
                p["last_seen"] = ls

            # actor diversity
            for a in actor_ctr.keys():
                p["unique_actors"].add(a)

            # examples
            for ex in examples:
                if ex not in p["examples"]:
                    p["examples"].append(ex)

        # finalize patterns
        patterns = []

        for p in pattern_map.values():
            p["actor_diversity"] = len(p["unique_actors"])
            p["unique_actors"] = sorted(list(p["unique_actors"]))

            # convert counters
            p["response_codes"] = dict(p["response_codes"])

            patterns.append(p)

        # sort: (IMPORTANT for downstream reasoning)
        patterns.sort(
            key=lambda x: (
                x["first_seen"] or "",
                -x["error_count"],
                -x["event_count"],
                -x["max_trigger_score"],
            )
        )

        output["incidents"].append(
            {
                "incident_id": incident_id,
                "start_time": incident.get("start_time"),
                "end_time": incident.get("end_time"),
                "cluster_ids": cluster_ids,
                "patterns": patterns,
            }
        )

    final_out = out_path or (artifacts_dir / "incident_patterns.json")
    write_json(final_out, output)

    return output


if __name__ == "__main__":
    build_incident_patterns(Path("outputs"))