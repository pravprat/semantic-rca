# semantic-rca/graph/build_graph.py
from __future__ import annotations

import json
from collections import Counter, defaultdict
from datetime import datetime
from typing import Dict, Any, List, Tuple, Optional


# ---------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------

def _parse_ts(ts: str) -> Optional[datetime]:
    if not ts or not isinstance(ts, str):
        return None

    s = ts.strip()

    if s.endswith("Z"):
        s = s[:-1] + "+00:00"

    try:
        return datetime.fromisoformat(s)
    except Exception:
        return None


def _load_events_jsonl(events_path: str) -> List[Dict[str, Any]]:
    events = []

    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            try:
                events.append(json.loads(line))
            except Exception:
                continue

    return events


# ---------------------------------------------------------------------
# Core causal inference
# ---------------------------------------------------------------------

MAX_BUCKET_CLUSTERS = 5


def build_incident_timebucket_edges(
    *,
    events: List[Dict[str, Any]],
    event_cluster_map: Dict[str, str],
    clusters: Dict[str, Any],
    incident: Dict[str, Any],
    bucket_seconds: int = 10,
    lookahead_buckets: int = 3,
    precedes_decay: float = 0.7,
) -> Tuple[Counter, Counter]:
    """
    Build edges for a single incident using timestamp buckets.

    Returns:
        co_occurs[(a,b)] -> weight
        precedes[(src,dst)] -> weight
    """

    inc_start = _parse_ts(incident.get("start_time"))
    inc_end = _parse_ts(incident.get("end_time"))

    if inc_start is None or inc_end is None:
        return Counter(), Counter()

    buckets: Dict[int, set] = defaultdict(set)

    # -----------------------------------------------------
    # Bucket events within incident window
    # -----------------------------------------------------

    for e in events:

        ts = _parse_ts(e.get("timestamp"))
        if ts is None:
            continue

        if ts < inc_start or ts > inc_end:
            continue

        eid = e.get("event_id")

        if not eid:
            continue

        # tolerate int/str mismatch
        cid = event_cluster_map.get(eid) or event_cluster_map.get(str(eid))

        if not cid:
            continue

        b = int((ts - inc_start).total_seconds() // bucket_seconds)

        buckets[b].add(cid)

    if not buckets:
        return Counter(), Counter()

    co = Counter()
    pre = Counter()

    bucket_ids = sorted(buckets.keys())

    # cluster type lookup
    cluster_types = {
        cid: c.get("cluster_type", "contextual")
        for cid, c in clusters.items()
    }

    # -----------------------------------------------------
    # Co-occurrence edges
    # -----------------------------------------------------

    for b in bucket_ids:

        cids = sorted(buckets[b])

        if len(cids) > MAX_BUCKET_CLUSTERS:
            cids = cids[:MAX_BUCKET_CLUSTERS]

        for i in range(len(cids)):
            for j in range(i + 1, len(cids)):

                a = cids[i]
                b2 = cids[j]

                if a == b2:
                    continue

                x, y = sorted((a, b2))

                co[(x, y)] += 1

    # -----------------------------------------------------
    # Temporal precedence edges
    # -----------------------------------------------------

    for b in bucket_ids:

        srcs = list(buckets[b])[:MAX_BUCKET_CLUSTERS]

        for k in range(1, lookahead_buckets + 1):

            dsts = buckets.get(b + k)

            if not dsts:
                continue

            dsts = list(dsts)[:MAX_BUCKET_CLUSTERS]

            w = precedes_decay ** (k - 1)

            for s in srcs:

                if cluster_types.get(s) == "baseline":
                    continue

                for d in dsts:

                    if s == d:
                        continue

                    if cluster_types.get(d) == "baseline":
                        continue

                    pre[(s, d)] += w

    return co, pre


# ---------------------------------------------------------------------
# Graph builder
# ---------------------------------------------------------------------

def build_semantic_graph_from_incidents(
    *,
    clusters: Dict[str, Any],
    incidents: List[Dict[str, Any]],
    events_path: str,
    event_cluster_map: Dict[str, str],
    bucket_seconds: int = 10,
    lookahead_buckets: int = 3,
) -> Dict[str, Any]:
    """
    Build global cluster graph by aggregating edges from all incidents.
    """

    events = _load_events_jsonl(events_path)

    co_all = Counter()
    pre_all = Counter()

    for inc in incidents:

        co, pre = build_incident_timebucket_edges(
            events=events,
            event_cluster_map=event_cluster_map,
            clusters=clusters,
            incident=inc,
            bucket_seconds=bucket_seconds,
            lookahead_buckets=lookahead_buckets,
        )

        co_all.update(co)
        pre_all.update(pre)

    # -----------------------------------------------------
    # Nodes
    # -----------------------------------------------------

    nodes = []

    for cid, c in clusters.items():
        nodes.append({
            "id": cid,
            "type": "cluster",

            # cluster statistics
            "size": int(c.get("size", len(c.get("member_indices", [])))),
            "cluster_type": c.get("cluster_type", "contextual"),

            # temporal metadata (important for RCA)
            "first_seen_ts": c.get("first_seen_ts"),
            "last_seen_ts": c.get("last_seen_ts"),

            # event count useful for baseline scoring
            "event_count": c.get("event_count", c.get("size"))
        })

    # -----------------------------------------------------
    # Edges
    # -----------------------------------------------------
    total_incidents = max(1, len(incidents))

    edges = []

    # co_occurs
    for (a, b), w in co_all.items():

        edges.append(
            {
                "from": a,
                "to": b,
                "relation": "co_occurs",
                "weight": float(w),
                "confidence": min(1.0, float(w) / total_incidents),
            }
        )

    # precedes
    for (s, d), w in pre_all.items():

        edges.append(
            {
                "from": s,
                "to": d,
                "relation": "precedes",
                "weight": float(w),
                "confidence": min(1.0, float(w) / total_incidents),
            }
        )

    # compute node degrees
    from collections import defaultdict

    out_degree = defaultdict(int)
    in_degree = defaultdict(int)

    for e in edges:
        src = e.get("from")
        dst = e.get("to")

        if src:
            out_degree[src] += 1
        if dst:
            in_degree[dst] += 1

    #attach degrees to nodes
    for n in nodes:
        cid = n["id"]
        n["out_degree"] = out_degree.get(cid, 0)
        n["in_degree"] = in_degree.get(cid, 0)

    return {
        "nodes": nodes,
        "edges": edges,
    }