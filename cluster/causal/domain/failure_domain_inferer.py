from __future__ import annotations

from collections import Counter
from typing import Dict, List, Any


def infer_failure_domain(events: List[Dict[str, Any]]) -> str:
    if not events:
        return "unknown"

    response_codes = [e.get("response_code") for e in events if e.get("response_code")]
    actors = [e.get("actor") for e in events if e.get("actor")]
    resources = [e.get("resource") for e in events if e.get("resource")]

    code_counts = Counter(response_codes)
    actor_counts = Counter(actors)
    resource_counts = Counter(resources)

    dominant_code = code_counts.most_common(1)[0][0] if code_counts else None

    scores = {
        "rbac_authorization": 0,
        "authentication": 0,
        "resource_missing": 0,
        "control_plane": 0,
        "admission_controller": 0,
        "scheduler": 0,
        "node_kubelet": 0,
        "networking": 0,
        "storage": 0,
    }

    # ---- Response code signals --------------------------------------

    if dominant_code == 403:
        scores["rbac_authorization"] += 3

    if dominant_code == 401:
        scores["authentication"] += 3

    if dominant_code == 404:
        scores["resource_missing"] += 2  # weaker than 403

    if dominant_code and int(dominant_code) >= 500:
        scores["control_plane"] += 3

    # ---- Actor signals ----------------------------------------------

    for actor in actor_counts:
        if "system:node" in actor:
            scores["node_kubelet"] += 2
            scores["rbac_authorization"] += 1

        if "gatekeeper" in actor:
            scores["admission_controller"] += 3

        if "kube-scheduler" in actor:
            scores["scheduler"] += 3

        if "kube-apiserver" in actor:
            scores["control_plane"] += 2

    # ---- Resource signals -------------------------------------------

    for r in resource_counts:
        if r in ["configmaps", "secrets"]:
            scores["rbac_authorization"] += 1

        if r in ["constrainttemplates", "assign", "assignmetadata"]:
            scores["admission_controller"] += 2

        if r in ["pods", "nodes"]:
            scores["scheduler"] += 1

        if "volume" in r:
            scores["storage"] += 2

    # ---- Systemic spread bonus --------------------------------------

    if len(resource_counts) >= 3:
        scores["rbac_authorization"] += 1

    # ---- Select best ------------------------------------------------

    domain = max(scores.items(), key=lambda x: x[1])[0]

    if scores[domain] == 0:
        return "unknown"

    return domain