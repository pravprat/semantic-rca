from collections import Counter
from typing import Dict, Any, List



def _safe_str(v: Any) -> str:
    if v is None:
        return ""
    return str(v).strip()


def _dominant_value(values: List[str]) -> str:
    cleaned = [v for v in (_safe_str(x) for x in values) if v]
    if not cleaned:
        return ""
    return Counter(cleaned).most_common(1)[0][0]


def extract_cluster_behavior(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> Dict[str, Any]:

    member_indices = cluster.get("member_indices") or cluster.get("event_indices") or []

    actors = []
    verbs = []
    resources = []
    statuses = []

    for idx in member_indices:

        if not (0 <= idx < len(events)):
            continue

        ev = events[idx]

        actor = (
            ev.get("actor")
            or ev.get("user")
            or ev.get("principal")
            or ev.get("service")
            or ""
        )

        verb = (
            ev.get("verb")
            or ev.get("operation")
            or ev.get("action")
            or ""
        )

        resource = (
            ev.get("resource")
            or ev.get("object")
            or ev.get("kind")
            or ""
        )

        status = (
            ev.get("response_code")
            or ev.get("status")
            or ev.get("code")
            or ""
        )

        actors.append(_safe_str(actor))
        verbs.append(_safe_str(verb))
        resources.append(_safe_str(resource))
        statuses.append(_safe_str(status))

    dominant_actor = _dominant_value(actors)
    dominant_operation = _dominant_value(verbs)
    dominant_resource = _dominant_value(resources)
    dominant_status = _dominant_value(statuses)

    frequency = len(member_indices)

    behavior_signature = (
        f"{dominant_actor}|{dominant_operation}|{dominant_resource}|{dominant_status}"
    )

    cluster_behavior = _cluster_behavior_text(
        dominant_actor,
        dominant_operation,
        dominant_resource,
        dominant_status
    )

    return {
        "cluster_behavior": cluster_behavior,
        "dominant_actor": dominant_actor,
        "dominant_operation": dominant_operation,
        "dominant_resource": dominant_resource,
        "dominant_status": dominant_status,
        "frequency": frequency,
        "behavior_signature": behavior_signature,
    }


def _cluster_behavior_text(actor, op, resource, status):

    actor_l = actor.lower()
    resource_l = resource.lower()
    status_l = str(status)

    if "gatekeeper" in actor_l and "assignmetadata" in resource_l:
        return "Gatekeeper admission mutation failures"

    if "gatekeeper" in actor_l and "constrainttemplate" in resource_l:
        return "Gatekeeper policy evaluation failures"

    if "system:node:" in actor_l and status_l.startswith("403"):
        return "Node RBAC permission failures"

    if "serviceaccount" in resource_l and status_l.startswith("404"):
        return "Service account lookup failures"

    if "rolebinding" in resource_l and status_l.startswith("404"):
        return "RBAC rolebinding lookup failures"

    if "secret" in resource_l and status_l.startswith("404"):
        return "Secret lookup failures"

    parts = []

    if actor:
        parts.append(actor.split(":")[-1])

    if op:
        parts.append(op)

    if resource:
        parts.append(resource)

    base = " ".join(parts).strip()

    if not base:
        base = "cluster behavior"

    status_s = str(status)

    if status_s.startswith("5"):
        return f"{base} server failures (HTTP {status})"

    if status_s.startswith("4"):
        return f"{base} client failures (HTTP {status})"

    if status_s.startswith("2"):
        return f"{base} operations (HTTP {status})"

    return f"{base} activity"