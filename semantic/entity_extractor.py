from typing import Dict, Any, List
from semantic.component_registry import COMPONENT_PATTERNS, COMPONENT_DOMAINS

def detect_component(text):

    if not text:
        return None, None

    t = text.lower()

    for comp, patterns in COMPONENT_PATTERNS.items():

        for p in patterns:

            if p in t:
                return comp, COMPONENT_DOMAINS.get(comp)

    return None, None

def infer_component_from_actor(actor: str):

    if not actor:
        return None

    if "system:serviceaccount:" in actor:

        parts = actor.split(":")

        if len(parts) >= 4:
            return parts[-1]

    return None

def _infer_status_class(code: Any) -> str:
    try:
        c = int(code)
        if 500 <= c < 600:
            return "5xx"
        if 400 <= c < 500:
            return "4xx"
        if 300 <= c < 400:
            return "3xx"
        if 200 <= c < 300:
            return "2xx"
    except Exception:
        pass
    return "unknown"

def _infer_component(actor: str, resource: str) -> str:

    actor = (actor or "").lower()
    resource = (resource or "").lower()

    # ------------------------------------------------
    # 1. Component registry lookup
    # ------------------------------------------------
    comp, _ = detect_component(actor)
    if comp:
        return comp

    comp, _ = detect_component(resource)
    if comp:
        return comp

    # ------------------------------------------------
    # 2. Actor-based inference
    # ------------------------------------------------
    comp = infer_component_from_actor(actor)
    if comp:
        return comp

    # ------------------------------------------------
    # fallback
    # ------------------------------------------------

    return "Unknown Component"


def _infer_failure_mode(status_class: str, resource: str) -> str:

    if status_class == "5xx":
        return "service_failure"

    if status_class == "4xx":
        if resource:
            return "resource_lookup_failure"
        return "client_request_failure"

    return "normal_operation"


def _build_semantic_label(component: str, failure_mode: str) -> str:

    if component == "Gatekeeper" and failure_mode == "resource_lookup_failure":
        return "Gatekeeper policy evaluation failures"

    if failure_mode == "service_failure":
        return f"{component} service failures"

    if failure_mode == "resource_lookup_failure":
        return f"{component} resource lookup failures"

    return f"{component} operational activity"


def extract_cluster_entities(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> Dict[str, Any]:

    rep_idx = cluster.get("representative_index")

    if rep_idx is None or rep_idx >= len(events):
        return {}

    ev = events[rep_idx]

    actor = ev.get("actor") or ev.get("user") or ""
    verb = ev.get("verb") or ev.get("operation") or ""
    resource = ev.get("resource") or ""
    status = ev.get("response_code") or ev.get("status")

    # ------------------------------------------------
    # status class
    # ------------------------------------------------
    status_class = _infer_status_class(status)

    # ------------------------------------------------
    # component detection
    # ------------------------------------------------

    component = None

    # 1️⃣ actor-based inference (service accounts etc)
    component = infer_component_from_actor(actor)

    # 2️⃣ registry detection from log text
    if not component:

        text = (
            ev.get("raw_text")
            or ev.get("message")
            or ev.get("msg")
            or ev.get("text")
            or ""
        )

        comp_from_text, _ = detect_component(text)

        if comp_from_text:
            component = comp_from_text

    # 3️⃣ fallback to existing heuristic
    if not component:
        component = _infer_component(actor, resource)

    # ------------------------------------------------
    # failure mode
    # ------------------------------------------------
    failure_mode = _infer_failure_mode(status_class, resource)

    # ------------------------------------------------
    # semantic label
    # ------------------------------------------------
    semantic_label = _build_semantic_label(component, failure_mode)

    return {
        "component": component,
        "actor": actor,
        "operation": verb,
        "resource": resource,
        "status_class": status_class,
        "failure_mode": failure_mode,
        "semantic_label": semantic_label
    }