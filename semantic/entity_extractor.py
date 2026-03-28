# semantic/entity_extractor.py

from typing import Dict, Any
from semantic.component_registry import resolve_component


# ---------------------------------------------------------
# Status → class
# ---------------------------------------------------------

def infer_status_class(code) -> str:
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


# ---------------------------------------------------------
# Failure mode (IMPORTANT)
# ---------------------------------------------------------

def infer_failure_mode(status_class: str, code: int, status_family: str = "unknown", failure_hint: str | None = None) -> str:

    if status_class == "5xx":
        return "service_failure"

    if status_class == "4xx":
        if code == 403:
            return "authz_failure"
        if code == 404:
            return "resource_not_found"
        if code == 409:
            return "conflict"

        return "client_error"

    if status_family == "failure":
        if failure_hint in {
            "timeout",
            "connection_refused",
            "connection_reset",
            "network_unreachable",
            "dns_failure",
            "tls_handshake",
            "tls_certificate",
            "replica_instability",
        }:
            return "dependency_failure"
        if failure_hint in {
            "rpc_error",
            "exception",
            "panic",
            "failed",
            "threshold_exceeded",
            "oom",
            "oom_killed",
            "crash_loop",
            "leader_election_failure",
        }:
            return "service_failure"
        if failure_hint in {"forbidden", "unauthorized", "access_denied", "permission_denied"}:
            return "authz_failure"
        return "service_failure"

    return "normal"


# ---------------------------------------------------------
# Main extraction
# ---------------------------------------------------------

def extract_event_semantics(event: Dict[str, Any]) -> Dict[str, Any]:

    actor = event.get("actor")
    verb = event.get("verb")
    resource = event.get("resource")
    code = event.get("response_code")
    status_family = event.get("status_family") or "unknown"
    failure_hint = event.get("failure_hint")

    raw_text = event.get("raw_text") or ""

    status_class = infer_status_class(code)
    failure_mode = infer_failure_mode(status_class, code, status_family=status_family, failure_hint=failure_hint)

    component, domain = resolve_component(actor, raw_text)

    return {
        "component": component,
        "domain": domain,
        "actor": actor,
        "operation": verb,
        "resource": resource,
        "status_class": status_class,
        "failure_mode": failure_mode,
    }