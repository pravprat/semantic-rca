# semantic/signature.py

from typing import Dict


def build_signature(sem: Dict) -> str:
    """
    Deterministic cluster signature

    Format:
        component:operation:resource:failure_mode
    """

    component = sem.get("component")
    operation = sem.get("operation")
    resource = sem.get("resource")
    failure_mode = sem.get("failure_mode")

    parts = []

    if component:
        parts.append(component)

    if operation:
        parts.append(operation)

    if resource:
        parts.append(resource)

    if failure_mode:
        parts.append(failure_mode)

    if not parts:
        return "unknown_signature"

    return ":".join(parts)