# semantic/signature.py

from typing import Dict


def build_cluster_signature(root: Dict) -> str:
    """
    Build deterministic signature for a root cause cluster.

    Signature format:

        component:operation:resource:status

    Example:
        gatekeeper:list:assignmetadata:4xx
    """

    component = root.get("component")
    operation = root.get("dominant_operation")
    resource = root.get("dominant_resource")
    status = root.get("status_class")

    parts = []

    if component:
        parts.append(component)

    if operation:
        parts.append(operation)

    if resource:
        parts.append(resource)

    if status:
        parts.append(status)

    if not parts:
        return "unknown_signature"

    return ":".join(parts)