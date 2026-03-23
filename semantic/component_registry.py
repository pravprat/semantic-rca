# semantic/component_registry.py

from typing import Dict, Tuple, Optional
import re

# ---------------------------------------------------------
# Component registry
# ---------------------------------------------------------

COMPONENT_REGISTRY = {

    "control_plane": {
        "kube-apiserver": ["kube-apiserver", "apiserver"],
        "kube-controller-manager": ["kube-controller-manager", "controller-manager"],
        "kube-scheduler": ["kube-scheduler", "scheduler"],
        "etcd": ["etcd"],
    },

    "node": {
        "kubelet": ["kubelet"],
        "kube-proxy": ["kube-proxy"],
    },

    "policy": {
        "gatekeeper": ["gatekeeper", "constraint", "constrainttemplates"],
        "kyverno": ["kyverno"],
        "opa": ["opa"],
    },

    "gitops": {
        "argocd": ["argocd", "argocd-server", "argocd-application-controller"],
        "flux": ["flux", "fluxcd"],
    },
}


# ---------------------------------------------------------
# Build fast lookup index
# ---------------------------------------------------------

def _build_index():
    patterns = {}
    domains = {}

    for domain, comps in COMPONENT_REGISTRY.items():
        for comp, pats in comps.items():
            patterns[comp] = [p.lower() for p in pats]
            domains[comp] = domain

    return patterns, domains


COMPONENT_PATTERNS, COMPONENT_DOMAINS = _build_index()


# ---------------------------------------------------------
# Matching (SAFE — word boundary)
# ---------------------------------------------------------

def _match(pattern: str, text: str) -> bool:
    return re.search(rf"\b{re.escape(pattern)}\b", text) is not None


# ---------------------------------------------------------
# Actor normalization (CRITICAL)
# ---------------------------------------------------------

def normalize_actor(actor: Optional[str]) -> Optional[str]:
    if not actor:
        return None

    a = actor.lower()

    if a.startswith("system:node:"):
        return "kubelet"

    if a.startswith("system:kube-scheduler"):
        return "kube-scheduler"

    if a.startswith("system:kube-controller-manager"):
        return "kube-controller-manager"

    if a.startswith("system:apiserver"):
        return "kube-apiserver"

    return actor


# ---------------------------------------------------------
# Component resolution
# ---------------------------------------------------------

def resolve_component(actor: str, text: str) -> Tuple[str, Optional[str]]:
    """
    Returns:
        component, domain
    """

    actor = normalize_actor(actor) or ""
    text = (text or "").lower()

    # 1. direct actor match
    for comp, pats in COMPONENT_PATTERNS.items():
        for p in pats:
            if _match(p, actor):
                return comp, COMPONENT_DOMAINS.get(comp)

    # 2. text match
    for comp, pats in COMPONENT_PATTERNS.items():
        for p in pats:
            if _match(p, text):
                return comp, COMPONENT_DOMAINS.get(comp)

    return "unknown_component", None