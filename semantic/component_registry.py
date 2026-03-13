"""
Component Registry

Defines known platform components and pattern hints used by
semantic entity extraction.

component -> pattern keywords

Domains allow higher-level reasoning later (RCA explainability,
system classification, dependency graphs).
"""

# ==========================================================
# Component registry (single line per component)
# ==========================================================

COMPONENT_REGISTRY = {

    "control_plane": {
        "kube-apiserver": ["kube-apiserver","apiserver"],
        "kube-controller-manager": ["kube-controller-manager","controller-manager"],
        "kube-scheduler": ["kube-scheduler","scheduler"],
        "etcd": ["etcd"]
    },

    "node": {
        "kubelet": ["kubelet"],
        "kube-proxy": ["kube-proxy"],
        "rke2": ["rke2"]
    },

    "policy": {
        "gatekeeper": ["gatekeeper","constraint","constrainttemplates"],
        "policy-engine": ["policy engine","policy-engine"],
        "kyverno": ["kyverno"],
        "opa": ["opa"]
    },

    "gitops": {
        "argocd": ["argocd","argocd-server","argocd-application-controller"],
        "flux": ["flux","fluxcd"]
    },

    "observability": {
        "prometheus": ["prometheus"],
        "grafana": ["grafana"],
        "loki": ["loki"],
        "metrics-server": ["metrics-server"]
    },

    "service_mesh": {
        "istio": ["istio","istiod"],
        "envoy": ["envoy"]
    },

    "database": {
        "mongodb": ["mongodb","mongo"],
        "postgres": ["postgres","postgresql"]
    },

    "cache": {
        "redis": ["redis"]
    },

    "vector_database": {
        "milvus": ["milvus"]
    },

    "enterprise_platform": {
        "dcn": ["dcn"],
        "dcn-manager": ["dcn manager","dcn-manager"],
        "aide-console": ["aide console","aide-console"]
    },

    "ai_platform": {
        "ai-data-engine": ["ai data engine","ai-data-engine"]
    }

}

# ==========================================================
# Flatten registry for quick lookup
# ==========================================================

def build_component_index():
    """
    Builds fast lookup maps used by the semantic layer.
    """

    component_patterns = {}
    component_domains = {}

    for domain, components in COMPONENT_REGISTRY.items():

        for comp, patterns in components.items():

            component_patterns[comp] = [p.lower() for p in patterns]
            component_domains[comp] = domain

    return component_patterns, component_domains


COMPONENT_PATTERNS, COMPONENT_DOMAINS = build_component_index()