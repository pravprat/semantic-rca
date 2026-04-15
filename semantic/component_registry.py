# semantic/component_registry.py

from typing import Any, Dict, List, Mapping, Optional, Tuple

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
        "rke2-server": ["rke2-server", "rke2 server", "rke2r1"],
        "rke2-agent": ["rke2-agent", "rke2 agent"],
        "traefik": ["traefik", "ingressroute", "ingress-controller"],
    },

    "node": {
        "kubelet": ["kubelet"],
        "kube-proxy": ["kube-proxy"],
    },

    "policy": {
        "gatekeeper": ["gatekeeper", "constraint", "constrainttemplates"],
        "kyverno": ["kyverno"],
        "opa": ["opa"],
        "vault": ["vault", "vault-agent-injector", "sidecar-injector"],
    },

    "gitops": {
        "argocd": ["argocd", "argocd-server", "argocd-application-controller"],
        "flux": ["flux", "fluxcd"],
    },

    "networking": {
        "calico-node": ["calico-node", "felix", "bird", "calico"],
        "calico-kube-controllers": ["calico-kube-controllers", "kube-controllers/ipam"],
        "metallb": ["metallb", "frr"],
        "tigera-operator": ["tigera-operator", "tigera"],
    },

    "storage": {
        "trident-operator": ["operator.trident.netapp.io", "trident-operator"],
        "trident-csi-controller": ["controller.csi.trident.netapp.io", "csi-attacher", "csi-provisioner", "csi-snapshotter"],
        "trident-main": ["trident-main"],
        "snapshot-controller": ["snapshot-controller", "csi-snapshotter"],
        "ontap-proxy": ["ontap-proxy"],
        "support-configuration": ["support-configuration"],
    },

    "observability": {
        "prometheus": ["prometheus", "kube-prometheus"],
        "fluent-bit": ["fluent-bit", "fluent"],
        "vector-store": ["vector-store", "vector-store-chart", "vector"],
        "log-rotation": ["log-rotation"],
        "file-metadata": ["file-metadata", "metadata-chart", "metadata"],
    },

    "data_platform": {
        "job-tracker": ["job-tracker"],
        "authorizer": ["authorizer"],
        "occmauth": ["occmauth"],
        "aide-control": ["aide", "dsai"],
        "policy-engine": ["policy-engine"],
        "kafka": ["kafka-controller", "kafka-exporter"],
        "content-processing": [
            "content-processing",
            "content-processing-datacollection",
            "content-processing-workspace",
        ],
        "ocr-service": ["ocr-service"],
        "file-preview": ["file-preview"],
        "dcn-manager": ["dcn_manager", "dcn-manager", "dcn_mgr", "dcn manager"],
        "mongodb": ["mongodb", "mongod", "replicasetmonitor", "connpool"],
        "milvus-core": [
            "milvus-etcd",
            "milvus",
            "milvus-operator",
            "milvus-autoscaler",
            "mixcoord",
            "querynode",
            "datanode",
            "indexnode",
            "proxy",
        ],
        # Keep patterns specific; bare "manager" is too ambiguous in controller logs.
        "scheduler-worker": ["scheduler", "worker", "scheduler-manager", "job-manager"],
    },

    "hardware": {
        "gpu-operator": ["gpu-operator", "gpu"],
        "node-feature-discovery": ["node-feature-discovery"],
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
# Namespace → subsystem (domain) hints from real clusters / ASUP
# Used when pod/container text alone is ambiguous (e.g. kube-system).
# Values must match top-level keys in COMPONENT_REGISTRY.
# ---------------------------------------------------------

SUBSYSTEM_NAMESPACE_HINTS: Dict[str, str] = {
    "metallb-ns": "networking",
    "aide-system": "data_platform",
    "trident": "storage",
    "trident-operator": "storage",
    "monitoring": "observability",
    "prometheus": "observability",
    "observability": "observability",
    "gatekeeper-system": "policy",
    "kyverno": "policy",
    "argocd": "gitops",
    "flux-system": "gitops",
    "tigera-operator": "networking",
    "calico-system": "networking",
    "gpu-operator-resources": "hardware",
    "aidp-console": "data_platform",
    "data-services-ai": "data_platform",
    "vault": "policy",
}

# ---------------------------------------------------------
# ASUP subsystem names (HCE Core / AIDE subsystem workbook)
# Maps namespace + pod prefix → official collector subsystem id.
# See: ASUP Subsystems - HCE Core Engineering (Confluence export).
# ---------------------------------------------------------

_ASUP_POD_PREFIX_RAW: List[Tuple[str, str]] = [
    ("aide-operator-application-manager-", "aide-infra"),
    ("aide-operator-controller-manager-", "aide-infra"),
    ("content-processing-datacollection-", "aide-cpe"),
    ("content-processing-workspace-", "aide-cpe"),
    ("aide-system-kafka-prometheus-kafka-exporter-", "aide-messaging"),
    ("cloudmanager-credentials-", "aide-security"),
    ("fluent-bit-log-rotation-", "aide-support-infra"),
    ("fluent-bit-aggregator-", "aide-support-infra"),
    ("fluent-bit-collector-", "aide-support-infra"),
    ("aide-system-cert-manager-", "aide-security"),
    ("kube-controller-manager-", "aide-k8s"),
    ("cloud-controller-manager-", "aide-k8s"),
    ("calico-kube-controllers-", "aide-k8s"),
    ("milvus-milvus-", "aide-vector-store"),
    ("milvus-operator-", "aide-vector-store"),
    ("milvus-etcd-", "aide-vector-store"),
    ("vector-store-", "aide-vector-store"),
    ("policy-engine-", "aide-policy-engine"),
    ("file-metadata-", "aide-cdc"),
    ("ontap-monitor-", "aide-cdc"),
    ("job-tracker-", "aide-infra"),
    ("licensing-", "aide-infra"),
    ("utility-", "aide-infra"),
    ("mongodb-", "aide-mdb"),
    ("kafka-controller-", "aide-messaging"),
    ("ocr-service-", "aide-cpe"),
    ("file-preview-", "aide-cpe"),
    ("authorizer-", "aide-security"),
    ("occmauth-", "aide-security"),
    ("supportability-", "aide-support-infra"),
    ("metadata-", "aide-mde"),
    ("data-services-ai-", "aide-dsai"),
    ("dsai-operator-", "aide-dsai"),
    ("aidp-console-operator-", "aide-bxp-console"),
    ("aidp-console-krakend-", "aide-bxp-console"),
    ("aidp-console-", "aide-bxp-console"),
    ("static-file-server-", "aide-bxp-console"),
    ("support-configuration-", "aide-bxp-console"),
    ("ontap-proxy-", "aide-bxp-console"),
    ("trident-operator-", "aide-k8s"),
    ("trident-controller-", "aide-k8s"),
    ("trident-node-linux-", "aide-k8s"),
    ("metallb-controller-", "aide-k8s"),
    ("metallb-speaker-", "aide-k8s"),
    ("vault-agent-injector-", "aide-security"),
    ("prometheus-kube-prometheus-stack-", "aide-k8s"),
    ("kube-prometheus-stack-", "aide-k8s"),
    ("snapshot-controller-", "aide-k8s"),
    ("kube-apiserver-", "aide-k8s"),
    ("kube-scheduler-", "aide-k8s"),
    ("kube-proxy-", "aide-k8s"),
    ("calico-node-", "aide-k8s"),
    ("calico-typha-", "aide-k8s"),
    ("tigera-operator-", "aide-k8s"),
    ("alertmanager-", "aide-k8s"),
    ("csi-snapshotter-", "aide-k8s"),
    ("scheduler-", "aide-infra"),
    ("etcd-", "aide-k8s"),
    ("vault-", "aide-security"),
]

ASUP_POD_PREFIX_RULES: List[Tuple[str, str]] = sorted(
    _ASUP_POD_PREFIX_RAW, key=lambda t: len(t[0]), reverse=True
)

# When no pod-prefix rule matches, use namespace-level ASUP subsystem (PDF “shared” bundles).
ASUP_NAMESPACE_FALLBACK: Dict[str, str] = {
    "vault": "aide-security",
    "trident": "aide-k8s",
    "metallb-ns": "aide-k8s",
    "calico-system": "aide-k8s",
    "kube-system": "aide-k8s",
    "prometheus": "aide-k8s",
    "default": "aide-k8s",
    "tigera-operator": "aide-k8s",
    "aidp-console": "aide-bxp-console",
    "data-services-ai": "aide-dsai",
    "aide-system": "aide-k8s",
    "gpu-operator": "aide-k8s",
}


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

    # Milvus bundles etcd as a chart; container_name may be plain "etcd" while pod is milvus-etcd-*.
    blob = f"{actor} {text}".lower()
    if "milvus-etcd" in blob:
        for p in COMPONENT_PATTERNS.get("milvus-core", []):
            if _match(p, blob):
                return "milvus-core", COMPONENT_DOMAINS.get("milvus-core")

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


def resolve_asup_subsystem(namespace: Optional[str], pod: Optional[str]) -> str:
    """
    Official AIDE ASUP subsystem id (e.g. aide-infra, aide-k8s) from namespace + pod name.

    Source: HCE Core Engineering “ASUP Subsystems” workbook (pod prefixes per subsystem).
    """
    pod_l = (pod or "").lower()
    ns = (namespace or "").strip().lower()
    for prefix, subsys in ASUP_POD_PREFIX_RULES:
        if pod_l.startswith(prefix):
            return subsys
    return ASUP_NAMESPACE_FALLBACK.get(ns, "unknown_asup_subsystem")


def resolve_subsystem_from_k8s(
    namespace: Optional[str],
    pod: Optional[str],
    container: Optional[str],
    text: str = "",
    labels: Optional[Mapping[str, Any]] = None,
) -> Tuple[str, str, Optional[str]]:
    """
    Map an ASUP / Kubernetes-enriched event to (subsystem, component, domain).

    subsystem is the COMPONENT_REGISTRY domain (e.g. networking, data_platform).
    """
    ns = (namespace or "").strip()
    pod_s = (pod or "").strip()
    container_s = (container or "").strip()
    label_blob = ""
    if labels:
        parts = []
        for k, v in labels.items():
            if isinstance(v, (str, int, float)):
                parts.append(f"{k}={v}")
        label_blob = " ".join(parts)

    combined = " ".join(
        x for x in (pod_s, container_s, label_blob, text or "") if x
    )
    actor_guess = container_s or pod_s
    comp, domain = resolve_component(actor_guess, combined)

    hinted = SUBSYSTEM_NAMESPACE_HINTS.get(ns)
    subsystem = domain or hinted or "unknown_subsystem"

    return subsystem, comp, domain