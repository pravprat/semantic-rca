from __future__ import annotations

from typing import Dict, List


BENIGN_PATTERNS = [
    "leader-election",
    "leader election",
    "lease",
    "heartbeat",
    "metrics",
    "prometheus",
    "scrape",
    "allowwatchbookmarks",
    "timeoutseconds",
    "watch=true",
    "startupapicheck",
    "helm.sh/hook",
    "cert-manager",
    "admission-create",
    "admission-patch",
]


BENIGN_RESOURCES = {
    "leases",
    "events",
}


def _text(ev: Dict) -> str:
    text = ev.get("text") or ev.get("message") or ""
    return text.lower()


def is_expected_churn_event(ev: Dict) -> bool:
    text = _text(ev)

    resource = (ev.get("resource") or "").lower()
    verb = (ev.get("verb") or "").lower()
    service = (ev.get("service") or "").lower()

    if resource in BENIGN_RESOURCES and verb in {"get", "watch", "update", "patch"}:
        return True

    if "metrics" in service or "prometheus" in service:
        return True

    for pat in BENIGN_PATTERNS:
        if pat in text:
            return True

    return False


def expected_churn_penalty(events: List[Dict]) -> float:
    if not events:
        return 0.0

    churn = sum(1 for ev in events if is_expected_churn_event(ev))
    ratio = churn / max(len(events), 1)

    if ratio >= 0.8:
        return 12.0
    if ratio >= 0.5:
        return 6.0
    if ratio >= 0.3:
        return 3.0
    return 0.0