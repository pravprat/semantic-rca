from __future__ import annotations

from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional

from .utils import load_json, write_json, confidence_label


def _sum_event_count(patterns: List[Dict[str, Any]]) -> int:
    return sum(int(p.get("event_count", 0)) for p in patterns)


def _sum_error_count(patterns: List[Dict[str, Any]]) -> int:
    return sum(int(p.get("error_count", 0)) for p in patterns)


def _max_trigger(patterns: List[Dict[str, Any]]) -> float:
    if not patterns:
        return 0.0
    return max(float(p.get("max_trigger_score", 0.0)) for p in patterns)


def _actor_diversity(patterns: List[Dict[str, Any]]) -> int:
    actors = set()
    for p in patterns:
        for a in p.get("unique_actors", []) or []:
            if a:
                actors.add(a)
        actor = p.get("actor")
        if actor:
            actors.add(actor)
    return len(actors)


def _control_plane_count(patterns: List[Dict[str, Any]]) -> int:
    return sum(1 for p in patterns if bool(p.get("is_control_plane")))


def _earliest_pattern(patterns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    valid = [p for p in patterns if p.get("first_seen")]
    if not valid:
        return patterns[0] if patterns else None
    valid.sort(key=lambda p: p.get("first_seen") or "")
    return valid[0]


def _is_failure_pattern(p: Dict[str, Any]) -> bool:
    return (
        p.get("http_class") in ("4xx", "5xx")
        and int(p.get("error_count", 0)) > 0
    )


def _has_failure_signal(patterns: List[Dict[str, Any]]) -> bool:
    return any(_is_failure_pattern(p) for p in patterns)


def _domain_groups(patterns: List[Dict[str, Any]]) -> Dict[str, List[Dict[str, Any]]]:
    groups: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
    for p in patterns:
        domain = str(p.get("failure_domain") or "other")
        groups[domain].append(p)

    for pats in groups.values():
        pats.sort(
            key=lambda x: (
                x.get("first_seen") or "",
                -int(x.get("error_count", 0)),
                -int(x.get("event_count", 0)),
                -float(x.get("max_trigger_score", 0.0)),
            )
        )
    return dict(groups)


def _earliest_rank_bonus(domain_patterns: List[Dict[str, Any]], all_patterns: List[Dict[str, Any]]) -> float:
    if not domain_patterns or not all_patterns:
        return 0.0

    ordered = [p for p in all_patterns if p.get("first_seen")]
    ordered.sort(key=lambda p: p.get("first_seen") or "")

    if not ordered:
        return 0.0

    domain_ids = {p.get("pattern_id") for p in domain_patterns}
    for rank, p in enumerate(ordered):
        if p.get("pattern_id") in domain_ids:
            if rank == 0:
                return 0.20
            if rank == 1:
                return 0.12
            if rank == 2:
                return 0.06
            return 0.0

    return 0.0


def _systemic_bonus(domain_patterns: List[Dict[str, Any]]) -> float:
    bonus = 0.0

    actor_div = _actor_diversity(domain_patterns)
    cp_count = _control_plane_count(domain_patterns)

    if actor_div >= 2:
        bonus += 0.10
    if actor_div >= 3:
        bonus += 0.05

    if cp_count >= 1:
        bonus += 0.05
    if cp_count >= 2:
        bonus += 0.05

    return min(bonus, 0.20)

def _domain_score(
    domain: str,
    domain_patterns: List[Dict[str, Any]],
    all_patterns: List[Dict[str, Any]],
) -> float:

    # --------------------------------------------------
    # Time weighting (FIX)
    # --------------------------------------------------
    ordered = sorted(
        [p for p in domain_patterns if p.get("first_seen")],
        key=lambda x: x.get("first_seen")
    )

    rank_map = {p.get("pattern_id"): i for i, p in enumerate(ordered)}

    def _time_weight(p):
        r = rank_map.get(p.get("pattern_id"), 0)
        return 1.0 / (1 + r)

    # --------------------------------------------------
    # Time-weighted signals
    # --------------------------------------------------
    event_count = sum(
        int(p.get("event_count", 0)) * _time_weight(p)
        for p in domain_patterns
    )

    error_count = sum(
        int(p.get("error_count", 0)) * _time_weight(p)
        for p in domain_patterns
    )

    trigger = 0.0
    for p in domain_patterns:
        t = float(p.get("max_trigger_score", 0.0))
        trigger = max(trigger, t * _time_weight(p))

    earliest = _earliest_pattern(domain_patterns)
    actor_div = _actor_diversity(domain_patterns)

    earliest_bonus = _earliest_rank_bonus(domain_patterns, all_patterns)
    systemic = _systemic_bonus(domain_patterns)

    # Base prior
    if domain == "authz":
        base = 0.42
    elif domain == "service_failure":
        base = 0.48
    else:
        base = 0.12

    weight = (
        min(event_count / 200.0, 0.10)
        + min(error_count / 200.0, 0.15)
        + min(trigger / 5.0, 0.10)
        + systemic
    )

    # Downstream noise suppression
    if earliest:
        earliest_ts = earliest.get("first_seen")

        late_noise = [
            p for p in domain_patterns
            if p.get("http_class") in ("404", "409")
            and p.get("first_seen") > earliest_ts
        ]

        if len(late_noise) >= 2:
            weight -= 0.10

    # AuthZ systemic boost
    if domain == "authz" and actor_div >= 2:
        weight += 0.10

    # Dominance
    dominance_factor = 1.0

    if earliest:
        if earliest.get("http_class") in ("4xx", "5xx") and actor_div >= 2:
            dominance_factor += 0.50

    score = (base + weight) * dominance_factor

    return round(min(score, 0.99), 3)

def _domain_title(domain: str) -> str:
    if domain == "authz":
        return "Authorization / RBAC failure"
    if domain == "service_failure":
        return "Control-plane / service failure"
    return "Unclassified failure domain"


def _domain_summary(domain: str, patterns: List[Dict[str, Any]]) -> str:
    first = _earliest_pattern(patterns) or {}
    actor_div = _actor_diversity(patterns)
    cp_count = _control_plane_count(patterns)

    if domain == "authz":
        return (
            "Repeated authorization failures were detected. "
            f"The earliest pattern is {first.get('verb')} {first.get('resource')} with {first.get('http_class')}. "
            f"The failure affects {actor_div} actor(s)"
            + (f", including {cp_count} control-plane pattern(s)." if cp_count else ".")
        )

    if domain == "service_failure":
        return (
            "Repeated 5xx failures indicate an unstable or failing service. "
            f"The earliest service-failure pattern is {first.get('verb')} {first.get('resource')} "
            f"from {first.get('service')}."
        )

    return (
        "An unclassified failure domain was identified from the incident patterns. "
        f"The earliest failure pattern is {first.get('verb')} {first.get('resource')} "
        f"with status class {first.get('http_class')}."
    )


def _build_domain_candidate(
    domain: str,
    domain_patterns: List[Dict[str, Any]],
    all_patterns: List[Dict[str, Any]],
) -> Dict[str, Any]:
    score = _domain_score(domain, domain_patterns, all_patterns)
    actor_div = _actor_diversity(domain_patterns)
    cp_count = _control_plane_count(domain_patterns)


    # --------------------------------------------------
    # Time-decayed counts (causal weighting)
    #
    # Earlier patterns get higher weight
    # Later patterns (likely effects) get reduced weight
    # --------------------------------------------------

    ordered = sorted(
        [p for p in domain_patterns if p.get("first_seen")],
        key=lambda x: x.get("first_seen")
    )

    rank_map = {p.get("pattern_id"): i for i, p in enumerate(ordered)}

    def _time_weight(p):
        r = rank_map.get(p.get("pattern_id"), 0)
        return 1.0 / (1 + r)  # 1.0, 0.5, 0.33, ...

    event_count = sum(
        int(p.get("event_count", 0)) * _time_weight(p)
        for p in domain_patterns
    )

    error_count = sum(
        int(p.get("error_count", 0)) * _time_weight(p)
        for p in domain_patterns
    )

    trigger = _max_trigger(domain_patterns)
    earliest = _earliest_pattern(domain_patterns)

    return {
        "candidate_type": domain,
        "title": _domain_title(domain),
        "summary": _domain_summary(domain, domain_patterns),
        "evidence_pattern_ids": [p["pattern_id"] for p in domain_patterns],
        "supporting_patterns": domain_patterns,
        "score": score,
        "confidence": {
            "value": score,
            "label": confidence_label(score),
        },
        "signals": {
            "event_count": event_count,
            "error_count": error_count,
            "actor_diversity": actor_div,
            "control_plane_count": cp_count,
            "max_trigger_score": round(trigger, 3),
            "earliest_pattern_id": earliest.get("pattern_id") if earliest else None,
            "earliest_first_seen": earliest.get("first_seen") if earliest else None,
        },
    }


def _fallback_single_pattern_candidate(patterns: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not patterns:
        return None

    failure_patterns = [p for p in patterns if _is_failure_pattern(p)]
    if not failure_patterns:
        return None

    failure_patterns.sort(
        key=lambda x: (
            x.get("first_seen") or "",
            -int(x.get("error_count", 0)),
            -float(x.get("max_trigger_score", 0.0)),
        )
    )
    p = failure_patterns[0]

    event_count = int(p.get("event_count", 0))
    error_count = int(p.get("error_count", 0))
    trigger = float(p.get("max_trigger_score", 0.0))
    actor_diversity = len([a for a in (p.get("unique_actors") or []) if a]) or 1

    score = min(
        0.55,
        0.10
        + min(event_count / 200.0, 0.10)
        + min(error_count / 200.0, 0.20)
        + min(trigger / 5.0, 0.10),
    )
    score = round(score, 3)

    return {
        "candidate_type": "dominant_failure_pattern",
        "title": "Dominant failure pattern",
        "summary": (
            f"Earliest / strongest failure pattern is {p.get('verb')} {p.get('resource')} "
            f"from {p.get('service')} with status class {p.get('http_class')}."
        ),
        "evidence_pattern_ids": [p["pattern_id"]],
        "supporting_patterns": [p],
        "score": score,
        "confidence": {
            "value": score,
            "label": confidence_label(score),
        },
        "signals": {
            "event_count": event_count,
            "error_count": error_count,
            "actor_diversity": actor_diversity,
            "control_plane_count": 1 if p.get("is_control_plane") else 0,
            "max_trigger_score": round(trigger, 3),
            "earliest_pattern_id": p.get("pattern_id"),
            "earliest_first_seen": p.get("first_seen"),
        },
    }


def build_incident_candidates(
    artifacts_dir: Path,
    patterns_path: Path | None = None,
    out_path: Path | None = None,
) -> Dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)
    patterns_obj = load_json(
        patterns_path or (artifacts_dir / "incident_patterns.json"),
        default={},
    ) or {}

    output: Dict[str, Any] = {
        "candidate_version": "1.4.3.step8",
        "incidents": [],
    }

    for inc in patterns_obj.get("incidents", []):
        patterns = inc.get("patterns", []) or []
        domain_groups = _domain_groups(patterns)
        candidates: List[Dict[str, Any]] = []

        authz_patterns = [p for p in domain_groups.get("authz", []) if _is_failure_pattern(p)]
        service_patterns = [p for p in domain_groups.get("service_failure", []) if _is_failure_pattern(p)]
        other_patterns = [p for p in domain_groups.get("other", []) if _is_failure_pattern(p)]

        if authz_patterns:
            candidates.append(_build_domain_candidate("authz", authz_patterns, patterns))

        if service_patterns:
            candidates.append(_build_domain_candidate("service_failure", service_patterns, patterns))

        if other_patterns and not candidates:
            candidates.append(_build_domain_candidate("other", other_patterns, patterns))

        if not candidates:
            fallback = _fallback_single_pattern_candidate(patterns)
            if fallback:
                candidates.append(fallback)

        candidates.sort(
            key=lambda x: (
                -float(x.get("score", 0.0)),
                x.get("signals", {}).get("earliest_first_seen") or "",
                x.get("candidate_type", ""),
            )
        )

        output["incidents"].append(
            {
                "incident_id": inc.get("incident_id"),
                "start_time": inc.get("start_time"),
                "end_time": inc.get("end_time"),
                "patterns": patterns,
                "candidates": candidates,
            }
        )

    final_out = out_path or (artifacts_dir / "incident_candidates.json")
    write_json(final_out, output)
    return output


if __name__ == "__main__":
    build_incident_candidates(Path("outputs"))