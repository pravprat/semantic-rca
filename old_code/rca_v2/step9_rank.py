from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Any

from .utils import load_json, write_json


MIN_CONFIDENT_SCORE = 0.50


# ---------------------------------------------------------
# Validation: Is this a real root cause candidate?
# ---------------------------------------------------------
def _is_valid_root_cause(candidate: Dict) -> bool:
    score = float(candidate.get("score", 0.0))
    if score < MIN_CONFIDENT_SCORE:
        return False

    patterns = candidate.get("supporting_patterns", []) or []

    # ------------------------------------------
    # Must have failure signal
    # ------------------------------------------
    failure_patterns = [
        p for p in patterns
        if p.get("http_class") in ("4xx", "5xx")
        and int(p.get("error_count", 0)) > 0
    ]

    if not failure_patterns:
        return False

    # ------------------------------------------
    # Identify earliest failure
    # ------------------------------------------
    ordered = sorted(
        [p for p in failure_patterns if p.get("first_seen")],
        key=lambda x: x.get("first_seen")
    )

    if not ordered:
        return False

    earliest = ordered[0]
    earliest_ts = earliest.get("first_seen")

    # ------------------------------------------
    # 🔥 NEW: propagation check
    # Must have downstream failures AFTER earliest
    # ------------------------------------------
    downstream = [
        p for p in failure_patterns
        if p.get("first_seen") > earliest_ts
    ]

    if len(downstream) < 2:
        return False

    # ------------------------------------------
    # 🔥 NEW: systemic check (multi-actor)
    # ------------------------------------------
    actors = set()
    for p in patterns:
        actors.update(p.get("unique_actors", []) or [])
        if p.get("actor"):
            actors.add(p.get("actor"))

    if len(actors) < 2:
        return False

    # ------------------------------------------
    # Existing strength logic (kept)
    # ------------------------------------------
    total_errors = sum(int(p.get("error_count", 0)) for p in failure_patterns)

    if total_errors >= 2:
        return True

    if any(int(p.get("error_count", 0)) >= 2 for p in failure_patterns):
        return True

    return False

# ---------------------------------------------------------
# Select best representative pattern
# ---------------------------------------------------------
def _select_primary_pattern(patterns: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not patterns:
        return {}

    failure_patterns = [
        p for p in patterns
        if p.get("http_class") in ("4xx", "5xx")
        and int(p.get("error_count", 0)) > 0
    ]

    if failure_patterns:
        return sorted(
            failure_patterns,
            key=lambda p: (
                p.get("first_seen") or "",
                -int(p.get("error_count", 0)),
            )
        )[0]

    return sorted(
        patterns,
        key=lambda p: p.get("first_seen") or ""
    )[0]


# ---------------------------------------------------------
# Ranking logic
# ---------------------------------------------------------
def _sort_candidates(candidates: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        candidates,
        key=lambda x: (
            -float(x.get("score", 0.0)),
            -int(x.get("signals", {}).get("error_count", 0)),
            x.get("signals", {}).get("earliest_first_seen", ""),
        )
    )


# ---------------------------------------------------------
# Main builder
# ---------------------------------------------------------
def build_ranked_root_causes(
    artifacts_dir: Path,
    candidates_path: Path | None = None,
    out_path: Path | None = None,
) -> Dict[str, Dict]:

    artifacts_dir = Path(artifacts_dir)

    candidates_obj = load_json(
        candidates_path or (artifacts_dir / "incident_candidates.json"),
        default={},
    ) or {}

    output: Dict[str, List[Dict]] = {"incidents": []}

    for inc in candidates_obj.get("incidents", []):

        incident_id = inc.get("incident_id")

        candidates = list(inc.get("candidates", []) or [])
        candidates = _sort_candidates(candidates)

        ranked: List[Dict] = []

        for i, cand in enumerate(candidates, start=1):

            supporting = cand.get("supporting_patterns", []) or []
            primary = _select_primary_pattern(supporting)

            ranked.append(
                {
                    "rank": i,
                    "candidate_type": cand.get("candidate_type"),
                    "title": cand.get("title"),
                    "summary": cand.get("summary"),
                    "score": cand.get("score"),
                    "confidence": cand.get("confidence"),
                    "evidence_pattern_ids": cand.get("evidence_pattern_ids", []),
                    "signals": cand.get("signals", {}),
                    "supporting_patterns": supporting,
                    "primary_pattern": {
                        "pattern_id": primary.get("pattern_id"),
                        "service": primary.get("service"),
                        "actor": primary.get("actor"),
                        "verb": primary.get("verb"),
                        "resource": primary.get("resource"),
                        "http_class": primary.get("http_class"),
                        "failure_domain": primary.get("failure_domain"),
                        "event_count": primary.get("event_count"),
                        "error_count": primary.get("error_count"),
                        "first_seen": primary.get("first_seen"),
                        "last_seen": primary.get("last_seen"),
                        "examples": primary.get("examples", []),
                    },
                }
            )

        # -------------------------------------------------
        # Validation gate (fixed behavior)
        # -------------------------------------------------
        valid = bool(ranked and _is_valid_root_cause(ranked[0]))

        # 🔧 FIX: Do NOT drop candidates — fallback instead
        if not valid and ranked:
            top = ranked[0]

            ranked = [{
                **top,
                "fallback": True,
                "confidence": {
                    "value": float(top.get("score", 0.0)),
                    "label": "low"
                }
            }]

        output["incidents"].append(
            {
                "incident_id": incident_id,
                "start_time": inc.get("start_time"),
                "end_time": inc.get("end_time"),
                "root_cause_candidates": ranked,
                "debug": {
                    "candidate_count": len(candidates),
                    "top_score": ranked[0]["score"] if ranked else None,
                    "valid_root_cause": valid,
                },
            }
        )

    final_out = out_path or (artifacts_dir / "incident_root_causes.json")
    write_json(final_out, output)

    return output


if __name__ == "__main__":
    build_ranked_root_causes(Path("outputs"))