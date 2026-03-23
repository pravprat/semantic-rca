from __future__ import annotations

from pathlib import Path
from typing import Dict, List, Any, Optional

from .utils import load_json, write_json, parse_ts


def _pattern_lookup(patterns: List[Dict[str, Any]]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for p in patterns:
        pid = p.get("pattern_id")
        if pid:
            out[str(pid)] = p
    return out


def _safe_ts(ts: Optional[str]):
    dt = parse_ts(ts)
    return dt


def _sort_patterns(patterns: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return sorted(
        patterns,
        key=lambda p: (
            p.get("first_seen") or "",
            -int(p.get("error_count", 0)),
            -int(p.get("event_count", 0)),
        ),
    )


def _candidate_pattern_ids(root_cause: Dict[str, Any]) -> List[str]:
    ids = root_cause.get("evidence_pattern_ids", []) or []
    return [str(x) for x in ids if x is not None]


def _classify_chain_step(
    pattern: Dict[str, Any],
    root_pattern_ids: set[str],
    root_first_seen: Optional[str],
) -> str:
    pid = str(pattern.get("pattern_id"))

    if pid in root_pattern_ids:
        return "root_cause"

    http_class = pattern.get("http_class")
    first_seen = pattern.get("first_seen")

    if root_first_seen and first_seen and first_seen > root_first_seen:
        if http_class in ("4xx", "5xx"):
            return "propagation"
        return "impact"

    if http_class in ("4xx", "5xx"):
        return "context"

    return "impact"


def _step_summary(pattern: Dict[str, Any]) -> str:
    service = pattern.get("service")
    verb = pattern.get("verb")
    resource = pattern.get("resource")
    http_class = pattern.get("http_class")
    domain = pattern.get("failure_domain")

    return (
        f"{service} performed {verb} {resource} with {http_class}"
        + (f" ({domain})" if domain else "")
    )


def _build_narrative(steps: List[Dict[str, Any]]) -> str:
    if not steps:
        return "No causal chain could be constructed."

    root = next((s for s in steps if s.get("role") == "root_cause"), steps[0])
    props = [s for s in steps if s.get("role") == "propagation"]
    impacts = [s for s in steps if s.get("role") == "impact"]

    text = (
        f"The incident begins with {root.get('summary')}. "
    )

    if props:
        text += "This propagates through "
        text += "; ".join(s.get("summary") for s in props[:3])
        text += ". "

    if impacts:
        text += "Downstream impact is observed in "
        text += "; ".join(s.get("summary") for s in impacts[:3])
        text += "."

    return text.strip()


def build_incident_causal_chains(
    artifacts_dir: Path,
    patterns_path: Path | None = None,
    root_causes_path: Path | None = None,
    out_path: Path | None = None,
) -> Dict[str, Any]:
    artifacts_dir = Path(artifacts_dir)

    patterns_obj = load_json(
        patterns_path or (artifacts_dir / "incident_patterns.json"),
        default={},
    ) or {}

    root_obj = load_json(
        root_causes_path or (artifacts_dir / "incident_root_causes.json"),
        default={},
    ) or {}

    patterns_by_incident = {
        str(inc.get("incident_id")): inc
        for inc in patterns_obj.get("incidents", [])
    }

    output: Dict[str, Any] = {
        "chain_version": "1.0",
        "incidents": [],
    }

    for inc in root_obj.get("incidents", []):
        iid = str(inc.get("incident_id"))
        patt_inc = patterns_by_incident.get(iid, {})
        patterns = _sort_patterns(patt_inc.get("patterns", []) or [])
        lookup = _pattern_lookup(patterns)

        candidates = inc.get("root_cause_candidates", []) or []
        top = candidates[0] if candidates else None

        if not top:
            output["incidents"].append(
                {
                    "incident_id": iid,
                    "start_time": inc.get("start_time"),
                    "end_time": inc.get("end_time"),
                    "root_cause_title": None,
                    "chain": [],
                    "narrative": "No confident root cause was identified, so no causal chain was constructed.",
                }
            )
            continue

        root_pattern_ids = set(_candidate_pattern_ids(top))
        root_primary = top.get("primary_pattern", {}) or {}
        root_first_seen = root_primary.get("first_seen")

        chain: List[Dict[str, Any]] = []
        included = []

        for p in patterns:
            role = _classify_chain_step(p, root_pattern_ids, root_first_seen)

            # Keep only the most useful steps
            if role == "context":
                continue

            step = {
                "pattern_id": p.get("pattern_id"),
                "role": role,
                "first_seen": p.get("first_seen"),
                "last_seen": p.get("last_seen"),
                "service": p.get("service"),
                "actor": p.get("actor"),
                "verb": p.get("verb"),
                "resource": p.get("resource"),
                "http_class": p.get("http_class"),
                "failure_domain": p.get("failure_domain"),
                "event_count": p.get("event_count"),
                "error_count": p.get("error_count"),
                "summary": _step_summary(p),
            }
            included.append(step)

        included.sort(
            key=lambda s: (
                s.get("first_seen") or "",
                0 if s.get("role") == "root_cause" else 1,
            )
        )

        for idx, step in enumerate(included, start=1):
            step["step"] = idx
            chain.append(step)

        output["incidents"].append(
            {
                "incident_id": iid,
                "start_time": inc.get("start_time"),
                "end_time": inc.get("end_time"),
                "root_cause_title": top.get("title"),
                "root_cause_confidence": top.get("confidence"),
                "chain": chain,
                "narrative": _build_narrative(chain),
            }
        )

    final_out = out_path or (artifacts_dir / "incident_causal_chains.json")
    write_json(final_out, output)
    return output


if __name__ == "__main__":
    build_incident_causal_chains(Path("outputs"))