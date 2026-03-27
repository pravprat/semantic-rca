# cluster/causal/validation/event_checks.py

from __future__ import annotations

from typing import List, Dict, Any


def validate_grounded_events(root_events: List[Dict[str, Any]]) -> None:
    for e in root_events:
        if not e.get("event_id"):
            raise RuntimeError("[causal_analysis] grounded event missing event_id")
        rc = e.get("response_code")
        if rc is not None:
            try:
                rc_val = int(rc)
            except Exception:
                # Non-numeric response code is tolerated for now.
                continue
            if rc_val < 400:
                # Accept non-HTTP fallback failure signals.
                status_family = str(e.get("status_family") or "").lower()
                sev = str(e.get("severity") or "").upper()
                if status_family != "failure" and sev not in {"ERROR", "FATAL"} and not e.get("failure_hint"):
                    raise RuntimeError("[causal_analysis] grounded event is not a failure")
        else:
            status_family = str(e.get("status_family") or "").lower()
            sev = str(e.get("severity") or "").upper()
            if status_family != "failure" and sev not in {"ERROR", "FATAL"} and not e.get("failure_hint"):
                raise RuntimeError("[causal_analysis] grounded event is not a failure")