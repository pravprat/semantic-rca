# semantic-rca/reports/summarize_cluster.py
from __future__ import annotations

from typing import Dict, Any, List


def summarize_cluster(cluster: Dict[str, Any], events: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    MVP summary:
      - Uses representative event + top keywords (very simple)
      - Later: replace with LLM or domain summarizer
    """
    rep_idx = cluster.get("representative_index")
    rep_event = events[rep_idx] if rep_idx is not None and rep_idx < len(events) else None

    rep_text = ""
    if rep_event:
        rep_text = rep_event.get("raw_text", "")[:400]

    return {
        "cluster_id": cluster.get("cluster_id"),
        "size": cluster.get("size"),
        "representative_text": rep_text
    }