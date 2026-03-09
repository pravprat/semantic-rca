import json
from collections import Counter
from pathlib import Path

# Resolve project root
PROJECT_ROOT = Path(__file__).resolve().parents[1]
OUTPUTS_DIR = PROJECT_ROOT / "outputs"

EVENTS_PATH = OUTPUTS_DIR / "events.jsonl"
CLUSTERS_PATH = OUTPUTS_DIR / "clusters.json"
OUT_JSON = OUTPUTS_DIR / "cluster_summaries.json"
OUT_MD = OUTPUTS_DIR / "cluster_summaries.md"


def load_events():
    if not EVENTS_PATH.exists():
        raise FileNotFoundError(f"Missing {EVENTS_PATH}")
    events = []
    with EVENTS_PATH.open("r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    return events


def summarize_cluster(cluster, events, top_k_samples=5):
    member_indices = cluster["member_indices"]
    rep_idx = cluster["representative_index"]

    rep_event = events[rep_idx]

    sample_events = [events[i] for i in member_indices[:top_k_samples]]

    severities = Counter(e.get("severity") for e in sample_events)
    services = Counter(e.get("service") for e in sample_events)

    return {
        "cluster_id": cluster["cluster_id"],
        "size": cluster["size"],
        "representative_event": {
            "event_index": rep_idx,
            "service": rep_event.get("service"),
            "severity": rep_event.get("severity"),
            "raw_text": rep_event.get("raw_text")
        },
        "top_services": dict(services),
        "top_severities": dict(severities),
        "sample_messages": [
            e.get("raw_text", "") for e in sample_events
        ]
    }


def write_markdown(summaries):
    lines = []
    lines.append("# Cluster Inspection Summary\n")

    for s in summaries:
        lines.append(f"## Cluster {s['cluster_id']} (size={s['size']})")
        rep = s["representative_event"]
        lines.append(f"- **Representative** [{rep['severity']}] {rep['service']}")
        lines.append(f"  - {rep['raw_text']}")
        lines.append(f"- **Top services:** {s['top_services']}")
        lines.append(f"- **Top severities:** {s['top_severities']}")
        lines.append(f"- **Sample messages:**")
        for msg in s["sample_messages"]:
            lines.append(f"  - {msg}")
        lines.append("")

    return "\n".join(lines)


def main():
    if not CLUSTERS_PATH.exists():
        raise FileNotFoundError(f"Missing {CLUSTERS_PATH}")

    events = load_events()

    with CLUSTERS_PATH.open("r", encoding="utf-8") as f:
        clusters = json.load(f)

    # Sort clusters by size (largest first)
    sorted_clusters = sorted(
        clusters.values(),
        key=lambda c: c["size"],
        reverse=True
    )

    summaries = []
    for c in sorted_clusters:
        summaries.append(summarize_cluster(c, events))

    # Write JSON
    with OUT_JSON.open("w", encoding="utf-8") as f:
        json.dump(summaries, f, ensure_ascii=False, indent=2)

    # Write Markdown (optional but very useful)
    with OUT_MD.open("w", encoding="utf-8") as f:
        f.write(write_markdown(summaries))

    print(f"[inspect] wrote {len(summaries)} cluster summaries")
    print(f"  - {OUT_JSON}")
    print(f"  - {OUT_MD}")


if __name__ == "__main__":
    main()