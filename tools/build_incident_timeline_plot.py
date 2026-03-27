#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
from collections import defaultdict
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple


def _parse_dt(value: Optional[str]) -> Optional[datetime]:
    if not value or not isinstance(value, str):
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s[:-1] + "+00:00"
    try:
        dt = datetime.fromisoformat(s)
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=timezone.utc)
        return dt
    except Exception:
        return None


def _floor_minute(dt: datetime, bucket_minutes: int) -> datetime:
    minute = (dt.minute // bucket_minutes) * bucket_minutes
    return dt.replace(minute=minute, second=0, microsecond=0)


def _load_json(path: Path) -> Any:
    with path.open("r", encoding="utf-8") as f:
        return json.load(f)


def _load_jsonl(path: Path) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    with path.open("r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line:
                rows.append(json.loads(line))
    return rows


def _series_for_incident(
    events: List[Dict[str, Any]],
    start: datetime,
    end: datetime,
    bucket_minutes: int,
) -> Tuple[List[datetime], List[int], List[int]]:
    pad = timedelta(minutes=max(1, bucket_minutes * 2))
    lo = start - pad
    hi = end + pad

    total_buckets: Dict[datetime, int] = defaultdict(int)
    fail_buckets: Dict[datetime, int] = defaultdict(int)

    cur = _floor_minute(lo, bucket_minutes)
    stop = _floor_minute(hi, bucket_minutes)
    while cur <= stop:
        total_buckets[cur] = 0
        fail_buckets[cur] = 0
        cur += timedelta(minutes=bucket_minutes)

    for ev in events:
        ts = _parse_dt(ev.get("timestamp"))
        if ts is None or ts < lo or ts > hi:
            continue
        b = _floor_minute(ts, bucket_minutes)
        total_buckets[b] += 1
        rc = ev.get("response_code")
        sev = str(ev.get("severity") or "").upper()
        if (isinstance(rc, int) and rc >= 400) or sev in {"ERROR", "FATAL"}:
            fail_buckets[b] += 1

    xs = sorted(total_buckets.keys())
    ys_total = [total_buckets[x] for x in xs]
    ys_fail = [fail_buckets[x] for x in xs]
    return xs, ys_total, ys_fail


def _classify_shape(xs: List[datetime], ys_fail: List[int], start: datetime, end: datetime) -> str:
    if not xs or not ys_fail:
        return "insufficient_data"
    peak = max(ys_fail)
    if peak <= 0:
        return "flat_or_no_failure_signal"

    in_window = [i for i, x in enumerate(xs) if start <= x <= end]
    pre = [i for i, x in enumerate(xs) if x < start]
    post = [i for i, x in enumerate(xs) if x > end]
    if not in_window:
        return "insufficient_data"

    post_avg = sum(ys_fail[i] for i in post) / max(1, len(post))
    in_avg = sum(ys_fail[i] for i in in_window) / max(1, len(in_window))
    pre_avg = sum(ys_fail[i] for i in pre) / max(1, len(pre))

    # Spike relative to pre-window baseline.
    if in_avg < max(1.0, pre_avg * 1.2):
        return "weak_spike"
    if post_avg <= in_avg * 0.4:
        return "spike_and_recover"
    if post_avg >= in_avg * 0.8:
        return "spike_and_plateau"
    return "spike_and_drop"


def _plot_png(
    out_png: Path,
    xs: List[datetime],
    ys_total: List[int],
    ys_fail: List[int],
    incident_id: str,
    start: datetime,
    end: datetime,
) -> None:
    import matplotlib.pyplot as plt

    plt.figure(figsize=(11, 4))
    plt.plot(xs, ys_total, label="total events", linewidth=1.5)
    plt.plot(xs, ys_fail, label="failure signal", linewidth=2.0)
    plt.axvline(start, linestyle="--", linewidth=1.2, label="incident start")
    plt.axvline(end, linestyle="--", linewidth=1.2, label="incident end")
    plt.title(f"Incident timeline: {incident_id}")
    plt.xlabel("time")
    plt.ylabel("event count per bucket")
    plt.legend(loc="upper left")
    plt.tight_layout()
    plt.savefig(out_png, dpi=120)
    plt.close()


def _write_interactive_html(
    out_html: Path,
    incident_id: str,
    start_iso: str,
    end_iso: str,
    shape_label: str,
    points: List[Dict[str, Any]],
) -> None:
    payload = json.dumps(points, ensure_ascii=True)
    html = f"""<!doctype html>
<html>
<head>
  <meta charset="utf-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1" />
  <title>Incident Timeline {incident_id}</title>
  <script src="https://cdn.plot.ly/plotly-2.35.2.min.js"></script>
  <style>
    body {{ font-family: Arial, sans-serif; margin: 12px; }}
    #chart {{ width: 100%; height: 560px; }}
    .meta {{ margin-bottom: 10px; }}
  </style>
</head>
<body>
  <div class="meta">
    <h3>Incident Timeline: {incident_id}</h3>
    <div><b>Shape:</b> {shape_label}</div>
    <div><b>Window:</b> {start_iso} -> {end_iso}</div>
  </div>
  <div id="chart"></div>
  <script>
    const points = {payload};
    const xs = points.map(p => p.ts);
    const total = points.map(p => p.total_events);
    const fail = points.map(p => p.failure_signal);
    const startTime = "{start_iso}";
    const endTime = "{end_iso}";

    const traces = [
      {{
        x: xs, y: total, mode: "lines+markers", name: "total events",
        line: {{ width: 2 }}
      }},
      {{
        x: xs, y: fail, mode: "lines+markers", name: "failure signal",
        line: {{ width: 2 }}
      }}
    ];

    const layout = {{
      hovermode: "x unified",
      xaxis: {{ title: "Time", rangeslider: {{ visible: true }} }},
      yaxis: {{ title: "Events per bucket" }},
      shapes: [
        {{
          type: "rect", xref: "x", yref: "paper",
          x0: startTime, x1: endTime, y0: 0, y1: 1,
          fillcolor: "rgba(255, 0, 0, 0.08)", line: {{ width: 0 }}
        }},
        {{
          type: "line", xref: "x", yref: "paper", x0: startTime, x1: startTime, y0: 0, y1: 1,
          line: {{ dash: "dash", width: 1 }}
        }},
        {{
          type: "line", xref: "x", yref: "paper", x0: endTime, x1: endTime, y0: 0, y1: 1,
          line: {{ dash: "dash", width: 1 }}
        }}
      ],
      annotations: [
        {{ x: startTime, y: 1, yref: "paper", text: "start", showarrow: false, yanchor: "bottom" }},
        {{ x: endTime, y: 1, yref: "paper", text: "end", showarrow: false, yanchor: "bottom" }}
      ]
    }};

    Plotly.newPlot("chart", traces, layout, {{responsive: true}});
  </script>
</body>
</html>
"""
    out_html.write_text(html, encoding="utf-8")


def build_incident_timeline_plot(
    events_path: Path,
    incidents_path: Path,
    out_dir: Path,
    bucket_minutes: int = 1,
) -> List[Dict[str, Any]]:
    events = _load_jsonl(events_path)
    incidents = _load_json(incidents_path)
    out_dir.mkdir(parents=True, exist_ok=True)

    summaries: List[Dict[str, Any]] = []
    for inc in incidents:
        incident_id = str(inc.get("incident_id") or "unknown")
        start = _parse_dt(inc.get("start_time"))
        end = _parse_dt(inc.get("end_time"))
        if start is None or end is None:
            continue

        xs, ys_total, ys_fail = _series_for_incident(events, start, end, bucket_minutes)
        label = _classify_shape(xs, ys_fail, start, end)

        png_path = out_dir / f"incident_timeline_{incident_id}.png"
        plot_status = "png_written"
        try:
            _plot_png(png_path, xs, ys_total, ys_fail, incident_id, start, end)
        except Exception:
            plot_status = "png_skipped_matplotlib_missing_or_failed"

        summary = {
            "incident_id": incident_id,
            "bucket_minutes": bucket_minutes,
            "start_time": inc.get("start_time"),
            "end_time": inc.get("end_time"),
            "shape_label": label,
            "peak_failure_bucket_count": max(ys_fail) if ys_fail else 0,
            "plot_status": plot_status,
            "png_path": str(png_path),
            "html_path": str(out_dir / f"incident_timeline_{incident_id}.html"),
            "points": [
                {
                    "ts": x.isoformat(),
                    "total_events": t,
                    "failure_signal": f,
                }
                for x, t, f in zip(xs, ys_total, ys_fail)
            ],
        }
        summaries.append(summary)

        json_path = out_dir / f"incident_timeline_{incident_id}.json"
        with json_path.open("w", encoding="utf-8") as f:
            json.dump(summary, f, ensure_ascii=False, indent=2)

        _write_interactive_html(
            out_html=out_dir / f"incident_timeline_{incident_id}.html",
            incident_id=incident_id,
            start_iso=str(inc.get("start_time")),
            end_iso=str(inc.get("end_time")),
            shape_label=label,
            points=summary["points"],
        )

    return summaries


def main() -> None:
    p = argparse.ArgumentParser(description="Build incident timeline graph(s) and summaries.")
    p.add_argument("--events", default="outputs/events.jsonl")
    p.add_argument("--incidents", default="outputs/incidents.json")
    p.add_argument("--out-dir", default="outputs")
    p.add_argument("--bucket-minutes", type=int, default=1)
    args = p.parse_args()

    summaries = build_incident_timeline_plot(
        events_path=Path(args.events),
        incidents_path=Path(args.incidents),
        out_dir=Path(args.out_dir),
        bucket_minutes=max(1, args.bucket_minutes),
    )
    print(f"[timeline] built summaries for {len(summaries)} incidents -> {args.out_dir}")


if __name__ == "__main__":
    main()

