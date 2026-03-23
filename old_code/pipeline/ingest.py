from __future__ import annotations

import json
import os
from pathlib import Path
from typing import List, Dict, Any

## Imports from the Pipeline
from parsers.log_reader import LogReader, iter_records_from_path
from parsers.eventizer import Eventizer
from semantic.enrichment import enrich_event

PROJECT_ROOT = Path(__file__).resolve().parent
OUTPUT_DIR = PROJECT_ROOT / "outputs"

def ensure_outputs():
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

def cmd_ingest(args):

    ensure_outputs()

    reader = LogReader()
    eventizer = Eventizer()

    events_path = os.path.join(OUTPUT_DIR, "events.jsonl")

    count = 0
    batch = []
    BATCH_SIZE = 2000

    with open(events_path, "w", encoding="utf-8") as out:

        for record in iter_records_from_path(reader, args.logfile):

            batch.append(record)

            if len(batch) >= BATCH_SIZE:

                for ev in eventizer.iter_events(batch):
                    ev_dict = ev.to_dict()
                    ev_dict = enrich_event(ev_dict)
                    out.write(json.dumps(ev_dict, ensure_ascii=False) + "\n")
                    count += 1

                batch.clear()

        # flush remainder
        if batch:
            for ev in eventizer.iter_events(batch):
                ev_dict = ev.to_dict()
                ev_dict = enrich_event(ev_dict)
                out.write(json.dumps(ev_dict, ensure_ascii=False) + "\n")
                count += 1

    print(f"[ingest] wrote {count} events -> {events_path}")

def load_events() -> List[Dict[str, Any]]:
    events_path = os.path.join(OUTPUT_DIR, "events.jsonl")
    events = []
    with open(events_path, "r", encoding="utf-8") as f:
        for line in f:
            events.append(json.loads(line))
    return events