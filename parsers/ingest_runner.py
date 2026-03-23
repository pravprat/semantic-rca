# parsers/ingest_runner.py

from __future__ import annotations

import json
from typing import List, Dict, Any

from parsers.log_reader import LogReader, iter_records_from_path
from parsers.eventizer import Eventizer
from semantic.enrichment import enrich_event


def run_ingest(
    logfile: str,
    output_path: str,
    batch_size: int = 2000,
) -> None:

    reader = LogReader()
    eventizer = Eventizer()

    count = 0
    batch: List[Dict[str, Any]] = []

    with open(output_path, "w", encoding="utf-8") as out:

        for record in iter_records_from_path(reader, logfile):
            batch.append(record)

            if len(batch) >= batch_size:
                count += _flush_batch(batch, eventizer, out)
                batch.clear()

        # ---- flush remaining -----------------------------------------
        if batch:
            count += _flush_batch(batch, eventizer, out)

    print(f"[ingest] wrote {count} events -> {output_path}")


def _flush_batch(batch, eventizer, out) -> int:
    count = 0

    for ev in eventizer.iter_events(batch):
        ev_dict = ev.to_dict()
        ev_dict = enrich_event(ev_dict)

        out.write(json.dumps(ev_dict, ensure_ascii=False) + "\n")
        count += 1

    return count