# parsers/ingest_runner.py

from __future__ import annotations

import json
from typing import List, Dict, Any

from parsers.log_reader import LogReader, iter_log_files
from parsers.eventizer import Eventizer
from semantic.enrichment import enrich_event


def run_ingest(
    logfile: str,
    output_path: str,
    batch_size: int = 2000,
    file_batch_size: int = 10,
) -> None:

    reader = LogReader()
    eventizer = Eventizer()

    count = 0
    batch: List[Dict[str, Any]] = []
    files = iter_log_files(logfile)
    total_files = len(files)

    if total_files == 0:
        raise RuntimeError(f"No input log files found at: {logfile}")

    print(
        f"[ingest] discovered files={total_files} "
        f"file_batch_size={file_batch_size} event_batch_size={batch_size}"
    )

    with open(output_path, "w", encoding="utf-8") as out:
        batch_file_index = 0

        for idx, file_path in enumerate(files, start=1):
            if (idx - 1) % max(1, file_batch_size) == 0:
                batch_file_index += 1
                end_idx = min(total_files, idx - 1 + max(1, file_batch_size))
                print(
                    f"[ingest] file_batch={batch_file_index} "
                    f"files={idx}-{end_idx}/{total_files}"
                )

            for record in reader.iter_records(file_path):
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