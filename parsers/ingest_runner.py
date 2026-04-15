# parsers/ingest_runner.py

from __future__ import annotations

import json
import gzip
from pathlib import Path
from typing import List, Dict, Any

from parsers.log_reader import LogReader, iter_log_files
from parsers.eventizer import Eventizer
from semantic.enrichment import enrich_event
from event_io import EventParquetBatchWriter, is_parquet_path, normalize_event_row


def run_ingest(
    logfile: str,
    output_path: str,
    batch_size: int = 5000,
    file_batch_size: int = 20,
    logfile_list: str | None = None,
) -> None:

    reader = LogReader()
    eventizer = Eventizer()

    count = 0
    batch: List[Dict[str, Any]] = []
    if logfile_list:
        list_path = Path(logfile_list)
        if not list_path.exists():
            raise RuntimeError(f"logfile list does not exist: {logfile_list}")
        raw_lines = list_path.read_text(encoding="utf-8").splitlines()
        files = [Path(x.strip()) for x in raw_lines if x.strip()]
    else:
        files = iter_log_files(logfile)
    total_files = len(files)

    if total_files == 0:
        src = f"logfile list {logfile_list}" if logfile_list else f"path {logfile}"
        raise RuntimeError(f"No input log files found from {src}")

    print(
        f"[ingest] discovered files={total_files} "
        f"file_batch_size={file_batch_size} event_batch_size={batch_size}"
    )

    as_parquet = is_parquet_path(output_path)
    out = None
    parquet_writer = EventParquetBatchWriter(output_path) if as_parquet else None
    if not as_parquet:
        out = open(output_path, "w", encoding="utf-8")
    try:
        batch_file_index = 0

        for idx, file_path in enumerate(files, start=1):
            if (idx - 1) % max(1, file_batch_size) == 0:
                batch_file_index += 1
                end_idx = min(total_files, idx - 1 + max(1, file_batch_size))
                print(
                    f"[ingest] file_batch={batch_file_index} "
                    f"files={idx}-{end_idx}/{total_files}"
                )

            if file_path.suffixes[-2:] == [".log", ".gz"] or file_path.suffix == ".gz":
                with gzip.open(file_path, "rt", encoding="utf-8", errors="replace") as f:
                    records_iter = reader._iter_lines(f, source_file=file_path.name)
                    for record in records_iter:
                        batch.append(record)
                        if len(batch) >= batch_size:
                            count += _flush_batch(batch, eventizer, out, parquet_writer)
                            batch.clear()
                continue

            for record in reader.iter_records(file_path):
                batch.append(record)

                if len(batch) >= batch_size:
                    count += _flush_batch(batch, eventizer, out, parquet_writer)
                    batch.clear()

        # ---- flush remaining -----------------------------------------
        if batch:
            count += _flush_batch(batch, eventizer, out, parquet_writer)
    finally:
        if out is not None:
            out.close()
        if parquet_writer is not None:
            parquet_writer.close()

    print(f"[ingest] wrote {count} events -> {output_path}")


def _flush_batch(batch, eventizer, out, parquet_writer) -> int:
    count = 0
    rows: List[Dict[str, Any]] = []

    for ev in eventizer.iter_events(batch):
        ev_dict = normalize_event_row(ev.to_dict())
        ev_dict = enrich_event(ev_dict)
        if out is not None:
            out.write(json.dumps(ev_dict, ensure_ascii=False) + "\n")
        else:
            rows.append(ev_dict)
        count += 1

    if parquet_writer is not None and rows:
        parquet_writer.write_rows(rows)
    return count