#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from validate_pipeline_steps import step1_validate


def main() -> None:
    p = argparse.ArgumentParser(description="Validate step 1 ingest outputs.")
    p.add_argument("--outputs-dir", default="outputs")
    p.add_argument("--raw-log", default=None)
    args = p.parse_args()
    step1_validate(Path(args.outputs_dir), Path(args.raw_log) if args.raw_log else None)


if __name__ == "__main__":
    main()

