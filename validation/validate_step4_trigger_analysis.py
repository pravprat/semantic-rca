#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from validate_pipeline_steps import step4_validate


def main() -> None:
    p = argparse.ArgumentParser(description="Validate step 4 trigger analysis outputs.")
    p.add_argument("--outputs-dir", default="outputs")
    args = p.parse_args()
    step4_validate(Path(args.outputs_dir))


if __name__ == "__main__":
    main()

