#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from validate_pipeline_steps import step8_validate


def main() -> None:
    p = argparse.ArgumentParser(description="Validate step 8 evidence bundle outputs.")
    p.add_argument("--outputs-dir", default="outputs")
    p.add_argument("--compat-v142", action="store_true")
    args = p.parse_args()
    step8_validate(Path(args.outputs_dir), compat_v142=args.compat_v142)


if __name__ == "__main__":
    main()

