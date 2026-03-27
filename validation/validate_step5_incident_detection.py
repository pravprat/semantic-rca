#!/usr/bin/env python3
from __future__ import annotations

import argparse
from pathlib import Path

from validate_pipeline_steps import step5_validate


def main() -> None:
    p = argparse.ArgumentParser(description="Validate step 5 incident detection outputs.")
    p.add_argument("--outputs-dir", default="outputs")
    p.add_argument("--compat-v142", action="store_true")
    args = p.parse_args()
    step5_validate(Path(args.outputs_dir), compat_v142=args.compat_v142)


if __name__ == "__main__":
    main()

