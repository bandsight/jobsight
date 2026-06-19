#!/usr/bin/env python3
"""Compatibility wrapper for the old workflow entry point."""

from jobsight.cli import main


if __name__ == "__main__":
    raise SystemExit(main(["run"]))
