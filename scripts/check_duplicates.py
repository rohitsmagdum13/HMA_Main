#!/usr/bin/env python
"""Standalone script to check for duplicate files."""
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "src"))

from hma_main.cli import main

if __name__ == "__main__":
    # Force duplicate check mode
    sys.argv.insert(1, "--mode")
    sys.argv.insert(2, "check-duplicates")
    main()