"""
DB utility CLI:
  hma-db init           # apply schema.sql
  hma-db load-mba       # load all MBA CSVs from S3 into staging tables
"""

from __future__ import annotations
import argparse
from pathlib import Path

from hma_main.db.engine import run_schema_sql
from hma_main.services.mba_csv_loader import load_all_mba_csvs


def main():
    ap = argparse.ArgumentParser("hma-db", description="HMA database utilities")
    sub = ap.add_subparsers(dest="cmd", required=True)

    p_init = sub.add_parser("init", help="Create/upgrade DB schema from schema.sql")
    p_init.add_argument(
        "--schema",
        default=str(Path("src/hma_main/db/schema.sql")),
        help="Path to schema.sql (default: src/hma_main/db/schema.sql)",
    )

    sub.add_parser("load-mba", help="Load MBA CSVs from S3 into staging tables")

    args = ap.parse_args()

    if args.cmd == "init":
        run_schema_sql(args.schema)
        print("✅ DB schema applied.")
    elif args.cmd == "load-mba":
        stats = load_all_mba_csvs()
        print(f"✅ Ingestion complete: {stats}")


if __name__ == "__main__":
    main()
