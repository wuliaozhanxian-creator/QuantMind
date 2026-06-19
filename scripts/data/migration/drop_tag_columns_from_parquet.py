#!/usr/bin/env python3
"""
一次性脚本：从 fundamental_aligned.parquet 移除 16 个标签列。

标签已迁移至 PG stock_tag 长表，parquet 不再需要这些列。
执行前自动备份原文件到 db/custom/backup/。

用法:
  python scripts/data/migration/drop_tag_columns_from_parquet.py --dry-run
  python scripts/data/migration/drop_tag_columns_from_parquet.py
"""

from __future__ import annotations

import argparse
import shutil
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
PARQUET_PATH = PROJECT_ROOT / "db" / "custom" / "fundamental_aligned.parquet"
BACKUP_DIR = PROJECT_ROOT / "db" / "custom" / "backup"

TAG_COLUMNS = [
    "idx_hs300",
    "idx_zz500",
    "idx_zz1000",
    "idx_chinext",
    "idx_margin",
    "idx_all",
    "concept_ai",
    "concept_chip",
    "concept_new_energy",
    "concept_pv",
    "concept_military",
    "concept_medical",
    "concept_fintech",
    "concept_consumption",
    "concept_state_owned",
    "concept_lithium",
]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"parquet not found: {PARQUET_PATH}")

    print(f"reading {PARQUET_PATH} ...")
    df = pd.read_parquet(PARQUET_PATH)
    print(f"  shape before: {df.shape}")

    present = [c for c in TAG_COLUMNS if c in df.columns]
    missing = [c for c in TAG_COLUMNS if c not in df.columns]
    print(f"  tag columns present: {len(present)}")
    if missing:
        print(f"  tag columns already absent: {missing}")

    if not present:
        print("nothing to drop; exiting.")
        return

    df = df.drop(columns=present)
    print(f"  shape after: {df.shape}")

    if args.dry_run:
        print("(dry-run, no write)")
        return

    BACKUP_DIR.mkdir(parents=True, exist_ok=True)
    backup_path = BACKUP_DIR / "fundamental_aligned.pre_tag_migration.parquet"
    print(f"backing up original -> {backup_path}")
    shutil.copy2(PARQUET_PATH, backup_path)

    print(f"writing {PARQUET_PATH} ...")
    df.to_parquet(PARQUET_PATH, index=False, engine="pyarrow")
    print("done.")


if __name__ == "__main__":
    main()
