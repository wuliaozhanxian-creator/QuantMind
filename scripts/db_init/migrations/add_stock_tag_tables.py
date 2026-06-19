#!/usr/bin/env python3
"""
迁移：新建 tag_dictionary / stock_tag 表，并从 stock_daily_latest 移除 16 个标签列。

分两阶段，可独立执行：
  阶段1（--phase=create，默认）：CREATE tag_dictionary + stock_tag + 索引 + FK
  阶段2（--phase=drop-columns）：ALTER stock_daily_latest DROP COLUMN 16 列

用法:
  python scripts/db_init/migrations/add_stock_tag_tables.py --phase=create
  python scripts/db_init/migrations/add_stock_tag_tables.py --phase=drop-columns
  python scripts/db_init/migrations/add_stock_tag_tables.py --phase=all --dry-run

生产迁移顺序：先 create + 回填 stock_tag + 验证，再 drop-columns。
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


TAG_COLUMNS_TO_DROP = [
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

CREATE_SQL = [
    """
    CREATE TABLE IF NOT EXISTS tag_dictionary (
        tag_code VARCHAR(64) PRIMARY KEY,
        tag_name VARCHAR(128) NOT NULL,
        tag_category VARCHAR(32) NOT NULL,
        source VARCHAR(64),
        is_active BOOLEAN NOT NULL DEFAULT TRUE,
        sort_order INTEGER NOT NULL DEFAULT 0,
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS stock_tag (
        id BIGSERIAL PRIMARY KEY,
        symbol VARCHAR(16) NOT NULL,
        tag_code VARCHAR(64) NOT NULL REFERENCES tag_dictionary(tag_code) ON DELETE RESTRICT,
        source VARCHAR(64),
        created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
        updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )
    """,
    "CREATE UNIQUE INDEX IF NOT EXISTS uq_stock_tag_symbol_code ON stock_tag(symbol, tag_code)",
    "CREATE INDEX IF NOT EXISTS ix_stock_tag_tag_code ON stock_tag(tag_code)",
    "CREATE INDEX IF NOT EXISTS ix_stock_tag_symbol ON stock_tag(symbol)",
]


def get_db_url() -> str:
    if load_dotenv is not None:
        project_root = Path(__file__).resolve().parents[3]
        env_path = project_root / ".env"
        if env_path.exists():
            load_dotenv(env_path)

    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url.replace("postgresql+asyncpg://", "postgresql://")
    host = os.getenv("DB_MASTER_HOST") or os.getenv("DB_HOST")
    port = os.getenv("DB_MASTER_PORT") or os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db = os.getenv("DB_NAME")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


def run_phase_create(engine, dry_run: bool) -> None:
    print("=== Phase: create tag tables ===")
    if dry_run:
        for sql in CREATE_SQL:
            print(sql.strip()[:200])
        print("(dry-run, no changes)")
        return
    with engine.begin() as conn:
        for sql in CREATE_SQL:
            conn.execute(text(sql))
    print("done.")


def run_phase_drop_columns(engine, dry_run: bool) -> None:
    print("=== Phase: drop tag columns from stock_daily_latest ===")
    if dry_run:
        for col in TAG_COLUMNS_TO_DROP:
            print(f"ALTER TABLE stock_daily_latest DROP COLUMN IF EXISTS {col}")
        print("(dry-run, no changes)")
        return
    with engine.begin() as conn:
        for col in TAG_COLUMNS_TO_DROP:
            conn.execute(text(f"ALTER TABLE stock_daily_latest DROP COLUMN IF EXISTS {col}"))
    print("done.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--phase",
        choices=["create", "drop-columns", "all"],
        default="create",
        help="create=建表; drop-columns=删 sdl 标签列; all=两阶段连续",
    )
    parser.add_argument("--dry-run", action="store_true", help="只打印 SQL 不执行")
    args = parser.parse_args()

    if args.dry_run:
        if args.phase in ("create", "all"):
            run_phase_create(None, True)
        if args.phase in ("drop-columns", "all"):
            run_phase_drop_columns(None, True)
        return

    engine = create_engine(get_db_url())
    if args.phase in ("create", "all"):
        run_phase_create(engine, False)
    if args.phase in ("drop-columns", "all"):
        run_phase_drop_columns(engine, False)


if __name__ == "__main__":
    main()
