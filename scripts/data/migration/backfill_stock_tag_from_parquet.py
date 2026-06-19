#!/usr/bin/env python3
"""
一次性回填：从 fundamental_aligned.parquet 的 idx_hs300/zz500/zz1000 列
回填 tag_dictionary + stock_tag 表。

仅这 3 个指数列有真实成员（各 300/500/1000）。其余 13 个标签列在 parquet 中
全为 0/NA（死列），不产生任何 stock_tag 行。

concept 类标签字典暂不种（无成员数据），待接入真实概念源后由管理接口添加。

用法:
  python scripts/data/migration/backfill_stock_tag_from_parquet.py --dry-run
  python scripts/data/migration/backfill_stock_tag_from_parquet.py
"""

from __future__ import annotations

import argparse
import os
from pathlib import Path

import pandas as pd
from sqlalchemy import create_engine, text

try:
    from dotenv import load_dotenv
except Exception:  # pragma: no cover
    load_dotenv = None


PROJECT_ROOT = Path(__file__).resolve().parents[3]
PARQUET_PATH = PROJECT_ROOT / "db" / "custom" / "fundamental_aligned.parquet"

INDEX_TAG_DEFS = [
    ("hs300", "沪深300", "index", "csi", 1),
    ("csi500", "中证500", "index", "csi", 2),
    ("csi1000", "中证1000", "index", "csi", 3),
    ("chinext", "创业板指", "index", "csi", 4),
    ("margin", "两融标的", "index", "csi", 5),
    ("all", "全市场", "index", "csi", 6),
]

INDEX_PARQUET_COL = {
    "hs300": "idx_hs300",
    "csi500": "idx_zz500",
    "csi1000": "idx_zz1000",
    "chinext": "idx_chinext",
    "margin": "idx_margin",
    "all": "idx_all",
}


def get_db_url() -> str:
    if load_dotenv is not None:
        env_path = PROJECT_ROOT / ".env"
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


def seed_tag_dictionary(engine, dry_run: bool) -> None:
    print("=== seed tag_dictionary ===")
    rows = [
        {
            "tag_code": code,
            "tag_name": name,
            "tag_category": cat,
            "source": src,
            "is_active": code in {"hs300", "csi500", "csi1000"},
            "sort_order": order,
        }
        for code, name, cat, src, order in INDEX_TAG_DEFS
    ]
    for r in rows:
        print(f"  upsert {r['tag_code']} ({r['tag_name']}) active={r['is_active']}")
    if dry_run:
        print("(dry-run, no changes)")
        return
    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO tag_dictionary
                    (tag_code, tag_name, tag_category, source, is_active, sort_order)
                VALUES (:tag_code, :tag_name, :tag_category, :source, :is_active, :sort_order)
                ON CONFLICT (tag_code) DO UPDATE SET
                    tag_name = EXCLUDED.tag_name,
                    tag_category = EXCLUDED.tag_category,
                    source = EXCLUDED.source,
                    is_active = EXCLUDED.is_active,
                    sort_order = EXCLUDED.sort_order
                """
            ),
            rows,
        )


def backfill_stock_tag(engine, dry_run: bool) -> None:
    print(f"=== backfill stock_tag from {PARQUET_PATH} ===")
    if not PARQUET_PATH.exists():
        raise FileNotFoundError(f"parquet not found: {PARQUET_PATH}")

    needed_cols = ["symbol"] + list(INDEX_PARQUET_COL.values())
    df = pd.read_parquet(PARQUET_PATH, columns=needed_cols)
    print(f"  parquet rows: {len(df)}")

    total_rows = 0
    all_rows: list[dict] = []
    for tag_code, col in INDEX_PARQUET_COL.items():
        if col not in df.columns:
            print(f"  WARN: column {col} not in parquet, skip {tag_code}")
            continue
        symbols = df.loc[df[col] == 1, "symbol"].unique().tolist()
        symbols = [str(s).upper().strip() for s in symbols if s and str(s).strip()]
        print(f"  {tag_code} <- {col}: {len(symbols)} symbols")
        for sym in symbols:
            all_rows.append({"symbol": sym, "tag_code": tag_code, "source": "parquet_backfill"})
        total_rows += len(symbols)

    print(f"  total stock_tag rows to insert: {total_rows}")
    if dry_run:
        print("(dry-run, no changes)")
        return

    if not all_rows:
        print("no rows to insert, skip.")
        return

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                INSERT INTO stock_tag (symbol, tag_code, source)
                VALUES (:symbol, :tag_code, :source)
                ON CONFLICT (symbol, tag_code) DO NOTHING
                """
            ),
            all_rows,
        )
    print("done.")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    engine = create_engine(get_db_url())
    seed_tag_dictionary(engine, args.dry_run)
    backfill_stock_tag(engine, args.dry_run)


if __name__ == "__main__":
    main()
