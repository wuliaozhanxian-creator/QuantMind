#!/usr/bin/env python3
"""兼容入口：将 fundamental_aligned.parquet 增量同步到 stock_daily_latest。

默认行为与历史 v2 脚本保持接近：
1. 新增交易日增量导入。
2. 额外回刷最近 30 个自然日，覆盖近期修订数据。

底层实现复用 scripts/data/maintenance/sync_stock_daily_latest_from_parquet.py，
避免维护两套独立导库逻辑。
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

from dotenv import load_dotenv
from sqlalchemy import create_engine


def project_root() -> Path:
    return Path(__file__).resolve().parents[1]


ROOT = project_root()
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from scripts.data.maintenance.sync_stock_daily_latest_from_parquet import (  # noqa: E402
    LOGGER,
    ensure_raw_columns,
    get_database_url,
    get_table_columns,
    get_table_coverage,
    invalidate_data_status_cache,
    load_incremental_frame,
    normalize_frame,
    to_rows,
    upsert_rows,
)


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Sync fundamental_aligned.parquet into stock_daily_latest")
    parser.add_argument("--database-url", default=None, help="Override DATABASE_URL")
    parser.add_argument(
        "--parquet-path",
        default=str(ROOT / "db" / "custom" / "fundamental_aligned.parquet"),
        help="Source parquet path",
    )
    parser.add_argument("--batch-size", type=int, default=1000, help="Upsert batch size")
    parser.add_argument(
        "--refresh-days",
        type=int,
        default=30,
        help="Also re-sync rows whose trade_date falls within the recent N calendar days",
    )
    parser.add_argument("--dry-run", action="store_true", help="Preview only")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    load_dotenv(ROOT / ".env", override=True)

    parquet_path = Path(args.parquet_path).expanduser().resolve()
    db_url = get_database_url(args.database_url)
    engine = create_engine(db_url)

    ensure_raw_columns(engine)
    table_columns = get_table_columns(engine)
    local_rows, local_min, local_max = get_table_coverage(engine)
    LOGGER.info(
        "[sync_to_postgres_v2] stock_daily_latest current rows=%s range=[%s, %s]",
        local_rows,
        local_min.date() if local_min is not None else None,
        local_max.date() if local_max is not None else None,
    )

    refresh_since = None
    if args.refresh_days and args.refresh_days > 0:
        import pandas as pd

        refresh_since = pd.Timestamp.today().normalize() - pd.Timedelta(days=int(args.refresh_days))
        LOGGER.info(
            "[sync_to_postgres_v2] recent refresh enabled: refresh_days=%s refresh_since=%s",
            args.refresh_days,
            refresh_since.date(),
        )

    incoming = load_incremental_frame(parquet_path, local_max, refresh_since=refresh_since)
    if incoming.empty:
        LOGGER.info("[sync_to_postgres_v2] no rows matched incremental/refresh window")
        return

    LOGGER.info(
        "[sync_to_postgres_v2] parquet rows=%s dates=%s..%s",
        len(incoming),
        incoming["trade_date"].min().date(),
        incoming["trade_date"].max().date(),
    )

    normalized, skipped_columns = normalize_frame(incoming, table_columns)
    if skipped_columns:
        LOGGER.info(
            "[sync_to_postgres_v2] table columns not present in parquet, leaving untouched/default: %s",
            skipped_columns,
        )

    if args.dry_run:
        return

    rows = to_rows(normalized, list(normalized.columns))
    upsert_rows(db_url, list(normalized.columns), rows, args.batch_size)
    invalidate_data_status_cache()

    local_rows, local_min, local_max = get_table_coverage(engine)
    LOGGER.info(
        "[sync_to_postgres_v2] stock_daily_latest synced rows=%s range=[%s, %s]",
        local_rows,
        local_min.date() if local_min is not None else None,
        local_max.date() if local_max is not None else None,
    )


if __name__ == "__main__":
    main()
