#!/usr/bin/env python3
"""从 stock_daily_latest 表生成因子特征快照"""
import os
import pandas as pd
import numpy as np
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

PG_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://quantmind:quantmind2026@localhost:5432/quantmind",
)
OUTPUT_DIR = Path(__file__).resolve().parents[3] / "db" / "feature_snapshots"


def compute_factors(df: pd.DataFrame) -> pd.DataFrame:
    """按 symbol 分组计算因子"""
    df = df.sort_values(["symbol", "trade_date"]).copy()
    g = df.groupby("symbol")

    # 动量因子
    df["mom_ret_1d"] = g["close"].pct_change(1)
    df["mom_ret_3d"] = g["close"].pct_change(3)
    df["mom_ret_5d"] = g["close"].pct_change(5)
    df["mom_ret_10d"] = g["close"].pct_change(10)
    df["mom_ret_20d"] = g["close"].pct_change(20)

    # 均线偏离
    df["ma_5"] = g["close"].rolling(5).mean().reset_index(level=0, drop=True)
    df["ma_10"] = g["close"].rolling(10).mean().reset_index(level=0, drop=True)
    df["ma_20"] = g["close"].rolling(20).mean().reset_index(level=0, drop=True)
    df["ma_60"] = g["close"].rolling(60).mean().reset_index(level=0, drop=True)
    df["ma_dev_5"] = (df["close"] - df["ma_5"]) / df["ma_5"]
    df["ma_dev_10"] = (df["close"] - df["ma_10"]) / df["ma_10"]
    df["ma_dev_20"] = (df["close"] - df["ma_20"]) / df["ma_20"]
    df["ma_dev_60"] = (df["close"] - df["ma_60"]) / df["ma_60"]

    # 波动率
    df["vol_std_5"] = g["mom_ret_1d"].rolling(5).std().reset_index(level=0, drop=True)
    df["vol_std_10"] = g["mom_ret_1d"].rolling(10).std().reset_index(level=0, drop=True)
    df["vol_std_20"] = g["mom_ret_1d"].rolling(20).std().reset_index(level=0, drop=True)
    df["vol_std_60"] = g["mom_ret_1d"].rolling(60).std().reset_index(level=0, drop=True)

    # 成交量因子
    df["vol_ma_5"] = g["volume"].rolling(5).mean().reset_index(level=0, drop=True)
    df["vol_ma_20"] = g["volume"].rolling(20).mean().reset_index(level=0, drop=True)
    df["vol_ratio_5_20"] = df["vol_ma_5"] / (df["vol_ma_20"] + 1)

    # 换手率代理(用成交量/20日平均成交量)
    df["turnover_proxy"] = df["volume"] / (df["volume"].rolling(20).mean() + 1)

    return df


def main():
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    # 读取3年真实历史数据
    cur.execute("""
        SELECT symbol, trade_date, open, high, low, close, volume, amount, pct_change
        FROM stock_daily_latest
        ORDER BY symbol, trade_date
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # 统一 symbol 格式
    df["symbol"] = df["symbol"].astype(str).str.upper()

    print(f"读取 {len(df)} 条记录, {df['symbol'].nunique()} 只标的")

    # 计算因子
    df = compute_factors(df)

    # 按年保存
    df["year"] = df["trade_date"].dt.year
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for year, year_df in df.groupby("year"):
        out = OUTPUT_DIR / f"model_features_{year}.parquet"
        year_df = year_df.drop(columns=["year"])
        year_df.to_parquet(out, index=False)
        nan_ratio = year_df.isna().sum().sum() / year_df.size
        print(
            f"{year}: {year_df.shape}, NaN率={nan_ratio:.1%}, 保存到 {out}"
        )

    print("特征快照生成完成")


if __name__ == "__main__":
    main()
