#!/usr/bin/env python3
"""补充银层换手率字段。

从CSMAR后复权行情中读取TurnoverRate1，添加到银层market_base表。
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.shared.stock_utils import StockCodeUtil

def prefix_symbol(value) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit():
        text = text.zfill(6)
    return StockCodeUtil.to_prefix(text)

def main(year: int = 2026):
    silver_dir = PROJECT_ROOT / "db" / "market_silver_v2"
    csmar_dir = PROJECT_ROOT / "db" / "csmar"
    
    # 读取银层
    silver_path = silver_dir / f"market_base_{year}.parquet"
    df_silver = pd.read_parquet(silver_path)
    df_silver['trade_date'] = pd.to_datetime(df_silver['trade_date'])
    print(f"银层行数: {len(df_silver):,}")
    print(f"银层日期范围: {df_silver.trade_date.min()} ~ {df_silver.trade_date.max()}")
    
    # 检查是否已有换手率
    if 'turnover_rate' in df_silver.columns:
        missing = df_silver['turnover_rate'].isna().sum()
        print(f"已有换手率字段，缺失率: {missing/len(df_silver)*100:.1f}%")
        if missing == 0:
            print("换手率已完整，无需更新")
            return
    
    # 读取后复权行情
    bward_dirs = [d for d in csmar_dir.glob("股票历史日行情信息表(后复权)*") if d.is_dir()]
    if not bward_dirs:
        print("未找到后复权行情目录")
        return
    
    bward_csv = bward_dirs[0] / "TRD_BwardQuotation.csv"
    print(f"\n读取: {bward_csv}")
    df_bward = pd.read_csv(bward_csv, encoding='utf-8-sig')
    
    # 过滤正常交易
    df_bward = df_bward[(df_bward['Filling'] == 0) & (df_bward['StateCode'] == 0)].copy()
    df_bward['TradingDate'] = pd.to_datetime(df_bward['TradingDate'])
    df_bward['symbol'] = df_bward['Symbol'].map(prefix_symbol)
    df_bward = df_bward[df_bward['symbol'].str.match(r'^(SH|SZ|BJ)\d{6}$', na=False)].copy()
    
    print(f"后复权行情行数: {len(df_bward):,}")
    print(f"日期范围: {df_bward.TradingDate.min()} ~ {df_bward.TradingDate.max()}")
    
    # 提取换手率
    df_turnover = df_bward[['symbol', 'TradingDate', 'TurnoverRate1']].copy()
    df_turnover = df_turnover.rename(columns={'TradingDate': 'trade_date', 'TurnoverRate1': 'turnover_rate'})
    
    # 合并到银层
    df_merged = df_silver.merge(df_turnover, on=['symbol', 'trade_date'], how='left')
    
    # 检查覆盖
    turnover_missing = df_merged['turnover_rate'].isna().sum()
    print(f"\n换手率缺失: {turnover_missing}/{len(df_merged)} ({turnover_missing/len(df_merged)*100:.1f}%)")
    
    # 写回
    df_merged.to_parquet(silver_path, index=False)
    print(f"已更新: {silver_path}")

if __name__ == "__main__":
    import argparse
    parser = argparse.ArgumentParser()
    parser.add_argument("--year", type=int, default=2026)
    args = parser.parse_args()
    main(args.year)