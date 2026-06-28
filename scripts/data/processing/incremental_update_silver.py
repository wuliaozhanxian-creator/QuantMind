#!/usr/bin/env python3
"""增量更新银层市场数据。

从CSMAR新下载的CSV文件中读取缺失日期的数据，追加到银层parquet。
"""

import pandas as pd
import numpy as np
from pathlib import Path
import sys

PROJECT_ROOT = Path(__file__).resolve().parents[3]
sys.path.insert(0, str(PROJECT_ROOT))

from backend.shared.stock_utils import StockCodeUtil

def prefix_symbol(value) -> str:
    """转换为前缀格式股票代码。"""
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit():
        text = text.zfill(6)
    return StockCodeUtil.to_prefix(text)

def main():
    silver_dir = PROJECT_ROOT / "db" / "market_silver_v2"
    csmar_dir = PROJECT_ROOT / "db" / "csmar"
    
    # 1. 读取现有银层2026年数据
    silver_path = silver_dir / "market_base_2026.parquet"
    df_silver = pd.read_parquet(silver_path)
    df_silver['trade_date'] = pd.to_datetime(df_silver['trade_date'])
    existing_dates = set(df_silver['trade_date'].dt.date)
    print(f"银层现有日期数: {len(existing_dates)}")
    print(f"银层最新日期: {max(existing_dates)}")
    
    # 2. 自动查找后复权行情CSV（排除zip文件）
    bward_dirs = sorted(
        [d for d in csmar_dir.glob("股票历史日行情信息表(后复权)*") if d.is_dir()],
        key=lambda x: x.name, reverse=True
    )
    if not bward_dirs:
        print("未找到后复权行情目录")
        return
    bward_csv = bward_dirs[0] / "TRD_BwardQuotation.csv"
    print(f"\n读取: {bward_csv}")
    df_bward = pd.read_csv(bward_csv, encoding='utf-8-sig')
    print(f"后复权行情总行数: {len(df_bward):,}")
    
    # 过滤正常交易数据
    df_bward = df_bward[(df_bward['Filling'] == 0) & (df_bward['StateCode'] == 0)].copy()
    df_bward['TradingDate'] = pd.to_datetime(df_bward['TradingDate'])
    
    # 3. 找出缺失日期
    csv_dates = set(df_bward['TradingDate'].dt.date)
    missing_dates = csv_dates - existing_dates
    print(f"\nCSV中日期数: {len(csv_dates)}")
    print(f"缺失日期: {sorted(missing_dates)}")
    
    if not missing_dates:
        print("无缺失日期，无需更新")
        return
    
    # 4. 筛选缺失日期数据
    df_new = df_bward[df_bward['TradingDate'].dt.date.isin(missing_dates)].copy()
    print(f"新增数据行数: {len(df_new):,}")
    
    # 5. 转换为银层格式
    df_new['symbol'] = df_new['Symbol'].map(prefix_symbol)
    df_new = df_new[df_new['symbol'].str.match(r'^(SH|SZ|BJ)\d{6}$', na=False)].copy()
    
    # 计算前复权价格 (后复权价格 / 后复权因子)
    # 注意：CSV中的ClosePrice是后复权价格，需要原始收盘价计算因子
    # 但CSV没有原始收盘价，需要从日个股回报率文件获取
    
    # 6. 自动查找日个股回报率获取原始收盘价（排除zip文件）
    ret_dirs = sorted(
        [d for d in csmar_dir.glob("日个股回报率文件*") if d.is_dir()],
        key=lambda x: x.name, reverse=True
    )
    if not ret_dirs:
        print("未找到日个股回报率目录")
        return
    # 优先使用较新的目录（文件名数字较大）
    ret_csv = ret_dirs[0] / "TRD_Dalyr.csv"
    if not ret_csv.exists():
        # 有些目录是 TRDNEW_Dalyr.csv
        ret_csv = ret_dirs[0] / "TRDNEW_Dalyr.csv"
    print(f"\n读取: {ret_csv}")
    df_ret = pd.read_csv(ret_csv, encoding='utf-8-sig')
    df_ret['Trddt'] = pd.to_datetime(df_ret['Trddt'])
    # 放宽过滤条件：保留有成交的记录（Dnshrtrd > 0）
    # 新CSV格式没有Filling列，只过滤有成交的记录
    if 'Filling' in df_ret.columns:
        df_ret = df_ret[(df_ret['Filling'] == 0) & (df_ret['Dnshrtrd'] > 0)].copy()
    else:
        df_ret = df_ret[df_ret['Dnshrtrd'] > 0].copy()
    
    # 筛选缺失日期
    df_ret_new = df_ret[df_ret['Trddt'].dt.date.isin(missing_dates)].copy()
    print(f"日个股回报率新增行数: {len(df_ret_new):,}")
    
    # 转换股票代码
    df_ret_new['symbol'] = df_ret_new['Stkcd'].map(prefix_symbol)
    df_ret_new = df_ret_new[df_ret_new['symbol'].str.match(r'^(SH|SZ|BJ)\d{6}$', na=False)].copy()
    
    # 7. 合并数据（加入Opnprc/Hiprc/Loprc获取精确的未复权OHLC）
    df_merged = df_new.merge(
        df_ret_new[['symbol', 'Trddt', 'Opnprc', 'Hiprc', 'Loprc', 'Clsprc', 'Dretwd', 'Dnshrtrd', 'Dnvaltrd', 'Dsmvtll', 'Dsmvosd']],
        left_on=['symbol', 'TradingDate'],
        right_on=['symbol', 'Trddt'],
        how='inner'
    )
    print(f"合并后行数: {len(df_merged):,}")
    
    if df_merged.empty:
        print("合并后无数据，退出")
        return
    
    # 8. 构建银层格式
    # 后复权因子 = 后复权收盘价 / 原始收盘价
    df_merged['bward_factor'] = df_merged['ClosePrice'] / df_merged['Clsprc']
    df_merged['fward_factor'] = 1.0 / df_merged['bward_factor']
    df_merged['factor'] = df_merged['bward_factor']  # 后复权因子（与银层历史口径一致）
    
    # 后复权价格（与build_silver_market.py口径一致，adj_*列存后复权价格）
    df_merged['adj_open'] = df_merged['OpenPrice']
    df_merged['adj_high'] = df_merged['HighPrice']
    df_merged['adj_low'] = df_merged['LowPrice']
    df_merged['adj_close'] = df_merged['ClosePrice']
    
    # 构建最终DataFrame（包含换手率）
    df_silver_new = pd.DataFrame({
        'symbol': df_merged['symbol'],
        'trade_date': df_merged['TradingDate'],
        'open': df_merged['Opnprc'],   # 直接读取未复权开盘价，避免浮点精度损失
        'high': df_merged['Hiprc'],   # 直接读取未复权最高价
        'low': df_merged['Loprc'],    # 直接读取未复权最低价
        'close': df_merged['Clsprc'],  # 原始收盘价
        'volume': df_merged['Dnshrtrd'],
        'amount': df_merged['Dnvaltrd'],
        'pre_close': df_merged['Clsprc'] / (1 + df_merged['Dretwd']),  # 前收盘价
        'pct_change': df_merged['Dretwd'] * 100,  # 百分比点
        'ret_with_dividend': df_merged['Dretwd'],
        'ret_no_dividend': df_merged['Dretwd'],  # 简化处理
        'market_type': 1,  # 默认A股
        'trade_status': 1,
        'filling': 0,
        'total_mv': df_merged['Dsmvtll'] * 10000,  # CSMAR单位千元，转为元
        'float_mv': df_merged['Dsmvosd'] * 10000,
        'turnover_rate': df_merged['TurnoverRate1'],  # 换手率（从后复权行情）
        'bward_factor': df_merged['bward_factor'],
        'fward_factor': df_merged['fward_factor'],
        'factor': df_merged['factor'],
        'adj_open': df_merged['adj_open'],
        'adj_high': df_merged['adj_high'],
        'adj_low': df_merged['adj_low'],
        'adj_close': df_merged['adj_close'],
    })
    
    print(f"\n新增银层数据:")
    print(f"  行数: {len(df_silver_new):,}")
    print(f"  日期: {sorted(df_silver_new['trade_date'].dt.date.unique())}")
    print(f"  股票数: {df_silver_new['symbol'].nunique()}")
    
    # 9. 合并到现有银层
    df_silver_combined = pd.concat([df_silver, df_silver_new], ignore_index=True)
    df_silver_combined = df_silver_combined.sort_values(['symbol', 'trade_date']).reset_index(drop=True)
    
    # 10. 写回
    df_silver_combined.to_parquet(silver_path, index=False)
    print(f"\n已更新: {silver_path}")
    print(f"总行数: {len(df_silver_combined):,}")
    print(f"日期范围: {df_silver_combined['trade_date'].min()} ~ {df_silver_combined['trade_date'].max()}")

if __name__ == "__main__":
    main()
