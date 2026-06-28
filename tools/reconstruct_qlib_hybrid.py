#!/usr/bin/env python3
"""
QuantMind Qlib 2016-2026 Hybrid Reconstruction Script
Author: Antigravity

This script reconstructs the complete 2016-2026 Qlib binary database:
1. Loads 2016-2025 history from DuckDB (which is complete and correct).
2. Loads 2026 unfrozen price/volume data from Parquet.
3. Performs a pandas ASOF join with DuckDB sparse factor data to align 
   post-adjustment factors for 2026, and calculates the exact raw prices.
4. Generates the Qlib float32 binary database for the full 2016-2026 period.
"""

import os
import sys
import shutil
import duckdb
import numpy as np
import pandas as pd
from pathlib import Path
from tqdm import tqdm

PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from backend.shared.stock_utils import StockCodeUtil

def reconstruct_qlib_hybrid():
    duckdb_path = Path("/Volumes/M2SSD/data/csmar.duckdb")
    if not duckdb_path.exists():
        duckdb_path = PROJECT_ROOT / "M2SSD" / "data" / "csmar.duckdb"
        
    parquet_path = PROJECT_ROOT / "db" / "custom" / "fundamental_aligned.parquet"
    qlib_dir = PROJECT_ROOT / "db" / "qlib_data"
    features_dir = qlib_dir / "features"
    temp_features_dir = qlib_dir / "features_temp"
    cal_path = qlib_dir / "calendars" / "day.txt"
    
    print(f"🔍 Database path: {duckdb_path}")
    print(f"🔍 Parquet path: {parquet_path}")
    print(f"📂 Qlib target dir: {qlib_dir}")
    
    if not duckdb_path.exists():
        print(f"❌ Error: DuckDB not found at {duckdb_path}!")
        return
        
    if not parquet_path.exists():
        print(f"❌ Error: Parquet file not found at {parquet_path}!")
        return
        
    if not cal_path.exists():
        print(f"❌ Error: Qlib calendars/day.txt not found at {cal_path}!")
        return

    # 1. Load Calendar
    print("📅 Loading global calendar...")
    with open(cal_path, "r") as f:
        calendar = [line.strip() for line in f if line.strip()]
    
    calendar_idx = {date: idx for idx, date in enumerate(calendar)}
    print(f"   Loaded {len(calendar)} trading days. Range: {calendar[0]} ~ {calendar[-1]}")
    
    # 2. Setup Atomic Directory Structure
    if temp_features_dir.exists():
        shutil.rmtree(temp_features_dir)
    temp_features_dir.mkdir(parents=True, exist_ok=True)
    
    # 3. Preserve Existing Index Binaries
    print("🏛️ Preserving existing index binaries...")
    indices = ["idx_sh000300", "idx_sh000852", "idx_sh000905"]
    for idx in indices:
        src_idx_dir = features_dir / idx
        dst_idx_dir = temp_features_dir / idx
        if src_idx_dir.exists():
            print(f"   Copying {idx} index directory intact...")
            shutil.copytree(src_idx_dir, dst_idx_dir)
        else:
            print(f"   ⚠️ Warning: Index directory {src_idx_dir} does not exist!")

    # 4. Load raw return CSV (日个股回报率文件_合并.csv)
    csmar_dir = PROJECT_ROOT / "M2SSD" / "CSMAR"
    raw_close_csv = csmar_dir / "日个股回报率文件" / "日个股回报率文件_合并.csv"
    factor_csv = csmar_dir / "股票价格复权因子表(日)" / "股票价格复权因子表(日)_合并.csv"
    
    # Verify file paths
    for f in [raw_close_csv, factor_csv]:
        if not f.exists():
            print(f"❌ Error: File not found at {f}")
            return
            
    print(f"📖 Reading raw prices from '{raw_close_csv.name}'...")
    # Load raw data for 2016-01-01 to 2026-05-15
    df_raw = pd.read_csv(
        raw_close_csv,
        usecols=['Stkcd', 'Trddt', 'Opnprc', 'Hiprc', 'Loprc', 'Clsprc', 'Dnshrtrd', 'Dnvaltrd'],
        dtype={'Stkcd': str}
    )
    df_raw['trade_date'] = pd.to_datetime(df_raw['Trddt']).dt.strftime('%Y-%m-%d')
    # Filter 2016 onwards strictly up to 2026-05-15
    df_raw = df_raw[(df_raw['trade_date'] >= '2016-01-01') & (df_raw['trade_date'] <= '2026-05-15')].copy()
    
    # Rename columns to match Qlib convention
    df_raw = df_raw.rename(columns={
        'Opnprc': 'raw_open',
        'Hiprc': 'raw_high',
        'Loprc': 'raw_low',
        'Clsprc': 'raw_close',
        'Dnshrtrd': 'raw_volume',
        'Dnvaltrd': 'raw_amount'
    })
    
    # Standardise symbols using StockCodeUtil
    print("🧹 Standardising symbol formats...")
    df_raw['symbol'] = df_raw['Stkcd'].str.zfill(6).map(StockCodeUtil.to_prefix)
    
    # Filter BJ stocks, keep only SH/SZ
    df_raw = df_raw[df_raw['symbol'].str.startswith(('SH', 'SZ'))].copy()
    
    # Drop intermediate columns
    df_raw = df_raw[['symbol', 'trade_date', 'raw_open', 'raw_high', 'raw_low', 'raw_close', 'raw_volume', 'raw_amount']]
    print(f"   Loaded {len(df_raw)} raw stock records.")
    
    # 5. Load factors
    print(f"📖 Loading cumulative factors from '{factor_csv.name}'...")
    df_fac = pd.read_csv(factor_csv, dtype={'Symbol': str})
    df_fac['symbol'] = df_fac['Symbol'].str.zfill(6).map(StockCodeUtil.to_prefix)
    df_fac = df_fac[df_fac['symbol'].str.startswith(('SH', 'SZ'))].copy()
    df_fac['TradingDatetime'] = pd.to_datetime(df_fac['TradingDate']).astype('datetime64[ns]')
    df_fac = df_fac.sort_values('TradingDatetime')
    
    # Convert dates to datetime objects for pandas merge_asof
    df_raw['trade_datetime'] = pd.to_datetime(df_raw['trade_date']).astype('datetime64[ns]')
    
    # Sort both for merge_asof
    df_raw = df_raw.sort_values('trade_datetime')
    
    # 6. ASOF join factors to raw data
    print("🧩 Aligning cumulative backward factors using pandas ASOF merge...")
    df_all = pd.merge_asof(
        df_raw,
        df_fac[['TradingDatetime', 'symbol', 'CumulateBwardFactor']],
        by='symbol',
        left_on='trade_datetime',
        right_on='TradingDatetime',
        direction='backward'
    )
    
    # Fill factors and compute mathematically correct prices
    print("🧮 Calculating raw unadjusted prices and backward-adjusted close...")
    df_all['factor'] = df_all['CumulateBwardFactor'].fillna(1.0)
    
    df_all['open'] = df_all['raw_open']
    df_all['high'] = df_all['raw_high']
    df_all['low'] = df_all['raw_low']
    df_all['close'] = df_all['raw_close']
    df_all['adj_close'] = df_all['raw_close'] * df_all['factor']
    
    # Drop duplicates by (symbol, trade_date)
    print("🧹 Removing duplicate symbol-date records...")
    df_all = df_all.drop_duplicates(subset=["symbol", "trade_date"]).copy()
    
    # Sort and list unique symbols
    df_all = df_all.sort_values(["symbol", "trade_date"])
    symbols = df_all["symbol"].unique()
    print(f"   Total unique stock symbols: {len(symbols)}")
    
    # 7. NumPy Binary Generation
    print("💾 Generating Qlib float32 binaries...")
    features = ["open", "high", "low", "close", "volume", "amount", "factor", "adjclose", "vwap", "change"]
    
    for symbol, sym_df in tqdm(df_all.groupby("symbol"), desc="Processing stocks"):
        start_date = sym_df["trade_date"].min()
        if start_date not in calendar_idx:
            continue
            
        start_idx = calendar_idx[start_date]
        sym_calendar = calendar[start_idx:]
        
        sym_df = sym_df.set_index("trade_date").reindex(sym_calendar)
        
        # Calculate VWAP before filling NaNs
        sym_df["vwap"] = sym_df["raw_amount"] / sym_df["raw_volume"]
        
        # Populate Qlib columns
        sym_df["open"] = sym_df["raw_open"]
        sym_df["high"] = sym_df["raw_high"]
        sym_df["low"] = sym_df["raw_low"]
        sym_df["close"] = sym_df["raw_close"]
        sym_df["volume"] = sym_df["raw_volume"] / 100.0
        sym_df["amount"] = sym_df["raw_amount"] / 1000.0
        sym_df["adjclose"] = sym_df["adj_close"]
        
        # Forward fill price/factor indicators
        price_cols = ["open", "high", "low", "close", "factor", "adjclose", "vwap"]
        sym_df[price_cols] = sym_df[price_cols].ffill()
        
        sym_df["factor"] = sym_df["factor"].fillna(1.0)
        sym_df["open"] = sym_df["open"].fillna(sym_df["close"])
        sym_df["high"] = sym_df["high"].fillna(sym_df["close"])
        sym_df["low"] = sym_df["low"].fillna(sym_df["close"])
        sym_df["adjclose"] = sym_df["adjclose"].fillna(sym_df["close"] * sym_df["factor"])
        sym_df["vwap"] = sym_df["vwap"].fillna(sym_df["close"])
        
        sym_df["change"] = sym_df["adjclose"].pct_change().fillna(0.0)
        sym_df["volume"] = sym_df["volume"].fillna(0.0)
        sym_df["amount"] = sym_df["amount"].fillna(0.0)
        
        # Create output directories
        symbol_dir = temp_features_dir / symbol.lower()
        symbol_dir.mkdir(parents=True, exist_ok=True)
        
        # Write Float32 binary files
        for feat in features:
            bin_path = symbol_dir / f"{feat}.day.bin"
            vals = sym_df[feat].values
            
            # Format: [start_index, val_1, val_2, ...]
            bin_data = np.concatenate(([np.float32(start_idx)], vals.astype(np.float32)))
            
            with open(bin_path, "wb") as f:
                f.write(bin_data.tobytes())
                
    # 8. Atomic Swapping of Features Directory
    print("🔄 Atomically replacing features directory...")
    old_features_backup = qlib_dir / "features_old"
    if old_features_backup.exists():
        shutil.rmtree(old_features_backup)
        
    if features_dir.exists():
        features_dir.rename(old_features_backup)
        
    temp_features_dir.rename(features_dir)
    
    if old_features_backup.exists():
        shutil.rmtree(old_features_backup)
        
    # 9. Re-writing all.txt instruments file
    print("📝 Rebuilding db/qlib_data/instruments/all.txt...")
    inst_dir = qlib_dir / "instruments"
    inst_dir.mkdir(parents=True, exist_ok=True)
    
    all_txt_path = inst_dir / "all.txt"
    instrument_lines = []
    
    # 1) Add Indices (starting from 2016-01-04)
    for idx in indices:
        instrument_lines.append(f"{idx.upper()}\t{calendar[0]}\t{calendar[-1]}")
        
    # 2) Add Stocks
    min_dates = df_all.groupby("symbol")["trade_date"].min().to_dict()
    for symbol in sorted(symbols):
        start_date = min_dates.get(symbol, "2016-01-04")
        instrument_lines.append(f"{symbol}\t{start_date}\t{calendar[-1]}")
        
    with open(all_txt_path, "w") as f:
        for line in instrument_lines:
            f.write(line + "\n")
            
    # 10. Clean up and update other instrument files
    print("📝 Rebuilding other indices instrument lists (CSI300, 500, 1000)...")
    for txt_file in inst_dir.glob("csi*.txt"):
        try:
            lines = txt_file.read_text().splitlines()
            new_lines = []
            for l in lines:
                parts = l.split()
                if not parts:
                    continue
                sym = parts[0].upper()
                if sym.startswith(("SH", "SZ")):
                    start_d = parts[1] if len(parts) >= 2 else "2016-01-04"
                    new_lines.append(f"{sym}\t{start_d}\t{calendar[-1]}")
            txt_file.write_text("\n".join(new_lines) + "\n")
        except Exception as e:
            print(f"   ⚠️ Error rewriting index constituent file {txt_file.name}: {e}")
            
    print(f"✨ SUCCESS: Hybrid 2016-2026 Qlib dataset reconstructed completely at {qlib_dir}!")

if __name__ == "__main__":
    reconstruct_qlib_hybrid()
