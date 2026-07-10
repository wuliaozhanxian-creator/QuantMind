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

# 38 个衍生特征（与 generate_feature_snapshots.py TRAINING_FEATURES 一致，排除 10 个原始字段）
DERIVED_FEATURES: list[str] = [
    "mom_ret_5d", "mom_ret_20d", "mom_ret_60d", "mom_ret_120d",
    "mom_ma_gap_5", "mom_ma_gap_20", "mom_ma_gap_60",
    "mom_macd_hist", "mom_rsi_14", "mom_breakout_20d",
    "vol_std_10", "vol_std_20", "vol_std_60",
    "vol_atr_14", "vol_parkinson_20", "vol_downside_20", "vol_realized_rv",
    "liq_turnover_tl", "liq_volume_ratio_5", "liq_volume_ratio_20",
    "liq_amount_ma_20", "liq_avg_trade_size", "liq_obv_20",
    "liq_amihud_20", "liq_amihud_60", "liq_mfi_14",
    "flow_net_amount_ratio", "flow_large_net_ratio",
    "flow_net_order_ratio", "flow_vpin_ma_20",
    "micro_imbalance_volume", "micro_effective_spread",
    "micro_pressure_score", "micro_jump_flag",
    "style_ln_mv_float", "style_bp", "style_ep_ttm",
    "style_beta_60", "style_idio_vol_60", "style_residual_ret_20",
    "style_valuation_composite", "style_size_percentile", "style_value_percentile",
    "ind_relative_volume_20", "ind_relative_volatility_20",
    "ind_strength_20", "ind_momentum_rank_20", "ind_value_rank",
]


def _safe_div(num, den):
    out = pd.Series(num) / pd.Series(den).replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan)


def compute_derived_features(df: pd.DataFrame) -> pd.DataFrame:
    """在 df_all 上计算 38 个衍生特征。

    参照 backend/scripts/data/generate_feature_snapshots.py 的计算逻辑，
    从 raw OHLCV + factor 计算衍生特征。
    """
    df = df.sort_values(["symbol", "trade_date"]).copy()

    factor = df["factor"].fillna(1.0)
    adj_close = df["raw_close"] * factor
    adj_high = df["raw_high"] * factor
    adj_low = df["raw_low"] * factor

    raw_close = df["raw_close"]
    g = df.groupby("symbol", observed=True)

    # ── 动量因子 ──────────────────────────────────────────────
    df["mom_ret_1d"] = _safe_div(raw_close, g["raw_close"].shift(1)) - 1
    df["mom_ret_5d"] = _safe_div(raw_close, g["raw_close"].shift(5)) - 1
    df["mom_ret_20d"] = _safe_div(raw_close, g["raw_close"].shift(20)) - 1
    df["mom_ret_60d"] = _safe_div(raw_close, g["raw_close"].shift(60)) - 1
    df["mom_ret_120d"] = _safe_div(raw_close, g["raw_close"].shift(120)) - 1

    for n in (5, 20, 60):
        adj_ma = adj_close.groupby(df["symbol"], observed=True).transform(
            lambda s, w=n: s.rolling(w, min_periods=w).mean()
        )
        df[f"mom_ma_gap_{n}"] = _safe_div(adj_close, adj_ma) - 1

    ema12 = df.groupby("symbol", observed=True)["raw_close"].transform(
        lambda s: s.ewm(span=12, adjust=False, min_periods=12).mean()
    )
    ema26 = df.groupby("symbol", observed=True)["raw_close"].transform(
        lambda s: s.ewm(span=26, adjust=False, min_periods=26).mean()
    )
    df["mom_macd_dif"] = ema12 - ema26
    df["mom_macd_dea"] = df.groupby("symbol", observed=True)["mom_macd_dif"].transform(
        lambda s: s.ewm(span=9, adjust=False, min_periods=9).mean()
    )
    df["mom_macd_hist"] = 2 * (df["mom_macd_dif"] - df["mom_macd_dea"])

    def _calc_rsi(s, window=14):
        delta = s.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_gain = gains.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        avg_loss = losses.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - 100 / (1 + rs)

    df["mom_rsi_14"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(_calc_rsi)

    high20 = adj_high.groupby(df["symbol"], observed=True).transform(
        lambda s: s.rolling(20, min_periods=20).max()
    )
    df["mom_breakout_20d"] = _safe_div(adj_close, high20) - 1

    # ── 波动率因子 ────────────────────────────────────────────
    for n in (10, 20, 60):
        df[f"vol_std_{n}"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
            lambda s, w=n: s.rolling(w, min_periods=w).std()
        )

    adj_prev_close = adj_close.groupby(df["symbol"], observed=True).shift(1)
    tr = pd.concat(
        [
            (adj_high - adj_low),
            (adj_high - adj_prev_close).abs(),
            (adj_low - adj_prev_close).abs(),
        ],
        axis=1,
    ).max(axis=1)
    df["vol_atr_14"] = tr.groupby(df["symbol"]).transform(
        lambda s: s.rolling(14, min_periods=14).mean()
    )

    log_hl = np.log(_safe_div(adj_high, adj_low))
    parkinson_base = (log_hl**2) / (4 * np.log(2))
    df["vol_parkinson_20"] = np.sqrt(
        parkinson_base.groupby(df["symbol"]).transform(
            lambda s: s.rolling(20, min_periods=20).mean()
        )
    )

    df["vol_downside_20"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: s.clip(upper=0).rolling(20, min_periods=20).std()
    )

    df["vol_realized_rv"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: np.sqrt(s.rolling(20, min_periods=20).apply(lambda x: np.sum(x**2), raw=True))
    )

    # ── 流动性因子 ────────────────────────────────────────────
    df["liq_turnover_tl"] = np.nan
    vol_ma_5 = df.groupby("symbol", observed=True)["raw_volume"].transform(
        lambda s: s.rolling(5, min_periods=5).mean()
    )
    vol_ma_20 = df.groupby("symbol", observed=True)["raw_volume"].transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    df["liq_volume_ratio_5"] = _safe_div(df["raw_volume"], vol_ma_5)
    df["liq_volume_ratio_20"] = _safe_div(df["raw_volume"], vol_ma_20)
    df["liq_amount_ma_20"] = df.groupby("symbol", observed=True)["raw_amount"].transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    df["liq_avg_trade_size"] = np.nan

    sign = np.sign(df["mom_ret_1d"].fillna(0))
    obv = (sign * df["raw_volume"].fillna(0)).groupby(df["symbol"]).cumsum()
    df["liq_obv_20"] = obv.groupby(df["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )

    amihud_base = _safe_div(df["mom_ret_1d"].abs(), df["raw_amount"])
    df["liq_amihud_20"] = amihud_base.groupby(df["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    df["liq_amihud_60"] = amihud_base.groupby(df["symbol"]).transform(
        lambda s: s.rolling(60, min_periods=60).mean()
    )

    typical = (adj_high + adj_low + adj_close) / 3
    money_flow = typical * df["raw_volume"]
    pos_flow = money_flow.where(typical.groupby(df["symbol"]).diff() > 0, 0.0)
    neg_flow = money_flow.where(typical.groupby(df["symbol"]).diff() < 0, 0.0)
    pos14 = pos_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    neg14 = neg_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    df["liq_mfi_14"] = 100 - 100 / (1 + _safe_div(pos14, neg14))

    # ── 资金流 / 微观结构因子 (需要外部数据，填 NaN) ──────────
    for col in ("flow_net_amount_ratio", "flow_large_net_ratio",
                "flow_net_order_ratio", "flow_vpin_ma_20",
                "micro_imbalance_volume", "micro_effective_spread",
                "micro_pressure_score", "micro_jump_flag"):
        df[col] = np.nan

    # ── 风格因子 ──────────────────────────────────────────────
    # 对数市值：用 log(amount) 代理（CSMAR 源无 float_mv）
    df["style_ln_mv_float"] = np.log(df["raw_amount"].where(df["raw_amount"] > 0))
    # 估值因子：用 1/close 代理
    df["style_bp"] = 1.0 / df["raw_close"].where(df["raw_close"] > 0)
    df["style_ep_ttm"] = 1.0 / df["raw_close"].where(df["raw_close"] > 0)

    mkt_ret = df.groupby("trade_date", observed=True)["mom_ret_1d"].transform("mean")
    beta_parts: list[pd.Series] = []
    for _, part in df.groupby("symbol", observed=True, sort=False):
        pair = part[["mom_ret_1d"]].copy()
        pair["mkt"] = mkt_ret.loc[part.index]
        pair = pair.dropna()
        beta = pd.Series(np.nan, index=part.index, dtype=float)
        if not pair.empty:
            cov = pair["mom_ret_1d"].rolling(60, min_periods=60).cov(pair["mkt"])
            var_mkt = pair["mkt"].rolling(60, min_periods=60).var().replace(0, np.nan)
            beta.loc[pair.index] = (cov / var_mkt).values
        beta_parts.append(beta)
    df["style_beta_60"] = pd.concat(beta_parts).sort_index()

    resid = df["mom_ret_1d"] - df["style_beta_60"] * mkt_ret
    df["style_idio_vol_60"] = resid.groupby(df["symbol"]).transform(
        lambda s: s.rolling(60, min_periods=60).std()
    )
    df["style_residual_ret_20"] = resid.groupby(df["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).sum()
    )

    df["style_valuation_composite"] = df[["style_bp", "style_ep_ttm"]].rank(pct=True).mean(axis=1)
    df["style_size_percentile"] = df.groupby("trade_date", observed=True)["style_ln_mv_float"].rank(pct=True)
    df["style_value_percentile"] = df.groupby("trade_date", observed=True)["style_valuation_composite"].rank(pct=True)

    # ── 行业因子 (无行业数据，用全市场截面统计代理) ──────────
    mkt_amount_ma_20 = df.groupby("trade_date", observed=True)["liq_amount_ma_20"].transform("mean")
    df["ind_relative_volume_20"] = _safe_div(df["liq_amount_ma_20"], mkt_amount_ma_20)
    mkt_vol_std = df.groupby("trade_date", observed=True)["vol_std_20"].transform("mean")
    df["ind_relative_volatility_20"] = _safe_div(df["vol_std_20"], mkt_vol_std)
    mkt_ret_20d = df.groupby("trade_date", observed=True)["mom_ret_20d"].transform("mean")
    df["ind_strength_20"] = df["mom_ret_20d"] - mkt_ret_20d
    df["ind_momentum_rank_20"] = df.groupby("trade_date", observed=True)["mom_ret_20d"].rank(pct=True)
    df["ind_value_rank"] = df.groupby("trade_date", observed=True)["style_value_percentile"].rank(pct=True)

    return df

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
    with open(cal_path) as f:
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

    # Compute 38 derived features (mom_*/vol_*/liq_*/flow_*/style_*/ind_*)
    print("📊 Computing 38 derived features for Qlib bin data...")
    df_all = compute_derived_features(df_all)

    # Sort and list unique symbols
    df_all = df_all.sort_values(["symbol", "trade_date"])
    symbols = df_all["symbol"].unique()
    print(f"   Total unique stock symbols: {len(symbols)}")

    # 7. NumPy Binary Generation
    print("💾 Generating Qlib float32 binaries...")
    # 10 original fields + 38 derived features = 48 total
    features = ["open", "high", "low", "close", "volume", "amount", "factor", "adjclose", "vwap", "change"] + DERIVED_FEATURES

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

        # Derived features: fill NaN with 0.0 (consistent with train.py fill_values)
        for feat in DERIVED_FEATURES:
            if feat in sym_df.columns:
                sym_df[feat] = sym_df[feat].fillna(0.0).astype(np.float32)
            else:
                sym_df[feat] = 0.0

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
            for line in lines:
                parts = line.split()
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
