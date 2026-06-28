#!/usr/bin/env python3
"""QuantMind L1 特征增量计算（适配新数据目录 db/csmar/）。

镜像脚本：基于 compute_l1_features_incremental.py，但适配新的数据目录结构：
- CSMAR 数据源：db/csmar/（而非 M2SSD/CSMAR/）
- 银层目录：db/market_silver_v2/
- 特征输出：db/feature_snapshots/

用法：
    python scripts/data/processing/compute_l1_incremental_new.py --date 2026-06-08
    python scripts/data/processing/compute_l1_incremental_new.py --date 2026-06-08 --write-audit
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# 新数据目录配置
DEFAULT_CSMAR_ROOT = PROJECT_ROOT / "db" / "csmar"
DEFAULT_SILVER_DIR = PROJECT_ROOT / "db" / "market_silver_v2"
DEFAULT_CATALOG = PROJECT_ROOT / "config" / "features" / "model_training_feature_catalog_v1.json"
DEFAULT_OUTPUT_DIR = PROJECT_ROOT / "db" / "feature_snapshots"


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Compute L1 features with new data directory (db/csmar/)")
    parser.add_argument("--date", required=True, help="目标日期 YYYY-MM-DD")
    parser.add_argument("--csmar-root", type=Path, default=DEFAULT_CSMAR_ROOT)
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument(
        "--output",
        type=Path,
        default=None,
        help="L1 切片输出路径（默认 db/feature_snapshots/daily_l1_new_YYYYMMDD.parquet）",
    )
    parser.add_argument(
        "--write-audit",
        action="store_true",
        help="是否输出审计 JSON",
    )
    parser.add_argument(
        "--merge-into-yearly",
        action="store_true",
        help="是否把当日 L1 结果回写到 model_features_YYYY.parquet",
    )
    return parser.parse_args()


def feature_keys(catalog_path: Path) -> list[str]:
    """从特征目录文件读取所有启用的特征键。"""
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    keys: list[str] = []
    for category in catalog["categories"]:
        for feature in category["features"]:
            if feature.get("enabled", True):
                keys.append(feature["key"])
    return keys


def normalize_symbol_column(df: pd.DataFrame, source_col: str, target_col: str = "symbol") -> pd.DataFrame:
    """标准化股票代码为 Prefix 格式（SH600000）。"""
    from backend.shared.stock_utils import StockCodeUtil
    
    def prefix_symbol(value: object) -> str:
        if pd.isna(value):
            return ""
        text = str(value).strip()
        if text.endswith(".0"):
            text = text[:-2]
        if text.isdigit():
            text = text.zfill(6)
        return StockCodeUtil.to_prefix(text)
    
    df[target_col] = df[source_col].map(prefix_symbol)
    return df[df[target_col].str.match(r"^(SH|SZ|BJ)\d{6}$", na=False)].copy()


def coerce_date(df: pd.DataFrame, source_col: str, target_col: str = "trade_date") -> pd.DataFrame:
    """转换日期列。"""
    df[target_col] = pd.to_datetime(df[source_col], errors="coerce")
    return df[df[target_col].notna()].copy()


def load_base_with_lookback(silver_dir: Path, year: int) -> pd.DataFrame:
    """加载银层数据，包含前一年作为预热窗口。"""
    base_path = silver_dir / f"market_base_{year}.parquet"
    if not base_path.exists():
        raise FileNotFoundError(f"缺少银层文件: {base_path}")

    frames: list[pd.DataFrame] = []
    prev_path = silver_dir / f"market_base_{year - 1}.parquet"
    if prev_path.exists():
        frames.append(pd.read_parquet(prev_path))
    frames.append(pd.read_parquet(base_path))

    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    return df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)


def safe_div(a: pd.Series | float, b: pd.Series | float) -> pd.Series | float:
    """安全除法，除数为0时返回NaN。"""
    if isinstance(a, pd.Series) or isinstance(b, pd.Series):
        a = pd.Series(a) if not isinstance(a, pd.Series) else a
        b = pd.Series(b) if not isinstance(b, pd.Series) else b
        return a / b.replace(0, np.nan)
    return a / b if b != 0 else np.nan


def _rolling_group(df: pd.DataFrame, col: str, window: int, fn: str) -> pd.Series:
    """按股票分组滚动计算。"""
    grouped = df.groupby("symbol", observed=True)[col]
    if fn == "mean":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).mean())
    if fn == "std":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).std())
    if fn == "max":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).max())
    if fn == "sum":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).sum())
    raise ValueError(fn)


def _ema(df: pd.DataFrame, col: str, span: int) -> pd.Series:
    """按股票分组计算EMA。"""
    return df.groupby("symbol", observed=True)[col].transform(
        lambda s: s.ewm(span=span, adjust=False, min_periods=span).mean()
    )


def _rsi(df: pd.DataFrame, ret_col: str, window: int) -> pd.Series:
    """计算RSI指标。"""
    def calc(s: pd.Series) -> pd.Series:
        delta = s
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_gain = gains.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        avg_loss = losses.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - 100 / (1 + rs)

    return df.groupby("symbol", observed=True)[ret_col].transform(calc)


def add_daily_technical_features(df: pd.DataFrame) -> pd.DataFrame:
    """计算技术指标特征。"""
    df = df.sort_values(["symbol", "trade_date"]).copy()
    g = df.groupby("symbol", observed=True)
    adj_close = df["adj_close"]
    adj_high = df["adj_high"]
    adj_low = df["adj_low"]
    adj_open = df["adj_open"]
    raw_close = df["close"]
    raw_high = df["high"]
    raw_low = df["low"]
    raw_open = df["open"]
    prev_close = g["adj_close"].shift(1)
    raw_prev_close = g["close"].shift(1)

    # 动量收益率（使用未复权价格）
    for n in (1, 3, 5, 10, 20, 60, 120):
        df[f"mom_ret_{n}d"] = safe_div(raw_close, g["close"].shift(n)) - 1
    
    for n in (5, 10, 20, 60, 120):
        ma = _rolling_group(df, "adj_close", n, "mean")
        df[f"mom_ma_gap_{n}"] = safe_div(adj_close, ma) - 1

    ema12 = _ema(df, "adj_close", 12)
    ema26 = _ema(df, "adj_close", 26)
    df["mom_ema_gap_12"] = safe_div(adj_close, ema12) - 1
    df["mom_ema_gap_26"] = safe_div(adj_close, ema26) - 1
    df["mom_macd_dif"] = ema12 - ema26
    df["mom_macd_dea"] = df.groupby("symbol", observed=True)["mom_macd_dif"].transform(
        lambda s: s.ewm(span=9, adjust=False, min_periods=9).mean()
    )
    df["mom_macd_hist"] = 2 * (df["mom_macd_dif"] - df["mom_macd_dea"])
    df["mom_rsi_6"] = _rsi(df, "mom_ret_1d", 6)
    df["mom_rsi_14"] = _rsi(df, "mom_ret_1d", 14)

    # KDJ
    low9 = df.groupby("symbol", observed=True)["adj_low"].transform(lambda s: s.rolling(9, min_periods=9).min())
    high9 = df.groupby("symbol", observed=True)["adj_high"].transform(lambda s: s.rolling(9, min_periods=9).max())
    rsv = safe_div(adj_close - low9, high9 - low9) * 100
    df["mom_kdj_k"] = rsv.groupby(df["symbol"]).transform(lambda s: s.ewm(alpha=1 / 3, adjust=False).mean())
    df["mom_kdj_d"] = df.groupby("symbol", observed=True)["mom_kdj_k"].transform(
        lambda s: s.ewm(alpha=1 / 3, adjust=False).mean()
    )
    df["mom_kdj_j"] = 3 * df["mom_kdj_k"] - 2 * df["mom_kdj_d"]

    high20 = df.groupby("symbol", observed=True)["adj_high"].transform(lambda s: s.rolling(20, min_periods=20).max())
    df["mom_breakout_20d"] = safe_div(adj_close, high20) - 1

    # 波动率
    tr = pd.concat(
        [(adj_high - adj_low), (adj_high - prev_close).abs(), (adj_low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    df["vol_true_range"] = tr
    for n in (14, 20):
        df[f"vol_atr_{n}"] = df.groupby("symbol", observed=True)["vol_true_range"].transform(
            lambda s: s.rolling(n, min_periods=n).mean()
        )
    
    for n in (5, 10, 20, 60):
        df[f"vol_std_{n}"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
            lambda s: s.rolling(n, min_periods=n).std()
        )

    # Parkinson/GK/RS 波动率
    log_hl = np.log(safe_div(adj_high, adj_low))
    parkinson_base = (log_hl**2) / (4 * np.log(2))
    log_co = np.log(safe_div(adj_close, adj_open))
    log_ho = np.log(safe_div(adj_high, adj_open))
    log_lo = np.log(safe_div(adj_low, adj_open))
    gk_base = 0.5 * log_hl**2 - (2 * np.log(2) - 1) * log_co**2
    rs_base = np.log(safe_div(adj_high, adj_close)) * log_ho + np.log(safe_div(adj_low, adj_close)) * log_lo
    
    for n in (10, 20):
        df[f"vol_parkinson_{n}"] = np.sqrt(_rolling_group(df.assign(_parkinson=parkinson_base), "_parkinson", n, "mean"))
        df[f"vol_gk_{n}"] = np.sqrt(_rolling_group(df.assign(_gk=gk_base.clip(lower=0)), "_gk", n, "mean"))
        df[f"vol_rs_{n}"] = np.sqrt(_rolling_group(df.assign(_rs=rs_base.clip(lower=0)), "_rs", n, "mean"))
    
    df["vol_downside_20"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: s.clip(upper=0).rolling(20, min_periods=20).std()
    )
    df["vol_upside_20"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: s.clip(lower=0).rolling(20, min_periods=20).std()
    )

    # 流动性指标
    df["liq_volume"] = df["volume"]
    df["liq_amount"] = df["amount"]
    for n in (5, 10, 20):
        df[f"liq_volume_ma_{n}"] = _rolling_group(df, "volume", n, "mean")
        df[f"liq_amount_ma_{n}"] = _rolling_group(df, "amount", n, "mean")
    for n in (5, 20):
        df[f"liq_volume_ratio_{n}"] = safe_div(df["volume"], df[f"liq_volume_ma_{n}"])
        df[f"liq_amount_ratio_{n}"] = safe_div(df["amount"], df[f"liq_amount_ma_{n}"])

    # OBV
    sign = np.sign(df["mom_ret_1d"].fillna(0))
    df["_obv"] = (sign * df["volume"].fillna(0)).groupby(df["symbol"]).cumsum()
    df["liq_obv_20"] = df.groupby("symbol", observed=True)["_obv"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["liq_obv_60"] = df.groupby("symbol", observed=True)["_obv"].transform(lambda s: s.rolling(60, min_periods=60).mean())

    # MFI
    typical = (adj_high + adj_low + adj_close) / 3
    money_flow = typical * df["volume"]
    pos_flow = money_flow.where(typical.groupby(df["symbol"]).diff() > 0, 0.0)
    neg_flow = money_flow.where(typical.groupby(df["symbol"]).diff() < 0, 0.0)
    pos14 = pos_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    neg14 = neg_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    df["liq_mfi_14"] = 100 - 100 / (1 + safe_div(pos14, neg14))

    # Amihud 非流动性
    amihud_base = df["mom_ret_1d"].abs() / df["amount"].replace(0, np.nan)
    df["_amihud_base"] = amihud_base
    for n in (20, 60):
        df[f"liq_amihud_{n}"] = df.groupby("symbol", observed=True)["_amihud_base"].transform(
            lambda s: s.rolling(n, min_periods=n).mean()
        )
    
    # 换手率（从银层继承）
    if "turnover_rate" in df.columns:
        df["liq_turnover_os"] = df["turnover_rate"]  # 当日换手率
        df["liq_turnover_tl"] = df.groupby("symbol", observed=True)["turnover_rate"].transform(
            lambda s: s.rolling(20, min_periods=1).mean()
        )  # 20日平均换手率

    # 清理临时列
    return df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")


def merge_three_factor(csmar_root: Path, df: pd.DataFrame, year: int) -> pd.DataFrame:
    """合并三因子模型指标（SMB、HML、MarketPremium）。
    
    三因子数据使用前一交易日的值（T-1日因子用于T日特征）。
    对于三因子表中缺失的日期，使用前向填充继承最近的因子值。
    """
    try:
        # 查找三因子表（排除zip文件）
        factor_dirs = [d for d in csmar_root.glob("三因子模型指标(日)*") if d.is_dir()]
        if not factor_dirs:
            return df
        
        factor_dir = factor_dirs[0]
        csv_files = list(factor_dir.glob("*.csv"))
        if not csv_files:
            return df
        
        factor_df = pd.read_csv(csv_files[0], encoding="utf-8-sig")
        factor_df["TradingDate"] = pd.to_datetime(factor_df["TradingDate"], errors="coerce")
        
        # 按日期聚合（取均值）
        factor_df = factor_df.groupby("TradingDate", as_index=False).agg({
            "RiskPremium1": "mean",
            "SMB1": "mean", 
            "HML1": "mean"
        })
        
        factor_df = factor_df.rename(columns={
            "TradingDate": "trade_date",
            "RiskPremium1": "style_mkt_premium",
            "SMB1": "style_smb",
            "HML1": "style_hml"
        })
        
        # 获取df中所有交易日，与三因子表对齐
        df_dates = df["trade_date"].unique()
        factor_dates = factor_df["trade_date"].unique()
        
        # 创建完整日期索引并前向填充
        all_dates = pd.DataFrame({"trade_date": sorted(set(df_dates) | set(factor_dates))})
        all_dates["trade_date"] = pd.to_datetime(all_dates["trade_date"])
        
        # 合并三因子到完整日期表
        all_dates = all_dates.merge(factor_df, on="trade_date", how="left")
        
        # 前向填充缺失值（当日使用最近可用的因子值）
        all_dates[["style_mkt_premium", "style_smb", "style_hml"]] = all_dates[
            ["style_mkt_premium", "style_smb", "style_hml"]
        ].ffill()
        
        # 三因子值滞后一天（T日特征使用T-1日因子）
        all_dates[["style_mkt_premium", "style_smb", "style_hml"]] = all_dates[
            ["style_mkt_premium", "style_smb", "style_hml"]
        ].shift(1)
        
        # 再次前向填充第一个交易日的缺失（用第一行可用值）
        all_dates[["style_mkt_premium", "style_smb", "style_hml"]] = all_dates[
            ["style_mkt_premium", "style_smb", "style_hml"]
        ].ffill()
        
        # 合并到df
        df = df.merge(all_dates, on="trade_date", how="left")
    except Exception:
        # 如果读取失败，保留NaN列
        for col in ("style_smb", "style_hml", "style_mkt_premium"):
            if col not in df.columns:
                df[col] = np.nan
    
    return df


def inherit_from_yearly(df: pd.DataFrame, yearly_path: Path, target_date: pd.Timestamp, 
                        columns: list[str]) -> pd.DataFrame:
    """从年度宽表继承指定字段。
    
    优先从目标日期继承；若目标日期无数据或指定列为空，
    则从最近一个有数据的交易日继承（行业分类、Beta等短期内不变）。
    """
    if not yearly_path.exists():
        return df
    
    try:
        yearly = pd.read_parquet(yearly_path)
        yearly["trade_date"] = pd.to_datetime(yearly["trade_date"])
        
        # 只保留需要的列
        inherit_cols = ["symbol", "trade_date"] + [c for c in columns if c in yearly.columns]
        
        # 获取目标日期的数据
        day_data = yearly[yearly["trade_date"] == target_date][inherit_cols].copy()
        
        # 若目标日期无数据，或指定继承列全部为空，从前一交易日继承
        need_fallback = day_data.empty
        if not day_data.empty:
            for col in columns:
                if col in day_data.columns and day_data[col].notna().any():
                    need_fallback = False
                    break
            else:
                # 所有继承列都为空，需要fallback
                need_fallback = True
        
        if need_fallback:
            available_dates = sorted(yearly["trade_date"].unique())
            past_dates = [d for d in available_dates if d < target_date]
            if past_dates:
                prev_date = past_dates[-1]
                day_data = yearly[yearly["trade_date"] == prev_date][inherit_cols].copy()
                # 继承时将日期替换为目标日期
                day_data["trade_date"] = target_date
        
        if day_data.empty:
            return df
        
        # 合并到df
        df = df.merge(day_data, on=["symbol", "trade_date"], how="left", suffixes=("", "_inherit"))
        
        # 用继承值填充
        for col in columns:
            if f"{col}_inherit" in df.columns:
                df[col] = df[col].combine_first(df[f"{col}_inherit"])
                df = df.drop(columns=[f"{col}_inherit"])
    except Exception:
        pass
    
    return df


def merge_style(df: pd.DataFrame) -> pd.DataFrame:
    """计算风格因子。"""
    df["style_ln_mv_total"] = np.log(pd.to_numeric(df.get("total_mv"), errors="coerce").where(lambda s: s > 0))
    df["style_ln_mv_float"] = np.log(pd.to_numeric(df.get("float_mv"), errors="coerce").where(lambda s: s > 0))
    df["style_size_percentile"] = df.groupby("trade_date", observed=True)["style_ln_mv_total"].rank(pct=True)
    
    # 占位：估值因子需要ASOF合并，暂置NaN
    for col in ("style_bp", "style_ep_ttm", "style_valuation_composite", "style_value_percentile"):
        if col not in df.columns:
            df[col] = np.nan
    
    return df


def select_local_l1_columns(df: pd.DataFrame, keys: list[str]) -> list[str]:
    """筛选本地可计算的L1特征列。"""
    local_keys: list[str] = []
    for k in keys:
        # 排除L2维度
        if k.startswith("flow_"):
            continue
        if k.startswith("micro_"):
            continue
        if k.startswith("vol_realized_"):
            continue
        if k.startswith("vol_jump_"):
            continue
        if k == "micro_jump_flag":
            continue
        local_keys.append(k)

    # 只取当前表中存在的列
    local_keys = [k for k in local_keys if k in df.columns]
    return local_keys


def write_audit(output_path: Path, payload: dict) -> None:
    """输出审计JSON。"""
    audit_path = output_path.with_suffix(".audit.json")
    audit_path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")


def main() -> None:
    args = parse_args()
    target_date = pd.to_datetime(args.date, errors="coerce")
    if pd.isna(target_date):
        raise ValueError(f"无效日期: {args.date}")
    target_date = target_date.normalize()
    year = int(target_date.year)
    ymd = target_date.strftime("%Y%m%d")

    # 默认输出路径
    if args.output is None:
        args.output = DEFAULT_OUTPUT_DIR / f"daily_l1_new_{ymd}.parquet"

    keys = feature_keys(args.catalog)

    # 加载银层数据
    df = load_base_with_lookback(args.silver_dir, year)
    
    # 计算技术指标
    df = add_daily_technical_features(df)
    
    # 计算风格因子
    df = merge_style(df)
    
    # 合并三因子模型指标
    df = merge_three_factor(args.csmar_root, df, year)
    
    # 从年度宽表继承行业分类和Beta
    yearly_path = DEFAULT_OUTPUT_DIR / f"model_features_{year}.parquet"
    inherit_cols = [
        "ind_code_l1", "ind_code_l2",
        "style_beta_20", "style_beta_60", "style_beta_120",
        "style_idio_vol_20", "style_idio_vol_60", "style_residual_ret_20"
    ]
    df_target = df[df["trade_date"] == target_date].copy()
    df_target = inherit_from_yearly(df_target, yearly_path, target_date, inherit_cols)
    
    # 将继承的字段合并回原df
    if not df_target.empty:
        inherit_merge_cols = ["symbol", "trade_date"] + [c for c in inherit_cols if c in df_target.columns]
        df = df.merge(df_target[inherit_merge_cols], on=["symbol", "trade_date"], how="left", suffixes=("", "_y"))
        for c in inherit_cols:
            if f"{c}_y" in df.columns:
                df[c] = df[c].combine_first(df[f"{c}_y"])
                df = df.drop(columns=[f"{c}_y"])

    # 筛选L1列
    local_keys = select_local_l1_columns(df, keys)
    
    # 提取目标日期数据
    out = df[df["trade_date"] == target_date][["symbol", "trade_date", *local_keys]].copy()
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    
    # 过滤B股
    b_mask = out["symbol"].str.match(r"^SH9\d{5}$", na=False) | out["symbol"].str.match(r"^SZ2\d{5}$", na=False)
    if b_mask.any():
        out = out[~b_mask].copy()
    
    out = out.sort_values(["trade_date", "symbol"]).reset_index(drop=True)

    # 写入输出
    args.output.parent.mkdir(parents=True, exist_ok=True)
    out.to_parquet(args.output, index=False)

    # 可选：合并到年度宽表
    if args.merge_into_yearly:
        yearly = DEFAULT_OUTPUT_DIR / f"model_features_{year}.parquet"
        if yearly.exists():
            base = pd.read_parquet(yearly)
            base["trade_date"] = pd.to_datetime(base["trade_date"])
            keep_old = base[base["trade_date"] != target_date].copy()
            day_old = base[base["trade_date"] == target_date].copy()
            local_cols = [c for c in local_keys if c in base.columns and c in out.columns]
            day_new = day_old.merge(
                out[["symbol", "trade_date", *local_cols]],
                on=["symbol", "trade_date"],
                how="left",
                suffixes=("", "__new"),
            )
            for c in local_cols:
                c_new = f"{c}__new"
                if c_new in day_new.columns:
                    day_new[c] = day_new[c_new]
            day_new = day_new[[c for c in day_new.columns if not c.endswith("__new")]]

            # 补充新增symbol
            day_keys = set(zip(day_new["symbol"], day_new["trade_date"]))
            out_extra = out[~out[["symbol", "trade_date"]].apply(tuple, axis=1).isin(day_keys)].copy()
            if not out_extra.empty:
                extra = pd.DataFrame(columns=base.columns)
                extra = pd.concat([extra, pd.DataFrame(index=range(len(out_extra)))], ignore_index=True)
                extra["symbol"] = out_extra["symbol"].values
                extra["trade_date"] = out_extra["trade_date"].values
                for c in local_cols:
                    extra[c] = out_extra[c].values
                day_new = pd.concat([day_new, extra], ignore_index=True)

            merged = pd.concat([keep_old, day_new], ignore_index=True).sort_values(["trade_date", "symbol"]).reset_index(drop=True)
            merged.to_parquet(yearly, index=False)

    # 可选：输出审计
    if args.write_audit:
        payload = {
            "date": target_date.date(),
            "rows": int(len(out)),
            "symbols": int(out["symbol"].nunique()),
            "local_feature_count": int(len(local_keys)),
            "output": str(args.output),
            "csmar_root": str(args.csmar_root),
            "silver_dir": str(args.silver_dir),
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "columns": local_keys,
        }
        write_audit(args.output, payload)

    print(json.dumps({
        "date": str(target_date.date()),
        "rows": len(out),
        "symbols": out["symbol"].nunique(),
        "local_feature_count": len(local_keys),
        "output": str(args.output),
    }, ensure_ascii=False))


if __name__ == "__main__":
    main()