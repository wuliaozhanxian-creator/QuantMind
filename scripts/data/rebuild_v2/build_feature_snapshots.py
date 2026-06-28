#!/usr/bin/env python3
"""Build QuantMind 152-column model feature snapshots from the v2 silver layer."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data.rebuild_v2.common import (
    DEFAULT_AUDIT_DIR,
    DEFAULT_CATALOG,
    DEFAULT_CSMAR_ROOT,
    DEFAULT_FEATURE_DIR,
    DEFAULT_SILVER_DIR,
    coerce_date,
    feature_keys,
    lag_daily_features,
    normalize_symbol_column,
    read_source_table,
    safe_div,
    safe_div_zero,
    write_metadata,
    zscore_by_date,
)


def _rolling_group(df: pd.DataFrame, col: str, window: int, fn: str) -> pd.Series:
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
    return df.groupby("symbol", observed=True)[col].transform(
        lambda s: s.ewm(span=span, adjust=False, min_periods=span).mean()
    )


def _rsi(df: pd.DataFrame, ret_col: str, window: int) -> pd.Series:
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

    # 收益率计算必须使用未复权价格 (raw_close)，避免复权因子切换导致口径不一致
    # 当 factor 从 f1 变为 f2 时，adj_close[t] / adj_close[t-1] = close[t] * f2 / (close[t-1] * f1)
    # 这会导致收益率被放大 f2/f1 倍，因此改用 raw_close 计算真实收益率
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

    low9 = _rolling_group(df, "adj_low", 9, "min") if False else None
    low9 = df.groupby("symbol", observed=True)["adj_low"].transform(lambda s: s.rolling(9, min_periods=9).min())
    high9 = df.groupby("symbol", observed=True)["adj_high"].transform(lambda s: s.rolling(9, min_periods=9).max())
    rsv = safe_div(adj_close - low9, high9 - low9) * 100
    df["mom_kdj_k"] = rsv.groupby(df["symbol"]).transform(lambda s: s.ewm(alpha=1 / 3, adjust=False).mean())
    df["mom_kdj_d"] = df.groupby("symbol", observed=True)["mom_kdj_k"].transform(
        lambda s: s.ewm(alpha=1 / 3, adjust=False).mean()
    )
    df["mom_kdj_j"] = 3 * df["mom_kdj_k"] - 2 * df["mom_kdj_d"]
    df["mom_roc_12"] = df["mom_ret_12d"] if "mom_ret_12d" in df else safe_div(adj_close, g["adj_close"].shift(12)) - 1
    high20 = df.groupby("symbol", observed=True)["adj_high"].transform(lambda s: s.rolling(20, min_periods=20).max())
    df["mom_breakout_20d"] = safe_div(adj_close, high20) - 1
    # 20 日窗口跨越复权因子切换时，使用 raw 口径可避免动量类统计被因子级别放大。
    df["mom_ret_20d"] = safe_div(raw_close, g["close"].shift(20)) - 1

    tr = pd.concat(
        [(adj_high - adj_low), (adj_high - prev_close).abs(), (adj_low - prev_close).abs()],
        axis=1,
    ).max(axis=1)
    df["vol_true_range"] = tr
    for n in (14, 20):
        df[f"vol_atr_{n}"] = df.groupby("symbol", observed=True)["vol_true_range"].transform(
            lambda s: s.rolling(n, min_periods=n).mean()
        )
    raw_tr = pd.concat(
        [(raw_high - raw_low), (raw_high - raw_prev_close).abs(), (raw_low - raw_prev_close).abs()],
        axis=1,
    ).max(axis=1)
    df["_raw_tr"] = raw_tr
    df["vol_atr_20"] = df.groupby("symbol", observed=True)["_raw_tr"].transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    for n in (5, 10, 20, 60):
        df[f"vol_std_{n}"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
            lambda s: s.rolling(n, min_periods=n).std()
        )

    log_hl = np.log(safe_div(adj_high, adj_low))
    parkinson_base = (log_hl**2) / (4 * np.log(2))
    log_co = np.log(safe_div(adj_close, adj_open))
    log_ho = np.log(safe_div(adj_high, adj_open))
    log_lo = np.log(safe_div(adj_low, adj_open))
    gk_base = 0.5 * log_hl**2 - (2 * np.log(2) - 1) * log_co**2
    rs_base = np.log(safe_div(adj_high, adj_close)) * log_ho + np.log(safe_div(adj_low, adj_close)) * log_lo
    df["_parkinson_base"] = parkinson_base
    df["_gk_base"] = gk_base
    df["_rs_base"] = rs_base
    for n in (10, 20):
        df[f"vol_parkinson_{n}"] = np.sqrt(_rolling_group(df, "_parkinson_base", n, "mean"))
        df[f"vol_gk_{n}"] = np.sqrt(_rolling_group(df, "_gk_base", n, "mean").clip(lower=0))
        df[f"vol_rs_{n}"] = np.sqrt(_rolling_group(df, "_rs_base", n, "mean").clip(lower=0))
    df["vol_downside_20"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: s.clip(upper=0).rolling(20, min_periods=20).std()
    )
    df["vol_upside_20"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: s.clip(lower=0).rolling(20, min_periods=20).std()
    )

    df["liq_volume"] = df["volume"]
    df["liq_amount"] = df["amount"]
    for n in (5, 10, 20):
        df[f"liq_volume_ma_{n}"] = _rolling_group(df, "volume", n, "mean")
        df[f"liq_amount_ma_{n}"] = _rolling_group(df, "amount", n, "mean")
    for n in (5, 20):
        df[f"liq_volume_ratio_{n}"] = safe_div(df["volume"], df[f"liq_volume_ma_{n}"])
        df[f"liq_amount_ratio_{n}"] = safe_div(df["amount"], df[f"liq_amount_ma_{n}"])

    sign = np.sign(df["mom_ret_1d"].fillna(0))
    df["_obv"] = (sign * df["volume"].fillna(0)).groupby(df["symbol"]).cumsum()
    df["liq_obv_20"] = df.groupby("symbol", observed=True)["_obv"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    df["liq_obv_60"] = df.groupby("symbol", observed=True)["_obv"].transform(lambda s: s.rolling(60, min_periods=60).mean())
    typical = (adj_high + adj_low + adj_close) / 3
    money_flow = typical * df["volume"]
    pos_flow = money_flow.where(typical.groupby(df["symbol"]).diff() > 0, 0.0)
    neg_flow = money_flow.where(typical.groupby(df["symbol"]).diff() < 0, 0.0)
    pos14 = pos_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    neg14 = neg_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    df["liq_mfi_14"] = 100 - 100 / (1 + safe_div(pos14, neg14))
    clv = safe_div((adj_close - adj_low) - (adj_high - adj_close), adj_high - adj_low)
    df["_ad"] = (clv.fillna(0) * df["volume"].fillna(0)).groupby(df["symbol"]).cumsum()
    df["liq_accdist_20"] = df.groupby("symbol", observed=True)["_ad"].transform(lambda s: s.rolling(20, min_periods=20).mean())
    amihud_base = df["mom_ret_1d"].abs() / df["amount"].replace(0, np.nan)
    for n in (20, 60):
        parts: list[pd.Series] = []
        for _, part in df.groupby("symbol", observed=True, sort=False):
            valid = amihud_base.loc[part.index].dropna()
            out = pd.Series(np.nan, index=part.index, dtype=float)
            if not valid.empty:
                out.loc[valid.index] = valid.rolling(n, min_periods=n).mean().values
            parts.append(out)
        df[f"liq_amihud_{n}"] = pd.concat(parts).sort_index()

    return df.drop(columns=[c for c in df.columns if c.startswith("_")], errors="ignore")


def merge_turnover(csmar_root: Path, df: pd.DataFrame, year: int) -> pd.DataFrame:
    src = read_source_table(csmar_root, "个股换手率表(日)", year)
    src = coerce_date(src, "Trddt")
    src = normalize_symbol_column(src, "Stkcd")
    out = src[["symbol", "trade_date"]].copy()
    out["liq_turnover_os"] = pd.to_numeric(src["ToverOs"], errors="coerce") / 100
    out["liq_turnover_tl"] = pd.to_numeric(src["ToverTl"], errors="coerce") / 100
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    return df.merge(out, on=["symbol", "trade_date"], how="left")


def merge_trade_stats(csmar_root: Path, df: pd.DataFrame, year: int) -> pd.DataFrame:
    src = read_source_table(csmar_root, "日交易统计文件", year)
    src = coerce_date(src, "Trddt")
    src = normalize_symbol_column(src, "Stkcd")
    out = src[["symbol", "trade_date"]].copy()
    out["liq_trade_count"] = pd.to_numeric(src.get("Toltrdtims"), errors="coerce")
    out["liq_avg_trade_size"] = safe_div(pd.to_numeric(src.get("Tolstknva"), errors="coerce"), out["liq_trade_count"])
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    df = df.merge(out, on=["symbol", "trade_date"], how="left")

    try:
        spread = read_source_table(csmar_root, "个股买卖价差表(日)", year)
        spread = coerce_date(spread, "Trddt")
        spread = normalize_symbol_column(spread, "Stkcd")
        fallback = spread[["symbol", "trade_date"]].copy()
        fallback["liq_trade_count_fallback"] = pd.to_numeric(spread.get("Num"), errors="coerce")
        fallback = fallback.drop_duplicates(["symbol", "trade_date"], keep="last")
        # fallback = lag_daily_features(fallback, ["liq_trade_count_fallback"])
        df = df.merge(fallback, on=["symbol", "trade_date"], how="left")
        df["liq_trade_count"] = df["liq_trade_count"].combine_first(df["liq_trade_count_fallback"])
        df["liq_avg_trade_size"] = df["liq_avg_trade_size"].combine_first(safe_div(df["liq_amount"], df["liq_trade_count"]))
        df = df.drop(columns=["liq_trade_count_fallback"], errors="ignore")
    except Exception:
        pass
    return df


def merge_realized_jump(csmar_root: Path, df: pd.DataFrame, year: int) -> pd.DataFrame:
    try:
        realized = read_source_table(csmar_root, "个股已实现指标表(日)", year)
        realized = coerce_date(realized, "Trddt")
        realized = normalize_symbol_column(realized, "Stkcd")
        out = realized[["symbol", "trade_date"]].copy()
        out["vol_realized_rv"] = pd.to_numeric(realized.get("RV"), errors="coerce")
        out["vol_realized_rrv"] = pd.to_numeric(realized.get("RRV"), errors="coerce")
        out["vol_realized_rskew"] = pd.to_numeric(realized.get("RSkew"), errors="coerce")
        out["vol_realized_rkurt"] = pd.to_numeric(realized.get("RKurt"), errors="coerce")
        out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
        # out = lag_daily_features(out, ["vol_realized_rv", "vol_realized_rrv", "vol_realized_rskew", "vol_realized_rkurt"])
        df = df.merge(out, on=["symbol", "trade_date"], how="left")
    except Exception:
        pass

    try:
        jump = read_source_table(csmar_root, "个股跳跃指标表(日)", year)
        # 兼容两套列名：TradingDate/Symbol 或 Trddt/Stkcd
        td_valid = pd.Series(False, index=jump.index)
        trddt_valid = pd.Series(False, index=jump.index)
        if "TradingDate" in jump.columns:
            td_valid = jump["TradingDate"].notna() & (jump["TradingDate"].astype(str).str.strip() != "")
        if "Trddt" in jump.columns:
            trddt_valid = jump["Trddt"].notna() & (~jump["Trddt"].astype(str).isin(["", "None", "nan"]))
        if td_valid.sum() > trddt_valid.sum() and "TradingDate" in jump.columns and "Symbol" in jump.columns:
            date_col, code_col = "TradingDate", "Symbol"
        elif "Trddt" in jump.columns and "Stkcd" in jump.columns:
            date_col, code_col = "Trddt", "Stkcd"
        else:
            raise KeyError(f"Jump table missing expected columns: {jump.columns.tolist()[:20]}")
        jump = coerce_date(jump, date_col)
        jump = normalize_symbol_column(jump, code_col)
        out = jump[["symbol", "trade_date"]].copy()
        rv = pd.to_numeric(jump.get("RV"), errors="coerce")
        out["vol_jump_zadj"] = pd.to_numeric(jump.get("Z_Adj"), errors="coerce")
        out["vol_jump_rjv_ratio"] = safe_div(pd.to_numeric(jump.get("RJV"), errors="coerce"), rv)
        out["vol_jump_sjv_ratio"] = safe_div(pd.to_numeric(jump.get("SJV"), errors="coerce"), rv)
        out["micro_jump_flag"] = pd.to_numeric(jump.get("ISJump"), errors="coerce")
        out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
        # out = lag_daily_features(out, ["vol_jump_zadj", "vol_jump_rjv_ratio", "vol_jump_sjv_ratio", "micro_jump_flag"])
        df = df.merge(out.drop_duplicates(["symbol", "trade_date"], keep="last"), on=["symbol", "trade_date"], how="left")
    except Exception:
        pass
    return df


def merge_flow_micro(csmar_root: Path, df: pd.DataFrame, year: int) -> pd.DataFrame:
    try:
        imb = read_source_table(csmar_root, "个股买卖不平衡指标表(日)", year)
        imb = coerce_date(imb, "Trddt")
        imb = normalize_symbol_column(imb, "Stkcd")
        out = imb[["symbol", "trade_date"]].copy()
        b_amt = pd.to_numeric(imb.get("B_Amount"), errors="coerce")
        s_amt = pd.to_numeric(imb.get("S_Amount"), errors="coerce")
        b_num = pd.to_numeric(imb.get("B_Num"), errors="coerce")
        s_num = pd.to_numeric(imb.get("S_Num"), errors="coerce")
        b_vol = pd.to_numeric(imb.get("B_Volume"), errors="coerce")
        s_vol = pd.to_numeric(imb.get("S_Volume"), errors="coerce")
        out["flow_net_amount"] = b_amt - s_amt
        out["flow_net_amount_ratio"] = safe_div_zero(b_amt - s_amt, b_amt + s_amt)
        for name, suffix in (("large", "L"), ("medium", "M"), ("small", "S")):
            ba = pd.to_numeric(imb.get(f"B_Amount_{suffix}"), errors="coerce")
            sa = pd.to_numeric(imb.get(f"S_Amount_{suffix}"), errors="coerce")
            bn = pd.to_numeric(imb.get(f"B_Num_{suffix}"), errors="coerce")
            sn = pd.to_numeric(imb.get(f"S_Num_{suffix}"), errors="coerce")
            out[f"flow_{name}_net_amount"] = ba - sa
            out[f"flow_{name}_net_ratio"] = safe_div_zero(ba - sa, ba + sa)
            out[f"micro_imbalance_{name}"] = safe_div_zero(ba - sa, ba + sa)
            if name == "large":
                out["flow_large_net_order"] = bn - sn
                out["flow_large_order_ratio"] = safe_div_zero(bn - sn, bn + sn)
        out["flow_net_order_count"] = b_num - s_num
        out["flow_net_order_ratio"] = safe_div_zero(b_num - s_num, b_num + s_num)
        out["micro_imbalance_volume"] = safe_div_zero(b_vol - s_vol, b_vol + s_vol)
        out["micro_imbalance_amount"] = safe_div_zero(b_amt - s_amt, b_amt + s_amt)
        out["micro_imbalance_count"] = safe_div_zero(b_num - s_num, b_num + s_num)
        cols = [c for c in out.columns if c not in ("symbol", "trade_date")]
        out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
        # out = lag_daily_features(out, cols)
        df = df.merge(out, on=["symbol", "trade_date"], how="left")
    except Exception:
        pass

    try:
        vpin = read_source_table(csmar_root, "个股知情交易概率指标表(日)", year)
        vpin = coerce_date(vpin, "Trddt")
        vpin = normalize_symbol_column(vpin, "Stkcd")
        out = vpin[["symbol", "trade_date"]].copy()
        out["flow_vpin"] = pd.to_numeric(vpin.get("VPIN"), errors="coerce")
        out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
        out = out.sort_values(["symbol", "trade_date"])
        out["flow_vpin_ma_5"] = _rolling_group(out, "flow_vpin", 5, "mean")
        out["flow_vpin_ma_20"] = _rolling_group(out, "flow_vpin", 20, "mean")
        out["flow_vpin_delta_5"] = out["flow_vpin"] - out.groupby("symbol", observed=True)["flow_vpin"].shift(5)
        # out = lag_daily_features(out, ["flow_vpin", "flow_vpin_ma_5", "flow_vpin_ma_20", "flow_vpin_delta_5"])
        df = df.merge(out, on=["symbol", "trade_date"], how="left")
    except Exception:
        pass

    try:
        spread = read_source_table(csmar_root, "个股买卖价差表(日)", year)
        spread = coerce_date(spread, "Trddt")
        spread = normalize_symbol_column(spread, "Stkcd")
        out = spread[["symbol", "trade_date"]].copy()
        mapping = {
            "micro_qsp_equal": "Qsp_equal",
            "micro_esp_equal": "Esp_equal",
            "micro_aqsp_equal": "AQsp_equal",
            "micro_qsp_time": "Qsp_time",
            "micro_esp_time": "Esp_time",
            "micro_qsp_volume": "Qsp_Volume",
            "micro_esp_volume": "Esp_Volume",
            "micro_qsp_amount": "Qsp_Amount",
            "micro_esp_amount": "Esp_Amount",
            "flow_qsp": "Qsp_equal",
            "flow_esp": "Esp_equal",
            "flow_aqsp": "AQsp_equal",
            "flow_qsp_time": "Qsp_time",
            "flow_esp_time": "Esp_time",
        }
        for target, source in mapping.items():
            out[target] = pd.to_numeric(spread.get(source), errors="coerce")
        out["micro_effective_spread"] = out["micro_esp_equal"]
        out["micro_quoted_spread"] = out["micro_qsp_equal"]
        out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
        out = out.sort_values(["symbol", "trade_date"])
        out["micro_spread_vol_20"] = _rolling_group(out, "micro_esp_equal", 20, "std")
        cols = [c for c in out.columns if c not in ("symbol", "trade_date")]
        # out = lag_daily_features(out, cols)
        df = df.merge(out, on=["symbol", "trade_date"], how="left")
    except Exception:
        pass

    for col in ("flow_net_amount", "flow_vpin", "flow_esp"):
        if col in df:
            df[f"_z_{col}"] = zscore_by_date(df, col)
    df["flow_pressure_index"] = df[[c for c in ("_z_flow_net_amount", "_z_flow_vpin", "_z_flow_esp") if c in df]].mean(axis=1)
    df["micro_pressure_score"] = df[[c for c in ("_z_flow_esp", "_z_flow_vpin", "micro_imbalance_amount") if c in df]].mean(axis=1)
    return df.drop(columns=[c for c in df.columns if c.startswith("_z_")], errors="ignore")


def merge_style(csmar_root: Path, df: pd.DataFrame, year: int) -> pd.DataFrame:
    df["style_ln_mv_total"] = np.log(pd.to_numeric(df.get("total_mv"), errors="coerce").where(lambda s: s > 0))
    df["style_ln_mv_float"] = np.log(pd.to_numeric(df.get("float_mv"), errors="coerce").where(lambda s: s > 0))
    df["style_size_percentile"] = df.groupby("trade_date", observed=True)["style_ln_mv_total"].rank(pct=True)

    try:
        # 估值口径需要跨年历史（ASOF），优先加载 year-2/year-1/year，
        # 避免仅拿到当年快照导致 effective_date 整体晚于交易日而全空。
        val_frames: list[pd.DataFrame] = []
        for y in (year - 2, year - 1, year):
            try:
                part = read_source_table(csmar_root, "相对价值指标", y)
                if not part.empty:
                    val_frames.append(part)
            except Exception:
                pass
        if not val_frames:
            val = read_source_table(csmar_root, "相对价值指标", None)
        else:
            val = pd.concat(val_frames, ignore_index=True)
        val = coerce_date(val, "Accper", "report_date")
        val = normalize_symbol_column(val, "Stkcd")
        val["effective_date"] = val["report_date"] + pd.offsets.MonthEnd(4)
        val = val.sort_values(["symbol", "effective_date"])
        # PB: F100701A 是市净率，直接使用
        val["style_bp"] = pd.to_numeric(val.get("F100701A"), errors="coerce")
        # EP_TTM = 1 / PE_TTM, F100101B 是 PE_TTM
        val["style_ep_ttm"] = 1 / pd.to_numeric(val.get("F100101B"), errors="coerce").replace(0, np.nan)
        val["style_valuation_composite"] = val[["style_bp", "style_ep_ttm"]].rank(pct=True).mean(axis=1)
        base = df[["symbol", "trade_date"]].sort_values(["symbol", "trade_date"])
        base["trade_date"] = pd.to_datetime(base["trade_date"]).astype("datetime64[ns]")
        val_asof = val[
            ["symbol", "effective_date", "style_bp", "style_ep_ttm", "style_valuation_composite"]
        ].rename(columns={"effective_date": "trade_date"})
        val_asof["trade_date"] = pd.to_datetime(val_asof["trade_date"]).astype("datetime64[ns]")
        asof_parts: list[pd.DataFrame] = []
        for symbol, base_part in base.groupby("symbol", observed=True, sort=False):
            val_part = val_asof[val_asof["symbol"] == symbol].sort_values("trade_date")
            if val_part.empty:
                merged = base_part.copy()
                merged["style_bp"] = np.nan
                merged["style_ep_ttm"] = np.nan
                merged["style_valuation_composite"] = np.nan
            else:
                merged = pd.merge_asof(
                    base_part.sort_values("trade_date"),
                    val_part.sort_values("trade_date").drop(columns=["symbol"]),
                    on="trade_date",
                    direction="backward",
                )
                merged["symbol"] = symbol
            asof_parts.append(merged)
        asof = pd.concat(asof_parts, ignore_index=True)
        df = df.merge(asof, on=["symbol", "trade_date"], how="left")
        df["style_value_percentile"] = df.groupby("trade_date", observed=True)["style_valuation_composite"].rank(pct=True)
    except Exception:
        pass

    try:
        fac_frames: list[pd.DataFrame] = []
        for y in (year - 1, year):
            try:
                part = read_source_table(csmar_root, "三因子模型指标(日)", y)
                if not part.empty:
                    fac_frames.append(part)
            except Exception:
                pass
        fac = pd.concat(fac_frames, ignore_index=True) if fac_frames else read_source_table(csmar_root, "三因子模型指标(日)", year)
        fac = coerce_date(fac, "TradingDate")
        for col in ("RiskPremium1", "SMB1", "HML1"):
            fac[col] = pd.to_numeric(fac[col], errors="coerce")
        fac = fac.groupby("trade_date", as_index=False)[["RiskPremium1", "SMB1", "HML1"]].mean()
        fac = fac.rename(
            columns={"RiskPremium1": "style_mkt_premium", "SMB1": "style_smb", "HML1": "style_hml"}
        )
        fac[["style_mkt_premium", "style_smb", "style_hml"]] = fac[
            ["style_mkt_premium", "style_smb", "style_hml"]
        ].shift(1)
        df = df.merge(fac, on="trade_date", how="left")
    except Exception:
        pass

    index_ret = load_zz500_return(csmar_root, year)
    if not index_ret.empty:
        df = df.merge(index_ret, on="trade_date", how="left")
        for n in (20, 60, 120):
            beta_parts: list[pd.Series] = []
            for _, part in df.groupby("symbol", observed=True, sort=False):
                pair = part[["trade_date", "mom_ret_1d", "idx_mkt_ret"]].dropna().sort_values("trade_date")
                beta = pd.Series(np.nan, index=part.index, dtype=float)
                if not pair.empty:
                    cov = pair["mom_ret_1d"].rolling(n, min_periods=n).cov(pair["idx_mkt_ret"])
                    var = pair["idx_mkt_ret"].rolling(n, min_periods=n).var().replace(0, np.nan)
                    beta.loc[pair.index] = (cov / var).values
                beta_parts.append(beta)
            df[f"style_beta_{n}"] = pd.concat(beta_parts).sort_index()
        for n in (20, 60):
            beta = df[f"style_beta_{n}"]
            resid = df["mom_ret_1d"] - beta * df["idx_mkt_ret"]
            df[f"style_idio_vol_{n}"] = resid.groupby(df["symbol"]).transform(lambda s: s.rolling(n, min_periods=20).std())
        resid20 = df["mom_ret_1d"] - df["style_beta_20"] * df["idx_mkt_ret"]
        df["style_residual_ret_20"] = resid20.groupby(df["symbol"]).transform(lambda s: s.rolling(20, min_periods=20).sum())
    for col in (
        "style_bp",
        "style_ep_ttm",
        "style_valuation_composite",
        "style_value_percentile",
        "style_smb",
        "style_hml",
        "style_mkt_premium",
        "style_beta_20",
        "style_beta_60",
        "style_beta_120",
        "style_idio_vol_20",
        "style_idio_vol_60",
        "style_residual_ret_20",
    ):
        if col not in df.columns:
            df[col] = np.nan
    return df


def load_zz500_return(csmar_root: Path, year: int) -> pd.DataFrame:
    def normalize_index_frame(raw: pd.DataFrame) -> pd.DataFrame:
        if raw.empty:
            return pd.DataFrame(columns=["trade_date", "index_code", "close"])
        date_candidates = [c for c in ("TradingDate", "Trddt", "Idxtrd01") if c in raw.columns]
        code_candidates = [c for c in ("Indexcd", "Symbol", "S_INFO_WINDCODE", "index_code", "Idxcd") if c in raw.columns]
        close_candidates = [c for c in ("Idxtrd05", "Clsindex", "CloseIndex", "close", "ClosePrice") if c in raw.columns]
        if not date_candidates or not code_candidates or not close_candidates:
            return pd.DataFrame(columns=["trade_date", "index_code", "close"])

        date_col = max(date_candidates, key=lambda c: raw[c].notna().sum())
        code_col = max(code_candidates, key=lambda c: raw[c].notna().sum())
        # close 列：优先选非空数最多的，但同一代码子集内如果 Idxtrd05 有值就优先
        # （Clsindex 可能覆盖历史但量级不一致）
        close_col = max(close_candidates, key=lambda c: pd.to_numeric(raw[c], errors="coerce").notna().sum())

        out = pd.DataFrame()
        out["trade_date"] = pd.to_datetime(raw[date_col], errors="coerce")
        out["index_code"] = (
            raw[code_col].astype(str).str.extract(r"(\d{6})", expand=False).fillna("").str.zfill(6)
        )
        out["close"] = pd.to_numeric(raw[close_col], errors="coerce")
        out = out[(out["trade_date"].notna()) & (out["index_code"] != "") & (out["close"].notna())].copy()
        return out

    frames: list[pd.DataFrame] = []
    for table_name in ("国内指数日行情文件",):  # 只用这张表；"指数文件"含全历史 Clsindex 量级不一致
        # 不传 year：避免 read_source_table 用稀疏的 Trddt 做
        # 年份过滤；Indexcd=000905 等指数子集的 Trddt 全空，日期在 Idxtrd01 列里
        try:
            df_full = read_source_table(csmar_root, table_name, None)
            part = normalize_index_frame(df_full)  # 内部按非空数选最佳日期列
            if not part.empty:
                part = part[part["trade_date"].dt.year.isin([year - 1, year])].copy()
                if not part.empty:
                    frames.append(part)
        except Exception:
            pass

    if not frames:
        return pd.DataFrame(columns=["trade_date", "idx_mkt_ret"])

    idx = pd.concat(frames, ignore_index=True)
    # 排除无效指数代码（000000 是 Indexcd 清洗残留）
    valid_codes = {"000300", "000905", "000016", "000001", "000903",
                   "399001", "399005", "399903", "399905", "399300", "930905"}
    idx_valid = idx[idx["index_code"].isin(valid_codes)]
    if idx_valid.empty:
        idx_valid = idx[idx["index_code"] != "000000"]  # fallback: 排除明显无效
    idx = idx_valid
    # 优先选覆盖天数最多的指数代码；000300(沪深300) 数据最完整（含历史年份），
    # 000905(中证500) 仅 2026 年有，不足 120 日窗口。选覆盖最广者。
    all_codes = idx.groupby("index_code", observed=True)["trade_date"].nunique()
    best_code = all_codes.sort_values(ascending=False).index[0]

    series = idx[idx["index_code"] == best_code].copy()
    # 同一日期可能来自不同表/来源，保留第一个（parquet 优先于 CSV fallback）
    series = series.sort_values("trade_date").drop_duplicates("trade_date", keep="first")
    series["idx_mkt_ret"] = series["close"].pct_change()
    return series[["trade_date", "idx_mkt_ret"]]


def merge_industry(csmar_root: Path, df: pd.DataFrame) -> pd.DataFrame:
    try:
        co = read_source_table(csmar_root, "公司文件", None)
        co = normalize_symbol_column(co, "Stkcd")
        ind = co[["symbol", "Indcd", "Nnindcd"]].rename(columns={"Indcd": "industry_l1", "Nnindcd": "industry_l2"})
        df = df.merge(ind.drop_duplicates("symbol", keep="last"), on="symbol", how="left")
    except Exception:
        df["industry_l1"] = np.nan
        df["industry_l2"] = np.nan

    for n in (1, 5, 10, 20, 60):
        df[f"ind_ret_{n}d"] = df.groupby(["trade_date", "industry_l1"], observed=True)[f"mom_ret_{n}d"].transform("mean")
    df["ind_vol_20"] = df.groupby(["trade_date", "industry_l1"], observed=True)["vol_std_20"].transform("mean")
    df["ind_turnover_20"] = df.groupby(["trade_date", "industry_l1"], observed=True)["liq_turnover_os"].transform("mean")
    df["ind_amount_20"] = df.groupby(["trade_date", "industry_l1"], observed=True)["liq_amount_ma_20"].transform("mean")
    df["ind_strength_20"] = df["mom_ret_20d"] - df["ind_ret_20d"]
    df["ind_strength_60"] = df["mom_ret_60d"] - df["ind_ret_60d"]
    df["ind_dispersion_20"] = df.groupby(["trade_date", "industry_l1"], observed=True)["mom_ret_20d"].transform("std")
    df["ind_up_breadth_20"] = df.groupby(["trade_date", "industry_l1"], observed=True)["mom_ret_20d"].transform(lambda s: (s > 0).mean())
    df["ind_down_breadth_20"] = df.groupby(["trade_date", "industry_l1"], observed=True)["mom_ret_20d"].transform(lambda s: (s < 0).mean())
    df["ind_relative_volume_20"] = safe_div(df["liq_volume_ma_20"], df.groupby(["trade_date", "industry_l1"], observed=True)["liq_volume_ma_20"].transform("mean"))
    df["ind_relative_volatility_20"] = safe_div(df["vol_std_20"], df.groupby(["trade_date", "industry_l1"], observed=True)["vol_std_20"].transform("mean"))
    df["ind_relative_flow_20"] = df["flow_net_amount_ratio"] - df.groupby(["trade_date", "industry_l1"], observed=True)["flow_net_amount_ratio"].transform("mean")
    industry_mom = df.groupby(["trade_date", "industry_l1"], observed=True)["ind_ret_20d"].transform("first")
    df["ind_momentum_rank_20"] = industry_mom.groupby(df["trade_date"]).rank(pct=True)
    df["ind_value_rank"] = df.groupby("trade_date", observed=True)["style_value_percentile"].rank(pct=True)
    df["ind_size_rank"] = df.groupby("trade_date", observed=True)["style_size_percentile"].rank(pct=True)
    code_l1 = pd.Series(pd.Categorical(df["industry_l1"]).codes, index=df.index).astype(float)
    code_l2 = pd.Series(pd.Categorical(df["industry_l2"]).codes, index=df.index).astype(float)
    df["ind_code_l1"] = code_l1.mask(code_l1 < 0)
    df["ind_code_l2"] = code_l2.mask(code_l2 < 0)
    return df.drop(columns=["industry_l1", "industry_l2"], errors="ignore")


def build_features(year: int, csmar_root: Path, silver_dir: Path, feature_dir: Path, audit_dir: Path, keys: list[str]) -> pd.DataFrame:
    base_path = silver_dir / f"market_base_{year}.parquet"
    if not base_path.exists():
        raise FileNotFoundError(f"Run build_silver_market.py first: {base_path}")
    frames: list[pd.DataFrame] = []
    prev_path = silver_dir / f"market_base_{year - 1}.parquet"
    if prev_path.exists():
        frames.append(pd.read_parquet(prev_path))
    frames.append(pd.read_parquet(base_path))
    df = pd.concat(frames, ignore_index=True)
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df = add_daily_technical_features(df)
    df = merge_turnover(csmar_root, df, year)
    df = merge_trade_stats(csmar_root, df, year)
    df = merge_realized_jump(csmar_root, df, year)
    df = merge_flow_micro(csmar_root, df, year)
    df = merge_style(csmar_root, df, year)
    df = merge_industry(csmar_root, df)

    for key in keys:
        if key not in df.columns:
            df[key] = np.nan

    out = df[df["trade_date"].dt.year == year][["symbol", "trade_date", *keys]].copy()
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    # 过滤B股：SH9xxxxx（上海B股，美元计价）和 SZ2xxxxx（深圳B股，港元计价）
    b_mask = out["symbol"].str.match(r"^SH9\d{5}$", na=False) | out["symbol"].str.match(r"^SZ2\d{5}$", na=False)
    if b_mask.any():
        print(f"[build_features] 过滤B股 {b_mask.sum()} 行, symbols={sorted(out.loc[b_mask, 'symbol'].unique())[:10]}")
        out = out[~b_mask].copy()
    out = out.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    metadata = {
        "year": year,
        "rows": int(len(out)),
        "symbols": int(out["symbol"].nunique()),
        "min_date": out["trade_date"].min(),
        "max_date": out["trade_date"].max(),
        "feature_count": len(keys),
        "missing_rate": {k: float(out[k].isna().mean()) for k in keys},
    }
    feature_dir.mkdir(parents=True, exist_ok=True)
    audit_dir.mkdir(parents=True, exist_ok=True)
    out.to_parquet(feature_dir / f"model_features_{year}.parquet", index=False)
    write_metadata(audit_dir / f"model_features_{year}.metadata.json", metadata)
    return out


def main() -> None:
    parser = argparse.ArgumentParser(description="Build QuantMind v2 152-dimensional feature snapshots")
    parser.add_argument("--csmar-root", type=Path, default=DEFAULT_CSMAR_ROOT)
    parser.add_argument("--silver-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--feature-dir", type=Path, default=DEFAULT_FEATURE_DIR)
    parser.add_argument("--audit-dir", type=Path, default=DEFAULT_FEATURE_DIR,
                        help="metadata 输出目录，默认与 feature-dir 相同")
    parser.add_argument("--catalog", type=Path, default=DEFAULT_CATALOG)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    args = parser.parse_args()

    keys = feature_keys(args.catalog)
    for year in args.years:
        out = build_features(year, args.csmar_root, args.silver_dir, args.feature_dir, args.audit_dir, keys)
        print(
            {
                "year": year,
                "rows": len(out),
                "min_date": out["trade_date"].min(),
                "max_date": out["trade_date"].max(),
                "feature_count": len(keys),
            }
        )


if __name__ == "__main__":
    main()
