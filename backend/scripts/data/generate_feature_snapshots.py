#!/usr/bin/env python3
"""从 stock_daily_latest 表生成 48 维训练特征快照。

原版仅生成 30 列（9 基础 + 21 简单衍生），导致 43/48 个训练特征缺失。
本版本参照 scripts/data/rebuild_v2/build_feature_snapshots.py 的计算逻辑，
从 OHLCV 基础数据 + 辅助字段（float_mv / bp / ep_ttm / industry）计算出
完整的 48 维训练特征，确保至少 30+ 个有非零数据。

数据源：stock_daily_latest 表（基础 OHLCV 字段有数据，衍生特征列为空）。
输出：db/feature_snapshots/model_features_{year}.parquet
"""
import os
import sys

import numpy as np
import pandas as pd
import psycopg2
from psycopg2.extras import RealDictCursor
from pathlib import Path

# T5.2 入库前校验：添加项目根目录到 sys.path 以导入 StockCodeUtil
_PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))
from backend.shared.stock_utils import StockCodeUtil

PG_DSN = os.getenv(
    "DATABASE_URL",
    "postgresql://quantmind:quantmind2026@localhost:5432/quantmind",
)
# 兼容 SQLAlchemy 格式 (postgresql+asyncpg:// -> postgresql://)
PG_DSN = PG_DSN.replace("postgresql+asyncpg://", "postgresql://").replace(
    "postgresql+psycopg2://", "postgresql://"
)
OUTPUT_DIR = Path(__file__).resolve().parents[3] / "db" / "feature_snapshots"

# 48 维训练特征（与 config.yaml features 列表 + catalog 一致）
TRAINING_FEATURES: list[str] = [
    # 动量因子 (10)
    "mom_ret_5d", "mom_ret_20d", "mom_ret_60d", "mom_ret_120d",
    "mom_ma_gap_5", "mom_ma_gap_20", "mom_ma_gap_60",
    "mom_macd_hist", "mom_rsi_14", "mom_breakout_20d",
    # 波动率因子 (7)
    "vol_std_10", "vol_std_20", "vol_std_60",
    "vol_atr_14", "vol_parkinson_20", "vol_downside_20", "vol_realized_rv",
    # 流动性因子 (9)
    "liq_turnover_tl", "liq_volume_ratio_5", "liq_volume_ratio_20",
    "liq_amount_ma_20", "liq_avg_trade_size", "liq_obv_20",
    "liq_amihud_20", "liq_amihud_60", "liq_mfi_14",
    # 资金流因子 (4) — 需要买卖分类数据，当前不可计算
    "flow_net_amount_ratio", "flow_large_net_ratio",
    "flow_net_order_ratio", "flow_vpin_ma_20",
    # 微观结构因子 (4) — 需要高频数据，当前不可计算
    "micro_imbalance_volume", "micro_effective_spread",
    "micro_pressure_score", "micro_jump_flag",
    # 风格因子 (9)
    "style_ln_mv_float", "style_bp", "style_ep_ttm",
    "style_beta_60", "style_idio_vol_60", "style_residual_ret_20",
    "style_valuation_composite", "style_size_percentile", "style_value_percentile",
    # 行业因子 (5)
    "ind_relative_volume_20", "ind_relative_volatility_20",
    "ind_strength_20", "ind_momentum_rank_20", "ind_value_rank",
]

# 从 stock_daily_latest 读取的字段
_DB_COLUMNS = [
    "symbol", "trade_date", "open", "high", "low", "close",
    "volume", "amount", "pct_change", "adj_factor",
    "float_mv", "total_mv", "bp", "ep_ttm", "industry",
]


def _safe_div(num, den):
    """安全除法，分母为 0 时返回 NaN。"""
    out = pd.Series(num) / pd.Series(den).replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan)


def _rolling_group(df, col, window, fn):
    grouped = df.groupby("symbol", observed=True)[col]
    if fn == "mean":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).mean())
    if fn == "std":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).std())
    if fn == "max":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).max())
    if fn == "min":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).min())
    if fn == "sum":
        return grouped.transform(lambda s: s.rolling(window, min_periods=window).sum())
    raise ValueError(fn)


def _ema(df, col, span):
    return df.groupby("symbol", observed=True)[col].transform(
        lambda s: s.ewm(span=span, adjust=False, min_periods=span).mean()
    )


def compute_factors(df: pd.DataFrame) -> pd.DataFrame:
    """计算 48 维训练特征。

    参照 scripts/data/rebuild_v2/build_feature_snapshots.py 的计算逻辑，
    从 OHLCV 基础数据 + 辅助字段计算衍生特征。
    """
    df = df.sort_values(["symbol", "trade_date"]).copy()

    # 复权因子（adj_factor 可能为 NULL，默认 1.0）
    factor = df.get("adj_factor")
    if factor is None:
        factor = pd.Series(1.0, index=df.index)
    else:
        factor = pd.to_numeric(factor, errors="coerce").fillna(1.0)

    # 复权价格
    adj_close = df["close"] * factor
    adj_high = df["high"] * factor
    adj_low = df["low"] * factor

    # 原始收盘价（用于收益率计算，避免复权因子切换导致口径不一致）
    raw_close = df["close"]

    g = df.groupby("symbol", observed=True)

    # ── 动量因子 ──────────────────────────────────────────────────────
    # 收益率使用原始价格计算
    df["mom_ret_1d"] = _safe_div(raw_close, g["close"].shift(1)) - 1
    df["mom_ret_5d"] = _safe_div(raw_close, g["close"].shift(5)) - 1
    df["mom_ret_20d"] = _safe_div(raw_close, g["close"].shift(20)) - 1
    df["mom_ret_60d"] = _safe_div(raw_close, g["close"].shift(60)) - 1
    df["mom_ret_120d"] = _safe_div(raw_close, g["close"].shift(120)) - 1

    # 均线偏离（使用复权价格）
    for n in (5, 20, 60):
        adj_ma = adj_close.groupby(df["symbol"], observed=True).transform(
            lambda s, w=n: s.rolling(w, min_periods=w).mean()
        )
        df[f"mom_ma_gap_{n}"] = _safe_div(adj_close, adj_ma) - 1

    # MACD
    ema12 = _ema(df, "close", 12)
    ema26 = _ema(df, "close", 26)
    df["mom_macd_dif"] = ema12 - ema26
    df["mom_macd_dea"] = df.groupby("symbol", observed=True)["mom_macd_dif"].transform(
        lambda s: s.ewm(span=9, adjust=False, min_periods=9).mean()
    )
    df["mom_macd_hist"] = 2 * (df["mom_macd_dif"] - df["mom_macd_dea"])

    # RSI(14)
    def _calc_rsi(s, window=14):
        delta = s.diff()
        gains = delta.clip(lower=0)
        losses = -delta.clip(upper=0)
        avg_gain = gains.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        avg_loss = losses.ewm(alpha=1 / window, adjust=False, min_periods=window).mean()
        rs = avg_gain / avg_loss.replace(0, np.nan)
        return 100 - 100 / (1 + rs)

    df["mom_rsi_14"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(_calc_rsi)

    # 20日突破强度
    high20 = adj_high.groupby(df["symbol"], observed=True).transform(
        lambda s: s.rolling(20, min_periods=20).max()
    )
    df["mom_breakout_20d"] = _safe_div(adj_close, high20) - 1

    # ── 波动率因子 ────────────────────────────────────────────────────
    for n in (10, 20, 60):
        df[f"vol_std_{n}"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
            lambda s, w=n: s.rolling(w, min_periods=w).std()
        )

    # ATR(14)
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

    # Parkinson 波动率(20日)
    log_hl = np.log(_safe_div(adj_high, adj_low))
    parkinson_base = (log_hl**2) / (4 * np.log(2))
    df["vol_parkinson_20"] = np.sqrt(
        parkinson_base.groupby(df["symbol"]).transform(
            lambda s: s.rolling(20, min_periods=20).mean()
        )
    )

    # 下行波动率(20日)
    df["vol_downside_20"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: s.clip(upper=0).rolling(20, min_periods=20).std()
    )

    # 已实现波动率(20日) — 日级别近似: sqrt(sum(ret^2))
    df["vol_realized_rv"] = df.groupby("symbol", observed=True)["mom_ret_1d"].transform(
        lambda s: np.sqrt(s.rolling(20, min_periods=20).apply(lambda x: np.sum(x**2), raw=True))
    )

    # ── 流动性因子 ────────────────────────────────────────────────────
    # liq_turnover_tl: 需要总股本数据，无法从 OHLCV 计算 → NaN
    df["liq_turnover_tl"] = np.nan

    # 量比
    vol_ma_5 = _rolling_group(df, "volume", 5, "mean")
    vol_ma_20 = _rolling_group(df, "volume", 20, "mean")
    df["liq_volume_ratio_5"] = _safe_div(df["volume"], vol_ma_5)
    df["liq_volume_ratio_20"] = _safe_div(df["volume"], vol_ma_20)

    # 成交额均线
    df["liq_amount_ma_20"] = _rolling_group(df, "amount", 20, "mean")

    # liq_avg_trade_size: 需要成交笔数，无法计算 → NaN
    df["liq_avg_trade_size"] = np.nan

    # OBV(20日均值)
    sign = np.sign(df["mom_ret_1d"].fillna(0))
    obv = (sign * df["volume"].fillna(0)).groupby(df["symbol"]).cumsum()
    df["liq_obv_20"] = obv.groupby(df["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )

    # Amihud 非流动性指标
    amihud_base = _safe_div(df["mom_ret_1d"].abs(), df["amount"])
    df["liq_amihud_20"] = amihud_base.groupby(df["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).mean()
    )
    df["liq_amihud_60"] = amihud_base.groupby(df["symbol"]).transform(
        lambda s: s.rolling(60, min_periods=60).mean()
    )

    # MFI(14) — 资金流量指标
    typical = (adj_high + adj_low + adj_close) / 3
    money_flow = typical * df["volume"]
    pos_flow = money_flow.where(typical.groupby(df["symbol"]).diff() > 0, 0.0)
    neg_flow = money_flow.where(typical.groupby(df["symbol"]).diff() < 0, 0.0)
    pos14 = pos_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    neg14 = neg_flow.groupby(df["symbol"]).transform(lambda s: s.rolling(14, min_periods=14).sum())
    df["liq_mfi_14"] = 100 - 100 / (1 + _safe_div(pos14, neg14))

    # ── 资金流因子 (需要买卖分类数据，当前不可计算) ──────────────────────
    for col in ("flow_net_amount_ratio", "flow_large_net_ratio",
                "flow_net_order_ratio", "flow_vpin_ma_20"):
        df[col] = np.nan

    # ── 微观结构因子 (需要高频数据，当前不可计算) ──────────────────────
    for col in ("micro_imbalance_volume", "micro_effective_spread",
                "micro_pressure_score", "micro_jump_flag"):
        df[col] = np.nan

    # ── 风格因子 ────────────────────────────────────────────────────
    # 对数市值：优先使用 float_mv，若为空则用 log(amount) 代理
    float_mv = pd.to_numeric(df.get("float_mv"), errors="coerce")
    ln_mv = np.log(float_mv.where(float_mv > 0))
    amount_proxy = np.log(df["amount"].where(df["amount"] > 0))
    df["style_ln_mv_float"] = ln_mv.fillna(amount_proxy)

    # 估值因子：优先读取 bp/ep_ttm，若为空用 1/close 代理
    bp = pd.to_numeric(df.get("bp"), errors="coerce")
    ep_ttm = pd.to_numeric(df.get("ep_ttm"), errors="coerce")
    inv_close = 1.0 / df["close"].where(df["close"] > 0)
    df["style_bp"] = bp.fillna(inv_close)
    df["style_ep_ttm"] = ep_ttm.fillna(inv_close)

    # 市场收益率代理：所有股票等权平均收益率
    mkt_ret = df.groupby("trade_date", observed=True)["mom_ret_1d"].transform("mean")

    # Beta(60日) — 逐 symbol 计算 rolling cov/var
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

    # 特质波动率(60日) 和 残差收益(20日)
    resid = df["mom_ret_1d"] - df["style_beta_60"] * mkt_ret
    df["style_idio_vol_60"] = resid.groupby(df["symbol"]).transform(
        lambda s: s.rolling(60, min_periods=60).std()
    )
    df["style_residual_ret_20"] = resid.groupby(df["symbol"]).transform(
        lambda s: s.rolling(20, min_periods=20).sum()
    )

    # 估值复合因子
    df["style_valuation_composite"] = df[["style_bp", "style_ep_ttm"]].rank(pct=True).mean(axis=1)
    # 规模分位数
    df["style_size_percentile"] = df.groupby("trade_date", observed=True)["style_ln_mv_float"].rank(pct=True)
    # 价值分位数
    df["style_value_percentile"] = df.groupby("trade_date", observed=True)["style_valuation_composite"].rank(pct=True)

    # ── 行业因子 ────────────────────────────────────────────────────
    industry_col = df.get("industry")
    if industry_col is not None and industry_col.notna().any():
        ind_group = ["trade_date", "industry"]
        # 行业 20日收益率均值
        ind_ret_20d = df.groupby(ind_group, observed=True)["mom_ret_20d"].transform("mean")
        # 行业相对成交量
        ind_vol_ma_20 = df.groupby(ind_group, observed=True)["liq_amount_ma_20" if "liq_amount_ma_20" in df else "volume"].transform("mean")
        df["ind_relative_volume_20"] = _safe_div(
            df.get("liq_amount_ma_20", df["volume"]),
            ind_vol_ma_20,
        )
        # 行业相对波动率
        ind_vol_std = df.groupby(ind_group, observed=True)["vol_std_20"].transform("mean")
        df["ind_relative_volatility_20"] = _safe_div(df["vol_std_20"], ind_vol_std)
        # 行业强度
        df["ind_strength_20"] = df["mom_ret_20d"] - ind_ret_20d
        # 行业动量排名
        df["ind_momentum_rank_20"] = ind_ret_20d.groupby(df["trade_date"]).rank(pct=True)
        # 行业价值排名
        df["ind_value_rank"] = df.groupby("trade_date", observed=True)["style_value_percentile"].rank(pct=True)
    else:
        # 无行业数据时，用全市场截面统计作为代理
        mkt_amount_ma_20 = df.groupby("trade_date", observed=True)["liq_amount_ma_20"].transform("mean")
        df["ind_relative_volume_20"] = _safe_div(df["liq_amount_ma_20"], mkt_amount_ma_20)
        mkt_vol_std = df.groupby("trade_date", observed=True)["vol_std_20"].transform("mean")
        df["ind_relative_volatility_20"] = _safe_div(df["vol_std_20"], mkt_vol_std)
        mkt_ret_20d = df.groupby("trade_date", observed=True)["mom_ret_20d"].transform("mean")
        df["ind_strength_20"] = df["mom_ret_20d"] - mkt_ret_20d
        df["ind_momentum_rank_20"] = df.groupby("trade_date", observed=True)["mom_ret_20d"].rank(pct=True)
        df["ind_value_rank"] = df.groupby("trade_date", observed=True)["style_value_percentile"].rank(pct=True)

    # 确保所有 48 个特征列都存在
    for feat in TRAINING_FEATURES:
        if feat not in df.columns:
            df[feat] = np.nan

    return df


def main():
    conn = psycopg2.connect(PG_DSN)
    cur = conn.cursor(cursor_factory=RealDictCursor)

    cols_sql = ", ".join(f'"{c}"' for c in _DB_COLUMNS)
    cur.execute(f"""
        SELECT {cols_sql}
        FROM stock_daily_latest
        ORDER BY symbol, trade_date
    """)
    rows = [dict(r) for r in cur.fetchall()]
    conn.close()

    df = pd.DataFrame(rows)
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # T5.2 入库前校验：股票代码标准化为 SH600000 前缀格式
    df["symbol"] = df["symbol"].astype(str).map(StockCodeUtil.to_prefix)

    print(f"读取 {len(df)} 条记录, {df['symbol'].nunique()} 只标的")

    # 计算因子
    df = compute_factors(df)

    # 输出列：symbol, trade_date + 48 维特征
    output_cols = ["symbol", "trade_date"] + TRAINING_FEATURES
    df = df[output_cols].copy()

    # 过滤B股
    b_mask = df["symbol"].str.match(r"^SH9\d{5}$", na=False) | df["symbol"].str.match(
        r"^SZ2\d{5}$", na=False
    )
    if b_mask.any():
        print(f"[generate] 过滤B股 {b_mask.sum()} 行")
        df = df[~b_mask].copy()

    # 按年保存
    df["year"] = df["trade_date"].dt.year
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    for year, year_df in df.groupby("year"):
        out = OUTPUT_DIR / f"model_features_{year}.parquet"
        year_df = year_df.drop(columns=["year"])

        # 诊断：非零特征统计
        nonzero_stats = {}
        for col in TRAINING_FEATURES:
            if col in year_df.columns:
                non_null = year_df[col].notna().sum()
                non_zero = (year_df[col] != 0).sum()
                nonzero_stats[col] = {
                    "non_null": int(non_null),
                    "non_zero": int(non_zero),
                    "non_zero_pct": round(float(non_zero) / max(len(year_df), 1) * 100, 1),
                }

        non_zero_count = sum(1 for v in nonzero_stats.values() if v["non_zero_pct"] > 0)
        year_df.to_parquet(out, index=False)
        print(
            f"{year}: {year_df.shape}, 非零特征={non_zero_count}/{len(TRAINING_FEATURES)}, "
            f"保存到 {out}"
        )

    print("特征快照生成完成")


if __name__ == "__main__":
    main()
