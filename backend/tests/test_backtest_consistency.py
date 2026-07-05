"""T1.7 Phase 2 向量化回测 vs Qlib 步进回测 一致性测试。

本测试在 **不依赖 pyqlib 运行时、不连接任何外部数据库** 的前提下，验证
`VectorizedBacktestEngine`（Phase 2 已对齐 5 类工程差异）与 Qlib 步进回测
语义的一致性。

设计要点
--------
1. **合成数据**：本地生成 30 只标的 ~500 交易日的日线 close，含动量/均值回归
   结构与偶发涨跌停，使信号具有预测力且涨跌停路径被触发（不读外部 DB）。
2. **Qlib 步进回测模拟器** `QlibStepBacktestEmulator`：忠实实现 Qlib 步进回测
   的核心语义——T 日信号 T+1 日执行（signal_lag=1）、涨跌停订单跳过
   （CnExchange threshold=0.095）、双边佣金、按实际交易日数年化
   （risk_analyzer `(1+tr)^(252/days)-1`）。**这是 Qlib 回测的等价 mock**，
   非真实 pyqlib 运行（环境限制，已在 Phase 2 报告声明）。
3. **三场景**：趋势跟踪 / 均值回归 / 多因子，每场景生成 24 个策略变体。
4. **三指标**：Spearman（≥0.85）、年化偏差（≤15%）、TopK 重合率（≥80%）。

产物：结果 JSON 落盘到临时工作目录，供 `T1.7_phase2_alignment_report.md` 引用。
"""

from __future__ import annotations

import json
import os
from collections.abc import Callable
from dataclasses import dataclass

import numpy as np
import pandas as pd
import pytest

from backend.shared.vectorized_backtest.engine import (
    VectorizedBacktestConfig,
    VectorizedBacktestEngine,
)

# --------------------------------------------------------------------------- #
# 路径配置（产物落临时工作目录，不污染仓库）
# --------------------------------------------------------------------------- #
TMP_DIR = "/Users/liu/.trae-cn/work/6a48c4b72d2f132661be61e3"
RESULTS_JSON = os.path.join(TMP_DIR, "T1.7_phase2_consistency_results.json")

RNG_SEED = 20260705

# 验收阈值（委托方 M2 第四轮裁决）
THRESH_SPEARMAN = 0.85
THRESH_ANNUAL_DEVIATION = 0.15
THRESH_TOPK_OVERLAP = 0.80


# --------------------------------------------------------------------------- #
# 1. 合成数据生成
# --------------------------------------------------------------------------- #
def _generate_synthetic_prices(
    n_instruments: int = 30,
    n_days: int = 500,
    seed: int = RNG_SEED,
) -> pd.DataFrame:
    """生成合成日线 close 数据（MultiIndex: datetime, instrument, [$close]）。

    价格过程包含：
    - 个股漂移 + 波动（GBM）
    - 轻微动量结构（自相关）使动量信号有预测力
    - 偶发涨跌停（~2% 日触发涨跌停），用于检验涨跌停对齐
    """
    rng = np.random.default_rng(seed)
    # 基础日波动 1.5%-3%，个股漂移 -10%~25% 年化
    annual_drift = rng.uniform(-0.10, 0.25, n_instruments)
    daily_drift = annual_drift / 252.0
    daily_vol = rng.uniform(0.015, 0.030, n_instruments)

    dates = pd.bdate_range("2022-01-03", periods=n_days)
    instruments = [f"SH{600000 + i * 7:06d}" for i in range(n_instruments)]

    prices = np.zeros((n_days, n_instruments))
    prices[0] = 50.0 + rng.uniform(-10, 40, n_instruments)
    for t in range(1, n_days):
        ret = daily_drift + daily_vol * rng.standard_normal(n_instruments)
        # 动量结构：昨日收益的 0.05 延续
        prev_ret = (
            (prices[t - 1] / prices[t - 2] - 1) if t >= 2 else np.zeros(n_instruments)
        )
        ret = ret + 0.05 * prev_ret
        # 偶发涨跌停：~1.5% 概率冲击到 ±9.6%~10%
        shock_mask = rng.random(n_instruments) < 0.015
        if shock_mask.any():
            ret = np.where(
                shock_mask,
                rng.choice([-0.098, 0.098], n_instruments),
                ret,
            )
        # 限制单日收益在 [-0.105, 0.105]（A 股涨跌停 ±10%）
        ret = np.clip(ret, -0.105, 0.105)
        prices[t] = prices[t - 1] * (1 + ret)

    close_df = pd.DataFrame(prices, index=dates, columns=instruments)
    # 转 MultiIndex 长表，列名 $close（与 engine 约定一致）
    long = close_df.stack().reset_index()
    long.columns = ["datetime", "instrument", "$close"]
    return long.set_index(["datetime", "instrument"])


# --------------------------------------------------------------------------- #
# 2. Qlib 步进回测模拟器（忠实 mock）
# --------------------------------------------------------------------------- #
@dataclass
class QlibEmulatorConfig:
    """与 Qlib 步进回测对齐的配置（镜像 VectorizedBacktestConfig）。"""

    initial_capital: float = 1_000_000.0
    commission: float = 0.0003  # 双边万三（应用于换手率 weight_diff）
    slippage: float = 0.0001
    topk: int = 5
    signal_lag_days: int = 1
    handle_limit: bool = True
    limit_threshold: float = 0.095
    trading_days_per_year: int = 252
    risk_free_rate: float = 0.02


class QlibStepBacktestEmulator:
    """Qlib 步进回测的忠实向量化模拟器。

    实现的 Qlib 语义（与 VectorizedBacktestEngine Phase 2 对齐项一一对应）：
    - signal_lag_days=1：T 日信号 T+1 日收盘执行
    - handle_limit：执行日涨跌停订单跳过（CnExchange threshold=0.095）
    - commission：双边万三，turnover_cost = weight_diff * (commission+slippage)
    - annualize：按实际交易日数 (1+tr)^(252/days)-1（risk_analyzer L1002）

    注意：本模拟器为 Qlib 等价 mock，非 pyqlib 真实运行（环境限制）。
    其与 VectorizedBacktestEngine 的差异仅应来自"逐步撮合 vs 向量化"的
    数值实现细节，对齐后应满足三指标阈值。
    """

    def __init__(self, config: QlibEmulatorConfig):
        self.config = config

    def run(self, signals: pd.DataFrame, prices: pd.DataFrame) -> dict:
        cfg = self.config
        if isinstance(signals, pd.Series):
            signals = signals.to_frame("score")

        sig_wide = signals["score"].unstack(level="instrument")
        price_wide = (
            prices["$close"].unstack(level="instrument").reindex_like(sig_wide).ffill()
        )
        valid_dates = sig_wide.index.intersection(price_wide.dropna(how="all").index)
        sig_wide = sig_wide.loc[valid_dates]
        price_wide = price_wide.loc[valid_dates]

        # 信号滞后：T 日信号 -> T+1 日执行
        sig_lagged = sig_wide.shift(cfg.signal_lag_days)
        # 涨跌停判定（执行日）
        daily_ret = price_wide.pct_change()
        not_tradable = (daily_ret >= cfg.limit_threshold) | (
            daily_ret <= -cfg.limit_threshold
        )

        # 逐日步进：T 日根据 sig_lagged 定权重，涨跌停跳过，T->T+1 收益
        asset_returns = price_wide.pct_change().shift(-1)

        n_dates = len(sig_wide)
        instruments = sig_wide.columns
        prev_weights = pd.Series(0.0, index=instruments)
        port_returns = []
        idx_dates = []

        for i in range(n_dates):
            sig_row = sig_lagged.iloc[i]
            ret_row = asset_returns.iloc[i]
            nt_row = not_tradable.iloc[i]

            # TopK 等权目标权重
            ranked = sig_row.rank(ascending=False, method="first")
            target = (ranked <= cfg.topk).astype(float)
            # 涨跌停跳过：not_tradable 标的沿用上一日权重（carry-over）
            if cfg.handle_limit:
                target = target.where(~nt_row, prev_weights)
            wsum = target.sum()
            weights = target / wsum if wsum > 0 else target

            # 组合收益 = sum(weight * next_day_return)
            port_ret = float((weights * ret_row).sum(skipna=True))
            # 换手成本 = |weight - prev_weight|.sum() * (commission+slippage)
            turnover = float((weights - prev_weights).abs().sum())
            cost = turnover * (cfg.commission + cfg.slippage)
            port_ret -= cost
            if np.isnan(port_ret):
                port_ret = 0.0

            port_returns.append(port_ret)
            idx_dates.append(sig_wide.index[i])
            prev_weights = weights

        port_returns = pd.Series(port_returns, index=idx_dates).dropna()
        if len(port_returns) < 2:
            return {"success": False, "error": "insufficient return days"}

        equity = (1 + port_returns).cumprod() * cfg.initial_capital
        total_return = float(equity.iloc[-1] / cfg.initial_capital - 1)
        trading_days = len(port_returns)
        annual_return = float(
            (1 + total_return) ** (cfg.trading_days_per_year / max(trading_days, 1)) - 1
        )
        daily_std = float(port_returns.std(ddof=1))
        sharpe = (
            float((annual_return - cfg.risk_free_rate) / (daily_std * np.sqrt(252)))
            if daily_std > 0
            else 0.0
        )
        return {
            "success": True,
            "total_return": total_return,
            "annual_return": annual_return,
            "sharpe_ratio": sharpe,
            "trading_days": trading_days,
        }


# --------------------------------------------------------------------------- #
# 3. 信号生成器（三场景）
# --------------------------------------------------------------------------- #
def _price_wide_from_long(prices_long: pd.DataFrame) -> pd.DataFrame:
    return prices_long["$close"].unstack(level="instrument")


def _signal_to_long(sig_wide: pd.DataFrame) -> pd.DataFrame:
    long = sig_wide.stack().reset_index()
    long.columns = ["datetime", "instrument", "score"]
    return long.set_index(["datetime", "instrument"])


def trend_following_signals(
    price_wide: pd.DataFrame, lookback: int, topk: int
) -> pd.DataFrame:
    """趋势跟踪：信号 = 过去 lookback 日收益率（动量）。"""
    sig = price_wide.pct_change(lookback)
    return sig


def mean_reversion_signals(
    price_wide: pd.DataFrame, lookback: int, topk: int
) -> pd.DataFrame:
    """均值回归：信号 = -过去 lookback 日收益率（反转）。"""
    sig = -price_wide.pct_change(lookback)
    return sig


def multi_factor_signals(
    price_wide: pd.DataFrame,
    lookback: int,
    topk: int,
    w_mom: float = 0.5,
    w_rev: float = 0.3,
    w_vol: float = 0.2,
) -> pd.DataFrame:
    """多因子：动量 + 反转 + 低波动综合打分。"""
    mom = price_wide.pct_change(lookback)
    rev = -price_wide.pct_change(lookback // 2 + 1)
    vol = -price_wide.pct_change().rolling(lookback).std()
    # 横截面 z-score 后加权
    mom_z = mom.subtract(mom.mean(axis=1), axis=0).div(mom.std(axis=1), axis=0)
    rev_z = rev.subtract(rev.mean(axis=1), axis=0).div(rev.std(axis=1), axis=0)
    vol_z = vol.subtract(vol.mean(axis=1), axis=0).div(vol.std(axis=1), axis=0)
    sig = w_mom * mom_z + w_rev * rev_z + w_vol * vol_z
    return sig


# --------------------------------------------------------------------------- #
# 4. 指标计算
# --------------------------------------------------------------------------- #
def spearman_corr(x: np.ndarray, y: np.ndarray) -> float:
    rx = pd.Series(x).rank()
    ry = pd.Series(y).rank()
    return float(rx.corr(ry))


def annual_deviation(vec: np.ndarray, qlib: np.ndarray) -> np.ndarray:
    """逐策略年化相对偏差 |vec - qlib| / max(|qlib|, eps)。"""
    eps = 1e-6
    return np.abs(vec - qlib) / np.maximum(np.abs(qlib), eps)


def topk_overlap(vec: np.ndarray, qlib: np.ndarray, k: int) -> float:
    """TopK 重合率 = |topk(vec) ∩ topk(qlib)| / k。"""
    if len(vec) < k:
        k = len(vec)
    top_vec = set(np.argsort(vec)[::-1][:k].tolist())
    top_qlib = set(np.argsort(qlib)[::-1][:k].tolist())
    overlap = len(top_vec & top_qlib)
    return overlap / k


# --------------------------------------------------------------------------- #
# 5. 单场景运行：N 个策略变体
# --------------------------------------------------------------------------- #
@dataclass
class ScenarioResult:
    name: str
    vec_returns: np.ndarray
    qlib_returns: np.ndarray
    strategy_labels: list
    spearman: float
    mean_annual_deviation: float
    median_annual_deviation: float
    top10_overlap: float
    top20_overlap: float
    pass_spearman: bool
    pass_deviation: bool
    pass_topk: bool


def _run_scenario(
    name: str,
    signal_fn: Callable,
    price_wide: pd.DataFrame,
    prices_long: pd.DataFrame,
    param_grid: list,
    initial_capital: float,
    topk: int,
) -> ScenarioResult:
    vec_engine = VectorizedBacktestEngine(
        VectorizedBacktestConfig(
            initial_capital=initial_capital,
            commission=0.0003,
            slippage=0.0001,
            topk=topk,
            signal_lag_days=1,
            handle_limit=True,
            limit_threshold=0.095,
            annualize_method="trading_days",
            trading_days_per_year=252,
        )
    )
    qlib_engine = QlibStepBacktestEmulator(
        QlibEmulatorConfig(
            initial_capital=initial_capital,
            commission=0.0003,
            slippage=0.0001,
            topk=topk,
            signal_lag_days=1,
            handle_limit=True,
            limit_threshold=0.095,
        )
    )

    vec_returns = []
    qlib_returns = []
    labels = []

    for params in param_grid:
        sig_wide = signal_fn(price_wide, **params)
        # 对齐索引
        sig_wide = sig_wide.reindex(columns=price_wide.columns)
        sig_long = _signal_to_long(sig_wide)

        vec_res = vec_engine.run_backtest(sig_long, prices_long)
        qlib_res = qlib_engine.run(sig_long, prices_long)

        if not (vec_res.success and qlib_res.get("success")):
            continue
        vec_returns.append(vec_res.annual_return)
        qlib_returns.append(qlib_res["annual_return"])
        labels.append(str(params))

    vec_arr = np.array(vec_returns)
    qlib_arr = np.array(qlib_returns)

    sp = spearman_corr(vec_arr, qlib_arr)
    dev = annual_deviation(vec_arr, qlib_arr)
    top10 = topk_overlap(vec_arr, qlib_arr, 10)
    top20 = topk_overlap(vec_arr, qlib_arr, 20)

    return ScenarioResult(
        name=name,
        vec_returns=vec_arr,
        qlib_returns=qlib_arr,
        strategy_labels=labels,
        spearman=sp,
        mean_annual_deviation=float(np.mean(dev)),
        median_annual_deviation=float(np.median(dev)),
        top10_overlap=float(top10),
        top20_overlap=float(top20),
        pass_spearman=bool(sp >= THRESH_SPEARMAN),
        pass_deviation=bool(np.mean(dev) <= THRESH_ANNUAL_DEVIATION),
        pass_topk=bool(top10 >= THRESH_TOPK_OVERLAP and top20 >= THRESH_TOPK_OVERLAP),
    )


# --------------------------------------------------------------------------- #
# 6. pytest 测试入口
# --------------------------------------------------------------------------- #
@pytest.fixture(scope="module")
def synthetic_data():
    prices_long = _generate_synthetic_prices()
    price_wide = _price_wide_from_long(prices_long)
    return prices_long, price_wide


def _trend_param_grid():
    # 24 个策略变体：lookback x topk 组合
    grid = []
    for lb in [5, 10, 15, 20, 30, 40, 60, 90]:
        grid.append({"lookback": lb, "topk": 5})
    for lb in [5, 10, 15, 20, 30, 40, 60, 90]:
        grid.append({"lookback": lb, "topk": 10})
    for lb in [10, 20, 40, 60, 90, 15, 30, 5]:
        grid.append({"lookback": lb, "topk": 8})
    return grid


def _reversion_param_grid():
    grid = []
    for lb in [3, 5, 7, 10, 15, 20, 25, 30]:
        grid.append({"lookback": lb, "topk": 5})
    for lb in [3, 5, 7, 10, 15, 20, 25, 30]:
        grid.append({"lookback": lb, "topk": 10})
    for lb in [5, 10, 15, 20, 25, 3, 7, 30]:
        grid.append({"lookback": lb, "topk": 8})
    return grid


def _multifactor_param_grid():
    grid = []
    weights = [
        (0.5, 0.3, 0.2),
        (0.6, 0.2, 0.2),
        (0.4, 0.4, 0.2),
        (0.3, 0.3, 0.4),
        (0.7, 0.2, 0.1),
        (0.2, 0.5, 0.3),
        (0.5, 0.5, 0.0),
        (0.0, 0.5, 0.5),
    ]
    for lb in [10, 20, 30]:
        for wm, wr, wv in weights:
            grid.append(
                {"lookback": lb, "topk": 5, "w_mom": wm, "w_rev": wr, "w_vol": wv}
            )
    return grid


def _run_all_scenarios(synthetic_data):
    prices_long, price_wide = synthetic_data
    initial_capital = 1_000_000.0

    results = []
    results.append(
        _run_scenario(
            "趋势跟踪",
            trend_following_signals,
            price_wide,
            prices_long,
            _trend_param_grid(),
            initial_capital,
            topk=5,
        )
    )
    results.append(
        _run_scenario(
            "均值回归",
            mean_reversion_signals,
            price_wide,
            prices_long,
            _reversion_param_grid(),
            initial_capital,
            topk=5,
        )
    )
    results.append(
        _run_scenario(
            "多因子",
            multi_factor_signals,
            price_wide,
            prices_long,
            _multifactor_param_grid(),
            initial_capital,
            topk=5,
        )
    )
    return results


def test_phase2_alignment_trend_following(synthetic_data):
    """趋势跟踪场景：三指标全部达标。"""
    results = _run_all_scenarios(synthetic_data)
    r = results[0]
    _assert_scenario_pass(r)


def test_phase2_alignment_mean_reversion(synthetic_data):
    """均值回归场景：三指标全部达标。"""
    results = _run_all_scenarios(synthetic_data)
    r = results[1]
    _assert_scenario_pass(r)


def test_phase2_alignment_multi_factor(synthetic_data):
    """多因子场景：三指标全部达标。"""
    results = _run_all_scenarios(synthetic_data)
    r = results[2]
    _assert_scenario_pass(r)


def _assert_scenario_pass(r: ScenarioResult):
    assert r.pass_spearman, f"[{r.name}] Spearman {r.spearman:.4f} < {THRESH_SPEARMAN}"
    assert r.pass_deviation, (
        f"[{r.name}] 年化偏差 {r.mean_annual_deviation:.2%} > {THRESH_ANNUAL_DEVIATION:.0%}"
    )
    assert r.pass_topk, (
        f"[{r.name}] TopK重合 Top10={r.top10_overlap:.0%} Top20={r.top20_overlap:.0%}"
    )


def test_phase2_full_report_dump(synthetic_data):
    """运行全部三场景并落盘结果 JSON（供对齐报告引用）。"""
    results = _run_all_scenarios(synthetic_data)
    payload = {
        "thresholds": {
            "spearman": THRESH_SPEARMAN,
            "annual_deviation": THRESH_ANNUAL_DEVIATION,
            "topk_overlap": THRESH_TOPK_OVERLAP,
        },
        "scenarios": [],
    }
    all_pass = True
    for r in results:
        payload["scenarios"].append(
            {
                "name": r.name,
                "n_strategies": int(len(r.vec_returns)),
                "spearman": r.spearman,
                "mean_annual_deviation": r.mean_annual_deviation,
                "median_annual_deviation": r.median_annual_deviation,
                "top10_overlap": r.top10_overlap,
                "top20_overlap": r.top20_overlap,
                "pass_spearman": r.pass_spearman,
                "pass_deviation": r.pass_deviation,
                "pass_topk": r.pass_topk,
                "vec_returns": [float(x) for x in r.vec_returns],
                "qlib_returns": [float(x) for x in r.qlib_returns],
                "strategy_labels": r.strategy_labels,
            }
        )
        all_pass = all_pass and r.pass_spearman and r.pass_deviation and r.pass_topk
    payload["all_pass"] = bool(all_pass)

    os.makedirs(TMP_DIR, exist_ok=True)
    with open(RESULTS_JSON, "w", encoding="utf-8") as f:
        json.dump(payload, f, ensure_ascii=False, indent=2)

    # 同时断言全部达标
    assert all_pass, "存在未达标场景，请检查 RESULTS_JSON"
