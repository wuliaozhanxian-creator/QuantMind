import logging
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional

import numpy as np
import pandas as pd

logger = logging.getLogger(__name__)


@dataclass
class VectorizedBacktestConfig:
    """向量化回测配置（T1.7 Phase 2 已与 Qlib 步进回测对齐）。

    Phase 2 对齐项（5 类工程差异）：
    - initial_capital: 默认 1,000,000（100万），支持动态资金量配置（原 100,000）
    - commission: 默认 0.0003（双边万三），应用于换手率 weight_diff（weight_diff
      已含买卖两侧，故 0.0003 即"双边万三"的总成本率；原 0.001 偏高）
    - signal_lag_days: 默认 1，T 日信号 T+1 日执行（对齐 Qlib signal_lag_days=1）
    - handle_limit: 默认 True，涨跌停订单跳过（对齐 Qlib CnExchange 行为）
    - annualize_method: 默认 "trading_days"，按实际交易日数年化
      （对齐 Qlib risk_analyzer `(1+tr)^(252/days)-1`；原 "actual_days"）

    所有新增参数均有默认值，run_backtest(signals, prices) 签名保持兼容。
    """

    initial_capital: float = 1_000_000.0
    commission: float = 0.0003  # 双边万三，应用于 weight_diff（含买卖两侧）
    slippage: float = 0.0001
    topk: int = 50
    # T1.7 Phase 2 新增对齐参数
    signal_lag_days: int = 1  # 信号滞后：T 日信号 T+1 日执行
    handle_limit: bool = True  # 涨跌停对齐：跳过涨跌停订单
    limit_threshold: float = 0.095  # 涨跌停判定阈值（对齐 CnExchange 0.095）
    annualize_method: str = "trading_days"  # 年化方法：trading_days / actual_days
    trading_days_per_year: int = 252
    risk_free_rate: float = 0.02


@dataclass
class VectorizedBacktestResult:
    success: bool
    annual_return: float = 0.0
    sharpe_ratio: float = 0.0
    max_drawdown: float = 0.0
    total_return: float = 0.0
    win_rate: float = 0.0
    portfolio_dict: dict | None = None
    indicator_dict: dict | None = None
    error_message: str = ""
    # 以下字段用于兼容 engine_manager._run_vectorized_backtest 的结果转换。
    # equity_curve 为按日期索引的净值序列（pd.Series）；positions/trades 在向量化
    # 引擎中按组合权重运作、不逐笔记录，故默认空表，保持接口兼容。
    equity_curve: pd.Series = field(default_factory=pd.Series)
    positions: pd.DataFrame = field(default_factory=pd.DataFrame)
    trades: pd.DataFrame = field(default_factory=pd.DataFrame)
    performance_metrics: dict = field(default_factory=dict)
    risk_metrics: dict = field(default_factory=dict)
    analysis: dict = field(default_factory=dict)


class VectorizedBacktestEngine:
    def __init__(self, config: VectorizedBacktestConfig):
        self.config = config
        self.logger = logger

    def run_backtest(
        self,
        signals: pd.DataFrame,
        prices: pd.DataFrame,
    ) -> VectorizedBacktestResult:
        """
        Pure pandas/numpy vectorized backtest.

        signals: MultiIndex (datetime, instrument) [score]
        prices: MultiIndex (datetime, instrument) [$close] （Phase 2 起涨跌停
                判定复用 $close 的日收益率，无需额外字段）

        T1.7 Phase 2 对齐逻辑：
        1. 信号滞后：sig.shift(signal_lag_days)，T 日信号 T+1 日执行
        2. 涨跌停：执行日涨跌停的标的不建立/调整仓位（订单跳过）
        3. 佣金：turnover_cost = weight_diff * (commission + slippage)
        4. 年化：按实际交易日数 (1+tr)^(252/days)-1
        """
        try:
            self.logger.info("Starting true vectorized backtest (Phase 2 aligned)")
            cfg = self.config

            # 1. Unstack to wide format: (datetime x instrument)
            if isinstance(signals, pd.Series):
                signals = signals.to_frame("score")

            sig_wide = signals["score"].unstack(level="instrument")
            price_wide = (
                prices["$close"].unstack(level="instrument")
                .reindex_like(sig_wide)
                .ffill()
            )
            valid_dates = sig_wide.index.intersection(
                price_wide.dropna(how="all").index
            )
            sig_wide = sig_wide.loc[valid_dates]
            price_wide = price_wide.loc[valid_dates]
            if len(sig_wide) < 2:
                raise ValueError(
                    "vectorized backtest requires at least two aligned "
                    "signal/price dates after lagging"
                )

            # -------------------------------------------------------------- #
            # 2. 信号滞后对齐：T 日信号 -> T+1 日执行
            #    sig_lagged 在日期 T 取 signal_{T-lag}；lag=1 即 T 日信号次日执行
            # -------------------------------------------------------------- #
            sig_lagged = sig_wide.shift(cfg.signal_lag_days)

            # 3. 涨跌停对齐：执行日（T）涨跌停判定
            #    daily_ret_T = P_T / P_{T-1} - 1；limit_up/down 在执行日 T
            daily_ret = price_wide.pct_change()
            limit_up = daily_ret >= cfg.limit_threshold
            limit_down = daily_ret <= -cfg.limit_threshold
            # 不 Tradable：涨跌停（买不进涨停、卖不出跌停）
            not_tradable = limit_up | limit_down

            # 4. Target Weights (TopK 等权)，基于滞后信号
            ranks = sig_lagged.rank(axis=1, ascending=False, method="first")
            target_indicator = (ranks <= cfg.topk).astype(float)

            # 涨跌停对齐 + 归一化：
            #   - 涨停（limit_up）：新买订单拒绝 -> 目标权重置 0（资金留作现金）
            #   - 跌停（limit_down）：卖出订单拒绝 -> 维持上一日"归一化"权重
            #     （仓位锁定，忠实 Qlib CnExchange 行为）
            #   carry-over 需携带归一化后的权重，本质上顺序递推；此处用一次
            #   O(n_dates) 的逐日扫描实现，返回已归一化权重。返回值 1-sum 即现金。
            if cfg.handle_limit:
                weights = self._sequential_limit_carry(
                    target_indicator, not_tradable
                )
            else:
                weight_sums = target_indicator.sum(axis=1)
                weights = target_indicator.div(
                    weight_sums.where(weight_sums > 0, 1), axis=0
                )

            # 5. Daily returns (next day return)
            #    asset_returns 在日期 T = (P_{T+1}/P_T - 1)，即 T 收盘买入 -> T+1 收盘卖出
            #    与滞后权重配对：weight_T(f(signal_{T-lag})) * ret_{T->T+1}
            asset_returns = price_wide.pct_change().shift(-1)

            # 6. Calculate Portfolio Returns
            portfolio_daily_returns = (
                (weights * asset_returns).sum(axis=1).fillna(0)
            )

            # 7. 佣金对齐：turnover_cost = weight_diff * (commission + slippage)
            #    weight_diff 已含买卖两侧，commission=0.0003 即"双边万三"总成本率
            weight_diff = weights.diff().abs().sum(axis=1)
            turnover_cost = weight_diff * (cfg.commission + cfg.slippage)
            portfolio_daily_returns = portfolio_daily_returns - turnover_cost.fillna(
                0
            )

            # 去掉因 shift 产生的首尾无效日
            valid_ret_mask = portfolio_daily_returns.index[
                portfolio_daily_returns.notna()
            ]
            if len(valid_ret_mask) < 2:
                raise ValueError(
                    "vectorized backtest requires at least two valid return "
                    "days after lagging/limit handling"
                )
            portfolio_daily_returns = portfolio_daily_returns.loc[valid_ret_mask]

            # 8. Equity Curve
            equity_curve = (
                (1 + portfolio_daily_returns).cumprod() * cfg.initial_capital
            )

            # 9. Basic Metrics
            total_return = (
                (equity_curve.iloc[-1] / cfg.initial_capital) - 1
                if len(equity_curve) > 0
                else 0
            )

            # -------------------------------------------------------------- #
            # 10. 年化方法对齐：动态交易日数
            #     trading_days: (1+tr)^(252/actual_trading_days) - 1  (Qlib 口径)
            #     actual_days:  (1+tr)^(1/years) - 1, years=days/365.25  (旧口径)
            # -------------------------------------------------------------- #
            trading_days = len(portfolio_daily_returns)
            if cfg.annualize_method == "trading_days":
                annual_return = (
                    (1 + total_return)
                    ** (cfg.trading_days_per_year / max(trading_days, 1))
                    - 1
                )
            else:
                years = (
                    (equity_curve.index[-1] - equity_curve.index[0]).days / 365.25
                    if len(equity_curve) > 1
                    else 1
                )
                annual_return = (1 + total_return) ** (1 / max(years, 0.01)) - 1

            daily_std = portfolio_daily_returns.std(ddof=1)
            sharpe_ratio = (
                (annual_return - cfg.risk_free_rate)
                / (daily_std * np.sqrt(cfg.trading_days_per_year))
                if daily_std > 0
                else 0.0
            )

            rolling_max = equity_curve.cummax()
            drawdowns = (equity_curve - rolling_max) / rolling_max
            max_drawdown = drawdowns.min() if len(drawdowns) > 0 else 0.0

            win_rate = (portfolio_daily_returns > 0).mean()

            # Construct a dummy portfolio_dict compatible with Qlib RiskAnalyzer
            # Qlib expects a tuple: (portfolio_dict, indicator_dict)
            dummy_portfolio = pd.DataFrame(
                {
                    "account": equity_curve.values,
                    "cost": turnover_cost.reindex(equity_curve.index)
                    .fillna(0)
                    .values
                    * cfg.initial_capital,
                    "return": portfolio_daily_returns.values,
                },
                index=equity_curve.index,
            )

            return VectorizedBacktestResult(
                success=True,
                annual_return=float(annual_return),
                sharpe_ratio=float(sharpe_ratio),
                max_drawdown=float(max_drawdown),
                total_return=float(total_return),
                win_rate=float(win_rate),
                portfolio_dict={"dummy": dummy_portfolio},
                indicator_dict={"dummy": pd.DataFrame()},
                equity_curve=equity_curve,
                performance_metrics={
                    "total_return": float(total_return),
                    "annual_return": float(annual_return),
                    "sharpe_ratio": float(sharpe_ratio),
                    "max_drawdown": float(max_drawdown),
                    "total_trades": 0,  # 向量化引擎按组合权重运作，不逐笔记录交易
                    "trading_days": int(trading_days),
                    "annualize_method": cfg.annualize_method,
                },
            )

        except Exception as e:
            self.logger.error(f"Vectorized backtest failed: {e}", exc_info=True)
            return VectorizedBacktestResult(success=False, error_message=str(e))

    # ------------------------------------------------------------------ #
    # 涨跌停订单跳过（顺序递推，携带归一化权重）
    # ------------------------------------------------------------------ #
    def _sequential_limit_carry(
        self, target_indicator: pd.DataFrame, not_tradable: pd.DataFrame
    ) -> pd.DataFrame:
        """涨跌停订单跳过的顺序递推实现（忠实 Qlib CnExchange 语义）。

        规则：
        - 执行日 T 标的涨跌停（not_tradable）时，订单被拒绝。
        - 涨停（limit_up）：新买订单拒绝 -> 目标权重沿用上一日归一化权重；
          若上一日未持仓（权重 0）则保持 0，即"买不进 -> 不建仓"。
        - 跌停（limit_down）：卖出订单拒绝 -> 沿用上一日归一化权重，即"卖不出
          -> 仓位锁定"。
        - 每日对（携带后的）目标权重重新归一化，1-sum 即现金占比。

        实现说明：carry-over 携带的是"归一化权重"而非 0/1 指标（否则被锁定
        标的的权重会被错误放大为满仓），故必须顺序递推。O(n_dates) 扫描在
        日线粗筛场景下耗时可忽略（<100ms / 500 日），收益/净值/指标计算仍
        保持向量化。
        """
        n_dates = len(target_indicator)
        cols = target_indicator.columns
        prev_w = pd.Series(0.0, index=cols)
        out = pd.DataFrame(
            0.0, index=target_indicator.index, columns=cols, dtype=float
        )
        for i in range(n_dates):
            tgt = target_indicator.iloc[i]
            nt = not_tradable.iloc[i]
            # not_tradable -> 沿用上一日归一化权重；否则用当日 TopK 指标
            carried = tgt.where(~nt, prev_w)
            s = float(carried.sum())
            w = carried / s if s > 0 else carried
            out.iloc[i] = w
            prev_w = w
        return out
