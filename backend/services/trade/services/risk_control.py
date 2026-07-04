"""
可配置风控规则引擎 (T4.2)

将原本散布在 risk_service.py / risk_gate.py / trade_config.py 中的硬编码风控规则
抽取为统一的、可配置的、支持热加载的规则引擎。

核心组件
--------
- ``RiskRuleConfig``      : 规则配置 dataclass (与 ORM ``RiskRule`` 区分，专用于引擎层)
- ``RiskCheckResult``     : 单次检查结果
- ``RuleType`` / ``RuleAction`` : 规则类型与处置动作枚举
- ``RiskControlEngine``   : 规则引擎，按 priority 逐条检查，命中 REJECT 立即返回
- ``RiskRuleLoader``      : 配置加载器，支持 YAML/JSON，基于 mtime 实现热加载
- ``RiskAuditCallback``   : 审计日志回调协议，命中 REJECT/WARN 时触发

设计原则
--------
1. 引擎本身保持纯逻辑、无 DB / Redis 依赖，便于单测；
   审计落库通过注入 ``audit_callback`` 实现，解耦持久化。
2. 与 T4.1 已有的 ``RiskService`` / ORM ``RiskRule`` 共存：
   - ORM ``RiskRule`` 仍作为 DB 持久化的规则源 (CRUD via API)；
   - 本引擎可同时从配置文件与 ORM 规则加载，合并为统一规则集。
3. 股票代码遵循 AGENTS.md 强制约定：``SH600000`` 前缀格式。
"""

from __future__ import annotations

import json
import logging
import os
import threading
from dataclasses import dataclass, field
from enum import Enum
from pathlib import Path
from typing import Any, Optional, Union
from collections.abc import Awaitable, Callable

logger = logging.getLogger(__name__)


# ─── 枚举定义 ─────────────────────────────────────────────────────────────────


class RuleType(str, Enum):
    """风控规则类型 (T4.2 要求的 5 类 + 扩展预留)"""

    PRICE_LIMIT = "PRICE_LIMIT"              # 涨跌停价格检查
    POSITION_LIMIT = "POSITION_LIMIT"        # 持仓上限检查 (单股 + 总仓位)
    ORDER_LIMIT = "ORDER_LIMIT"              # 单笔下单金额/数量限额
    FREQUENCY_LIMIT = "FREQUENCY_LIMIT"      # 日内交易频次限制
    CAPITAL_CHECK = "CAPITAL_CHECK"          # 可用资金/持仓检查


class RuleAction(str, Enum):
    """规则命中后的处置动作"""

    REJECT = "REJECT"                        # 立即拒绝，阻断订单
    WARN = "WARN"                            # 警告，继续检查后续规则
    REQUIRE_CONFIRMATION = "REQUIRE_CONFIRMATION"  # 需人工确认
    PASS = "PASS"                            # 通过 (仅用于结果)


# ─── 数据结构 ─────────────────────────────────────────────────────────────────


@dataclass
class RiskRuleConfig:
    """
    规则配置 (引擎层 dataclass)。

    注意：命名为 ``RiskRuleConfig`` 而非 ``RiskRule``，以避免与
    ``backend/services/trade/models/risk_rule.py`` 中的 ORM 模型 ``RiskRule``
    发生命名冲突。两者可通过 ``RiskRuleLoader.from_orm_rules`` 互转。
    """

    rule_id: str                             # 唯一标识，如 "price_limit_main"
    rule_type: str | RuleType          # 规则类型
    enabled: bool = True                     # 是否启用
    params: dict[str, Any] = field(default_factory=dict)  # 规则参数
    action: str | RuleAction = RuleAction.REJECT     # 处置动作
    priority: int = 100                      # 数字越小优先级越高，越早执行
    description: str = ""                    # 规则描述

    def __post_init__(self) -> None:
        # 归一化类型为字符串大写，便于 dispatch
        self.rule_type = str(
            getattr(self.rule_type, "value", self.rule_type)
        ).upper()
        self.action = str(getattr(self.action, "value", self.action)).upper()

    @classmethod
    def from_dict(cls, raw: dict[str, Any]) -> RiskRuleConfig:
        """从 dict (YAML/JSON 反序列化结果) 构造规则配置。"""
        return cls(
            rule_id=str(raw["rule_id"]),
            rule_type=raw.get("rule_type", ""),
            enabled=bool(raw.get("enabled", True)),
            params=dict(raw.get("params", {}) or {}),
            action=raw.get("action", RuleAction.REJECT),
            priority=int(raw.get("priority", 100)),
            description=str(raw.get("description", "")),
        )

    @classmethod
    def from_orm(cls, orm_rule: Any) -> RiskRuleConfig:
        """
        从 ORM ``RiskRule`` (T4.1 模型) 转换为引擎层配置。

        兼容 ORM 的字段命名：``rule_name`` -> ``rule_id``、
        ``is_active`` -> ``enabled``、``parameters`` -> ``params``。
        ORM 默认 action 缺省为 REJECT。
        """
        params = dict(getattr(orm_rule, "parameters", {}) or {})
        # ORM rule_type 使用小写语义类型 (max_order_size 等)，引擎侧统一映射
        raw_type = str(getattr(orm_rule, "rule_type", "") or "").lower()
        mapped = _map_orm_rule_type(raw_type, params)
        return cls(
            rule_id=str(getattr(orm_rule, "rule_name", "") or f"orm_{id(orm_rule)}"),
            rule_type=mapped,
            enabled=bool(getattr(orm_rule, "is_active", True)),
            params=params,
            action=RuleAction.REJECT,
            priority=int(getattr(orm_rule, "priority", 100)),
            description=str(getattr(orm_rule, "description", "") or ""),
        )


def _map_orm_rule_type(raw_type: str, params: dict[str, Any]) -> str:
    """将 ORM 旧式 rule_type (max_order_size 等) 映射为引擎标准 RuleType。"""
    mapping = {
        "max_order_size": RuleType.ORDER_LIMIT.value,
        "min_order_size": RuleType.ORDER_LIMIT.value,
        "max_position_size": RuleType.POSITION_LIMIT.value,
        "max_daily_trades": RuleType.FREQUENCY_LIMIT.value,
    }
    return mapping.get(raw_type, raw_type.upper() or RuleType.ORDER_LIMIT.value)


@dataclass
class RiskCheckResult:
    """单次风控检查结果。"""

    action: str = RuleAction.PASS.value       # PASS / REJECT / WARN / REQUIRE_CONFIRMATION
    rule_id: str | None = None             # 命中的规则 ID (PASS 时为 None)
    rule_type: str | None = None           # 命中的规则类型
    message: str = ""                         # 人可读说明
    details: dict[str, Any] = field(default_factory=dict)  # 命中详情 (数值等)
    violations: list[dict[str, Any]] = field(default_factory=list)  # 累积违规列表

    @property
    def passed(self) -> bool:
        return self.action == RuleAction.PASS.value

    @classmethod
    def pass_result(cls) -> RiskCheckResult:
        return cls(action=RuleAction.PASS.value, message="All risk rules passed")

    @classmethod
    def reject(
        cls, rule: RiskRuleConfig, message: str, details: dict[str, Any] | None = None
    ) -> RiskCheckResult:
        return cls(
            action=RuleAction.REJECT.value,
            rule_id=rule.rule_id,
            rule_type=str(rule.rule_type),
            message=message,
            details=details or {},
            violations=[
                {
                    "rule": rule.rule_id,
                    "rule_type": str(rule.rule_type),
                    "message": message,
                    "details": details or {},
                }
            ],
        )

    @classmethod
    def warn(
        cls, rule: RiskRuleConfig, message: str, details: dict[str, Any] | None = None
    ) -> RiskCheckResult:
        return cls(
            action=RuleAction.WARN.value,
            rule_id=rule.rule_id,
            rule_type=str(rule.rule_type),
            message=message,
            details=details or {},
            violations=[
                {
                    "rule": rule.rule_id,
                    "rule_type": str(rule.rule_type),
                    "message": message,
                    "details": details or {},
                }
            ],
        )


# 审计日志回调协议：async (event_type, rule, order_info, details) -> None
RiskAuditCallback = Callable[[str, RiskRuleConfig, dict[str, Any], dict[str, Any]], Awaitable[None]]


# ─── 板块识别工具 ─────────────────────────────────────────────────────────────


def detect_board(symbol: str) -> str:
    """
    识别 A 股板块，返回 ``MAIN`` / ``GEM`` / ``STAR`` / ``BJ``。

    输入支持 AGENTS.md 强制的前缀格式 (``SH600000`` / ``SZ000001``)，
    同时兼容后缀格式 (``600000.SH``) 与纯数字代码。
    """
    s = str(symbol or "").strip().upper()
    if not s:
        return "MAIN"
    # 提取 6 位数字代码
    code = s
    if "." in s:
        code = s.split(".", 1)[0]
    elif len(s) == 8 and s[:2].isalpha():
        code = s[2:]
    code = "".join(ch for ch in code if ch.isdigit())

    if code.startswith("688"):
        return "STAR"        # 科创板 ±20%
    if code.startswith("30"):
        return "GEM"         # 创业板 ±20%
    if code.startswith(("4", "8")) and not code.startswith("83"):
        # 北交所：4xx/8xx (83 段部分仍归属北交所，统一按 BJ 处理)
        return "BJ"          # 北交所 ±30%
    if code.startswith("8") or code.startswith("9"):
        return "BJ"
    return "MAIN"            # 主板 ±10%


def compute_limit_prices(prev_close: float, board: str, params: dict[str, Any]) -> tuple[float, float]:
    """
    基于昨收价与板块涨跌停比例，计算涨停价 / 跌停价。

    params 支持键：``main_board_pct`` / ``gem_board_pct`` / ``star_board_pct``
    / ``bj_board_pct`` / ``st_pct``，缺省值与 trade_config 默认对齐。
    """
    pct_map = {
        "MAIN": float(params.get("main_board_pct", 0.10)),
        "GEM": float(params.get("gem_board_pct", 0.20)),
        "STAR": float(params.get("star_board_pct", 0.20)),
        "BJ": float(params.get("bj_board_pct", 0.30)),
    }
    pct = pct_map.get(board, pct_map["MAIN"])
    # ST 股票由调用方在 params 中传 st_pct 覆盖 (params["is_st"]=True 时)
    if bool(params.get("is_st", False)):
        pct = float(params.get("st_pct", 0.05))
    limit_up = round(prev_close * (1.0 + pct), 2)
    limit_down = round(prev_close * (1.0 - pct), 2)
    return limit_up, limit_down


# ─── 订单/账户字段访问辅助 ─────────────────────────────────────────────────────


def _get(obj: Any, key: str, default: Any = None) -> Any:
    """兼容 ORM Order 对象 (属性) 与 dict 的字段读取。"""
    if obj is None:
        return default
    if isinstance(obj, dict):
        return obj.get(key, default)
    # 优先属性，其次 dict-style (SQLAlchemy Column 不支持 []，但部分 dataclass 可能支持)
    val = getattr(obj, key, None)
    if val is not None:
        return val
    try:
        return obj[key]
    except Exception:
        return default


def _get_float(obj: Any, key: str, default: float = 0.0) -> float:
    val = _get(obj, key, default)
    try:
        return float(val) if val is not None else default
    except (TypeError, ValueError):
        return default


def _get_str(obj: Any, key: str, default: str = "") -> str:
    val = _get(obj, key, default)
    if val is None:
        return default
    # 兼容 Enum (OrderSide.BUY 等)
    return str(getattr(val, "value", val) or default)


def _build_order_info(order: Any, context: dict[str, Any]) -> dict[str, Any]:
    """构造审计日志用的订单摘要。"""
    return {
        "order_id": _get_str(order, "order_id", "") or _get_str(order, "id", ""),
        "symbol": _get_str(order, "symbol", ""),
        "side": _get_str(order, "side", ""),
        "order_type": _get_str(order, "order_type", ""),
        "trade_action": _get_str(order, "trade_action", ""),
        "price": _get_float(order, "price", 0.0),
        "quantity": _get_float(order, "quantity", 0.0),
        "order_value": _get_float(order, "order_value", 0.0),
        "tenant_id": str(context.get("tenant_id", "")),
        "user_id": str(context.get("user_id", "")),
    }


# ─── 规则引擎 ─────────────────────────────────────────────────────────────────


class RiskControlEngine:
    """
    可配置风控规则引擎。

    用法
    ----
        loader = RiskRuleLoader("config/risk_rules.yaml")
        rules = loader.load()
        engine = RiskControlEngine(rules, audit_callback=audit_service.log_rule_hit)
        result = await engine.check(order, account, context)
        if result.action == "REJECT":
            ...  # 阻断订单

    规则按 ``priority`` 升序执行 (数字越小越早)；命中 ``REJECT`` 立即返回。
    ``WARN`` 记录违规但继续后续规则；最终若仅有 WARN 则返回 WARN。
    """

    def __init__(
        self,
        rules: list[RiskRuleConfig],
        audit_callback: RiskAuditCallback | None = None,
    ) -> None:
        self._rules: list[RiskRuleConfig] = sorted(
            rules, key=lambda r: (r.priority, r.rule_id)
        )
        self._audit_callback = audit_callback
        self._lock = threading.RLock()

    # ── 规则管理 ──────────────────────────────────────────────────────────

    @property
    def rules(self) -> list[RiskRuleConfig]:
        with self._lock:
            return list(self._rules)

    def reload(self, rules: list[RiskRuleConfig]) -> None:
        """热加载新规则集 (由 RiskRuleLoader.check_and_reload 调用)。"""
        with self._lock:
            old_ids = {r.rule_id for r in self._rules}
            new_ids = {r.rule_id for r in rules}
            self._rules = sorted(rules, key=lambda r: (r.priority, r.rule_id))
        added = new_ids - old_ids
        removed = old_ids - new_ids
        logger.info(
            "[RiskEngine] reload complete: total=%d, added=%s, removed=%s",
            len(self._rules),
            sorted(added),
            sorted(removed),
        )

    def get_rule(self, rule_id: str) -> RiskRuleConfig | None:
        with self._lock:
            for r in self._rules:
                if r.rule_id == rule_id:
                    return r
        return None

    # ── 核心检查 ──────────────────────────────────────────────────────────

    async def check(
        self,
        order: Any,
        account: dict[str, Any] | None = None,
        context: dict[str, Any] | None = None,
    ) -> RiskCheckResult:
        """
        逐条检查规则。

        参数
        ----
        order   : Order ORM 对象或 dict，需含 symbol/side/price/quantity/order_value
        account : dict，含 portfolio_value/available_cash/available_position/
                  current_position_value/total_position_value
        context : dict，含 prev_close/daily_trade_count/symbol_trade_count/
                  tenant_id/user_id 等
        """
        account = account or {}
        context = context or {}
        order_info = _build_order_info(order, context)

        accumulated_violations: list[dict[str, Any]] = []
        warn_rule: RiskRuleConfig | None = None
        warn_message = ""
        warn_details: dict[str, Any] = {}

        with self._lock:
            rules_snapshot = list(self._rules)

        for rule in rules_snapshot:
            if not rule.enabled:
                continue
            try:
                hit, message, details = await self._apply_rule(rule, order, account, context)
            except Exception as exc:  # 单条规则异常不阻断整体检查
                logger.exception(
                    "[RiskEngine] rule %s raised exception: %s", rule.rule_id, exc
                )
                hit, message, details = True, f"rule_internal_error: {exc}", {
                    "rule_id": rule.rule_id,
                    "exception": str(exc),
                }

            if not hit:
                continue

            violation = {
                "rule": rule.rule_id,
                "rule_type": str(rule.rule_type),
                "action": rule.action,
                "message": message,
                "details": details,
            }
            accumulated_violations.append(violation)

            # 审计回调 (REJECT / WARN / REQUIRE_CONFIRMATION 均记录)
            if self._audit_callback is not None:
                try:
                    await self._audit_callback(
                        rule.action, rule, order_info, {**details, "message": message}
                    )
                except Exception as audit_exc:
                    logger.warning(
                        "[RiskEngine] audit_callback failed for %s: %s",
                        rule.rule_id,
                        audit_exc,
                    )

            if rule.action == RuleAction.REJECT.value:
                # 立即拒绝，携带累积违规清单
                result = RiskCheckResult(
                    action=RuleAction.REJECT.value,
                    rule_id=rule.rule_id,
                    rule_type=str(rule.rule_type),
                    message=message,
                    details=details,
                    violations=accumulated_violations,
                )
                logger.warning(
                    "[RiskEngine] order REJECTED by rule=%s: %s",
                    rule.rule_id,
                    message,
                )
                return result

            if rule.action == RuleAction.WARN.value and warn_rule is None:
                warn_rule = rule
                warn_message = message
                warn_details = details

            if rule.action == RuleAction.REQUIRE_CONFIRMATION.value and warn_rule is None:
                warn_rule = rule
                warn_message = message
                warn_details = details

        if warn_rule is not None:
            return RiskCheckResult(
                action=warn_rule.action,
                rule_id=warn_rule.rule_id,
                rule_type=str(warn_rule.rule_type),
                message=warn_message,
                details=warn_details,
                violations=accumulated_violations,
            )

        return RiskCheckResult.pass_result()

    async def _apply_rule(
        self,
        rule: RiskRuleConfig,
        order: Any,
        account: dict[str, Any],
        context: dict[str, Any],
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        分发到对应规则处理器。

        返回 (hit: bool, message: str, details: dict)。
        hit=True 表示命中 (违规)，由调用方根据 rule.action 决定处置。
        """
        handler = self._HANDLERS.get(str(rule.rule_type))
        if handler is None:
            # 未识别的规则类型跳过 (不阻断)
            logger.debug("[RiskEngine] no handler for rule_type=%s, skip", rule.rule_type)
            return False, "", {}
        return await handler(self, rule, order, account, context)

    # ── 规则处理器 ────────────────────────────────────────────────────────

    async def _check_price_limit(
        self,
        rule: RiskRuleConfig,
        order: Any,
        account: dict[str, Any],
        context: dict[str, Any],
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        PRICE_LIMIT : 涨跌停价格检查。

        依赖 context["prev_close"] (昨收价)。若无昨收价则跳过 (不阻断)。
        检查 order.price 是否落在 [limit_down, limit_up] 区间内：
          - 买单价格高于涨停价 -> 拒绝 (买不到)
          - 卖单价格低于跌停价 -> 拒绝 (卖不出)
          - 价格超出区间 -> 拒绝
        """
        prev_close = float(context.get("prev_close") or 0.0)
        if prev_close <= 0:
            return False, "", {"skipped": "prev_close missing"}

        price = _get_float(order, "price", 0.0)
        if price <= 0:
            return False, "", {"skipped": "price missing (market order)"}

        symbol = _get_str(order, "symbol", "")
        board = detect_board(symbol)
        limit_up, limit_down = compute_limit_prices(prev_close, board, rule.params)
        side = _get_str(order, "side", "").lower()

        details = {
            "symbol": symbol,
            "board": board,
            "prev_close": prev_close,
            "price": price,
            "limit_up": limit_up,
            "limit_down": limit_down,
            "side": side,
        }

        # 价格超出涨跌停区间
        if price > limit_up + 1e-9:
            return True, (
                f"价格 {price:.2f} 超过涨停价 {limit_up:.2f} (板块={board}, 昨收={prev_close:.2f})"
            ), details
        if price < limit_down - 1e-9:
            return True, (
                f"价格 {price:.2f} 低于跌停价 {limit_down:.2f} (板块={board}, 昨收={prev_close:.2f})"
            ), details

        # 已涨停时禁止买入、已跌停时禁止卖出 (避免无效委托)
        # 通过 params["block_near_limit"]=true 开启，near_limit_tolerance 控制贴近容差
        block_near_limit = bool(rule.params.get("block_near_limit", False))
        tolerance = float(rule.params.get("near_limit_tolerance", 0.01))
        if block_near_limit and side == "buy" and abs(price - limit_up) <= tolerance:
            return True, f"标的已涨停 (价格={price:.2f}≈涨停价{limit_up:.2f})，禁止买入", details
        if block_near_limit and side == "sell" and abs(price - limit_down) <= tolerance:
            return True, f"标的已跌停 (价格={price:.2f}≈跌停价{limit_down:.2f})，禁止卖出", details

        return False, "", details

    async def _check_position_limit(
        self,
        rule: RiskRuleConfig,
        order: Any,
        account: dict[str, Any],
        context: dict[str, Any],
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        POSITION_LIMIT : 持仓上限检查。

        params:
          max_single_position_pct : 单股持仓占总资产上限 (默认 0.10)
          max_total_position_pct  : 总仓位占总资产上限 (默认 1.0)
        account:
          portfolio_value          : 总资产
          current_position_value   : 该股当前持仓市值
          total_position_value     : 当前总持仓市值
        """
        portfolio_value = float(account.get("portfolio_value", 0.0) or 0.0)
        if portfolio_value <= 0:
            return False, "", {"skipped": "portfolio_value missing"}

        order_value = _get_float(order, "order_value", 0.0)
        side = _get_str(order, "side", "").lower()
        # 仅买单约束持仓上限 (卖出是减仓)
        if side != "buy":
            return False, "", {"skipped": "sell order, position limit n/a"}

        max_single = float(rule.params.get("max_single_position_pct", 0.10))
        max_total = float(rule.params.get("max_total_position_pct", 1.0))

        current_pos_value = float(account.get("current_position_value", 0.0) or 0.0)
        total_position_value = float(account.get("total_position_value", 0.0) or 0.0)

        # 单股持仓上限
        expected_single_pct = (current_pos_value + order_value) / portfolio_value
        if expected_single_pct > max_single + 1e-9:
            return True, (
                f"单股持仓占比 {expected_single_pct:.1%} 超过上限 {max_single:.1%} "
                f"(当前={current_pos_value:.2f}, 本次={order_value:.2f}, 总资产={portfolio_value:.2f})"
            ), {
                "symbol": _get_str(order, "symbol", ""),
                "expected_single_pct": expected_single_pct,
                "max_single_position_pct": max_single,
            }

        # 总仓位上限
        expected_total_pct = (total_position_value + order_value) / portfolio_value
        if expected_total_pct > max_total + 1e-9:
            return True, (
                f"总仓位占比 {expected_total_pct:.1%} 超过上限 {max_total:.1%} "
                f"(当前总持仓={total_position_value:.2f}, 本次={order_value:.2f})"
            ), {
                "expected_total_pct": expected_total_pct,
                "max_total_position_pct": max_total,
            }

        return False, "", {"expected_single_pct": expected_single_pct}

    async def _check_order_limit(
        self,
        rule: RiskRuleConfig,
        order: Any,
        account: dict[str, Any],
        context: dict[str, Any],
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        ORDER_LIMIT : 单笔下单金额/数量限额。

        params:
          max_order_value : 单笔金额上限 (默认 1_000_000)
          min_order_value : 单笔金额下限 (默认 100)
          max_order_quantity : 单笔数量上限 (可选)
        """
        order_value = _get_float(order, "order_value", 0.0)
        quantity = _get_float(order, "quantity", 0.0)
        max_value = float(rule.params.get("max_order_value", 1_000_000.0))
        min_value = float(rule.params.get("min_order_value", 100.0))
        max_qty = rule.params.get("max_order_quantity")

        if order_value > max_value + 1e-9:
            return True, (
                f"单笔金额 {order_value:.2f} 超过上限 {max_value:.2f}"
            ), {"order_value": order_value, "max_order_value": max_value}
        if order_value > 0 and order_value < min_value - 1e-9:
            return True, (
                f"单笔金额 {order_value:.2f} 低于下限 {min_value:.2f}"
            ), {"order_value": order_value, "min_order_value": min_value}
        if max_qty is not None and quantity > float(max_qty) + 1e-9:
            return True, (
                f"单笔数量 {quantity:.0f} 超过上限 {float(max_qty):.0f}"
            ), {"quantity": quantity, "max_order_quantity": float(max_qty)}

        return False, "", {"order_value": order_value}

    async def _check_frequency_limit(
        self,
        rule: RiskRuleConfig,
        order: Any,
        account: dict[str, Any],
        context: dict[str, Any],
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        FREQUENCY_LIMIT : 日内交易频次限制。

        params:
          max_daily_trades      : 单用户日内总交易次数上限 (默认 100)
          max_per_symbol_trades : 单股日内交易次数上限 (默认 5)
        context:
          daily_trade_count     : 当日已下单次数
          symbol_trade_count    : 当日该股已交易次数
        """
        max_daily = int(rule.params.get("max_daily_trades", 100))
        max_per_symbol = int(rule.params.get("max_per_symbol_trades", 5))
        daily_count = int(context.get("daily_trade_count", 0) or 0)
        symbol_count = int(context.get("symbol_trade_count", 0) or 0)

        if daily_count >= max_daily:
            return True, (
                f"日内交易次数 {daily_count} 已达上限 {max_daily}"
            ), {"daily_trade_count": daily_count, "max_daily_trades": max_daily}
        if symbol_count >= max_per_symbol:
            symbol = _get_str(order, "symbol", "")
            return True, (
                f"标的 {symbol} 日内交易次数 {symbol_count} "
                f"已达上限 {max_per_symbol}"
            ), {
                "symbol": symbol,
                "symbol_trade_count": symbol_count,
                "max_per_symbol_trades": max_per_symbol,
            }

        return False, "", {
            "daily_trade_count": daily_count,
            "symbol_trade_count": symbol_count,
        }

    async def _check_capital_check(
        self,
        rule: RiskRuleConfig,
        order: Any,
        account: dict[str, Any],
        context: dict[str, Any],
    ) -> tuple[bool, str, dict[str, Any]]:
        """
        CAPITAL_CHECK : 可用资金/持仓检查。

        买入查资金：order_value + 估算佣金 <= available_cash
        卖出查持仓：order.quantity <= available_position

        params:
          commission_rate_buy : 买入佣金率 (默认 0.0003)
          min_commission      : 最低佣金 (默认 5.0)
          commission_rate_sell: 卖出佣金率 (默认 0.0013, 含印花税)
        account:
          available_cash      : 可用资金
          available_position  : 可用持仓数量 (卖出时)
        """
        side = _get_str(order, "side", "").lower()
        order_value = _get_float(order, "order_value", 0.0)
        quantity = _get_float(order, "quantity", 0.0)

        if side == "buy":
            available_cash = float(account.get("available_cash", -1.0))
            # available_cash < 0 视为未知，跳过 (不阻断)
            if available_cash < 0:
                return False, "", {"skipped": "available_cash unknown"}
            rate = float(rule.params.get("commission_rate_buy", 0.0003))
            min_comm = float(rule.params.get("min_commission", 5.0))
            commission = max(order_value * rate, min_comm)
            required = order_value + commission
            if required > available_cash + 1e-9:
                return True, (
                    f"可用资金不足: 需要 {required:.2f} (含佣金 {commission:.2f}), "
                    f"可用 {available_cash:.2f}"
                ), {
                    "required": required,
                    "available_cash": available_cash,
                    "commission": commission,
                }
            return False, "", {"required": required, "available_cash": available_cash}

        if side == "sell":
            available_position = float(account.get("available_position", -1.0))
            if available_position < 0:
                return False, "", {"skipped": "available_position unknown"}
            if quantity > available_position + 1e-9:
                return True, (
                    f"可用持仓不足: 需要 {quantity:.0f}, 可用 {available_position:.0f}"
                ), {
                    "quantity": quantity,
                    "available_position": available_position,
                }
            return False, "", {
                "quantity": quantity,
                "available_position": available_position,
            }

        return False, "", {"skipped": f"unknown side={side}"}

    # 处理器注册表 (类级，避免动态查找)
    _HANDLERS: dict[str, Callable] = {}  # type: ignore[assignment]

    def __init_subclass__(cls, **kwargs: Any) -> None:
        super().__init_subclass__(**kwargs)


# 注册处理器 (在类定义完成后绑定)
RiskControlEngine._HANDLERS = {
    RuleType.PRICE_LIMIT.value: RiskControlEngine._check_price_limit,
    RuleType.POSITION_LIMIT.value: RiskControlEngine._check_position_limit,
    RuleType.ORDER_LIMIT.value: RiskControlEngine._check_order_limit,
    RuleType.FREQUENCY_LIMIT.value: RiskControlEngine._check_frequency_limit,
    RuleType.CAPITAL_CHECK.value: RiskControlEngine._check_capital_check,
}


# ─── 配置加载器 ───────────────────────────────────────────────────────────────


class RiskRuleLoader:
    """
    风控规则配置加载器。

    支持:
      - 从 YAML / JSON 文件加载
      - 从 ORM ``RiskRule`` 列表加载 (与 T4.1 DB 规则合并)
      - 基于文件 mtime 的热加载检测
    """

    def __init__(self, config_path: str | Path | None = None) -> None:
        self.config_path = Path(config_path) if config_path else None
        self._last_mtime: float | None = None
        self._lock = threading.RLock()

    def load(self) -> list[RiskRuleConfig]:
        """加载配置文件中的规则集 (YAML 优先，回退 JSON)。"""
        if self.config_path is None or not self.config_path.exists():
            logger.warning(
                "[RiskLoader] config not found: %s, return empty rules",
                self.config_path,
            )
            return []
        rules: list[RiskRuleConfig] = []
        try:
            text = self.config_path.read_text(encoding="utf-8")
            data: dict[str, Any]
            if self.config_path.suffix.lower() in (".yaml", ".yml"):
                try:
                    import yaml  # type: ignore

                    data = yaml.safe_load(text) or {}
                except ImportError:
                    logger.error(
                        "[RiskLoader] PyYAML not installed, cannot parse %s",
                        self.config_path,
                    )
                    return []
            else:
                data = json.loads(text) if text.strip() else {}

            raw_rules = data.get("rules", []) if isinstance(data, dict) else []
            for raw in raw_rules:
                if not isinstance(raw, dict):
                    continue
                try:
                    rules.append(RiskRuleConfig.from_dict(raw))
                except (KeyError, TypeError) as exc:
                    logger.warning(
                        "[RiskLoader] skip invalid rule %s: %s", raw, exc
                    )

            # 更新 mtime 快照
            self._last_mtime = self.config_path.stat().st_mtime
            logger.info(
                "[RiskLoader] loaded %d rules from %s",
                len(rules),
                self.config_path,
            )
        except Exception as exc:
            logger.exception("[RiskLoader] failed to load %s: %s", self.config_path, exc)
        return rules

    def has_changed(self) -> bool:
        """检测配置文件是否在上次加载后被修改。"""
        if self.config_path is None or not self.config_path.exists():
            return False
        try:
            current_mtime = self.config_path.stat().st_mtime
        except OSError:
            return False
        return self._last_mtime is None or current_mtime > self._last_mtime + 1e-6

    def check_and_reload(self, engine: RiskControlEngine) -> bool:
        """
        若配置文件已变更则重新加载并热更新到引擎。

        返回 True 表示发生了重载，False 表示无变更。
        线程安全：使用 RLock 保护 mtime 读写。
        """
        with self._lock:
            if not self.has_changed():
                return False
            new_rules = self.load()
            engine.reload(new_rules)
            return True

    @staticmethod
    def from_orm_rules(orm_rules: list[Any]) -> list[RiskRuleConfig]:
        """将 ORM ``RiskRule`` 列表转换为引擎配置列表。"""
        return [RiskRuleConfig.from_orm(r) for r in orm_rules]

    @staticmethod
    def merge(
        file_rules: list[RiskRuleConfig], orm_rules: list[RiskRuleConfig]
    ) -> list[RiskRuleConfig]:
        """
        合并配置文件规则与 ORM 规则。

        冲突策略：以 file_rules 为基础，orm_rules 中 rule_id 相同的覆盖；
        orm_rules 中新增的追加。这样运维可通过配置文件设定基线，DB 覆盖细粒度调整。
        """
        merged: dict[str, RiskRuleConfig] = {}
        for r in file_rules:
            merged[r.rule_id] = r
        for r in orm_rules:
            merged[r.rule_id] = r
        return list(merged.values())


# ─── 模块级便捷单例 (惰性初始化，避免 import 时强依赖配置文件) ───────────────────

_default_engine: RiskControlEngine | None = None
_default_loader: RiskRuleLoader | None = None
_engine_lock = threading.Lock()


def _default_config_path() -> Path:
    """定位默认配置文件路径 config/risk_rules.yaml。"""
    # backend/services/trade/services/risk_control.py -> 项目根
    here = Path(__file__).resolve()
    project_root = here.parents[4]  # backend/services/trade/services -> root
    candidates = [
        project_root / "config" / "risk_rules.yaml",
        project_root / "config" / "risk_rules.yml",
        project_root / "config" / "risk_rules.json",
    ]
    for cand in candidates:
        if cand.exists():
            return cand
    return candidates[0]  # 返回默认期望路径 (即使不存在)


def get_default_engine(audit_callback: RiskAuditCallback | None = None) -> RiskControlEngine:
    """
    获取模块级默认引擎单例。

    首次调用时从 ``config/risk_rules.yaml`` 加载规则；若文件不存在则返回空规则引擎。
    后续可通过 ``reload_default_engine`` 触发热加载。
    """
    global _default_engine, _default_loader
    with _engine_lock:
        if _default_engine is None:
            _default_loader = RiskRuleLoader(_default_config_path())
            rules = _default_loader.load()
            _default_engine = RiskControlEngine(rules, audit_callback=audit_callback)
            logger.info(
                "[RiskEngine] default engine initialized with %d rules", len(rules)
            )
        return _default_engine


def reload_default_engine() -> bool:
    """热加载默认引擎的规则 (若配置文件已变更)。"""
    global _default_loader
    with _engine_lock:
        if _default_loader is None or _default_engine is None:
            return False
        return _default_loader.check_and_reload(_default_engine)
