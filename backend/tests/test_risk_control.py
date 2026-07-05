"""
可配置风控规则引擎测试 (T4.2)

覆盖：
  - 5 类规则 (PRICE_LIMIT/POSITION_LIMIT/ORDER_LIMIT/FREQUENCY_LIMIT/CAPITAL_CHECK)
    每类至少 1 通过 + 1 拒绝场景
  - 引擎优先级、WARN 累积、disabled 规则跳过、异常隔离
  - 配置加载 (YAML) + 热加载 (mtime 检测 + engine.reload)
  - 审计日志回调 + RiskAuditService 落库 (FakeAsyncSession)
  - 板块识别 / 涨跌停价计算 / ORM 规则转换 / 合并

运行：python -m pytest backend/tests/test_risk_control.py -v
"""

from __future__ import annotations

import asyncio
import time
from pathlib import Path
from types import SimpleNamespace
from typing import Any

import pytest

from backend.services.trade.services.risk_control import (
    RiskCheckResult,
    RiskControlEngine,
    RiskRuleConfig,
    RiskRuleLoader,
    RuleAction,
    RuleType,
    compute_limit_prices,
    detect_board,
    get_default_engine,
    reload_default_engine,
)

# ─── 公共 fixtures / helpers ─────────────────────────────────────────────────

def make_order(
    symbol: str = "SH600000",
    side: str = "buy",
    price: float = 10.0,
    quantity: float = 100,
    order_value: float | None = None,
    order_type: str = "limit",
    trade_action: str = "buy_to_open",
    tenant_id: str = "default",
) -> SimpleNamespace:
    """构造轻量订单对象 (兼容 ORM Order 字段访问)。"""
    return SimpleNamespace(
        order_id="ord-test-001",
        id=1,
        symbol=symbol,
        side=side,
        price=price,
        quantity=quantity,
        order_value=order_value if order_value is not None else price * quantity,
        order_type=order_type,
        trade_action=trade_action,
        tenant_id=tenant_id,
    )

def make_rule(
    rule_id: str,
    rule_type: str,
    params: dict[str, Any] | None = None,
    action: str = RuleAction.REJECT.value,
    priority: int = 100,
    enabled: bool = True,
) -> RiskRuleConfig:
    return RiskRuleConfig(
        rule_id=rule_id,
        rule_type=rule_type,
        enabled=enabled,
        params=params or {},
        action=action,
        priority=priority,
    )

def run(coro):
    """同步执行 async 协程 (兼容无 event loop 的测试上下文)。"""
    return asyncio.get_event_loop().run_until_complete(coro)

class _FakeAsyncSession:
    """模拟 AsyncSession，捕获 add/commit/rollback 用于审计落库验证。"""

    def __init__(self) -> None:
        self.added: list[Any] = []
        self.commits = 0
        self.rollbacks = 0

    def add(self, obj: Any) -> None:
        self.added.append(obj)

    async def commit(self) -> None:
        self.commits += 1

    async def rollback(self) -> None:
        self.rollbacks += 1

# ─── 板块识别 / 涨跌停价计算 ─────────────────────────────────────────────────

@pytest.mark.unit
def test_detect_board_prefix_format():
    """SH600000 前缀格式板块识别 (AGENTS.md 强制格式)"""
    assert detect_board("SH600000") == "MAIN"
    assert detect_board("SZ000001") == "MAIN"
    assert detect_board("SZ300001") == "GEM"  # 创业板
    assert detect_board("SH688001") == "STAR"  # 科创板
    assert detect_board("BJ430001") == "BJ"  # 北交所
    assert detect_board("BJ830001") == "BJ"

@pytest.mark.unit
def test_detect_board_suffix_and_numeric_compatible():
    """兼容后缀格式与纯数字代码"""
    assert detect_board("600000.SH") == "MAIN"
    assert detect_board("300001.SZ") == "GEM"
    assert detect_board("688001") == "STAR"

@pytest.mark.unit
def test_compute_limit_prices_main_board():
    """主板 ±10%"""
    up, down = compute_limit_prices(10.0, "MAIN", {})
    assert up == 11.0
    assert down == 9.0

@pytest.mark.unit
def test_compute_limit_prices_star_board_20pct():
    """科创板 ±20%"""
    up, down = compute_limit_prices(10.0, "STAR", {})
    assert up == pytest.approx(12.0)
    assert down == pytest.approx(8.0)

@pytest.mark.unit
def test_compute_limit_prices_st_stock_5pct():
    """ST 股 ±5% (params.is_st=True 触发)"""
    up, down = compute_limit_prices(10.0, "MAIN", {"is_st": True, "st_pct": 0.05})
    assert up == 10.5
    assert down == 9.5

# ─── 1. PRICE_LIMIT ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_price_limit_pass():
    """价格在涨跌停区间内 -> PASS"""
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    order = make_order(symbol="SH600000", side="buy", price=10.5)
    ctx = {"prev_close": 10.0}
    engine = RiskControlEngine([rule])
    result = await engine.check(order, context=ctx)
    assert result.passed

@pytest.mark.asyncio
@pytest.mark.unit
async def test_price_limit_reject_above_limit_up():
    """买单价格超过涨停价 -> REJECT"""
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    order = make_order(symbol="SH600000", side="buy", price=11.5)  # 涨停 11.0
    ctx = {"prev_close": 10.0}
    engine = RiskControlEngine([rule])
    result = await engine.check(order, context=ctx)
    assert result.action == RuleAction.REJECT.value
    assert result.rule_id == "price_limit"
    assert "涨停价" in result.message

@pytest.mark.asyncio
@pytest.mark.unit
async def test_price_limit_reject_below_limit_down():
    """卖单价格低于跌停价 -> REJECT"""
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    order = make_order(symbol="SH600000", side="sell", price=8.5)  # 跌停 9.0
    ctx = {"prev_close": 10.0}
    engine = RiskControlEngine([rule])
    result = await engine.check(order, context=ctx)
    assert result.action == RuleAction.REJECT.value
    assert "跌停价" in result.message

@pytest.mark.asyncio
@pytest.mark.unit
async def test_price_limit_star_board_20pct():
    """科创板 ±20%：11.5 不超涨停 (12.0) -> PASS"""
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    order = make_order(symbol="SH688001", side="buy", price=11.5)
    ctx = {"prev_close": 10.0}
    engine = RiskControlEngine([rule])
    result = await engine.check(order, context=ctx)
    assert result.passed

@pytest.mark.asyncio
@pytest.mark.unit
async def test_price_limit_block_near_limit_buy():
    """已涨停禁买 (block_near_limit=true)"""
    rule = make_rule(
        "price_limit",
        RuleType.PRICE_LIMIT.value,
        params={"block_near_limit": True, "near_limit_tolerance": 0.01},
        priority=10,
    )
    order = make_order(symbol="SH600000", side="buy", price=11.0)  # 贴近涨停
    ctx = {"prev_close": 10.0}
    engine = RiskControlEngine([rule])
    result = await engine.check(order, context=ctx)
    assert result.action == RuleAction.REJECT.value
    assert "涨停" in result.message

# ─── 2. POSITION_LIMIT ───────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_position_limit_pass():
    """单股持仓占比 5% < 10% 上限 -> PASS"""
    rule = make_rule(
        "position_limit",
        RuleType.POSITION_LIMIT.value,
        params={"max_single_position_pct": 0.10, "max_total_position_pct": 1.0},
        priority=20,
    )
    order = make_order(side="buy", order_value=5000.0)  # 5% of 100k
    account = {
        "portfolio_value": 100000.0,
        "current_position_value": 0.0,
        "total_position_value": 0.0,
    }
    engine = RiskControlEngine([rule])
    result = await engine.check(order, account=account)
    assert result.passed

@pytest.mark.asyncio
@pytest.mark.unit
async def test_position_limit_reject_single_exceed():
    """单股持仓占比 15% > 10% 上限 -> REJECT"""
    rule = make_rule(
        "position_limit",
        RuleType.POSITION_LIMIT.value,
        params={"max_single_position_pct": 0.10, "max_total_position_pct": 1.0},
        priority=20,
    )
    order = make_order(side="buy", order_value=15000.0)  # 15% of 100k
    account = {
        "portfolio_value": 100000.0,
        "current_position_value": 0.0,
        "total_position_value": 0.0,
    }
    engine = RiskControlEngine([rule])
    result = await engine.check(order, account=account)
    assert result.action == RuleAction.REJECT.value
    assert "单股持仓占比" in result.message

@pytest.mark.asyncio
@pytest.mark.unit
async def test_position_limit_reject_total_exceed():
    """总仓位占比超 100% -> REJECT"""
    rule = make_rule(
        "position_limit",
        RuleType.POSITION_LIMIT.value,
        params={"max_single_position_pct": 0.50, "max_total_position_pct": 0.8},
        priority=20,
    )
    order = make_order(side="buy", order_value=30000.0)  # 当前 60% + 30% = 90% > 80%
    account = {
        "portfolio_value": 100000.0,
        "current_position_value": 0.0,
        "total_position_value": 60000.0,
    }
    engine = RiskControlEngine([rule])
    result = await engine.check(order, account=account)
    assert result.action == RuleAction.REJECT.value
    assert "总仓位占比" in result.message

# ─── 3. ORDER_LIMIT ──────────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_order_limit_pass():
    """单笔金额在 [100, 1000000] 内 -> PASS"""
    rule = make_rule(
        "order_limit",
        RuleType.ORDER_LIMIT.value,
        params={"max_order_value": 1000000.0, "min_order_value": 100.0},
        priority=30,
    )
    order = make_order(order_value=50000.0)
    engine = RiskControlEngine([rule])
    result = await engine.check(order)
    assert result.passed

@pytest.mark.asyncio
@pytest.mark.unit
async def test_order_limit_reject_too_large():
    """单笔金额超过上限 -> REJECT"""
    rule = make_rule(
        "order_limit",
        RuleType.ORDER_LIMIT.value,
        params={"max_order_value": 1000000.0, "min_order_value": 100.0},
        priority=30,
    )
    order = make_order(order_value=2_000_000.0)
    engine = RiskControlEngine([rule])
    result = await engine.check(order)
    assert result.action == RuleAction.REJECT.value
    assert "超过上限" in result.message

@pytest.mark.asyncio
@pytest.mark.unit
async def test_order_limit_reject_too_small():
    """单笔金额低于下限 -> REJECT"""
    rule = make_rule(
        "order_limit",
        RuleType.ORDER_LIMIT.value,
        params={"max_order_value": 1000000.0, "min_order_value": 100.0},
        priority=30,
    )
    order = make_order(order_value=50.0)
    engine = RiskControlEngine([rule])
    result = await engine.check(order)
    assert result.action == RuleAction.REJECT.value
    assert "低于下限" in result.message

# ─── 4. FREQUENCY_LIMIT ──────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_frequency_limit_pass():
    """日内交易次数未达上限 -> PASS"""
    rule = make_rule(
        "frequency_limit",
        RuleType.FREQUENCY_LIMIT.value,
        params={"max_daily_trades": 100, "max_per_symbol_trades": 5},
        priority=40,
    )
    ctx = {"daily_trade_count": 10, "symbol_trade_count": 2}
    engine = RiskControlEngine([rule])
    result = await engine.check(make_order(), context=ctx)
    assert result.passed

@pytest.mark.asyncio
@pytest.mark.unit
async def test_frequency_limit_reject_daily_exceed():
    """日内总交易次数达上限 -> REJECT"""
    rule = make_rule(
        "frequency_limit",
        RuleType.FREQUENCY_LIMIT.value,
        params={"max_daily_trades": 100, "max_per_symbol_trades": 5},
        priority=40,
    )
    ctx = {"daily_trade_count": 100, "symbol_trade_count": 0}
    engine = RiskControlEngine([rule])
    result = await engine.check(make_order(), context=ctx)
    assert result.action == RuleAction.REJECT.value
    assert "日内交易次数" in result.message

@pytest.mark.asyncio
@pytest.mark.unit
async def test_frequency_limit_reject_symbol_exceed():
    """单股日内交易次数达上限 -> REJECT"""
    rule = make_rule(
        "frequency_limit",
        RuleType.FREQUENCY_LIMIT.value,
        params={"max_daily_trades": 100, "max_per_symbol_trades": 5},
        priority=40,
    )
    ctx = {"daily_trade_count": 10, "symbol_trade_count": 5}
    engine = RiskControlEngine([rule])
    result = await engine.check(make_order(symbol="SH600000"), context=ctx)
    assert result.action == RuleAction.REJECT.value
    assert "SH600000" in result.message

# ─── 5. CAPITAL_CHECK ────────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_capital_check_buy_pass():
    """买入资金充足 -> PASS"""
    rule = make_rule(
        "capital_check",
        RuleType.CAPITAL_CHECK.value,
        params={"commission_rate_buy": 0.0003, "min_commission": 5.0},
        priority=50,
    )
    order = make_order(side="buy", order_value=10000.0)
    account = {"available_cash": 20000.0}
    engine = RiskControlEngine([rule])
    result = await engine.check(order, account=account)
    assert result.passed

@pytest.mark.asyncio
@pytest.mark.unit
async def test_capital_check_buy_reject_insufficient_cash():
    """买入资金不足 -> REJECT"""
    rule = make_rule(
        "capital_check",
        RuleType.CAPITAL_CHECK.value,
        params={"commission_rate_buy": 0.0003, "min_commission": 5.0},
        priority=50,
    )
    order = make_order(side="buy", order_value=10000.0)
    account = {"available_cash": 5000.0}  # 不足 10000 + 佣金
    engine = RiskControlEngine([rule])
    result = await engine.check(order, account=account)
    assert result.action == RuleAction.REJECT.value
    assert "可用资金不足" in result.message

@pytest.mark.asyncio
@pytest.mark.unit
async def test_capital_check_sell_reject_insufficient_position():
    """卖出持仓不足 -> REJECT"""
    rule = make_rule("capital_check", RuleType.CAPITAL_CHECK.value, priority=50)
    order = make_order(side="sell", quantity=200)
    account = {"available_position": 100}  # 仅 100 股
    engine = RiskControlEngine([rule])
    result = await engine.check(order, account=account)
    assert result.action == RuleAction.REJECT.value
    assert "可用持仓不足" in result.message

# ─── 引擎行为：优先级 / WARN / disabled / 异常隔离 ────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_engine_priority_order_reject_stops_early():
    """高优先级规则 (小 priority 数字) 先执行，REJECT 立即返回"""
    # price_limit priority=10 会先命中 REJECT，capital_check priority=50 不应执行
    price_rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    capital_rule = make_rule("capital_check", RuleType.CAPITAL_CHECK.value, priority=50)
    order = make_order(
        side="buy", price=11.5, order_value=1000000.0
    )  # 超涨停 + 资金不足
    account = {"available_cash": 100.0}
    ctx = {"prev_close": 10.0}
    engine = RiskControlEngine([capital_rule, price_rule])  # 传入顺序打乱
    result = await engine.check(order, account=account, context=ctx)
    assert result.action == RuleAction.REJECT.value
    # 应被 price_limit (priority=10) 命中
    assert result.rule_id == "price_limit"

@pytest.mark.asyncio
@pytest.mark.unit
async def test_engine_warn_continues_and_accumulates():
    """WARN 不阻断，继续后续规则，累积 violations"""
    warn_rule = make_rule(
        "order_limit_warn",
        RuleType.ORDER_LIMIT.value,
        params={"max_order_value": 100.0, "min_order_value": 0.0},
        action=RuleAction.WARN.value,
        priority=30,
    )
    pass_rule = make_rule(
        "capital_check",
        RuleType.CAPITAL_CHECK.value,
        params={"commission_rate_buy": 0.0003, "min_commission": 5.0},
        priority=50,
    )
    order = make_order(side="buy", order_value=500.0)  # 超 WARN 上限 100
    account = {"available_cash": 10000.0}
    engine = RiskControlEngine([warn_rule, pass_rule])
    result = await engine.check(order, account=account)
    assert result.action == RuleAction.WARN.value
    assert result.rule_id == "order_limit_warn"
    assert len(result.violations) >= 1

@pytest.mark.asyncio
@pytest.mark.unit
async def test_engine_disabled_rule_skipped():
    """disabled 规则不执行"""
    disabled_rule = make_rule(
        "order_limit_disabled",
        RuleType.ORDER_LIMIT.value,
        params={"max_order_value": 100.0},
        enabled=False,
        priority=10,
    )
    order = make_order(order_value=5000.0)  # 若启用会被拒
    engine = RiskControlEngine([disabled_rule])
    result = await engine.check(order)
    assert result.passed  # disabled -> 跳过 -> PASS

@pytest.mark.asyncio
@pytest.mark.unit
async def test_engine_rule_exception_does_not_crash(monkeypatch):
    """单条规则抛异常时被捕获，不阻断整体检查"""
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)

    async def boom(self, rule, order, account, context):
        raise RuntimeError("simulated handler error")

    # 直接 patch 分发表 (类属性 _HANDLERS 在类定义时已绑定原方法引用)
    monkeypatch.setitem(RiskControlEngine._HANDLERS, RuleType.PRICE_LIMIT.value, boom)
    engine = RiskControlEngine([rule])
    result = await engine.check(make_order(), context={"prev_close": 10.0})
    # 异常被捕获，规则被视为命中 (rule_internal_error)，因 action=REJECT -> REJECT
    assert result.action == RuleAction.REJECT.value
    assert "rule_internal_error" in result.message

# ─── 配置加载 (YAML) + 热加载 ────────────────────────────────────────────────

@pytest.mark.unit
def test_config_load_from_yaml(tmp_path):
    """RiskRuleLoader 从 YAML 文件加载规则"""
    cfg = tmp_path / "risk_rules.yaml"
    cfg.write_text(
        """
rules:
  - rule_id: price_limit
    rule_type: PRICE_LIMIT
    enabled: true
    action: REJECT
    priority: 10
    params:
      main_board_pct: 0.10
  - rule_id: order_limit
    rule_type: ORDER_LIMIT
    enabled: false
    priority: 30
    params:
      max_order_value: 500000
""",
        encoding="utf-8",
    )
    loader = RiskRuleLoader(cfg)
    rules = loader.load()
    assert len(rules) == 2
    assert rules[0].rule_id == "price_limit"
    assert rules[0].rule_type == RuleType.PRICE_LIMIT.value
    assert rules[1].enabled is False

@pytest.mark.unit
def test_config_load_missing_file_returns_empty():
    """配置文件不存在时返回空列表 (不抛异常)"""
    loader = RiskRuleLoader(Path("/nonexistent/risk_rules.yaml"))
    rules = loader.load()
    assert rules == []

@pytest.mark.unit
def test_hot_reload_detects_mtime_change(tmp_path):
    """文件 mtime 变化触发 has_changed=True"""
    cfg = tmp_path / "risk_rules.yaml"
    cfg.write_text("rules: []\n", encoding="utf-8")
    loader = RiskRuleLoader(cfg)
    loader.load()
    assert loader.has_changed() is False

    # 修改文件 (确保 mtime 推进)
    time.sleep(0.05)
    cfg.write_text(
        "rules:\n  - rule_id: x\n    rule_type: ORDER_LIMIT\n", encoding="utf-8"
    )
    assert loader.has_changed() is True

@pytest.mark.asyncio
@pytest.mark.unit
async def test_check_and_reload_updates_engine(tmp_path):
    """check_and_reload 检测变更后热更新引擎规则集"""
    cfg = tmp_path / "risk_rules.yaml"
    cfg.write_text("rules: []\n", encoding="utf-8")
    loader = RiskRuleLoader(cfg)
    engine = RiskControlEngine(loader.load())
    assert len(engine.rules) == 0

    # 写入新规则
    time.sleep(0.05)
    cfg.write_text(
        "rules:\n  - rule_id: order_limit\n    rule_type: ORDER_LIMIT\n    priority: 30\n",
        encoding="utf-8",
    )
    reloaded = loader.check_and_reload(engine)
    assert reloaded is True
    assert len(engine.rules) == 1
    assert engine.rules[0].rule_id == "order_limit"

    # 再次调用无变更
    reloaded_again = loader.check_and_reload(engine)
    assert reloaded_again is False

@pytest.mark.asyncio
@pytest.mark.unit
async def test_default_engine_loads_project_config():
    """默认引擎单例从 config/risk_rules.yaml 加载 5 类规则"""
    # 重置单例 (模块级缓存)
    import backend.services.trade.services.risk_control as rc_mod

    rc_mod._default_engine = None
    rc_mod._default_loader = None

    engine = get_default_engine()
    rule_types = {r.rule_type for r in engine.rules if r.enabled}
    assert RuleType.PRICE_LIMIT.value in rule_types
    assert RuleType.POSITION_LIMIT.value in rule_types
    assert RuleType.ORDER_LIMIT.value in rule_types
    assert RuleType.FREQUENCY_LIMIT.value in rule_types
    assert RuleType.CAPITAL_CHECK.value in rule_types

# ─── ORM 规则转换 / 合并 ─────────────────────────────────────────────────────

@pytest.mark.unit
def test_orm_rule_conversion():
    """ORM RiskRule -> RiskRuleConfig 转换 (含 rule_type 映射)"""
    fake_orm = SimpleNamespace(
        rule_name="max_order_size_db",
        rule_type="max_order_size",
        is_active=True,
        parameters={"max_value": 200000},
        priority=5,
        description="DB-level order limit",
    )
    cfg = RiskRuleConfig.from_orm(fake_orm)
    assert cfg.rule_id == "max_order_size_db"
    assert cfg.rule_type == RuleType.ORDER_LIMIT.value  # max_order_size -> ORDER_LIMIT
    assert cfg.enabled is True
    assert cfg.params == {"max_value": 200000}
    assert cfg.priority == 5

@pytest.mark.unit
def test_merge_file_and_orm_rules():
    """合并：ORM 同 rule_id 覆盖 file，新增追加"""
    file_rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    orm_rule = make_rule(
        "price_limit",
        RuleType.PRICE_LIMIT.value,
        params={"main_board_pct": 0.05},
        priority=5,
    )
    orm_new = make_rule("custom_rule", RuleType.ORDER_LIMIT.value, priority=99)
    merged = RiskRuleLoader.merge([file_rule], [orm_rule, orm_new])
    by_id = {r.rule_id: r for r in merged}
    assert len(merged) == 2
    # ORM 覆盖 file
    assert by_id["price_limit"].priority == 5
    assert by_id["price_limit"].params["main_board_pct"] == 0.05
    # ORM 新增
    assert "custom_rule" in by_id

# ─── 审计日志 ────────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_audit_callback_invoked_on_reject():
    """REJECT 时 audit_callback 被调用"""
    calls: list[tuple] = []

    async def audit_cb(event_type, rule, order_info, details):
        calls.append(
            (event_type, rule.rule_id, order_info.get("symbol"), details.get("message"))
        )

    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    engine = RiskControlEngine([rule], audit_callback=audit_cb)
    order = make_order(symbol="SH600000", side="buy", price=11.5)
    await engine.check(
        order, context={"prev_close": 10.0, "tenant_id": "default", "user_id": "1"}
    )

    assert len(calls) == 1
    assert calls[0][0] == RuleAction.REJECT.value
    assert calls[0][1] == "price_limit"
    assert calls[0][2] == "SH600000"

@pytest.mark.asyncio
@pytest.mark.unit
async def test_audit_callback_skipped_on_pass():
    """PASS 时 audit_callback 不被调用"""
    calls: list[tuple] = []

    async def audit_cb(event_type, rule, order_info, details):
        calls.append(event_type)

    rule = make_rule("order_limit", RuleType.ORDER_LIMIT.value, priority=10)
    engine = RiskControlEngine([rule], audit_callback=audit_cb)
    await engine.check(make_order(order_value=5000.0))
    assert calls == []

@pytest.mark.asyncio
@pytest.mark.unit
async def test_audit_service_persist_to_fake_db():
    """RiskAuditService 落库到 FakeAsyncSession (验证 add+commit)"""
    from backend.services.trade.services.risk_audit_service import RiskAuditService
    from backend.services.trade.models.risk_audit import RiskAuditLog

    fake_db = _FakeAsyncSession()
    audit = RiskAuditService(fake_db)
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    order_info = {
        "order_id": "ord-1",
        "symbol": "SH600000",
        "tenant_id": "default",
        "user_id": "1",
    }
    details = {"message": "超过涨停价", "limit_up": 11.0, "price": 11.5}

    await audit.log_rule_hit(RuleAction.REJECT.value, rule, order_info, details)

    assert fake_db.commits == 1
    assert len(fake_db.added) == 1
    entry = fake_db.added[0]
    assert isinstance(entry, RiskAuditLog)
    assert entry.event_type == "REJECT"
    assert entry.rule_id == "price_limit"
    assert entry.rule_type == RuleType.PRICE_LIMIT.value
    assert entry.tenant_id == "default"
    assert entry.user_id == "1"
    assert entry.order_info["symbol"] == "SH600000"
    assert "limit_up" in entry.hit_details

@pytest.mark.asyncio
@pytest.mark.unit
async def test_audit_service_no_db_session_graceful_skip():
    """无 DB session 时不抛异常，仅记日志"""
    from backend.services.trade.services.risk_audit_service import RiskAuditService

    audit = RiskAuditService(db=None)
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    # 不应抛异常
    await audit.log_rule_hit(
        RuleAction.WARN.value, rule, {"symbol": "SH600000"}, {"message": "warn"}
    )

@pytest.mark.asyncio
@pytest.mark.unit
async def test_audit_service_persist_failure_rolls_back():
    """审计落库异常时回滚且不阻断"""
    from backend.services.trade.services.risk_audit_service import RiskAuditService

    class _FailingSession(_FakeAsyncSession):
        async def commit(self):
            raise RuntimeError("db connection lost")

    fake_db = _FailingSession()
    audit = RiskAuditService(fake_db)
    rule = make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10)
    # 不应抛异常
    await audit.log_rule_hit(
        RuleAction.REJECT.value, rule, {"symbol": "SH600000"}, {"message": "reject"}
    )
    assert fake_db.rollbacks == 1

@pytest.mark.asyncio
@pytest.mark.unit
async def test_audit_service_log_rule_reload():
    """规则热加载变更写入 RULE_RELOAD 审计记录"""
    from backend.services.trade.services.risk_audit_service import RiskAuditService
    from backend.services.trade.models.risk_audit import RiskAuditLog

    fake_db = _FakeAsyncSession()
    audit = RiskAuditService(fake_db)
    await audit.log_rule_reload("ADDED", rule_id="new_rule", rule_type="ORDER_LIMIT")

    assert fake_db.commits == 1
    entry = fake_db.added[0]
    assert isinstance(entry, RiskAuditLog)
    assert entry.event_type == "RULE_RELOAD"
    assert entry.rule_id == "new_rule"
    assert entry.hit_details["change_type"] == "ADDED"

# ─── 全链路集成 ──────────────────────────────────────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_full_engine_all_rules_pass():
    """5 类规则全部加载，合规订单 -> PASS"""
    rules = [
        make_rule(
            "price_limit",
            RuleType.PRICE_LIMIT.value,
            params={"main_board_pct": 0.10},
            priority=10,
        ),
        make_rule(
            "position_limit",
            RuleType.POSITION_LIMIT.value,
            params={"max_single_position_pct": 0.10, "max_total_position_pct": 1.0},
            priority=20,
        ),
        make_rule(
            "order_limit",
            RuleType.ORDER_LIMIT.value,
            params={"max_order_value": 1000000.0, "min_order_value": 100.0},
            priority=30,
        ),
        make_rule(
            "frequency_limit",
            RuleType.FREQUENCY_LIMIT.value,
            params={"max_daily_trades": 100, "max_per_symbol_trades": 5},
            priority=40,
        ),
        make_rule(
            "capital_check",
            RuleType.CAPITAL_CHECK.value,
            params={"commission_rate_buy": 0.0003, "min_commission": 5.0},
            priority=50,
        ),
    ]
    order = make_order(
        symbol="SH600000", side="buy", price=10.0, quantity=100, order_value=1000.0
    )
    account = {
        "portfolio_value": 100000.0,
        "available_cash": 50000.0,
        "current_position_value": 0.0,
        "total_position_value": 0.0,
    }
    context = {
        "prev_close": 10.0,
        "daily_trade_count": 3,
        "symbol_trade_count": 1,
        "tenant_id": "default",
        "user_id": "1",
    }
    engine = RiskControlEngine(rules)
    result = await engine.check(order, account=account, context=context)
    assert result.passed, f"expected PASS, got {result.action}: {result.message}"

@pytest.mark.asyncio
@pytest.mark.unit
async def test_full_engine_reject_by_capital_when_others_pass():
    """价格/仓位/限额/频次都过，但资金不足 -> REJECT by capital_check"""
    rules = [
        make_rule("price_limit", RuleType.PRICE_LIMIT.value, priority=10),
        make_rule(
            "position_limit",
            RuleType.POSITION_LIMIT.value,
            params={"max_single_position_pct": 0.50},
            priority=20,
        ),
        make_rule(
            "order_limit",
            RuleType.ORDER_LIMIT.value,
            params={"max_order_value": 1000000.0, "min_order_value": 100.0},
            priority=30,
        ),
        make_rule("frequency_limit", RuleType.FREQUENCY_LIMIT.value, priority=40),
        make_rule("capital_check", RuleType.CAPITAL_CHECK.value, priority=50),
    ]
    order = make_order(
        symbol="SH600000", side="buy", price=10.0, quantity=1000, order_value=10000.0
    )
    account = {
        "available_cash": 100.0,
        "portfolio_value": 100000.0,
        "current_position_value": 0.0,
        "total_position_value": 0.0,
    }
    context = {"prev_close": 10.0, "daily_trade_count": 0, "symbol_trade_count": 0}
    engine = RiskControlEngine(rules)
    result = await engine.check(order, account=account, context=context)
    assert result.action == RuleAction.REJECT.value
    assert result.rule_id == "capital_check"

# ─── RiskService 桥接 (check_order_risk_engine) ──────────────────────────────

@pytest.mark.asyncio
@pytest.mark.unit
async def test_risk_service_bridge_returns_compatible_structure(monkeypatch):
    """RiskService.check_order_risk_engine 返回 {passed, violations, action} 结构"""
    from backend.services.trade.services.risk_service import RiskService
    from backend.services.trade.services import risk_control as rc_mod
    from backend.services.trade.services import risk_audit_service as ras_mod

    # 重置默认引擎单例，确保使用项目 config
    rc_mod._default_engine = None
    rc_mod._default_loader = None

    fake_db = _FakeAsyncSession()
    svc = RiskService(db=fake_db, redis=SimpleNamespace(delete=lambda *a, **k: None))

    # 屏蔽 ORM 规则查询 (避免真实 DB)
    async def fake_list_rules(self, active_only=True):
        return []

    monkeypatch.setattr(RiskService, "list_rules", fake_list_rules)

    order = make_order(
        symbol="SH600000", side="buy", price=10.0, quantity=100, order_value=1000.0
    )
    result = await svc.check_order_risk_engine(
        user_id=1,
        order=order,
        portfolio_value=100000.0,
        available_cash=50000.0,
        daily_trade_count=0,
        prev_close=10.0,
        use_orm_rules=False,
    )
    assert "passed" in result
    assert "violations" in result
    assert "action" in result
    assert isinstance(result["violations"], list)
    assert result["passed"] is True

if __name__ == "__main__":
    # 兼容直接运行：python backend/tests/test_risk_control.py
    import sys

    sys.exit(pytest.main([__file__, "-v", "-p", "asyncio"]))
