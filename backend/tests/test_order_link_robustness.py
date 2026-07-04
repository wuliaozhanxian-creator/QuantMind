"""
订单链路"本地优先"持久化强化 + 异常补偿 + 端到端测试 (T4.1)

覆盖场景：
  A: 本地写成功 + 外部提交成功 → 订单 FILLED
  B: 本地写成功 + 外部提交失败(Broker拒绝) → 订单 REJECTED + 补偿验证
  C: 本地写成功 + 外部提交超时 → AWAITING_BRIDGE_ACK + 超时扫描器介入
  D: 本地写成功 + 外部提交崩溃(Exception) → 订单 REJECTED + 通知已发送

额外验证：
  - 本地优先原则：transition SUBMITTED 落库失败时绝不调用 broker
  - transition_order_status commit 失败时回滚内存状态
  - 对账扫描器 _scan_reconcile_candidates_once 将待核查订单入队
"""

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.trade.models.enums import (
    OrderSide,
    OrderStatus,
    OrderType,
    PositionSide,
    TradeAction,
    TradingMode,
)
from backend.services.trade.services import order_timeout_scanner
from backend.services.trade.services.broker_client import BrokerResult
from backend.services.trade.services.trading_engine import TradingEngine
from backend.services.trade.services.order_service import OrderService


# ==================== Fake 依赖 ====================


class _FakeRedis:
    def delete(self, *_a, **_kw):
        pass

    def delete_pattern(self, *_a, **_kw):
        pass

    def get(self, *_a, **_kw):
        return None

    def set(self, *_a, **_kw):
        pass

    def publish_event(self, *_a, **_kw):
        pass


class _FakeDb:
    """模拟 AsyncSession，记录 commit/refresh/rollback 调用"""

    def __init__(self):
        self.commit_calls = 0
        self.refresh_calls = 0
        self.rollback_calls = 0

    async def commit(self):
        self.commit_calls += 1

    async def refresh(self, *_a, **_kw):
        self.refresh_calls += 1

    async def rollback(self):
        self.rollback_calls += 1

    def add(self, *_a, **_kw):
        pass

    async def execute(self, *_a, **_kw):
        return _ScalarResult([])


class _CommitFailDb(_FakeDb):
    """模拟 commit 始终失败的 DB，用于验证本地优先回滚"""

    async def commit(self):
        self.commit_calls += 1
        raise RuntimeError("DB connection lost")


class _ScalarResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return list(self._values)


class _FakeSession:
    """扫描器测试用的 fake DB session"""

    def __init__(self, orders):
        self.orders = list(orders)
        self.committed = False

    async def execute(self, _stmt):
        return _ScalarResult(self.orders)

    async def commit(self):
        self.committed = True


class _FakeSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FilteringSession:
    """支持简单 remarks 过滤的 fake session，模拟 SQL NOT LIKE 条件"""

    def __init__(self, orders):
        self._all_orders = list(orders)
        self.committed = False

    async def execute(self, stmt):
        try:
            compiled = stmt.compile(compile_kwargs={"literal_binds": True})
            stmt_str = str(compiled)
        except Exception:
            stmt_str = ""

        result = list(self._all_orders)
        # 模拟 NOT LIKE '%[RECONCILE_QUEUED]%' 过滤
        if "RECONCILE_QUEUED" in stmt_str:
            result = [
                o
                for o in result
                if "[RECONCILE_QUEUED]" not in (getattr(o, "remarks", "") or "")
            ]
        return _ScalarResult(result)

    async def commit(self):
        self.committed = True


# ==================== Helpers ====================


async def _noop_async(*_a, **_kw):
    pass


def _make_order(**overrides):
    """构建测试用订单对象（使用 SH600036 前缀格式）"""
    defaults = dict(
        order_id=uuid4(),
        id=1,
        tenant_id="default",
        user_id="1001",
        portfolio_id=1,
        symbol="SH600036",
        symbol_name="招商银行",
        side=SimpleNamespace(value="buy"),
        order_type=SimpleNamespace(value="limit"),
        trade_action=SimpleNamespace(value="buy_to_open"),
        position_side=SimpleNamespace(value="long"),
        is_margin_trade=False,
        trading_mode=TradingMode.SIMULATION,
        client_order_id="test-cid-001",
        remarks="",
        status=OrderStatus.PENDING,
        quantity=100.0,
        price=10.0,
        stop_price=None,
        order_value=1000.0,
        filled_quantity=0.0,
        filled_value=0.0,
        commission=0.0,
        average_price=None,
        version=1,
        submitted_at=None,
        filled_at=None,
        cancelled_at=None,
        expired_at=None,
        exchange_order_id=None,
    )
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _setup_engine(monkeypatch, db=None, redis=None):
    """
    构建注入 mock 依赖的 TradingEngine。

    返回 (engine, transitions, notifications):
      - transitions: transition_order_status 的调用记录
      - notifications: _safe_schedule_notification 的调用记录（每条为通知 kwargs dict）
    """
    db = db or _FakeDb()
    redis = redis or _FakeRedis()
    engine = TradingEngine(db=db, redis=redis)

    transitions = []
    notifications = []

    async def _fake_transition(order, new_status, remarks=None):
        transitions.append(
            {"order_id": order.order_id, "new_status": new_status, "remarks": remarks}
        )
        if remarks:
            order.remarks = (
                f"{order.remarks or ''} [{new_status.value.upper()}: {remarks}]"
            ).strip()
        order.status = new_status
        if new_status == OrderStatus.SUBMITTED:
            order.submitted_at = datetime.now()
        elif new_status == OrderStatus.FILLED:
            order.filled_at = datetime.now()
        return order

    def _track_notification(coro):
        notifications.append(coro)

    monkeypatch.setattr(
        engine.order_service, "transition_order_status", _fake_transition
    )
    monkeypatch.setattr(
        "backend.services.trade.services.trading_engine._safe_schedule_notification",
        _track_notification,
    )
    monkeypatch.setattr(
        "backend.services.trade.services.trading_engine.publish_notification_async",
        lambda **kw: kw,
    )
    monkeypatch.setattr(engine, "_sync_account_to_redis", _noop_async)

    return engine, transitions, notifications


# ==================== 场景A：本地写成功 + 外部提交成功 → FILLED ====================


@pytest.mark.asyncio
async def test_scenario_a_local_persist_and_broker_fill(monkeypatch):
    """场景A：本地写成功 + 外部提交成功 → 订单 FILLED"""
    engine, transitions, notifications = _setup_engine(monkeypatch)
    order = _make_order()

    broker_calls = []

    class _FillBroker:
        async def place_order(self, **kwargs):
            broker_calls.append(kwargs)
            # 本地优先断言：broker 被调用时订单必须已是 SUBMITTED（本地已落库）
            assert order.status == OrderStatus.SUBMITTED, (
                "本地优先原则违反：broker.place_order 调用时订单应为 SUBMITTED，"
                f"实际为 {order.status}"
            )
            return BrokerResult(
                success=True,
                filled_quantity=100,
                filled_price=10.5,
                commission=3.15,
                exchange_order_id="EXCH-001",
                message="filled",
            )

    monkeypatch.setattr(engine, "_get_stock_broker", lambda _mode: _FillBroker())

    async def _fake_create_trade(order, quantity, price, commission=0.0, **kw):
        order.filled_quantity = quantity
        order.filled_value = quantity * price
        order.average_price = price
        order.status = OrderStatus.FILLED
        order.filled_at = datetime.now()
        return SimpleNamespace(trade_id=uuid4())

    monkeypatch.setattr(engine.trade_service, "create_trade", _fake_create_trade)

    result = await engine.submit_order(order, tenant_id="default")

    # 验证：本地先 SUBMITTED，再 broker 成交，最终 FILLED
    assert [t["new_status"] for t in transitions] == [OrderStatus.SUBMITTED]
    assert len(broker_calls) == 1
    assert broker_calls[0]["symbol"] == "SH600036"
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == 100
    assert order.average_price == 10.5
    assert order.exchange_order_id == "EXCH-001"
    assert result["success"] is True
    assert result["status"] == "filled"
    # 成交通知已发送
    assert any(n.get("title") == "订单成交确认" for n in notifications)


# ==================== 场景B：本地写成功 + 外部提交失败(Broker拒绝) → REJECTED ====================


@pytest.mark.asyncio
async def test_scenario_b_broker_rejected_with_compensation(monkeypatch):
    """场景B：本地写成功 + 外部提交失败(Broker拒绝) → 订单 REJECTED + 补偿验证"""
    engine, transitions, notifications = _setup_engine(monkeypatch)
    order = _make_order()

    class _RejectBroker:
        async def place_order(self, **kwargs):
            assert order.status == OrderStatus.SUBMITTED, (
                "本地优先原则：broker 拒绝前订单必须已 SUBMITTED"
            )
            return BrokerResult(success=False, message="Insufficient funds")

    monkeypatch.setattr(engine, "_get_stock_broker", lambda _mode: _RejectBroker())

    result = await engine.submit_order(order, tenant_id="default")

    # 验证状态转换链：PENDING -> SUBMITTED -> REJECTED
    status_seq = [t["new_status"] for t in transitions]
    assert OrderStatus.SUBMITTED in status_seq
    assert OrderStatus.REJECTED in status_seq
    assert status_seq.index(OrderStatus.SUBMITTED) < status_seq.index(
        OrderStatus.REJECTED
    )

    # 订单最终 REJECTED
    assert order.status == OrderStatus.REJECTED
    assert result["success"] is False
    assert result["status"] == "rejected"

    # 补偿验证1：无成交
    assert order.filled_quantity == 0
    assert order.filled_value == 0.0

    # 补偿验证2：拒绝通知已发送
    reject_notifies = [n for n in notifications if n.get("title") == "订单被拒绝"]
    assert len(reject_notifies) == 1
    assert "SH600036" in reject_notifies[0]["content"]

    # 补偿验证3：remarks 包含拒绝原因
    assert "Broker拒绝" in (order.remarks or "")


# ==================== 场景C：本地写成功 + 外部提交超时 → AWAITING_BRIDGE_ACK ====================


@pytest.mark.asyncio
async def test_scenario_c_broker_timeout_marks_awaiting_ack(monkeypatch):
    """场景C-1：本地写成功 + 外部提交超时 → 订单标记 AWAITING_BRIDGE_ACK"""
    engine, transitions, notifications = _setup_engine(monkeypatch)
    order = _make_order(trading_mode=TradingMode.REAL)

    class _TimeoutBroker:
        async def place_order(self, **kwargs):
            assert order.status == OrderStatus.SUBMITTED, (
                "本地优先原则：broker 超时前订单必须已 SUBMITTED"
            )
            raise asyncio.TimeoutError()

    monkeypatch.setattr(engine, "_get_stock_broker", lambda _mode: _TimeoutBroker())

    result = await engine.submit_order(order, tenant_id="default")

    # 超时绝对不能直接标记为失败，订单保持 SUBMITTED
    assert order.status == OrderStatus.SUBMITTED
    assert result["success"] is True  # 非 REJECTED 视为提交成功（待回报）

    # 验证标记
    assert "[AWAITING_BRIDGE_ACK]" in (order.remarks or "")
    assert "[BRIDGE_ACK_TIMEOUT_PENDING_REVIEW]" in (order.remarks or "")

    # 待核查通知已发送
    review_notifies = [n for n in notifications if n.get("title") == "订单待核查"]
    assert len(review_notifies) == 1

    # 验证：transition 被调用（SUBMITTED + 超时 remarks 更新），但没有 REJECTED
    rejected_calls = [t for t in transitions if t["new_status"] == OrderStatus.REJECTED]
    assert len(rejected_calls) == 0, "超时不应标记 REJECTED"


@pytest.mark.asyncio
async def test_scenario_c_timeout_scanner_intervenes(monkeypatch):
    """场景C-2：超时扫描器介入，识别 AWAITING_BRIDGE_ACK 订单并标记待核查"""
    order = _make_order(
        trading_mode=TradingMode.REAL,
        status=OrderStatus.SUBMITTED,
        submitted_at=datetime.now() - timedelta(seconds=300),
        exchange_order_id=None,
        remarks="[AWAITING_BRIDGE_ACK]",
    )
    session = _FakeSession([order])
    notifications = []

    monkeypatch.setattr(
        "backend.services.trade.services.order_timeout_scanner.get_session",
        lambda: _FakeSessionContext(session),
    )

    async def _fake_notification(**kwargs):
        notifications.append(kwargs)
        return None

    monkeypatch.setattr(
        order_timeout_scanner, "publish_notification_async", _fake_notification
    )
    monkeypatch.setattr(order_timeout_scanner, "_BRIDGE_ACK_TIMEOUT_SECONDS", 120)

    count = await order_timeout_scanner._scan_bridge_ack_timeout_once()

    # 扫描器识别到该订单
    assert count == 1
    assert session.committed is True
    # 订单状态不变（不误判 EXPIRED/REJECTED）
    assert order.status == OrderStatus.SUBMITTED
    assert "[BRIDGE_ACK_TIMEOUT_PENDING_REVIEW]" in (order.remarks or "")
    assert notifications[0]["title"] == "桥接回报超时待核查"


@pytest.mark.asyncio
async def test_scenario_c_reconcile_scanner_queues_pending_review(monkeypatch):
    """场景C-3：对账扫描器将 [BRIDGE_ACK_TIMEOUT_PENDING_REVIEW] 订单入队，避免误判 EXPIRED"""
    order = _make_order(
        trading_mode=TradingMode.REAL,
        status=OrderStatus.SUBMITTED,
        submitted_at=datetime.now() - timedelta(minutes=45),
        exchange_order_id=None,
        remarks="[AWAITING_BRIDGE_ACK] [BRIDGE_ACK_TIMEOUT_PENDING_REVIEW]",
        client_order_id="recon-cid-001",
    )
    session = _FakeSession([order])
    notifications = []

    monkeypatch.setattr(
        "backend.services.trade.services.order_timeout_scanner.get_session",
        lambda: _FakeSessionContext(session),
    )

    async def _fake_notification(**kwargs):
        notifications.append(kwargs)
        return None

    monkeypatch.setattr(
        order_timeout_scanner, "publish_notification_async", _fake_notification
    )

    # 对账扫描器入队
    count = await order_timeout_scanner._scan_reconcile_candidates_once()
    assert count == 1
    assert "[RECONCILE_QUEUED]" in (order.remarks or "")
    assert "recon-cid-001" in (order.remarks or "")
    assert any(n["title"] == "订单进入对账队列" for n in notifications)

    # 验证：入队后 _scan_once 不再将其判为 EXPIRED
    # 使用 _FilteringSession 模拟 SQL NOT LIKE '%[RECONCILE_QUEUED]%' 过滤
    session2 = _FilteringSession([order])
    monkeypatch.setattr(
        "backend.services.trade.services.order_timeout_scanner.get_session",
        lambda: _FakeSessionContext(session2),
    )
    expired_count = await order_timeout_scanner._scan_once()
    assert expired_count == 0, "已入对账队列的订单不应被直接判为 EXPIRED"


# ==================== 场景D：本地写成功 + 外部提交崩溃(Exception) → REJECTED ====================


@pytest.mark.asyncio
async def test_scenario_d_broker_crash_rejected(monkeypatch):
    """场景D：本地写成功 + 外部提交崩溃(Exception) → 订单 REJECTED + 通知已发送"""
    engine, transitions, notifications = _setup_engine(monkeypatch)
    order = _make_order()

    class _CrashBroker:
        async def place_order(self, **kwargs):
            assert order.status == OrderStatus.SUBMITTED, (
                "本地优先原则：broker 崩溃前订单必须已 SUBMITTED"
            )
            raise RuntimeError("Broker process crashed")

    monkeypatch.setattr(engine, "_get_stock_broker", lambda _mode: _CrashBroker())

    result = await engine.submit_order(order, tenant_id="default")

    # 订单最终 REJECTED
    assert order.status == OrderStatus.REJECTED
    assert result["success"] is False
    assert result["status"] == "rejected"

    # 通知已发送
    crash_notifies = [n for n in notifications if n.get("title") == "订单执行失败"]
    assert len(crash_notifies) == 1
    assert "Broker process crashed" in crash_notifies[0]["content"]

    # remarks 包含异常信息
    assert "执行异常" in (order.remarks or "")

    # 无成交
    assert order.filled_quantity == 0


# ==================== 本地优先原则强化验证 ====================


@pytest.mark.asyncio
async def test_local_first_persistence_failure_aborts_broker(monkeypatch):
    """
    本地优先原则核心验证：
    transition SUBMITTED 落库失败时，绝不调用 broker.place_order。
    """
    engine, transitions, notifications = _setup_engine(monkeypatch)

    # 用真实 OrderService + commit 失败的 DB 重新构建
    fail_db = _CommitFailDb()
    engine_fail = TradingEngine(db=fail_db, redis=_FakeRedis())
    order = _make_order()

    broker_called = []

    class _AnyBroker:
        async def place_order(self, **kwargs):
            broker_called.append(kwargs)
            return BrokerResult(success=True, filled_quantity=100, filled_price=10.0)

    monkeypatch.setattr(engine_fail, "_get_stock_broker", lambda _mode: _AnyBroker())
    monkeypatch.setattr(
        "backend.services.trade.services.trading_engine._safe_schedule_notification",
        lambda _c: None,
    )
    monkeypatch.setattr(
        "backend.services.trade.services.trading_engine.publish_notification_async",
        lambda **kw: kw,
    )
    monkeypatch.setattr(engine_fail, "_sync_account_to_redis", _noop_async)

    result = await engine_fail.submit_order(order, tenant_id="default")

    # 核心断言：本地落库失败，broker 绝不能被调用
    assert len(broker_called) == 0, (
        "本地优先原则违反：SUBMITTED 落库失败后不应调用 broker.place_order"
    )
    assert result["success"] is False
    assert "Local persistence failed" in result["message"]
    # 订单状态回滚（commit 失败后内存状态被恢复）
    # 由于 commit 失败，order.status 应保持 PENDING 或被标记 REJECTED（取决于二次 transition）
    assert order.status in (OrderStatus.PENDING, OrderStatus.REJECTED)
    # commit 至少被调用过（SUBMITTED 尝试 + 可能的 REJECTED 尝试）
    assert fail_db.commit_calls >= 1


@pytest.mark.asyncio
async def test_transition_order_status_rollback_on_commit_failure(monkeypatch):
    """
    验证 OrderService.transition_order_status 在 commit 失败时回滚内存状态。
    确保“假成功”不会导致调用方继续外部提交。
    """
    fail_db = _CommitFailDb()
    redis = _FakeRedis()
    service = OrderService(db=fail_db, redis=redis)
    order = _make_order(status=OrderStatus.PENDING)

    original_status = order.status
    original_remarks = order.remarks

    with pytest.raises(RuntimeError, match="DB connection lost"):
        await service.transition_order_status(order, OrderStatus.SUBMITTED)

    # commit 失败后，内存状态必须回滚到原始值
    assert order.status == original_status, (
        "commit 失败后内存状态必须回滚，避免调用方基于假成功继续外部提交"
    )
    assert order.remarks == original_remarks
    # rollback 被调用
    assert fail_db.rollback_calls == 1
    # commit 被尝试过
    assert fail_db.commit_calls == 1


# ==================== 补充：Bridge ACK remarks 落库失败不影响订单状态 ====================


@pytest.mark.asyncio
async def test_bridge_ack_remarks_commit_failure_keeps_submitted(monkeypatch):
    """
    验证 Bridge ACK 分支：broker 已成功派发后，remarks 落库失败不应导致 REJECTED。
    """
    engine, transitions, notifications = _setup_engine(monkeypatch, db=_CommitFailDb())
    order = _make_order(trading_mode=TradingMode.REAL)

    class _DispatchBroker:
        async def place_order(self, **kwargs):
            # 返回成功但无即时成交（Bridge 派发模式）
            return BrokerResult(
                success=True,
                filled_quantity=0,
                filled_price=0,
                exchange_order_id="",
                message="bridge dispatched",
            )

    monkeypatch.setattr(engine, "_get_stock_broker", lambda _mode: _DispatchBroker())

    result = await engine.submit_order(order, tenant_id="default")

    # 订单保持 SUBMITTED，不被误判 REJECTED
    assert order.status == OrderStatus.SUBMITTED, (
        "Bridge 派发成功后 remarks 落库失败不应导致 REJECTED"
    )
    # 无 REJECTED 转换
    rejected = [t for t in transitions if t["new_status"] == OrderStatus.REJECTED]
    assert len(rejected) == 0
    assert result["success"] is True
