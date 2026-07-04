"""
订单对账闭环测试 (T4.1-followup)

覆盖场景：
  A: 对账查询返回 FILLED → 本地状态更新为 FILLED
  B: 对账查询返回 CANCELLED → 本地状态更新为 CANCELLED
  C: 对账查询返回 STILL_PENDING → 保持 RECONCILE_QUEUED
  D: 对账查询连续失败5次 → 标记 RECONCILE_FAILED_MANUAL_REVIEW

额外验证：
  - MockBroker.query_order / OrderStatusResult 数据结构可用性
  - 对账闭环后 _scan_once 不会误判已闭环订单
"""

from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.trade.models.enums import OrderStatus, TradingMode
from backend.services.trade.services import order_timeout_scanner
from backend.services.trade.services.broker_client import (
    MockBroker,
    OrderStatusResult,
)


# ==================== Fake 依赖 ====================


class _ScalarResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return list(self._values)


class _ReconcileSession:
    """支持 RECONCILE_QUEUED / RECONCILE_FAILED 过滤的 fake session。

    模拟两种 SQL 过滤：
      1. 对账闭环查询: remarks LIKE '%[RECONCILE_QUEUED]%'
         AND remarks NOT LIKE '%[RECONCILE_FAILED_MANUAL_REVIEW]%'
      2. _scan_once 查询: remarks NOT LIKE '%[RECONCILE_QUEUED]%'
    """

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
        if "RECONCILE_FAILED_MANUAL_REVIEW" in stmt_str:
            # 对账闭环查询：包含 [RECONCILE_QUEUED] 且不包含 [RECONCILE_FAILED_MANUAL_REVIEW]
            result = [
                o
                for o in result
                if "[RECONCILE_QUEUED]" in (getattr(o, "remarks", "") or "")
                and "[RECONCILE_FAILED_MANUAL_REVIEW]"
                not in (getattr(o, "remarks", "") or "")
            ]
        elif "RECONCILE_QUEUED" in stmt_str:
            # _scan_once 查询：NOT LIKE '%[RECONCILE_QUEUED]%' → 排除已入队订单
            result = [
                o
                for o in result
                if "[RECONCILE_QUEUED]"
                not in (getattr(o, "remarks", "") or "")
            ]
        return _ScalarResult(result)

    async def commit(self):
        self.committed = True


class _FakeSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


class _FakeReconcileBroker:
    """可配置的对账用 fake broker。"""

    def __init__(self, query_result=None, query_exception=None):
        self._query_result = query_result
        self._query_exception = query_exception
        self.query_calls = []

    async def query_order(self, client_order_id):
        self.query_calls.append(client_order_id)
        if self._query_exception is not None:
            raise self._query_exception
        return self._query_result


# ==================== Helpers ====================


def _make_reconcile_order(**overrides):
    """构建对账测试用订单（SH600036 前缀格式，REAL 模式，RECONCILE_QUEUED）。"""
    defaults = {
        "order_id": uuid4(),
        "tenant_id": "default",
        "user_id": "1001",
        "symbol": "SH600036",
        "client_order_id": "recon-cid-001",
        "status": OrderStatus.SUBMITTED,
        "trading_mode": TradingMode.REAL,
        "remarks": (
            "[AWAITING_BRIDGE_ACK] [BRIDGE_ACK_TIMEOUT_PENDING_REVIEW] "
            "[RECONCILE_QUEUED]"
        ),
        "filled_quantity": 0.0,
        "average_price": None,
        "filled_value": 0.0,
        "filled_at": None,
        "cancelled_at": None,
        "expired_at": None,
        "exchange_order_id": None,
        "submitted_at": datetime.now() - timedelta(minutes=45),
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _setup_reconcile(monkeypatch, broker, orders):
    """设置对账闭环测试环境，返回 (session, notifications)。"""
    session = _ReconcileSession(orders)
    notifications = []

    monkeypatch.setattr(
        "backend.services.trade.services.order_timeout_scanner.get_session",
        lambda: _FakeSessionContext(session),
    )
    monkeypatch.setattr(
        order_timeout_scanner, "_get_reconcile_broker", lambda: broker
    )

    async def _fake_notification(**kwargs):
        notifications.append(kwargs)
        return None

    monkeypatch.setattr(
        order_timeout_scanner, "publish_notification_async", _fake_notification
    )
    return session, notifications


# ==================== 场景A：对账查询返回 FILLED → 本地状态更新为 FILLED ====================


@pytest.mark.asyncio
async def test_scenario_a_reconcile_filled(monkeypatch):
    """场景A：对账查询返回 FILLED → 本地状态更新为 FILLED"""
    order = _make_reconcile_order(client_order_id="recon-fill-001")
    broker = _FakeReconcileBroker(
        query_result=OrderStatusResult(
            status="FILLED",
            filled_quantity=100,
            avg_price=10.5,
            exchange_order_id="EXCH-RECON-001",
            message="filled",
        )
    )
    session, notifications = _setup_reconcile(monkeypatch, broker, [order])

    count = await order_timeout_scanner._reconcile_queued_orders_once()

    # 验证：订单状态更新为 FILLED，成交信息已记录
    assert count == 1
    assert session.committed is True
    assert order.status == OrderStatus.FILLED
    assert order.filled_quantity == 100
    assert order.average_price == 10.5
    assert order.filled_value == 1050.0
    assert order.filled_at is not None
    assert order.exchange_order_id == "EXCH-RECON-001"
    assert "[RECONCILE_FILLED:" in (order.remarks or "")
    assert "qty=100" in (order.remarks or "")

    # broker 被调用一次，传入正确的 client_order_id
    assert len(broker.query_calls) == 1
    assert broker.query_calls[0] == "recon-fill-001"

    # 成交通知已发送
    filled_notifies = [
        n for n in notifications if n["title"] == "对账确认成交"
    ]
    assert len(filled_notifies) == 1
    assert "SH600036" in filled_notifies[0]["content"]


# ==================== 场景B：对账查询返回 CANCELLED → 本地状态更新为 CANCELLED ====================


@pytest.mark.asyncio
async def test_scenario_b_reconcile_cancelled(monkeypatch):
    """场景B：对账查询返回 CANCELLED → 本地状态更新为 CANCELLED"""
    order = _make_reconcile_order(client_order_id="recon-cancel-001")
    broker = _FakeReconcileBroker(
        query_result=OrderStatusResult(
            status="CANCELLED",
            exchange_order_id="EXCH-RECON-002",
            message="cancelled by user",
        )
    )
    session, notifications = _setup_reconcile(monkeypatch, broker, [order])

    count = await order_timeout_scanner._reconcile_queued_orders_once()

    # 验证：订单状态更新为 CANCELLED
    assert count == 1
    assert order.status == OrderStatus.CANCELLED
    assert order.cancelled_at is not None
    assert "[RECONCILE_CANCELLED]" in (order.remarks or "")
    assert len(broker.query_calls) == 1
    assert broker.query_calls[0] == "recon-cancel-001"

    # 撤单通知已发送
    cancel_notifies = [
        n for n in notifications if n["title"] == "对账确认撤单"
    ]
    assert len(cancel_notifies) == 1


# ==================== 场景C：对账查询返回 STILL_PENDING → 保持 RECONCILE_QUEUED ====================


@pytest.mark.asyncio
async def test_scenario_c_reconcile_still_pending(monkeypatch):
    """场景C：对账查询返回 STILL_PENDING → 保持 RECONCILE_QUEUED"""
    order = _make_reconcile_order(client_order_id="recon-pending-001")
    broker = _FakeReconcileBroker(
        query_result=OrderStatusResult(
            status="STILL_PENDING",
            message="order still pending at exchange",
        )
    )
    session, notifications = _setup_reconcile(monkeypatch, broker, [order])

    count = await order_timeout_scanner._reconcile_queued_orders_once()

    # 验证：状态保持 SUBMITTED，未被误判为终态
    assert count == 1
    assert order.status == OrderStatus.SUBMITTED

    # 仍保留 RECONCILE_QUEUED 标记，未被移除
    assert "[RECONCILE_QUEUED]" in (order.remarks or "")

    # 不应包含 RECONCILE_FAILED（单次 STILL_PENDING 不足以标记失败）
    assert "[RECONCILE_FAILED_MANUAL_REVIEW]" not in (order.remarks or "")

    # 应记录查询次数（第一次查询）
    assert "[RECONCILE_QUERY_ATTEMPTS:1]" in (order.remarks or "")
    assert "[RECONCILE_STILL_PENDING:" in (order.remarks or "")

    assert len(broker.query_calls) == 1

    # 单次查询未达阈值，不应发送人工介入通知
    assert not any(
        n["title"] == "对账失败待人工介入" for n in notifications
    )


# ==================== 场景D：连续失败5次 → RECONCILE_FAILED_MANUAL_REVIEW ====================


@pytest.mark.asyncio
async def test_scenario_d_reconcile_max_failures(monkeypatch):
    """场景D：对账查询连续失败5次 → 标记 RECONCILE_FAILED_MANUAL_REVIEW"""
    order = _make_reconcile_order(client_order_id="recon-fail-001")
    broker = _FakeReconcileBroker(
        query_exception=ConnectionError("broker connection lost")
    )
    session, notifications = _setup_reconcile(monkeypatch, broker, [order])
    monkeypatch.setattr(
        order_timeout_scanner, "_RECONCILE_MAX_QUERY_ATTEMPTS", 5
    )

    # 连续调用5次，每次查询都失败
    for i in range(5):
        count = await order_timeout_scanner._reconcile_queued_orders_once()
        assert count == 1, f"attempt {i + 1} should process the order"

    # 5次后应标记 RECONCILE_FAILED_MANUAL_REVIEW
    assert "[RECONCILE_FAILED_MANUAL_REVIEW]" in (order.remarks or "")
    assert "[RECONCILE_QUERY_ATTEMPTS:5]" in (order.remarks or "")
    assert "MANUAL_REVIEW" in (order.remarks or "")

    # 状态保持 SUBMITTED（未被误判为终态，等待人工核查）
    assert order.status == OrderStatus.SUBMITTED

    # broker.query_order 被调用5次
    assert len(broker.query_calls) == 5
    assert all(c == "recon-fail-001" for c in broker.query_calls)

    # 人工介入通知已发送（仅第5次触发，不重复）
    failed_notifies = [
        n for n in notifications if n["title"] == "对账失败待人工介入"
    ]
    assert len(failed_notifies) == 1
    assert "SH600036" in failed_notifies[0]["content"]
    assert "5" in failed_notifies[0]["content"]

    # 第6次调用：已标记 RECONCILE_FAILED 的订单不应再被处理（幂等性验证）
    count6 = await order_timeout_scanner._reconcile_queued_orders_once()
    assert count6 == 0, (
        "已标记 RECONCILE_FAILED_MANUAL_REVIEW 的订单不应再被对账扫描器处理"
    )
    assert len(broker.query_calls) == 5  # 未增加


# ==================== 补充：MockBroker + OrderStatusResult 数据结构验证 ====================


@pytest.mark.asyncio
async def test_mockbroker_query_order_returns_preset_result():
    """验证 MockBroker.query_order 能正确返回预设的 OrderStatusResult。"""
    broker = MockBroker()
    expected = OrderStatusResult(
        status="FILLED",
        filled_quantity=200,
        avg_price=9.8,
        exchange_order_id="EXCH-MOCK-001",
    )
    broker.set_order_result("mock-cid-001", expected)

    result = await broker.query_order("mock-cid-001")

    assert result.status == "FILLED"
    assert result.filled_quantity == 200
    assert result.avg_price == 9.8
    assert result.exchange_order_id == "EXCH-MOCK-001"
    assert "mock-cid-001" in broker.query_order_calls


@pytest.mark.asyncio
async def test_mockbroker_query_order_default_still_pending():
    """验证 MockBroker.query_order 未预设时返回 STILL_PENDING。"""
    broker = MockBroker()
    result = await broker.query_order("unknown-cid")
    assert result.status == "STILL_PENDING"


# ==================== 补充：对账闭环后 _scan_once 不误判 ====================


@pytest.mark.asyncio
async def test_reconciled_filled_order_not_expired_by_scan_once(monkeypatch):
    """验证：对账闭环标记 FILLED 后，_scan_once 不会将其误判为 EXPIRED。"""
    order = _make_reconcile_order(client_order_id="recon-scan-001")
    broker = _FakeReconcileBroker(
        query_result=OrderStatusResult(
            status="FILLED",
            filled_quantity=100,
            avg_price=10.0,
            exchange_order_id="EXCH-SCAN-001",
        )
    )
    session, _ = _setup_reconcile(monkeypatch, broker, [order])

    # 对账闭环 → FILLED
    await order_timeout_scanner._reconcile_queued_orders_once()
    assert order.status == OrderStatus.FILLED

    # _scan_once 查询 SUBMITTED 订单，FILLED 订单不会被查到
    expired_count = await order_timeout_scanner._scan_once()
    assert expired_count == 0
