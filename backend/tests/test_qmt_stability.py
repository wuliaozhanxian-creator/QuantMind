"""
QMT 对接稳定性强化测试 (T4.3)

覆盖场景：
  1. 连接断开自动重连
     A: 指数退避序列计算 (1s/2s/4s/8s/16s/32s/60s/60s)
     B: 状态机流转 CONNECTED -> DISCONNECTED -> RECONNECTING -> CONNECTED
     C: 心跳检测失败 -> 标记断开 -> 触发重连
     D: 连续重连失败超过 10 次 -> 标记 FAILED + 回调通知
     E: 重连成功 -> 重置重连计数
     F: is_available 在 FAILED 状态返回 False
  2. 订单状态查询轮询
     G: 轮询查询返回 FILLED -> 本地状态更新为 FILLED
     H: 轮询查询返回 CANCELLED -> 本地状态更新为 CANCELLED
     I: 轮询查询返回 STILL_PENDING -> 保持 SUBMITTED + 记录查询时间
     J: 轮询跳过已在对账队列的订单
     K: 轮询节流 — 距上次查询未满间隔则跳过
     L: 轮询辅助函数 _parse/_write_poll_last_query
  3. 回报解析容错
     M: _safe_float / _safe_str 容错
     N: order_filled 无效 filled_qty -> ValueError + 异常记录
     O: QMT 状态映射（已成交/部成/已撤/废单/未成交）
  4. query_order 双层查询
     P: QMTBridgeBroker.query_order bridge 端点成功
     Q: QMTBridgeBroker.query_order bridge 404 -> DB 回退
     R: QMTBroker.query_order HTTP 成功

约束：不连接外部数据库，所有 DB 访问通过 monkeypatch 注入 fake session。
"""

import asyncio
from datetime import datetime, timedelta
from types import SimpleNamespace
from uuid import uuid4

import pytest

from backend.services.trade.models.enums import OrderStatus, TradingMode
from backend.services.trade.services import order_timeout_scanner
from backend.services.trade.services.broker_client import (
    BrokerConnectionManager,
    BrokerConnectionState,
    MockBroker,
    OrderStatusResult,
    QMTBroker,
    QMTBridgeBroker,
    compute_reconnect_backoff,
)
from backend.services.trade.services.execution_stream_consumer import (
    ExecutionStreamConsumer,
)


# ==================== 场景 A: 指数退避序列计算 ====================


class TestComputeReconnectBackoff:
    """验证指数退避序列：1s, 2s, 4s, 8s, 16s, 32s, 60s, 60s, ..."""

    def test_backoff_sequence(self):
        expected = [1.0, 2.0, 4.0, 8.0, 16.0, 32.0, 60.0, 60.0, 60.0, 60.0]
        actual = [
            compute_reconnect_backoff(i, base=1.0, max_interval=60.0)
            for i in range(1, 11)
        ]
        assert actual == expected

    def test_backoff_attempt_zero_returns_base(self):
        assert compute_reconnect_backoff(0) == 1.0

    def test_backoff_respects_max_interval(self):
        assert compute_reconnect_backoff(100, max_interval=30.0) == 30.0

    def test_backoff_custom_base(self):
        assert compute_reconnect_backoff(1, base=2.0) == 2.0
        assert compute_reconnect_backoff(2, base=2.0) == 4.0
        assert compute_reconnect_backoff(3, base=2.0) == 8.0


# ==================== 场景 B-F: 连接状态机 & 自动重连 ====================


class _NoSleep:
    """无延迟 sleep 替身（测试用，避免真实等待）。

    使用 asyncio.sleep(0) 让出事件循环控制权，避免紧密循环阻塞其他任务。
    """

    async def __call__(self, seconds: float):
        # 让出控制权但不实际等待
        await asyncio.sleep(0)


class TestBrokerConnectionManager:
    """连接状态机管理器测试。"""

    @pytest.mark.asyncio
    async def test_b_state_machine_connect_disconnect_reconnect(self):
        """场景B: 状态流转 CONNECTED -> DISCONNECTED -> RECONNECTING -> CONNECTED"""
        call_count = {"n": 0}

        async def health_check():
            call_count["n"] += 1
            # 第2次起成功
            return call_count["n"] >= 2

        manager = BrokerConnectionManager(
            broker_name="test-broker",
            health_check_func=health_check,
        )
        manager._sleep_func = _NoSleep()
        # 不启动心跳，仅测试重连逻辑
        manager._running = True
        await manager._set_state(BrokerConnectionState.CONNECTED)
        assert manager.state == BrokerConnectionState.CONNECTED

        # 模拟连接断开
        await manager.mark_disconnected()
        assert manager.state in (
            BrokerConnectionState.DISCONNECTED,
            BrokerConnectionState.RECONNECTING,
        )

        # 等待重连任务完成
        if manager._reconnect_task is not None:
            await asyncio.wait_for(manager._reconnect_task, timeout=5.0)

        # 第2次健康检查成功 -> 重连成功
        assert manager.state == BrokerConnectionState.CONNECTED
        assert manager.reconnect_attempts == 0  # 重置
        manager._running = False

    @pytest.mark.asyncio
    async def test_c_heartbeat_failure_triggers_disconnect(self):
        """场景C: 心跳检测失败 -> 标记断开 -> 触发重连"""
        check_results = [True, False, True]  # 第1次正常，第2次失败，第3次恢复
        idx = {"i": 0}

        async def health_check():
            i = idx["i"]
            idx["i"] += 1
            if i < len(check_results):
                return check_results[i]
            return True

        manager = BrokerConnectionManager(
            broker_name="test-hb",
            health_check_func=health_check,
            heartbeat_interval=0.01,  # 极短间隔加速测试
        )
        # 使用真实 sleep（极短）以允许事件循环切换
        manager._sleep_func = asyncio.sleep

        await manager.start()
        assert manager.state == BrokerConnectionState.CONNECTED

        # 等待足够时间让心跳触发断开+重连
        await asyncio.sleep(0.15)

        # 心跳失败后应触发重连，最终恢复 CONNECTED
        if manager._reconnect_task is not None:
            await asyncio.wait_for(manager._reconnect_task, timeout=5.0)

        assert manager.state == BrokerConnectionState.CONNECTED
        await manager.stop()

    @pytest.mark.asyncio
    async def test_d_max_reconnect_failures_marked_failed(self):
        """场景D: 连续重连失败超过 10 次 -> 标记 FAILED + 回调通知"""
        failure_calls = []

        async def always_fail():
            return False

        async def on_failure(broker_name, reason):
            failure_calls.append((broker_name, reason))

        manager = BrokerConnectionManager(
            broker_name="test-fail",
            health_check_func=always_fail,
            on_failure_callback=on_failure,
        )
        manager._sleep_func = _NoSleep()
        manager._running = True
        await manager._set_state(BrokerConnectionState.CONNECTED)

        await manager.mark_disconnected()

        # 等待重连任务耗尽 10 次尝试
        if manager._reconnect_task is not None:
            await asyncio.wait_for(manager._reconnect_task, timeout=10.0)

        assert manager.state == BrokerConnectionState.FAILED
        assert manager.reconnect_attempts == manager.MAX_RECONNECT_ATTEMPTS
        assert len(failure_calls) == 1
        assert failure_calls[0][0] == "test-fail"
        assert "10" in failure_calls[0][1]
        manager._running = False

    @pytest.mark.asyncio
    async def test_e_successful_reconnect_resets_attempts(self):
        """场景E: 重连成功 -> 重置重连计数"""
        call_count = {"n": 0}

        async def health_check():
            call_count["n"] += 1
            return call_count["n"] >= 2  # 第2次起成功

        manager = BrokerConnectionManager(
            broker_name="test-reset",
            health_check_func=health_check,
        )
        manager._sleep_func = _NoSleep()
        manager._running = True
        await manager._set_state(BrokerConnectionState.CONNECTED)

        await manager.mark_disconnected()

        if manager._reconnect_task is not None:
            await asyncio.wait_for(manager._reconnect_task, timeout=5.0)

        assert manager.state == BrokerConnectionState.CONNECTED
        assert manager.reconnect_attempts == 0
        manager._running = False

    @pytest.mark.asyncio
    async def test_f_is_available_returns_false_when_failed(self):
        """场景F: FAILED 状态下 is_available 返回 False"""

        async def always_fail():
            return False

        manager = BrokerConnectionManager(
            broker_name="test-unavail",
            health_check_func=always_fail,
        )
        manager._sleep_func = _NoSleep()
        manager._running = True
        await manager._set_state(BrokerConnectionState.CONNECTED)

        await manager.mark_disconnected()

        if manager._reconnect_task is not None:
            await asyncio.wait_for(manager._reconnect_task, timeout=10.0)

        assert manager.state == BrokerConnectionState.FAILED
        assert manager.is_available() is False
        manager._running = False

    @pytest.mark.asyncio
    async def test_backoff_sequence_recorded(self):
        """验证重连过程中的退避序列被正确记录。"""

        async def always_fail():
            return False

        manager = BrokerConnectionManager(
            broker_name="test-backoff",
            health_check_func=always_fail,
        )
        manager._sleep_func = _NoSleep()
        manager._running = True
        # 缩小最大尝试次数加速测试
        manager.MAX_RECONNECT_ATTEMPTS = 5
        await manager._set_state(BrokerConnectionState.CONNECTED)

        await manager.mark_disconnected()

        if manager._reconnect_task is not None:
            await asyncio.wait_for(manager._reconnect_task, timeout=10.0)

        # 验证退避序列：1, 2, 4, 8, 16
        assert manager._reconnect_sleeps == [1.0, 2.0, 4.0, 8.0, 16.0]
        manager._running = False

    @pytest.mark.asyncio
    async def test_force_reconnect(self):
        """验证手动重连。"""

        async def ok_check():
            return True

        manager = BrokerConnectionManager(
            broker_name="test-force",
            health_check_func=ok_check,
        )
        manager._sleep_func = _NoSleep()

        result = await manager.force_reconnect()
        assert result is True
        assert manager.state == BrokerConnectionState.CONNECTED


# ==================== 场景 G-L: 订单状态查询轮询 ====================


class _ScalarResult:
    def __init__(self, values):
        self._values = values

    def scalars(self):
        return self

    def all(self):
        return list(self._values)


class _PollSession:
    """支持轮询查询过滤的 fake session。"""

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
        # 轮询查询过滤：SUBMITTED + REAL + 未 RECONCILE_QUEUED + 未 BRIDGE_ACK_TIMEOUT
        if "RECONCILE_QUEUED" in stmt_str or "BRIDGE_ACK_TIMEOUT" in stmt_str:
            filtered = []
            for o in result:
                remarks = getattr(o, "remarks", "") or ""
                if (
                    "[RECONCILE_QUEUED]" not in remarks
                    and "[BRIDGE_ACK_TIMEOUT_PENDING_REVIEW]" not in remarks
                ):
                    filtered.append(o)
            return _ScalarResult(filtered)
        return _ScalarResult(result)

    async def commit(self):
        self.committed = True


class _PollSessionContext:
    def __init__(self, session):
        self.session = session

    async def __aenter__(self):
        return self.session

    async def __aexit__(self, exc_type, exc, tb):
        return False


def _make_poll_order(**overrides):
    """构建轮询测试用订单（SH600036 前缀格式，REAL+SUBMITTED，已提交 60s）。"""
    defaults = {
        "order_id": uuid4(),
        "tenant_id": "default",
        "user_id": "1001",
        "symbol": "SH600036",
        "client_order_id": "poll-cid-001",
        "status": OrderStatus.SUBMITTED,
        "trading_mode": TradingMode.REAL,
        "remarks": "",
        "filled_quantity": 0.0,
        "average_price": None,
        "filled_value": 0.0,
        "filled_at": None,
        "cancelled_at": None,
        "expired_at": None,
        "exchange_order_id": None,
        "submitted_at": datetime.now() - timedelta(seconds=60),
        "quantity": 100.0,
    }
    defaults.update(overrides)
    return SimpleNamespace(**defaults)


def _setup_poll(monkeypatch, broker, orders):
    """设置轮询测试环境。"""
    session = _PollSession(orders)
    notifications = []

    monkeypatch.setattr(
        "backend.services.trade.services.order_timeout_scanner.get_session",
        lambda: _PollSessionContext(session),
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


class TestOrderPolling:
    """订单状态查询轮询测试。"""

    @pytest.mark.asyncio
    async def test_g_poll_filled_updates_status(self, monkeypatch):
        """场景G: 轮询查询返回 FILLED -> 本地状态更新为 FILLED"""
        order = _make_poll_order(client_order_id="poll-fill-001")
        broker = MockBroker()
        broker.set_order_result(
            "poll-fill-001",
            OrderStatusResult(
                status="FILLED",
                filled_quantity=100,
                avg_price=10.5,
                exchange_order_id="EXCH-POLL-001",
            ),
        )
        session, notifications = _setup_poll(monkeypatch, broker, [order])

        count = await order_timeout_scanner._scan_pending_orders_for_query_once()

        assert count == 1
        assert session.committed is True
        assert order.status == OrderStatus.FILLED
        assert order.filled_quantity == 100
        assert order.average_price == 10.5
        assert order.filled_value == 1050.0
        assert order.filled_at is not None
        assert order.exchange_order_id == "EXCH-POLL-001"
        assert "[POLL_FILLED:" in (order.remarks or "")
        assert "[POLL_LAST_QUERY:" in (order.remarks or "")

        filled_notifies = [n for n in notifications if n["title"] == "轮询确认成交"]
        assert len(filled_notifies) == 1

    @pytest.mark.asyncio
    async def test_h_poll_cancelled_updates_status(self, monkeypatch):
        """场景H: 轮询查询返回 CANCELLED -> 本地状态更新为 CANCELLED"""
        order = _make_poll_order(client_order_id="poll-cancel-001")
        broker = MockBroker()
        broker.set_order_result(
            "poll-cancel-001",
            OrderStatusResult(status="CANCELLED", exchange_order_id="EXCH-POLL-002"),
        )
        session, notifications = _setup_poll(monkeypatch, broker, [order])

        count = await order_timeout_scanner._scan_pending_orders_for_query_once()

        assert count == 1
        assert order.status == OrderStatus.CANCELLED
        assert order.cancelled_at is not None
        assert "[POLL_CANCELLED]" in (order.remarks or "")
        cancel_notifies = [n for n in notifications if n["title"] == "轮询确认撤单"]
        assert len(cancel_notifies) == 1

    @pytest.mark.asyncio
    async def test_i_poll_still_pending_records_time(self, monkeypatch):
        """场景I: 轮询查询返回 STILL_PENDING -> 保持 SUBMITTED + 记录查询时间"""
        order = _make_poll_order(client_order_id="poll-pending-001")
        broker = MockBroker()
        broker.set_order_result(
            "poll-pending-001",
            OrderStatusResult(status="STILL_PENDING", message="still at exchange"),
        )
        session, _ = _setup_poll(monkeypatch, broker, [order])

        count = await order_timeout_scanner._scan_pending_orders_for_query_once()

        assert count == 1
        assert order.status == OrderStatus.SUBMITTED
        assert "[POLL_LAST_QUERY:" in (order.remarks or "")
        # 不应包含终态标记
        assert "[POLL_FILLED:" not in (order.remarks or "")
        assert "[POLL_CANCELLED]" not in (order.remarks or "")

    @pytest.mark.asyncio
    async def test_j_poll_skips_reconcile_queued(self, monkeypatch):
        """场景J: 轮询跳过已在对账队列的订单"""
        order = _make_poll_order(
            client_order_id="poll-skip-001",
            remarks="[AWAITING_BRIDGE_ACK] [BRIDGE_ACK_TIMEOUT_PENDING_REVIEW] [RECONCILE_QUEUED]",
        )
        broker = MockBroker()
        session, _ = _setup_poll(monkeypatch, broker, [order])

        count = await order_timeout_scanner._scan_pending_orders_for_query_once()

        assert count == 0
        assert order.status == OrderStatus.SUBMITTED
        assert len(broker.query_order_calls) == 0  # 未调用查询

    @pytest.mark.asyncio
    async def test_k_poll_throttling(self, monkeypatch):
        """场景K: 轮询节流 — 距上次查询未满间隔则跳过"""
        # 写入一个很近的查询时间（1秒前），间隔为 30 秒
        recent_time = datetime.now() - timedelta(seconds=1)
        order = _make_poll_order(
            client_order_id="poll-throttle-001",
            remarks=order_timeout_scanner._write_poll_last_query("", recent_time),
        )
        broker = MockBroker()
        session, _ = _setup_poll(monkeypatch, broker, [order])

        count = await order_timeout_scanner._scan_pending_orders_for_query_once()

        assert count == 0  # 被节流跳过
        assert len(broker.query_order_calls) == 0

    def test_l_parse_and_write_poll_last_query(self):
        """场景L: 轮询辅助函数 _parse/_write_poll_last_query"""
        when = datetime(2026, 7, 5, 14, 30, 0)
        written = order_timeout_scanner._write_poll_last_query("", when)
        assert "[POLL_LAST_QUERY:2026-07-05T14:30:00" in written

        parsed = order_timeout_scanner._parse_poll_last_query(written)
        assert parsed is not None
        assert parsed.year == 2026
        assert parsed.hour == 14

        # 空 remarks
        assert order_timeout_scanner._parse_poll_last_query(None) is None
        assert order_timeout_scanner._parse_poll_last_query("") is None
        assert order_timeout_scanner._parse_poll_last_query("no marker here") is None

        # 更新已有标记
        when2 = datetime(2026, 7, 5, 15, 0, 0)
        updated = order_timeout_scanner._write_poll_last_query(written, when2)
        assert updated.count("[POLL_LAST_QUERY:") == 1  # 不重复
        assert "15:00:00" in updated

    @pytest.mark.asyncio
    async def test_poll_broker_unavailable_skips_gracefully(self, monkeypatch):
        """轮询时 broker 不可用 -> 优雅跳过，不崩溃"""
        order = _make_poll_order(client_order_id="poll-nobroker-001")
        session, _ = _setup_poll(monkeypatch, None, [order])

        count = await order_timeout_scanner._scan_pending_orders_for_query_once()

        assert count == 1  # 仍计入处理数（记录了查询时间）
        assert order.status == OrderStatus.SUBMITTED
        assert "[POLL_LAST_QUERY:" in (order.remarks or "")


# ==================== 场景 M-N: 回报解析容错 ====================


class TestReportParsingTolerance:
    """回报解析容错测试。"""

    def test_m_safe_float(self):
        consumer = ExecutionStreamConsumer()
        assert consumer._safe_float(100) == 100.0
        assert consumer._safe_float("10.5") == 10.5
        assert consumer._safe_float(None) == 0.0
        assert consumer._safe_float("") == 0.0
        assert consumer._safe_float("invalid") == 0.0
        assert consumer._safe_float(None, default=-1.0) == -1.0
        assert consumer._safe_float([1, 2]) == 0.0  # 异常类型

    def test_m_safe_str(self):
        consumer = ExecutionStreamConsumer()
        assert consumer._safe_str("hello") == "hello"
        assert consumer._safe_str(None) == ""
        assert consumer._safe_str(None, default="fallback") == "fallback"
        assert consumer._safe_str(123) == "123"
        # 超长截断
        long_str = "x" * 1000
        assert len(consumer._safe_str(long_str, max_len=100)) == 100
        assert len(consumer._safe_str(long_str, max_len=0)) == 1000  # max_len=0 不截断

    @pytest.mark.asyncio
    async def test_n_order_filled_invalid_qty_raises_and_logs(self, monkeypatch):
        """场景N: order_filled 无效 filled_qty -> ValueError + 异常记录"""
        consumer = ExecutionStreamConsumer()
        anomalies = []
        monkeypatch.setattr(
            consumer, "_log_report_anomaly",
            lambda fields, reason: anomalies.append((fields, reason)),
        )

        fields = {
            "event_type": "order_filled",
            "filled_qty": "invalid",
            "filled_price": "10.0",
            "broker_order_id": "b-001",
            "exec_id": "e-001",
        }

        with pytest.raises(ValueError, match="invalid filled_qty"):
            await consumer._handle_order_filled(fields)

        # 异常被记录
        assert len(anomalies) == 1
        assert "invalid filled_qty" in anomalies[0][1]

    @pytest.mark.asyncio
    async def test_n_order_filled_zero_price_raises_and_logs(self, monkeypatch):
        """order_filled 价格为 0 -> ValueError + 异常记录"""
        consumer = ExecutionStreamConsumer()
        anomalies = []
        monkeypatch.setattr(
            consumer, "_log_report_anomaly",
            lambda fields, reason: anomalies.append((fields, reason)),
        )

        fields = {
            "event_type": "order_filled",
            "filled_qty": 100,
            "filled_price": 0,
            "broker_order_id": "b-002",
            "exec_id": "e-002",
        }

        with pytest.raises(ValueError):
            await consumer._handle_order_filled(fields)
        assert len(anomalies) == 1

    @pytest.mark.asyncio
    async def test_log_report_anomaly_records_fields(self):
        """_log_report_anomaly 正确记录关键字段"""
        consumer = ExecutionStreamConsumer()
        # 不崩溃即可（日志记录）
        consumer._log_report_anomaly(
            {
                "event_type": "order_filled",
                "client_order_id": "cid-anomaly",
                "broker_order_id": "bid-anomaly",
            },
            "test anomaly reason",
        )
        # 验证不抛异常即可


# ==================== 场景 O: QMT 状态映射 ====================


class TestQMTStatusMapping:
    """QMT 柜台状态映射测试。"""

    def test_o_status_mapping_chinese(self):
        """场景O: QMT 中文状态映射"""
        assert QMTBroker._map_qmt_status("已成交") == "FILLED"
        assert QMTBroker._map_qmt_status("部成") == "PARTIALLY_FILLED"
        assert QMTBroker._map_qmt_status("已撤") == "CANCELLED"
        assert QMTBroker._map_qmt_status("废单") == "REJECTED"
        assert QMTBroker._map_qmt_status("未成交") == "STILL_PENDING"

    def test_o_status_mapping_english(self):
        """QMT 英文状态映射"""
        assert QMTBroker._map_qmt_status("FILLED") == "FILLED"
        assert QMTBroker._map_qmt_status("DEAL") == "FILLED"
        assert QMTBroker._map_qmt_status("TRADED") == "FILLED"
        assert QMTBroker._map_qmt_status("PARTIALLY_FILLED") == "PARTIALLY_FILLED"
        assert QMTBroker._map_qmt_status("PARTIAL") == "PARTIALLY_FILLED"
        assert QMTBroker._map_qmt_status("CANCELLED") == "CANCELLED"
        assert QMTBroker._map_qmt_status("CANCELED") == "CANCELLED"
        assert QMTBroker._map_qmt_status("REJECTED") == "REJECTED"
        assert QMTBroker._map_qmt_status("REJECT") == "REJECTED"
        assert QMTBroker._map_qmt_status("PENDING") == "STILL_PENDING"
        assert QMTBroker._map_qmt_status("SUBMITTED") == "STILL_PENDING"
        assert QMTBroker._map_qmt_status("ACTIVE") == "STILL_PENDING"

    def test_o_status_mapping_unknown(self):
        """未知状态映射为 STILL_PENDING"""
        assert QMTBroker._map_qmt_status("UNKNOWN_STATUS") == "STILL_PENDING"
        assert QMTBroker._map_qmt_status("") == "STILL_PENDING"
        assert QMTBroker._map_qmt_status(None) == "STILL_PENDING"
        assert QMTBroker._map_qmt_status(123) == "STILL_PENDING"


# ==================== 场景 P-Q: QMTBridgeBroker.query_order 双层查询 ====================


class _FakeHttpResponse:
    """模拟 httpx.Response。"""

    def __init__(self, status_code=200, json_data=None, text=""):
        self.status_code = status_code
        self._json_data = json_data
        self.text = text

    def json(self):
        return self._json_data or {}


class _FakeHttpxClient:
    """模拟 httpx.AsyncClient。"""

    def __init__(self, responses=None):
        # responses: dict mapping url_substring -> _FakeHttpResponse or list
        self._responses = responses or {}
        self.requests = []

    async def get(self, url, **kwargs):
        self.requests.append(("GET", url, kwargs))
        for key, resp in self._responses.items():
            if key in url:
                if isinstance(resp, list):
                    return resp.pop(0)
                return resp
        return _FakeHttpResponse(status_code=404, text="not found")

    async def post(self, url, **kwargs):
        self.requests.append(("POST", url, kwargs))
        for key, resp in self._responses.items():
            if key in url:
                if isinstance(resp, list):
                    return resp.pop(0)
                return resp
        return _FakeHttpResponse(status_code=404, text="not found")


class TestQMTBridgeBrokerQueryOrder:
    """QMTBridgeBroker.query_order 双层查询测试。"""

    @pytest.fixture(autouse=True)
    def _mock_service_token(self, monkeypatch):
        """mock create_service_token 避免依赖 SECRET_KEY 环境变量。"""
        monkeypatch.setattr(
            "backend.services.trade.services.broker_client.create_service_token",
            lambda service, **kw: f"mock-token-{service}",
        )

    @pytest.mark.asyncio
    async def test_p_bridge_endpoint_success(self, monkeypatch):
        """场景P: bridge 端点返回成功 -> 直接使用柜台回报"""
        broker = QMTBridgeBroker(stream_base_url="http://stream-test:8003")
        fake_client = _FakeHttpxClient(
            responses={
                "/api/v1/internal/bridge/query": _FakeHttpResponse(
                    status_code=200,
                    json_data={
                        "ok": True,
                        "payload": {
                            "status": "已成交",
                            "filled_quantity": 200,
                            "avg_price": 9.8,
                            "exchange_order_id": "EXCH-BRIDGE-001",
                        },
                    },
                )
            }
        )
        broker._session = fake_client

        result = await broker.query_order("bridge-cid-001")

        assert result.status == "FILLED"
        assert result.filled_quantity == 200
        assert result.avg_price == 9.8
        assert result.exchange_order_id == "EXCH-BRIDGE-001"

    @pytest.mark.asyncio
    async def test_q_bridge_404_falls_back_to_db(self, monkeypatch):
        """场景Q: bridge 端点 404 -> 回退本地 DB 查询"""
        broker = QMTBridgeBroker(stream_base_url="http://stream-test:8003")
        fake_client = _FakeHttpxClient(
            responses={
                "/api/v1/internal/bridge/query": _FakeHttpResponse(
                    status_code=404, text="not implemented"
                )
            }
        )
        broker._session = fake_client

        # mock DB fallback
        async def fake_db_query(cid):
            return OrderStatusResult(
                status="FILLED",
                filled_quantity=100,
                avg_price=10.0,
                exchange_order_id="EXCH-DB-001",
                message="db fallback",
            )

        monkeypatch.setattr(broker, "_query_order_from_db", fake_db_query)

        result = await broker.query_order("bridge-cid-404")

        assert result.status == "FILLED"
        assert result.filled_quantity == 100
        assert result.exchange_order_id == "EXCH-DB-001"

    @pytest.mark.asyncio
    async def test_q_bridge_exception_falls_back_to_db(self, monkeypatch):
        """bridge 端点异常（连接超时）-> 回退 DB"""
        broker = QMTBridgeBroker(stream_base_url="http://stream-test:8003")

        class _RaisingClient:
            async def get(self, url, **kwargs):
                raise ConnectionError("timeout")

            async def post(self, url, **kwargs):
                raise ConnectionError("timeout")

        broker._session = _RaisingClient()

        async def fake_db_query(cid):
            return OrderStatusResult(status="STILL_PENDING", message="db fallback")

        monkeypatch.setattr(broker, "_query_order_from_db", fake_db_query)

        result = await broker.query_order("bridge-cid-exc")
        assert result.status == "STILL_PENDING"

    @pytest.mark.asyncio
    async def test_empty_client_order_id_returns_still_pending(self):
        """空 client_order_id -> STILL_PENDING"""
        broker = QMTBridgeBroker(stream_base_url="http://stream-test:8003")
        result = await broker.query_order("")
        assert result.status == "STILL_PENDING"
        assert "empty" in result.message


# ==================== 场景 R: QMTBroker.query_order HTTP 查询 ====================


class TestQMTBrokerQueryOrder:
    """QMTBroker.query_order 测试。"""

    @pytest.mark.asyncio
    async def test_r_http_query_success(self, monkeypatch):
        """场景R: QMT Bridge HTTP 查询成功"""
        broker = QMTBroker(qmt_host="127.0.0.1", qmt_port=18080)
        fake_client = _FakeHttpxClient(
            responses={
                "/api/order/query": _FakeHttpResponse(
                    status_code=200,
                    json_data={
                        "status": "已成交",
                        "filled_quantity": 300,
                        "avg_price": 11.2,
                        "exchange_order_id": "EXCH-QMT-001",
                    },
                )
            }
        )
        broker._session = fake_client

        result = await broker.query_order("qmt-cid-001")

        assert result.status == "FILLED"
        assert result.filled_quantity == 300
        assert result.avg_price == 11.2
        assert result.exchange_order_id == "EXCH-QMT-001"

    @pytest.mark.asyncio
    async def test_r_http_404_falls_back_to_db(self, monkeypatch):
        """QMT Bridge HTTP 404 -> 回退 DB"""
        broker = QMTBroker(qmt_host="127.0.0.1", qmt_port=18080)
        fake_client = _FakeHttpxClient(
            responses={
                "/api/order/query": _FakeHttpResponse(status_code=404, text="not found")
            }
        )
        broker._session = fake_client

        async def fake_db_query(cid):
            return OrderStatusResult(
                status="CANCELLED",
                exchange_order_id="EXCH-DB-CANCEL",
                message="db fallback",
            )

        monkeypatch.setattr(broker, "_query_order_from_db", fake_db_query)

        result = await broker.query_order("qmt-cid-404")
        assert result.status == "CANCELLED"

    @pytest.mark.asyncio
    async def test_r_http_exception_falls_back_to_db(self, monkeypatch):
        """QMT Bridge HTTP 异常 -> 回退 DB"""
        broker = QMTBroker(qmt_host="127.0.0.1", qmt_port=18080)

        class _RaisingClient:
            async def get(self, url, **kwargs):
                raise ConnectionRefusedError("connection refused")

            async def post(self, url, **kwargs):
                raise ConnectionRefusedError("connection refused")

        broker._session = _RaisingClient()

        async def fake_db_query(cid):
            return OrderStatusResult(status="REJECTED", message="db fallback")

        monkeypatch.setattr(broker, "_query_order_from_db", fake_db_query)

        result = await broker.query_order("qmt-cid-exc")
        assert result.status == "REJECTED"


# ==================== 补充: BaseBroker 连接管理集成 ====================


class TestBaseBrokerConnectionIntegration:
    """BaseBroker 连接管理集成测试。"""

    def test_broker_without_manager_is_available(self):
        """未配置连接管理器的 broker 默认可用（向后兼容）。"""
        broker = MockBroker()
        assert broker.is_available() is True
        assert broker.get_connection_manager() is None

    @pytest.mark.asyncio
    async def test_broker_with_manager_reflects_state(self):
        """配置连接管理器后 is_available 反映状态。"""

        async def ok_check():
            return True

        manager = BrokerConnectionManager(
            broker_name="mock-with-mgr",
            health_check_func=ok_check,
        )
        manager._sleep_func = _NoSleep()
        await manager.start()

        broker = MockBroker()
        broker.set_connection_manager(manager)
        assert broker.is_available() is True  # CONNECTED

        await manager.stop()
        assert broker.is_available() is False  # DISCONNECTED after stop

    @pytest.mark.asyncio
    async def test_qmt_broker_has_connection_manager(self):
        """QMTBroker 和 QMTBridgeBroker 初始化时自带连接管理器。"""
        qmt = QMTBroker()
        assert qmt.get_connection_manager() is not None
        assert qmt.get_connection_manager().broker_name == "QMTBroker"

        bridge = QMTBridgeBroker(stream_base_url="http://test:8003")
        assert bridge.get_connection_manager() is not None
        assert bridge.get_connection_manager().broker_name == "QMTBridgeBroker"
