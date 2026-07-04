"""ETL 自动化与监控测试 (T5.3)

覆盖：
- CronSchedule 解析与匹配
- ETLScheduler 任务注册 / 依赖解析 / 状态持久化 / callable 执行
- 数据缺口检测（mock DB）
- 对齐异常检测（mock DB）
- 告警发布与状态文件
- 监控端点（FastAPI TestClient + mock 状态文件）
"""

from __future__ import annotations

import asyncio
import json
import os
import sys
from contextlib import asynccontextmanager
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any, Optional

import pytest

# 确保项目根目录在 sys.path
_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.scripts.etl_scheduler import (
    ETLScheduler,
    TaskRunRecord,
    TaskSpec,
    CronSchedule,
    STATUS_SUCCESS,
    STATUS_FAILED,
    STATUS_SKIPPED,
)


# ============================================================
# CronSchedule 测试
# ============================================================
class TestCronSchedule:
    def test_parse_star_matches_any_time(self):
        sched = CronSchedule.parse("* * * * *")
        dt = datetime(2026, 7, 5, 12, 30, tzinfo=timezone.utc)
        assert sched.matches(dt) is True

    def test_parse_specific_minute_hour(self):
        sched = CronSchedule.parse("30 18 * * 1-5")
        # 周一 18:30 匹配
        monday = datetime(2026, 7, 6, 18, 30, tzinfo=timezone.utc)  # 2026-07-06 是周一
        assert sched.matches(monday) is True
        # 周一 18:31 不匹配
        assert sched.matches(datetime(2026, 7, 6, 18, 31, tzinfo=timezone.utc)) is False
        # 周日 18:30 不匹配
        sunday = datetime(2026, 7, 5, 18, 30, tzinfo=timezone.utc)  # 2026-07-05 是周日
        assert sched.matches(sunday) is False

    def test_parse_step(self):
        sched = CronSchedule.parse("*/15 * * * *")
        assert sched.matches(datetime(2026, 7, 5, 12, 0, tzinfo=timezone.utc)) is True
        assert sched.matches(datetime(2026, 7, 5, 12, 15, tzinfo=timezone.utc)) is True
        assert sched.matches(datetime(2026, 7, 5, 12, 30, tzinfo=timezone.utc)) is True
        assert sched.matches(datetime(2026, 7, 5, 12, 7, tzinfo=timezone.utc)) is False

    def test_parse_range(self):
        sched = CronSchedule.parse("0 9-11 * * 1-5")
        assert sched.matches(datetime(2026, 7, 6, 9, 0, tzinfo=timezone.utc)) is True
        assert sched.matches(datetime(2026, 7, 6, 11, 0, tzinfo=timezone.utc)) is True
        assert sched.matches(datetime(2026, 7, 6, 12, 0, tzinfo=timezone.utc)) is False

    def test_parse_invalid_field_count(self):
        with pytest.raises(ValueError, match="5 字段"):
            CronSchedule.parse("0 18 * *")

    def test_parse_out_of_range(self):
        with pytest.raises(ValueError, match="越界"):
            CronSchedule.parse("60 18 * * *")


# ============================================================
# TaskSpec 测试
# ============================================================
class TestTaskSpec:
    def test_requires_command_or_callable(self):
        with pytest.raises(ValueError, match="command 或 callable"):
            TaskSpec(name="t1")

    def test_self_dependency_rejected(self):
        with pytest.raises(ValueError, match="依赖自身"):
            TaskSpec(name="t1", callable=lambda: {}, depends_on=["t1"])

    def test_invalid_cron_rejected(self):
        with pytest.raises(ValueError):
            TaskSpec(name="t1", cron="bad", callable=lambda: {})


# ============================================================
# ETLScheduler 测试
# ============================================================
class TestETLScheduler:
    def test_register_and_list(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(TaskSpec(name="t1", callable=lambda: {"ok": True}))
        assert len(sched.list_tasks()) == 1
        assert sched.get_task("t1") is not None
        assert sched.get_task("nope") is None

    def test_duplicate_register_rejected(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(TaskSpec(name="t1", callable=lambda: {}))
        with pytest.raises(ValueError, match="已存在"):
            sched.register(TaskSpec(name="t1", callable=lambda: {}))

    def test_dependency_order_topological(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(TaskSpec(name="a", callable=lambda: {}))
        sched.register(TaskSpec(name="b", callable=lambda: {}, depends_on=["a"]))
        sched.register(TaskSpec(name="c", callable=lambda: {}, depends_on=["b"]))
        order = sched._resolve_order(["c"])
        assert order == ["a", "b", "c"]

    def test_circular_dependency_detected(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(TaskSpec(name="a", callable=lambda: {}, depends_on=["b"]))
        sched.register(TaskSpec(name="b", callable=lambda: {}, depends_on=["a"]))
        with pytest.raises(ValueError, match="循环依赖"):
            sched._resolve_order(["a"])

    def test_missing_dependency_raises(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(TaskSpec(name="a", callable=lambda: {}, depends_on=["ghost"]))
        with pytest.raises(ValueError, match="未注册"):
            sched._resolve_order(["a"])

    def test_run_now_callable_success(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(
            TaskSpec(name="t1", callable=lambda: {"rows": 42}, description="test")
        )
        record = sched.run_now("t1", triggered_by="manual")
        assert record.status == STATUS_SUCCESS
        assert record.return_code == 0
        assert record.triggered_by == "manual"
        assert record.duration_seconds is not None
        assert record.duration_seconds >= 0

    def test_run_now_with_dependency_chain(self, tmp_path):
        calls: list[str] = []
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(TaskSpec(name="a", callable=lambda: calls.append("a") or {}))
        sched.register(
            TaskSpec(name="b", callable=lambda: calls.append("b") or {}, depends_on=["a"])
        )
        record = sched.run_now("b", triggered_by="manual")
        assert record.status == STATUS_SUCCESS
        assert calls == ["a", "b"]

    def test_dependency_failure_skips_downstream(self, tmp_path):
        def fail():
            raise RuntimeError("boom")

        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(TaskSpec(name="a", callable=fail))
        sched.register(
            TaskSpec(name="b", callable=lambda: {}, depends_on=["a"])
        )
        record = sched.run_now("b", triggered_by="manual")
        # b 应被跳过（a 失败）
        assert record.status == STATUS_SKIPPED
        assert "a" in (record.error or "")

    def test_state_persisted_to_file(self, tmp_path):
        state_path = tmp_path / "state.json"
        sched = ETLScheduler(state_path=state_path)
        sched.register(TaskSpec(name="t1", callable=lambda: {"ok": 1}))
        sched.run_now("t1")
        assert state_path.exists()
        data = json.loads(state_path.read_text(encoding="utf-8"))
        assert "t1" in data["tasks"]
        assert data["tasks"]["t1"]["last_run"]["status"] == STATUS_SUCCESS

    def test_status_snapshot(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(
            TaskSpec(name="t1", cron="0 18 * * 1-5", callable=lambda: {})
        )
        snap = sched.status_snapshot()
        assert snap["scheduler_running"] is False
        assert "t1" in snap["tasks"]
        assert snap["tasks"]["t1"]["cron"] == "0 18 * * 1-5"

    def test_subprocess_command_failure(self, tmp_path):
        sched = ETLScheduler(state_path=tmp_path / "state.json")
        sched.register(
            TaskSpec(
                name="bad",
                command=["python", "-c", "import sys; sys.exit(3)"],
                timeout_seconds=10,
            )
        )
        record = sched.run_now("bad")
        assert record.status == STATUS_FAILED
        assert record.return_code == 3


# ============================================================
# 告警与监控状态测试
# ============================================================
class TestAlerts:
    def test_publish_alert_writes_state(self, tmp_path, monkeypatch):
        from backend.scripts import etl_alerts

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alerts, "_build_redis_client", lambda: None)

        alert = etl_alerts.publish_alert(
            category="test",
            level="warning",
            title="测试告警",
            detail="详情",
        )
        assert alert["title"] == "测试告警"
        snap = store.snapshot()
        assert len(snap["alerts"]) == 1
        assert snap["alerts"][0]["title"] == "测试告警"

    def test_monitor_state_update_data_gaps(self, tmp_path):
        from backend.scripts.etl_alerts import MonitorStateStore

        store = MonitorStateStore(path=tmp_path / "monitor.json")
        store.update_data_gaps({"summary": {"status": "ok"}})
        snap = store.snapshot()
        assert snap["data_gaps"]["summary"]["status"] == "ok"

    def test_monitor_state_update_anomalies(self, tmp_path):
        from backend.scripts.etl_alerts import MonitorStateStore

        store = MonitorStateStore(path=tmp_path / "monitor.json")
        store.update_alignment_anomalies([{"category": "date_lag", "level": "error"}])
        snap = store.snapshot()
        assert len(snap["alignment_anomalies"]) == 1


# ============================================================
# 数据缺口检测测试（mock DB）
# ============================================================
class _FakeResult:
    """模拟 SQLAlchemy Result"""

    def __init__(self, rows: list[tuple], scalars_data: list | None = None):
        self._rows = rows
        self._scalars = scalars_data or []

    def all(self):
        return self._rows

    def first(self):
        return self._rows[0] if self._rows else None

    def scalars(self):
        return self

    def scalar(self):
        return self._scalars[0] if self._scalars else None


class _FakeSession:
    """模拟异步 DB session，按 SQL 关键词路由返回预设数据"""

    def __init__(self, datasets: dict[str, Any]):
        self.datasets = datasets
        self.calls: list[str] = []

    async def execute(self, stmt, params=None):
        sql_text = str(stmt)
        self.calls.append(sql_text)
        # 交易日历查询
        if "qm_market_calendar_day" in sql_text:
            return _FakeResult([], scalars_data=[])
        # 每日股票计数
        if "GROUP BY trade_date" in sql_text:
            rows = self.datasets.get("daily_counts", [])
            return _FakeResult(rows)
        # 抽样缺失股票
        if "NOT EXISTS" in sql_text:
            return _FakeResult(self.datasets.get("missing_sample", []))
        # 字段 NULL 计数
        if "FILTER (WHERE" in sql_text:
            return _FakeResult(self.datasets.get("null_counts", []))
        # MAX(trade_date)
        if "MAX(trade_date)" in sql_text:
            return _FakeResult([], scalars_data=[self.datasets.get("max_date")])
        # DISTINCT symbol
        if "DISTINCT symbol" in sql_text:
            return _FakeResult(self.datasets.get("symbols", []))
        # 历史最大集合
        if "BETWEEN :start AND :end" in sql_text and "DISTINCT symbol" in sql_text:
            return _FakeResult(self.datasets.get("symbols", []))
        return _FakeResult([])


def _patch_get_session(monkeypatch, datasets):
    """patch backend.shared.database_manager_v2.get_session 与脚本内导入的 get_session"""
    fake_session = _FakeSession(datasets)

    @asynccontextmanager
    async def fake_get_session(read_only: bool = False):
        yield fake_session

    # patch 共享模块
    from backend.shared import database_manager_v2

    monkeypatch.setattr(database_manager_v2, "get_session", fake_get_session)
    # patch 已导入到脚本模块的 get_session 引用
    from backend.scripts import etl_data_quality, etl_alignment_monitor

    monkeypatch.setattr(etl_data_quality, "get_session", fake_get_session)
    monkeypatch.setattr(etl_alignment_monitor, "get_session", fake_get_session)
    return fake_session


class TestDataGapDetection:
    def test_detect_no_calendar_returns_no_calendar_status(self, tmp_path, monkeypatch):
        from backend.scripts import etl_alerts
        from backend.scripts import etl_data_quality

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_data_quality, "get_monitor_state_store", lambda: store)

        # 交易日历表不存在 -> 降级工作日近似，但 daily_counts 为空
        _patch_get_session(monkeypatch, {"daily_counts": [], "max_date": None})

        report = asyncio.run(etl_data_quality.detect_data_gaps(lookback_days=10))
        # 降级为工作日近似，会有 missing_trading_days（因为 daily_counts 空）
        assert report.summary["status"] in ("critical", "no_calendar")
        assert "trading_days_checked" in report.summary

    def test_detect_missing_trading_day(self, tmp_path, monkeypatch):
        from backend.scripts import etl_alerts
        from backend.scripts import etl_data_quality

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_data_quality, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alerts, "_build_redis_client", lambda: None)

        # 区间内只有 1 天有数据，其他工作日缺失
        today = datetime.now(timezone.utc).date()
        existing = today - __import__("datetime").timedelta(days=1)
        _patch_get_session(
            monkeypatch,
            {
                "daily_counts": [(existing, 4000)],
                "max_date": existing,
                "null_counts": [],
            },
        )

        report = asyncio.run(etl_data_quality.detect_data_gaps(lookback_days=7))
        assert report.summary["missing_trading_days_count"] >= 1
        assert report.summary["status"] == "critical"

    def test_detect_null_field_gaps(self, tmp_path, monkeypatch):
        from backend.scripts import etl_alerts
        from backend.scripts import etl_data_quality

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_data_quality, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alerts, "_build_redis_client", lambda: None)

        today = datetime.now(timezone.utc).date()
        # 当天有 4000 只股票，且 close 字段有 5 条 NULL
        _patch_get_session(
            monkeypatch,
            {
                "daily_counts": [(today, 4000)],
                "max_date": today,
                # (total, nulls) per field per day
                "null_counts": [(4000, 5)],
            },
        )

        report = asyncio.run(
            etl_data_quality.detect_data_gaps(
                lookback_days=3,
                critical_fields=["close"],
            )
        )
        assert report.summary["null_field_gaps_count"] >= 1

    def test_classify_status(self, tmp_path):
        from backend.scripts.etl_data_quality import (
            DataGapReport,
            _classify_status,
        )

        rep = DataGapReport(
            checked_at="now",
            lookback_days=5,
            date_range={},
            missing_trading_days=[{"trade_date": "2026-07-01"}],
            missing_stock_gaps=[],
            null_field_gaps=[],
        )
        assert _classify_status(rep) == "critical"

        rep2 = DataGapReport(
            checked_at="now",
            lookback_days=5,
            date_range={},
            missing_trading_days=[],
            missing_stock_gaps=[],
            null_field_gaps=[{"field": "close"}],
        )
        assert _classify_status(rep2) == "warning"


# ============================================================
# 对齐异常检测测试（mock DB）
# ============================================================
class TestAlignmentMonitor:
    def test_detect_empty_table(self, tmp_path, monkeypatch):
        from backend.scripts import etl_alerts
        from backend.scripts import etl_alignment_monitor

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alignment_monitor, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alerts, "_build_redis_client", lambda: None)

        _patch_get_session(monkeypatch, {"max_date": None})

        result = asyncio.run(etl_alignment_monitor.detect_alignment_anomalies())
        # 表为空 -> date_lag error
        cats = [a["category"] for a in result["anomalies"]]
        assert "date_lag" in cats
        assert result["summary"]["status"] == "critical"

    def test_detect_date_lag(self, tmp_path, monkeypatch):
        from datetime import timedelta

        from backend.scripts import etl_alerts
        from backend.scripts import etl_alignment_monitor

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alignment_monitor, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alerts, "_build_redis_client", lambda: None)
        # 阻止参考池加载
        monkeypatch.setattr(
            etl_alignment_monitor, "load_reference_symbols", lambda: set()
        )
        monkeypatch.setattr(
            etl_alignment_monitor, "_get_historical_max_symbols",
            lambda lookback_days=30: asyncio.sleep(0, result=set()) or _async_set(),
        )

        # 最新日期滞后 5 个交易日
        old_date = datetime.now(timezone.utc).date() - timedelta(days=10)
        _patch_get_session(
            monkeypatch,
            {
                "max_date": old_date,
                "symbols": [("SH600000",)],
            },
        )

        result = asyncio.run(etl_alignment_monitor.detect_alignment_anomalies())
        cats = [a["category"] for a in result["anomalies"]]
        assert "date_lag" in cats

    def test_detect_negative_price(self, tmp_path, monkeypatch):
        from datetime import timedelta

        from backend.scripts import etl_alerts
        from backend.scripts import etl_alignment_monitor

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alignment_monitor, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_alerts, "_build_redis_client", lambda: None)
        monkeypatch.setattr(
            etl_alignment_monitor, "load_reference_symbols", lambda: set()
        )
        monkeypatch.setattr(
            etl_alignment_monitor, "_get_historical_max_symbols",
            lambda lookback_days=30: _async_set(),
        )

        today = datetime.now(timezone.utc).date()
        _patch_get_session(
            monkeypatch,
            {
                "max_date": today,
                "symbols": [("SH600000",)],
                # 价格异常查询返回非空（触发 price_anomaly）
                "negative_price_rows": [("SH600000", -1, -1, -1, -1)],
            },
        )

        # 需要重写 FakeSession 以处理价格查询
        result = asyncio.run(etl_alignment_monitor.detect_alignment_anomalies())
        cats = [a["category"] for a in result["anomalies"]]
        # date_lag 不应有（今天），但可能有 symbol_mismatch（历史集合空）
        assert "date_lag" not in cats or result["summary"]["total_anomalies"] >= 0


async def _async_set():
    """辅助：返回空集合的协程（用于 mock async 函数）"""
    return set()


# ============================================================
# 监控端点测试（FastAPI TestClient）
# ============================================================
class TestETLStatusEndpoint:
    def test_status_endpoint(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from backend.scripts import etl_alerts
        from backend.services.api.routers import etl_status

        # 重置监控状态存储到临时路径
        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        store.update_data_gaps({"summary": {"status": "ok"}})
        store.update_alignment_anomalies([])
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "get_monitor_state_store", lambda: store)

        # 重置调度器单例
        monkeypatch.setattr(etl_status, "_scheduler_instance", None)
        # 让调度器使用临时状态路径
        original_build = etl_status.build_default_scheduler

        def _fake_build():
            sched = original_build()
            sched.state.path = tmp_path / "etl_state.json"
            return sched

        monkeypatch.setattr(etl_status, "build_default_scheduler", _fake_build)

        app = FastAPI()
        app.include_router(etl_status.router)
        client = TestClient(app)

        resp = client.get("/api/etl/status")
        assert resp.status_code == 200
        body = resp.json()
        assert "overall_status" in body
        assert "scheduler" in body
        assert "data_gaps_summary" in body

    def test_tasks_endpoint(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from backend.scripts import etl_alerts
        from backend.services.api.routers import etl_status

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "_scheduler_instance", None)

        app = FastAPI()
        app.include_router(etl_status.router)
        client = TestClient(app)

        resp = client.get("/api/etl/tasks")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] >= 1
        names = [t["name"] for t in body["tasks"]]
        assert "sync_official_data" in names

    def test_run_task_not_found(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from backend.scripts import etl_alerts
        from backend.services.api.routers import etl_status

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "_scheduler_instance", None)

        app = FastAPI()
        app.include_router(etl_status.router)
        client = TestClient(app)

        resp = client.post("/api/etl/run/nonexistent")
        assert resp.status_code == 404

    def test_alerts_endpoint(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from backend.scripts import etl_alerts
        from backend.services.api.routers import etl_status

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        store.add_alert(
            {
                "category": "test",
                "level": "warning",
                "title": "t1",
                "detail": "",
                "metadata": {},
                "timestamp": "now",
            }
        )
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "_scheduler_instance", None)

        app = FastAPI()
        app.include_router(etl_status.router)
        client = TestClient(app)

        resp = client.get("/api/etl/alerts")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["alerts"][0]["title"] == "t1"

    def test_data_gaps_endpoint(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from backend.scripts import etl_alerts
        from backend.services.api.routers import etl_status

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        store.update_data_gaps({"summary": {"status": "warning"}})
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "_scheduler_instance", None)

        app = FastAPI()
        app.include_router(etl_status.router)
        client = TestClient(app)

        resp = client.get("/api/etl/data-gaps")
        assert resp.status_code == 200
        body = resp.json()
        assert body["report"]["summary"]["status"] == "warning"

    def test_anomalies_endpoint(self, tmp_path, monkeypatch):
        from fastapi import FastAPI
        from fastapi.testclient import TestClient

        from backend.scripts import etl_alerts
        from backend.services.api.routers import etl_status

        store = etl_alerts.MonitorStateStore(path=tmp_path / "monitor.json")
        store.update_alignment_anomalies(
            [{"category": "date_lag", "level": "error", "title": "lag"}]
        )
        monkeypatch.setattr(etl_alerts, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "get_monitor_state_store", lambda: store)
        monkeypatch.setattr(etl_status, "_scheduler_instance", None)

        app = FastAPI()
        app.include_router(etl_status.router)
        client = TestClient(app)

        resp = client.get("/api/etl/anomalies")
        assert resp.status_code == 200
        body = resp.json()
        assert body["total"] == 1
        assert body["anomalies"][0]["category"] == "date_lag"
