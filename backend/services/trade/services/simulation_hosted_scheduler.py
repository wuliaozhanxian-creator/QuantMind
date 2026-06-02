"""
Simulation Hosted Scheduler - 模拟盘托管调度器
按 live_trade_config 配置自动触发调仓任务
"""

from __future__ import annotations

import asyncio
import hashlib
import json
import logging
import math
import os
from dataclasses import dataclass
from datetime import datetime, date
from typing import Any
from zoneinfo import ZoneInfo

import pandas as pd

try:
    from exchange_calendars import get_calendar
except ImportError:
    get_calendar = None

from backend.services.trade.redis_client import RedisClient
from backend.services.trade.services.manual_execution_service import (
    manual_execution_service,
)

logger = logging.getLogger(__name__)

_SH_TZ = ZoneInfo("Asia/Shanghai")

_DEFAULT_LIVE_TRADE_CONFIG: dict[str, Any] = {
    "rebalance_days": 3,
    "schedule_type": "interval",
    "trade_weekdays": [],
    "enabled_sessions": ["PM"],
    "sell_time": "14:45",
    "buy_time": "14:50",
    "sell_first": True,
    "order_type": "MARKET",
    "max_price_deviation": 0.02,
    "max_orders_per_cycle": 20,
}


@dataclass(frozen=True)
class SimulationScheduleDecision:
    should_trigger: bool
    phase: str
    trade_date: str
    reason: str


def _to_int(value: Any, default: int) -> int:
    try:
        number = int(float(value))
        return number if math.isfinite(number) else default
    except Exception:
        return default


def _normalize_live_trade_config(value: Any) -> dict[str, Any]:
    cfg = value if isinstance(value, dict) else {}
    merged = {**_DEFAULT_LIVE_TRADE_CONFIG, **cfg}
    merged["schedule_type"] = str(merged.get("schedule_type") or "interval").lower()
    merged["trade_weekdays"] = [
        str(item).upper() for item in (merged.get("trade_weekdays") or [])
    ]
    merged["enabled_sessions"] = [
        str(item).upper() for item in (merged.get("enabled_sessions") or ["PM"])
    ]
    merged["order_type"] = str(merged.get("order_type") or "MARKET").upper()
    merged["rebalance_days"] = max(1, _to_int(merged.get("rebalance_days"), 3))
    merged["max_orders_per_cycle"] = max(
        1, _to_int(merged.get("max_orders_per_cycle"), 20)
    )
    return merged


def _parse_started_at(value: Any) -> date | None:
    text = str(value or "").strip()
    if not text:
        return None
    try:
        parsed = datetime.fromisoformat(text.replace("Z", "+00:00"))
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=_SH_TZ)
        return parsed.astimezone(_SH_TZ).date()
    except Exception:
        return None


def _session_index(day: date) -> int | None:
    if get_calendar is None:
        return None
    try:
        calendar = get_calendar("XSHG")
        session = calendar.date_to_session(pd.Timestamp(day), direction="previous")
        return int(calendar.sessions.get_loc(session))
    except Exception:
        return None


def _is_interval_rebalance_day(
    current_day: date,
    *,
    started_day: date | None,
    rebalance_days: int,
) -> bool:
    if rebalance_days <= 1:
        return True
    if started_day is None:
        return True

    current_idx = _session_index(current_day)
    started_idx = _session_index(started_day)
    if current_idx is not None and started_idx is not None:
        return max(0, current_idx - started_idx) % rebalance_days == 0

    return max(0, (current_day - started_day).days) % rebalance_days == 0


def _is_trading_day(day: date) -> bool:
    if get_calendar is None:
        return day.weekday() < 5
    try:
        calendar = get_calendar("XSHG")
        return calendar.is_session(pd.Timestamp(day))
    except Exception:
        return day.weekday() < 5


def _is_enabled_session(now_hhmm: str, live_trade_config: dict[str, Any]) -> bool:
    enabled = set(live_trade_config.get("enabled_sessions") or [])
    if "AM" in enabled and "09:30" <= now_hhmm <= "11:30":
        return True
    if "PM" in enabled and "13:00" <= now_hhmm <= "15:00":
        return True
    return False


def _resolve_phase(now_hhmm: str, live_trade_config: dict[str, Any]) -> str:
    sell_time = str(live_trade_config.get("sell_time") or "14:45")
    buy_time = str(live_trade_config.get("buy_time") or "14:50")
    if sell_time == buy_time:
        return "ALL" if now_hhmm >= sell_time else "IDLE"
    if now_hhmm < sell_time:
        return "IDLE"
    if sell_time <= now_hhmm < buy_time:
        return "SELL" if bool(live_trade_config.get("sell_first", True)) else "BUY"
    return "BUY"


def _should_trigger(
    *,
    now: datetime,
    live_trade_config: dict[str, Any],
    started_day: date | None,
) -> SimulationScheduleDecision:
    local_now = now.astimezone(_SH_TZ)
    now_hhmm = local_now.strftime("%H:%M")
    trade_date = local_now.date().isoformat()

    if not _is_trading_day(local_now.date()):
        return SimulationScheduleDecision(False, "IDLE", trade_date, "non_trading_day")

    if not _is_enabled_session(now_hhmm, live_trade_config):
        return SimulationScheduleDecision(False, "IDLE", trade_date, "outside_session")

    schedule_type = str(live_trade_config.get("schedule_type") or "interval").lower()
    if schedule_type == "weekly":
        weekday = local_now.strftime("%a").upper()[:3]
        allowed = set(live_trade_config.get("trade_weekdays") or [])
        if weekday not in allowed:
            return SimulationScheduleDecision(False, "IDLE", trade_date, "weekday_skip")
    else:
        if not _is_interval_rebalance_day(
            local_now.date(),
            started_day=started_day,
            rebalance_days=max(1, _to_int(live_trade_config.get("rebalance_days"), 3)),
        ):
            return SimulationScheduleDecision(
                False, "IDLE", trade_date, "interval_skip"
            )

    phase = _resolve_phase(now_hhmm, live_trade_config)
    if phase == "IDLE":
        return SimulationScheduleDecision(False, phase, trade_date, "before_window")
    return SimulationScheduleDecision(True, phase, trade_date, "matched")


def _task_id(
    *,
    tenant_id: str,
    user_id: str,
    strategy_id: str,
    trade_date: str,
    phase: str,
) -> str:
    source = json.dumps(
        {
            "tenant_id": tenant_id,
            "user_id": user_id,
            "strategy_id": strategy_id,
            "trade_date": trade_date,
            "phase": phase,
            "mode": "SIMULATION",
        },
        ensure_ascii=False,
        sort_keys=True,
        separators=(",", ":"),
    )
    return f"hosted_sim_{hashlib.sha1(source.encode('utf-8')).hexdigest()[:16]}"


def _lock_key(
    *,
    tenant_id: str,
    user_id: str,
    strategy_id: str,
    trade_date: str,
    phase: str,
) -> str:
    return (
        f"qm:hosted:simulation:{tenant_id}:{user_id}:{strategy_id}:{trade_date}:{phase}"
    )


class SimulationHostedScheduler:
    def __init__(self, redis: RedisClient, interval_seconds: int = 30):
        self.redis = redis
        self.interval_seconds = max(5, int(interval_seconds or 30))
        self._stopped = asyncio.Event()
        self._task: asyncio.Task | None = None

    async def start(self) -> None:
        if self._task and not self._task.done():
            return
        self._stopped.clear()
        self._task = asyncio.create_task(
            self._run(), name="simulation-hosted-scheduler"
        )
        logger.info(
            "SimulationHostedScheduler started, interval=%ss", self.interval_seconds
        )

    async def stop(self) -> None:
        self._stopped.set()
        if self._task:
            self._task.cancel()
            try:
                await self._task
            except asyncio.CancelledError:
                pass
        logger.info("SimulationHostedScheduler stopped")

    async def _run(self) -> None:
        while not self._stopped.is_set():
            try:
                await self.run_once()
            except asyncio.CancelledError:
                raise
            except Exception as exc:
                logger.error(
                    "SimulationHostedScheduler loop failed: %s", exc, exc_info=True
                )

            try:
                await asyncio.wait_for(
                    self._stopped.wait(), timeout=self.interval_seconds
                )
            except asyncio.TimeoutError:
                continue

    async def run_once(self, *, now: datetime | None = None) -> int:
        if not self.redis.client:
            return 0

        triggered = 0
        current = now or datetime.now(_SH_TZ)
        for raw_key in self.redis.client.scan_iter(
            match="trade:active_strategy:*", count=500
        ):
            try:
                did_trigger = await self._process_key(str(raw_key), now=current)
                if did_trigger:
                    triggered += 1
            except Exception as exc:
                logger.warning(
                    "SimulationHostedScheduler skipped key=%s error=%s",
                    raw_key,
                    exc,
                    exc_info=True,
                )
        return triggered

    async def _process_key(self, key: str, *, now: datetime) -> bool:
        raw = self.redis.client.get(key)
        if not raw:
            return False
        try:
            active_data = json.loads(raw)
        except Exception:
            return False
        if not isinstance(active_data, dict):
            return False
        if str(active_data.get("mode") or "").upper() != "SIMULATION":
            return False

        parts = key.split(":")
        if len(parts) < 4:
            return False
        tenant_id = parts[-2].strip() or "default"
        user_id = parts[-1].strip()
        strategy_id = str(active_data.get("strategy_id") or "").strip()
        if not user_id or not strategy_id:
            return False

        live_trade_config = _normalize_live_trade_config(
            active_data.get("live_trade_config")
        )
        execution_config = (
            dict(active_data.get("execution_config"))
            if isinstance(active_data.get("execution_config"), dict)
            else {}
        )
        execution_config["trading_mode"] = "SIMULATION"
        started_day = _parse_started_at(active_data.get("started_at"))
        decision = _should_trigger(
            now=now,
            live_trade_config=live_trade_config,
            started_day=started_day,
        )
        if not decision.should_trigger:
            return False

        lock_key = _lock_key(
            tenant_id=tenant_id,
            user_id=user_id,
            strategy_id=strategy_id,
            trade_date=decision.trade_date,
            phase=decision.phase,
        )
        task_id = _task_id(
            tenant_id=tenant_id,
            user_id=user_id,
            strategy_id=strategy_id,
            trade_date=decision.trade_date,
            phase=decision.phase,
        )
        try:
            if not self.redis.client.set(lock_key, task_id, ex=36 * 3600, nx=True):
                return False
        except Exception:
            logger.warning("Failed to write simulation hosted lock: %s", lock_key)
            return False

        try:
            result = await manual_execution_service.create_hosted_task(
                tenant_id=tenant_id,
                user_id=user_id,
                strategy_id=strategy_id,
                trading_mode="SIMULATION",
                execution_config=execution_config,
                live_trade_config=live_trade_config,
                trigger_context={
                    "source": "simulation_hosted_scheduler",
                    "schedule_type": live_trade_config.get("schedule_type"),
                    "phase": decision.phase,
                    "triggered_at": now.astimezone(_SH_TZ).isoformat(),
                    "runner_trade_date": decision.trade_date,
                    "runner_mode": "SIMULATION",
                    "started_at": active_data.get("started_at"),
                },
                parent_runtime_id=str(active_data.get("run_id") or "").strip() or None,
                note="auto schedule from simulation hosted scheduler",
                task_id=task_id,
            )
            logger.info(
                "SimulationHostedScheduler task scheduled: tenant=%s user=%s strategy=%s phase=%s task=%s status=%s",
                tenant_id,
                user_id,
                strategy_id,
                decision.phase,
                task_id,
                result.get("status") if isinstance(result, dict) else None,
            )
            return True
        except Exception:
            try:
                self.redis.client.delete(lock_key)
            except Exception:
                pass
            raise


simulation_hosted_scheduler = None


def get_simulation_hosted_scheduler(redis: RedisClient) -> SimulationHostedScheduler:
    global simulation_hosted_scheduler
    if simulation_hosted_scheduler is None:
        simulation_hosted_scheduler = SimulationHostedScheduler(redis)
    return simulation_hosted_scheduler
