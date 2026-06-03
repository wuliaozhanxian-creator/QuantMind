from datetime import datetime
from zoneinfo import ZoneInfo

from backend.services.trade.services.simulation_hosted_scheduler import (
    _normalize_live_trade_config,
    _should_trigger,
)


def test_same_sell_buy_time_am_interval_triggers_all_phase():
    cfg = _normalize_live_trade_config(
        {
            "schedule_type": "interval",
            "rebalance_days": 3,
            "enabled_sessions": ["AM"],
            "sell_time": "09:30",
            "buy_time": "09:30",
            "sell_first": True,
            "order_type": "MARKET",
            "max_price_deviation": 0.02,
            "max_orders_per_cycle": 20,
        }
    )
    started_day = datetime(2026, 6, 3, tzinfo=ZoneInfo("Asia/Shanghai")).date()

    decision = _should_trigger(
        now=datetime(2026, 6, 3, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        live_trade_config=cfg,
        started_day=started_day,
    )

    assert decision.should_trigger is True
    assert decision.phase == "ALL"
    assert decision.reason == "matched"


def test_am_interval_skips_until_next_configured_trade_day():
    cfg = _normalize_live_trade_config(
        {
            "schedule_type": "interval",
            "rebalance_days": 3,
            "enabled_sessions": ["AM"],
            "sell_time": "09:30",
            "buy_time": "09:30",
        }
    )
    started_day = datetime(2026, 6, 3, tzinfo=ZoneInfo("Asia/Shanghai")).date()

    next_day = _should_trigger(
        now=datetime(2026, 6, 4, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        live_trade_config=cfg,
        started_day=started_day,
    )
    weekend = _should_trigger(
        now=datetime(2026, 6, 6, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        live_trade_config=cfg,
        started_day=started_day,
    )
    third_trade_day = _should_trigger(
        now=datetime(2026, 6, 8, 9, 30, tzinfo=ZoneInfo("Asia/Shanghai")),
        live_trade_config=cfg,
        started_day=started_day,
    )

    assert next_day.should_trigger is False
    assert next_day.reason == "interval_skip"
    assert weekend.should_trigger is False
    assert weekend.reason == "non_trading_day"
    assert third_trade_day.should_trigger is True
    assert third_trade_day.phase == "ALL"
