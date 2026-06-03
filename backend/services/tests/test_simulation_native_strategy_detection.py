from backend.services.trade.routers.real_trading_lifecycle import (
    _is_native_strategy_config_without_on_tick,
)


def test_native_strategy_config_without_on_tick_returns_true():
    code = """
STRATEGY_CONFIG = {"class": "RedisLongShortTopkStrategy"}
"""
    assert _is_native_strategy_config_without_on_tick(code) is True


def test_native_strategy_config_with_on_tick_returns_false():
    code = """
STRATEGY_CONFIG = {"class": "RedisLongShortTopkStrategy"}

def on_tick(context):
    context.log("tick")
"""
    assert _is_native_strategy_config_without_on_tick(code) is False
