from datetime import datetime

import pytest
from sqlalchemy import text
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker, create_async_engine
from zoneinfo import ZoneInfo

from backend.services.trade.simulation.services.corporate_action_service import (
    SimulationCorporateActionService,
)
from backend.services.trade.simulation.services.eod_service import (
    _resolve_target_trade_date,
)
from backend.services.trade.simulation.services.eod_service import _load_close_price
from backend.services.trade.routers.simulation import (
    _should_prefer_redis_account_snapshot,
)


@pytest.mark.asyncio
async def test_corporate_action_price_loader_uses_stock_daily_latest_close_directly():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE stock_daily_latest (
                    symbol TEXT,
                    trade_date TEXT,
                    close FLOAT,
                    adj_factor FLOAT
                )
                """
            )
        )

    async with session_factory() as session:
        await session.execute(
            text(
                """
                INSERT INTO stock_daily_latest(symbol, trade_date, close, adj_factor)
                VALUES ('SH600159', '2026-06-17', 2.42, 5.212126859504132)
                """
            )
        )
        await session.commit()
        price = await SimulationCorporateActionService._load_latest_price(
            session, "SH600159"
        )

    await engine.dispose()

    assert price == pytest.approx(2.42)


@pytest.mark.asyncio
async def test_eod_close_price_loader_does_not_divide_by_adj_factor():
    engine = create_async_engine("sqlite+aiosqlite:///:memory:")
    session_factory = async_sessionmaker(
        engine, class_=AsyncSession, expire_on_commit=False
    )
    async with engine.begin() as conn:
        await conn.execute(
            text(
                """
                CREATE TABLE stock_daily_latest (
                    symbol TEXT,
                    trade_date TEXT,
                    close FLOAT,
                    adj_factor FLOAT
                )
                """
            )
        )

    async with session_factory() as session:
        await session.execute(
            text(
                """
                INSERT INTO stock_daily_latest(symbol, trade_date, close, adj_factor)
                VALUES ('SZ002495', '2026-06-17', 2.19, 9.801048858447489)
                """
            )
        )
        await session.commit()
        price = await _load_close_price(session, "SZ002495")

    await engine.dispose()

    assert price == pytest.approx(2.19)


def test_simulation_account_prefers_redis_before_daily_reprice_ready_time():
    tz = ZoneInfo("Asia/Shanghai")
    assert _should_prefer_redis_account_snapshot(
        datetime(2026, 6, 19, 2, 30, tzinfo=tz)
    )
    assert not _should_prefer_redis_account_snapshot(
        datetime(2026, 6, 19, 4, 0, tzinfo=tz)
    )
    assert _should_prefer_redis_account_snapshot(
        datetime(2026, 6, 19, 10, 0, tzinfo=tz)
    )


def test_simulation_eod_worker_targets_previous_trade_day_for_overnight_run():
    tz = ZoneInfo("Asia/Shanghai")
    assert (
        _resolve_target_trade_date(datetime(2026, 6, 19, 3, 30, tzinfo=tz)).isoformat()
        == "2026-06-18"
    )
