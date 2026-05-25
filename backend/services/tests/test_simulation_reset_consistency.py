import json

import pytest

from backend.services.trade.deps import AuthContext
from backend.services.trade.routers import simulation as simulation_router
from backend.services.trade.services.simulation_manager import SimulationAccountManager


class _FakeRedisClient:
    def __init__(self):
        self.store = {}

    def get(self, key):
        return self.store.get(key)

    def set(self, key, value):
        self.store[key] = value

    def eval(self, script, numkeys, *args):
        key = args[0]
        symbol = args[1]
        delta_cash = float(args[2])
        delta_volume = float(args[3])
        price = float(args[4])
        account = json.loads(self.store[key])
        account["cash"] = float(account.get("cash") or 0.0) + delta_cash
        positions = dict(account.get("positions") or {})
        pos = dict(positions.get(symbol) or {"volume": 0, "cost": 0, "market_value": 0, "price": 0})
        pos["volume"] = float(pos.get("volume") or 0.0) + delta_volume
        pos["price"] = price
        pos["market_value"] = float(pos["volume"] or 0.0) * price
        positions[symbol] = pos
        account["positions"] = positions
        account["market_value"] = sum(float(p.get("volume") or 0.0) * float(p.get("price") or 0.0) for p in positions.values())
        account["total_asset"] = float(account.get("cash") or 0.0) + float(account.get("market_value") or 0.0)
        self.store[key] = json.dumps(account, ensure_ascii=False)
        return {"success": True}


class _FakeRedis:
    def __init__(self):
        self.client = _FakeRedisClient()


@pytest.mark.asyncio
async def test_reset_ignores_custom_initial_cash_and_returns_to_default(monkeypatch):
    redis = _FakeRedis()
    auth = AuthContext(user_id="1001", tenant_id="default", raw_sub="1001", roles=["user"])
    manager = SimulationAccountManager(redis)

    async def _fake_build_realtime_positions_from_db(*, tenant_id, user_id, since_at=None):
        return {}, 0.0

    monkeypatch.setattr(
        simulation_router,
        "_build_realtime_positions_from_db",
        _fake_build_realtime_positions_from_db,
    )

    # 先写入一个历史 settings，模拟旧口径值
    await manager.set_initial_cash(user_id=1001, initial_cash=300_000, tenant_id="default")

    # 即便前端仍传入其它值，重置也应统一回到默认 100 万
    await simulation_router.reset_simulation_account(
        request=simulation_router.AccountResetRequest(initial_cash=500_000),
        auth=auth,
        redis=redis,
    )

    # 读取账户时 initial_equity 应与默认重置资金一致
    resp = await simulation_router.get_simulation_account(auth=auth, redis=redis)
    data = resp["data"]
    assert data["cash"] == 1_000_000
    assert data["total_asset"] == 1_000_000
    assert data["initial_equity"] == 1_000_000
    assert data["baseline"]["initial_equity"] == 1_000_000

    settings = await manager.get_settings(
        user_id=1001,
        tenant_id="default",
        default_initial_cash=1_000_000,
        cooldown_days=30,
    )
    assert settings["initial_cash"] == 1_000_000


@pytest.mark.asyncio
async def test_get_simulation_account_casts_numeric_user_id_before_db_query(monkeypatch):
    redis = _FakeRedis()
    auth = AuthContext(
        user_id="40455298",
        tenant_id="default",
        raw_sub="40455298",
        roles=["user"],
    )
    manager = SimulationAccountManager(redis)
    await manager.init_account(user_id=auth.user_id, initial_cash=1_000_000, tenant_id="default")

    captured = {"user_id": None}

    async def _fake_build_realtime_positions_from_db(*, tenant_id, user_id, since_at=None):
        captured["user_id"] = user_id
        return {}, 0.0

    monkeypatch.setattr(
        simulation_router,
        "_build_realtime_positions_from_db",
        _fake_build_realtime_positions_from_db,
    )

    resp = await simulation_router.get_simulation_account(auth=auth, redis=redis)
    assert resp["success"] is True
    assert captured["user_id"] == 40455298


@pytest.mark.asyncio
async def test_get_simulation_account_non_numeric_user_id_skips_db_query(monkeypatch):
    redis = _FakeRedis()
    auth = AuthContext(
        user_id="sandbox-user",
        tenant_id="default",
        raw_sub="sandbox-user",
        roles=["user"],
    )
    manager = SimulationAccountManager(redis)
    await manager.init_account(user_id=auth.user_id, initial_cash=1_000_000, tenant_id="default")

    async def _should_not_run(*args, **kwargs):
        raise AssertionError("db aggregation should be skipped for non-numeric user_id")

    monkeypatch.setattr(
        simulation_router,
        "_build_realtime_positions_from_db",
        _should_not_run,
    )

    resp = await simulation_router.get_simulation_account(auth=auth, redis=redis)
    assert resp["success"] is True
    assert resp["data"]["cash"] == 1_000_000
