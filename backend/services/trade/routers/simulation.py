from datetime import date
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status
from pydantic import BaseModel

from backend.services.trade.deps import AuthContext, get_auth_context, get_redis
from backend.services.trade.redis_client import RedisClient
from backend.services.trade.simulation.services.fund_snapshot_service import (
    SimulationFundSnapshotService,
)
from backend.services.trade.simulation.services.simulation_manager import (
    SimulationAccountManager,
)

router = APIRouter()


DEFAULT_INITIAL_CASH = 1_000_000.0
SIM_AMOUNT_STEP = 100_000
COOLDOWN_DAYS = 30


class AccountResetRequest(BaseModel):
    initial_cash: float | None = None


class SimulationSettingsRequest(BaseModel):
    initial_cash: float


class SimulationSettingsResponse(BaseModel):
    initial_cash: float
    last_modified_at: str | None = None
    next_allowed_modified_at: str | None = None
    can_modify: bool
    cooldown_days: int
    amount_step: int


class SimulationFundSnapshotResponse(BaseModel):
    snapshot_date: date
    total_asset: Decimal
    available_balance: Decimal
    frozen_balance: Decimal
    market_value: Decimal
    initial_capital: Decimal
    total_pnl: Decimal
    today_pnl: Decimal
    source: str


@router.get("/settings")
async def get_simulation_settings(
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
):
    manager = SimulationAccountManager(redis)
    data = await manager.get_settings(
        user_id=auth.user_id,
        tenant_id=auth.tenant_id,
        default_initial_cash=DEFAULT_INITIAL_CASH,
        cooldown_days=COOLDOWN_DAYS,
    )
    data["amount_step"] = SIM_AMOUNT_STEP
    return {"success": True, "data": SimulationSettingsResponse(**data)}


async def _capture_simulation_snapshot(redis: RedisClient) -> None:
    try:
        await SimulationFundSnapshotService.capture_all(redis)
    except Exception as exc:
        # 快照失败不应影响主流程；只记录日志，避免配置/重置接口被历史数据采集问题阻断。
        import logging

        logging.getLogger(__name__).warning(
            "Failed to capture simulation fund snapshot: %s",
            exc,
            exc_info=True,
        )


@router.put("/settings")
async def update_simulation_settings(
    request: SimulationSettingsRequest,
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
):
    manager = SimulationAccountManager(redis)
    result = await manager.set_settings(
        user_id=auth.user_id,
        tenant_id=auth.tenant_id,
        initial_cash=request.initial_cash,
        amount_step=SIM_AMOUNT_STEP,
        cooldown_days=COOLDOWN_DAYS,
    )
    if not result.get("success"):
        error = result.get("error", "SETTINGS_UPDATE_FAILED")
        if error.startswith("INITIAL_CASH_INVALID_STEP"):
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"初始金额必须为{int(SIM_AMOUNT_STEP / 10000)}万元的整数倍",
            )
        if error == "INITIAL_CASH_COOLDOWN":
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail={
                    "message": "初始金额每30天只能修改一次",
                    "next_allowed_modified_at": result.get("next_allowed_modified_at"),
                },
            )
        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail="更新模拟盘设置失败",
        )

    data = result.get("data", {})
    data["amount_step"] = SIM_AMOUNT_STEP
    return {
        "success": True,
        "message": "Simulation settings updated",
        "data": SimulationSettingsResponse(**data),
    }


@router.post("/reset")
async def reset_simulation_account(
    request: AccountResetRequest,
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
):
    """
    Reset simulation account with initial cash.
    """
    manager = SimulationAccountManager(redis)
    if request.initial_cash is None:
        settings = await manager.get_settings(
            user_id=auth.user_id,
            tenant_id=auth.tenant_id,
            default_initial_cash=DEFAULT_INITIAL_CASH,
            cooldown_days=COOLDOWN_DAYS,
        )
        initial_cash = float(settings.get("initial_cash", DEFAULT_INITIAL_CASH))
    else:
        initial_cash = float(request.initial_cash)
    if initial_cash < SIM_AMOUNT_STEP or int(initial_cash) % SIM_AMOUNT_STEP != 0:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=f"初始金额必须为{int(SIM_AMOUNT_STEP / 10000)}万元的整数倍",
        )

    account = await manager.init_account(auth.user_id, initial_cash, tenant_id=auth.tenant_id)
    await _capture_simulation_snapshot(redis)
    return {"success": True, "message": "Simulation account reset", "data": account}


@router.get("/account")
async def get_simulation_account(
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
):
    """
    Get current simulation account state.
    """
    manager = SimulationAccountManager(redis)
    account = await manager.get_account(auth.user_id, tenant_id=auth.tenant_id)
    if not account:
        # Auto-init if not exists, and keep the account consistent with the latest saved settings.
        settings = await manager.get_settings(
            user_id=auth.user_id,
            tenant_id=auth.tenant_id,
            default_initial_cash=DEFAULT_INITIAL_CASH,
            cooldown_days=COOLDOWN_DAYS,
        )
        initial_cash = float(settings.get("initial_cash", DEFAULT_INITIAL_CASH))
        account = await manager.init_account(
            auth.user_id,
            initial_cash=initial_cash,
            tenant_id=auth.tenant_id,
        )
        await _capture_simulation_snapshot(redis)

    return {"success": True, "data": account}


@router.post("/snapshots/capture")
async def capture_simulation_fund_snapshot(
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
):
    """手动触发一次模拟账户资金快照采集（按天 upsert）。"""
    result = await SimulationFundSnapshotService.capture_all(redis)
    return {
        "success": True,
        "message": "simulation fund snapshot captured",
        "data": {
            "upserted_rows": result.upserted_rows,
            "scanned_accounts": result.scanned_accounts,
            "requested_by": str(auth.user_id),
        },
    }


@router.get("/snapshots/daily", response_model=list[SimulationFundSnapshotResponse])
async def list_simulation_fund_snapshots(
    days: int = Query(default=30, ge=1, le=3650),
    auth: AuthContext = Depends(get_auth_context),
):
    """查询当前用户的模拟盘日级资金快照历史。"""
    snapshots = await SimulationFundSnapshotService.list_user_daily(
        tenant_id=auth.tenant_id,
        user_id=str(auth.user_id),
        days=days,
    )

    def _to_response(snapshot) -> SimulationFundSnapshotResponse:
        snapshot_date = getattr(snapshot, "snapshot_date", None)
        if hasattr(snapshot_date, "date"):
            snapshot_date = snapshot_date.date()

        total_asset = getattr(snapshot, "total_asset", None)
        if total_asset is None:
            total_asset = getattr(snapshot, "total_assets", None)
        if total_asset is None:
            total_asset = 0

        available_balance = getattr(snapshot, "available_balance", None)
        if available_balance is None:
            available_balance = getattr(snapshot, "cash", None)
        if available_balance is None:
            available_balance = 0

        frozen_balance = getattr(snapshot, "frozen_balance", None)
        if frozen_balance is None:
            frozen_balance = 0

        market_value = getattr(snapshot, "market_value", None)
        if market_value is None:
            market_value = 0

        initial_capital = getattr(snapshot, "initial_capital", None)
        if initial_capital is None:
            initial_capital = total_asset

        total_pnl = getattr(snapshot, "total_pnl", None)
        if total_pnl is None:
            total_pnl = total_asset - initial_capital

        today_pnl = getattr(snapshot, "today_pnl", None)
        if today_pnl is None:
            today_pnl = getattr(snapshot, "today_pnl_raw", None)
        if today_pnl is None:
            today_pnl = 0

        source = getattr(snapshot, "source", None)
        if source is None:
            source = getattr(snapshot, "data", {}).get("source") if getattr(snapshot, "data", None) else None
        if not source:
            source = "redis_simulation_account"

        return SimulationFundSnapshotResponse(
            snapshot_date=snapshot_date,
            total_asset=total_asset,
            available_balance=available_balance,
            frozen_balance=frozen_balance,
            market_value=market_value,
            initial_capital=initial_capital,
            total_pnl=total_pnl,
            today_pnl=today_pnl,
            source=source,
        )

    return [
        _to_response(s)
        for s in snapshots
    ]
