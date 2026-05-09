from datetime import date
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status, File, UploadFile
from pydantic import BaseModel
from sqlalchemy import text
import httpx
import logging

from backend.services.trade.deps import AuthContext, get_auth_context, get_redis, get_db
from backend.services.trade.redis_client import RedisClient
from backend.services.trade.simulation.services.fund_snapshot_service import (
    SimulationFundSnapshotService,
)
from backend.services.trade.simulation.services.simulation_manager import (
    SimulationAccountManager,
)
from backend.services.trade.simulation.services.ocr_service import SimulationOCRService
from backend.services.trade.trade_config import settings
from backend.shared.database_manager_v2 import get_db_manager
from backend.shared.stock_utils import StockCodeUtil
from backend.services.trade.utils.stock_lookup import lookup_symbol_by_name, lookup_symbol_name

logger = logging.getLogger(__name__)

router = APIRouter()


DEFAULT_INITIAL_CASH = 1_000_000.0
SIM_AMOUNT_STEP = 100_000
COOLDOWN_DAYS = 30


async def _get_latest_price(symbol: str) -> float:
    """
    获取股票最新价格逻辑：
    1. 优先尝试行情服务实时数据
    2. 如果失败或数据为0，查询数据库 stock_daily_latest 获取最后一个交易日收盘价
    """
    market_url = settings.MARKET_DATA_SERVICE_URL.rstrip("/")
    price = 0.0
    
    # Level 1: 实时行情
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            resp = await client.get(f"{market_url}/api/v1/quotes/{symbol}")
            if resp.status_code == 200:
                q_data = resp.json()
                price = float(q_data.get("current_price") or q_data.get("last_price") or 0)
    except Exception as e:
        logger.warning(f"Failed to fetch real-time price for {symbol}: {e}")

    # Level 2: 数据库兜底
    if price <= 0:
        try:
            db_manager = get_db_manager()
            # 数据库 stock_daily_latest 使用 Prefix 格式 (SH600191)
            query = text("""
                SELECT close
                FROM stock_daily_latest
                WHERE symbol = :symbol
                ORDER BY trade_date DESC LIMIT 1
            """)
            
            async with db_manager.get_master_session() as session:
                result = await session.execute(query, {"symbol": symbol})
                row = result.fetchone()
                if row:
                    price = float(row[0] or 0)
                    logger.info(f"Fallback to DB latest close price for {symbol}: {price}")
        except Exception as e:
            logger.error(f"Database fallback failed for {symbol}: {e}")
            
    return price


async def _resolve_symbol_by_name(name: str) -> Optional[str]:
    """
    通过股票名称反查标准 Prefix 代码
    """
    if not name:
        return None
        
    try:
        clean_name = name.replace("*", "").strip()

        # 1. 优先走本地股票索引，避免依赖数据库字段结构
        local_symbol = lookup_symbol_by_name(clean_name)
        if local_symbol:
            return StockCodeUtil.to_prefix(local_symbol)
        
        db_manager = get_db_manager()
        async with db_manager.get_master_session() as session:
            columns_result = await session.execute(text("""
                SELECT column_name
                FROM information_schema.columns
                WHERE table_name = 'stock_daily_latest'
            """))
            available_columns = {str(row[0]) for row in columns_result.fetchall()}

            # 先做严格匹配，再做模糊匹配，避免名称片段误命中其他股票
            for column_name in ("name", "stock_name"):
                if column_name not in available_columns:
                    continue
                for operator, value in (("=", clean_name), ("LIKE", f"%{clean_name}%")):
                    query = text(f"""
                        SELECT symbol FROM stock_daily_latest
                        WHERE {column_name} {operator} :name
                        ORDER BY trade_date DESC
                        LIMIT 1
                    """)
                    result = await session.execute(query, {"name": value})
                    row = result.fetchone()
                    if row:
                        return row[0]
    except Exception as e:
        logger.error(f"Failed to resolve symbol for name {name}: {e}")
        
    return None


def _normalize_display_name(raw_name: Optional[str]) -> str:
    text_value = str(raw_name or "").strip()
    if not text_value:
        return ""
    return text_value.replace("*", "").replace("ST", "", 1).strip()


async def _resolve_symbol_from_ocr_item(symbol: Optional[str], name: Optional[str]) -> tuple[Optional[str], str]:
    """
    OCR 结果修正策略：
    1. 优先使用名称反查结果；
    2. 如果名称无法反查，再退回代码标准化结果；
    3. 返回最终 symbol 与匹配来源。
    """
    resolved_by_name = await _resolve_symbol_by_name(name or "")
    if resolved_by_name:
        return resolved_by_name, "name"

    normalized_symbol = StockCodeUtil.to_prefix(symbol or "") or None
    if normalized_symbol:
        return normalized_symbol, "code"

    return None, "unknown"


class AccountResetRequest(BaseModel):
    initial_cash: float | None = None


class HoldingItem(BaseModel):
    symbol: str
    quantity: float
    name: Optional[str] = None
    current_price: Optional[float] = None


class SyncHoldingsRequest(BaseModel):
    holdings: List[HoldingItem]


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
    return {
        "success": True, 
        "data": {
            "initial_cash": data.get("initial_cash", DEFAULT_INITIAL_CASH),
            "can_modify": False, # Modification deprecated
            "amount_step": SIM_AMOUNT_STEP,
            "cooldown_days": COOLDOWN_DAYS
        }
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


async def _get_user_api_key(db, user_id: str) -> Optional[str]:
    """从数据库获取用户配置的 AI-IDE API Key"""
    from sqlalchemy import text
    try:
        result = await db.execute(
            text("SELECT ai_ide_api_key FROM user_profiles WHERE user_id = :user_id"),
            {"user_id": user_id}
        )
        row = result.fetchone()
        return row[0] if row else None
    except Exception as e:
        import logging
        logging.getLogger(__name__).warning(f"Failed to fetch user api key: {e}")
        return None


@router.post("/sync/ocr")
async def ocr_sync_holdings(
    images: List[UploadFile] = File(...),
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
    db = Depends(get_db),
):
    # 获取用户自定义 API Key (优先使用用户中心的配置)
    user_api_key = await _get_user_api_key(db, auth.user_id)
    
    ocr_service = SimulationOCRService(api_key=user_api_key)
    image_data = []
    for img in images:
        content = await img.read()
        image_data.append(content)
        
    recognized_items = await ocr_service.analyze_images(image_data)
    
    # 4. 后处理：纠偏代码 & 获取实时行情
    results = []
    
    for item in recognized_items:
        original_symbol = item.get("symbol")
        name = item.get("name")

        symbol, matched_by = await _resolve_symbol_from_ocr_item(original_symbol, name)
        
        if not symbol:
            continue
            
        current_price = await _get_latest_price(symbol)
            
        results.append({
            **item,
            "symbol": symbol,
            "matched_by": matched_by,
            "current_price": current_price,
            "market_value": round(current_price * item["quantity"], 2)
        })
            
    return {"success": True, "data": results}


@router.post("/sync/confirm")
async def confirm_holding_sync(
    request: SyncHoldingsRequest,
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
):
    """
    确认同步 OCR 识别的持仓。
    逻辑：根据识别出的股票和数量，拉取当前最新市价，并重新计算账户初始金额，使同步后的盈亏对齐。
    """
    manager = SimulationAccountManager(redis)
    
    # 1. 预先获取所有股票的最新价格并计算总市值
    sync_positions = []
    total_market_value = 0.0
    
    for item in request.holdings:
        price = float(item.current_price or 0)
        if price <= 0:
            price = await _get_latest_price(item.symbol)
        
        if price <= 0:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail=f"无法获取股票 {item.symbol} 的实时价格或历史收盘价，同步中止。请检查代码是否正确。"
            )
        
        total_market_value += price * item.quantity
        sync_positions.append({
            "symbol": item.symbol,
            "quantity": item.quantity,
            "price": price
        })

    # 2. 计算同步后的“初始总资产”
    # 逻辑：我们将“可用现金”默认设为一个较小的基数
    sync_cash = 100.0 # 保留 100 元作为底仓现金
    calculated_initial_cash = total_market_value + sync_cash
    
    # 3. 初始化账户 (重置现金为 calculated_initial_cash)
    await manager.init_account(auth.user_id, calculated_initial_cash, auth.tenant_id)
    
    # 4. 写入持仓 (通过 update_balance 扣除现金，从而使总资产保持不变，盈亏从 0 开始)
    for pos in sync_positions:
        await manager.update_balance(
            user_id=auth.user_id,
            tenant_id=auth.tenant_id,
            symbol=pos["symbol"],
            delta_cash=-(pos["quantity"] * pos["price"]),
            delta_volume=pos["quantity"],
            price=pos["price"]
        )
            
    # 5. 捕获快照
    await _capture_simulation_snapshot(redis)
    
    return {"success": True, "message": f"持仓同步成功，初始资产已对齐至 {calculated_initial_cash:,.2f}"}
