import asyncio
from datetime import date, datetime
from decimal import Decimal
from typing import List, Optional

from fastapi import APIRouter, Depends, HTTPException, Query, status, File, UploadFile
from pydantic import BaseModel

from backend.services.trade.deps import AuthContext, get_auth_context, get_redis
from backend.services.trade.redis_client import RedisClient
from backend.services.trade.simulation.services.fund_snapshot_service import (
    SimulationFundSnapshotService,
)
from backend.services.trade.simulation.services.simulation_manager import (
    SimulationAccountManager,
)
from backend.services.trade.simulation.services.ocr_service import SimulationOCRService
from backend.services.trade.trade_config import settings
from backend.shared.database_manager_v2 import get_db_manager, get_session
import logging
import httpx
from sqlalchemy import text
from backend.shared.auth import get_internal_call_secret

logger = logging.getLogger(__name__)

router = APIRouter()


_SIM_TRADES_HAS_TRADE_ACTION: bool | None = None
_SIM_TRADES_TIME_COLUMN: str | None = None


async def _sim_trades_has_trade_action_column() -> bool:
    """检测 sim_trades 是否存在 trade_action 列。

    兼容老库结构：早期模拟盘表只有 side，没有 trade_action。
    这里做一次轻量探测并缓存，避免每次持仓聚合都触发异常重试。
    """
    global _SIM_TRADES_HAS_TRADE_ACTION
    if _SIM_TRADES_HAS_TRADE_ACTION is not None:
        return _SIM_TRADES_HAS_TRADE_ACTION

    try:
        db_manager = get_db_manager()
        query = text(
            """
            SELECT 1
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'sim_trades'
              AND column_name = 'trade_action'
            LIMIT 1
            """
        )
        async with db_manager.get_master_session() as session:
            row = (await session.execute(query)).first()
        _SIM_TRADES_HAS_TRADE_ACTION = bool(row)
    except Exception as exc:
        logger.warning("Failed to inspect sim_trades.trade_action column: %s", exc)
        _SIM_TRADES_HAS_TRADE_ACTION = False

    return _SIM_TRADES_HAS_TRADE_ACTION


async def _sim_trades_time_column() -> str | None:
    """返回 sim_trades 可用于时间过滤的列名。

    优先使用 executed_at；老库若没有该列，则回退到 created_at。
    两者都没有时返回 None，调用方需要跳过时间过滤。
    """
    global _SIM_TRADES_TIME_COLUMN
    if _SIM_TRADES_TIME_COLUMN is not None:
        return _SIM_TRADES_TIME_COLUMN

    try:
        db_manager = get_db_manager()
        query = text(
            """
            SELECT column_name
            FROM information_schema.columns
            WHERE table_schema = 'public'
              AND table_name = 'sim_trades'
              AND column_name IN ('executed_at', 'created_at')
            ORDER BY CASE column_name WHEN 'executed_at' THEN 0 ELSE 1 END
            LIMIT 1
            """
        )
        async with db_manager.get_master_session() as session:
            row = (await session.execute(query)).first()
        _SIM_TRADES_TIME_COLUMN = str(row[0]) if row and row[0] else None
    except Exception as exc:
        logger.warning("Failed to inspect sim_trades time column: %s", exc)
        _SIM_TRADES_TIME_COLUMN = None

    return _SIM_TRADES_TIME_COLUMN


async def _get_latest_price(symbol: str) -> float:
    """
    获取股票最新价格逻辑：
    1. 优先尝试行情服务实时数据
    2. 如果失败或数据为0，查询数据库 stock_daily_latest 获取最后一天收盘价
    """
    market_url = settings.MARKET_DATA_SERVICE_URL.rstrip("/")
    price = 0.0
    raw_symbol = str(symbol or "").strip().upper()
    prefix_symbol = raw_symbol
    suffix_symbol = raw_symbol
    if raw_symbol.endswith((".SH", ".SZ", ".BJ")) and len(raw_symbol) > 3:
        prefix_symbol = f"{raw_symbol[-2:]}{raw_symbol[:-3]}"
    elif raw_symbol.startswith(("SH", "SZ", "BJ")) and len(raw_symbol) > 2:
        suffix_symbol = f"{raw_symbol[2:]}.{raw_symbol[:2]}"

    # Level 1: 实时行情
    try:
        async with httpx.AsyncClient(timeout=3.0) as client:
            headers = {"X-Internal-Call": get_internal_call_secret()}
            for candidate in [raw_symbol, prefix_symbol, suffix_symbol]:
                if not candidate:
                    continue
                resp = await client.get(f"{market_url}/api/v1/quotes/{candidate}", headers=headers)
                if resp.status_code != 200:
                    continue
                q_data = resp.json()
                price = float(q_data.get("current_price") or q_data.get("last_price") or 0)
                if price > 0:
                    break
    except Exception as e:
        logger.warning(f"Failed to fetch real-time price for {raw_symbol}: {e}")

    # Level 2: 数据库兜底
    if price <= 0:
        try:
            db_manager = get_db_manager()
            # 数据库 stock_daily_latest 使用 Prefix 格式 (SH600191)
            query = text("""
                SELECT close, adj_factor FROM stock_daily_latest
                WHERE symbol = :symbol
                ORDER BY trade_date DESC LIMIT 1
            """)

            async with db_manager.get_master_session() as session:
                result = await session.execute(query, {"symbol": prefix_symbol})
                row = result.fetchone()
                if not row and suffix_symbol != prefix_symbol:
                    result = await session.execute(query, {"symbol": suffix_symbol})
                    row = result.fetchone()
                if row:
                    hfq_close = float(row[0])
                    adj_factor = float(row[1] or 1.0)
                    # 计算名义价格 (除权价)
                    price = hfq_close / adj_factor if adj_factor > 0 else hfq_close
                    logger.info(f"Fallback to DB nominal price for {raw_symbol}: {price} (Hfq: {hfq_close}, Adj: {adj_factor})")
        except Exception as e:
            logger.error(f"Database fallback failed for {raw_symbol}: {e}")
            
    return price


async def _resolve_symbol_by_name(name: str) -> Optional[str]:
    """
    通过股票名称反查标准 Prefix 代码
    """
    if not name:
        return None
        
    try:
        db_manager = get_db_manager()
        # 清理名称中的特殊字符，如 *ST
        clean_name = name.replace("*", "").strip()
        query = text("""
            SELECT symbol FROM stock_daily_latest 
            WHERE stock_name LIKE :name
            ORDER BY trade_date DESC LIMIT 1
        """)
        
        async with db_manager.get_master_session() as session:
            # 先试完全匹配
            result = await session.execute(query, {"name": f"%{clean_name}%"})
            row = result.fetchone()
            if row:
                return row[0]
    except Exception as e:
        logger.error(f"Failed to resolve symbol for name {name}: {e}")
        
    return None


async def _build_realtime_positions_from_db(
    *,
    tenant_id: str,
    user_id: int,
    since_at: Optional[datetime] = None,
) -> tuple[dict[str, dict[str, float]], float]:
    """从 sim_trades 聚合当前持仓，并用最新行情重算持仓市值。支持多空双向。"""
    since_naive = None
    if isinstance(since_at, datetime):
        since_naive = (
            since_at.replace(tzinfo=None)
            if since_at.tzinfo is None
            else since_at.astimezone().replace(tzinfo=None)
        )

    base_filter = "WHERE tenant_id = :tenant_id AND user_id = :user_id"
    extra_filter = ""
    # sim_trades.user_id 在当前库结构里按字符串存储，显式转成 str 避免 asyncpg 类型不匹配。
    params: dict = {"tenant_id": tenant_id, "user_id": str(user_id)}
    if since_naive is not None:
        time_column = await _sim_trades_time_column()
        if time_column:
            extra_filter = f" AND {time_column} >= :since_at"
            params["since_at"] = since_naive

    has_trade_action = await _sim_trades_has_trade_action_column()
    if has_trade_action:
        # 多头净量：buy_to_open 增仓 / sell_to_close 平仓
        # trade_action 为空时（旧数据）保持原先 buy=+/sell=- 逻辑
        long_sql = f"""
            SELECT symbol,
                   SUM(CASE
                           WHEN trade_action IN ('buy_to_open', 'buy') OR (trade_action IS NULL AND side = 'buy')
                               THEN quantity
                           WHEN trade_action IN ('sell_to_close') OR (trade_action IS NULL AND side = 'sell')
                               THEN -quantity
                           ELSE 0
                       END) AS volume
            FROM sim_trades
            {base_filter}{extra_filter}
            GROUP BY symbol
            HAVING SUM(CASE
                           WHEN trade_action IN ('buy_to_open', 'buy') OR (trade_action IS NULL AND side = 'buy')
                               THEN quantity
                           WHEN trade_action IN ('sell_to_close') OR (trade_action IS NULL AND side = 'sell')
                               THEN -quantity
                           ELSE 0
                       END) > 0.000001
            ORDER BY symbol
        """

        # 空头净量：sell_to_open 开空 / buy_to_close 平空
        short_sql = f"""
            SELECT symbol,
                   SUM(CASE
                           WHEN trade_action = 'sell_to_open' THEN quantity
                           WHEN trade_action = 'buy_to_close' THEN -quantity
                           ELSE 0
                       END) AS volume
            FROM sim_trades
            {base_filter}{extra_filter}
              AND trade_action IN ('sell_to_open', 'buy_to_close')
            GROUP BY symbol
            HAVING SUM(CASE
                           WHEN trade_action = 'sell_to_open' THEN quantity
                           WHEN trade_action = 'buy_to_close' THEN -quantity
                           ELSE 0
                       END) > 0.000001
            ORDER BY symbol
        """
    else:
        # 兼容老库：sim_trades 尚未包含 trade_action 列时，只按 side 聚合多头净量。
        # 由于无法区分“开空/平空”，这里不构造空头仓位，避免错误报表和异常重试。
        long_sql = f"""
            SELECT symbol,
                   SUM(CASE
                           WHEN side = 'buy' THEN quantity
                           WHEN side = 'sell' THEN -quantity
                           ELSE 0
                       END) AS volume
            FROM sim_trades
            {base_filter}{extra_filter}
            GROUP BY symbol
            HAVING SUM(CASE
                           WHEN side = 'buy' THEN quantity
                           WHEN side = 'sell' THEN -quantity
                           ELSE 0
                       END) > 0.000001
            ORDER BY symbol
        """
        short_sql = None

    try:
        async with get_session(read_only=True) as session:
            long_rows = (await session.execute(text(long_sql), params)).mappings().all()
            short_rows = (
                (await session.execute(text(short_sql), params)).mappings().all()
                if short_sql
                else []
            )
    except Exception as e:
        logger.warning(f"Database query failed in _build_realtime_positions_from_db: {e}")
        long_rows = []
        short_rows = []

    positions: dict[str, dict[str, float]] = {}
    total_market_value = 0.0

    long_candidates: list[tuple[str, float]] = []
    short_candidates: list[tuple[str, float]] = []
    symbols: set[str] = set()

    for row in long_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        volume = float(row.get("volume") or 0.0)
        if not symbol or volume <= 0:
            continue
        long_candidates.append((symbol, volume))
        symbols.add(symbol)

    for row in short_rows:
        symbol = str(row.get("symbol") or "").strip().upper()
        volume = float(row.get("volume") or 0.0)
        if not symbol or volume <= 0:
            continue
        short_candidates.append((symbol, volume))
        symbols.add(symbol)

    price_pairs = await asyncio.gather(
        *[_get_latest_price(sym) for sym in sorted(symbols)],
        return_exceptions=True,
    )
    latest_price_map: dict[str, float] = {}
    for sym, price in zip(sorted(symbols), price_pairs):
        latest_price_map[sym] = float(price) if isinstance(price, (int, float)) else 0.0

    for symbol, volume in long_candidates:
        last_price = float(latest_price_map.get(symbol) or 0.0)
        if last_price <= 0:
            continue
        market_value = round(last_price * volume, 2)
        positions[symbol] = {
            "volume": volume,
            "available_volume": volume,
            "price": round(last_price, 4),
            "last_price": round(last_price, 4),
            "market_value": market_value,
            "side": "long",
        }
        total_market_value += market_value

    for symbol, volume in short_candidates:
        last_price = float(latest_price_map.get(symbol) or 0.0)
        if last_price <= 0:
            continue
        market_value = round(last_price * volume, 2)
        key = f"{symbol}:short"
        positions[key] = {
            "symbol": symbol,
            "volume": volume,
            "available_volume": volume,
            "price": round(last_price, 4),
            "last_price": round(last_price, 4),
            "market_value": market_value,
            "side": "short",
        }
        total_market_value += market_value

    return positions, round(total_market_value, 2)


DEFAULT_INITIAL_CASH = 1_000_000.0
SIM_AMOUNT_STEP = 100_000
COOLDOWN_DAYS = 30


class AccountResetRequest(BaseModel):
    initial_cash: float | None = None


class HoldingItem(BaseModel):
    symbol: str
    quantity: float
    name: Optional[str] = None


class SyncHoldingsRequest(BaseModel):
    holdings: List[HoldingItem]
    available_cash: Optional[float] = None


# SimulationSettingsRequest removed as it is deprecated.


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


# update_simulation_settings (PUT /settings) removed as it is deprecated.


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
    initial_cash = DEFAULT_INITIAL_CASH

    # 模拟盘重置口径固定回到 100 万，不再接受外部初始资金参数。
    await manager.set_initial_cash(auth.user_id, initial_cash, tenant_id=auth.tenant_id)
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
    如果账户不存在，返回空账户（total_asset=0），不自动初始化。
    """
    manager = SimulationAccountManager(redis)
    account = await manager.get_account(auth.user_id, tenant_id=auth.tenant_id)
    if not account:
        # 不再自动初始化，返回空账户标记
        return {
            "success": True,
            "data": {
                "cash": 0.0,
                "total_asset": 0.0,
                "market_value": 0.0,
                "positions": {},
                "account_not_initialized": True,
            }
        }

    # 从 settings 中读取 initial_cash 作为 initial_equity
    settings = await manager.get_settings(
        user_id=auth.user_id,
        tenant_id=auth.tenant_id,
        default_initial_cash=DEFAULT_INITIAL_CASH,
        cooldown_days=COOLDOWN_DAYS,
    )
    initial_equity = float(settings.get("initial_cash", DEFAULT_INITIAL_CASH))
    reset_anchor_raw = settings.get("last_modified_at")
    reset_anchor = None
    if isinstance(reset_anchor_raw, str) and reset_anchor_raw.strip():
        try:
            reset_anchor = datetime.fromisoformat(
                reset_anchor_raw.replace("Z", "+00:00")
            )
        except Exception:
            reset_anchor = None

    # 从数据库成交表聚合持仓，并按最新行情重算实时市值（优先口径）。
    try:
        user_id_int = int(auth.user_id)
    except (TypeError, ValueError):
        user_id_int = 0

    db_positions: dict[str, dict[str, float]] = {}
    db_market_value = 0.0
    if user_id_int > 0:
        db_positions, db_market_value = await _build_realtime_positions_from_db(
            tenant_id=auth.tenant_id,
            user_id=user_id_int,
            since_at=reset_anchor,
        )

    cash = float(account.get("cash") or account.get("available_balance") or 0.0)
    market_value = db_market_value
    positions = db_positions
    redis_positions_map = account.get("positions") if isinstance(account.get("positions"), dict) else {}
    if not positions:
        # 兼容历史链路：若数据库尚无成交，则回退到 Redis 仓位。
        raw_positions = account.get("positions") or {}
        if isinstance(raw_positions, dict):
            for sym, pos in raw_positions.items():
                symbol = str(sym or "").strip().upper()
                if not symbol or not isinstance(pos, dict):
                    continue
                volume = float(pos.get("volume") or 0.0)
                last_price = float(pos.get("price") or pos.get("last_price") or 0.0)
                if volume <= 0 or last_price <= 0:
                    continue
                market_val = round(volume * last_price, 2)
                positions[symbol] = {
                    "volume": volume,
                    "available_volume": float(pos.get("available_volume") or volume),
                    "price": round(last_price, 4),
                    "last_price": round(last_price, 4),
                    "market_value": market_val,
                }
                market_value += market_val
        market_value = round(market_value, 2)
    else:
        # DB 聚合口径优先时，补齐仓位成本价（来自 Redis 持仓对象）用于浮动盈亏计算。
        if isinstance(redis_positions_map, dict):
            for symbol, pos in positions.items():
                if not isinstance(pos, dict):
                    continue
                candidates = [
                    redis_positions_map.get(symbol),
                    redis_positions_map.get(f"{symbol}::long"),
                ]
                # Prefix / Suffix 双格式匹配
                if symbol.endswith((".SH", ".SZ", ".BJ")) and len(symbol) > 3:
                    prefix = f"{symbol[-2:]}{symbol[:-3]}"
                    candidates.append(redis_positions_map.get(prefix))
                    candidates.append(redis_positions_map.get(f"{prefix}::long"))
                elif symbol.startswith(("SH", "SZ", "BJ")) and len(symbol) > 2:
                    suffix = f"{symbol[2:]}.{symbol[:2]}"
                    candidates.append(redis_positions_map.get(suffix))
                    candidates.append(redis_positions_map.get(f"{suffix}::long"))

                redis_pos = next((item for item in candidates if isinstance(item, dict)), None)
                if redis_pos:
                    cost_price = float(
                        redis_pos.get("cost")
                        or redis_pos.get("cost_price")
                        or redis_pos.get("avg_cost")
                        or redis_pos.get("avg_price")
                        or 0.0
                    )
                    if cost_price > 0:
                        pos["cost_price"] = round(cost_price, 4)

    total_asset = round(cash + market_value, 2)
    floating_pnl = 0.0
    for pos in positions.values():
        if not isinstance(pos, dict):
            continue
        volume = float(pos.get("volume") or 0.0)
        last_price = float(pos.get("last_price") or pos.get("price") or 0.0)
        cost_price = float(
            pos.get("cost_price")
            or pos.get("avg_cost")
            or pos.get("avg_price")
            or pos.get("cost")
            or 0.0
        )
        side = str(pos.get("side") or "long").lower()
        if volume <= 0 or last_price <= 0 or cost_price <= 0:
            continue
        if side == "short":
            floating_pnl += (cost_price - last_price) * volume
        else:
            floating_pnl += (last_price - cost_price) * volume
    floating_pnl = round(floating_pnl, 2)

    # 今日盈亏基线优先取上一交易日资金快照；没有历史快照时回退初始权益。
    day_open_equity = initial_equity
    try:
        today = date.today()
        async with get_session(read_only=True) as session:
            prev_stmt = text("""
                SELECT total_asset
                FROM simulation_fund_snapshots
                WHERE tenant_id = :tenant_id
                  AND user_id = :user_id
                  AND snapshot_date < :today
                ORDER BY snapshot_date DESC
                LIMIT 1
            """)
            prev_row = (
                await session.execute(
                    prev_stmt,
                    {"tenant_id": auth.tenant_id, "user_id": str(auth.user_id), "today": today},
                )
            ).first()
            if prev_row and prev_row[0] is not None:
                day_open_equity = float(prev_row[0])
    except Exception:
        day_open_equity = initial_equity
    today_pnl = round(total_asset - day_open_equity, 2)
    total_pnl = round(total_asset - initial_equity, 2)

    # 添加 initial_equity 和 baseline 字段
    account["cash"] = cash
    account["available_cash"] = cash
    account["market_value"] = market_value
    account["total_asset"] = total_asset
    account["today_pnl"] = today_pnl
    account["daily_pnl"] = today_pnl
    account["floating_pnl"] = floating_pnl
    account["total_pnl"] = total_pnl
    account["day_open_equity"] = day_open_equity
    account["positions"] = positions
    account["position_count"] = len(positions)
    account["initial_equity"] = initial_equity
    account["valuation_source"] = "sim_trades_plus_realtime_quote"
    account["baseline"] = {
        "initial_equity": initial_equity,
        "day_open_equity": day_open_equity,
        "month_open_equity": initial_equity,
    }

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
    return [
        SimulationFundSnapshotResponse(
            snapshot_date=s.snapshot_date,
            total_asset=s.total_asset,
            available_balance=s.available_balance,
            frozen_balance=s.frozen_balance,
            market_value=s.market_value,
            initial_capital=s.initial_capital,
            total_pnl=s.total_pnl,
            today_pnl=s.today_pnl,
            source=s.source,
        )
        for s in snapshots
    ]


@router.post("/sync/ocr")
async def ocr_sync_holdings(
    images: List[UploadFile] = File(...),
    auth: AuthContext = Depends(get_auth_context),
    redis: RedisClient = Depends(get_redis),
):
    ocr_service = SimulationOCRService()
    image_data = []
    for img in images:
        content = await img.read()
        image_data.append(content)
        
    ocr_result = await ocr_service.analyze_images(image_data)
    recognized_items = ocr_result.get("holdings", [])
    available_cash = ocr_result.get("available_cash")
    
    # 4. 后处理：纠偏代码 & 获取实时行情
    results = []
    
    for item in recognized_items:
        original_symbol = item.get("symbol")
        name = item.get("name")
        
        # 优先使用 OCR 识别出的代码（已在服务层通过 stocks_index.json 对齐）
        symbol = original_symbol if original_symbol else await _resolve_symbol_by_name(name)
        
        if not symbol:
            logger.warning(f"Skipping holding {name}: Symbol could not be resolved.")
            continue
            
        current_price = await _get_latest_price(symbol)
            
        results.append({
            **item,
            "symbol": symbol,
            "current_price": current_price,
            "market_value": round(current_price * item["quantity"], 2)
        })
            
    return {
        "success": True, 
        "data": results,
        "available_cash": available_cash
    }


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

    # 2. 计算同步后的”初始总资产”
    # 逻辑：我们将”可用现金”优先使用截图识别到的数值，如果没有则使用默认基数
    sync_cash = request.available_cash if request.available_cash is not None else 100.0
    calculated_initial_cash = total_market_value + sync_cash

    # 3. 更新 settings 中的 initial_cash（用于前端显示初始权益）
    await manager.set_initial_cash(auth.user_id, calculated_initial_cash, auth.tenant_id)

    # 4. 初始化账户 (重置现金为 calculated_initial_cash)
    await manager.init_account(auth.user_id, calculated_initial_cash, auth.tenant_id)

    # 5. 写入持仓 (通过 update_balance 扣除现金，从而使总资产保持不变，盈亏从 0 开始)
    for pos in sync_positions:
        # delta_cash = -(数量 * 现价)，这样操作后：
        # 现金减少，市值增加，总资产 = calculated_initial_cash 保持不变
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
