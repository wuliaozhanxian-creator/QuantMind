"""
Synthetic execution engine for simulation orders.
"""

import logging
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import httpx
from sqlalchemy.ext.asyncio import AsyncSession

from backend.services.trade.simulation.models.order import (
    OrderStatus,
    OrderType,
    SimOrder,
)
from backend.services.trade.simulation.models.trade import SimTrade
from backend.services.trade.simulation.services.simulation_manager import (
    SimulationAccountManager,
)
from backend.services.trade.trade_config import settings
from backend.shared.auth import get_internal_call_secret
from backend.shared.trade_account_cache import write_trade_account_cache

logger = logging.getLogger(__name__)


class ExecutionResult:
    def __init__(
        self,
        *,
        success: bool,
        price: float = 0.0,
        quantity: float = 0.0,
        commission: float = 0.0,
        stamp_duty: float = 0.0,
        transfer_fee: float = 0.0,
        price_source: str | None = None,
        message: str = "",
    ):
        self.success = success
        self.price = price
        self.quantity = quantity
        self.commission = commission
        self.stamp_duty = stamp_duty
        self.transfer_fee = transfer_fee
        self.price_source = price_source
        self.message = message


@dataclass
class MarketSnapshot:
    price: float
    price_source: str
    limit_up: bool = False
    limit_down: bool = False
    suspended: bool = False


class SimulationExecutionEngine:
    def __init__(self, db: AsyncSession, manager: SimulationAccountManager):
        self.db = db
        self.manager = manager
        self._http: httpx.AsyncClient | None = None

    async def _http_client(self) -> httpx.AsyncClient:
        if self._http is None:
            self._http = httpx.AsyncClient(timeout=5.0)
        return self._http

    @staticmethod
    def _as_float(value: Any) -> float | None:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_int(value: Any) -> int | None:
        try:
            if value is None:
                return None
            return int(float(value))
        except (TypeError, ValueError):
            return None

    @staticmethod
    def _as_bool(value: Any) -> bool:
        if isinstance(value, bool):
            return value
        if value is None:
            return False
        if isinstance(value, (int, float)):
            return bool(value)
        text = str(value).strip().lower()
        if not text:
            return False
        return text in {"1", "true", "yes", "y", "on"}

    @staticmethod
    def _is_price_near(price: float, limit_price: float | None, tolerance: float = 0.0015) -> bool:
        if limit_price is None or limit_price <= 0 or price <= 0:
            return False
        return abs(price - limit_price) / max(limit_price, 1e-6) <= tolerance

    async def _latest_price(
        self,
        symbol: str,
        *,
        user_id: int | None = None,
        tenant_id: str | None = None,
    ) -> MarketSnapshot:
        raw_symbol = str(symbol or "").strip().upper()
        prefix_symbol = raw_symbol
        suffix_symbol = raw_symbol
        if raw_symbol.endswith((".SH", ".SZ", ".BJ")) and len(raw_symbol) > 3:
            prefix_symbol = f"{raw_symbol[-2:]}{raw_symbol[:-3]}"
        elif raw_symbol.startswith(("SH", "SZ", "BJ")) and len(raw_symbol) > 2:
            suffix_symbol = f"{raw_symbol[2:]}.{raw_symbol[:2]}"

        market_url = settings.MARKET_DATA_SERVICE_URL.rstrip("/")

        # Level 1: 实时行情服务
        try:
            client = await self._http_client()
            headers = {"X-Internal-Call": get_internal_call_secret()}
            if user_id is not None:
                headers["X-User-Id"] = str(user_id)
                headers["X-Tenant-Id"] = str(tenant_id or "default")

            for candidate in [raw_symbol, prefix_symbol, suffix_symbol]:
                if not candidate:
                    continue
                endpoint = f"{market_url}/api/v1/quotes/{candidate}"
                resp = await client.get(endpoint, headers=headers)
                if resp.status_code != 200:
                    continue
                data = resp.json()
                px = self._as_float(data.get("current_price") or data.get("last_price"))
                if px and px > 0:
                    limit_up = self._as_bool(data.get("is_limit_up"))
                    limit_down = self._as_bool(data.get("is_limit_down"))
                    suspended = self._as_bool(data.get("suspended") or data.get("is_suspended"))
                    limit_up_price = self._as_float(data.get("limit_up_today"))
                    limit_down_price = self._as_float(data.get("limit_down_today"))
                    if not limit_up and self._is_price_near(px, limit_up_price):
                        limit_up = True
                    if not limit_down and self._is_price_near(px, limit_down_price):
                        limit_down = True

                    pre_close = self._as_float(data.get("pre_close") or data.get("close_price"))
                    ask1_volume = self._as_int(data.get("ask1_volume"))
                    bid1_volume = self._as_int(data.get("bid1_volume"))
                    if pre_close and pre_close > 0:
                        change_ratio = (px - pre_close) / pre_close
                        if not limit_up and ask1_volume is not None and ask1_volume <= 0 and change_ratio >= 0.095:
                            limit_up = True
                        if not limit_down and bid1_volume is not None and bid1_volume <= 0 and change_ratio <= -0.095:
                            limit_down = True

                    return MarketSnapshot(
                        price=px,
                        price_source="market_data_service",
                        limit_up=limit_up,
                        limit_down=limit_down,
                        suspended=suspended,
                    )
        except Exception as e:
            logger.warning("Failed to fetch market quote for %s: %s", raw_symbol, e)

        # Level 2: 数据库兜底 (L2 Fallback)
        try:
            from sqlalchemy import text

            query_with_limits = text("""
                SELECT close, adj_factor, limit_up_today, limit_down_today, volume
                FROM stock_daily_latest
                WHERE symbol = :symbol
                ORDER BY trade_date DESC LIMIT 1
            """)
            try:
                result = await self.db.execute(query_with_limits, {"symbol": prefix_symbol})
                row = result.fetchone()
                if not row and suffix_symbol != prefix_symbol:
                    result = await self.db.execute(query_with_limits, {"symbol": suffix_symbol})
                    row = result.fetchone()
                if row:
                    hfq_close = float(row[0])
                    adj_factor = float(row[1] or 1.0)
                    price = hfq_close / adj_factor if adj_factor > 0 else hfq_close
                    logger.info("Fallback to DB nominal price for %s: %s", raw_symbol, price)
                    return MarketSnapshot(
                        price=price,
                        price_source="db_fallback",
                        limit_up=self._is_price_near(price, self._as_float(row[2])),
                        limit_down=self._is_price_near(price, self._as_float(row[3])),
                        suspended=(self._as_float(row[4]) or 0.0) <= 0.0,
                    )
            except Exception:
                query_legacy = text("""
                    SELECT close, adj_factor
                    FROM stock_daily_latest
                    WHERE symbol = :symbol
                    ORDER BY trade_date DESC LIMIT 1
                """)
                legacy_result = await self.db.execute(query_legacy, {"symbol": prefix_symbol})
                legacy_row = legacy_result.fetchone()
                if not legacy_row and suffix_symbol != prefix_symbol:
                    legacy_result = await self.db.execute(query_legacy, {"symbol": suffix_symbol})
                    legacy_row = legacy_result.fetchone()
                if legacy_row:
                    hfq_close = float(legacy_row[0])
                    adj_factor = float(legacy_row[1] or 1.0)
                    price = hfq_close / adj_factor if adj_factor > 0 else hfq_close
                    logger.info("Fallback to DB legacy nominal price for %s: %s", raw_symbol, price)
                    return MarketSnapshot(price=price, price_source="db_fallback")
        except Exception as e:
            logger.error("Database fallback failed for %s: %s", raw_symbol, e)

        # Level 3: 拒单而非随机价格，避免污染账户数据
        return MarketSnapshot(
            price=0.0,
            price_source="unavailable",
            suspended=True,
        )

    async def execute_order(self, order: SimOrder) -> ExecutionResult:
        snapshot = await self._latest_price(
            order.symbol,
            user_id=order.user_id,
            tenant_id=order.tenant_id,
        )
        base_price = snapshot.price
        fetched_source = snapshot.price_source
        slippage = settings.SIMULATION_SLIPPAGE_BPS / 10000

        side = str(order.side.value).lower()

        # 风控检查
        if snapshot.suspended:
            return ExecutionResult(success=False, message="Security is suspended, cannot trade")
        if base_price <= 0:
            return ExecutionResult(success=False, message="No valid market price, cannot trade")
        if order.quantity <= 0:
            return ExecutionResult(success=False, message="Quantity must be positive")

        # 整手校验：A股必须以100股为单位
        if not float(order.quantity).is_integer() or int(order.quantity) % 100 != 0:
            return ExecutionResult(
                success=False,
                message="A-share simulation quantity must be an integral board lot of 100",
            )

        if side == "buy" and snapshot.limit_up:
            return ExecutionResult(success=False, message="Limit-up locked, buy order cannot be filled")
        if side == "sell" and snapshot.limit_down:
            return ExecutionResult(success=False, message="Limit-down locked, sell order cannot be filled")

        if order.order_type == OrderType.MARKET:
            direction = 1 if side == "buy" else -1
            exec_price = round(base_price * (1 + direction * slippage), 4)
            price_source = fetched_source
        elif order.order_type == OrderType.LIMIT:
            if order.price is None or order.price <= 0:
                return ExecutionResult(success=False, message="Limit price required")
            limit_price = float(order.price)
            direction = 1 if side == "buy" else -1
            simulated_price = round(base_price * (1 + direction * slippage), 4)
            if side == "buy":
                if limit_price < base_price:
                    return ExecutionResult(success=False, message="Buy limit price is below market price")
                exec_price = min(simulated_price, round(limit_price, 4))
            else:
                if limit_price > base_price:
                    return ExecutionResult(success=False, message="Sell limit price is above market price")
                exec_price = max(simulated_price, round(limit_price, 4))
            price_source = "limit_price"
        else:
            return ExecutionResult(success=False, message=f"Unsupported order type: {order.order_type}")

        # 完整费用模型：佣金 + 印花税（卖出）+ 过户费
        commission = round(order.quantity * exec_price * settings.SIMULATION_COMMISSION_RATE, 2)
        stamp_duty = (
            round(order.quantity * exec_price * settings.SIMULATION_STAMP_DUTY_RATE, 2)
            if side == "sell"
            else 0.0
        )
        transfer_fee = round(order.quantity * exec_price * settings.SIMULATION_TRANSFER_FEE_RATE, 2)
        total_fee = commission + stamp_duty + transfer_fee

        gross = order.quantity * exec_price
        if order.side.value == "buy":
            delta_cash = -(gross + total_fee)
            delta_volume = order.quantity
        else:
            delta_cash = gross - total_fee
            delta_volume = -order.quantity

        trade_action = str(getattr(order, "trade_action", None) or "").strip() or None
        position_side = str(getattr(order, "position_side", None) or "long").strip() or "long"
        is_margin_trade = bool(getattr(order, "is_margin_trade", False))

        update = await self.manager.update_balance(
            user_id=order.user_id,
            symbol=order.symbol,
            delta_cash=delta_cash,
            delta_volume=delta_volume,
            price=exec_price,
            tenant_id=order.tenant_id,
            trade_action=trade_action,
            position_side=position_side,
            is_margin_trade=is_margin_trade,
        )
        if not update.get("success"):
            reason = update.get("reason", "BALANCE_UPDATE_FAILED")
            if reason == "INSUFFICIENT_CASH":
                return ExecutionResult(success=False, message="Insufficient cash for buy order")
            if reason == "INSUFFICIENT_HOLDINGS":
                return ExecutionResult(success=False, message="Insufficient holdings for sell order")
            return ExecutionResult(success=False, message=f"Balance update failed: {reason}")

        return ExecutionResult(
            success=True,
            price=exec_price,
            quantity=order.quantity,
            commission=commission,
            stamp_duty=stamp_duty,
            transfer_fee=transfer_fee,
            price_source=price_source,
        )

    async def apply_filled(self, order: SimOrder, result: ExecutionResult) -> SimTrade:
        trade_value = result.quantity * result.price
        trade = SimTrade(
            order_id=order.order_id,
            tenant_id=order.tenant_id,
            user_id=order.user_id,
            portfolio_id=order.portfolio_id,
            symbol=order.symbol,
            side=order.side,
            quantity=result.quantity,
            price=result.price,
            trade_value=trade_value,
            commission=result.commission,
            stamp_duty=result.stamp_duty,
            transfer_fee=result.transfer_fee,
            total_fee=result.commission + result.stamp_duty + result.transfer_fee,
            executed_at=datetime.now(),
            price_source=result.price_source,
            trade_action=getattr(order, "trade_action", None),
            position_side=getattr(order, "position_side", None) or "long",
            is_margin_trade=int(bool(getattr(order, "is_margin_trade", False))),
        )
        self.db.add(trade)

        order.status = OrderStatus.FILLED
        order.submitted_at = order.submitted_at or datetime.now()
        order.filled_at = datetime.now()
        order.filled_quantity = result.quantity
        order.average_price = result.price
        order.filled_value = trade_value
        order.commission = result.commission
        order.total_fee = result.commission + result.stamp_duty + result.transfer_fee
        order.order_value = order.quantity * (order.price or 0)
        order.execution_model = "synthetic_price"
        order.price_source = result.price_source

        await self.db.commit()
        await self.db.refresh(order)
        await self.db.refresh(trade)
        await self._sync_trade_account(order.tenant_id, order.user_id)
        return trade

    async def mark_rejected(self, order: SimOrder, message: str):
        order.status = OrderStatus.REJECTED
        order.submitted_at = order.submitted_at or datetime.now()
        order.remarks = f"Execution rejected: {message}"
        await self.db.commit()
        await self.db.refresh(order)

    async def _sync_trade_account(self, tenant_id: str, user_id: int):
        if not self.manager.redis.client:
            return
        account = await self.manager.get_account(user_id, tenant_id=tenant_id)
        if not account:
            return
        payload = dict(account)
        payload.setdefault("timestamp", datetime.now().isoformat())
        write_trade_account_cache(self.manager.redis, tenant_id, user_id, payload)
