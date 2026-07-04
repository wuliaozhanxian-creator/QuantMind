"""
悬挂订单超时扫描器

每隔 SCAN_INTERVAL_SECONDS 秒扫描一次，将超过 ORDER_TIMEOUT_MINUTES 分钟
仍停留在 SUBMITTED 状态的实盘订单标记为 EXPIRED，并推送用户通知。

环境变量：
  ORDER_TIMEOUT_MINUTES    超时分钟数，默认 30
  ORDER_SCAN_INTERVAL_SEC  扫描间隔秒数，默认 300 (5分钟)
"""

from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timedelta

from sqlalchemy import and_, select

from backend.services.trade.models.enums import OrderStatus
from backend.services.trade.models.order import Order, TradingMode
from backend.shared.database_manager_v2 import get_session
from backend.shared.notification_publisher import publish_notification_async

logger = logging.getLogger(__name__)

_TIMEOUT_MINUTES = int(os.getenv("ORDER_TIMEOUT_MINUTES", "30"))
_SCAN_INTERVAL = int(os.getenv("ORDER_SCAN_INTERVAL_SEC", "300"))
_BRIDGE_ACK_TIMEOUT_SECONDS = int(os.getenv("BRIDGE_ACK_TIMEOUT_SECONDS", "120"))
_BRIDGE_ACK_SCAN_INTERVAL = int(os.getenv("BRIDGE_ACK_SCAN_INTERVAL_SEC", "5"))
_AWAITING_BRIDGE_ACK_MARKER = "[AWAITING_BRIDGE_ACK]"
_BRIDGE_ACK_TIMEOUT_MARKER = "[BRIDGE_ACK_TIMEOUT_PENDING_REVIEW]"
# 对账队列标记：标记后该订单进入人工/自动对账流程，避免被 _scan_once 误判为 EXPIRED。
_RECONCILE_QUEUED_MARKER = "[RECONCILE_QUEUED]"
_RECONCILE_SCAN_INTERVAL = int(os.getenv("ORDER_RECONCILE_SCAN_INTERVAL_SEC", "60"))


async def _scan_once() -> int:
    """扫描一次，返回本次过期的订单数量。"""
    cutoff = datetime.now() - timedelta(minutes=_TIMEOUT_MINUTES)
    expired_count = 0

    async with get_session() as db:
        stmt = (
            select(Order)
            .where(
                and_(
                    Order.status == OrderStatus.SUBMITTED,
                    Order.trading_mode == TradingMode.REAL,
                    Order.submitted_at <= cutoff,
                    # 已进入对账队列的订单状态未知，不能直接判 EXPIRED，交由对账流程确认
                    ~Order.remarks.like(f"%{_RECONCILE_QUEUED_MARKER}%"),
                )
            )
            .limit(200)
        )
        result = await db.execute(stmt)
        orders = list(result.scalars().all())

        for order in orders:
            try:
                order.status = OrderStatus.EXPIRED
                order.remarks = (
                    order.remarks or ""
                ) + f" [EXPIRED: submitted_at={order.submitted_at}, timeout={_TIMEOUT_MINUTES}m]"
                expired_count += 1
                logger.warning(
                    "order %s expired after %d min (submitted_at=%s)",
                    order.order_id,
                    _TIMEOUT_MINUTES,
                    order.submitted_at,
                )
                # 推送通知（fire-and-forget）
                try:
                    await publish_notification_async(
                        user_id=str(order.user_id),
                        tenant_id=str(order.tenant_id or "default"),
                        title="订单超时过期",
                        content=(
                            f"{order.symbol} 订单 {str(order.order_id)[:8]}... "
                            f"已超过 {_TIMEOUT_MINUTES} 分钟未收到成交回报，已标记为过期。"
                        ),
                        type="trading",
                        level="warning",
                        action_url="/trading",
                    )
                except Exception as notify_exc:
                    logger.warning("notify failed for expired order %s: %s", order.order_id, notify_exc)
            except Exception as exc:
                logger.error("failed to expire order %s: %s", order.order_id, exc)

        if orders:
            await db.commit()

    return expired_count


async def _scan_bridge_ack_timeout_once() -> int:
    """
    扫描 bridge 派发后未收到 ACK/回报的订单，短超时后标记待人工核查。
    仅处理：
    - REAL + SUBMITTED
    - 备注含 [AWAITING_BRIDGE_ACK]
    - 尚未写入 [BRIDGE_ACK_TIMEOUT_PENDING_REVIEW]
    - 无 exchange_order_id
    - submitted_at 超过 BRIDGE_ACK_TIMEOUT_SECONDS
    """
    if _BRIDGE_ACK_TIMEOUT_SECONDS <= 0:
        return 0

    cutoff = datetime.now() - timedelta(seconds=_BRIDGE_ACK_TIMEOUT_SECONDS)
    flagged_count = 0

    async with get_session() as db:
        stmt = (
            select(Order)
            .where(
                and_(
                    Order.status == OrderStatus.SUBMITTED,
                    Order.trading_mode == TradingMode.REAL,
                    Order.submitted_at <= cutoff,
                    Order.exchange_order_id.is_(None),
                    Order.remarks.is_not(None),
                    Order.remarks.like(f"%{_AWAITING_BRIDGE_ACK_MARKER}%"),
                    ~Order.remarks.like(f"%{_BRIDGE_ACK_TIMEOUT_MARKER}%"),
                )
            )
            .limit(500)
        )
        result = await db.execute(stmt)
        orders = list(result.scalars().all())

        for order in orders:
            try:
                client_order_id = getattr(order, "client_order_id", None)
                symbol = getattr(order, "symbol", "")
                side = getattr(order, "side", "")
                suffix = (
                    f"{_BRIDGE_ACK_TIMEOUT_MARKER} "
                    f"[PENDING_REVIEW: bridge_ack_timeout={_BRIDGE_ACK_TIMEOUT_SECONDS}s, "
                    f"submitted_at={order.submitted_at}]"
                )
                order.remarks = f"{(order.remarks or '').strip()} {suffix}".strip()
                flagged_count += 1
                logger.warning(
                    "order %s flagged for bridge ack timeout review=%ss (submitted_at=%s client_order_id=%s symbol=%s side=%s)",
                    order.order_id,
                    _BRIDGE_ACK_TIMEOUT_SECONDS,
                    order.submitted_at,
                    str(client_order_id or ""),
                    str(symbol or ""),
                    str(side or ""),
                )
                try:
                    await publish_notification_async(
                        user_id=str(order.user_id),
                        tenant_id=str(order.tenant_id or "default"),
                        title="桥接回报超时待核查",
                        content=(
                            f"{symbol} 订单 {str(order.order_id)[:8]}... "
                            f"桥接 {_BRIDGE_ACK_TIMEOUT_SECONDS} 秒未回报，已标记待核查，暂未判定拒单。"
                        ),
                        type="trading",
                        level="warning",
                        action_url="/trading",
                    )
                except Exception as notify_exc:
                    logger.warning("notify failed for bridge timeout order %s: %s", order.order_id, notify_exc)
            except Exception as exc:
                logger.error("failed to flag bridge timeout order %s: %s", order.order_id, exc)

        if orders:
            await db.commit()

    return flagged_count


async def _scan_reconcile_candidates_once() -> int:
    """
    对账候选扫描：将已被标记 [BRIDGE_ACK_TIMEOUT_PENDING_REVIEW] 的订单
    加入对账队列（写入 [RECONCILE_QUEUED] 标记），并记录对账所需的线索字段。

    这些订单状态未知（可能已成交 / 已拒绝 / 仍挂单），不能直接判定 EXPIRED，
    必须由人工或自动对账任务根据 client_order_id / exchange_order_id 向 broker
    查询最终状态后再做处置。本函数只负责“入队”，不直接改订单状态，避免误判。

    被标记 [RECONCILE_QUEUED] 后：
      - _scan_once 会跳过该订单，不再直接判 EXPIRED
      - 后续对账任务可按该标记批量拉取并比对 broker 回报
    """
    flagged_count = 0

    async with get_session() as db:
        stmt = (
            select(Order)
            .where(
                and_(
                    Order.status == OrderStatus.SUBMITTED,
                    Order.trading_mode == TradingMode.REAL,
                    Order.remarks.is_not(None),
                    Order.remarks.like(f"%{_BRIDGE_ACK_TIMEOUT_MARKER}%"),
                    ~Order.remarks.like(f"%{_RECONCILE_QUEUED_MARKER}%"),
                )
            )
            .limit(500)
        )
        result = await db.execute(stmt)
        orders = list(result.scalars().all())

        for order in orders:
            try:
                client_order_id = getattr(order, "client_order_id", None)
                exchange_order_id = getattr(order, "exchange_order_id", None)
                symbol = getattr(order, "symbol", "")
                suffix = (
                    f"{_RECONCILE_QUEUED_MARKER} "
                    f"[RECONCILE_QUEUE: queued_at={datetime.now().isoformat()}, "
                    f"client_order_id={client_order_id}, "
                    f"exchange_order_id={exchange_order_id}, "
                    f"submitted_at={order.submitted_at}]"
                )
                order.remarks = f"{(order.remarks or '').strip()} {suffix}".strip()
                flagged_count += 1
                logger.warning(
                    "order %s queued for reconciliation (client_order_id=%s "
                    "exchange_order_id=%s symbol=%s submitted_at=%s)",
                    order.order_id,
                    str(client_order_id or ""),
                    str(exchange_order_id or ""),
                    str(symbol or ""),
                    order.submitted_at,
                )
                try:
                    await publish_notification_async(
                        user_id=str(order.user_id),
                        tenant_id=str(order.tenant_id or "default"),
                        title="订单进入对账队列",
                        content=(
                            f"{symbol} 订单 {str(order.order_id)[:8]}... "
                            "桥接回报超时且状态未知，已进入对账队列，请核查柜台最终状态。"
                        ),
                        type="trading",
                        level="warning",
                        action_url="/trading",
                    )
                except Exception as notify_exc:
                    logger.warning(
                        "notify failed for reconcile queue order %s: %s",
                        order.order_id,
                        notify_exc,
                    )
            except Exception as exc:
                logger.error(
                    "failed to queue order %s for reconciliation: %s",
                    order.order_id,
                    exc,
                )

        if orders:
            await db.commit()

    return flagged_count


async def run_order_timeout_scanner() -> None:
    """后台无限循环，定期扫描悬挂订单。"""
    logger.info(
        "Order timeout scanner started: timeout=%dm, interval=%ds, bridge_ack_timeout=%ss, bridge_scan_interval=%ss, reconcile_scan_interval=%ds",
        _TIMEOUT_MINUTES,
        _SCAN_INTERVAL,
        _BRIDGE_ACK_TIMEOUT_SECONDS,
        _BRIDGE_ACK_SCAN_INTERVAL,
        _RECONCILE_SCAN_INTERVAL,
    )
    next_long_scan = datetime.now()
    next_reconcile_scan = datetime.now()
    while True:
        await asyncio.sleep(max(1, _BRIDGE_ACK_SCAN_INTERVAL))
        try:
            bridge_count = await _scan_bridge_ack_timeout_once()
            if bridge_count:
                logger.info("Order timeout scanner: flagged %d bridge-timeout order(s) for review", bridge_count)

            now = datetime.now()
            # 对账候选扫描：将待核查订单入队，避免被 _scan_once 误判 EXPIRED
            if now >= next_reconcile_scan:
                reconcile_count = await _scan_reconcile_candidates_once()
                if reconcile_count:
                    logger.info(
                        "Order timeout scanner: queued %d order(s) for reconciliation",
                        reconcile_count,
                    )
                next_reconcile_scan = now + timedelta(seconds=max(1, _RECONCILE_SCAN_INTERVAL))

            if now >= next_long_scan:
                count = await _scan_once()
                if count:
                    logger.info("Order timeout scanner: expired %d order(s)", count)
                next_long_scan = now + timedelta(seconds=max(1, _SCAN_INTERVAL))
        except Exception as exc:
            logger.error("Order timeout scanner error: %s", exc)
