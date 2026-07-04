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
import re
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
# 对账闭环相关常量
_RECONCILE_FAILED_MARKER = "[RECONCILE_FAILED_MANUAL_REVIEW]"
_RECONCILE_QUERY_ATTEMPTS_MARKER = "[RECONCILE_QUERY_ATTEMPTS:"
_RECONCILE_MAX_QUERY_ATTEMPTS = int(os.getenv("RECONCILE_MAX_QUERY_ATTEMPTS", "5"))
_RECONCILE_QUERY_ATTEMPTS_RE = re.compile(r"\[RECONCILE_QUERY_ATTEMPTS:\d+\]")

# ==================== 订单状态查询轮询（T4.3）====================
# 对已提交但未成交的订单，定期主动查询 broker 获取柜台回报。
_QMT_ORDER_POLL_INTERVAL_SEC = int(os.getenv("QMT_ORDER_POLL_INTERVAL_SEC", "30"))
_QMT_ORDER_POLL_SCAN_INTERVAL_SEC = int(
    os.getenv("QMT_ORDER_POLL_SCAN_INTERVAL_SEC", "30")
)
_QMT_ORDER_POLL_MIN_AGE_SEC = int(os.getenv("QMT_ORDER_POLL_MIN_AGE_SEC", "30"))
_QMT_ORDER_POLL_LAST_QUERY_MARKER = "[POLL_LAST_QUERY:"
_QMT_ORDER_POLL_LAST_QUERY_RE = re.compile(
    r"\[POLL_LAST_QUERY:\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?\]"
)
_QMT_ORDER_POLL_MAX_PER_SCAN = int(os.getenv("QMT_ORDER_POLL_MAX_PER_SCAN", "50"))


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


# ==================== 对账闭环（T4.1-followup）====================


def _parse_query_attempts(remarks: str | None) -> int:
    """从 remarks 解析当前对账查询次数。"""
    if not remarks:
        return 0
    match = _RECONCILE_QUERY_ATTEMPTS_RE.search(remarks)
    if not match:
        return 0
    try:
        inner = match.group(0)
        return int(inner[len(_RECONCILE_QUERY_ATTEMPTS_MARKER) : -1])
    except (ValueError, IndexError):
        return 0


def _write_query_attempts(remarks: str, attempts: int) -> str:
    """写入或更新对账查询次数标记。"""
    marker = f"{_RECONCILE_QUERY_ATTEMPTS_MARKER}{attempts}]"
    base = remarks or ""
    if _RECONCILE_QUERY_ATTEMPTS_RE.search(base):
        return _RECONCILE_QUERY_ATTEMPTS_RE.sub(marker, base)
    return f"{base.strip()} {marker}".strip()


def _get_reconcile_broker():
    """获取对账用 broker（REAL 模式）。

    生产环境通过 create_broker 创建；测试中可 monkeypatch 此函数
    返回 MockBroker 或 None 以控制对账行为。
    """
    try:
        from backend.services.trade.services.broker_client import create_broker
        from backend.services.trade.trade_config import settings

        return create_broker(
            enable_real=True,
            broker_type=getattr(settings, "REAL_BROKER_TYPE", "bridge"),
            stream_base_url=getattr(
                settings, "MARKET_DATA_SERVICE_URL", "http://stream-gateway:8003"
            ),
        )
    except Exception as exc:
        logger.error("Failed to create reconcile broker: %s", exc)
        return None


async def _notify_reconcile(
    order: Order, title: str, content: str, level: str = "warning"
) -> None:
    """发送对账结果通知（fire-and-forget）。"""
    try:
        await publish_notification_async(
            user_id=str(order.user_id),
            tenant_id=str(order.tenant_id or "default"),
            title=title,
            content=content,
            type="trading",
            level=level,
            action_url="/trading",
        )
    except Exception as exc:
        logger.warning(
            "notify failed for reconcile order %s: %s", order.order_id, exc
        )


async def _mark_reconcile_failed(order: Order) -> None:
    """标记对账失败，需人工介入。"""
    suffix = (
        f"{_RECONCILE_FAILED_MARKER} "
        f"[MANUAL_REVIEW: max_query_attempts={_RECONCILE_MAX_QUERY_ATTEMPTS} exceeded]"
    )
    order.remarks = f"{(order.remarks or '').strip()} {suffix}".strip()
    logger.warning(
        "order %s reconcile failed after %d attempts, manual review required",
        order.order_id,
        _RECONCILE_MAX_QUERY_ATTEMPTS,
    )
    await _notify_reconcile(
        order,
        "对账失败待人工介入",
        (
            f"{order.symbol} 订单 {str(order.order_id)[:8]}... "
            f"对账查询连续 {_RECONCILE_MAX_QUERY_ATTEMPTS} 次未获得终态，"
            "需人工核查柜台最终状态。"
        ),
        level="error",
    )


async def _reconcile_single_order(order: Order, broker) -> None:
    """处理单个对账订单：查询 broker 并更新本地状态。

    - FILLED/PARTIALLY_FILLED/CANCELLED/REJECTED → 更新本地状态，闭环完成
    - STILL_PENDING → 保持 RECONCILE_QUEUED，累计查询次数
    - 查询异常 → 保持 RECONCILE_QUEUED，累计查询失败次数
    - 超过 _RECONCILE_MAX_QUERY_ATTEMPTS → 标记 RECONCILE_FAILED_MANUAL_REVIEW
    """
    client_order_id = str(getattr(order, "client_order_id", "") or "").strip()
    remarks = order.remarks or ""
    attempts = _parse_query_attempts(remarks)

    # broker 不可用视为查询失败
    if broker is None:
        attempts += 1
        remarks = _write_query_attempts(remarks, attempts)
        remarks = (
            f"{remarks} [RECONCILE_QUERY_FAIL: "
            f"broker unavailable, attempts={attempts}]"
        ).strip()
        order.remarks = remarks
        logger.warning(
            "order %s reconcile broker unavailable, attempts=%d",
            order.order_id,
            attempts,
        )
        if attempts >= _RECONCILE_MAX_QUERY_ATTEMPTS:
            await _mark_reconcile_failed(order)
        return

    # 调用 broker.query_order 查询实际状态
    try:
        result = await broker.query_order(client_order_id)
    except Exception as exc:
        attempts += 1
        remarks = _write_query_attempts(remarks, attempts)
        remarks = (
            f"{remarks} [RECONCILE_QUERY_FAIL: {exc}, attempts={attempts}]"
        ).strip()
        order.remarks = remarks
        logger.warning(
            "order %s reconcile query failed: %s, attempts=%d",
            order.order_id,
            exc,
            attempts,
        )
        if attempts >= _RECONCILE_MAX_QUERY_ATTEMPTS:
            await _mark_reconcile_failed(order)
        return

    status = str(getattr(result, "status", "") or "").upper()

    if status == "FILLED":
        order.status = OrderStatus.FILLED
        order.filled_quantity = float(
            result.filled_quantity or order.filled_quantity or 0
        )
        order.average_price = float(result.avg_price or order.average_price or 0)
        order.filled_value = order.filled_quantity * order.average_price
        order.filled_at = datetime.now()
        if result.exchange_order_id:
            order.exchange_order_id = str(result.exchange_order_id)
        order.remarks = (
            f"{(order.remarks or '').strip()} "
            f"[RECONCILE_FILLED: qty={order.filled_quantity}, "
            f"avg_price={order.average_price}]"
        ).strip()
        logger.info("order %s reconciled as FILLED", order.order_id)
        await _notify_reconcile(
            order,
            "对账确认成交",
            (
                f"{order.symbol} 对账确认已成交 {order.filled_quantity} 股，"
                f"均价 {order.average_price}"
            ),
            level="success",
        )

    elif status == "PARTIALLY_FILLED":
        order.status = OrderStatus.PARTIALLY_FILLED
        order.filled_quantity = float(
            result.filled_quantity or order.filled_quantity or 0
        )
        order.average_price = float(result.avg_price or order.average_price or 0)
        order.filled_value = order.filled_quantity * order.average_price
        if result.exchange_order_id:
            order.exchange_order_id = str(result.exchange_order_id)
        order.remarks = (
            f"{(order.remarks or '').strip()} "
            f"[RECONCILE_PARTIAL: qty={order.filled_quantity}, "
            f"avg_price={order.average_price}]"
        ).strip()
        logger.info("order %s reconciled as PARTIALLY_FILLED", order.order_id)
        await _notify_reconcile(
            order,
            "对账确认部分成交",
            (
                f"{order.symbol} 对账确认部分成交 {order.filled_quantity} 股，"
                f"均价 {order.average_price}"
            ),
            level="info",
        )

    elif status == "CANCELLED":
        order.status = OrderStatus.CANCELLED
        order.cancelled_at = datetime.now()
        order.remarks = (
            f"{(order.remarks or '').strip()} [RECONCILE_CANCELLED]"
        ).strip()
        logger.info("order %s reconciled as CANCELLED", order.order_id)
        await _notify_reconcile(
            order,
            "对账确认撤单",
            f"{order.symbol} 对账确认已撤单",
            level="info",
        )

    elif status == "REJECTED":
        order.status = OrderStatus.REJECTED
        order.remarks = (
            f"{(order.remarks or '').strip()} [RECONCILE_REJECTED]"
        ).strip()
        logger.info("order %s reconciled as REJECTED", order.order_id)
        await _notify_reconcile(
            order,
            "对账确认拒单",
            f"{order.symbol} 对账确认已被柜台拒绝",
            level="warning",
        )

    else:
        # STILL_PENDING 或未知状态：保持 RECONCILE_QUEUED，累计查询次数
        attempts += 1
        remarks = _write_query_attempts(order.remarks or "", attempts)
        order.remarks = (
            f"{remarks} [RECONCILE_STILL_PENDING: attempts={attempts}]"
        ).strip()
        logger.info(
            "order %s still pending after reconcile query, attempts=%d",
            order.order_id,
            attempts,
        )
        if attempts >= _RECONCILE_MAX_QUERY_ATTEMPTS:
            await _mark_reconcile_failed(order)


async def _reconcile_queued_orders_once() -> int:
    """对账闭环：扫描 [RECONCILE_QUEUED] 订单，调用 broker.query_order 查询最终状态。

    流程：
      1. 查询所有标记 [RECONCILE_QUEUED] 且未标记 [RECONCILE_FAILED_MANUAL_REVIEW] 的订单
      2. 获取对账用 broker（REAL 模式）
      3. 逐单调用 broker.query_order(client_order_id)
      4. 根据回报更新本地订单状态（FILLED/CANCELLED/REJECTED/STILL_PENDING）
      5. 查询失败或持续 STILL_PENDING 累计次数，超过阈值标记人工核查

    返回本次处理的订单数量。
    """
    processed_count = 0

    async with get_session() as db:
        stmt = (
            select(Order)
            .where(
                and_(
                    Order.status == OrderStatus.SUBMITTED,
                    Order.trading_mode == TradingMode.REAL,
                    Order.remarks.is_not(None),
                    Order.remarks.like(f"%{_RECONCILE_QUEUED_MARKER}%"),
                    ~Order.remarks.like(f"%{_RECONCILE_FAILED_MARKER}%"),
                )
            )
            .limit(100)
        )
        result = await db.execute(stmt)
        orders = list(result.scalars().all())

        if not orders:
            return 0

        broker = _get_reconcile_broker()
        for order in orders:
            try:
                await _reconcile_single_order(order, broker)
                processed_count += 1
            except Exception as exc:
                logger.error(
                    "reconcile order %s failed: %s", order.order_id, exc
                )

        await db.commit()

    return processed_count


async def _scan_pending_orders_for_query_once() -> int:
    """订单状态查询轮询（T4.3）。

    对已提交但未成交的 REAL 订单，主动调用 broker.query_order 查询柜台回报，
    避免单纯依赖 ExecutionStreamConsumer 被动接收回报（回报丢失/延迟时主动补齐）。

    筛选条件：
      - REAL + SUBMITTED
      - submitted_at 距今 >= _QMT_ORDER_POLL_MIN_AGE_SEC 秒
      - 有 client_order_id
      - 未进入 RECONCILE_QUEUED（已在对账流程中，不重复查询）
      - 距上次查询 >= _QMT_ORDER_POLL_INTERVAL_SEC 秒（通过 remarks 标记节流）

    处理逻辑：
      - FILLED/PARTIALLY_FILLED/CANCELLED/REJECTED → 更新本地状态（与对账闭环一致）
      - STILL_PENDING → 记录查询时间，等待下次轮询
      - 查询异常 → 记录日志，不崩溃

    返回本次处理的订单数量。
    """
    if _QMT_ORDER_POLL_MIN_AGE_SEC <= 0:
        return 0

    min_age_cutoff = datetime.now() - timedelta(seconds=_QMT_ORDER_POLL_MIN_AGE_SEC)
    poll_recheck_cutoff = datetime.now() - timedelta(seconds=_QMT_ORDER_POLL_INTERVAL_SEC)
    processed_count = 0

    async with get_session() as db:
        stmt = (
            select(Order)
            .where(
                and_(
                    Order.status == OrderStatus.SUBMITTED,
                    Order.trading_mode == TradingMode.REAL,
                    Order.submitted_at <= min_age_cutoff,
                    Order.client_order_id.is_not(None),
                    ~Order.remarks.like(f"%{_RECONCILE_QUEUED_MARKER}%"),
                    ~Order.remarks.like(f"%{_BRIDGE_ACK_TIMEOUT_MARKER}%"),
                )
            )
            .limit(_QMT_ORDER_POLL_MAX_PER_SCAN)
        )
        result = await db.execute(stmt)
        orders = list(result.scalars().all())

        if not orders:
            return 0

        broker = _get_reconcile_broker()
        for order in orders:
            # 节流：检查距上次查询是否足够久
            last_query = _parse_poll_last_query(order.remarks)
            if last_query is not None and last_query > poll_recheck_cutoff:
                continue  # 距上次查询未满间隔，跳过

            client_order_id = str(getattr(order, "client_order_id", "") or "").strip()
            if not client_order_id:
                continue

            try:
                await _poll_single_order(order, broker, client_order_id)
                processed_count += 1
            except Exception as exc:
                logger.error(
                    "poll order %s failed: %s", order.order_id, exc
                )

        if orders:
            await db.commit()

    return processed_count


def _parse_poll_last_query(remarks: str | None) -> datetime | None:
    """从 remarks 解析上次轮询查询时间。"""
    if not remarks:
        return None
    match = _QMT_ORDER_POLL_LAST_QUERY_RE.search(remarks)
    if not match:
        return None
    raw = match.group(0)[len(_QMT_ORDER_POLL_LAST_QUERY_MARKER):-1]
    try:
        return datetime.fromisoformat(raw)
    except (ValueError, TypeError):
        return None


def _write_poll_last_query(remarks: str, when: datetime) -> str:
    """写入或更新上次轮询查询时间标记。"""
    marker = f"{_QMT_ORDER_POLL_LAST_QUERY_MARKER}{when.isoformat()}]"
    base = remarks or ""
    if _QMT_ORDER_POLL_LAST_QUERY_RE.search(base):
        return _QMT_ORDER_POLL_LAST_QUERY_RE.sub(marker, base)
    return f"{base.strip()} {marker}".strip()


async def _poll_single_order(order: Order, broker, client_order_id: str) -> None:
    """处理单个轮询订单：查询 broker 并更新本地状态。

    逻辑与 _reconcile_single_order 一致，但额外写入 POLL_LAST_QUERY 时间标记
    用于节流，且不累计 RECONCILE_QUERY_ATTEMPTS（轮询是主动补齐，非对账补救）。
    """
    now = datetime.now()
    order.remarks = _write_poll_last_query(order.remarks or "", now)

    if broker is None:
        logger.warning(
            "poll order %s: broker unavailable, skip (will retry next scan)",
            order.order_id,
        )
        return

    try:
        result = await broker.query_order(client_order_id)
    except Exception as exc:
        logger.warning(
            "poll order %s query failed: %s", order.order_id, exc
        )
        return

    status = str(getattr(result, "status", "") or "").upper()

    if status == "FILLED":
        order.status = OrderStatus.FILLED
        order.filled_quantity = float(
            result.filled_quantity or order.filled_quantity or 0
        )
        order.average_price = float(result.avg_price or order.average_price or 0)
        order.filled_value = order.filled_quantity * order.average_price
        order.filled_at = datetime.now()
        if result.exchange_order_id:
            order.exchange_order_id = str(result.exchange_order_id)
        order.remarks = (
            f"{(order.remarks or '').strip()} "
            f"[POLL_FILLED: qty={order.filled_quantity}, "
            f"avg_price={order.average_price}]"
        ).strip()
        logger.info("order %s poll confirmed FILLED", order.order_id)
        await _notify_reconcile(
            order,
            "轮询确认成交",
            (
                f"{order.symbol} 轮询确认已成交 {order.filled_quantity} 股，"
                f"均价 {order.average_price}"
            ),
            level="success",
        )

    elif status == "PARTIALLY_FILLED":
        order.status = OrderStatus.PARTIALLY_FILLED
        order.filled_quantity = float(
            result.filled_quantity or order.filled_quantity or 0
        )
        order.average_price = float(result.avg_price or order.average_price or 0)
        order.filled_value = order.filled_quantity * order.average_price
        if result.exchange_order_id:
            order.exchange_order_id = str(result.exchange_order_id)
        order.remarks = (
            f"{(order.remarks or '').strip()} "
            f"[POLL_PARTIAL: qty={order.filled_quantity}, "
            f"avg_price={order.average_price}]"
        ).strip()
        logger.info("order %s poll confirmed PARTIALLY_FILLED", order.order_id)

    elif status == "CANCELLED":
        order.status = OrderStatus.CANCELLED
        order.cancelled_at = datetime.now()
        order.remarks = (
            f"{(order.remarks or '').strip()} [POLL_CANCELLED]"
        ).strip()
        logger.info("order %s poll confirmed CANCELLED", order.order_id)
        await _notify_reconcile(
            order,
            "轮询确认撤单",
            f"{order.symbol} 轮询确认已撤单",
            level="info",
        )

    elif status == "REJECTED":
        order.status = OrderStatus.REJECTED
        order.remarks = (
            f"{(order.remarks or '').strip()} [POLL_REJECTED]"
        ).strip()
        logger.info("order %s poll confirmed REJECTED", order.order_id)
        await _notify_reconcile(
            order,
            "轮询确认拒单",
            f"{order.symbol} 轮询确认已被柜台拒绝",
            level="warning",
        )

    else:
        # STILL_PENDING 或未知状态：仅记录查询时间，等待下次轮询
        logger.debug(
            "order %s poll still pending (status=%s)", order.order_id, status
        )


async def run_order_timeout_scanner() -> None:
    """后台无限循环，定期扫描悬挂订单。"""
    logger.info(
        "Order timeout scanner started: timeout=%dm, interval=%ds, bridge_ack_timeout=%ss, bridge_scan_interval=%ss, reconcile_scan_interval=%ds, reconcile_max_attempts=%d, poll_scan_interval=%ds, poll_min_age=%ds",
        _TIMEOUT_MINUTES,
        _SCAN_INTERVAL,
        _BRIDGE_ACK_TIMEOUT_SECONDS,
        _BRIDGE_ACK_SCAN_INTERVAL,
        _RECONCILE_SCAN_INTERVAL,
        _RECONCILE_MAX_QUERY_ATTEMPTS,
        _QMT_ORDER_POLL_SCAN_INTERVAL_SEC,
        _QMT_ORDER_POLL_MIN_AGE_SEC,
    )
    next_long_scan = datetime.now()
    next_reconcile_scan = datetime.now()
    next_poll_scan = datetime.now()
    while True:
        await asyncio.sleep(max(1, _BRIDGE_ACK_SCAN_INTERVAL))
        try:
            bridge_count = await _scan_bridge_ack_timeout_once()
            if bridge_count:
                logger.info("Order timeout scanner: flagged %d bridge-timeout order(s) for review", bridge_count)

            now = datetime.now()
            # 对账候选扫描：将待核查订单入队 + 对账闭环查询
            if now >= next_reconcile_scan:
                # 阶段1：将 [BRIDGE_ACK_TIMEOUT_PENDING_REVIEW] 订单入队
                reconcile_count = await _scan_reconcile_candidates_once()
                if reconcile_count:
                    logger.info(
                        "Order timeout scanner: queued %d order(s) for reconciliation",
                        reconcile_count,
                    )
                # 阶段2：对账闭环 — 查询已入队订单的 broker 终态并更新本地状态
                closed_count = await _reconcile_queued_orders_once()
                if closed_count:
                    logger.info(
                        "Order timeout scanner: reconciled %d order(s) via broker query",
                        closed_count,
                    )
                next_reconcile_scan = now + timedelta(seconds=max(1, _RECONCILE_SCAN_INTERVAL))

            # T4.3: 订单状态查询轮询 — 主动查询已提交未成交订单的柜台回报
            if now >= next_poll_scan:
                poll_count = await _scan_pending_orders_for_query_once()
                if poll_count:
                    logger.info(
                        "Order timeout scanner: polled %d pending order(s) via broker query",
                        poll_count,
                    )
                next_poll_scan = now + timedelta(
                    seconds=max(1, _QMT_ORDER_POLL_SCAN_INTERVAL_SEC)
                )

            if now >= next_long_scan:
                count = await _scan_once()
                if count:
                    logger.info("Order timeout scanner: expired %d order(s)", count)
                next_long_scan = now + timedelta(seconds=max(1, _SCAN_INTERVAL))
        except Exception as exc:
            logger.error("Order timeout scanner error: %s", exc)
