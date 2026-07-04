#!/usr/bin/env python3
"""
实时行情数据推送器
Updated: 2026-07-04 - K线推送接入真实数据源（KLineService: stock_daily_latest / klines）
Updated: 2026-02-19 - 接入远程 Redis 行情快照数据源
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Set
from collections.abc import Iterable

from backend.services.stream.market_app.database import AsyncSessionLocal
from backend.services.stream.market_app.market_config import settings
from backend.services.stream.market_app.models import Quote
from backend.services.stream.market_app.services.remote_redis_source import (
    RemoteRedisDataSource,
)

from .manager import manager

logger = logging.getLogger(__name__)

# 全局数据源实例（延迟初始化）
_remote_redis_source: RemoteRedisDataSource | None = None


def get_remote_redis_source() -> RemoteRedisDataSource:
    global _remote_redis_source
    if _remote_redis_source is None:
        _remote_redis_source = RemoteRedisDataSource()
    return _remote_redis_source


# K线缓存用的 Redis 客户端（懒加载、复用，避免每次请求新建连接）
_kline_redis: Any = None


async def _get_kline_redis():
    """获取（懒加载、复用）K线缓存 Redis 客户端；不可用时返回 None（跳过缓存）"""
    global _kline_redis
    if _kline_redis is None:
        try:
            from backend.services.stream.market_app.database import get_redis

            client = await get_redis()
            if client is not None:
                _kline_redis = client
        except Exception as e:
            logger.warning(f"K线缓存 Redis 不可用，将跳过缓存: {e}")
    return _kline_redis


def _as_utc_aware(dt: datetime | None) -> datetime:
    if dt is None:
        return datetime.now(timezone.utc)
    if dt.tzinfo is None:
        return dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


class QuotePusher:
    """实时行情推送器

    负责推送实时股票行情数据到订阅的客户端
    """

    def __init__(self):
        """初始化推送器"""
        self.running = False
        self.subscribed_stocks: set[str] = set()  # 存储所有正在被订阅的代码
        self.push_task: asyncio.Task | None = None  # 中心化推送任务
        self.push_interval = 2.0  # 全局拉取间隔（秒）
        self.cache: dict[str, dict[str, Any]] = {}  # 行情缓存
        self.persist_to_db = True
        self.write_series = False  # 远程 Redis 只读，不写入时序数据
        self.warmup_symbols: set[str] = {
            s.strip() for s in (settings.STREAM_WARMUP_SYMBOLS or "").split(",") if s.strip()
        }
        logger.info("实时行情推送器初始化")

    async def start(self):
        """启动推送器"""
        if self.running:
            return
        self.running = True
        self.push_task = asyncio.create_task(self._centralized_push_loop())
        logger.info("实时行情推送器启动")

    async def stop(self):
        """停止推送器"""
        self.running = False
        if self.push_task:
            self.push_task.cancel()
            try:
                await self.push_task
            except asyncio.CancelledError:
                pass
        self.push_task = None
        logger.info("实时行情推送器停止")

    async def subscribe_quote(self, stock_code: str):
        """订阅股票行情"""
        self.subscribed_stocks.add(stock_code)
        logger.info(f"订阅列表增加: {stock_code}, 当前共 {len(self.subscribed_stocks)} 只")

    async def unsubscribe_quote(self, stock_code: str):
        """取消订阅股票行情"""
        if stock_code in self.subscribed_stocks:
            self.subscribed_stocks.remove(stock_code)
            logger.info(f"订阅列表移除: {stock_code}")

    async def reconcile_subscriptions(self, topics: Iterable[str]):
        """根据连接管理器中的主题重算股票订阅集合。"""
        stock_topics = {
            topic.split("stock.", 1)[1] for topic in topics if isinstance(topic, str) and topic.startswith("stock.")
        }
        if stock_topics != self.subscribed_stocks:
            self.subscribed_stocks = stock_topics
            logger.info("订阅列表已重算: %d 只股票", len(self.subscribed_stocks))

    async def _centralized_push_loop(self):
        """
        中心化行情推送循环
        一次性抓取所有被订阅的代码，降低 Redis IO 压力
        """
        source = get_remote_redis_source()

        while self.running:
            try:
                # 无订阅时仍拉取一小组保活标的，维持 quote->series->落库闭环
                if not self.subscribed_stocks and not self.warmup_symbols:
                    await asyncio.sleep(1.0)
                    continue

                # 1. 批量抓取行情
                stock_list = list(self.subscribed_stocks) if self.subscribed_stocks else list(self.warmup_symbols)
                results = await source.fetch_quotes(stock_list)

                if self.write_series and results:
                    await self._append_series_points(source, results)
                if self.persist_to_db and results:
                    await self._persist_quotes(results)

                # 2. 分发数据
                for quote in results:
                    stock_code = quote["symbol"]
                    topic = f"stock.{stock_code}"

                    # 转化为推送协议格式
                    push_data = {
                        "stock_code": stock_code,
                        "price": quote["current_price"],
                        "open": quote.get("open_price"),
                        "high": quote.get("high_price"),
                        "low": quote.get("low_price"),
                        "volume": quote.get("volume"),
                        "amount": quote.get("amount"),
                        "is_stale": quote.get("is_stale", False),
                        "timestamp": (
                            quote["timestamp"].isoformat()
                            if isinstance(quote["timestamp"], datetime)
                            else quote["timestamp"]
                        ),
                    }

                    # 3. 检查是否有变化并推送
                    if self._has_quote_changed(stock_code, push_data):
                        message = {
                            "type": "quote",
                            "stock_code": stock_code,
                            "data": push_data,
                            "timestamp": time.time(),
                        }

                        count = await manager.publish(topic, message)
                        if count > 0:
                            logger.debug(f"推送行情 {stock_code} 到 {count} 个客户端")

                        self.cache[stock_code] = push_data

                # 等待下次全量拉取
                await asyncio.sleep(self.push_interval)

            except asyncio.CancelledError:
                break
            except Exception as e:
                logger.error(f"中心化推送循环错误: {e}")
                await asyncio.sleep(2.0)

    async def _append_series_points(self, source: RemoteRedisDataSource, quotes: list[dict[str, Any]]) -> None:
        """将 WS 推送使用的同一批行情写入 Redis 时序集合。"""
        try:
            for quote in quotes:
                symbol = quote.get("symbol")
                if not symbol:
                    continue
                await source.append_series_point(symbol=symbol, quote=quote)
        except Exception as e:
            logger.error(f"写入行情时序失败: {e}")

    async def _persist_quotes(self, quotes: list[dict[str, Any]]) -> None:
        """将 WS 推送使用的同一批行情落库到 quotes 表。"""
        rows: list[Quote] = []
        for quote in quotes:
            symbol = quote.get("symbol")
            current_price = quote.get("current_price")
            if not symbol or current_price is None:
                continue
            rows.append(
                Quote(
                    symbol=str(symbol),
                    timestamp=(
                        _as_utc_aware(quote.get("timestamp"))
                        if isinstance(quote.get("timestamp"), datetime)
                        else datetime.now(timezone.utc)
                    ),
                    open_price=quote.get("open_price"),
                    high_price=quote.get("high_price"),
                    low_price=quote.get("low_price"),
                    close_price=quote.get("close_price"),
                    current_price=current_price,
                    volume=int(quote.get("volume") or 0),
                    amount=quote.get("amount"),
                    data_source=quote.get("data_source", "remote_redis"),
                )
            )

        if not rows:
            return

        try:
            async with AsyncSessionLocal() as session:
                session.add_all(rows)
                await session.commit()
        except Exception as e:
            logger.error(f"行情落库失败: {e}")

    def _has_quote_changed(self, stock_code: str, new_data: dict[str, Any]) -> bool:
        """
        检查行情是否有变化

        Args:
            stock_code: 股票代码
            new_data: 新行情数据

        Returns:
            是否有变化
        """
        if stock_code not in self.cache:
            return True

        old_data = self.cache[stock_code]

        # 比较价格是否变化
        return old_data.get("price") != new_data.get("price")

    async def push_kline(self, stock_code: str, period: str = "1min"):
        """
        推送K线数据

        Args:
            stock_code: 股票代码
            period: K线周期
        """
        topic = f"kline.{stock_code}.{period}"

        # 从真实数据源获取最新K线（stock_daily_latest / klines 表）
        kline_data = await self._fetch_kline(stock_code, period)

        if kline_data:
            message = {
                "type": "kline",
                "stock_code": stock_code,
                "period": period,
                "data": kline_data,
                "timestamp": time.time(),
            }

            await manager.publish(topic, message)

    async def _fetch_kline(self, stock_code: str, period: str) -> dict[str, Any] | None:
        """
        获取最新K线数据（接入真实数据源）

        通过 KLineService 读取 stock_daily_latest（日线）/ klines（分钟线）表，
        不再返回硬编码假数据。Redis 缓存键含 start_time/end_time，避免区间碰撞。

        Args:
            stock_code: 股票代码（任意格式，内部标准化为 SH/SZ/BJ 前缀）
            period: K线周期（1min / 5min / 1d 等）

        Returns:
            最新一根K线 dict（open/high/low/close/volume/timestamp），无数据时返回 None
        """
        from backend.services.stream.market_app.services.kline_service import KLineService
        from backend.shared.stock_utils import StockCodeUtil

        # 1. 标准化股票代码为 SH/SZ/BJ 前缀格式（项目强制规范）
        symbol = StockCodeUtil.to_prefix(stock_code)
        if not symbol:
            logger.warning("K线查询股票代码为空: %s", stock_code)
            return None

        # 2. 映射 period → 数据库 interval
        interval = self._period_to_interval(period)

        # 3. 计算查询窗口（同时作为缓存键组成部分，避免区间碰撞）
        end_time = datetime.now(timezone.utc)
        lookback = timedelta(days=30) if interval == "1d" else timedelta(days=1)
        start_time = end_time - lookback

        try:
            redis = await _get_kline_redis()
            async with AsyncSessionLocal() as db:
                service = KLineService(db, redis)
                klines = await service.get_klines(
                    symbol=symbol,
                    interval=interval,
                    start_time=start_time,
                    end_time=end_time,
                    limit=1,
                    use_cache=True,
                )
        except Exception as e:
            logger.error("获取K线数据失败 %s %s: %s", symbol, interval, e, exc_info=True)
            return None

        if not klines:
            logger.debug("K线数据为空 %s %s", symbol, interval)
            return None

        # KLineService 按 timestamp DESC 返回，第一条即最新
        k = klines[0]
        ts = k.timestamp
        return {
            "open": float(k.open_price or 0),
            "high": float(k.high_price or 0),
            "low": float(k.low_price or 0),
            "close": float(k.close_price or 0),
            "volume": int(k.volume or 0),
            "timestamp": ts.isoformat() if ts else datetime.now(timezone.utc).isoformat(),
        }

    @staticmethod
    def _period_to_interval(period: str) -> str:
        """将前端 period 映射为数据库 interval"""
        if not period:
            return "1d"
        p = period.strip().lower()
        minute_map = {
            "1m": "1min", "1min": "1min",
            "5m": "5min", "5min": "5min",
            "15m": "15min", "15min": "15min",
            "30m": "30min", "30min": "30min",
            "60m": "60min", "60min": "60min", "1h": "60min",
        }
        if p in minute_map:
            return minute_map[p]
        if p in ("1d", "day", "daily", "d"):
            return "1d"
        if p in ("1w", "week", "weekly"):
            return "1w"
        return "1d"

    def get_stats(self) -> dict[str, Any]:
        """
        获取推送统计

        Returns:
            统计信息
        """
        return {
            "running": self.running,
            "active_pushers": 1 if self.push_task and not self.push_task.done() else 0,
            "subscribed_stocks": len(self.subscribed_stocks),
            "cached_stocks": len(self.cache),
            "push_interval": self.push_interval,
        }


# 全局推送器实例
quote_pusher = QuotePusher()
