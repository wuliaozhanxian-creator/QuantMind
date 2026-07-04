"""
统一市场数据服务
提供股票数据获取、处理和管理功能
数据来源:PostgreSQL(klines / stock_daily_latest / stocks 表,真实历史数据)
"""

import asyncio
import logging
import os
import re
import threading
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any, Dict, List, Optional

import psycopg2
import psycopg2.pool
from psycopg2.extras import RealDictCursor

from ..stock_utils import StockCodeUtil
from ..unified_config import get_config

logger = logging.getLogger(__name__)

# PostgreSQL 连接配置(从环境变量读取,不硬编码密码)
_PG_HOST = os.getenv("DB_HOST", "localhost")
_PG_PORT = os.getenv("DB_PORT", "5432")
_PG_NAME = os.getenv("DB_NAME", "quantmind")
_PG_USER = os.getenv("DB_USER", "quantmind")
_PG_PASS = os.getenv("DB_PASSWORD", "")
if not _PG_PASS:
    raise RuntimeError("DB_PASSWORD 环境变量未设置")
PG_DSN = f"host={_PG_HOST} port={_PG_PORT} dbname={_PG_NAME} user={_PG_USER} password={_PG_PASS}"

# 模块级连接池(懒加载,线程安全)
_pool = None
_pool_lock = threading.Lock()


def _get_pool():
    """获取或创建连接池(双重检查锁)"""
    global _pool
    if _pool is None:
        with _pool_lock:
            if _pool is None:
                _pool = psycopg2.pool.SimpleConnectionPool(5, 20, PG_DSN)
    return _pool


def _pg_query(sql: str, params: tuple | None = None) -> list[dict]:
    """同步执行 PG 查询,返回 dict 列表(供 async 方法用 to_thread 包装)"""
    pool = _get_pool()
    conn = pool.getconn()
    try:
        with conn.cursor(cursor_factory=RealDictCursor) as cur:
            cur.execute(sql, params or ())
            return [dict(r) for r in cur.fetchall()]
    finally:
        pool.putconn(conn)


def _code6(symbol: str) -> str:
    """提取6位纯数字代码: 600519 / 600519.SH / SH600519 -> 600519"""
    s = symbol.strip().upper()
    for prefix in ("SH", "SZ", "BJ"):
        if s.startswith(prefix):
            return s[len(prefix):]
    return s.split(".")[0]


def _exchange_of(code_or_symbol: str) -> str:
    """根据股票代码判断交易所(使用 StockCodeUtil 标准化)"""
    p = StockCodeUtil.to_prefix(code_or_symbol)
    if p.startswith("SH"):
        return "上交所"
    if p.startswith("SZ"):
        return "深交所"
    if p.startswith("BJ"):
        return "北交所"
    return "未知"


@dataclass
class MarketData:
    """市场数据结构"""

    symbol: str
    timestamp: datetime
    open: float
    high: float
    low: float
    close: float
    volume: int
    adj_close: float | None = None


@dataclass
class StockInfo:
    """股票信息"""

    symbol: str
    name: str
    exchange: str
    industry: str | None = None
    market_cap: float | None = None


class MarketDataService:
    """统一市场数据服务(真实数据库查询)"""

    def __init__(self):
        self.config = get_config()

    async def get_market_data(
        self,
        symbols: list[str],
        start_date: datetime,
        end_date: datetime,
        timeframe: str = "1d",
    ) -> dict[str, Any]:
        """获取市场数据(从 klines 表读取真实历史数据)"""
        if not symbols:
            raise ValueError("股票代码列表不能为空")

        try:
            if start_date >= end_date:
                raise ValueError("开始日期必须早于结束日期")

            # 限制数据量
            days_diff = (end_date - start_date).days
            if days_diff > 365 * 5:
                logger.warning(f"数据范围过大，限制为5年: {days_diff}天 -> 1825天")
                end_date = start_date + timedelta(days=365 * 5)

            interval = "1d" if timeframe in ("1d", "daily", "day") else timeframe

            result = {
                "symbols": symbols,
                "start_date": start_date.isoformat(),
                "end_date": end_date.isoformat(),
                "timeframe": timeframe,
                "data": {},
                "errors": [],
            }

            for symbol in symbols:
                try:
                    stock_data = await self._get_stock_data_from_db(symbol, start_date, end_date, interval)
                    result["data"][symbol] = stock_data
                    logger.info(f"成功获取股票 {symbol} 数据: {len(stock_data)} 条记录")
                except Exception as e:
                    error_msg = f"获取股票 {symbol} 数据失败: {e}"
                    logger.warning(error_msg)
                    result["data"][symbol] = []
                    result["errors"].append(error_msg)

            if all(len(data) == 0 for data in result["data"].values()):
                raise ValueError("所有股票数据获取失败")

            return result

        except ValueError as e:
            logger.error(f"获取市场数据参数错误: {e}")
            raise
        except Exception as e:
            logger.error(f"获取市场数据失败: {e}")
            raise RuntimeError(f"市场数据获取失败: {e}")

    async def _get_stock_data_from_db(
        self, symbol: str, start_date: datetime, end_date: datetime, interval: str = "1d"
    ) -> list[dict[str, Any]]:
        """从 klines 表获取真实历史数据"""
        code = _code6(symbol)
        if not re.match(r'^\d{6}$', code):
            return []
        sql = """
            SELECT timestamp, open_price, high_price, low_price, close_price,
                   volume, amount, change, change_percent, turnover_rate
            FROM klines
            WHERE symbol = %s AND interval = %s
              AND timestamp >= %s AND timestamp <= %s
            ORDER BY timestamp ASC
        """
        rows = await asyncio.to_thread(
            _pg_query, sql, (code, interval, start_date, end_date)
        )

        data = []
        for r in rows:
            data.append(
                {
                    "date": r["timestamp"].isoformat() if r["timestamp"] else None,
                    "open": float(r["open_price"] or 0),
                    "high": float(r["high_price"] or 0),
                    "low": float(r["low_price"] or 0),
                    "close": float(r["close_price"] or 0),
                    "volume": int(r["volume"] or 0),
                    "amount": float(r["amount"] or 0),
                    "change": float(r["change"]) if r["change"] is not None else None,
                    "change_percent": float(r["change_percent"])
                    if r["change_percent"] is not None
                    else None,
                    "adj_close": float(r["close_price"] or 0),
                }
            )
        return data

    async def get_stock_info(self, symbol: str) -> StockInfo | None:
        """获取股票信息(从 stock_daily_latest + stocks 表读取真实信息)"""
        if not symbol or not symbol.strip():
            return None

        try:
            code = _code6(symbol)
            if not code.isdigit() or len(code) != 6:
                return None

            # 从 stock_daily_latest 取最新一条,获取真实名称
            sdl_symbol = StockCodeUtil.to_prefix(code)
            sql = """
                SELECT symbol, stock_name, trade_date
                FROM stock_daily_latest
                WHERE symbol = %s
                ORDER BY trade_date DESC LIMIT 1
            """
            rows = await asyncio.to_thread(_pg_query, sql, (sdl_symbol,))

            # 从 stocks 表取 exchange / industry
            sym_dot = StockCodeUtil.to_suffix(code)
            sql2 = "SELECT symbol, name, exchange, industry FROM stocks WHERE symbol = %s"
            rows2 = await asyncio.to_thread(_pg_query, sql2, (sym_dot,))

            name = ""
            exchange = _exchange_of(code)
            industry = None

            if rows:
                name = rows[0].get("stock_name") or ""
            if rows2:
                r2 = rows2[0]
                name = name or r2.get("name") or ""
                exchange = r2.get("exchange") or exchange
                industry = r2.get("industry")

            if not name:
                name = f"股票{code}"

            return StockInfo(
                symbol=symbol,
                name=name,
                exchange=exchange,
                industry=industry,
            )

        except Exception as e:
            logger.error(f"获取股票 {symbol} 信息失败: {e}")
            return None

    async def search_stocks(self, keyword: str, limit: int = 10) -> list[StockInfo]:
        """搜索股票(从 stocks 表 + stock_daily_latest 名称搜索)"""
        if not keyword or not keyword.strip():
            return []

        if limit <= 0 or limit > 100:
            limit = 10

        try:
            results: list[StockInfo] = []
            kw = f"%{keyword.strip()}%"

            # 1. 按代码搜索 stocks 表
            sql = """
                SELECT symbol, name, exchange, industry
                FROM stocks
                WHERE symbol LIKE %s
                ORDER BY symbol
                LIMIT %s
            """
            rows = await asyncio.to_thread(_pg_query, sql, (kw, limit))
            for r in rows:
                results.append(
                    StockInfo(
                        symbol=r["symbol"],
                        name=r.get("name") or "",
                        exchange=r.get("exchange") or "",
                        industry=r.get("industry"),
                    )
                )

            # 2. 若结果不足,按名称在 stock_daily_latest 搜索
            if len(results) < limit:
                sql2 = """
                    SELECT DISTINCT symbol, stock_name
                    FROM stock_daily_latest
                    WHERE stock_name LIKE %s
                    LIMIT %s
                """
                rows2 = await asyncio.to_thread(_pg_query, sql2, (kw, limit - len(results)))
                existing = {r.symbol for r in results}
                for r in rows2:
                    sym = r.get("symbol", "")
                    if sym and sym not in existing:
                        code = _code6(sym)
                        results.append(
                            StockInfo(
                                symbol=sym,
                                name=r.get("stock_name") or "",
                                exchange=_exchange_of(sym),
                            )
                        )

            logger.info(f"搜索 '{keyword}' 找到 {len(results)} 只股票")
            return results[:limit]

        except Exception as e:
            logger.error(f"搜索股票失败: {e}")
            return []

    async def get_stock_pool(self, pool_name: str = "default") -> list[str]:
        """获取股票池(从 stock_daily_latest 取有数据的标的)"""
        if not pool_name or not pool_name.strip():
            pool_name = "default"

        try:
            # 取 stock_daily_latest 中最新的 N 只标的
            sql = """
                SELECT DISTINCT symbol
                FROM stock_daily_latest
                ORDER BY symbol
                LIMIT 200
            """
            rows = await asyncio.to_thread(_pg_query, sql)
            pool = [r["symbol"] for r in rows if r.get("symbol")]

            if not pool:
                # 降级:返回默认蓝筹池
                pool = ["SH600519", "SH600036", "SZ000001", "SZ000858"]

            logger.info(f"获取股票池 {pool_name}: {len(pool)} 只股票")
            return pool

        except Exception as e:
            logger.error(f"获取股票池失败: {e}")
            return ["SH600519", "SH600036", "SZ000001", "SZ000858"]


# 全局实例
_market_data_service = None


def get_market_data_service() -> MarketDataService:
    """获取市场数据服务实例"""
    global _market_data_service
    if _market_data_service is None:
        _market_data_service = MarketDataService()
    return _market_data_service


# 便捷函数
async def get_market_data(
    symbols: list[str], start_date: datetime, end_date: datetime, timeframe: str = "1d"
) -> dict[str, Any]:
    """获取市场数据便捷函数"""
    service = get_market_data_service()
    return await service.get_market_data(symbols, start_date, end_date, timeframe)


async def get_stock_info(symbol: str) -> StockInfo | None:
    """获取股票信息便捷函数"""
    service = get_market_data_service()
    return await service.get_stock_info(symbol)


async def search_stocks(keyword: str, limit: int = 10) -> list[StockInfo]:
    """搜索股票便捷函数"""
    service = get_market_data_service()
    return await service.search_stocks(keyword, limit)


async def get_stock_pool(pool_name: str = "default") -> list[str]:
    """获取股票池便捷函数"""
    service = get_market_data_service()
    return await service.get_stock_pool(pool_name)
