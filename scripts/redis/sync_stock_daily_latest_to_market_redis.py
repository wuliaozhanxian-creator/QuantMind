#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""将 stock_daily_latest 同步到目标行情 Redis。

默认行为：仅同步最新 trade_date（建议生产常态使用）。
可选：--all 同步全表历史。

写入键：
- stock:{symbol}                      -> 最新快照（供行情查询）
- sdl:{trade_date}:{symbol}           -> 指定交易日快照
- sdl:latest_trade_date               -> 最新交易日
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
from typing import Any

import asyncpg
import redis

ROOT = Path(__file__).resolve().parents[2]


def load_env(path: Path) -> None:
    if not path.exists():
        return
    for line in path.read_text(encoding="utf-8").splitlines():
        s = line.strip()
        if not s or s.startswith("#") or "=" not in s:
            continue
        k, v = s.split("=", 1)
        k = k.strip()
        if not k or os.getenv(k) is not None:
            continue
        os.environ[k] = v.strip().strip('"').strip("'")


def env(*keys: str, default: str = "") -> str:
    for k in keys:
        v = os.getenv(k)
        if v is not None and str(v).strip() != "":
            return str(v).strip()
    return default


def to_symbol(raw: str) -> str:
    s = (raw or "").strip().upper()
    if not s:
        return s
    if "." in s:
        return s
    if s.startswith("SH"):
        return f"{s[2:]}.SH"
    if s.startswith("SZ"):
        return f"{s[2:]}.SZ"
    if s.startswith("BJ"):
        return f"{s[2:]}.BJ"
    if s.startswith("6"):
        return f"{s}.SH"
    if s.startswith(("0", "2", "3")):
        return f"{s}.SZ"
    if s.startswith(("4", "8", "9")):
        return f"{s}.BJ"
    return s


def _num(v: Any, d: float = 0.0) -> float:
    if v is None:
        return d
    try:
        return float(v)
    except Exception:
        return d


def _int(v: Any, d: int = 0) -> int:
    if v is None:
        return d
    try:
        return int(v)
    except Exception:
        return d


async def main() -> None:
    parser = argparse.ArgumentParser(description="Sync stock_daily_latest to market redis")
    parser.add_argument("--all", action="store_true", help="同步全表历史")
    parser.add_argument("--trade-date", type=str, default="", help="仅同步指定交易日，格式 YYYY-MM-DD")
    parser.add_argument("--batch", type=int, default=2000, help="批量写入大小")
    parser.add_argument("--ttl", type=int, default=0, help="stock:* 键过期秒数，0 表示不过期")
    args = parser.parse_args()

    load_env(ROOT / ".env")

    db_host = env("DB_HOST", default="127.0.0.1")
    db_port = int(env("DB_PORT", default="5432"))
    db_name = env("DB_NAME", default="quantmind")
    db_user = env("DB_USER", default="quantmind")
    db_password = env("DB_PASSWORD", default="")

    redis_host = env("REDIS_MARKET_HOST", "REMOTE_QUOTE_REDIS_HOST", default="127.0.0.1")
    redis_port = int(env("REDIS_MARKET_PORT", "REMOTE_QUOTE_REDIS_PORT", default="36379"))
    redis_password = env("REDIS_MARKET_PASSWORD", "REMOTE_QUOTE_REDIS_PASSWORD", default="") or None
    redis_db = int(env("REDIS_MARKET_DB", "REDIS_DB_MARKET", default="0"))

    pg = await asyncpg.connect(
        host=db_host,
        port=db_port,
        database=db_name,
        user=db_user,
        password=db_password,
        command_timeout=180,
    )
    r = redis.Redis(
        host=redis_host,
        port=redis_port,
        password=redis_password,
        db=redis_db,
        decode_responses=True,
        socket_connect_timeout=8,
        socket_timeout=12,
    )
    r.ping()

    if args.trade_date:
        target_date = args.trade_date
    elif args.all:
        target_date = None
    else:
        target_date = await pg.fetchval("SELECT MAX(trade_date)::text FROM stock_daily_latest")

    if not target_date and not args.all:
        raise SystemExit("stock_daily_latest 无可同步数据")

    where_sql = ""
    params: list[Any] = []
    if target_date:
        where_sql = "WHERE trade_date = $1"
        params = [target_date]

    total = await pg.fetchval(f"SELECT COUNT(*) FROM stock_daily_latest {where_sql}", *params)
    print(f"[sync] target_date={target_date or 'ALL'} total_rows={total} redis={redis_host}:{redis_port}/{redis_db}")

    if total == 0:
        await pg.close()
        r.close()
        return

    offset = 0
    written = 0
    while offset < total:
        if target_date:
            rows = await pg.fetch(
                f"""
                SELECT trade_date, symbol, stock_name, open, high, low, close, volume, amount,
                       pct_change, turnover_rate, pe_ttm, pb, total_mv, float_mv,
                       rsi_6, rsi_14, ma5, ma10, ma20, ma60, return_1d, return_3d,
                       return_5d, return_10d, return_20d, volume_ratio_5, volume_ratio_20,
                       consecutive_limit_up_days, industry, province, listed_days
                FROM stock_daily_latest
                {where_sql}
                ORDER BY symbol
                OFFSET {offset} LIMIT {args.batch}
                """,
                *params,
            )
        else:
            rows = await pg.fetch(
                f"""
                SELECT trade_date, symbol, stock_name, open, high, low, close, volume, amount,
                       pct_change, turnover_rate, pe_ttm, pb, total_mv, float_mv,
                       rsi_6, rsi_14, ma5, ma10, ma20, ma60, return_1d, return_3d,
                       return_5d, return_10d, return_20d, volume_ratio_5, volume_ratio_20,
                       consecutive_limit_up_days, industry, province, listed_days
                FROM stock_daily_latest
                ORDER BY trade_date, symbol
                OFFSET {offset} LIMIT {args.batch}
                """
            )

        if not rows:
            break

        pipe = r.pipeline(transaction=False)
        for row in rows:
            trade_date = str(row["trade_date"])
            symbol = to_symbol(str(row["symbol"] or ""))
            if not symbol:
                continue

            mapping = {
                "symbol": symbol,
                "trade_date": trade_date,
                "stock_name": str(row["stock_name"] or ""),
                "open": _num(row["open"]),
                "high": _num(row["high"]),
                "low": _num(row["low"]),
                "close": _num(row["close"]),
                "volume": _num(row["volume"]),
                "amount": _num(row["amount"]),
                "pct_change": _num(row["pct_change"]),
                "turnover_rate": _num(row["turnover_rate"]),
                "pe_ttm": _num(row["pe_ttm"]),
                "pb": _num(row["pb"]),
                "total_mv": _num(row["total_mv"]),
                "float_mv": _num(row["float_mv"]),
                "rsi_6": _num(row["rsi_6"]),
                "rsi_14": _num(row["rsi_14"]),
                "ma5": _num(row["ma5"]),
                "ma10": _num(row["ma10"]),
                "ma20": _num(row["ma20"]),
                "ma60": _num(row["ma60"]),
                "return_1d": _num(row["return_1d"]),
                "return_3d": _num(row["return_3d"]),
                "return_5d": _num(row["return_5d"]),
                "return_10d": _num(row["return_10d"]),
                "return_20d": _num(row["return_20d"]),
                "volume_ratio_5": _num(row["volume_ratio_5"]),
                "volume_ratio_20": _num(row["volume_ratio_20"]),
                "consecutive_limit_up_days": _int(row["consecutive_limit_up_days"]),
                "industry": str(row["industry"] or ""),
                "province": str(row["province"] or ""),
                "listed_days": _int(row["listed_days"]),
                "timestamp": int(asyncio.get_event_loop().time()),
            }

            latest_key = f"stock:{symbol}"
            day_key = f"sdl:{trade_date}:{symbol}"

            pipe.hset(latest_key, mapping=mapping)
            if args.ttl > 0:
                pipe.expire(latest_key, args.ttl)
            pipe.hset(day_key, mapping=mapping)
            written += 1

        pipe.execute()
        offset += len(rows)
        print(f"[sync] progress {offset}/{total}")

    if target_date:
        r.set("sdl:latest_trade_date", target_date)

    sample = r.hgetall("stock:000001.SZ")
    print(f"[sync] done written={written} sample_stock_000001_SZ={'OK' if sample else 'MISSING'}")

    await pg.close()
    r.close()


if __name__ == "__main__":
    asyncio.run(main())
