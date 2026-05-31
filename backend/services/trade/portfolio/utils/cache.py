"""Async cache helpers for trade portfolio module."""

import json
import logging
from typing import Any, Optional

from backend.services.trade.redis_client import get_redis as get_trade_redis

logger = logging.getLogger(__name__)


class RedisCache:
    def __init__(self):
        self._client = None

    def _get_client(self):
        if self._client is None:
            # 使用 trade 专用 Redis 连接 (DB 2)，与交易数据在同一 DB
            self._client = get_trade_redis().client
        return self._client

    async def get(self, key: str) -> Any | None:
        try:
            value = self._get_client().get(key)
            if value is None:
                return None
            if isinstance(value, bytes):
                value = value.decode("utf-8")
            return json.loads(value)
        except Exception as exc:
            logger.warning(f"Redis get failed ({key}): {exc}")
            return None

    async def set(self, key: str, value: Any, ttl: int = 300) -> bool:
        try:
            payload = json.dumps(value, default=str)
            return bool(self._get_client().setex(key, ttl, payload))
        except Exception as exc:
            logger.warning(f"Redis set failed ({key}): {exc}")
            return False

    async def delete(self, key: str) -> bool:
        try:
            return bool(self._get_client().delete(key))
        except Exception as exc:
            logger.warning(f"Redis delete failed ({key}): {exc}")
            return False


cache = RedisCache()


def get_cache_key(prefix: str, *args: Any) -> str:
    return f"portfolio:{prefix}:" + ":".join(str(arg) for arg in args)