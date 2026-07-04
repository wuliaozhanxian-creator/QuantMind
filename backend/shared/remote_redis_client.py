import logging
import os
import threading
from typing import Optional

from redis import ConnectionPool, Redis

logger = logging.getLogger(__name__)

# ============================================================
# Redis 配置 — 默认使用本地 Redis（通过环境变量覆盖）
# ============================================================
REMOTE_REDIS_HOST = os.getenv("REMOTE_QUOTE_REDIS_HOST", os.getenv("REDIS_HOST", "localhost"))
REMOTE_REDIS_PORT = int(os.getenv("REMOTE_QUOTE_REDIS_PORT", os.getenv("REDIS_PORT", "6379")))
REMOTE_REDIS_USER = os.getenv("REMOTE_QUOTE_REDIS_USER", os.getenv("REDIS_USER", ""))
REMOTE_REDIS_PASSWORD = os.getenv("REMOTE_QUOTE_REDIS_PASSWORD", os.getenv("REDIS_PASSWORD", ""))
REMOTE_REDIS_DB = int(os.getenv("REMOTE_QUOTE_REDIS_DB", os.getenv("REDIS_DB", "0")))

# 连接池上限（参考 redis_sentinel_client.py 的 REDIS_MAX_CONNECTIONS）
REMOTE_REDIS_MAX_CONNECTIONS = int(os.getenv("REMOTE_REDIS_MAX_CONNECTIONS", "50"))

# ============================================================
# 连接池单例（按 db 分组）
# —— Redis 连接池绑定特定 db，为保持 get_remote_redis_client(db=...)
#    签名向后兼容，按 db 维度各维护一个 ConnectionPool + Redis 单例，
#    避免每次调用新建 Redis 对象导致远程 Redis（106.53.100.144）连接耗尽。
#    参考实现：
#      - backend/services/engine/qlib_app/cache_manager.py (ConnectionPool + close)
#      - backend/shared/redis_sentinel_client.py (max_connections + close)
# ============================================================
_pools: dict[int, ConnectionPool] = {}
_clients: dict[int, Redis] = {}
_lock = threading.Lock()


def _build_pool(db: int) -> ConnectionPool:
    """构造一个绑定到指定 db 的连接池"""
    return ConnectionPool(
        host=REMOTE_REDIS_HOST,
        port=REMOTE_REDIS_PORT,
        db=db,
        username=REMOTE_REDIS_USER or None,
        password=REMOTE_REDIS_PASSWORD or None,
        max_connections=REMOTE_REDIS_MAX_CONNECTIONS,
        decode_responses=True,
        socket_timeout=5,
        socket_connect_timeout=5,
        socket_keepalive=True,
        health_check_interval=30,
    )


def get_remote_redis_client(db: int = None) -> Redis:
    """获取 Redis 客户端（基于连接池的单例，按 db 分组）

    与历史实现保持签名兼容：传入 db 则使用指定 db，否则使用
    ``REMOTE_REDIS_DB``。同一个 db 仅创建一次 ConnectionPool 与 Redis
    实例，后续调用复用，避免连接泄漏。返回的 ``Redis`` 对象接口不变。
    """
    if db is None:
        db = REMOTE_REDIS_DB
    client = _clients.get(db)
    if client is not None:
        return client
    with _lock:
        # 双检锁：进入锁后再次确认，避免并发重复创建
        client = _clients.get(db)
        if client is None:
            pool = _pools.get(db)
            if pool is None:
                pool = _build_pool(db)
                _pools[db] = pool
                logger.info(
                    "remote redis pool created: host=%s port=%s db=%d max_connections=%d",
                    REMOTE_REDIS_HOST,
                    REMOTE_REDIS_PORT,
                    db,
                    REMOTE_REDIS_MAX_CONNECTIONS,
                )
            client = Redis(connection_pool=pool, decode_responses=True)
            _clients[db] = client
        return client


def close_remote_redis_client(db: Optional[int] = None) -> None:
    """关闭远程 Redis 连接池，优雅释放资源

    Args:
        db: 指定要关闭的 db；为 None 时关闭所有 db 的连接池与客户端。
    """
    with _lock:
        if db is None:
            dbs = list(_pools.keys())
        else:
            dbs = [db] if db in _pools else []
        for d in dbs:
            client = _clients.pop(d, None)
            if client is not None:
                try:
                    client.close()
                except Exception as e:  # noqa: BLE001
                    logger.error("close remote redis client failed: db=%d err=%s", d, e)
            pool = _pools.pop(d, None)
            if pool is not None:
                try:
                    pool.disconnect()
                    logger.info("remote redis pool closed: db=%d", d)
                except Exception as e:  # noqa: BLE001
                    logger.error("close remote redis pool failed: db=%d err=%s", d, e)
