import logging
import os

from redis import Redis

logger = logging.getLogger(__name__)

# ============================================================
# Redis 配置 — 默认使用本地 Redis（通过环境变量覆盖）
# ============================================================
REMOTE_REDIS_HOST = os.getenv("REMOTE_QUOTE_REDIS_HOST", os.getenv("REDIS_HOST", "localhost"))
REMOTE_REDIS_PORT = int(os.getenv("REMOTE_QUOTE_REDIS_PORT", os.getenv("REDIS_PORT", "6379")))
REMOTE_REDIS_USER = os.getenv("REMOTE_QUOTE_REDIS_USER", os.getenv("REDIS_USER", ""))
REMOTE_REDIS_PASSWORD = os.getenv("REMOTE_QUOTE_REDIS_PASSWORD", os.getenv("REDIS_PASSWORD", ""))
REMOTE_REDIS_DB = int(os.getenv("REMOTE_QUOTE_REDIS_DB", os.getenv("REDIS_DB", "0")))


def get_remote_redis_client(db: int = None) -> Redis:
    """获取 Redis 客户端"""
    if db is None:
        db = REMOTE_REDIS_DB
    return Redis(
        host=REMOTE_REDIS_HOST,
        port=REMOTE_REDIS_PORT,
        db=db,
        username=REMOTE_REDIS_USER or None,
        password=REMOTE_REDIS_PASSWORD or None,
        decode_responses=True,
    )
