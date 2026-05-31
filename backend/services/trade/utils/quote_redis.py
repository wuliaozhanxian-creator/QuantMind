"""交易服务行情 Redis 连接 — 远程行情服务器 DB 3"""

from backend.shared.remote_redis_client import get_remote_redis_client


def get_quote_redis():
    """获取交易服务行情 Redis 客户端（远程 106 服务器 DB 3）"""
    return get_remote_redis_client(db=3)
