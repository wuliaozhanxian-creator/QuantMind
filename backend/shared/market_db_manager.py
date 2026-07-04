import logging
import os
from contextlib import asynccontextmanager
from typing import AsyncGenerator

from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, create_async_engine
from sqlalchemy.orm import sessionmaker

logger = logging.getLogger(__name__)

# ============================================================
# 行情数据库配置 — 使用容器内本地 PostgreSQL
# ------------------------------------------------------------
# 历史背景：M2 之前此模块连接远程行情服务器（106.53.100.144）的
#   PostgreSQL，使用 Fernet 硬编码加密凭据。M2 安全加固后已改为
#   读取容器内本地 PostgreSQL（与业务库同源，DB_* 环境变量），
#   远程 PostgreSQL 已废弃，仅远程 Redis 仍在使用。
#
# 只读凭据隔离（T5.1）：
#   - 本地 PG 允许读写用户（DB_USER=quantmind 等业务账号）
#   - 若未来恢复远程 PG 连接（host 非本地），则强制要求只读用户
#     白名单中的账号，防止行情读取模块以读写凭据连接远程库
#   - 远程 PG 只读用户白名单：quantmind_market / readonly_monitor
#     / quantmind_readonly
# ============================================================
MARKET_DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
MARKET_DB_PORT = int(os.getenv("DB_PORT", "5432"))
MARKET_DB_NAME = os.getenv("DB_NAME", "quantmind")
MARKET_DB_USER = os.getenv("DB_USER", "quantmind")
MARKET_DB_PASSWORD = os.getenv("DB_PASSWORD", "")

MARKET_DB_URL = f"postgresql+asyncpg://{MARKET_DB_USER}:{MARKET_DB_PASSWORD}@{MARKET_DB_HOST}:{MARKET_DB_PORT}/{MARKET_DB_NAME}"

# T5.1 只读凭据隔离：远程 PG 只读用户白名单
READONLY_PG_USERS = {"quantmind_market", "readonly_monitor", "quantmind_readonly"}
# 本地 PG host 白名单（允许读写用户）
_LOCAL_PG_HOSTS = {"db", "127.0.0.1", "localhost", "postgres"}


def _assert_readonly_credential() -> None:
    """T5.1 只读凭据隔离断言

    本地 PG 允许读写用户；远程 PG（host 非本地）必须使用只读用户白名单
    中的账号，防止行情读取模块以读写凭据连接远程行情库执行写入操作。
    """
    host = (MARKET_DB_HOST or "").strip().lower()
    user = (MARKET_DB_USER or "").strip()
    if host in _LOCAL_PG_HOSTS:
        return  # 本地 PG，允许读写用户
    # 远程 PG：必须使用只读用户白名单中的账号
    if user not in READONLY_PG_USERS:
        raise RuntimeError(
            f"[T5.1] 远程 PG 只读凭据隔离失败：host={MARKET_DB_HOST} user={user} "
            f"不在只读用户白名单 {sorted(READONLY_PG_USERS)} 中。"
            f"行情读取模块禁止以读写凭据连接远程行情库（远程 PG 仅允许只读访问）。"
        )


_market_engine: AsyncEngine | None = None
_market_session_factory: sessionmaker | None = None


def get_market_engine() -> AsyncEngine:
    """获取行情数据库的异步引擎（容器内 PG）

    注意：此连接为行情数据读取连接，禁止通过此连接执行
    INSERT/UPDATE/DELETE 写入远程行情库（T5.1 只读约束）。
    """
    global _market_engine, _market_session_factory
    if _market_engine is None:
        _assert_readonly_credential()  # T5.1 只读凭据断言
        logger.info(f"Initializing market DB engine: {MARKET_DB_HOST}:{MARKET_DB_PORT}/{MARKET_DB_NAME}")
        _market_engine = create_async_engine(
            MARKET_DB_URL,
            pool_size=10,
            max_overflow=20,
            pool_pre_ping=True,
            echo=False,
        )
        _market_session_factory = sessionmaker(
            _market_engine, class_=AsyncSession, expire_on_commit=False
        )
    return _market_engine


@asynccontextmanager
async def get_market_session() -> AsyncGenerator[AsyncSession, None]:
    """获取行情数据库会话"""
    get_market_engine()  # Ensure engine is initialized
    async with _market_session_factory() as session:
        try:
            yield session
        finally:
            await session.close()
