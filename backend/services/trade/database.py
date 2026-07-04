"""
Database connection and session management

历史遗留兼容层：trade 服务实际的数据库引擎与会话由
``backend.shared.database_manager_v2`` 统一管理（见 ``trade/main.py``
的 lifespan 与 ``trade/deps.py`` 的 ``get_db``）。本模块不再创建独立
engine，避免与 database_manager_v2 形成双池资源浪费。

为保持向后兼容，仍保留 ``get_db`` / ``init_db`` / ``close_db`` 三个
函数入口，内部全部委托给 database_manager_v2。
"""

import logging

from backend.shared.schema_registry import create_registered_tables

logger = logging.getLogger(__name__)


async def get_db():
    """获取数据库 Session（委托给 database_manager_v2）

    与 ``trade/deps.py`` 的 ``get_db`` 保持一致，统一使用
    database_manager_v2 的连接池，避免维护冗余 engine。
    """
    from backend.shared.database_manager_v2 import get_session

    async with get_session(read_only=False) as session:
        yield session


async def init_db():
    """Initialize database (create tables for all trade schemas).

    委托给 database_manager_v2 的引擎，避免在本模块重建独立 engine。
    """
    from backend.shared.database_manager_v2 import get_db_manager, init_database

    await init_database()
    await create_registered_tables(
        get_db_manager()._master_engine,
        schema_keys=("trade.core", "trade.simulation", "trade.portfolio"),
    )
    logger.info("Database tables created successfully")


async def close_db():
    """Close database connections（委托给 database_manager_v2）

    本模块不再持有独立 engine，关闭操作统一交由 database_manager_v2 处理，
    保持接口向后兼容。
    """
    from backend.shared.database_manager_v2 import close_database

    await close_database()
    logger.info("Database connections closed")
