"""
企业级高性能最新行情同步脚本 (v2.0 - 并行与原子化优化)
功能：将 stock_daily 中的最新数据原子化刷新到 stock_daily_latest，支持并行索引与影子表切换。
"""

import logging
import sys
import os
import asyncio
from datetime import datetime
import time

# 确保可以导入 backend
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

try:
    from backend.shared.database_manager_v2 import get_session
    from sqlalchemy import text
except ImportError:
    print("❌ 无法加载数据库管理器，请确保在项目根目录运行。")
    sys.exit(1)

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger(__name__)

async def run_sql(session, sql, params=None):
    """辅助函数：执行 SQL"""
    return await session.execute(text(sql), params or {})

async def sync_component(name, task_coro):
    """并行任务包装器"""
    start = time.time()
    logger.info(f"开始同步组件: {name}...")
    result = await task_coro
    elapsed = time.time() - start
    logger.info(f"组件 {name} 同步完成, 耗时: {elapsed:.2f}s")
    return result

async def perform_sync():
    async with get_session() as session:
        # 1. 探测最新交易日 (Index Search)
        res = await session.execute(text("SELECT MAX(trade_date) FROM stock_daily"))
        latest_date = res.scalar()
        if not latest_date:
            logger.error("❌ 原始表为空")
            return

        logger.info(f"📅 目标日期: {latest_date}")

        # 2. 创建影子表 (Shadow Table)
        # 影子表与正式表结构完全一致
        logger.info("🏗️ 正在创建影子表 stock_daily_latest_tmp...")
        await session.execute(text("DROP TABLE IF EXISTS stock_daily_latest_tmp"))
        await session.execute(text("CREATE TABLE stock_daily_latest_tmp (LIKE stock_daily_latest INCLUDING ALL)"))

        # 3. 执行高速并行导入 (Bulk Insert)
        # 字段需手动列出以确保类型和顺序匹配
        insert_sql = f"""
            INSERT INTO stock_daily_latest_tmp (
                trade_date, code, stock_name, open, high, low, close,
                volume, turnover, pct_change, turnover_rate,
                pe_ttm, pb, total_mv, is_st, is_hs300, is_csi1000,
                listing_market, industry, nindnme, corp_nature
            )
            SELECT
                trade_date, code, stock_name, open, high, low, close,
                volume, turnover, pct_change, turnover_rate,
                pe_ttm, pb, total_mv, is_st, is_hs300, is_csi1000,
                listing_market, industry, nindnme, corp_nature
            FROM stock_daily
            WHERE trade_date = :d
        """
        
        start_time = time.time()
        res = await session.execute(text(insert_sql), {"d": latest_date})
        logger.info(f"📥 数据导入完成: {res.rowcount} 条记录, 耗时: {time.time() - start_time:.2f}s")

        # 4. 并行构建索引 (Parallel Maintenance)
        # 在临时表上并行创建索引，可以显著缩短总时长
        logger.info("⚡ 正在并行构建影子表索引...")
        index_tasks = [
            run_sql(session, "CREATE INDEX IF NOT EXISTS idx_tmp_trade_date ON stock_daily_latest_tmp(trade_date)"),
            run_sql(session, "CREATE INDEX IF NOT EXISTS idx_tmp_industry ON stock_daily_latest_tmp(industry)"),
            run_sql(session, "CREATE INDEX IF NOT EXISTS idx_tmp_mv ON stock_daily_latest_tmp(total_mv DESC)"),
            run_sql(session, "CREATE INDEX IF NOT EXISTS idx_tmp_pe ON stock_daily_latest_tmp(pe_ttm)"),
        ]
        await asyncio.gather(*index_tasks)

        # 5. 原子化交换 (Atomic Swap)
        # 在一个事务内完成重命名，确保前端查询无感知（零停机）
        logger.info("🔄 正在执行原子化表交换...")
        swap_sql = """
            BEGIN;
            DROP TABLE IF EXISTS stock_daily_latest_old;
            ALTER TABLE stock_daily_latest RENAME TO stock_daily_latest_old;
            ALTER TABLE stock_daily_latest_tmp RENAME TO stock_daily_latest;
            DROP TABLE stock_daily_latest_old;
            COMMIT;
        """
        # 注意：SQLAlchemy 的 session.execute 已经处理了事务，所以我们直接运行 rename
        try:
            await session.execute(text("DROP TABLE IF EXISTS stock_daily_latest_old"))
            await session.execute(text("ALTER TABLE stock_daily_latest RENAME TO stock_daily_latest_old"))
            await session.execute(text("ALTER TABLE stock_daily_latest_tmp RENAME TO stock_daily_latest"))
            await session.execute(text("DROP TABLE stock_daily_latest_old"))
            await session.commit()
            logger.info("✅ 原子化交换完成!")
        except Exception as e:
            await session.rollback()
            logger.error(f"❌ 交换失败，已回滚: {e}")
            raise

        # 6. 刷新统计信息 (Optimization)
        # 确保向导查询使用最新的执行计划
        logger.info("📊 正在更新数据库统计信息 (ANALYZE)...")
        await session.execute(text("ANALYZE stock_daily_latest"))
        
        logger.info(f"✨ 同步任务圆满完成! 总耗时: {time.time() - start_time:.2f}s")

if __name__ == "__main__":
    asyncio.run(perform_sync())
