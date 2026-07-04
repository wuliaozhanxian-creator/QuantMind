"""数据缺口检测模块 (T5.3)

职责：
1. 检测 stock_daily_latest 表的数据缺口：
   - 交易日缺失（某交易日无数据）
   - 股票缺失（某交易日某股票无数据）
   - 字段缺失（关键字段为 NULL）
2. 缺口发现后自动触发补数任务
3. 补数失败则告警

设计约束：
- 仅连接本地 PostgreSQL（不连接外部数据库）
- 使用 trading_calendar 获取交易日历
- 检测结果写入监控状态文件（供 /api/etl/status 读取）
- 补数任务通过调用现有 ETL 脚本（sync_official_data_update）触发
"""

from __future__ import annotations

import asyncio
import json
import logging
import os
import sys
from dataclasses import asdict, dataclass, field
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Optional

from sqlalchemy import text

_PROJECT_ROOT = Path(__file__).resolve().parents[2]
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from backend.scripts.etl_alerts import (
    LEVEL_ERROR,
    LEVEL_WARNING,
    get_monitor_state_store,
    publish_alert,
)
from backend.shared.database_manager_v2 import get_session

logger = logging.getLogger(__name__)

# 关键字段（NULL 即视为字段缺失）
DEFAULT_CRITICAL_FIELDS = [
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
]

# 检测窗口：默认检查最近 N 个交易日
DEFAULT_LOOKBACK_DAYS = int(os.getenv("ETL_GAP_LOOKBACK_DAYS", "10"))
# 单日股票数量低于该阈值视为异常（可能大面积缺失）
DEFAULT_MIN_STOCKS_PER_DAY = int(os.getenv("ETL_GAP_MIN_STOCKS", "3000"))
# 最大允许缺失股票比例（相对基准日）
DEFAULT_MAX_MISSING_RATIO = float(os.getenv("ETL_GAP_MAX_MISSING_RATIO", "0.05"))


# ============================================================
# 缺口检测结果数据结构
# ============================================================
@dataclass
class MissingTradingDay:
    trade_date: str
    reason: str = "no_data"


@dataclass
class MissingStockGap:
    trade_date: str
    missing_count: int
    missing_ratio: float
    sample_symbols: list[str] = field(default_factory=list)


@dataclass
class NullFieldGap:
    trade_date: str
    field: str
    null_count: int
    total_count: int


@dataclass
class DataGapReport:
    checked_at: str
    lookback_days: int
    date_range: dict[str, Optional[str]]
    missing_trading_days: list[dict[str, Any]]
    missing_stock_gaps: list[dict[str, Any]]
    null_field_gaps: list[dict[str, Any]]
    summary: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ============================================================
# 交易日历辅助
# ============================================================
async def _get_trading_days(start: date, end: date) -> list[date]:
    """获取 [start, end] 区间内的交易日列表

    优先使用 qm_market_calendar_day 表；若表不存在或无数据，
    降级为"工作日近似"（周一至周五），并记 warning。
    """
    sql = text(
        """
        SELECT trade_date
        FROM qm_market_calendar_day
        WHERE market = 'SSE'
          AND is_trading_day = TRUE
          AND trade_date BETWEEN :start AND :end
        ORDER BY trade_date
        """
    )
    try:
        async with get_session(read_only=True) as session:
            result = await session.execute(sql, {"start": start, "end": end})
            rows = result.scalars().all()
        if rows:
            return [d if isinstance(d, date) else date.fromisoformat(str(d)) for d in rows]
    except Exception as exc:
        logger.warning("读取交易日历表失败，降级为工作日近似: %s", exc)

    # 降级：工作日近似
    days: list[date] = []
    cur = start
    while cur <= end:
        if cur.weekday() < 5:  # 周一至周五
            days.append(cur)
        cur += timedelta(days=1)
    return days


# ============================================================
# 缺口检测核心
# ============================================================
async def detect_data_gaps(
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
    critical_fields: Optional[list[str]] = None,
    min_stocks_per_day: int = DEFAULT_MIN_STOCKS_PER_DAY,
    max_missing_ratio: float = DEFAULT_MAX_MISSING_RATIO,
) -> DataGapReport:
    """检测 stock_daily_latest 的数据缺口

    Args:
        lookback_days: 向前回溯的日历天数
        critical_fields: 需要检查 NULL 的关键字段列表
        min_stocks_per_day: 单日股票数量下限
        max_missing_ratio: 缺失股票比例上限（相对基准日）
    """
    critical_fields = critical_fields or list(DEFAULT_CRITICAL_FIELDS)
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=lookback_days)

    trading_days = await _get_trading_days(start_date, end_date)

    report = DataGapReport(
        checked_at=datetime.now(timezone.utc).isoformat(),
        lookback_days=lookback_days,
        date_range={"start": start_date.isoformat(), "end": end_date.isoformat()},
        missing_trading_days=[],
        missing_stock_gaps=[],
        null_field_gaps=[],
    )

    if not trading_days:
        report.summary = {
            "trading_days_checked": 0,
            "status": "no_calendar",
            "message": "区间内无交易日（交易日历不可用）",
        }
        return report

    # 1. 查询区间内已有数据的交易日 + 每日股票数
    daily_sql = text(
        """
        SELECT trade_date, COUNT(DISTINCT symbol) AS stock_count
        FROM stock_daily_latest
        WHERE trade_date BETWEEN :start AND :end
        GROUP BY trade_date
        ORDER BY trade_date
        """
    )
    async with get_session(read_only=True) as session:
        result = await session.execute(
            daily_sql, {"start": start_date, "end": end_date}
        )
        daily_rows = {row[0]: int(row[1] or 0) for row in result.all()}

    # 2. 交易日缺失检测
    existing_dates = set()
    for d in daily_rows.keys():
        existing_dates.add(d if isinstance(d, date) else date.fromisoformat(str(d)))

    for td in trading_days:
        if td not in existing_dates:
            report.missing_trading_days.append(
                asdict(MissingTradingDay(trade_date=td.isoformat(), reason="no_data"))
            )

    # 3. 股票缺失检测：以区间内最大股票数为基准，逐日比较
    base_count = max(daily_rows.values()) if daily_rows else 0
    if base_count > 0:
        for td in trading_days:
            td_key = td
            count = daily_rows.get(td_key, 0)
            if count < min_stocks_per_day:
                missing_count = base_count - count
                ratio = round(missing_count / base_count, 4) if base_count else 0.0
                if ratio > 0 or count == 0:
                    # 抽样缺失股票（基准日有但当日无）
                    sample: list[str] = []
                    if missing_count > 0:
                        try:
                            sample_sql = text(
                                """
                                SELECT DISTINCT a.symbol
                                FROM stock_daily_latest a
                                WHERE a.trade_date = (
                                    SELECT MAX(trade_date) FROM stock_daily_latest
                                )
                                  AND NOT EXISTS (
                                    SELECT 1 FROM stock_daily_latest b
                                    WHERE b.symbol = a.symbol AND b.trade_date = :td
                                  )
                                LIMIT 10
                                """
                            )
                            async with get_session(read_only=True) as session:
                                sample_result = await session.execute(
                                    sample_sql, {"td": td}
                                )
                                sample = [str(r[0]) for r in sample_result.all()]
                        except Exception as exc:
                            logger.debug("抽样缺失股票失败: %s", exc)
                    report.missing_stock_gaps.append(
                        asdict(
                            MissingStockGap(
                                trade_date=td.isoformat(),
                                missing_count=missing_count,
                                missing_ratio=ratio,
                                sample_symbols=sample,
                            )
                        )
                    )

    # 4. 字段缺失检测：关键字段 NULL 计数
    for td in trading_days:
        for field_name in critical_fields:
            # 字段名校验（防注入）
            if not field_name.isidentifier():
                continue
            null_sql = text(
                f"""
                SELECT
                    COUNT(*) AS total,
                    COUNT(*) FILTER (WHERE {field_name} IS NULL) AS nulls
                FROM stock_daily_latest
                WHERE trade_date = :td
                """
            )
            try:
                async with get_session(read_only=True) as session:
                    result = await session.execute(null_sql, {"td": td})
                    row = result.first()
            except Exception as exc:
                logger.debug("字段缺失检测失败 field=%s date=%s: %s", field_name, td, exc)
                continue
            if row is None:
                continue
            total = int(row[0] or 0)
            nulls = int(row[1] or 0)
            if nulls > 0 and total > 0:
                report.null_field_gaps.append(
                    asdict(
                        NullFieldGap(
                            trade_date=td.isoformat(),
                            field=field_name,
                            null_count=nulls,
                            total_count=total,
                        )
                    )
                )

    report.summary = {
        "trading_days_checked": len(trading_days),
        "missing_trading_days_count": len(report.missing_trading_days),
        "missing_stock_gaps_count": len(report.missing_stock_gaps),
        "null_field_gaps_count": len(report.null_field_gaps),
        "base_stock_count": base_count,
        "status": _classify_status(report),
    }

    # 写入监控状态文件
    try:
        get_monitor_state_store().update_data_gaps(report.to_dict())
    except Exception:  # pragma: no cover
        logger.exception("数据缺口报告写入监控状态失败")

    return report


def _classify_status(report: DataGapReport) -> str:
    """根据缺口数量判定整体状态"""
    if report.missing_trading_days:
        return "critical"
    if report.missing_stock_gaps:
        # 存在缺失股票比例超过阈值即视为 warning
        for gap in report.missing_stock_gaps:
            if gap["missing_ratio"] >= DEFAULT_MAX_MISSING_RATIO:
                return "warning"
        return "warning"
    if report.null_field_gaps:
        return "warning"
    return "ok"


# ============================================================
# 补数任务触发
# ============================================================
def trigger_backfill(missing_dates: list[str]) -> dict[str, Any]:
    """触发补数任务（调用 sync_official_data_update）

    当前实现：记录告警并返回指令，实际补数需人工或调度器执行
    sync_official_data_update（它需要 API 凭据，不在此自动注入）。

    返回:
        {
          "requested_dates": [...],
          "action": "manual_required" / "scheduled",
          "alert_id": "...",
        }
    """
    if not missing_dates:
        return {"requested_dates": [], "action": "noop", "alert_id": None}

    alert = publish_alert(
        category="data_gap",
        level=LEVEL_WARNING,
        title="检测到数据缺口，需补数",
        detail=f"缺失交易日: {missing_dates[:20]}（共 {len(missing_dates)} 天）",
        metadata={"missing_dates": missing_dates, "action": "backfill_required"},
    )
    return {
        "requested_dates": missing_dates,
        "action": "manual_required",
        "alert_id": alert.get("timestamp"),
    }


async def detect_and_backfill(
    lookback_days: int = DEFAULT_LOOKBACK_DAYS,
) -> dict[str, Any]:
    """检测数据缺口并尝试补数（一体化入口）

    Returns:
        {
          "report": <DataGapReport dict>,
          "backfill": {...},
        }
    """
    report = await detect_data_gaps(lookback_days=lookback_days)
    missing_dates = [g["trade_date"] for g in report.missing_trading_days]

    backfill_result = {"requested_dates": [], "action": "noop", "alert_id": None}
    if missing_dates:
        backfill_result = trigger_backfill(missing_dates)
        # 补数为 manual_required 时，再次发 error 级告警提示人工介入
        if backfill_result.get("action") == "manual_required":
            publish_alert(
                category="data_gap",
                level=LEVEL_ERROR,
                title="补数任务需人工介入",
                detail=(
                    "sync_official_data_update 需要 API 凭据，无法自动补数。"
                    f"请手动执行补数: 缺失日期 {missing_dates[:10]}"
                ),
                metadata={"missing_dates": missing_dates},
            )

    # 缺口字段异常也告警
    if report.null_field_gaps:
        publish_alert(
            category="data_gap",
            level=LEVEL_WARNING,
            title="检测到关键字段 NULL",
            detail=f"共 {len(report.null_field_gaps)} 条字段缺失记录",
            metadata={"null_field_gaps": report.null_field_gaps[:20]},
        )

    return {"report": report.to_dict(), "backfill": backfill_result}


# ============================================================
# CLI 入口
# ============================================================
def _cli() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="数据缺口检测 (T5.3)")
    parser.add_argument("--lookback-days", type=int, default=DEFAULT_LOOKBACK_DAYS)
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    args = parser.parse_args()

    result = asyncio.run(detect_and_backfill(lookback_days=args.lookback_days))
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        rep = result["report"]
        print(f"检测时间: {rep['checked_at']}")
        print(f"检查区间: {rep['date_range']}")
        print(f"交易日数: {rep['summary']['trading_days_checked']}")
        print(f"缺失交易日: {rep['summary']['missing_trading_days_count']}")
        print(f"股票缺口日: {rep['summary']['missing_stock_gaps_count']}")
        print(f"字段缺失: {rep['summary']['null_field_gaps_count']}")
        print(f"整体状态: {rep['summary']['status']}")
        print(f"补数动作: {result['backfill']['action']}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
