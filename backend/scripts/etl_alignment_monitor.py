"""对齐异常告警模块 (T5.3)

职责：
1. 检测数据对齐异常：
   - 本地库与参考股票池的股票列表不一致
   - 本地库最新日期与当前交易日差异超过阈值
   - 特征值异常（如价格<0、成交量为0但价格变动）
2. 异常发现后记录日志 + 可选通知（Redis pub/sub + 状态文件）

设计约束：
- 不连接外部数据库（仅本地库）
- 参考股票池来源：
  * 优先使用 margin_stock_pool（融资融券股票池，本地 Excel/JSON）
  * 或通过环境变量 ETL_REFERENCE_SYMBOLS_PATH 指定
  * 兜底：使用本地库自身历史最大股票集合作为基准
- 检测结果写入监控状态文件（供 /api/etl/status 读取）
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

# 最新日期滞后阈值（交易日）：超过则告警
DEFAULT_MAX_LAG_DAYS = int(os.getenv("ETL_ALIGN_MAX_LAG_DAYS", "1"))
# 价格异常阈值
DEFAULT_MIN_PRICE = float(os.getenv("ETL_ALIGN_MIN_PRICE", "0"))
# 单日涨跌幅超过该比例且成交量为0视为异常
DEFAULT_ZERO_VOLUME_PCT_THRESHOLD = float(os.getenv("ETL_ALIGN_ZERO_VOL_PCT", "0.05"))
# 参考股票池路径（可选）
REFERENCE_SYMBOLS_PATH = os.getenv("ETL_REFERENCE_SYMBOLS_PATH", "")


# ============================================================
# 异常检测结果数据结构
# ============================================================
@dataclass
class AlignmentAnomaly:
    category: str  # symbol_mismatch / date_lag / price_anomaly / zero_volume_anomaly
    level: str  # info / warning / error
    title: str
    detail: str
    metadata: dict[str, Any] = field(default_factory=dict)
    detected_at: str = ""

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


# ============================================================
# 参考股票池加载
# ============================================================
def load_reference_symbols() -> set[str]:
    """加载参考股票池

    优先级：
    1. ETL_REFERENCE_SYMBOLS_PATH 指定的文件（每行一个代码 或 JSON 数组）
    2. margin_stock_pool（融资融券股票池）
    3. 兜底：返回空集合（后续用本地库历史最大集合）
    """
    path = REFERENCE_SYMBOLS_PATH.strip()
    if path:
        p = Path(path)
        if p.exists():
            try:
                content = p.read_text(encoding="utf-8").strip()
                if p.suffix.lower() == ".json" or content.startswith("["):
                    symbols = json.loads(content)
                    return {str(s).strip().upper() for s in symbols if s}
                # 纯文本：每行一个代码
                return {
                    line.strip().upper()
                    for line in content.splitlines()
                    if line.strip()
                }
            except Exception as exc:
                logger.warning("参考股票池加载失败 %s: %s", path, exc)

    # 尝试 margin_stock_pool
    try:
        from backend.shared.margin_stock_pool import normalize_symbol

        # margin_stock_pool 默认从固定 Excel 加载，路径可能在配置中
        pool_path = os.getenv(
            "MARGIN_STOCK_POOL_PATH",
            str(_PROJECT_ROOT / "db" / "margin_pool.xlsx"),
        )
        if Path(pool_path).exists():
            try:
                from backend.shared.margin_stock_pool import MarginStockPoolService

                service = MarginStockPoolService(pool_path)
                snapshot = service.refresh()
                return set(snapshot.symbols)
            except Exception as exc:
                logger.debug("margin_stock_pool 加载失败: %s", exc)
    except Exception:
        pass

    return set()


# ============================================================
# 异常检测核心
# ============================================================
async def _get_latest_trade_date() -> Optional[date]:
    sql = text("SELECT MAX(trade_date) FROM stock_daily_latest")
    try:
        async with get_session(read_only=True) as session:
            result = await session.execute(sql)
            value = result.scalar()
        if value is None:
            return None
        return value if isinstance(value, date) else date.fromisoformat(str(value))
    except Exception as exc:
        logger.error("获取最新交易日失败: %s", exc)
        return None


async def _get_latest_symbols(trade_date: date) -> set[str]:
    sql = text(
        "SELECT DISTINCT symbol FROM stock_daily_latest WHERE trade_date = :td"
    )
    try:
        async with get_session(read_only=True) as session:
            result = await session.execute(sql, {"td": trade_date})
            return {str(r[0]).strip().upper() for r in result.all() if r[0]}
    except Exception as exc:
        logger.error("获取最新交易日股票列表失败: %s", exc)
        return set()


async def _get_historical_max_symbols(lookback_days: int = 30) -> set[str]:
    """获取最近 N 天内出现过的所有股票（作为兜底基准）"""
    end_date = datetime.now(timezone.utc).date()
    start_date = end_date - timedelta(days=lookback_days)
    sql = text(
        """
        SELECT DISTINCT symbol FROM stock_daily_latest
        WHERE trade_date BETWEEN :start AND :end
        """
    )
    try:
        async with get_session(read_only=True) as session:
            result = await session.execute(
                sql, {"start": start_date, "end": end_date}
            )
            return {str(r[0]).strip().upper() for r in result.all() if r[0]}
    except Exception as exc:
        logger.error("获取历史股票集合失败: %s", exc)
        return set()


async def _detect_symbol_mismatch(
    latest_date: date, latest_symbols: set[str]
) -> Optional[AlignmentAnomaly]:
    """检测股票列表与参考池不一致"""
    reference = load_reference_symbols()
    # 兜底：参考池为空时，用历史最大集合
    if not reference:
        reference = await _get_historical_max_symbols()
        if not reference:
            return None

    missing_from_latest = reference - latest_symbols
    extra_in_latest = latest_symbols - reference

    # 仅当差异显著时才告警（避免新股/退市股的噪声）
    if not missing_from_latest and not extra_in_latest:
        return None

    # 差异比例阈值：超过 5% 才告警
    ref_count = len(reference)
    missing_ratio = len(missing_from_latest) / ref_count if ref_count else 0.0
    if missing_ratio < 0.05 and len(extra_in_latest) < 50:
        return None

    level = LEVEL_ERROR if missing_ratio >= 0.10 else LEVEL_WARNING
    return AlignmentAnomaly(
        category="symbol_mismatch",
        level=level,
        title="股票列表与参考池不一致",
        detail=(
            f"最新交易日 {latest_date} 共 {len(latest_symbols)} 只股票，"
            f"参考池 {ref_count} 只；缺失 {len(missing_from_latest)} 只，"
            f"多余 {len(extra_in_latest)} 只"
        ),
        metadata={
            "trade_date": latest_date.isoformat(),
            "latest_count": len(latest_symbols),
            "reference_count": ref_count,
            "missing_count": len(missing_from_latest),
            "extra_count": len(extra_in_latest),
            "missing_sample": sorted(missing_from_latest)[:20],
            "extra_sample": sorted(extra_in_latest)[:20],
        },
        detected_at=datetime.now(timezone.utc).isoformat(),
    )


async def _detect_date_lag(latest_date: Optional[date]) -> Optional[AlignmentAnomaly]:
    """检测最新日期与当前交易日差异超过阈值"""
    if latest_date is None:
        return AlignmentAnomaly(
            category="date_lag",
            level=LEVEL_ERROR,
            title="stock_daily_latest 表为空",
            detail="无法获取最新交易日，表可能为空或不可访问",
            metadata={"latest_date": None},
            detected_at=datetime.now(timezone.utc).isoformat(),
        )

    today = datetime.now(timezone.utc).date()
    # 计算交易日滞后（用工作日近似，避免依赖交易日历表）
    lag_days = 0
    cur = latest_date
    while cur < today:
        cur += timedelta(days=1)
        if cur.weekday() < 5:  # 工作日
            lag_days += 1
        if lag_days > 30:  # 防止极端情况死循环
            break

    if lag_days <= DEFAULT_MAX_LAG_DAYS:
        return None

    level = LEVEL_ERROR if lag_days >= 3 else LEVEL_WARNING
    return AlignmentAnomaly(
        category="date_lag",
        level=level,
        title="本地数据最新日期滞后超过阈值",
        detail=(
            f"最新交易日 {latest_date} 滞后当前约 {lag_days} 个交易日"
            f"（阈值 {DEFAULT_MAX_LAG_DAYS}）"
        ),
        metadata={
            "latest_date": latest_date.isoformat(),
            "today": today.isoformat(),
            "lag_trading_days": lag_days,
            "threshold": DEFAULT_MAX_LAG_DAYS,
        },
        detected_at=datetime.now(timezone.utc).isoformat(),
    )


async def _detect_price_anomaly(latest_date: date) -> list[AlignmentAnomaly]:
    """检测价格异常（价格<0、成交量为0但价格变动）"""
    anomalies: list[AlignmentAnomaly] = []

    # 价格 <= 0 异常
    negative_price_sql = text(
        """
        SELECT symbol, open, high, low, close
        FROM stock_daily_latest
        WHERE trade_date = :td
          AND (open < 0 OR high < 0 OR low < 0 OR close < 0
               OR open = 0 OR high = 0 OR low = 0 OR close = 0)
        LIMIT 100
        """
    )
    try:
        async with get_session(read_only=True) as session:
            result = await session.execute(negative_price_sql, {"td": latest_date})
            rows = result.all()
    except Exception as exc:
        logger.error("价格异常检测失败: %s", exc)
        rows = []

    if rows:
        samples = [
            {
                "symbol": str(r[0]),
                "open": float(r[1]) if r[1] is not None else None,
                "high": float(r[2]) if r[2] is not None else None,
                "low": float(r[3]) if r[3] is not None else None,
                "close": float(r[4]) if r[4] is not None else None,
            }
            for r in rows[:20]
        ]
        anomalies.append(
            AlignmentAnomaly(
                category="price_anomaly",
                level=LEVEL_ERROR,
                title="检测到非正价格",
                detail=f"最新交易日 {latest_date} 共 {len(rows)} 条价格<=0记录",
                metadata={
                    "trade_date": latest_date.isoformat(),
                    "count": len(rows),
                    "samples": samples,
                },
                detected_at=datetime.now(timezone.utc).isoformat(),
            )
        )

    # 成交量为0但价格变动异常（需 pct_change 字段）
    zero_vol_sql = text(
        """
        SELECT symbol, close, pct_change, volume
        FROM stock_daily_latest
        WHERE trade_date = :td
          AND COALESCE(volume, 0) = 0
          AND COALESCE(pct_change, 0) != 0
        LIMIT 100
        """
    )
    try:
        async with get_session(read_only=True) as session:
            result = await session.execute(zero_vol_sql, {"td": latest_date})
            rows = result.all()
    except Exception as exc:
        # pct_change 字段可能不存在，降级跳过
        logger.debug("零成交量异常检测失败（可能字段不存在）: %s", exc)
        rows = []

    if rows:
        samples = [
            {
                "symbol": str(r[0]),
                "close": float(r[1]) if r[1] is not None else None,
                "pct_change": float(r[2]) if r[2] is not None else None,
                "volume": 0,
            }
            for r in rows[:20]
        ]
        anomalies.append(
            AlignmentAnomaly(
                category="zero_volume_anomaly",
                level=LEVEL_WARNING,
                title="成交量为0但价格变动",
                detail=f"最新交易日 {latest_date} 共 {len(rows)} 条零成交量但有价格变动记录",
                metadata={
                    "trade_date": latest_date.isoformat(),
                    "count": len(rows),
                    "samples": samples,
                },
                detected_at=datetime.now(timezone.utc).isoformat(),
            )
        )

    return anomalies


# ============================================================
# 主检测入口
# ============================================================
async def detect_alignment_anomalies() -> dict[str, Any]:
    """检测全部对齐异常，返回异常列表 + 写入状态文件"""
    checked_at = datetime.now(timezone.utc).isoformat()
    anomalies: list[AlignmentAnomaly] = []

    latest_date = await _get_latest_trade_date()

    # 1. 日期滞后
    lag_anomaly = await _detect_date_lag(latest_date)
    if lag_anomaly:
        anomalies.append(lag_anomaly)

    if latest_date is not None:
        latest_symbols = await _get_latest_symbols(latest_date)

        # 2. 股票列表对齐
        mismatch = await _detect_symbol_mismatch(latest_date, latest_symbols)
        if mismatch:
            anomalies.append(mismatch)

        # 3. 价格异常
        anomalies.extend(await _detect_price_anomaly(latest_date))

    # 序列化
    anomaly_dicts = [a.to_dict() for a in anomalies]

    # 写入监控状态文件
    try:
        get_monitor_state_store().update_alignment_anomalies(anomaly_dicts)
    except Exception:  # pragma: no cover
        logger.exception("对齐异常写入监控状态失败")

    # 发布告警
    for anomaly in anomalies:
        publish_alert(
            category=f"alignment:{anomaly.category}",
            level=anomaly.level,
            title=anomaly.title,
            detail=anomaly.detail,
            metadata=anomaly.metadata,
        )

    summary = {
        "checked_at": checked_at,
        "total_anomalies": len(anomalies),
        "error_count": sum(1 for a in anomalies if a.level == LEVEL_ERROR),
        "warning_count": sum(1 for a in anomalies if a.level == LEVEL_WARNING),
        "categories": list({a.category for a in anomalies}),
        "status": "ok" if not anomalies else (
            "critical" if any(a.level == LEVEL_ERROR for a in anomalies) else "warning"
        ),
    }

    return {"anomalies": anomaly_dicts, "summary": summary}


# ============================================================
# CLI 入口
# ============================================================
def _cli() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="对齐异常告警 (T5.3)")
    parser.add_argument("--json", action="store_true", help="以 JSON 输出")
    args = parser.parse_args()

    result = asyncio.run(detect_alignment_anomalies())
    if args.json:
        print(json.dumps(result, ensure_ascii=False, indent=2))
    else:
        print(f"检测时间: {result['summary']['checked_at']}")
        print(f"异常总数: {result['summary']['total_anomalies']}")
        print(f"错误数: {result['summary']['error_count']}")
        print(f"警告数: {result['summary']['warning_count']}")
        print(f"整体状态: {result['summary']['status']}")
        for a in result["anomalies"]:
            print(f"  [{a['level']}] {a['category']}: {a['title']}")
    return 0


if __name__ == "__main__":
    sys.exit(_cli())
