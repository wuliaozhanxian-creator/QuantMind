"""ETL 监控端点 (T5.3)

提供 ETL 调度状态、数据缺口、对齐异常的只读监控接口。

端点：
- GET /api/etl/status        综合状态：调度器 + 数据缺口 + 对齐异常 + 最近告警
- GET /api/etl/tasks         已注册 ETL 任务列表 + 各任务最近执行状态
- GET /api/etl/data-gaps     最近一次数据缺口检测报告
- GET /api/etl/anomalies     最近一次对齐异常检测报告
- GET /api/etl/alerts        最近告警列表
- POST /api/etl/run/{name}   手动触发指定 ETL 任务（含依赖）

设计要点：
- 状态读取自本地 JSON 状态文件（与调度器进程解耦，避免跨进程内存共享）
- 触发任务通过 ETLScheduler 实例（进程内）或 subprocess（进程外）执行
- 只读端点不依赖外部数据库，仅读本地状态文件
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from fastapi import APIRouter, HTTPException, Query

from backend.scripts.etl_alerts import get_monitor_state_store
from backend.scripts.etl_scheduler import (
    STATUS_FAILED,
    STATUS_SUCCESS,
    build_default_scheduler,
)
from backend.shared.logging_config import get_logger

logger = get_logger(__name__)

router = APIRouter(prefix="/api/etl", tags=["ETL-Monitoring"])

# 进程内调度器单例（懒加载；API 服务通常不常驻调度器，但 run 端点需要它）
_scheduler_instance = None


def _get_scheduler():
    global _scheduler_instance
    if _scheduler_instance is None:
        _scheduler_instance = build_default_scheduler()
    return _scheduler_instance


def _safe_load_json(path) -> Optional[dict[str, Any]]:
    """安全加载 JSON 文件，失败返回 None"""
    try:
        from pathlib import Path

        p = Path(path)
        if not p.exists():
            return None
        import json

        return json.loads(p.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("加载 ETL 状态文件失败 %s: %s", path, exc)
        return None


# ============================================================
# 综合状态端点
# ============================================================
@router.get("/status")
async def etl_status():
    """返回 ETL 综合监控状态

    聚合：
    - 调度器任务元信息 + 最近执行状态
    - 最近一次数据缺口检测报告
    - 最近一次对齐异常检测报告
    - 最近告警列表
    """
    scheduler = _get_scheduler()
    scheduler_snap = scheduler.status_snapshot()

    monitor_snap = get_monitor_state_store().snapshot()

    # 数据缺口摘要
    data_gaps = monitor_snap.get("data_gaps", {}) or {}
    gap_summary = data_gaps.get("summary", {}) if isinstance(data_gaps, dict) else {}

    # 对齐异常摘要
    anomalies = monitor_snap.get("alignment_anomalies", []) or []
    error_anomalies = [a for a in anomalies if a.get("level") == "error"]

    # 最近告警
    alerts = monitor_snap.get("alerts", []) or []

    # 任务最近执行状态摘要
    task_states = scheduler_snap.get("task_states", {}) or {}
    task_summary = {
        "total_tasks": len(scheduler_snap.get("tasks", {})),
        "tasks_with_last_run": sum(
            1 for v in task_states.values() if v.get("last_run")
        ),
        "last_failed_tasks": [
            name
            for name, v in task_states.items()
            if v.get("last_run", {}) and v["last_run"].get("status") == STATUS_FAILED
        ],
    }

    # 整体健康度
    overall_status = "healthy"
    if error_anomalies or task_summary["last_failed_tasks"]:
        overall_status = "critical"
    elif gap_summary.get("status") in ("warning", "critical") or anomalies:
        overall_status = "warning"
    elif gap_summary.get("status") == "ok" and not anomalies:
        overall_status = "healthy"

    return {
        "overall_status": overall_status,
        "checked_at": monitor_snap.get("updated_at"),
        "scheduler": {
            "running": scheduler_snap.get("scheduler_running", False),
            "task_summary": task_summary,
        },
        "data_gaps_summary": gap_summary,
        "alignment_anomalies_summary": {
            "total": len(anomalies),
            "errors": len(error_anomalies),
            "status": "critical" if error_anomalies else ("warning" if anomalies else "ok"),
        },
        "recent_alerts": alerts[:10],
        "recent_alerts_count": len(alerts),
    }


# ============================================================
# 任务列表端点
# ============================================================
@router.get("/tasks")
async def etl_tasks():
    """返回已注册 ETL 任务列表 + 各任务最近执行状态"""
    scheduler = _get_scheduler()
    snap = scheduler.status_snapshot()

    tasks_meta = snap.get("tasks", {}) or {}
    task_states = snap.get("task_states", {}) or {}

    result = []
    for name, meta in tasks_meta.items():
        state = task_states.get(name, {})
        last_run = state.get("last_run")
        history = state.get("history", []) or []
        result.append(
            {
                "name": name,
                "cron": meta.get("cron"),
                "enabled": meta.get("enabled", True),
                "depends_on": meta.get("depends_on", []),
                "description": meta.get("description", ""),
                "last_run": last_run,
                "recent_history": history[:5],
                "run_count": len(history),
                "last_status": last_run.get("status") if last_run else None,
                "last_duration_seconds": last_run.get("duration_seconds") if last_run else None,
            }
        )

    return {
        "tasks": result,
        "total": len(result),
        "scheduler_running": snap.get("scheduler_running", False),
    }


# ============================================================
# 数据缺口端点
# ============================================================
@router.get("/data-gaps")
async def etl_data_gaps():
    """返回最近一次数据缺口检测报告"""
    monitor_snap = get_monitor_state_store().snapshot()
    data_gaps = monitor_snap.get("data_gaps", {}) or {}
    return {
        "report": data_gaps,
        "updated_at": monitor_snap.get("updated_at"),
    }


# ============================================================
# 对齐异常端点
# ============================================================
@router.get("/anomalies")
async def etl_anomalies():
    """返回最近一次对齐异常检测报告"""
    monitor_snap = get_monitor_state_store().snapshot()
    anomalies = monitor_snap.get("alignment_anomalies", []) or []
    return {
        "anomalies": anomalies,
        "total": len(anomalies),
        "updated_at": monitor_snap.get("updated_at"),
    }


# ============================================================
# 告警列表端点
# ============================================================
@router.get("/alerts")
async def etl_alerts(
    limit: int = Query(50, ge=1, le=500, description="返回告警数量上限"),
    category: Optional[str] = Query(None, description="按类别过滤"),
    level: Optional[str] = Query(None, description="按级别过滤"),
):
    """返回最近告警列表"""
    monitor_snap = get_monitor_state_store().snapshot()
    alerts = monitor_snap.get("alerts", []) or []

    filtered = alerts
    if category:
        filtered = [a for a in filtered if a.get("category", "").startswith(category)]
    if level:
        filtered = [a for a in filtered if a.get("level") == level]

    return {
        "alerts": filtered[:limit],
        "total": len(filtered),
        "total_all": len(alerts),
    }


# ============================================================
# 手动触发任务端点
# ============================================================
@router.post("/run/{task_name}")
async def etl_run_task(task_name: str):
    """手动触发指定 ETL 任务（含依赖，按拓扑序执行）

    注意：此端点在 API 服务进程内同步执行任务，长耗时任务可能阻塞请求。
    建议仅用于短任务或调试，生产环境应通过调度器 cron 触发。
    """
    scheduler = _get_scheduler()
    spec = scheduler.get_task(task_name)
    if spec is None:
        raise HTTPException(
            status_code=404,
            detail=f"ETL 任务不存在: {task_name}。可用任务见 /api/etl/tasks",
        )

    if not spec.enabled:
        raise HTTPException(
            status_code=409,
            detail=f"ETL 任务已禁用: {task_name}",
        )

    try:
        record = scheduler.run_now(task_name, triggered_by="api")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"任务依赖解析失败: {exc}") from exc
    except Exception as exc:
        logger.exception("ETL 任务触发失败: %s", task_name)
        raise HTTPException(status_code=500, detail=f"任务执行失败: {exc}") from exc

    return {
        "task_name": record.task_name,
        "status": record.status,
        "started_at": record.started_at,
        "finished_at": record.finished_at,
        "duration_seconds": record.duration_seconds,
        "return_code": record.return_code,
        "error": record.error,
        "output": (record.output or "")[-2000:] if record.output else None,
    }


@router.post("/run-all")
async def etl_run_all():
    """手动触发全部已注册 ETL 任务（按依赖拓扑序）"""
    scheduler = _get_scheduler()
    try:
        results = scheduler.run_all(triggered_by="api")
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=f"任务依赖解析失败: {exc}") from exc
    except Exception as exc:
        logger.exception("ETL 全量任务触发失败")
        raise HTTPException(status_code=500, detail=f"任务执行失败: {exc}") from exc

    return {
        "results": {k: v.to_dict() for k, v in results.items()},
        "total": len(results),
        "failed": sum(1 for r in results.values() if r.status == STATUS_FAILED),
        "succeeded": sum(1 for r in results.values() if r.status == STATUS_SUCCESS),
        "skipped": sum(1 for r in results.values() if r.status == "skipped"),
    }
