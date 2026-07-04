"""ETL 告警与状态共享模块 (T5.3)

职责：
- 统一发布 ETL 告警（日志 + Redis pub/sub + 状态文件）
- 维护 ETL 监控状态快照（供 /api/etl/status 端点读取）

设计约束：
- 不连接外部数据库（仅本地库 + 本地 JSON）
- Redis pub/sub 为 best-effort，失败仅记日志不阻断主流程
- 状态文件原子写入（tmp + replace）
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

logger = logging.getLogger(__name__)

_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 监控状态文件路径（与 etl_scheduler 状态文件同目录）
DEFAULT_MONITOR_STATE_PATH = _PROJECT_ROOT / "logs" / "etl_monitor_state.json"

# Redis pub/sub 频道
ETL_ALERT_CHANNEL = os.getenv("ETL_ALERT_CHANNEL", "etl:alerts")

# 告警级别
LEVEL_INFO = "info"
LEVEL_WARNING = "warning"
LEVEL_ERROR = "error"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class MonitorStateStore:
    """ETL 监控状态持久化（本地 JSON）

    结构：
    {
      "data_gaps": {...},        # 数据缺口检测结果
      "alignment_anomalies": [...],  # 对齐异常列表
      "alerts": [...],           # 最近告警记录
      "updated_at": "<ISO8601>"
    }
    """

    _MAX_ALERTS = int(os.getenv("ETL_MAX_ALERTS", "100"))

    def __init__(self, path: str | Path | None = None):
        self.path = Path(path) if path else DEFAULT_MONITOR_STATE_PATH
        self._lock = threading.Lock()
        self._cache: dict[str, Any] = {
            "data_gaps": {},
            "alignment_anomalies": [],
            "alerts": [],
            "updated_at": None,
        }
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            self._cache = json.loads(self.path.read_text(encoding="utf-8"))
            for key in ("data_gaps", "alignment_anomalies", "alerts"):
                self._cache.setdefault(key, [] if key != "data_gaps" else {})
        except Exception as exc:
            logger.warning("ETL 监控状态文件加载失败，重置: %s (%s)", self.path, exc)
            self._cache = {
                "data_gaps": {},
                "alignment_anomalies": [],
                "alerts": [],
                "updated_at": None,
            }

    def _flush(self) -> None:
        self._cache["updated_at"] = _now_iso()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self.path)
        except Exception as exc:
            logger.error("ETL 监控状态文件写入失败: %s (%s)", self.path, exc)

    def update_data_gaps(self, gaps: dict[str, Any]) -> None:
        with self._lock:
            self._cache["data_gaps"] = gaps
            self._flush()

    def update_alignment_anomalies(self, anomalies: list[dict[str, Any]]) -> None:
        with self._lock:
            self._cache["alignment_anomalies"] = anomalies
            self._flush()

    def add_alert(self, alert: dict[str, Any]) -> None:
        with self._lock:
            self._cache["alerts"].insert(0, alert)
            self._cache["alerts"] = self._cache["alerts"][: self._MAX_ALERTS]
            self._flush()

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            return json.loads(json.dumps(self._cache))


# 全局单例（懒加载）
_global_store: Optional[MonitorStateStore] = None
_global_store_lock = threading.Lock()


def get_monitor_state_store() -> MonitorStateStore:
    global _global_store
    if _global_store is None:
        with _global_store_lock:
            if _global_store is None:
                _global_store = MonitorStateStore()
    return _global_store


def reset_monitor_state_store(path: str | Path | None = None) -> MonitorStateStore:
    """重置全局单例（测试用）"""
    global _global_store
    with _global_store_lock:
        _global_store = MonitorStateStore(path)
    return _global_store


# ============================================================
# Redis pub/sub 告警发布（best-effort）
# ============================================================
def _build_redis_client():
    """构建本地 Redis 客户端（best-effort，失败返回 None）"""
    try:
        import redis  # type: ignore
    except Exception:
        return None
    try:
        password = os.getenv("REDIS_PASSWORD") or None
        return redis.Redis(
            host=os.getenv("REDIS_HOST", "localhost"),
            port=int(os.getenv("REDIS_PORT", "6379")),
            password=password,
            db=int(os.getenv("REDIS_DB", "0")),
            decode_responses=True,
            socket_connect_timeout=2,
            socket_timeout=3,
        )
    except Exception as exc:
        logger.debug("ETL 告警 Redis 客户端初始化失败: %s", exc)
        return None


def publish_alert(
    *,
    category: str,
    level: str = LEVEL_WARNING,
    title: str,
    detail: str = "",
    metadata: Optional[dict[str, Any]] = None,
) -> dict[str, Any]:
    """发布一条 ETL 告警

    - 写入监控状态文件（供 /api/etl/status 读取）
    - 尝试通过 Redis pub/sub 广播（失败仅记日志）
    - 记录结构化日志

    Args:
        category: 告警类别，如 "data_gap" / "alignment" / "etl_failure"
        level: info / warning / error
        title: 简短标题
        detail: 详细描述
        metadata: 附加结构化数据
    """
    alert = {
        "category": str(category or "unknown"),
        "level": level if level in (LEVEL_INFO, LEVEL_WARNING, LEVEL_ERROR) else LEVEL_WARNING,
        "title": str(title or "")[:200],
        "detail": str(detail or "")[:4000],
        "metadata": metadata or {},
        "timestamp": _now_iso(),
    }

    # 1. 写入状态文件
    try:
        get_monitor_state_store().add_alert(alert)
    except Exception:  # pragma: no cover
        logger.exception("ETL 告警写入状态文件失败")

    # 2. Redis pub/sub（best-effort）
    client = _build_redis_client()
    if client is not None:
        try:
            client.publish(ETL_ALERT_CHANNEL, json.dumps(alert, ensure_ascii=False))
        except Exception as exc:
            logger.warning("ETL 告警 Redis pub/sub 失败: %s", exc)
        finally:
            try:
                client.close()
            except Exception:
                pass

    # 3. 结构化日志
    log_msg = "ETL告警 [%s/%s] %s: %s"
    if level == LEVEL_ERROR:
        logger.error(log_msg, category, level, title, detail)
    elif level == LEVEL_WARNING:
        logger.warning(log_msg, category, level, title, detail)
    else:
        logger.info(log_msg, category, level, title, detail)

    return alert
