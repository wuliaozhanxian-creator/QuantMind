"""统一就绪探针（readiness probe）工具。

为各服务的 ``/readiness`` 端点提供标准化的依赖探测与响应构建逻辑，与
``/health``（存活探针）形成语义分离：

- ``/health``：存活探针（liveness），仅检查进程是否活着，恒返回 200。
- ``/readiness``：就绪探针（readiness），实时探测下游依赖连通性，依赖不可用返回 503。

约束：
- 探测超时统一 2s（``PROBE_TIMEOUT_SECONDS``），不得无限等待。
- 探测成功返回 200 + ``{"status": "ready", "checks": {"db": "ok", "redis": "ok"}}``。
- 探测失败返回 503 + ``{"status": "not_ready", "checks": {"db": "fail", "redis": "ok"}}``。
- 仅探测本地 DB 与 Redis，不连接任何外部数据库。
"""

from __future__ import annotations

import asyncio
import logging
from collections.abc import Awaitable, Callable
from typing import Any

from fastapi.responses import JSONResponse

logger = logging.getLogger(__name__)

# 探测超时硬约束：2 秒
PROBE_TIMEOUT_SECONDS: float = 2.0

async def probe_async(
    name: str,
    factory: Callable[[], Awaitable[Any]],
    timeout: float = PROBE_TIMEOUT_SECONDS,
) -> str:
    """执行异步探测，返回 ``"ok"`` 或 ``"fail"``。

    Args:
        name: 探测项名称（用于日志）。
        factory: 返回协程的可调用对象，协程内执行实际探测（如 ``SELECT 1``）。
        timeout: 超时秒数，默认 2s。
    """
    try:
        await asyncio.wait_for(factory(), timeout=timeout)
        return "ok"
    except asyncio.TimeoutError:
        logger.warning("readiness probe '%s' timed out after %ss", name, timeout)
        return "fail"
    except Exception as exc:  # noqa: BLE001 - 探测失败需兜底为 fail
        logger.warning("readiness probe '%s' failed: %s", name, exc)
        return "fail"

async def probe_sync(
    name: str,
    func: Callable[[], Any],
    timeout: float = PROBE_TIMEOUT_SECONDS,
) -> str:
    """执行同步探测（在线程池中运行），返回 ``"ok"`` 或 ``"fail"``。

    Args:
        name: 探测项名称（用于日志）。
        func: 同步可调用对象，执行实际探测（如 ``redis.ping()``）。
        timeout: 超时秒数，默认 2s。
    """
    try:
        await asyncio.wait_for(asyncio.to_thread(func), timeout=timeout)
        return "ok"
    except asyncio.TimeoutError:
        logger.warning("readiness probe '%s' timed out after %ss", name, timeout)
        return "fail"
    except Exception as exc:  # noqa: BLE001 - 探测失败需兜底为 fail
        logger.warning("readiness probe '%s' failed: %s", name, exc)
        return "fail"

def build_readiness_response(checks: dict[str, str]) -> JSONResponse:
    """根据探测结果构建标准化 ``/readiness`` 响应。

    所有探测项均为 ``"ok"`` 时返回 200，否则返回 503。
    """
    all_ready = bool(checks) and all(v == "ok" for v in checks.values())
    return JSONResponse(
        status_code=200 if all_ready else 503,
        content={
            "status": "ready" if all_ready else "not_ready",
            "checks": checks,
        },
    )
