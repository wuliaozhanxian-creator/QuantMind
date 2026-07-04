import asyncio
import importlib
import os
import sys
from contextlib import asynccontextmanager
from typing import Optional
from urllib.parse import quote_plus

from fastapi import FastAPI, Request, status
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from sqlalchemy import text

from backend.shared.auth import AuthManager, get_internal_call_secret
from backend.shared.config_manager import init_unified_config
from backend.shared.cors import resolve_cors_origins
from backend.shared.database_pool import init_default_databases as init_sync_db_pool
from backend.shared.error_contract import install_error_contract_handlers
from backend.shared.logging_config import get_logger
from backend.shared.openapi_utils import quantmind_generate_unique_id
from backend.shared.readiness import (
    build_readiness_response,
    probe_sync,
)
from backend.shared.request_id import install_request_id_middleware
from backend.shared.request_logging import install_access_log_middleware
from backend.shared.service_health_metrics import (
    build_metrics_response,
    set_service_health,
)

logger = get_logger(__name__)


# 兼容 qlib_app 内部裸导入路径（from qlib_app.*）
if "qlib_app" not in sys.modules:
    sys.modules["qlib_app"] = importlib.import_module("backend.services.engine.qlib_app")


@asynccontextmanager
async def lifespan(app: FastAPI):
    app.state.startup_healthy = True
    try:
        await init_unified_config(service_name="quantmind-engine")
        logger.info("✅ QuantMind Engine Unified Config Loaded")
    except Exception as e:
        app.state.startup_healthy = False
        logger.error(f"❌ Unified config load failed: {e}")

    try:
        init_sync_db_pool(pool_size=20, max_overflow=10)
    except Exception as e:
        app.state.startup_healthy = False
        logger.error(f"❌ Sync database pool init failed: {e}")

    try:
        await _bootstrap_qlib_runtime()
        logger.info("✅ Qlib runtime bootstrap completed")
    except Exception as e:
        app.state.startup_healthy = False
        logger.error(f"❌ Qlib runtime bootstrap failed: {e}")

    try:
        from backend.shared.model_registry import model_registry_service

        await model_registry_service.ensure_tables()
    except Exception as e:
        app.state.startup_healthy = False
        logger.error(f"❌ Model registry table ensure failed: {e}")

    set_service_health("quantmind-engine", bool(getattr(app.state, "startup_healthy", True)))

    # 启动 VectorizedMatcher（受 ENABLE_VECTORIZED_MATCHER 环境变量控制）
    vm_task: asyncio.Task | None = None
    try:
        from backend.services.trade.services.vectorized_matcher import VectorizedMatcher
        _vm = VectorizedMatcher()
        vm_task = asyncio.create_task(_vm.start(), name="vectorized_matcher")
        app.state.vectorized_matcher = _vm
        logger.info("✅ VectorizedMatcher task created (active if ENABLE_VECTORIZED_MATCHER=true)")
    except Exception as e:
        logger.warning(f"⚠️ VectorizedMatcher startup skipped: {e}")

    # --- 此处 Yield，之后代码在启动后运行 ---
    yield

    # 启动预热向量解析/字段检索（2026-05-03：暂时关闭强制预热以加快启动速度）
    warmup_enabled = os.getenv("AI_STRATEGY_WARMUP", "false").strip().lower() not in ("0", "false", "no", "off")
    if warmup_enabled:
        try:
            from backend.services.engine.ai_strategy.services.startup_health import run_startup_health_checks
            await run_startup_health_checks()
            logger.info("✅ AI Strategy Warmup completed successfully")
        except Exception as e:
            app.state.startup_healthy = False
            logger.error(f"❌ AI Strategy Warmup failed: {e}")
    else:
        logger.info("AI Strategy Warmup disabled by env")

    # --- 停止逻辑 ---
    if vm_task and not vm_task.done():
        try:
            app.state.vectorized_matcher.stop()
            await asyncio.wait_for(vm_task, timeout=5.0)
        except Exception:
            vm_task.cancel()
    logger.info("🔚 QuantMind Engine shutdown complete")


def _run_ai_strategy_warmup_sync() -> None:
    from backend.services.engine.ai_strategy.services.selection.schema_retriever import (
        get_schema_retriever,
    )
    from backend.services.engine.ai_strategy.services.selection.vector_parser import (
        get_strategy_vector_parser,
    )

    asyncio.run(get_strategy_vector_parser())
    asyncio.run(get_schema_retriever())


async def _bootstrap_qlib_runtime() -> None:
    """在 engine 生命周期内补齐 qlib 初始化与回测表检查。"""
    from backend.services.engine.qlib_app import get_qlib_service
    from backend.services.engine.qlib_app.services.backtest_persistence import (
        BacktestPersistence,
    )
    from backend.services.engine.qlib_app.services.optimization_persistence import (
        OptimizationPersistence,
    )

    qlib_service = get_qlib_service()
    await asyncio.to_thread(qlib_service.initialize)
    await BacktestPersistence().ensure_tables()
    await OptimizationPersistence().ensure_tables()


app = FastAPI(
    title="QuantMind Computational Engine",
    version="2.0.0",
    description="收敛后的计算引擎服务（整合了 AI 策略生成、模型推理、回测等模块）",
    lifespan=lifespan,
    generate_unique_id_function=quantmind_generate_unique_id,
)

install_request_id_middleware(app)
install_error_contract_handlers(app)
install_access_log_middleware(app, service_name="quantmind-engine")


@app.middleware("http")
async def auth_middleware(request: Request, call_next):
    """
    统一认证中间件：支持 JWT 令牌 (Bearer) 校验。

    安全变更 (T6.2): X-Internal-Call header 仅对 /api/v1/internal/* 路径生效，
    不再作为业务路由的认证替代。所有 /api/v1/* 业务路由必须通过有效 JWT。
    """
    path = request.url.path
    method = request.method.upper()

    # 1. 尝试从 JWT 获取用户身份
    user_id = None
    tenant_id = "default"
    auth_header = request.headers.get("Authorization")
    if auth_header and auth_header.startswith("Bearer "):
        try:
            token = auth_header.split(" ")[1]
            payload = AuthManager().verify_token(token)
            user_id = str(payload.get("sub") or payload.get("user_id") or "")
            tenant_id = str(payload.get("tenant_id") or "default")
        except Exception:
            pass

    # 2. X-Internal-Call 仅对专用内部路径生效（T6.2 收紧）
    internal_secret = request.headers.get("X-Internal-Call")
    is_internal_path = path.startswith("/api/v1/internal/")
    if internal_secret and is_internal_path:
        try:
            expected_secret = get_internal_call_secret()
            if internal_secret == expected_secret:
                user_id = request.headers.get("X-User-Id") or user_id or "internal"
                tenant_id = request.headers.get("X-Tenant-Id") or tenant_id
        except RuntimeError:
            logger.warning("INTERNAL_CALL_SECRET 未配置，内部路径认证失败")

    # 3. 所有 /api/v1/* 业务路由必须通过有效用户身份（OPTIONS 放行）
    if method != "OPTIONS" and path.startswith("/api/v1/"):
        if not user_id:
            return JSONResponse(
                status_code=status.HTTP_401_UNAUTHORIZED,
                content={"detail": "Authentication required (valid JWT or internal secret for internal paths)"},
            )

    if user_id:
        request.state.user = {"user_id": user_id, "tenant_id": tenant_id, "sub": user_id}

    response = await call_next(request)
    return response


app.add_middleware(
    CORSMiddleware,
    allow_origins=resolve_cors_origins(logger=logger),
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# 1. AI 策略生成
try:
    from backend.services.engine.ai_strategy.api.routes.strategy_backtest_loop import router as strategy_loop_router
    from backend.services.engine.ai_strategy.api.v1.routes import router as ai_strat_router
    from backend.services.engine.ai_strategy.api.v1.wizard import router as wizard_router

    app.include_router(ai_strat_router, prefix="/api/v1", tags=["AI Strategy"])
    app.include_router(wizard_router, prefix="/api/v1", tags=["Strategy Wizard"])
    app.include_router(strategy_loop_router, prefix="/api/v1", tags=["Strategy Loop"])
except ImportError as e:
    logger.error(f"❌ Failed to load AI Strategy routers: {e}")


# 2. 模型推理
try:
    from backend.services.engine.routers.inference import router as inference_router

    app.include_router(inference_router, prefix="/api/v1/inference", tags=["AI Inference"])
except ImportError as e:
    logger.error(f"❌ Failed to load AI Inference router: {e}")

# 3. 闭环编排
try:
    from backend.services.engine.routers.pipeline import router as pipeline_router

    app.include_router(pipeline_router, prefix="/api/v1/pipeline", tags=["Pipeline"])
except ImportError as e:
    logger.error(f"❌ Failed to load Pipeline router: {e}")

# 3.1 实盘E2E最小契约接口
try:
    from backend.services.engine.routers.realtime_contract import router as realtime_contract_router

    app.include_router(realtime_contract_router, prefix="/api/v1")
except ImportError as e:
    logger.error(f"❌ Failed to load Realtime Contract router: {e}")

# 4. 量化回测
try:
    from backend.services.engine.qlib_app.api.backtest import router as backtest_router

    app.include_router(backtest_router, prefix="/api/v1", tags=["Qlib Backtest"])
except ImportError as e:
    logger.error(f"❌ Failed to load Qlib Backtest router: {e}")

# 4.1 高级分析
try:
    from backend.services.engine.qlib_app.api.analysis import router as analysis_router

    # analysis_router 自带 /api/v1/analysis 前缀
    app.include_router(analysis_router, tags=["Qlib Analysis"])
except ImportError as e:
    logger.error(f"❌ Failed to load Qlib Analysis router: {e}")

# 5. 用户策略管理
try:
    from backend.services.engine.qlib_app.api.user_strategies import router as strategies_router

    app.include_router(strategies_router, prefix="/api/v1/strategies", tags=["Strategies"])
except ImportError as e:
    logger.error(f"❌ Failed to load Strategies router: {e}")

# 5.1 管理员策略模板管理
try:
    from backend.services.engine.qlib_app.api.admin_templates import router as admin_templates_router

    app.include_router(
        admin_templates_router,
        prefix="/api/v1/admin/strategy-templates",
        tags=["Admin-StrategyTemplates"],
    )
    logger.info("✅ Admin Strategy Templates router loaded")
except ImportError as e:
    logger.error(f"❌ Failed to load Admin Strategy Templates router: {e}")

# 6. 股票查询与智能选股
try:
    from backend.services.engine.stock_query_app.routes import router as stock_router
    from backend.services.engine.stock_query_app.smart_screener_api import router as smart_screener_router

    app.include_router(stock_router)
    app.include_router(smart_screener_router)
    logger.info("✅ Stock Query & Smart Screener routers loaded")
except ImportError as e:
    logger.warning(f"⚠️ Stock Query router not available: {e}")


try:
    from backend.services.engine.routers.ai_ide.chat import router as ai_chat_router
    from backend.services.engine.routers.ai_ide.config import router as ai_config_router
    from backend.services.engine.routers.ai_ide.executor import router as ai_executor_router
    from backend.services.engine.routers.ai_ide.workspace import router as ai_workspace_router

    app.include_router(ai_chat_router, prefix="/api/v1/ai-ide/ai", tags=["Cloud IDE-AI"])
    app.include_router(ai_config_router, prefix="/api/v1/ai-ide/config", tags=["Cloud IDE-Config"])
    app.include_router(ai_executor_router, prefix="/api/v1/ai-ide/execute", tags=["Cloud IDE-Executor"])
    app.include_router(ai_workspace_router, prefix="/api/v1/ai-ide/files", tags=["Cloud IDE-Workspace"])
    logger.info("✅ Cloud AI-IDE routers loaded")
except ImportError as e:
    logger.error(f"❌ Failed to load Cloud AI-IDE routers: {e}")


@app.get("/health")
async def health_check():
    """存活探针（liveness）：仅检查进程是否活着，恒返回 200。

    下游依赖状态取自最近一次 /readiness 探测结果缓存（不实时探测），
    避免频繁探测拖慢 /health 性能。
    """
    startup_healthy = bool(getattr(app.state, "startup_healthy", True))
    set_service_health("quantmind-engine", startup_healthy)
    last_checks = getattr(app.state, "last_readiness_checks", None) or {}
    components = {
        "database": "connected" if last_checks.get("db") == "ok" else "disconnected",
        "redis": "connected" if last_checks.get("redis") == "ok" else "disconnected",
    }
    return {
        "status": "healthy" if startup_healthy else "degraded",
        "service": "quantmind-engine",
        "components": components,
    }


@app.get("/readiness")
async def readiness_check():
    """就绪探针（readiness）：实时探测下游依赖（DB/Redis）连通性。

    探测超时 2s；依赖不可用返回 503 + {"status": "not_ready", "checks": {...}}。
    """
    from backend.shared.database_pool import get_db
    from backend.shared.redis_sentinel_client import get_redis_sentinel_client

    def _db_probe():
        # 复用现有同步连接池执行 SELECT 1
        with get_db("postgres") as session:
            session.execute(text("SELECT 1")).fetchone()

    def _redis_probe():
        return get_redis_sentinel_client().ping()

    checks = {
        "db": await probe_sync("engine:db", _db_probe),
        "redis": await probe_sync("engine:redis", _redis_probe),
    }
    app.state.last_readiness_checks = checks
    return build_readiness_response(checks)


@app.get("/")
async def root():
    return {"message": "QuantMind Engine Core V2 is running"}


@app.get("/metrics")
async def metrics():
    return build_metrics_response()


if __name__ == "__main__":
    import uvicorn

    uvicorn.run(app, host="0.0.0.0", port=8001, access_log=False)
