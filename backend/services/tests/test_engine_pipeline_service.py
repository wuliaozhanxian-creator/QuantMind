"""Pipeline router tests for quantmind-engine."""

from contextlib import asynccontextmanager, contextmanager
from datetime import datetime

from fastapi import FastAPI
from fastapi.testclient import TestClient

from backend.services.engine.services.pipeline_service import (
    PipelineRunResult,
    PipelineRunStatus,
)

INTERNAL_HEADERS = {
    "X-Service-Token": "test-service-token",  # T6.5-P3: service JWT 替代 X-Internal-Call
    "X-User-Id": "u1",
    "X-Tenant-Id": "t1",
    # /api/v1/pipeline/* 属于业务路由（非 /api/v1/internal/*），T6.2 收紧后
    # 改用 JWT Bearer 方式认证。
    "Authorization": "Bearer test-token",
}


@contextmanager
def _client(monkeypatch):
    from backend.services.engine import main as engine_main
    from backend.shared.auth import AuthManager

    # T6.5-P3: service JWT 由 SECRET_KEY 签发，不再依赖 INTERNAL_CALL_SECRET
    monkeypatch.setenv("SECRET_KEY", "test-secret-key-for-service-jwt")
    # auth_middleware 仅对 /api/v1/internal/* 路径接受 X-Service-Token，
    # 业务路由必须通过有效 JWT。这里 mock verify_token，使中间件能够从
    # Bearer token 解出身份并写入 request.state.user，供路由依赖读取。
    monkeypatch.setattr(
        AuthManager,
        "verify_token",
        lambda self, token: {"sub": "u1", "tenant_id": "t1"},
    )

    @asynccontextmanager
    async def mock_lifespan(app: FastAPI):
        yield

    original_lifespan = engine_main.app.router.lifespan_context
    engine_main.app.router.lifespan_context = mock_lifespan
    try:
        with TestClient(engine_main.app) as client:
            yield client
    finally:
        engine_main.app.router.lifespan_context = original_lifespan


def test_pipeline_run_endpoint_success(monkeypatch):
    from backend.services.engine.routers import pipeline as pipeline_router

    run_status = PipelineRunStatus(
        run_id="r1",
        status="running",
        stage="queued",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    async def fake_create_run(payload):
        return "r1"

    async def fake_get_status(run_id: str, *, user_id: str, tenant_id: str):
        return run_status

    monkeypatch.setattr(pipeline_router.pipeline_service, "create_run", fake_create_run)
    monkeypatch.setattr(pipeline_router.pipeline_service, "get_status", fake_get_status)
    monkeypatch.setattr(
        pipeline_router,
        "_enqueue_pipeline_run",
        lambda *args, **kwargs: None,
    )

    with _client(monkeypatch) as client:
        resp = client.post(
            "/api/v1/pipeline/runs",
            headers=INTERNAL_HEADERS,
            json={
                "prompt": "test strategy",
                "user_id": "u1",
                "tenant_id": "t1",
            },
        )

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["run_id"] == "r1"
    assert payload["status"] == "running"


def test_pipeline_status_not_found(monkeypatch):
    from backend.services.engine.routers import pipeline as pipeline_router

    async def fake_get_status(run_id: str, *, user_id: str, tenant_id: str):
        return None

    monkeypatch.setattr(pipeline_router.pipeline_service, "get_status", fake_get_status)

    with _client(monkeypatch) as client:
        resp = client.get(
            "/api/v1/pipeline/runs/notfound?user_id=u1&tenant_id=t1",
            headers=INTERNAL_HEADERS,
        )

    assert resp.status_code == 404


def test_pipeline_status_success(monkeypatch):
    from backend.services.engine.routers import pipeline as pipeline_router

    status = PipelineRunStatus(
        run_id="r2",
        status="running",
        stage="generation",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    async def fake_get_status(run_id: str, *, user_id: str, tenant_id: str):
        return status

    monkeypatch.setattr(pipeline_router.pipeline_service, "get_status", fake_get_status)

    with _client(monkeypatch) as client:
        resp = client.get(
            "/api/v1/pipeline/runs/r2?user_id=u1&tenant_id=t1",
            headers=INTERNAL_HEADERS,
        )

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["run_id"] == "r2"
    assert payload["stage"] == "generation"


def test_pipeline_cleanup_success(monkeypatch):
    from backend.services.engine.routers import pipeline as pipeline_router

    async def fake_cleanup(*, user_id: str, tenant_id: str, keep_days: int):
        assert user_id == "u1"
        assert tenant_id == "t1"
        assert keep_days == 30
        return 7

    monkeypatch.setattr(pipeline_router.pipeline_service, "cleanup_old_runs", fake_cleanup)

    with _client(monkeypatch) as client:
        resp = client.delete(
            "/api/v1/pipeline/runs?user_id=u1&tenant_id=t1",
            headers=INTERNAL_HEADERS,
        )

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["status"] == "success"
    assert payload["deleted"] == 7


def test_pipeline_result_contains_fallback_metadata(monkeypatch):
    from backend.services.engine.routers import pipeline as pipeline_router

    result = PipelineRunResult(
        run_id="r3",
        status="completed",
        stage="completed",
        strategy_code="print('ok')",
        inference_result={
            "status": "success",
            "model_id": "alpha158",
            "predictions": [0.1],
            "symbols": ["SH600000"],
            "fallback_used": True,
            "fallback_reason": "主模型维度门禁未通过",
            "active_model_id": "alpha158",
            "active_data_source": "db/Alpha158_bin",
        },
        fallback_used=True,
        fallback_reason="主模型维度门禁未通过",
        active_model_id="alpha158",
        active_data_source="db/Alpha158_bin",
        created_at=datetime.utcnow(),
        updated_at=datetime.utcnow(),
    )

    async def fake_get_result(run_id: str, *, user_id: str, tenant_id: str):
        return result

    monkeypatch.setattr(pipeline_router.pipeline_service, "get_result", fake_get_result)

    with _client(monkeypatch) as client:
        resp = client.get(
            "/api/v1/pipeline/runs/r3/result?user_id=u1&tenant_id=t1",
            headers=INTERNAL_HEADERS,
        )

    assert resp.status_code == 200, resp.text
    payload = resp.json()
    assert payload["fallback_used"] is True
    assert payload["active_model_id"] == "alpha158"
    assert payload["active_data_source"] == "db/Alpha158_bin"
    assert payload["inference_result"]["fallback_used"] is True
