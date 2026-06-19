from __future__ import annotations

from types import SimpleNamespace

import pytest

from backend.services.engine.inference.router_service import InferenceRouterService


class _FakeInferenceService:
    def __init__(self):
        self.model_loader = SimpleNamespace(get_model_metadata=self._get_meta)
        self._responses = {}

    @staticmethod
    def _get_meta(model_id: str, **_kwargs):
        if model_id == "model_qlib":
            return {"feature_columns": [f"feature_{i}" for i in range(4)]}
        if model_id == "alpha158":
            return {"feature_columns": ["open", "high", "low", "close"]}
        return {}

    def predict(self, model_id, data, **kwargs):
        fn = self._responses.get(model_id)
        if fn is None:
            return {"status": "error", "model_id": model_id, "error": "not configured"}
        return fn(data)


def test_router_primary_success(monkeypatch):
    svc = _FakeInferenceService()
    svc._responses["model_qlib"] = lambda _data: {"status": "success", "model_id": "model_qlib", "predictions": [0.1]}
    router = InferenceRouterService(inference_service=svc)
    monkeypatch.setattr(router, "primary_model_id", "model_qlib")
    monkeypatch.setattr(router, "primary_data_source", "db/qlib_data")

    data = {"feature_0": 1, "feature_1": 2, "feature_2": 3, "feature_3": 4}
    result = router.predict_with_fallback("model_qlib", data)
    assert result["status"] == "success"
    assert result["fallback_used"] is False
    assert result["active_model_id"] == "model_qlib"
    assert result["active_data_source"] == "db/qlib_data"


def test_router_primary_failure_does_not_fallback(monkeypatch):
    svc = _FakeInferenceService()
    svc._responses["model_qlib"] = lambda _data: {"status": "error", "model_id": "model_qlib", "error": "primary broken"}
    router = InferenceRouterService(inference_service=svc)
    monkeypatch.setattr(router, "primary_model_id", "model_qlib")

    data = {"feature_0": 1, "feature_1": 2, "feature_2": 3, "feature_3": 4}
    result = router.predict_with_fallback("model_qlib", data)
    assert result["status"] == "error"
    assert result["fallback_used"] is False
    assert result["active_model_id"] == "model_qlib"


def test_router_non_primary_model_uses_primary_data_source(monkeypatch):
    svc = _FakeInferenceService()
    svc._responses["alpha158"] = lambda _data: {"status": "success", "model_id": "alpha158", "predictions": [0.3]}
    router = InferenceRouterService(inference_service=svc)
    monkeypatch.setattr(router, "primary_model_id", "model_qlib")
    monkeypatch.setattr(router, "primary_data_source", "db/qlib_data")

    result = router.predict_with_fallback("alpha158", {"open": 1, "high": 2, "low": 1, "close": 2})
    assert result["status"] == "success"
    assert result["fallback_used"] is False
    assert result["active_model_id"] == "alpha158"
    assert result["active_data_source"] == "db/qlib_data"


def test_router_sync_resolution_path_uses_sync_model_registry(monkeypatch, tmp_path):
    svc = _FakeInferenceService()
    model_dir = tmp_path / "user_model"
    model_dir.mkdir()
    svc._responses["user_model"] = lambda _data: {"status": "success", "model_id": "user_model", "predictions": [0.4]}

    router = InferenceRouterService(inference_service=svc)
    monkeypatch.setattr(router, "primary_model_id", "model_qlib")
    monkeypatch.setattr(
        "backend.shared.model_registry.model_registry_service.resolve_effective_model_sync",
        lambda **_kwargs: {
            "effective_model_id": "user_model",
            "model_source": "strategy_binding",
            "fallback_used": False,
            "fallback_reason": "",
            "storage_path": str(model_dir),
            "model_file": "model.lgb",
            "status": "ready",
        },
    )

    result = router.predict_with_fallback(
        "ignored",
        {"feature_0": 1, "feature_1": 2, "feature_2": 3, "feature_3": 4},
        tenant_id="default",
        user_id="79311845",
        strategy_id="48",
    )

    assert result["status"] == "success"
    assert result["fallback_used"] is False
    assert result["active_model_id"] == "user_model"


@pytest.mark.anyio
async def test_router_async_returns_error_when_no_model_resolved(monkeypatch):
    svc = _FakeInferenceService()
    router = InferenceRouterService(inference_service=svc)
    monkeypatch.setattr(router, "primary_model_id", "model_qlib")

    async def _raise(**_kwargs):
        raise LookupError("未找到可用模型: no user model configured")

    monkeypatch.setattr(router, "resolve_effective_model", _raise)

    result = await router.predict_with_fallback_async(
        "",
        {"feature_0": 1},
        tenant_id="default",
        user_id="79311845",
        trace_id="t1",
    )
    assert result["status"] == "error"
    assert "未找到可用模型" in result["error"]
    assert result["fallback_used"] is False
    assert result["active_model_id"] == ""


def test_router_sync_returns_error_when_no_model_resolved(monkeypatch):
    svc = _FakeInferenceService()
    router = InferenceRouterService(inference_service=svc)
    monkeypatch.setattr(router, "primary_model_id", "model_qlib")

    def _raise(**_kwargs):
        raise LookupError("未找到可用模型: no user model configured")

    monkeypatch.setattr(
        "backend.shared.model_registry.model_registry_service.resolve_effective_model_sync",
        _raise,
    )

    result = router.predict_with_fallback(
        "",
        {"feature_0": 1},
        tenant_id="default",
        user_id="79311845",
    )
    assert result["status"] == "error"
    assert "未找到可用模型" in result["error"]
    assert result["fallback_used"] is False
    assert result["active_model_id"] == ""
