from __future__ import annotations

import asyncio
import json
import logging
import os
from pathlib import Path
from typing import Any

from backend.shared.model_registry import model_registry_service

from .script_runner import ExecutionResult, InferenceScriptRunner
from .service import InferenceService

logger = logging.getLogger(__name__)

_INDEPENDENT_MODEL_SOURCES = {
    "explicit_model_id",
    "strategy_binding",
    "user_default",
    "explicit_system_model",
}

def _build_execution_meta(
    *,
    fallback_used: bool,
    fallback_reason: str,
    active_model_id: str,
    effective_model_id: str,
    model_source: str,
    independent_execution: bool | None = None,
) -> dict[str, Any]:
    if independent_execution is None:
        independent_execution = model_source in _INDEPENDENT_MODEL_SOURCES

    model_switch_used = bool(fallback_used) or (
        bool(active_model_id)
        and bool(effective_model_id)
        and str(active_model_id) != str(effective_model_id)
    )
    return {
        "execution_mode": (
            "independent_model" if independent_execution else "system_chain"
        ),
        "model_switch_used": model_switch_used,
        "model_switch_reason": (
            str(fallback_reason or "") if model_switch_used else ""
        ),
    }

def _get_model_data_dir(model_dir: Path) -> str:
    """
    从模型配置中获取推理数据目录。

    优先级：
    1. metadata.json 中的 qlib_data_path 字段（绝对路径）
    2. metadata.json 中的 data_source 字段判断：
       - "qlib" -> db/qlib_data
       - "parquet" 或其他 -> db/feature_snapshots
    3. 默认值 -> db/feature_snapshots
    """
    meta_file = Path(model_dir) / "metadata.json"
    if meta_file.is_file():
        try:
            meta = json.loads(meta_file.read_text(encoding="utf-8"))
            qlib_data_path = meta.get("qlib_data_path")
            if qlib_data_path:
                return qlib_data_path

            data_source = str(meta.get("data_source", "")).lower()
            if data_source == "qlib":
                return "db/qlib_data"
        except Exception:
            logger.debug("ignored exception", exc_info=True)

    return "db/feature_snapshots"

class InferenceRouterService:
    """统一推理编排层：显式模型/策略绑定/默认模型（无 alpha158 兜底）。"""

    def __init__(self, inference_service: InferenceService | None = None):
        self.inference_service = inference_service or InferenceService()
        self.primary_model_id = os.getenv("PRIMARY_MODEL_ID", "model_qlib")
        self.primary_model_dir = os.getenv(
            "MODELS_PRODUCTION", "/app/models/production/model_qlib"
        )
        self.primary_data_source = os.getenv(
            "QLIB_PRIMARY_DATA_PATH", "db/feature_snapshots"
        )

    def _resolve_data_source(self, model_id: str, model_source: str = "") -> str:
        if model_source in {"explicit_model_id", "strategy_binding", "user_default"}:
            return "user_model_registry"
        return self.primary_data_source

    async def resolve_effective_model(
        self,
        *,
        tenant_id: str,
        user_id: str,
        strategy_id: str | None = None,
        model_id: str | None = None,
    ) -> dict[str, Any]:
        resolved = await model_registry_service.resolve_effective_model(
            tenant_id=tenant_id,
            user_id=user_id,
            strategy_id=strategy_id,
            model_id=model_id,
        )
        return resolved.to_dict()

    def _finalize(
        self,
        *,
        raw: dict[str, Any],
        fallback_used: bool,
        fallback_reason: str,
        active_model_id: str,
        active_data_source: str,
        trace_id: str | None,
        model_source: str,
        effective_model_id: str,
        independent_execution: bool | None = None,
    ) -> dict[str, Any]:
        result = self._enrich_result(
            raw,
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            active_model_id=active_model_id,
            active_data_source=active_data_source,
            model_source=model_source,
            effective_model_id=effective_model_id,
            independent_execution=independent_execution,
        )
        logger.info(
            "[InferenceRouter] trace_id=%s active_model_id=%s effective_model_id=%s source=%s fallback_used=%s fallback_reason=%s status=%s",
            trace_id or "n/a",
            result.get("active_model_id"),
            result.get("effective_model_id"),
            result.get("model_source"),
            result.get("fallback_used"),
            result.get("fallback_reason") or "",
            result.get("status"),
        )
        return result

    @staticmethod
    def _enrich_result(
        raw: dict[str, Any],
        *,
        fallback_used: bool,
        fallback_reason: str,
        active_model_id: str,
        active_data_source: str,
        model_source: str,
        effective_model_id: str,
        independent_execution: bool | None = None,
    ) -> dict[str, Any]:
        result = dict(raw or {})
        execution_meta = _build_execution_meta(
            fallback_used=fallback_used,
            fallback_reason=fallback_reason,
            active_model_id=active_model_id,
            effective_model_id=effective_model_id,
            model_source=model_source,
            independent_execution=independent_execution,
        )
        result["fallback_used"] = bool(fallback_used)
        result["fallback_reason"] = str(fallback_reason or "")
        result["execution_mode"] = execution_meta["execution_mode"]
        result["model_switch_used"] = execution_meta["model_switch_used"]
        result["model_switch_reason"] = execution_meta["model_switch_reason"]
        result["active_model_id"] = str(active_model_id)
        result["active_data_source"] = str(active_data_source)
        result["model_source"] = str(model_source or "")
        result["effective_model_id"] = str(effective_model_id or active_model_id)
        return result

    def _predict_single_model(
        self,
        *,
        model_id: str,
        data: dict[str, Any] | list[dict[str, Any]],
        model_dir: str | None = None,
        cache_namespace: str | None = None,
    ) -> dict[str, Any]:
        model_path = Path(model_dir) if model_dir else None
        return self.inference_service.predict(
            model_id,
            data,
            model_dir=model_path,
            cache_namespace=cache_namespace,
        )

    def _predict_system_chain(
        self,
        *,
        model_id: str,
        data: dict[str, Any] | list[dict[str, Any]],
        trace_id: str | None = None,
        model_source: str = "",
        effective_model_id: str | None = None,
    ) -> dict[str, Any]:
        effective_mid = str(effective_model_id or model_id or self.primary_model_id)
        requested_model = str(model_id or self.primary_model_id)

        primary = self._predict_single_model(model_id=requested_model, data=data)
        return self._finalize(
            raw=primary,
            fallback_used=False,
            fallback_reason="",
            active_model_id=requested_model,
            active_data_source=self._resolve_data_source(
                requested_model, model_source=model_source
            ),
            trace_id=trace_id,
            model_source=model_source,
            effective_model_id=effective_mid,
            independent_execution=requested_model != self.primary_model_id,
        )

    def predict_with_fallback(
        self,
        model_id: str,
        data: dict[str, Any] | list[dict[str, Any]],
        *,
        trace_id: str | None = None,
        tenant_id: str | None = None,
        user_id: str | None = None,
        strategy_id: str | None = None,
    ) -> dict[str, Any]:
        requested_model_id = str(model_id or "").strip() or self.primary_model_id
        if tenant_id and user_id:
            try:
                asyncio.get_running_loop()
                raise RuntimeError(
                    "predict_with_fallback must use async API when tenant_id/user_id are provided"
                )
            except RuntimeError as loop_err:
                if "must use async API" in str(loop_err):
                    raise
            try:
                resolved = model_registry_service.resolve_effective_model_sync(
                    tenant_id=str(tenant_id),
                    user_id=str(user_id),
                    strategy_id=strategy_id,
                    model_id=requested_model_id or None,
                )
            except LookupError as e:
                return self._build_resolve_error_result(
                    error=str(e),
                    requested_model_id=requested_model_id,
                    trace_id=trace_id,
                )
            return self._predict_with_resolved(
                requested_model_id=requested_model_id,
                resolved=resolved,
                data=data,
                trace_id=trace_id,
                tenant_id=tenant_id,
                user_id=user_id,
            )

        return self._predict_system_chain(
            model_id=requested_model_id,
            data=data,
            trace_id=trace_id,
            model_source="legacy_request",
            effective_model_id=requested_model_id,
        )

    async def predict_with_fallback_async(
        self,
        model_id: str,
        data: dict[str, Any] | list[dict[str, Any]],
        *,
        tenant_id: str,
        user_id: str,
        strategy_id: str | None = None,
        trace_id: str | None = None,
    ) -> dict[str, Any]:
        requested_model_id = str(model_id or "").strip() or self.primary_model_id
        try:
            resolved = await self.resolve_effective_model(
                tenant_id=str(tenant_id),
                user_id=str(user_id),
                strategy_id=strategy_id,
                model_id=requested_model_id or None,
            )
        except LookupError as e:
            return self._build_resolve_error_result(
                error=str(e),
                requested_model_id=requested_model_id,
                trace_id=trace_id,
            )
        return self._predict_with_resolved(
            requested_model_id=requested_model_id,
            resolved=resolved,
            data=data,
            trace_id=trace_id,
            tenant_id=tenant_id,
            user_id=user_id,
        )

    @staticmethod
    def _build_resolve_error_result(
        *,
        error: str,
        requested_model_id: str,
        trace_id: str | None,
    ) -> dict[str, Any]:
        return {
            "status": "error",
            "error": error,
            "model_id": requested_model_id,
            "trace_id": trace_id or "",
            "fallback_used": False,
            "fallback_reason": "",
            "active_model_id": "",
            "active_data_source": "",
            "model_source": "",
            "effective_model_id": "",
            "execution_mode": "system_chain",
            "model_switch_used": False,
            "model_switch_reason": "",
        }

    def _predict_with_resolved(
        self,
        *,
        requested_model_id: str,
        resolved: dict[str, Any],
        data: dict[str, Any] | list[dict[str, Any]],
        trace_id: str | None,
        tenant_id: str,
        user_id: str,
    ) -> dict[str, Any]:
        effective_model_id = str(
            resolved.get("effective_model_id") or self.primary_model_id
        )
        model_source = str(resolved.get("model_source") or "")
        storage_path = str(resolved.get("storage_path") or "").strip()

        if (
            storage_path
            and Path(storage_path).exists()
            and model_source
            in {
                "explicit_model_id",
                "strategy_binding",
                "user_default",
            }
        ):
            user_result = self._predict_single_model(
                model_id=effective_model_id,
                data=data,
                model_dir=storage_path,
                cache_namespace=f"{tenant_id}:{user_id}",
            )
            return self._finalize(
                raw=user_result,
                fallback_used=False,
                fallback_reason="",
                active_model_id=effective_model_id,
                active_data_source=self._resolve_data_source(
                    effective_model_id, model_source=model_source
                ),
                trace_id=trace_id,
                model_source=model_source,
                effective_model_id=effective_model_id,
                independent_execution=True,
            )

        chain_model = effective_model_id or requested_model_id or self.primary_model_id

        return self._predict_system_chain(
            model_id=chain_model,
            data=data,
            trace_id=trace_id,
            model_source=model_source,
            effective_model_id=effective_model_id,
        )

    def run_daily_inference_script(
        self,
        *,
        date: str,
        tenant_id: str,
        user_id: str,
        strategy_id: str | None = None,
        model_id: str | None = None,
        resolved_model: dict[str, Any] | None = None,
        redis_client=None,
    ) -> ExecutionResult:
        if resolved_model is not None:
            resolved = dict(resolved_model)
        else:
            resolved = model_registry_service.resolve_effective_model_sync(
                tenant_id=str(tenant_id),
                user_id=str(user_id),
                strategy_id=str(strategy_id) if strategy_id is not None else None,
                model_id=model_id,
            )
        effective_model_id = str(
            resolved.get("effective_model_id") or self.primary_model_id
        )
        model_source = str(resolved.get("model_source") or "")
        storage_path = str(resolved.get("storage_path") or "").strip()

        explicit_storage_dir = (
            storage_path if storage_path and Path(storage_path).exists() else ""
        )
        independent_execution = model_source in _INDEPENDENT_MODEL_SOURCES

        if explicit_storage_dir:
            primary_dir = explicit_storage_dir
            primary_id = effective_model_id
            primary_data_dir = _get_model_data_dir(Path(primary_dir))
        else:
            primary_dir = self.primary_model_dir
            primary_id = self.primary_model_id
            primary_data_dir = self.primary_data_source

        runner = InferenceScriptRunner(
            primary_model_dir=primary_dir,
            primary_data_dir=primary_data_dir,
            primary_model_id=primary_id,
        )
        result = runner.execute(
            date, tenant_id=tenant_id, user_id=user_id, redis_client=redis_client
        )
        execution_meta = _build_execution_meta(
            fallback_used=bool(result.fallback_used),
            fallback_reason=result.fallback_reason,
            active_model_id=result.active_model_id,
            effective_model_id=effective_model_id,
            model_source=model_source,
            independent_execution=independent_execution,
        )
        result.execution_mode = str(execution_meta["execution_mode"])
        result.model_switch_used = bool(execution_meta["model_switch_used"])
        result.model_switch_reason = str(execution_meta["model_switch_reason"])

        if result.success and model_source in _INDEPENDENT_MODEL_SOURCES:
            result.active_model_id = effective_model_id
        return result
