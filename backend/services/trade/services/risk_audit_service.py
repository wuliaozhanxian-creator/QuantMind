"""
Risk Audit Service (T4.2)

将 ``RiskControlEngine`` 的命中事件 (REJECT/WARN/REQUIRE_CONFIRMATION) 持久化到
``risk_audit_log`` 表，并提供规则热加载变更的审计记录接口。

设计要点
--------
- 实现 ``RiskAuditCallback`` 协议，可直接注入 ``RiskControlEngine``；
- 持有可选的 ``AsyncSession``，DB 不可用时降级为仅记日志 (不阻断交易链路)；
- 提供同步上下文管理器风格的 ``with_session`` 以便在请求作用域内复用同一 session。
"""

from __future__ import annotations

import logging
from typing import Any, Optional

from sqlalchemy.ext.asyncio import AsyncSession

from backend.services.trade.models.risk_audit import RiskAuditLog
from backend.services.trade.services.risk_control import RiskRuleConfig

logger = logging.getLogger(__name__)

class RiskAuditService:
    """
    风控审计日志服务。

    用法
    ----
        audit = RiskAuditService(db_session)
        engine = RiskControlEngine(rules, audit_callback=audit.log_rule_hit)
        # 引擎命中规则时会自动调用 audit.log_rule_hit(...)
    """

    def __init__(self, db: AsyncSession | None = None) -> None:
        self._db = db

    def bind_session(self, db: AsyncSession) -> RiskAuditService:
        """绑定请求作用域的 DB session (链式返回 self)。"""
        self._db = db
        return self

    @property
    def db(self) -> AsyncSession | None:
        return self._db

    async def log_rule_hit(
        self,
        event_type: str,
        rule: RiskRuleConfig,
        order_info: dict[str, Any],
        details: dict[str, Any],
    ) -> None:
        """
        ``RiskAuditCallback`` 实现：记录单次规则命中。

        参数对齐引擎回调签名：
          event_type : rule.action (REJECT / WARN / REQUIRE_CONFIRMATION)
          rule       : RiskRuleConfig
          order_info : 订单摘要 (由引擎 _build_order_info 生成)
          details    : 命中详情 + message
        """
        message = str(details.get("message", "") or "")
        tenant_id = str(order_info.get("tenant_id", "") or "default")
        user_id = str(order_info.get("user_id", "") or "") or None

        log_entry = RiskAuditLog(
            event_type=str(event_type).upper(),
            rule_id=rule.rule_id,
            rule_type=str(rule.rule_type),
            order_info=dict(order_info or {}),
            hit_details=dict(details or {}),
            message=message,
            tenant_id=tenant_id,
            user_id=user_id,
        )

        if self._db is None:
            # DB 不可用时仅记日志，不抛异常 (审计失败不应阻断交易风控)
            logger.warning(
                "[RiskAudit] no DB session, skip persist: rule=%s event=%s msg=%s",
                rule.rule_id,
                event_type,
                message,
            )
            return

        try:
            self._db.add(log_entry)
            await self._db.commit()
            logger.info(
                "[RiskAudit] logged: rule=%s event=%s order=%s",
                rule.rule_id,
                event_type,
                order_info.get("order_id", ""),
            )
        except Exception as exc:
            # 持久化失败回滚，避免污染 session；审计本身不阻断主流程
            logger.exception("[RiskAudit] persist failed: %s", exc)
            try:
                await self._db.rollback()
            except Exception:
                logger.debug("ignored exception", exc_info=True)

    async def log_rule_reload(
        self,
        change_type: str,
        rule_id: str | None = None,
        rule_type: str | None = None,
        details: dict[str, Any] | None = None,
        tenant_id: str = "default",
    ) -> None:
        """
        记录规则热加载变更 (新增/删除/修改)。

        change_type : ADDED / REMOVED / MODIFIED / RELOADED
        """
        log_entry = RiskAuditLog(
            event_type="RULE_RELOAD",
            rule_id=rule_id,
            rule_type=rule_type,
            hit_details={"change_type": change_type, **(details or {})},
            message=f"rule {change_type.lower()}: {rule_id or 'batch'}",
            tenant_id=tenant_id,
        )
        if self._db is None:
            logger.warning(
                "[RiskAudit] no DB session, skip reload log: %s %s",
                change_type,
                rule_id,
            )
            return
        try:
            self._db.add(log_entry)
            await self._db.commit()
        except Exception as exc:
            logger.exception("[RiskAudit] reload log persist failed: %s", exc)
            try:
                await self._db.rollback()
            except Exception:
                logger.debug("ignored exception", exc_info=True)

    async def query_recent(
        self,
        tenant_id: str = "default",
        user_id: str | None = None,
        event_type: str | None = None,
        limit: int = 100,
    ) -> list[RiskAuditLog]:
        """查询最近的风控审计记录 (供 API/前端展示)。"""
        if self._db is None:
            return []
        from sqlalchemy import select

        stmt = select(RiskAuditLog).where(RiskAuditLog.tenant_id == tenant_id)
        if user_id:
            stmt = stmt.where(RiskAuditLog.user_id == user_id)
        if event_type:
            stmt = stmt.where(RiskAuditLog.event_type == event_type.upper())
        stmt = stmt.order_by(RiskAuditLog.created_at.desc()).limit(limit)
        result = await self._db.execute(stmt)
        return list(result.scalars().all())

# 模块级单例 (DB session 由调用方在请求作用域内 bind)
risk_audit_service = RiskAuditService()
