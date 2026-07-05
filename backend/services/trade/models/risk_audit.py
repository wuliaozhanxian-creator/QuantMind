"""
Risk Audit Log Model (T4.2)

记录风控规则命中事件 (REJECT / WARN / REQUIRE_CONFIRMATION) 与规则热加载事件，
用于合规审计与事后追溯。

表名：``risk_audit_log``
"""

from sqlalchemy import JSON, Column, DateTime, Index, Integer, String, Text
from sqlalchemy.sql import func

from .base import Base


class RiskAuditLog(Base):
    """风控审计日志 (每次规则命中/规则变更落库一条)"""

    __tablename__ = "risk_audit_log"

    # 主键
    id = Column(Integer, primary_key=True, autoincrement=True)

    # 事件元信息
    event_type = Column(String(32), nullable=False, index=True)
    # REJECT / WARN / REQUIRE_CONFIRMATION / RULE_RELOAD / RULE_PASS

    # 命中的规则信息 (RULE_RELOAD 时记录变更类型)
    rule_id = Column(String(100), nullable=True, index=True)
    rule_type = Column(String(50), nullable=True, index=True)

    # 订单上下文 (JSON: order_id/symbol/side/price/quantity/order_value/tenant_id/user_id)
    order_info = Column(JSON, nullable=True)

    # 命中详情 (JSON: 阈值/实际值/计算中间量等)
    hit_details = Column(JSON, nullable=True)

    # 人可读消息
    message = Column(Text, nullable=True)

    # 租户/用户隔离
    tenant_id = Column(String(64), nullable=False, default="default", index=True)
    user_id = Column(String(64), nullable=True, index=True)

    # 时间戳 (数据库生成，避免应用时钟漂移)
    created_at = Column(
        DateTime(timezone=True),
        nullable=False,
        server_default=func.now(),
        index=True,
    )

    __table_args__ = (
        Index(
            "idx_risk_audit_tenant_user_created", "tenant_id", "user_id", "created_at"
        ),
        Index("idx_risk_audit_rule_type_created", "rule_type", "created_at"),
    )

    def __repr__(self):
        return (
            f"<RiskAuditLog(id={self.id}, event_type={self.event_type}, "
            f"rule_id={self.rule_id}, created_at={self.created_at})>"
        )
