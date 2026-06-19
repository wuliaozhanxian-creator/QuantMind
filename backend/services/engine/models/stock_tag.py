from sqlalchemy import (
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Index,
    Integer,
    String,
    UniqueConstraint,
)
from sqlalchemy.sql import func

from backend.shared.database import Base


class TagDictionary(Base):
    """标签字典 — 指数/概念/板块标签的元数据。"""

    __tablename__ = "tag_dictionary"

    tag_code = Column(String(64), primary_key=True, comment="标签机器码: hs300/ai/chip")
    tag_name = Column(String(128), nullable=False, comment="标签中文名: 沪深300/AI")
    tag_category = Column(
        String(32), nullable=False, comment="分类: index/concept/board/custom"
    )
    source = Column(String(64), nullable=True, comment="数据来源: csi/metadata_json/manual")
    is_active = Column(Boolean, default=True, nullable=False, comment="软停用")
    sort_order = Column(Integer, default=0, nullable=False, comment="展示排序")
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    def __repr__(self):
        return f"<TagDictionary(tag_code={self.tag_code})>"


class StockTag(Base):
    """股票-标签成员关系（长表）。一只股票多个标签 = 多行。"""

    __tablename__ = "stock_tag"

    id = Column(
        BigInteger().with_variant(Integer, "sqlite"),
        primary_key=True,
        autoincrement=True,
    )
    symbol = Column(String(16), nullable=False, comment="股票代码 Prefix 格式: SH600191")
    tag_code = Column(
        String(64),
        ForeignKey("tag_dictionary.tag_code", ondelete="RESTRICT"),
        nullable=False,
        comment="标签机器码",
    )
    source = Column(String(64), nullable=True, comment="条目级来源")
    created_at = Column(
        DateTime(timezone=True), server_default=func.now(), nullable=False
    )
    updated_at = Column(
        DateTime(timezone=True),
        server_default=func.now(),
        onupdate=func.now(),
        nullable=False,
    )

    __table_args__ = (
        UniqueConstraint("symbol", "tag_code", name="uq_stock_tag_symbol_code"),
        Index("ix_stock_tag_tag_code", "tag_code"),
        Index("ix_stock_tag_symbol", "symbol"),
    )

    def __repr__(self):
        return f"<StockTag(symbol={self.symbol}, tag_code={self.tag_code})>"
