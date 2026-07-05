"""stock_tag ORM 模型结构测试。

用 SQLite 内存库验证建表、约束、索引。不依赖真实 PG。
"""

from __future__ import annotations

import pytest
from sqlalchemy import create_engine, inspect, text
from sqlalchemy.exc import IntegrityError
from sqlalchemy.orm import sessionmaker

from backend.services.engine.models.stock_tag import StockTag, TagDictionary


@pytest.fixture()
def engine():
    eng = create_engine("sqlite:///:memory:")
    TagDictionary.metadata.create_all(
        eng, tables=[TagDictionary.__table__, StockTag.__table__]
    )
    return eng


@pytest.fixture()
def session(engine):
    Session = sessionmaker(bind=engine)
    sess = Session()
    sess.add(
        TagDictionary(
            tag_code="hs300",
            tag_name="沪深300",
            tag_category="index",
            is_active=True,
            sort_order=0,
        )
    )
    sess.add(
        TagDictionary(
            tag_code="ai",
            tag_name="AI",
            tag_category="concept",
            is_active=True,
            sort_order=1,
        )
    )
    sess.commit()
    return sess


def test_table_names_registered() -> None:
    assert TagDictionary.__tablename__ == "tag_dictionary"
    assert StockTag.__tablename__ == "stock_tag"


def test_tag_dictionary_columns(engine) -> None:
    cols = {c["name"] for c in inspect(engine).get_columns("tag_dictionary")}
    expected = {
        "tag_code",
        "tag_name",
        "tag_category",
        "source",
        "is_active",
        "sort_order",
        "created_at",
        "updated_at",
    }
    assert expected <= cols


def test_stock_tag_columns(engine) -> None:
    cols = {c["name"] for c in inspect(engine).get_columns("stock_tag")}
    expected = {"id", "symbol", "tag_code", "source", "created_at", "updated_at"}
    assert expected <= cols


def test_stock_tag_unique_constraint_and_indexes(engine) -> None:
    inspector = inspect(engine)
    indexes = {idx["name"] for idx in inspector.get_indexes("stock_tag")}
    assert "ix_stock_tag_tag_code" in indexes
    assert "ix_stock_tag_symbol" in indexes

    uniques = inspector.get_unique_constraints("stock_tag")
    assert any(set(u["column_names"]) == {"symbol", "tag_code"} for u in uniques)


def test_foreign_key_to_tag_dictionary(engine) -> None:
    fks = inspect(engine).get_foreign_keys("stock_tag")
    assert any(
        fk["referred_table"] == "tag_dictionary"
        and fk["constrained_columns"] == ["tag_code"]
        for fk in fks
    )


def test_unique_constraint_blocks_duplicate_membership(session) -> None:
    session.add(StockTag(symbol="SH600000", tag_code="hs300"))
    session.commit()

    session.add(StockTag(symbol="SH600000", tag_code="hs300"))
    with pytest.raises(IntegrityError):
        session.commit()
    session.rollback()


def test_one_stock_multiple_tags(session) -> None:
    session.add(StockTag(symbol="SH600000", tag_code="hs300"))
    session.add(StockTag(symbol="SH600000", tag_code="ai"))
    session.commit()

    rows = (
        session.query(StockTag.tag_code)
        .filter(StockTag.symbol == "SH600000")
        .order_by(StockTag.tag_code)
        .all()
    )
    assert [r[0] for r in rows] == ["ai", "hs300"]


def test_tag_dictionary_default_is_active(engine) -> None:
    Session = sessionmaker(bind=engine)
    sess = Session()
    td = TagDictionary(tag_code="manual", tag_name="自定义", tag_category="custom")
    sess.add(td)
    sess.commit()
    assert td.is_active is True or td.is_active == 1
