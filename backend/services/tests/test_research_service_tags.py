"""research_service 标签 SQL 生成测试。

测试 _concept_tags_subquery / _index_tags_subquery / _is_index_member 生成的
SQL 片段结构正确：引用 stock_tag + tag_dictionary，过滤正确 tag_category，
对无标签股票返回 '[]'::jsonb。
"""

from __future__ import annotations

import importlib

# research_service 模块可能依赖较重，单独加载其纯函数辅助。
# 如果导入失败则跳过（本地无 PG 环境时常见）。
pytest = __import__("pytest")

try:
    rs = importlib.import_module("backend.services.api.routers.research_service")
    _HELPERS_AVAILABLE = hasattr(rs, "_concept_tags_subquery")
except Exception:  # pragma: no cover
    _HELPERS_AVAILABLE = False

pytestmark = pytest.mark.skipif(
    not _HELPERS_AVAILABLE,
    reason="research_service 辅助函数不可用",
)


def test_concept_tags_subquery_references_stock_tag_and_dictionary() -> None:
    sql = rs._concept_tags_subquery("sdl.symbol")
    assert "stock_tag" in sql
    assert "tag_dictionary" in sql
    assert "to_jsonb" in sql
    assert "'[]'::jsonb" in sql
    assert "st.symbol = sdl.symbol" in sql
    # concept 聚合应包含 concept 和 board 两类
    assert "'concept'" in sql
    assert "'board'" in sql
    # 不应包含 index 类
    assert "tag_category = 'index'" not in sql


def test_index_tags_subquery_filters_index_category_only() -> None:
    sql = rs._index_tags_subquery("sdl.symbol")
    assert "stock_tag" in sql
    assert "tag_dictionary" in sql
    assert "tag_category = 'index'" in sql
    assert "to_jsonb" in sql
    assert "'[]'::jsonb" in sql
    # 不应包含 concept/board
    assert "'concept'" not in sql
    assert "'board'" not in sql


def test_concept_tags_subquery_orders_by_sort_order() -> None:
    sql = rs._concept_tags_subquery("sdl.symbol")
    assert "ORDER BY td.sort_order" in sql


def test_is_index_member_generates_exists_predicate() -> None:
    sql = rs._is_index_member("sdl.symbol", "hs300")
    assert "EXISTS" in sql
    assert "stock_tag" in sql
    assert "st.symbol = sdl.symbol" in sql
    assert "'hs300'" in sql


def test_is_index_member_escapes_tag_code_as_literal() -> None:
    sql = rs._is_index_member("s.symbol", "csi1000")
    assert "st.tag_code = 'csi1000'" in sql


def test_subquery_respects_is_active_flag() -> None:
    concept_sql = rs._concept_tags_subquery("sdl.symbol")
    index_sql = rs._index_tags_subquery("sdl.symbol")
    assert "td.is_active" in concept_sql
    assert "td.is_active" in index_sql
