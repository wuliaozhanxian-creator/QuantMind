from datetime import date

from backend.services.engine.ai_strategy.steps.step1_stock_selection import (
    _parse_dsl,
)
from backend.services.engine.ai_strategy.steps.step2_pool_confirmation import (
    _build_compat_table_sql,
    _build_pool_summary,
    _inject_trade_date_filter,
    _replace_table_with_compat_subquery,
    _tag_membership_sql,
)
from backend.shared.stock_tag_utils import (
    is_membership_true_op,
    resolve_tag_code,
)


def test_parse_dsl_supports_single_equals() -> None:
    conditions, combiners = _parse_dsl("SELECT symbol WHERE idx_hs300 = 1")

    assert combiners == []
    assert conditions == [{"type": "simple", "factor": "idx_hs300", "op": "=", "value": 1.0}]


def test_tag_factor_aliases_resolve_to_tag_codes() -> None:
    assert resolve_tag_code("idx_hs300") == "hs300"
    assert resolve_tag_code("is_hs300") == "hs300"
    assert resolve_tag_code("is_csi300") == "hs300"
    assert resolve_tag_code("csi1000") == "csi1000"
    assert resolve_tag_code("concept_ai") == "ai"
    assert resolve_tag_code("concept_chip") == "chip"
    assert resolve_tag_code("amount") is None
    assert resolve_tag_code("pe_ttm") is None


def test_membership_op_classification() -> None:
    assert is_membership_true_op("=", 1) is True
    assert is_membership_true_op("=", 0) is False
    assert is_membership_true_op("==", 1) is True
    assert is_membership_true_op("!=", 0) is True
    assert is_membership_true_op("!=", 1) is False
    assert is_membership_true_op(">", 0) is True
    assert is_membership_true_op(">", 0.5) is True
    assert is_membership_true_op(">=", 0) is True
    assert is_membership_true_op("<=", 0) is False
    assert is_membership_true_op("<", 1) is False


def test_tag_membership_sql_generates_exists_predicate() -> None:
    sql = _tag_membership_sql("hs300")
    assert "EXISTS" in sql
    assert "stock_tag" in sql
    assert "'hs300'" in sql

    neg = _tag_membership_sql("csi500", negate=True)
    assert "NOT EXISTS" in neg or "NOT EXISTS" in neg.replace("NOT ", "NOT ")
    assert "'csi500'" in neg


def test_inject_trade_date_filter_for_raw_sql() -> None:
    sql = "SELECT symbol FROM stock_daily_latest WHERE pe_ttm <= 20 ORDER BY total_mv DESC"

    normalized = _inject_trade_date_filter(sql, date(2026, 4, 30))

    assert "trade_date = '2026-04-30'" in normalized
    assert normalized.endswith("ORDER BY total_mv DESC")


def test_compat_subquery_exposes_basic_column_aliases() -> None:
    columns = {
        "trade_date",
        "symbol",
        "name",
        "amount",
        "close",
        "total_mv",
        "pe_ttm",
        "pb",
        "volume",
    }
    compat_sql = _build_compat_table_sql("stock_daily_latest", columns)
    rewritten = _replace_table_with_compat_subquery(
        "SELECT code as symbol, stock_name as name FROM stock_daily_latest WHERE pe_ttm <= 20",
        "stock_daily_latest",
        compat_sql,
    )

    assert "symbol AS code" in compat_sql
    assert "name AS stock_name" in compat_sql
    assert "amount AS turnover" in compat_sql
    assert "FROM (SELECT" in rewritten


def test_pool_summary_uses_yi_buckets() -> None:
    class _Item:
        def __init__(self, market_cap: float) -> None:
            self.metrics = {"market_cap": market_cap}

    summary, charts = _build_pool_summary([_Item(80), _Item(150), _Item(300)], None, universe_total=10)

    assert summary["matchRate"] == 30.0
    assert charts["marketCap"] == [
        {"bucket": "<100亿", "value": 1},
        {"bucket": "100-200亿", "value": 1},
        {"bucket": ">=200亿", "value": 1},
    ]
