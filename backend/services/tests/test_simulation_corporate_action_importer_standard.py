from datetime import datetime

from backend.services.trade.simulation.services.corporate_action_importer import (
    load_standard_corp_action_csv,
    map_standard_corp_action_row,
)


def _row(**overrides):
    base = {
        "symbol": "000001.SZ",
        "trade_date": "2026-06-22",
        "interest": "0.0",
        "stock_bonus": "0.0",
        "stock_gift": "0.0",
        "allot_num": "0.0",
        "allot_price": "0.0",
        "gugai": "0.0",
        "dr": "1.0",
        "event_type": "",
        "has_dividend": "0",
        "has_stock_action": "0",
        "is_high_transfer": "0",
    }
    base.update(overrides)
    return base


def test_pure_dividend_maps_to_single_dividend_row():
    decisions = map_standard_corp_action_row(
        _row(interest="0.25", event_type="分红", dr="1.05"),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    d = decisions[0]
    assert d.status == "accepted"
    assert d.mapped is not None
    assert d.mapped.action_type == "dividend"
    assert d.mapped.cash_dividend_per_share == 0.25
    assert d.mapped.share_ratio == 0.0
    assert d.mapped.ex_date == datetime(2026, 6, 22)
    assert d.mapped.symbol == "SZ000001"


def test_pure_stock_gift_maps_to_bonus_share_with_gift_as_ratio():
    decisions = map_standard_corp_action_row(
        _row(symbol="000793.SZ", stock_gift="1.2", event_type="转增", dr="2.191666"),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    d = decisions[0]
    assert d.mapped is not None
    assert d.mapped.action_type == "bonus_share"
    assert d.mapped.share_ratio == 1.2
    assert d.mapped.cash_dividend_per_share == 0.0


def test_pure_stock_bonus_maps_to_bonus_share_with_bonus_as_ratio():
    decisions = map_standard_corp_action_row(
        _row(stock_bonus="0.5", event_type="送股", dr="1.5"),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    d = decisions[0]
    assert d.mapped is not None
    assert d.mapped.action_type == "bonus_share"
    assert d.mapped.share_ratio == 0.5


def test_combined_dividend_and_stock_gift_produces_two_rows():
    decisions = map_standard_corp_action_row(
        _row(
            symbol="000937.SZ",
            interest="0.1",
            stock_gift="0.1",
            event_type="分红|转增",
            dr="1.121649",
        ),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 2
    by_action = {d.mapped.action_type: d.mapped for d in decisions if d.mapped}
    assert by_action["dividend"].cash_dividend_per_share == 0.1
    assert by_action["bonus_share"].share_ratio == 0.1


def test_combined_dividend_bonus_gift_merges_bonus_into_one_row():
    decisions = map_standard_corp_action_row(
        _row(
            interest="0.2",
            stock_bonus="0.3",
            stock_gift="0.4",
            event_type="分红|送股|转增",
            dr="2.0",
        ),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 2
    by_action = {d.mapped.action_type: d.mapped for d in decisions if d.mapped}
    assert by_action["dividend"].cash_dividend_per_share == 0.2
    assert by_action["bonus_share"].share_ratio == 0.7


def test_rights_issue_maps_to_rights_issue_row():
    decisions = map_standard_corp_action_row(
        _row(
            symbol="600000.SH",
            allot_num="0.5",
            allot_price="8.0",
            event_type="配股",
            dr="1.0",
        ),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    d = decisions[0]
    assert d.mapped is not None
    assert d.mapped.action_type == "rights_issue"
    assert d.mapped.share_ratio == 0.5
    assert d.mapped.rights_price == 8.0
    assert d.mapped.symbol == "SH600000"


def test_missing_symbol_is_skipped():
    decisions = map_standard_corp_action_row(
        _row(symbol="", interest="0.1"),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    assert decisions[0].status == "skipped"
    assert decisions[0].reason == "missing_symbol"


def test_all_zero_fields_is_skipped():
    decisions = map_standard_corp_action_row(
        _row(event_type=""),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    assert decisions[0].status == "skipped"
    assert decisions[0].reason == "no_actionable_fields"


def test_dr_anomaly_flagged_when_pure_bonus_dr_clearly_wrong():
    # stock_gift=1.2 → expected dr=2.2; actual dr=3.0 is way off → flagged.
    decisions = map_standard_corp_action_row(
        _row(symbol="000793.SZ", stock_gift="1.2", event_type="转增", dr="3.0"),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    d = decisions[0]
    assert d.mapped is not None
    assert "dr_anomaly=" in (d.mapped.note or "")


def test_dr_anomaly_not_flagged_when_within_tolerance():
    # 000793.SZ: stock_gift=1.2, dr=2.191666. Expected 2.2, diff ~0.008 (within 0.02 tolerance).
    # This is real-world price-rounding noise, not a data error.
    decisions = map_standard_corp_action_row(
        _row(symbol="000793.SZ", stock_gift="1.2", event_type="转增", dr="2.191666"),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    d = decisions[0]
    assert d.mapped is not None
    assert "dr_anomaly=" not in (d.mapped.note or "")


def test_dr_anomaly_not_flagged_for_precise_small_event():
    # 001211.SZ: stock_gift=0.39462, dr=1.394679. Expected 1.39462, diff <0.0001.
    decisions = map_standard_corp_action_row(
        _row(
            symbol="001211.SZ", stock_gift="0.39462", event_type="转增", dr="1.394679"
        ),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 1
    d = decisions[0]
    assert d.mapped is not None
    assert "dr_anomaly=" not in (d.mapped.note or "")


def test_dr_anomaly_not_checked_for_combined_events_with_interest():
    # Combined分红|转增: dr validation requires P (not in CSV), so skipped.
    decisions = map_standard_corp_action_row(
        _row(
            symbol="002116.SZ",
            interest="0.219309",
            stock_gift="0.2",
            event_type="分红|转增",
            dr="1.230245",
        ),
        source="standard_csv:test.csv",
    )
    assert len(decisions) == 2
    for d in decisions:
        assert d.mapped is not None
        assert "dr_anomaly=" not in (d.mapped.note or "")


def test_load_standard_csv_reads_utf8_sig(tmp_path):
    csv_path = tmp_path / "corp_actions.csv"
    csv_path.write_text(
        "﻿symbol,trade_date,interest,stock_bonus,stock_gift,allot_num,allot_price,gugai,dr,event_type,has_dividend,has_stock_action,is_high_transfer\n"
        "000793.SZ,2026-06-22,0.0,0.0,1.2,0.0,0.0,0.0,2.191666,转增,0,1,0\n"
        "000937.SZ,2026-06-17,0.1,0.0,0.1,0.0,0.0,0.0,1.121649,分红|转增,1,1,0\n",
        encoding="utf-8",
    )
    decisions = load_standard_corp_action_csv(csv_path)
    accepted = [d for d in decisions if d.status == "accepted"]
    assert len(accepted) == 3  # 1 (转增) + 2 (分红|转增)
    symbols = {d.mapped.symbol for d in accepted if d.mapped}
    assert symbols == {"SZ000793", "SZ000937"}


def test_source_defaults_to_standard_csv_prefix(tmp_path):
    csv_path = tmp_path / "corp_actions.csv"
    csv_path.write_text(
        "﻿symbol,trade_date,interest,stock_bonus,stock_gift,allot_num,allot_price,gugai,dr,event_type,has_dividend,has_stock_action,is_high_transfer\n"
        "000001.SZ,2026-06-22,0.25,0.0,0.0,0.0,0.0,0.0,1.0,分红,1,0,0\n",
        encoding="utf-8",
    )
    decisions = load_standard_corp_action_csv(csv_path)
    assert decisions[0].mapped is not None
    assert decisions[0].mapped.source == "standard_csv:corp_actions.csv"
