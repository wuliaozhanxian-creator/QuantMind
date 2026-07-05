"""
Helpers for importing raw corporate action CSV files into simulation events.
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
import csv
from pathlib import Path

from backend.shared.stock_utils import StockCodeUtil


@dataclass(slots=True)
class SimulationCorporateActionImportRow:
    symbol: str
    action_type: str
    ex_date: datetime | None
    effective_date: datetime | None
    cash_dividend_per_share: float
    share_ratio: float
    rights_price: float
    source: str
    note: str | None


@dataclass(slots=True)
class CorporateActionImportDecision:
    status: str
    reason: str
    raw_symbol: str
    raw_type: str
    raw_bonus: float
    raw_allotment: float
    mapped: SimulationCorporateActionImportRow | None = None


def _parse_float(value: str | None) -> float:
    try:
        return float(str(value or "").strip() or 0.0)
    except (TypeError, ValueError):
        return 0.0


def _parse_datetime(value: str | None) -> datetime | None:
    raw = str(value or "").strip()
    if not raw:
        return None
    return datetime.strptime(raw, "%Y-%m-%d")


def map_raw_corporate_action_row(
    raw_row: dict[str, str],
    *,
    source: str,
) -> CorporateActionImportDecision:
    raw_symbol = str(raw_row.get("symbol") or "").strip()
    raw_code = str(raw_row.get("code") or "").strip()
    raw_type = str(raw_row.get("type") or "").strip()
    raw_bonus = _parse_float(raw_row.get("bonus"))
    raw_allotment = _parse_float(raw_row.get("allotment"))
    ex_date = _parse_datetime(raw_row.get("date"))

    if not raw_symbol and not raw_code:
        return CorporateActionImportDecision(
            status="skipped",
            reason="missing_symbol",
            raw_symbol=raw_symbol,
            raw_type=raw_type,
            raw_bonus=raw_bonus,
            raw_allotment=raw_allotment,
        )

    normalized_symbol = StockCodeUtil.to_prefix(raw_code or raw_symbol)

    # Current upstream CSV semantics confirmed with user samples:
    # type=1 + bonus>0 means cash dividend where bonus is "cash per 10 shares".
    if raw_type != "1":
        return CorporateActionImportDecision(
            status="skipped",
            reason="unsupported_type",
            raw_symbol=normalized_symbol,
            raw_type=raw_type,
            raw_bonus=raw_bonus,
            raw_allotment=raw_allotment,
        )

    if raw_bonus <= 0:
        return CorporateActionImportDecision(
            status="skipped",
            reason="non_positive_bonus",
            raw_symbol=normalized_symbol,
            raw_type=raw_type,
            raw_bonus=raw_bonus,
            raw_allotment=raw_allotment,
        )

    if raw_allotment > 0:
        return CorporateActionImportDecision(
            status="skipped",
            reason="unsupported_allotment_format",
            raw_symbol=normalized_symbol,
            raw_type=raw_type,
            raw_bonus=raw_bonus,
            raw_allotment=raw_allotment,
        )

    per_share_dividend = round(raw_bonus / 10.0, 6)
    note = (
        "imported_from_raw_csv"
        f"; raw_type={raw_type}"
        f"; raw_bonus_per_10_shares={raw_bonus}"
        f"; raw_allotment={raw_allotment}"
    )
    return CorporateActionImportDecision(
        status="accepted",
        reason="cash_dividend_per_10_shares",
        raw_symbol=normalized_symbol,
        raw_type=raw_type,
        raw_bonus=raw_bonus,
        raw_allotment=raw_allotment,
        mapped=SimulationCorporateActionImportRow(
            symbol=normalized_symbol,
            action_type="dividend",
            ex_date=ex_date,
            effective_date=None,
            cash_dividend_per_share=per_share_dividend,
            share_ratio=0.0,
            rights_price=0.0,
            source=source,
            note=note,
        ),
    )


def load_raw_corporate_action_csv(
    file_path: str | Path,
    *,
    source: str | None = None,
) -> list[CorporateActionImportDecision]:
    path = Path(file_path)
    resolved_source = source or f"raw_csv:{path.name}"
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        return [
            map_raw_corporate_action_row(row, source=resolved_source) for row in reader
        ]


def _check_dr_anomaly(
    interest: float, stock_bonus: float, stock_gift: float, dr: float
) -> str | None:
    """Sanity-check dr against expected formula. Only validates pure-bonus events
    (interest == 0), since combined events require the pre-event close price P
    which is not in the CSV. Returns anomaly description or None.
    """
    if dr <= 0:
        return "dr_non_positive"
    total_bonus = stock_bonus + stock_gift
    if interest == 0 and total_bonus > 0:
        expected = 1.0 + total_bonus
        if abs(dr - expected) > 0.02:
            return f"dr={dr:.6f}_expected={expected:.6f}_for_pure_bonus"
    return None


def map_standard_corp_action_row(
    raw_row: dict[str, str],
    *,
    source: str,
) -> list[CorporateActionImportDecision]:
    """Map one row from corp_actions.csv (13-column standard format).

    One input row may produce 1-3 import decisions:
    - `分红` → 1 dividend row
    - `送股` / `转增` → 1 bonus_share row
    - `分红|送股` / `分红|转增` → 1 dividend + 1 bonus_share row
    - `分红|送股|转增` → 1 dividend + 1 bonus_share row (送股+转增 combined)
    - `配股` → 1 rights_issue row

    stock_bonus / stock_gift are PER-SHARE values in the CSV (already normalized
    from QMT's per-10-shares). Do NOT divide by 10 here.
    """
    raw_symbol = str(raw_row.get("symbol") or "").strip()
    ex_date = _parse_datetime(raw_row.get("trade_date"))
    interest = _parse_float(raw_row.get("interest"))
    stock_bonus = _parse_float(raw_row.get("stock_bonus"))
    stock_gift = _parse_float(raw_row.get("stock_gift"))
    allot_num = _parse_float(raw_row.get("allot_num"))
    allot_price = _parse_float(raw_row.get("allot_price"))
    event_type = str(raw_row.get("event_type") or "").strip()
    dr = _parse_float(raw_row.get("dr"))

    if not raw_symbol:
        return [
            CorporateActionImportDecision(
                status="skipped",
                reason="missing_symbol",
                raw_symbol=raw_symbol,
                raw_type=event_type,
                raw_bonus=stock_bonus,
                raw_allotment=allot_num,
            )
        ]

    normalized_symbol = StockCodeUtil.to_prefix(raw_symbol)
    dr_anomaly = _check_dr_anomaly(interest, stock_bonus, stock_gift, dr)

    def _note(extra: str) -> str:
        parts = [
            "imported_from_standard_csv",
            f"event_type={event_type}",
            extra,
            f"dr={dr}",
        ]
        if dr_anomaly:
            parts.append(f"dr_anomaly={dr_anomaly}")
        return "; ".join(parts)[:255]

    decisions: list[CorporateActionImportDecision] = []

    if interest > 0:
        decisions.append(
            CorporateActionImportDecision(
                status="accepted",
                reason="cash_dividend_per_share",
                raw_symbol=normalized_symbol,
                raw_type=event_type,
                raw_bonus=stock_bonus,
                raw_allotment=allot_num,
                mapped=SimulationCorporateActionImportRow(
                    symbol=normalized_symbol,
                    action_type="dividend",
                    ex_date=ex_date,
                    effective_date=None,
                    cash_dividend_per_share=round(interest, 6),
                    share_ratio=0.0,
                    rights_price=0.0,
                    source=source,
                    note=_note(f"interest={interest}"),
                ),
            )
        )

    total_bonus_ratio = stock_bonus + stock_gift
    if total_bonus_ratio > 0:
        decisions.append(
            CorporateActionImportDecision(
                status="accepted",
                reason="bonus_share_combined",
                raw_symbol=normalized_symbol,
                raw_type=event_type,
                raw_bonus=stock_bonus,
                raw_allotment=allot_num,
                mapped=SimulationCorporateActionImportRow(
                    symbol=normalized_symbol,
                    action_type="bonus_share",
                    ex_date=ex_date,
                    effective_date=None,
                    cash_dividend_per_share=0.0,
                    share_ratio=round(total_bonus_ratio, 6),
                    rights_price=0.0,
                    source=source,
                    note=_note(f"stock_bonus={stock_bonus}; stock_gift={stock_gift}"),
                ),
            )
        )

    if allot_num > 0:
        decisions.append(
            CorporateActionImportDecision(
                status="accepted",
                reason="rights_issue",
                raw_symbol=normalized_symbol,
                raw_type=event_type,
                raw_bonus=stock_bonus,
                raw_allotment=allot_num,
                mapped=SimulationCorporateActionImportRow(
                    symbol=normalized_symbol,
                    action_type="rights_issue",
                    ex_date=ex_date,
                    effective_date=None,
                    cash_dividend_per_share=0.0,
                    share_ratio=round(allot_num, 6),
                    rights_price=round(allot_price, 6),
                    source=source,
                    note=_note(f"allot_num={allot_num}; allot_price={allot_price}"),
                ),
            )
        )

    if not decisions:
        decisions.append(
            CorporateActionImportDecision(
                status="skipped",
                reason="no_actionable_fields",
                raw_symbol=normalized_symbol,
                raw_type=event_type,
                raw_bonus=stock_bonus,
                raw_allotment=allot_num,
            )
        )

    return decisions


def load_standard_corp_action_csv(
    file_path: str | Path,
    *,
    source: str | None = None,
) -> list[CorporateActionImportDecision]:
    path = Path(file_path)
    resolved_source = source or f"standard_csv:{path.name}"
    decisions: list[CorporateActionImportDecision] = []
    with path.open("r", encoding="utf-8-sig", newline="") as handle:
        reader = csv.DictReader(handle)
        for row in reader:
            decisions.extend(map_standard_corp_action_row(row, source=resolved_source))
    return decisions
