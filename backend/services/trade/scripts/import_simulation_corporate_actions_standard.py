"""
Import corp_actions.csv (13-column standard format) into simulation_corporate_actions.

Usage:
    source .venv/bin/activate
    python backend/services/trade/scripts/import_simulation_corporate_actions_standard.py --dry-run
    python backend/services/trade/scripts/import_simulation_corporate_actions_standard.py --replace-existing

Field mapping (per memory: project_corp_actions_data_source.md):
    分红                 -> 1 dividend row (cash_dividend_per_share=interest)
    送股 / 转增          -> 1 bonus_share row (share_ratio=stock_bonus or stock_gift)
    分红|送股            -> 1 dividend + 1 bonus_share(stock_bonus)
    分红|转增            -> 1 dividend + 1 bonus_share(stock_gift)
    分红|送股|转增       -> 1 dividend + 1 bonus_share(stock_bonus + stock_gift)
    配股                 -> 1 rights_issue row (share_ratio=allot_num, rights_price=allot_price)

stock_bonus / stock_gift are PER-SHARE values in the CSV (already normalized from QMT's per-10-shares).
Do NOT divide by 10 here.
"""

from __future__ import annotations

import argparse
import asyncio
import os
from pathlib import Path
import sys

from sqlalchemy import select

BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../.."))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)
os.environ["PYTHONPATH"] = BASE_DIR

from backend.services.trade.simulation.models.corporate_action import (  # noqa: E402
    SimulationCorporateAction,
)
from backend.services.trade.simulation.services.corporate_action_importer import (  # noqa: E402
    load_standard_corp_action_csv,
)
from backend.shared.database_manager_v2 import (  # noqa: E402
    close_database,
    init_database,
)


def _build_arg_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Import corp_actions.csv (standard format) into simulation_corporate_actions.",
    )
    parser.add_argument(
        "--file",
        default="data/corp_actions.csv",
        help="CSV path, default: data/corp_actions.csv",
    )
    parser.add_argument(
        "--source",
        default=None,
        help="Optional source label written into simulation_corporate_actions.source.",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Preview import result without writing database rows.",
    )
    parser.add_argument(
        "--replace-existing",
        action="store_true",
        help="Delete same symbol+action_type+ex_date+source rows before insert.",
    )
    return parser


async def _run(args: argparse.Namespace) -> int:
    decisions = load_standard_corp_action_csv(args.file, source=args.source)
    accepted = [item for item in decisions if item.status == "accepted" and item.mapped]
    skipped = [item for item in decisions if item.status != "accepted"]

    print(f"Loaded decisions: {len(decisions)}")
    print(f"Accepted rows: {len(accepted)}")
    print(f"Skipped rows: {len(skipped)}")

    skipped_by_reason: dict[str, int] = {}
    for item in skipped:
        skipped_by_reason[item.reason] = skipped_by_reason.get(item.reason, 0) + 1
    if skipped_by_reason:
        print("Skipped summary:")
        for reason, count in sorted(skipped_by_reason.items()):
            print(f"  - {reason}: {count}")

    dr_anomalies = [
        item for item in accepted
        if item.mapped and item.mapped.note and "dr_anomaly=" in (item.mapped.note or "")
    ]
    if dr_anomalies:
        print(f"dr anomaly warnings: {len(dr_anomalies)} (pure-bonus events where dr != 1 + bonus_ratio)")
        for item in dr_anomalies[:10]:
            mapped = item.mapped
            assert mapped is not None
            anomaly_part = next(
                p for p in (mapped.note or "").split("; ") if p.startswith("dr_anomaly=")
            )
            print(
                f"  - {mapped.symbol} {mapped.action_type} "
                f"ex_date={mapped.ex_date.date() if mapped.ex_date else None} "
                f"{anomaly_part}"
            )

    by_action: dict[str, int] = {}
    for item in accepted:
        assert item.mapped is not None
        by_action[item.mapped.action_type] = by_action.get(item.mapped.action_type, 0) + 1
    if by_action:
        print("Accepted by action_type:")
        for action_type, count in sorted(by_action.items()):
            print(f"  - {action_type}: {count}")

    preview = accepted[:10]
    if preview:
        print("Accepted preview:")
        for item in preview:
            mapped = item.mapped
            assert mapped is not None
            print(
                "  - "
                f"{mapped.symbol} {mapped.action_type} "
                f"ex_date={mapped.ex_date.date() if mapped.ex_date else None} "
                f"cash={mapped.cash_dividend_per_share} "
                f"share_ratio={mapped.share_ratio} "
                f"rights_price={mapped.rights_price}"
            )

    if args.dry_run:
        print("Dry-run only. No database changes applied.")
        return 0

    await init_database()
    inserted = 0
    deleted = 0
    try:
        from backend.shared.database_manager_v2 import get_db_manager

        db_manager = get_db_manager()
        async with db_manager.get_master_session() as session:
            for item in accepted:
                mapped = item.mapped
                assert mapped is not None
                stmt = select(SimulationCorporateAction).where(
                    SimulationCorporateAction.symbol == mapped.symbol,
                    SimulationCorporateAction.action_type == mapped.action_type,
                    SimulationCorporateAction.ex_date == mapped.ex_date,
                    SimulationCorporateAction.source == mapped.source,
                )
                existing = list((await session.execute(stmt)).scalars().all())
                if existing and not args.replace_existing:
                    continue
                for row in existing:
                    await session.delete(row)
                    deleted += 1
                session.add(
                    SimulationCorporateAction(
                        symbol=mapped.symbol,
                        action_type=mapped.action_type,
                        ex_date=mapped.ex_date,
                        effective_date=mapped.effective_date,
                        cash_dividend_per_share=mapped.cash_dividend_per_share,
                        share_ratio=mapped.share_ratio,
                        rights_price=mapped.rights_price,
                        source=mapped.source,
                        note=mapped.note,
                        status="pending",
                    )
                )
                inserted += 1
        print(f"Inserted rows: {inserted}")
        print(f"Deleted existing rows: {deleted}")
        return 0
    finally:
        await close_database()


def main() -> int:
    parser = _build_arg_parser()
    args = parser.parse_args()
    file_path = Path(args.file)
    if not file_path.exists():
        print(f"File not found: {file_path}")
        return 1
    return asyncio.run(_run(args))


if __name__ == "__main__":
    raise SystemExit(main())
