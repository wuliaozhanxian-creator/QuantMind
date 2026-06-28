#!/usr/bin/env python3
"""QuantMind 每日 L1/L2 日更总控脚本。

流程：
1. 本地计算单日 L1 切片
2. 工作站按低内存批处理模式计算单日 L2 切片
3. 将 L2 切片同步回本地
4. 合并 L1/L2 到年度 model_features 宽表

说明：
- 默认使用保守的工作站参数：每批 200 只股票、2 个进程。
- 不执行任何数据库写入。
"""

from __future__ import annotations

import argparse
import json
import os
import shlex
import subprocess
import sys
from pathlib import Path

import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Run QuantMind daily L1/L2 pipeline")
    parser.add_argument("--date", required=True, help="交易日，格式 YYYY-MM-DD")
    parser.add_argument("--l2-batch-size", type=int, default=200, help="工作站 L2 每批股票数")
    parser.add_argument("--l2-workers", type=int, default=2, help="工作站 L2 单批并行进程数")
    parser.add_argument("--skip-l1", action="store_true", help="跳过本地 L1 计算")
    parser.add_argument("--skip-l2", action="store_true", help="跳过工作站 L2 计算")
    parser.add_argument("--skip-sync", action="store_true", help="跳过 L2 回传本地")
    parser.add_argument("--skip-merge", action="store_true", help="跳过本地年度宽表合并")
    parser.add_argument("--backup", action="store_true", help="合并前生成年度宽表备份")
    parser.add_argument("--drop-b-shares", action="store_true", help="合并时按规则过滤 B 股")
    return parser.parse_args()


def run_cmd(cmd: list[str], *, cwd: Path | None = None, input_text: str | None = None) -> None:
    subprocess.run(
        cmd,
        check=True,
        cwd=str(cwd or PROJECT_ROOT),
        text=True,
        input=input_text,
    )


def resolve_trade_date(date_str: str) -> tuple[pd.Timestamp, str]:
    trade_date = pd.to_datetime(date_str, errors="coerce")
    if pd.isna(trade_date):
        raise ValueError(f"无效日期: {date_str}")
    trade_date = trade_date.normalize()
    return trade_date, trade_date.strftime("%Y%m%d")


def db_root() -> Path:
    return (PROJECT_ROOT / "db").resolve()


def run_local_l1(trade_date: pd.Timestamp, ymd: str) -> Path:
    output = PROJECT_ROOT / "db" / "feature_snapshots" / f"daily_l1_local_{ymd}.parquet"
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "data" / "processing" / "compute_l1_features_incremental.py"),
        "--date",
        trade_date.strftime("%Y-%m-%d"),
        "--output",
        str(output),
        "--write-audit",
    ]
    run_cmd(cmd)
    return output


def build_remote_l2_script(ymd: str, batch_size: int, workers: int) -> str:
    payload = {
        "date": ymd,
        "batch_size": batch_size,
        "workers": workers,
    }
    return f"""
import json
import math
import sys
from pathlib import Path
import pandas as pd

cfg = json.loads({json.dumps(json.dumps(payload), ensure_ascii=False)})
sys.path.insert(0, '/quantmind')
from scripts.data.calc_hf_52d_features import normalize_secu_code, process_day_parallel, OUTPUT_DIR

DATE = cfg['date']
BATCH_SIZE = int(cfg['batch_size'])
WORKERS = int(cfg['workers'])
snap_path = Path('/quantmind/baidudesk') / f'snapshot_{{DATE}}.parquet'
out_path = OUTPUT_DIR / f'hf_{{DATE}}.parquet'

print(f'[INFO] start batched L2 date={{DATE}} batch_size={{BATCH_SIZE}} workers={{WORKERS}}', flush=True)
if not snap_path.exists():
    raise FileNotFoundError(snap_path)

sym = pd.read_parquet(snap_path, columns=['SecuCode'])
symbols = normalize_secu_code(sym['SecuCode']).dropna().astype(str).str.strip().str.upper()
symbols = sorted(set(symbols.tolist()))
print(f'[INFO] total symbols={{len(symbols)}}', flush=True)

parts = []
for idx in range(0, len(symbols), BATCH_SIZE):
    batch = symbols[idx: idx + BATCH_SIZE]
    batch_no = idx // BATCH_SIZE + 1
    total_batches = math.ceil(len(symbols) / BATCH_SIZE)
    print(f'[INFO] batch {{batch_no}}/{{total_batches}} symbols={{len(batch)}} first={{batch[0]}} last={{batch[-1]}}', flush=True)
    df = process_day_parallel(DATE, workers=WORKERS, symbols=set(batch))
    if df is not None and not df.empty:
        parts.append(df)

if not parts:
    raise RuntimeError('no hf features generated in batched run')

final = pd.concat(parts, ignore_index=True)
final['symbol'] = final['symbol'].astype(str).str.strip().str.upper()
final['trade_date'] = pd.to_datetime(final['trade_date']).dt.strftime('%Y-%m-%d')
final = final.sort_values(['trade_date', 'symbol']).drop_duplicates(['trade_date', 'symbol'], keep='last').reset_index(drop=True)
out_path.parent.mkdir(parents=True, exist_ok=True)
final.to_parquet(out_path, index=False)
print(f'[INFO] final rows={{len(final)}} symbols={{final["symbol"].nunique()}} path={{out_path}}', flush=True)
"""


def run_remote_l2(ymd: str, batch_size: int, workers: int) -> None:
    script = build_remote_l2_script(ymd, batch_size, workers)
    cmd = [
        str(PROJECT_ROOT / "scripts" / "ops" / "workstation_ssh.sh"),
        "cd /quantmind && python3 -",
    ]
    run_cmd(cmd, input_text=script)


def sync_remote_l2(ymd: str) -> Path:
    local_path = db_root() / "hf_features" / f"hf_{ymd}.parquet"
    local_path.parent.mkdir(parents=True, exist_ok=True)
    if local_path.exists():
        local_path.unlink()

    remote_cmd = f"cd /quantmind && tar cf - db/hf_features/hf_{ymd}.parquet"
    ssh_cmd = f"{shlex.quote(str(PROJECT_ROOT / 'scripts' / 'ops' / 'workstation_ssh.sh'))} {shlex.quote(remote_cmd)}"
    cmd = f"{ssh_cmd} | tar xf - -C {shlex.quote(str(db_root().parent))}"
    subprocess.run(cmd, check=True, shell=True, cwd=str(PROJECT_ROOT))
    return local_path


def validate_l2_symbols(l2_path: Path) -> None:
    df = pd.read_parquet(l2_path, columns=["symbol"])
    s = df["symbol"].astype(str).str.strip().str.upper()
    bad = s[~s.str.match(r"^(SH|SZ|BJ)\\d{{6}}$", na=False)]
    if not bad.empty:
        sample = bad.drop_duplicates().head(20).tolist()
        raise RuntimeError(f"L2 切片存在未标准化股票代码: count={bad.nunique()} sample={sample}")


def merge_local(trade_date: pd.Timestamp, l2_path: Path, backup: bool, drop_b_shares: bool) -> Path:
    audit_path = PROJECT_ROOT / "audit" / "daily_152" / f"merge_l1_l2_{trade_date.strftime('%Y%m%d')}.json"
    audit_path.parent.mkdir(parents=True, exist_ok=True)
    cmd = [
        sys.executable,
        str(PROJECT_ROOT / "scripts" / "data" / "processing" / "merge_l1_l2_into_yearly.py"),
        "--date",
        trade_date.strftime("%Y-%m-%d"),
        "--l2-path",
        str(l2_path),
        "--audit-output",
        str(audit_path),
    ]
    if backup:
        cmd.append("--backup")
    if drop_b_shares:
        cmd.append("--drop-b-shares")
    run_cmd(cmd)
    return audit_path


def main() -> int:
    args = parse_args()
    trade_date, ymd = resolve_trade_date(args.date)

    if not args.skip_l1:
        run_local_l1(trade_date, ymd)

    if not args.skip_l2:
        run_remote_l2(ymd, batch_size=args.l2_batch_size, workers=args.l2_workers)

    l2_path = db_root() / "hf_features" / f"hf_{ymd}.parquet"
    if not args.skip_sync:
        l2_path = sync_remote_l2(ymd)

    validate_l2_symbols(l2_path)

    if not args.skip_merge:
        merge_local(
            trade_date,
            l2_path=l2_path,
            backup=bool(args.backup),
            drop_b_shares=bool(args.drop_b_shares),
        )

    print(
        json.dumps(
            {
                "date": trade_date.strftime("%Y-%m-%d"),
                "l2_path": str(l2_path),
                "skip_l1": bool(args.skip_l1),
                "skip_l2": bool(args.skip_l2),
                "skip_sync": bool(args.skip_sync),
                "skip_merge": bool(args.skip_merge),
                "l2_batch_size": int(args.l2_batch_size),
                "l2_workers": int(args.l2_workers),
            },
            ensure_ascii=False,
            indent=2,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
