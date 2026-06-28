#!/usr/bin/env python3
"""合并当日 L1/L2 切片到年度 model_features，并更新 metadata。

用途：
1) 读取本地 L1 切片（daily_l1_local_YYYYMMDD.parquet）
2) 读取工作站回传 L2 切片（hf_YYYYMMDD_*.parquet）
3) 按 (symbol, trade_date) 覆盖合并到 model_features_YYYY.parquet
4) 同步更新 model_features_YYYY.metadata.json
"""

from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd


PROJECT_ROOT = Path(__file__).resolve().parents[3]


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Merge daily L1/L2 slices into yearly model_features parquet")
    parser.add_argument("--date", required=True, help="交易日，格式 YYYY-MM-DD")
    parser.add_argument(
        "--yearly-path",
        type=Path,
        default=None,
        help="年度特征路径，默认 db/feature_snapshots/model_features_YYYY.parquet",
    )
    parser.add_argument(
        "--l1-path",
        type=Path,
        default=None,
        help="L1切片路径，默认 db/feature_snapshots/daily_l1_local_YYYYMMDD.parquet",
    )
    parser.add_argument(
        "--l2-path",
        type=Path,
        default=None,
        help="L2切片路径，默认 db/hf_features/hf_YYYYMMDD.parquet",
    )
    parser.add_argument(
        "--metadata-path",
        type=Path,
        default=None,
        help="metadata 路径，默认与 yearly 同目录同名前缀 .metadata.json",
    )
    parser.add_argument(
        "--backup",
        action="store_true",
        help="写回前生成备份（建议开启）",
    )
    parser.add_argument(
        "--drop-b-shares",
        action="store_true",
        help="按 v2 规则过滤 B 股（SH9xxxxx / SZ2xxxxx）",
    )
    parser.add_argument(
        "--audit-output",
        type=Path,
        default=None,
        help="可选：输出合并审计 JSON 路径",
    )
    return parser.parse_args()


def _normalize_day(df: pd.DataFrame, target_date: pd.Timestamp) -> pd.DataFrame:
    out = df.copy()
    out["trade_date"] = pd.to_datetime(out["trade_date"], errors="coerce").dt.normalize()
    out = out[out["trade_date"] == target_date].copy()
    out["symbol"] = out["symbol"].astype(str).str.strip().str.upper()
    out = out.dropna(subset=["symbol", "trade_date"])
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    return out


def _drop_b_share_rows(df: pd.DataFrame) -> pd.DataFrame:
    mask = df["symbol"].str.match(r"^(SH9\d{5}|SZ2\d{5})$", na=False)
    if mask.any():
        return df[~mask].copy()
    return df


def _overlay_from_source(day: pd.DataFrame, src: pd.DataFrame, base_cols: list[str]) -> pd.DataFrame:
    keys = ["symbol", "trade_date"]
    src_cols = [c for c in src.columns if c in base_cols and c not in keys]
    if not src_cols:
        return day

    src2 = src[keys + src_cols].copy()
    src2["_in_src"] = 1
    merged = day.merge(src2, on=keys, how="left", suffixes=("", "__new"))

    in_src = merged["_in_src"].fillna(0).astype(int).eq(1)
    for c in src_cols:
        c_new = f"{c}__new"
        # 对于出现在源切片中的 key，无论值是否 NaN，都覆盖旧值
        merged[c] = merged[c_new].where(in_src, merged[c])

    drop_cols = ["_in_src"] + [f"{c}__new" for c in src_cols]
    merged = merged.drop(columns=drop_cols, errors="ignore")
    return merged


def _sha256(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(1024 * 1024), b""):
            h.update(chunk)
    return h.hexdigest()


def build_metadata(year: int, yearly_path: Path) -> dict:
    full = pd.read_parquet(yearly_path)
    full["trade_date"] = pd.to_datetime(full["trade_date"], errors="coerce").dt.normalize()

    key_df = full[["symbol", "trade_date"]].copy()
    key_df["symbol"] = key_df["symbol"].astype(str)

    feature_cols = [c for c in full.columns if c not in {"symbol", "trade_date"}]
    feature_cov = float(full[feature_cols].notna().mean().mean()) if feature_cols else 0.0
    req_cov: dict[str, float] = {}
    for c in ["open", "high", "low", "close", "factor"]:
        req_cov[c] = float(full[c].notna().mean()) if c in full.columns else 0.0

    stat = yearly_path.stat()
    return {
        "year": year,
        "dataset": yearly_path.name,
        "path": str(yearly_path),
        "rows": int(len(key_df)),
        "symbols": int(key_df["symbol"].nunique()),
        "trade_days": int(key_df["trade_date"].dt.strftime("%Y-%m-%d").nunique()),
        "date_min": key_df["trade_date"].min().strftime("%Y-%m-%d") if len(key_df) else None,
        "date_max": key_df["trade_date"].max().strftime("%Y-%m-%d") if len(key_df) else None,
        "columns_total": int(len(full.columns)),
        "feature_columns": int(max(len(full.columns) - 2, 0)),
        "duplicate_key_count": int(key_df.duplicated(["symbol", "trade_date"]).sum()),
        "symbol_prefix_pass_rate": float(
            key_df["symbol"].str.match(r"^(SH|SZ|BJ)\d{6}$", na=False).mean()
        )
        if len(key_df)
        else 0.0,
        "feature_coverage": feature_cov,
        "required_field_coverage": req_cov,
        "file_size_bytes": int(stat.st_size),
        "file_sha256": _sha256(yearly_path),
        "file_mtime": datetime.fromtimestamp(stat.st_mtime).isoformat(timespec="seconds"),
        "generated_at": datetime.now().isoformat(timespec="seconds"),
    }


def main() -> None:
    args = parse_args()
    target_date = pd.to_datetime(args.date, errors="coerce")
    if pd.isna(target_date):
        raise ValueError(f"无效日期: {args.date}")
    target_date = target_date.normalize()
    year = int(target_date.year)
    ymd = target_date.strftime("%Y%m%d")

    yearly_path = (
        args.yearly_path
        if args.yearly_path is not None
        else PROJECT_ROOT / "db" / "feature_snapshots" / f"model_features_{year}.parquet"
    )
    l1_path = (
        args.l1_path
        if args.l1_path is not None
        else PROJECT_ROOT / "db" / "feature_snapshots" / f"daily_l1_local_{ymd}.parquet"
    )
    l2_path = (
        args.l2_path
        if args.l2_path is not None
        else PROJECT_ROOT / "db" / "hf_features" / f"hf_{ymd}.parquet"
    )
    metadata_path = (
        args.metadata_path
        if args.metadata_path is not None
        else yearly_path.parent / f"{yearly_path.stem}.merge.metadata.json"
    )

    for p in [yearly_path, l1_path, l2_path]:
        if not p.exists():
            raise FileNotFoundError(f"文件不存在: {p}")

    base = pd.read_parquet(yearly_path)
    base["trade_date"] = pd.to_datetime(base["trade_date"], errors="coerce").dt.normalize()
    base_cols = base.columns.tolist()

    l1 = _normalize_day(pd.read_parquet(l1_path), target_date)
    l2 = _normalize_day(pd.read_parquet(l2_path), target_date)
    if args.drop_b_shares:
        l1 = _drop_b_share_rows(l1)
        l2 = _drop_b_share_rows(l2)

    keys = pd.DataFrame({"symbol": sorted(set(l1["symbol"]).union(set(l2["symbol"])))})
    keys["trade_date"] = target_date
    if keys.empty:
        raise RuntimeError("L1/L2 均为空，无法执行当日合并")

    day_old = base[base["trade_date"] == target_date].copy()
    day = keys.merge(day_old, on=["symbol", "trade_date"], how="left")
    day = day.reindex(columns=base_cols)

    day = _overlay_from_source(day, l1, base_cols)
    day = _overlay_from_source(day, l2, base_cols)
    day = day.reindex(columns=base_cols)

    old_day_rows = int(len(day_old))
    out = pd.concat([base[base["trade_date"] != target_date], day], ignore_index=True)
    out = out.sort_values(["trade_date", "symbol"]).drop_duplicates(["symbol", "trade_date"], keep="last")
    out = out.reset_index(drop=True)

    backup_path = yearly_path.with_suffix(f".bak_{ymd}_merge.parquet")
    if args.backup:
        base.to_parquet(backup_path, index=False)

    out.to_parquet(yearly_path, index=False)
    metadata = build_metadata(year, yearly_path)
    metadata_path.write_text(json.dumps(metadata, ensure_ascii=False, indent=2), encoding="utf-8")

    summary = {
        "date": target_date.strftime("%Y-%m-%d"),
        "yearly_path": str(yearly_path),
        "metadata_path": str(metadata_path),
        "backup_path": str(backup_path) if args.backup else "",
        "old_day_rows": old_day_rows,
        "new_day_rows": int((out["trade_date"] == target_date).sum()),
        "symbols_l1": int(l1["symbol"].nunique()),
        "symbols_l2": int(l2["symbol"].nunique()),
        "symbols_union": int(keys["symbol"].nunique()),
        "total_rows": int(len(out)),
    }
    if args.audit_output:
        args.audit_output.parent.mkdir(parents=True, exist_ok=True)
        args.audit_output.write_text(json.dumps(summary, ensure_ascii=False, indent=2), encoding="utf-8")

    print(json.dumps(summary, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
