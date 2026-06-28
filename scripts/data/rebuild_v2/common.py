#!/usr/bin/env python3
"""Shared helpers for the QuantMind v2 feature rebuild pipeline.

The v2 pipeline treats CSMAR raw tables as immutable input and produces
rebuildable parquet caches.  It intentionally does not write to PostgreSQL.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Iterable

import numpy as np
import pandas as pd

from backend.shared.stock_utils import StockCodeUtil


PROJECT_ROOT = Path(__file__).resolve().parents[3]
DEFAULT_CSMAR_ROOT = PROJECT_ROOT / "M2SSD" / "CSMAR"
DEFAULT_CATALOG = PROJECT_ROOT / "config" / "features" / "model_training_feature_catalog_v1.json"
DEFAULT_SILVER_DIR = PROJECT_ROOT / "db" / "market_silver_v2"
DEFAULT_FEATURE_DIR = PROJECT_ROOT / "db" / "feature_snapshots"
DEFAULT_AUDIT_DIR = PROJECT_ROOT / "db" / "feature_audit_v2"


def feature_keys(catalog_path: Path = DEFAULT_CATALOG) -> list[str]:
    catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
    keys: list[str] = []
    for category in catalog["categories"]:
        for feature in category["features"]:
            if feature.get("enabled", True):
                keys.append(feature["key"])
    return keys


def prefix_symbol(value: object) -> str:
    if pd.isna(value):
        return ""
    text = str(value).strip()
    if text.endswith(".0"):
        text = text[:-2]
    if text.isdigit():
        text = text.zfill(6)
    return StockCodeUtil.to_prefix(text)


def normalize_symbol_column(df: pd.DataFrame, source_col: str, target_col: str = "symbol") -> pd.DataFrame:
    df[target_col] = df[source_col].map(prefix_symbol)
    return df[df[target_col].str.match(r"^(SH|SZ|BJ)\d{6}$", na=False)].copy()


def coerce_date(df: pd.DataFrame, source_col: str, target_col: str = "trade_date") -> pd.DataFrame:
    df[target_col] = pd.to_datetime(df[source_col], errors="coerce")
    return df[df[target_col].notna()].copy()


def read_source_table(csmar_root: Path, table_dir: str, year: int | None = None) -> pd.DataFrame:
    """Read merged parquet when present, otherwise fall back to the annual CSVs."""

    table_path = csmar_root / table_dir
    parquet = table_path / f"{table_dir}_合并.parquet"
    if parquet.exists():
        df = pd.read_parquet(parquet)
        if year is not None:
            date_col = infer_date_column(df.columns)
            if date_col:
                d = pd.to_datetime(df[date_col], errors="coerce")
                df_filtered = df[d.dt.year == year].copy()
                if not df_filtered.empty:
                    return df_filtered
        else:
            return df

    if year is not None:
        annual_dir = table_path / "年度切分"
        if annual_dir.exists():
            candidates = sorted(annual_dir.glob(f"*_{year}.csv"))
            # 过滤掉macOS隐藏文件
            candidates = [p for p in candidates if not p.name.startswith('._')]
            if candidates:
                dfs = []
                for p in candidates:
                    try:
                        df = pd.read_csv(p, low_memory=False, encoding='utf-8-sig')
                        dfs.append(df)
                    except UnicodeDecodeError:
                        df = pd.read_csv(p, low_memory=False, encoding='gbk')
                        dfs.append(df)
                return pd.concat(dfs, ignore_index=True) if dfs else pd.DataFrame()

    raise FileNotFoundError(f"Cannot find CSMAR source for {table_dir}")


def infer_date_column(columns: Iterable[str]) -> str | None:
    for col in ("Trddt", "TradingDate", "Idxtrd01", "SgnDate", "Accper", "Annodt"):
        if col in columns:
            return col
    return None


def safe_div(num: pd.Series | np.ndarray, den: pd.Series | np.ndarray) -> pd.Series:
    out = pd.Series(num) / pd.Series(den).replace(0, np.nan)
    return out.replace([np.inf, -np.inf], np.nan)


def safe_div_zero(num: pd.Series | np.ndarray, den: pd.Series | np.ndarray) -> pd.Series:
    num_s = pd.Series(num)
    den_s = pd.Series(den)
    out = num_s / den_s.replace(0, np.nan)
    out = out.replace([np.inf, -np.inf], np.nan)
    return out.where(den_s != 0, 0.0)


def zscore_by_date(df: pd.DataFrame, col: str) -> pd.Series:
    grouped = df.groupby("trade_date", observed=True)[col]
    mean = grouped.transform("mean")
    std = grouped.transform("std").replace(0, np.nan)
    return (df[col] - mean) / std


def lag_daily_features(df: pd.DataFrame, columns: list[str]) -> pd.DataFrame:
    if not columns:
        return df
    df = df.sort_values(["symbol", "trade_date"]).copy()
    df[columns] = df.groupby("symbol", observed=True)[columns].shift(1)
    return df


def write_metadata(path: Path, payload: dict) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(payload, ensure_ascii=False, indent=2, default=str), encoding="utf-8")
