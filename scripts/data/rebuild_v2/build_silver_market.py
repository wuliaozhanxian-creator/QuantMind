#!/usr/bin/env python3
"""Build the v2 silver market layer.

Outputs:
- db/market_silver_v2/market_ohlcv_raw_YYYY.parquet
- db/market_silver_v2/adjustment_factors_daily_YYYY.parquet
- db/market_silver_v2/market_base_YYYY.parquet
"""

from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

import numpy as np
import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parents[3]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from scripts.data.rebuild_v2.common import (
    DEFAULT_CSMAR_ROOT,
    DEFAULT_SILVER_DIR,
    coerce_date,
    normalize_symbol_column,
    prefix_symbol,
    read_source_table,
    write_metadata,
)

DEFAULT_FUNDAMENTAL_PATH = PROJECT_ROOT / "db" / "custom" / "fundamental_aligned.parquet"
DEFAULT_FUNDAMENTAL_SLICED_ROOT = PROJECT_ROOT / "db" / "custom"
FUNDAMENTAL_COLUMNS = [
    "symbol",
    "trade_date",
    "open",
    "high",
    "low",
    "close",
    "volume",
    "amount",
    "raw_open",
    "raw_high",
    "raw_low",
    "raw_close",
    "adj_factor",
    "turnover_rate",
    "pct_change",
    "total_mv",
    "float_mv",
]


def build_raw_ohlcv(csmar_root: Path, year: int) -> pd.DataFrame:
    df = read_source_table(csmar_root, "日个股回报率文件", year)
    df = coerce_date(df, "Trddt")
    df = normalize_symbol_column(df, "Stkcd")

    # 过滤非交易日：只保留有成交量的数据（volume > 0）
    # CSMAR原始数据包含节假日记录（volume=0），需要过滤掉
    volume_raw = pd.to_numeric(df["Dnshrtrd"], errors="coerce")
    df = df[volume_raw > 0].copy()

    out = pd.DataFrame(
        {
            "symbol": df["symbol"],
            "trade_date": df["trade_date"],
            "open": pd.to_numeric(df["Opnprc"], errors="coerce"),
            "high": pd.to_numeric(df["Hiprc"], errors="coerce"),
            "low": pd.to_numeric(df["Loprc"], errors="coerce"),
            "close": pd.to_numeric(df["Clsprc"], errors="coerce"),
            "volume": pd.to_numeric(df["Dnshrtrd"], errors="coerce"),
            "amount": pd.to_numeric(df["Dnvaltrd"], errors="coerce"),
            "pre_close": pd.to_numeric(df.get("PreClosePrice"), errors="coerce"),
            "pct_change": pd.to_numeric(df.get("ChangeRatio"), errors="coerce"),
            "ret_with_dividend": pd.to_numeric(df.get("Dretwd"), errors="coerce"),
            "ret_no_dividend": pd.to_numeric(df.get("Dretnd"), errors="coerce"),
            "market_type": df.get("Markettype"),
            "trade_status": df.get("Trdsta"),
            "filling": pd.to_numeric(df.get("Filling"), errors="coerce"),
            "total_mv": pd.to_numeric(df.get("Dsmvtll"), errors="coerce") * 10000,
            "float_mv": pd.to_numeric(df.get("Dsmvosd"), errors="coerce") * 10000,
        }
    )
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    out = out.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    return out


def _read_parquet_maybe_columns(path: Path, columns: list[str]) -> pd.DataFrame:
    try:
        return pd.read_parquet(path, columns=columns)
    except Exception:
        return pd.read_parquet(path)


def _normalize_fundamental_year(df: pd.DataFrame, year: int) -> pd.DataFrame:
    if "symbol" not in df.columns or "trade_date" not in df.columns:
        raise ValueError("fundamental 数据缺少 symbol/trade_date 列")

    df["trade_date"] = pd.to_datetime(df["trade_date"], errors="coerce")
    df = df[df["trade_date"].notna()].copy()
    df = df[df["trade_date"].dt.year == year].copy()
    if df.empty:
        return df

    df["symbol"] = df["symbol"].map(prefix_symbol)
    df = df[df["symbol"].str.match(r"^(SH|SZ|BJ)\d{6}$", na=False)].copy()
    df = df.sort_values(["trade_date", "symbol"]).drop_duplicates(["symbol", "trade_date"], keep="last")
    return df.reset_index(drop=True)


def read_fundamental_year_merged(fundamental_path: Path, year: int) -> pd.DataFrame:
    if not fundamental_path.exists():
        raise FileNotFoundError(f"fundamental merged file not found: {fundamental_path}")
    df = _read_parquet_maybe_columns(fundamental_path, FUNDAMENTAL_COLUMNS)
    return _normalize_fundamental_year(df, year)


def _candidate_slice_files(sliced_root: Path, year: int) -> list[Path]:
    if not sliced_root.exists():
        return []
    pat = re.compile(rf"(^|[^0-9]){year}([^0-9]|$)")
    files = [p for p in sliced_root.rglob("*.parquet") if not p.name.startswith("._")]
    # 排除默认合并文件，避免重复。
    files = [p for p in files if p.name != DEFAULT_FUNDAMENTAL_PATH.name]
    scored: list[tuple[int, Path]] = []
    for p in files:
        target = f"{p.stem} {p.parent.name}"
        if pat.search(target):
            score = 0
            lower = p.name.lower()
            if "fundamental" in lower:
                score += 2
            if "aligned" in lower:
                score += 1
            scored.append((score, p))
    return [p for _, p in sorted(scored, key=lambda x: (-x[0], str(x[1])))]


def read_fundamental_year_sliced(sliced_root: Path, year: int) -> pd.DataFrame:
    candidates = _candidate_slice_files(sliced_root, year)
    if not candidates:
        return pd.DataFrame()
    frames: list[pd.DataFrame] = []
    for p in candidates:
        try:
            part = _read_parquet_maybe_columns(p, FUNDAMENTAL_COLUMNS)
            if "trade_date" not in part.columns:
                continue
            d = pd.to_datetime(part["trade_date"], errors="coerce")
            part = part[d.dt.year == year].copy()
            if not part.empty:
                frames.append(part)
        except Exception:
            continue
    if not frames:
        return pd.DataFrame()
    df = pd.concat(frames, ignore_index=True)
    return _normalize_fundamental_year(df, year)


def read_fundamental_year(
    fundamental_path: Path,
    sliced_root: Path,
    mode: str,
    year: int,
) -> tuple[pd.DataFrame, str]:
    if mode == "merged":
        return read_fundamental_year_merged(fundamental_path, year), "merged"
    if mode == "sliced":
        return read_fundamental_year_sliced(sliced_root, year), "sliced"
    # auto: 优先切片，再回退合并
    sliced = read_fundamental_year_sliced(sliced_root, year)
    if not sliced.empty:
        return sliced, "sliced"
    return read_fundamental_year_merged(fundamental_path, year), "merged"


def build_raw_ohlcv_from_fundamental(
    fundamental_path: Path,
    sliced_root: Path,
    mode: str,
    year: int,
) -> tuple[pd.DataFrame, pd.DataFrame, str]:
    src, mode_used = read_fundamental_year(fundamental_path, sliced_root, mode, year)
    if src.empty:
        return pd.DataFrame(), src, mode_used

    def num(col: str, fallback: str | None = None) -> pd.Series:
        if col in src.columns:
            return pd.to_numeric(src[col], errors="coerce")
        if fallback and fallback in src.columns:
            return pd.to_numeric(src[fallback], errors="coerce")
        return pd.Series(np.nan, index=src.index, dtype=float)

    # silver 原始层保持“未复权”字段语义，对应 fundamental 的 raw_*。
    raw_open = num("raw_open", "open")
    raw_high = num("raw_high", "high")
    raw_low = num("raw_low", "low")
    raw_close = num("raw_close", "close")
    volume = num("volume")
    amount = num("amount")

    out = pd.DataFrame(
        {
            "symbol": src["symbol"],
            "trade_date": src["trade_date"],
            "open": raw_open,
            "high": raw_high,
            "low": raw_low,
            "close": raw_close,
            "volume": volume,
            "amount": amount,
            "pre_close": raw_close.groupby(src["symbol"]).shift(1),
            "pct_change": num("pct_change"),
            "ret_with_dividend": np.nan,
            "ret_no_dividend": np.nan,
            "market_type": np.nan,
            "trade_status": np.nan,
            "filling": np.nan,
            "total_mv": num("total_mv"),
            "float_mv": num("float_mv"),
        }
    )
    out = out[pd.to_numeric(out["volume"], errors="coerce") > 0].copy()
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    out = out.sort_values(["trade_date", "symbol"]).reset_index(drop=True)
    return out, src, mode_used


def build_daily_factors(csmar_root: Path, raw: pd.DataFrame, year: int) -> pd.DataFrame:
    keys = raw[["symbol", "trade_date", "close"]].drop_duplicates(["symbol", "trade_date"], keep="last").copy()
    keys["close"] = pd.to_numeric(keys["close"], errors="coerce")

    # 优先使用“后复权收盘价 / 原始收盘价”直接反推每日后复权累计因子，确保口径闭合。
    bwd = read_source_table(csmar_root, "股票历史日行情信息表(后复权)", year)
    bwd = coerce_date(bwd, "TradingDate")
    bwd = normalize_symbol_column(bwd, "Symbol")
    bwd = bwd[["symbol", "trade_date", "ClosePrice"]].copy()
    bwd["ClosePrice"] = pd.to_numeric(bwd["ClosePrice"], errors="coerce")
    bwd = bwd.drop_duplicates(["symbol", "trade_date"], keep="last")

    factors = keys.merge(bwd, on=["symbol", "trade_date"], how="left")
    factors["bward_factor"] = factors["ClosePrice"] / factors["close"].replace(0, np.nan)
    factors["bward_factor"] = factors["bward_factor"].where(factors["bward_factor"] > 0, np.nan)
    factors["fward_factor"] = 1.0 / factors["bward_factor"].replace(0, np.nan)

    # 回退：若后复权日行情缺失，再尝试使用“复权因子表(日)”补齐。
    try:
        events = read_source_table(csmar_root, "股票价格复权因子表(日)", None)
        events = coerce_date(events, "TradingDate")
        events = normalize_symbol_column(events, "Symbol")
        events = events[["symbol", "trade_date", "CumulateBwardFactor", "CumulateFwardFactor"]].copy()
        events["evt_bward"] = pd.to_numeric(events["CumulateBwardFactor"], errors="coerce")
        events["evt_fward"] = pd.to_numeric(events["CumulateFwardFactor"], errors="coerce")
        events = events.drop_duplicates(["symbol", "trade_date"], keep="last")
        factors = factors.merge(events[["symbol", "trade_date", "evt_bward", "evt_fward"]], on=["symbol", "trade_date"], how="left")
        factors["bward_factor"] = factors["bward_factor"].fillna(factors["evt_bward"])
        factors["fward_factor"] = factors["fward_factor"].fillna(factors["evt_fward"])
    except Exception:
        pass

    factors = factors.sort_values(["symbol", "trade_date"])
    factors[["bward_factor", "fward_factor"]] = factors.groupby("symbol", observed=True)[["bward_factor", "fward_factor"]].ffill()
    factors["bward_factor"] = factors["bward_factor"].fillna(1.0)
    factors["fward_factor"] = factors["fward_factor"].fillna(1.0)
    return factors[["symbol", "trade_date", "bward_factor", "fward_factor"]].reset_index(drop=True)


def build_daily_factors_from_fundamental(raw: pd.DataFrame, fundamental_year: pd.DataFrame) -> pd.DataFrame:
    if raw.empty:
        return pd.DataFrame(columns=["symbol", "trade_date", "bward_factor", "fward_factor"])

    out = fundamental_year[["symbol", "trade_date"]].copy()
    adj = pd.to_numeric(fundamental_year.get("adj_factor"), errors="coerce")
    close_adj = pd.to_numeric(fundamental_year.get("close"), errors="coerce")
    close_raw = pd.to_numeric(fundamental_year.get("raw_close"), errors="coerce")
    ratio = close_adj / close_raw.replace(0, np.nan)
    # 券商源存在 adj_factor==0 的记录，回退用 close/raw_close 反推因子。
    adj = adj.where(adj > 0, np.nan)
    adj = adj.fillna(ratio)
    adj = adj.where(adj > 0, np.nan)
    out["bward_factor"] = adj
    out["fward_factor"] = 1.0 / adj.replace(0, np.nan)
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")

    keys = raw[["symbol", "trade_date"]].drop_duplicates()
    factors = keys.merge(out, on=["symbol", "trade_date"], how="left")
    factors = factors.sort_values(["symbol", "trade_date"])
    factors[["bward_factor", "fward_factor"]] = factors.groupby("symbol", observed=True)[
        ["bward_factor", "fward_factor"]
    ].ffill()
    factors["bward_factor"] = factors["bward_factor"].fillna(1.0)
    factors["fward_factor"] = factors["fward_factor"].fillna(1.0)
    return factors.reset_index(drop=True)


def build_market_base(raw: pd.DataFrame, factors: pd.DataFrame) -> pd.DataFrame:
    out = raw.merge(factors, on=["symbol", "trade_date"], how="left")
    # 复权因子前向填充：先按股票分组排序，再前向填充
    # 如果某天没有复权因子数据，继承上一交易日的因子，而非直接使用1.0
    out = out.sort_values(["symbol", "trade_date"])
    out["bward_factor"] = out.groupby("symbol")["bward_factor"].transform(
        lambda x: x.ffill().fillna(1.0)  # 先前向填充，如果股票从头就没有因子数据才用1.0
    )
    out["fward_factor"] = out.groupby("symbol")["fward_factor"].transform(
        lambda x: x.ffill().fillna(1.0)
    )
    out["factor"] = out["bward_factor"]
    for col in ("open", "high", "low", "close"):
        out[f"adj_{col}"] = out[col] * out["factor"]
    out = out.drop_duplicates(["symbol", "trade_date"], keep="last")
    return out.sort_values(["trade_date", "symbol"]).reset_index(drop=True)


def audit_against_bward(csmar_root: Path, base: pd.DataFrame, year: int) -> dict:
    try:
        bwd = read_source_table(csmar_root, "股票历史日行情信息表(后复权)", year)
        bwd = coerce_date(bwd, "TradingDate")
        bwd = normalize_symbol_column(bwd, "Symbol")
        bwd = bwd[["symbol", "trade_date", "ClosePrice"]].copy()
        bwd["bward_close_source"] = pd.to_numeric(bwd["ClosePrice"], errors="coerce")
        check = base[["symbol", "trade_date", "adj_close"]].merge(
            bwd[["symbol", "trade_date", "bward_close_source"]],
            on=["symbol", "trade_date"],
            how="inner",
        )
        diff = (check["adj_close"] - check["bward_close_source"]).abs()
        return {
            "bward_check_rows": int(len(check)),
            "bward_close_abs_diff_mean": float(diff.mean()) if len(diff) else None,
            "bward_close_abs_diff_p99": float(diff.quantile(0.99)) if len(diff) else None,
            "bward_close_abs_diff_max": float(diff.max()) if len(diff) else None,
        }
    except Exception as exc:
        return {"bward_check_error": str(exc)}


def audit_against_fundamental(base: pd.DataFrame, fundamental_year: pd.DataFrame) -> dict:
    if fundamental_year.empty:
        return {"fundamental_check_rows": 0}
    cols = ["symbol", "trade_date", "close", "raw_close", "adj_factor"]
    src = fundamental_year[[c for c in cols if c in fundamental_year.columns]].copy()
    for c in ("close", "raw_close", "adj_factor"):
        if c not in src.columns:
            src[c] = np.nan
    src["close"] = pd.to_numeric(src.get("close"), errors="coerce")
    src["raw_close"] = pd.to_numeric(src.get("raw_close"), errors="coerce")
    src["adj_factor"] = pd.to_numeric(src.get("adj_factor"), errors="coerce")
    check = base[["symbol", "trade_date", "close", "adj_close"]].merge(
        src[["symbol", "trade_date", "close", "raw_close", "adj_factor"]],
        on=["symbol", "trade_date"],
        how="inner",
        suffixes=("_base_raw", "_src_adj"),
    )
    if check.empty:
        return {"fundamental_check_rows": 0}
    raw_diff = (check["close_base_raw"] - check["raw_close"]).abs()
    adj_diff = (check["adj_close"] - check["close_src_adj"]).abs()
    return {
        "fundamental_check_rows": int(len(check)),
        "raw_close_abs_diff_mean": float(raw_diff.mean()),
        "raw_close_abs_diff_p99": float(raw_diff.quantile(0.99)),
        "raw_close_abs_diff_max": float(raw_diff.max()),
        "adj_close_abs_diff_mean": float(adj_diff.mean()),
        "adj_close_abs_diff_p99": float(adj_diff.quantile(0.99)),
        "adj_close_abs_diff_max": float(adj_diff.max()),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Build QuantMind v2 silver market layer")
    parser.add_argument("--csmar-root", type=Path, default=DEFAULT_CSMAR_ROOT)
    parser.add_argument("--fundamental-path", type=Path, default=DEFAULT_FUNDAMENTAL_PATH)
    parser.add_argument("--fundamental-sliced-root", type=Path, default=DEFAULT_FUNDAMENTAL_SLICED_ROOT)
    parser.add_argument(
        "--fundamental-mode",
        choices=("auto", "sliced", "merged"),
        default="auto",
        help="fundamental源模式：auto=优先切片再回退合并",
    )
    parser.add_argument(
        "--source",
        choices=("auto", "fundamental", "csmar"),
        default="auto",
        help="auto=优先fundamental，缺失时回退CSMAR",
    )
    parser.add_argument("--output-dir", type=Path, default=DEFAULT_SILVER_DIR)
    parser.add_argument("--years", nargs="+", type=int, required=True)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    args.output_dir.mkdir(parents=True, exist_ok=True)
    for year in args.years:
        source_used = "csmar"
        fundamental_mode_used = ""
        using_fundamental = False
        fundamental_year = pd.DataFrame()

        if args.source in ("auto", "fundamental"):
            try:
                raw, fundamental_year, fundamental_mode_used = build_raw_ohlcv_from_fundamental(
                    args.fundamental_path,
                    args.fundamental_sliced_root,
                    args.fundamental_mode,
                    year,
                )
                if not raw.empty:
                    factors = build_daily_factors_from_fundamental(raw, fundamental_year)
                    source_used = f"fundamental_{fundamental_mode_used}"
                    using_fundamental = True
                elif args.source == "fundamental":
                    raise ValueError(f"fundamental 数据源在 {year} 年无可用数据")
            except Exception:
                if args.source == "fundamental":
                    raise
                raw = pd.DataFrame()
                factors = pd.DataFrame()
        else:
            raw = pd.DataFrame()
            factors = pd.DataFrame()

        if not using_fundamental:
            raw = build_raw_ohlcv(args.csmar_root, year)
            factors = build_daily_factors(args.csmar_root, raw, year)

        base = build_market_base(raw, factors)
        if using_fundamental:
            audit = audit_against_fundamental(base, fundamental_year)
        else:
            audit = audit_against_bward(args.csmar_root, base, year)

        metadata = {
            "year": year,
            "source": source_used,
            "fundamental_mode_requested": args.fundamental_mode,
            "fundamental_mode_used": fundamental_mode_used or None,
            "raw_rows": int(len(raw)),
            "factor_rows": int(len(factors)),
            "base_rows": int(len(base)),
            "min_date": base["trade_date"].min(),
            "max_date": base["trade_date"].max(),
            "symbols": int(base["symbol"].nunique()),
            **audit,
        }
        print(metadata)
        if not args.dry_run:
            raw.to_parquet(args.output_dir / f"market_ohlcv_raw_{year}.parquet", index=False)
            factors.to_parquet(args.output_dir / f"adjustment_factors_daily_{year}.parquet", index=False)
            base.to_parquet(args.output_dir / f"market_base_{year}.parquet", index=False)
            write_metadata(args.output_dir / f"market_base_{year}.metadata.json", metadata)


if __name__ == "__main__":
    main()
