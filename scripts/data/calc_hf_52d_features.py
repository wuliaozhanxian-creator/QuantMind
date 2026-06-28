"""
QuantMind 高频因子计算脚本 — 单日批处理版
========================================
策略：
  1. 每次仅处理1天（控制总体内存，防止OOM）
  2. 单日内优先按股票批次切分，避免一次性持有全量 L2 数据
  3. 每个批次内再做有限并行，兼顾内存与速度
"""

import glob
import argparse
import math
import numpy as np
import pandas as pd
import tempfile
from pathlib import Path
from multiprocessing import Pool, cpu_count

# ==================== 全局配置 ====================
PROJECT_ROOT    = Path(__file__).resolve().parents[2]
BAIDUDESK_DIR   = PROJECT_ROOT / "baidudesk"
OUTPUT_DIR      = PROJECT_ROOT / "db" / "hf_features"

# 单日内股票切分并行度，榨干64核
N_WORKERS = min(cpu_count(), 64)

OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

PAUSED_L2_FIELDS = {
    "flow_large_net_amount",
    "flow_medium_net_amount",
    "liq_trade_count",
    "liq_avg_trade_size",
    "flow_aqsp",
    "flow_net_order_count",
    "flow_net_order_ratio",
    "flow_large_net_order",
    "flow_large_order_ratio",
    "flow_small_net_amount",
    "flow_small_net_ratio",
    "flow_medium_net_ratio",
    "flow_large_net_ratio",
    "micro_imbalance_count",
    "micro_imbalance_small",
    "micro_imbalance_medium",
    "micro_imbalance_large",
    "flow_net_amount",
    "flow_net_amount_ratio",
    "micro_imbalance_volume",
    "micro_imbalance_amount",
    "flow_vpin",
    "vol_realized_rv",
    "vol_realized_rrv",
    "vol_realized_rskew",
    "vol_realized_rkurt",
    "vol_jump_zadj",
    "vol_jump_rjv_ratio",
    "vol_jump_sjv_ratio",
    "micro_jump_flag",
}

SNAP_COLUMNS = ['TradingDay', 'SecuCode', 'TickTime', 'TickTimeDiff', 'Price', 'AskPrice1', 'BidPrice1', 'Volume']
DEAL_COLUMNS = ['TradingDay', 'SecuCode', 'DealTime', 'Price', 'Volume', 'Side', 'BuyID', 'SellID']

_G_SNAP = None
_G_DEAL = None
_G_SNAP_BUCKETS = None
_G_DEAL_BUCKETS = None
_G_TMP_DIR = None
_G_DATE_STR = None

# ==================== 工具函数 ====================
def to_prefix(code_str: str) -> str:
    s = str(code_str).strip()
    if s.isdigit():
        s = s.zfill(6)
        return 'SH' + s if s.startswith('6') else 'SZ' + s
    return s

def downcast(df: pd.DataFrame) -> pd.DataFrame:
    for c in df.select_dtypes('float64').columns:
        df[c] = df[c].astype('float32')
    for c in df.select_dtypes('int64').columns:
        if c.lower().endswith('id'):
            continue
        df[c] = pd.to_numeric(df[c], downcast='integer')
    return df

def normalize_secu_code(series: pd.Series) -> pd.Series:
    s = series.astype(str).str.strip()
    digits = s.str.fullmatch(r"\d+")
    s = s.where(~digits, s.str.zfill(6))
    mask6 = s.str.len() == 6
    sh_mask = mask6 & s.str.startswith('6')
    sz_mask = mask6 & (~s.str.startswith('6'))
    s = s.where(~sh_mask, 'SH' + s)
    s = s.where(~sz_mask, 'SZ' + s)
    return s

def ensure_datetime(df: pd.DataFrame) -> pd.DataFrame:
    if 'dt' not in df.columns:
        dt = pd.to_datetime(
            df['TradingDay'].astype(str) + df['TickTime'].astype(str).str.zfill(9) + '000',
            format='%Y%m%d%H%M%S%f',
            errors='coerce',
        )
        df = df.assign(dt=dt)
    return df

def safe_weighted_avg(v: pd.Series, w: pd.Series) -> float:
    vv = pd.to_numeric(v, errors='coerce')
    ww = pd.to_numeric(w, errors='coerce').fillna(0)
    mask = vv.notna() & ww.gt(0)
    if not mask.any():
        return np.nan
    return float((vv[mask] * ww[mask]).sum() / ww[mask].sum())

def standard_normal_cdf(x: pd.Series) -> pd.Series:
    return 0.5 * (1.0 + pd.Series(x, index=x.index).map(lambda t: math.erf(float(t) / math.sqrt(2.0))))

def safe_read_parquet(path: Path, columns: list[str], filters: list[tuple[str, str, list]] | None = None) -> pd.DataFrame:
    if filters:
        try:
            return pd.read_parquet(path, columns=columns, filters=filters)
        except Exception:
            pass
    return pd.read_parquet(path, columns=columns)

# ==================== 特征计算模块 ====================
def calc_spread(df_snap: pd.DataFrame) -> pd.DataFrame:
    df = df_snap[(df_snap['AskPrice1'] > 0) & (df_snap['BidPrice1'] > 0) & (df_snap['Price'] > 0)].copy()
    if df.empty:
        return pd.DataFrame()

    df['mid'] = (df['AskPrice1'] + df['BidPrice1']) / 2.0
    df = df[df['mid'] > 0].copy()
    if df.empty:
        return pd.DataFrame()

    # 文档口径：相对价差按 bp 输出
    df['qsp_rel'] = (df['AskPrice1'] - df['BidPrice1']) / df['mid'] * 10000.0
    df['esp_rel'] = 2.0 * (df['Price'] - df['mid']).abs() / df['mid'] * 10000.0
    df['aqsp'] = df['AskPrice1'] - df['BidPrice1']
    df['aesp'] = 2.0 * (df['Price'] - df['mid']).abs()
    df['amt_tick'] = df['Price'] * df['Volume']

    if 'TickTimeDiff' in df.columns:
        w_time = pd.to_numeric(df['TickTimeDiff'], errors='coerce').fillna(0)
    else:
        w_time = pd.Series(1.0, index=df.index)
    df['w_time'] = w_time.mask(w_time <= 0, 0)

    key = ['TradingDay', 'SecuCode']
    rows: list[dict[str, float | int | str]] = []
    for (trd, secu), g in df.groupby(key, sort=False):
        vol_w = pd.to_numeric(g['Volume'], errors='coerce').fillna(0)
        amt_w = pd.to_numeric(g['amt_tick'], errors='coerce').fillna(0)
        t_w = pd.to_numeric(g['w_time'], errors='coerce').fillna(0)
        t_eff = t_w if t_w.sum() > 0 else pd.Series(1.0, index=g.index)
        rows.append({
            'TradingDay': trd,
            'SecuCode': secu,
            'qsp_equal': float(pd.to_numeric(g['qsp_rel'], errors='coerce').mean()),
            'esp_equal': float(pd.to_numeric(g['esp_rel'], errors='coerce').mean()),
            'qsp_time': safe_weighted_avg(g['qsp_rel'], t_eff),
            'esp_time': safe_weighted_avg(g['esp_rel'], t_eff),
            'qsp_volume': safe_weighted_avg(g['qsp_rel'], vol_w),
            'esp_volume': safe_weighted_avg(g['esp_rel'], vol_w),
            'qsp_amount': safe_weighted_avg(g['qsp_rel'], amt_w),
            'esp_amount': safe_weighted_avg(g['esp_rel'], amt_w),
            'aqsp_equal': float(pd.to_numeric(g['aqsp'], errors='coerce').mean()),
            'aesp_equal': float(pd.to_numeric(g['aesp'], errors='coerce').mean()),
            'aqsp_time': safe_weighted_avg(g['aqsp'], t_eff),
            'aesp_time': safe_weighted_avg(g['aesp'], t_eff),
            'aqsp_volume': safe_weighted_avg(g['aqsp'], vol_w),
            'aesp_volume': safe_weighted_avg(g['aesp'], vol_w),
            'aqsp_amount': safe_weighted_avg(g['aqsp'], amt_w),
            'aesp_amount': safe_weighted_avg(g['aesp'], amt_w),
            'spread_num': int(len(g)),
        })

    out = pd.DataFrame(rows)
    out['micro_qsp_time'] = out['qsp_time']
    out['micro_esp_time'] = out['esp_time']
    out['micro_qsp_volume'] = out['qsp_volume']
    out['micro_esp_volume'] = out['esp_volume']
    out['micro_qsp_amount'] = out['qsp_amount']
    out['micro_esp_amount'] = out['esp_amount']
    out['micro_qsp_equal'] = out['qsp_equal']
    out['micro_esp_equal'] = out['esp_equal']
    out['micro_aqsp_equal'] = out['aqsp_equal']
    out['micro_effective_spread'] = out['esp_equal']
    out['micro_quoted_spread'] = out['qsp_equal']
    out['flow_qsp'] = out['qsp_equal']
    out['flow_esp'] = out['esp_equal']
    out['flow_qsp_time'] = out['qsp_time']
    out['flow_esp_time'] = out['esp_time']
    out['flow_aqsp'] = out['aqsp_equal']
    return out

def calc_realized_and_jump(df_snap: pd.DataFrame) -> pd.DataFrame:
    cols = ['TradingDay', 'SecuCode', 'TickTime', 'Price']
    df = ensure_datetime(df_snap[cols].copy())
    df = df.dropna(subset=['dt', 'Price'])
    df = df[df['Price'] > 0].copy()
    if df.empty:
        return pd.DataFrame()

    bars = (
        df.groupby(['TradingDay', 'SecuCode', pd.Grouper(key='dt', freq='5min')], sort=False)['Price']
        .agg(close='last', high='max', low='min')
        .reset_index()
    )
    bars = bars[(bars['close'] > 0) & (bars['high'] > 0) & (bars['low'] > 0)].copy()
    if bars.empty:
        return pd.DataFrame()

    out_rows: list[dict[str, float | int | str]] = []
    mu_43 = (2 ** (2 / 3)) * math.gamma(7 / 6) / math.gamma(1 / 2)
    z_alpha = 1.645  # A 档：5%

    for (trd, secu), g in bars.groupby(['TradingDay', 'SecuCode'], sort=False):
        g = g.sort_values('dt')
        r = 100.0 * np.log(g['close']).diff().dropna()
        rng = 100.0 * (np.log(g['high']) - np.log(g['low']))
        rng = rng.replace([np.inf, -np.inf], np.nan).dropna()

        m = int(len(r))
        if m <= 0:
            continue
        rv = float((r ** 2).sum())
        rrv = float((rng ** 2).sum()) if not rng.empty else np.nan
        r3 = float((r ** 3).sum())
        r4 = float((r ** 4).sum())
        rskew = np.nan if rv <= 0 else float(np.sqrt(m) * r3 / (rv ** 1.5))
        rkurt = np.nan if rv <= 0 else float(m * r4 / (rv ** 2))

        abs_r = r.abs().reset_index(drop=True)
        bv = np.nan
        tq = np.nan
        z_adj = np.nan
        is_jump = 0
        rjv = np.nan
        rs_n = float(((r[r < 0]) ** 2).sum())
        rs_p = float(((r[r > 0]) ** 2).sum())
        sjv = rs_p - rs_n
        num_n = int((r < 0).sum())
        num_p = int((r > 0).sum())

        if m >= 2:
            bv_sum = float((abs_r.iloc[1:].to_numpy() * abs_r.iloc[:-1].to_numpy()).sum())
            bv = float((math.pi * m / (2 * m - 1)) * bv_sum)
        if m >= 3 and bv is not None and bv > 0:
            tri = (
                (abs_r.iloc[2:].to_numpy() ** (4 / 3))
                * (abs_r.iloc[1:-1].to_numpy() ** (4 / 3))
                * (abs_r.iloc[:-2].to_numpy() ** (4 / 3))
            )
            tq = float(m * (mu_43 ** -3) * (m / (m - 2)) * tri.sum())
            den = ((math.pi ** 2) / 4 + math.pi - 5) * max(1.0, tq / (bv ** 2)) if bv > 0 else np.nan
            if den and den > 0 and rv > 0:
                z_adj = float((np.sqrt(m) * (np.log(rv) - np.log(bv))) / np.sqrt(den))
                is_jump = int(z_adj > z_alpha)
                rjv = float((rv - bv) if is_jump else 0.0)

        out_rows.append({
            'TradingDay': trd,
            'SecuCode': secu,
            'vol_realized_rv': rv,
            'vol_realized_rrv': rrv,
            'vol_realized_rskew': rskew,
            'vol_realized_rkurt': rkurt,
            'jump_alpha': 'A',
            'jump_bv': bv,
            'vol_jump_zadj': z_adj,
            'micro_jump_flag': is_jump,
            'jump_rjv_raw': rjv,
            'jump_rs_n_raw': rs_n,
            'jump_rs_p_raw': rs_p,
            'jump_sjv_raw': sjv,
            'num_5m': m,
            'num_neg_5m': num_n,
            'num_pos_5m': num_p,
        })

    agg = pd.DataFrame(out_rows)
    if agg.empty:
        return agg
    agg['vol_jump_rjv_ratio'] = agg['jump_rjv_raw'] / agg['vol_realized_rv'].replace(0, np.nan)
    agg['vol_jump_sjv_ratio'] = agg['jump_sjv_raw'] / agg['vol_realized_rv'].replace(0, np.nan)
    return agg

def calc_vpin(df_deal: pd.DataFrame, n_buckets: int = 50) -> pd.DataFrame:
    cols = ['TradingDay', 'SecuCode', 'DealTime', 'Price', 'Volume']
    if any(c not in df_deal.columns for c in cols):
        return pd.DataFrame()
    df = df_deal[cols].copy()
    df = df[(df['Price'] > 0) & (df['Volume'] > 0)].copy()
    if df.empty:
        return pd.DataFrame()
    df['dt'] = pd.to_datetime(
        df['TradingDay'].astype(str) + df['DealTime'].astype(str).str.zfill(9) + '000',
        format='%Y%m%d%H%M%S%f',
        errors='coerce',
    )
    df = df.dropna(subset=['dt'])
    if df.empty:
        return pd.DataFrame()
    one_min = (
        df.groupby(['TradingDay', 'SecuCode', pd.Grouper(key='dt', freq='1min')], sort=False)
        .agg(price=('Price', 'last'), volume=('Volume', 'sum'))
        .reset_index()
    )
    out_rows: list[dict[str, float | int | str]] = []
    for (trd, secu), g in one_min.groupby(['TradingDay', 'SecuCode'], sort=False):
        g = g.sort_values('dt')
        dp = g['price'].diff()
        sigma_dp = float(dp.std())
        if not np.isfinite(sigma_dp) or sigma_dp <= 0:
            z = pd.Series(np.full(len(g), 0.5), index=g.index)
        else:
            z = standard_normal_cdf(dp.fillna(0) / sigma_dp)
        vb = pd.to_numeric(g['volume'], errors='coerce').fillna(0) * z
        vs = pd.to_numeric(g['volume'], errors='coerce').fillna(0) * (1 - z)
        vpin_raw = (vb - vs).abs()
        if n_buckets > 1:
            vpin = float(vpin_raw.rolling(n_buckets, min_periods=max(2, n_buckets // 2)).sum().iloc[-1])
            v_denom = float((g['volume'].rolling(n_buckets, min_periods=max(2, n_buckets // 2)).sum().iloc[-1]))
            vpin = np.nan if (not np.isfinite(vpin) or not np.isfinite(v_denom) or v_denom <= 0) else float(vpin / v_denom)
        else:
            denom = float(g['volume'].sum())
            vpin = np.nan if denom <= 0 else float(vpin_raw.sum() / denom)
        out_rows.append({
            'TradingDay': trd,
            'SecuCode': secu,
            'vpin_approx': vpin,
            'num_1m': int(len(g)),
            'vpin_n': int(n_buckets),
        })
    return pd.DataFrame(out_rows)

def calc_imbalance(df_deal: pd.DataFrame) -> pd.DataFrame:
    df = df_deal.copy()
    if df.empty:
        return pd.DataFrame()
    df = df[(df['Price'] > 0) & (df['Volume'] > 0)].copy()
    if df.empty:
        return pd.DataFrame()
    df['amount'] = df['Price'] * df['Volume']
    df = df[df['Side'].isin([0, 1])].copy()
    if df.empty:
        return pd.DataFrame()
    key = ['TradingDay', 'SecuCode']
    buy = (df['Side'] == 0).astype(float)
    sell = (df['Side'] == 1).astype(float)
    # A 股口径：特大>=100万，大单[20万,100万)，中单[5万,20万)，小单<5万
    xl = (df['amount'] >= 1_000_000).astype(float)
    lg = ((df['amount'] >= 200_000) & (df['amount'] < 1_000_000)).astype(float)
    md = ((df['amount'] >= 50_000) & (df['amount'] < 200_000)).astype(float)
    sm = (df['amount'] < 50_000).astype(float)

    feat = pd.DataFrame({
        'TradingDay': df['TradingDay'],
        'SecuCode': df['SecuCode'],
        'B_Num': buy,
        'S_Num': sell,
        'B_Volume': df['Volume'] * buy,
        'S_Volume': df['Volume'] * sell,
        'B_Amount': df['amount'] * buy,
        'S_Amount': df['amount'] * sell,
        # 文档中 Order 为成交笔数口径，此处与 Num 一致
        'B_Order': buy,
        'S_Order': sell,
        'B_Amount_L': df['amount'] * buy * xl,
        'S_Amount_L': df['amount'] * sell * xl,
        'B_Amount_B': df['amount'] * buy * lg,
        'S_Amount_B': df['amount'] * sell * lg,
        'B_Amount_M': df['amount'] * buy * md,
        'S_Amount_M': df['amount'] * sell * md,
        'B_Amount_S': df['amount'] * buy * sm,
        'S_Amount_S': df['amount'] * sell * sm,
        'B_Num_L': buy * xl,
        'S_Num_L': sell * xl,
        'B_Order_L': buy * xl,
        'S_Order_L': sell * xl,
    })
    res = feat.groupby(key, sort=False).sum(numeric_only=True)
    eps = 1e-10

    b_num = res['B_Num']; s_num = res['S_Num']
    b_vol = res['B_Volume']; s_vol = res['S_Volume']
    b_amt = res['B_Amount']; s_amt = res['S_Amount']
    b_ord = res['B_Order']; s_ord = res['S_Order']
    b_xl_amt = res['B_Amount_L']; s_xl_amt = res['S_Amount_L']
    b_lg_amt = res['B_Amount_B']; s_lg_amt = res['S_Amount_B']
    b_md_amt = res['B_Amount_M']; s_md_amt = res['S_Amount_M']
    b_sm_amt = res['B_Amount_S']; s_sm_amt = res['S_Amount_S']
    b_xl_ord = res['B_Order_L']; s_xl_ord = res['S_Order_L']

    res['flow_net_amount'] = b_amt - s_amt
    res['flow_net_amount_ratio'] = (b_amt - s_amt) / (b_amt + s_amt + eps)
    res['flow_large_net_amount'] = b_xl_amt - s_xl_amt
    res['flow_large_net_ratio'] = (b_xl_amt - s_xl_amt) / (b_xl_amt + s_xl_amt + eps)
    res['flow_medium_net_amount'] = b_lg_amt - s_lg_amt
    res['flow_medium_net_ratio'] = (b_lg_amt - s_lg_amt) / (b_lg_amt + s_lg_amt + eps)
    res['flow_small_net_amount'] = b_sm_amt - s_sm_amt
    res['flow_small_net_ratio'] = (b_sm_amt - s_sm_amt) / (b_sm_amt + s_sm_amt + eps)
    res['flow_net_order_count'] = b_ord - s_ord
    res['flow_net_order_ratio'] = (b_ord - s_ord) / (b_ord + s_ord + eps)
    res['flow_large_net_order'] = b_xl_ord - s_xl_ord
    res['flow_large_order_ratio'] = (b_xl_ord - s_xl_ord) / (b_xl_ord + s_xl_ord + eps)
    res['net_inflow_L'] = b_xl_amt - s_xl_amt
    res['micro_imbalance_volume'] = (b_vol - s_vol) / (b_vol + s_vol + eps)
    res['micro_imbalance_amount'] = (b_amt - s_amt) / (b_amt + s_amt + eps)
    res['micro_imbalance_count'] = (b_num - s_num) / (b_num + s_num + eps)
    res['micro_imbalance_large'] = (b_xl_amt - s_xl_amt) / (b_xl_amt + s_xl_amt + eps)
    res['micro_imbalance_medium'] = (b_md_amt - s_md_amt) / (b_md_amt + s_md_amt + eps)
    res['micro_imbalance_small'] = (b_sm_amt - s_sm_amt) / (b_sm_amt + s_sm_amt + eps)
    res['liq_trade_count'] = b_num + s_num
    return res.reset_index()

def calc_pressure(df: pd.DataFrame) -> pd.DataFrame:
    def zs(col):
        s = pd.to_numeric(df.get(col, pd.Series(dtype=float)), errors='coerce')
        std = s.std()
        return (s - s.mean()) / std if std and std > 0 else pd.Series(0.0, index=df.index)
    df['flow_pressure_index']  = pd.concat([zs('flow_net_amount'), zs('flow_vpin'), zs('flow_esp')], axis=1).mean(axis=1)
    df['micro_pressure_score'] = pd.concat([zs('flow_esp'), zs('flow_vpin'), zs('micro_imbalance_amount')], axis=1).mean(axis=1)
    return df

# ==================== 分片计算包装函数 ====================
def process_chunk(args):
    df_snap_chunk, df_deal_chunk = args
    key = ['TradingDay', 'SecuCode']
    
    res_spread = calc_spread(df_snap_chunk)
    res_rv_jump = calc_realized_and_jump(df_snap_chunk)
    res_imb = calc_imbalance(df_deal_chunk)
    res_vpin = calc_vpin(df_deal_chunk, n_buckets=50)
    
    df_hf = pd.DataFrame()
    if not res_spread.empty and not res_rv_jump.empty:
        df_hf = res_spread.merge(res_rv_jump, on=key, how='outer')
    elif not res_spread.empty: df_hf = res_spread
    elif not res_rv_jump.empty: df_hf = res_rv_jump

    if not df_hf.empty and not res_imb.empty:
        df_hf = df_hf.merge(res_imb, on=key, how='outer')
    elif not df_hf.empty: pass
    elif not res_imb.empty: df_hf = res_imb

    if not df_hf.empty and not res_vpin.empty:
        df_hf = df_hf.merge(res_vpin, on=key, how='outer')
    elif df_hf.empty and not res_vpin.empty:
        df_hf = res_vpin

    return df_hf

def process_chunk_by_bucket(bucket_id: int) -> str | None:
    snap_chunk = _G_SNAP[_G_SNAP_BUCKETS == bucket_id]
    deal_chunk = _G_DEAL[_G_DEAL_BUCKETS == bucket_id]
    df_hf = process_chunk((snap_chunk, deal_chunk))
    if df_hf.empty:
        return None
    out = Path(_G_TMP_DIR) / f"hf_{_G_DATE_STR}_bucket_{bucket_id:03d}.parquet"
    df_hf.to_parquet(out, index=False)
    return str(out)

# ==================== 单日处理逻辑 ====================
def process_day_parallel(
    date_str: str,
    workers: int | None = None,
    symbols: set[str] | None = None,
    *,
    out_path: Path | None = None,
    write_output: bool = True,
) -> pd.DataFrame | None:
    global _G_SNAP, _G_DEAL, _G_SNAP_BUCKETS, _G_DEAL_BUCKETS, _G_TMP_DIR, _G_DATE_STR
    snap_path = BAIDUDESK_DIR / f"snapshot_{date_str}.parquet"
    deal_path = BAIDUDESK_DIR / f"deal_{date_str}.parquet"
    target_path = out_path or (OUTPUT_DIR / f"hf_{date_str}.parquet")
    if not snap_path.exists() or not deal_path.exists():
        print(f"[SKIP] {date_str}: 文件不齐")
        return None

    print(f"========== 读取 {date_str} 数据 ==========")
    n_workers = int(workers or N_WORKERS)
    n_workers = max(1, n_workers)
    if symbols and len(symbols) <= 4:
        n_workers = 1

    symbol_filters = None
    if symbols:
        raw6 = []
        for s in symbols:
            if len(s) == 8 and (s.startswith('SH') or s.startswith('SZ') or s.startswith('BJ')):
                raw6.append(s[2:])
            else:
                raw6.append(s)
        vals = list(set(raw6))
        if vals and all(str(x).isdigit() for x in vals):
            symbol_filters = [('SecuCode', 'in', [int(x) for x in vals])]
        else:
            symbol_filters = [('SecuCode', 'in', list(set(raw6 + list(symbols))))]

    df_snap = safe_read_parquet(snap_path, columns=SNAP_COLUMNS, filters=symbol_filters)
    df_snap['Price'] = df_snap['Price'] / 100.0
    df_snap['AskPrice1'] = df_snap['AskPrice1'] / 100.0
    df_snap['BidPrice1'] = df_snap['BidPrice1'] / 100.0
    df_snap['SecuCode'] = normalize_secu_code(df_snap['SecuCode'])
    if symbols:
        df_snap = df_snap[df_snap['SecuCode'].isin(symbols)].copy()
    if not df_snap.empty:
        df_snap = ensure_datetime(df_snap)
    df_snap = downcast(df_snap)

    df_deal = safe_read_parquet(deal_path, columns=DEAL_COLUMNS, filters=symbol_filters)
    df_deal['Price'] = df_deal['Price'] / 100.0
    df_deal['SecuCode'] = normalize_secu_code(df_deal['SecuCode'])
    if symbols:
        df_deal = df_deal[df_deal['SecuCode'].isin(symbols)].copy()
    df_deal = downcast(df_deal)
    if df_snap.empty and df_deal.empty:
        print(f"[WARN] {date_str} 在指定股票范围内无数据。")
        return None

    snap_codes, _ = pd.factorize(df_snap['SecuCode'], sort=False)
    deal_codes, _ = pd.factorize(df_deal['SecuCode'], sort=False)
    _G_SNAP_BUCKETS = np.mod(snap_codes, n_workers)
    _G_DEAL_BUCKETS = np.mod(deal_codes, n_workers)
    _G_SNAP = df_snap
    _G_DEAL = df_deal

    print(f"========== 股票已分桶为 {n_workers} 份，启动并行计算 ==========")
    with tempfile.TemporaryDirectory(prefix=f"hf_{date_str}_") as tmp_dir:
        _G_TMP_DIR = tmp_dir
        _G_DATE_STR = date_str
        if n_workers == 1:
            paths = [process_chunk_by_bucket(0)]
        else:
            with Pool(processes=n_workers) as pool:
                paths = pool.map(process_chunk_by_bucket, range(n_workers))
        part_paths = [p for p in paths if p]
        if not part_paths:
            print(f"[WARN] {date_str} 没有任何特征生成。")
            return None
        parts = [pd.read_parquet(p) for p in part_paths]
        df_hf_all = pd.concat(parts, ignore_index=True)

    _G_SNAP = None
    _G_DEAL = None
    _G_SNAP_BUCKETS = None
    _G_DEAL_BUCKETS = None
    _G_TMP_DIR = None
    _G_DATE_STR = None

    df_hf_all.rename(columns={'TradingDay':'trade_date','SecuCode':'symbol'}, inplace=True)
    df_hf_all['trade_date'] = pd.to_datetime(df_hf_all['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')
    if 'vpin_approx' in df_hf_all.columns:
        df_hf_all['flow_vpin'] = df_hf_all['vpin_approx']
    df_hf_all = calc_pressure(df_hf_all)
    for col in PAUSED_L2_FIELDS:
        if col in df_hf_all.columns:
            df_hf_all[col] = np.nan

    if write_output:
        df_hf_all.to_parquet(target_path, index=False)
        print(f"[OK] {date_str} 完成并行计算 → {df_hf_all.shape}  保存至 {target_path}")
    else:
        print(f"[OK] {date_str} 完成并行计算 → {df_hf_all.shape}")
    return df_hf_all

def chunked_list(items: list[str], batch_size: int) -> list[list[str]]:
    if batch_size <= 0:
        return [items]
    return [items[i:i + batch_size] for i in range(0, len(items), batch_size)]

def process_day_batched(
    date_str: str,
    batch_size: int,
    batch_workers: int,
    symbols: set[str] | None = None,
) -> pd.DataFrame | None:
    snap_path = BAIDUDESK_DIR / f"snapshot_{date_str}.parquet"
    if not snap_path.exists():
        print(f"[SKIP] {date_str}: 文件不齐")
        return None

    print(f"========== 读取 {date_str} 股票列表 ==========")
    sym = pd.read_parquet(snap_path, columns=['SecuCode'])
    all_symbols = normalize_secu_code(sym['SecuCode']).dropna().astype(str).str.strip().str.upper()
    if symbols:
        all_symbols = all_symbols[all_symbols.isin(symbols)]
    all_symbols = sorted(set(all_symbols.tolist()))
    if not all_symbols:
        print(f"[WARN] {date_str} 无可计算股票。")
        return None

    batches = chunked_list(all_symbols, batch_size)
    total_batches = len(batches)
    out_path = OUTPUT_DIR / f"hf_{date_str}.parquet"
    parts: list[pd.DataFrame] = []

    print(f"========== 股票已分批为 {total_batches} 批，每批最多 {batch_size} 只，批内进程数 {batch_workers} ==========")
    for idx, batch in enumerate(batches, start=1):
        print(
            f"[BATCH] {idx}/{total_batches} symbols={len(batch)} first={batch[0]} last={batch[-1]}",
            flush=True,
        )
        df = process_day_parallel(
            date_str,
            workers=batch_workers,
            symbols=set(batch),
            out_path=out_path,
            write_output=False,
        )
        if df is not None and not df.empty:
            parts.append(df)

    if not parts:
        print(f"[WARN] {date_str} 没有任何特征生成。")
        return None

    df_hf_all = pd.concat(parts, ignore_index=True)
    df_hf_all.rename(columns={'TradingDay':'trade_date','SecuCode':'symbol'}, inplace=True)
    df_hf_all['trade_date'] = pd.to_datetime(df_hf_all['trade_date'].astype(str)).dt.strftime('%Y-%m-%d')
    if 'vpin_approx' in df_hf_all.columns:
        df_hf_all['flow_vpin'] = df_hf_all['vpin_approx']
    df_hf_all = calc_pressure(df_hf_all)
    for col in PAUSED_L2_FIELDS:
        if col in df_hf_all.columns:
            df_hf_all[col] = np.nan

    df_hf_all = df_hf_all.sort_values(['trade_date', 'symbol']).drop_duplicates(['trade_date', 'symbol'], keep='last').reset_index(drop=True)
    df_hf_all.to_parquet(out_path, index=False)
    print(f"[OK] {date_str} 批处理完成 → {df_hf_all.shape}  保存至 {out_path}")
    return df_hf_all

# ==================== 主入口 ====================
def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="QuantMind L2 高频 52维特征计算")
    parser.add_argument("--date", type=str, default=None, help="仅计算指定日期，格式 YYYYMMDD")
    parser.add_argument("--workers", type=int, default=N_WORKERS, help="并行进程数，建议 16~24")
    parser.add_argument("--batch-size", type=int, default=500, help="单批股票数，<=0 表示关闭批处理")
    parser.add_argument("--batch-workers", type=int, default=min(cpu_count(), 8), help="批内并行进程数")
    parser.add_argument("--symbols", type=str, default=None, help="仅计算指定股票，逗号分隔，例如 600519,000001")
    return parser.parse_args()

def main():
    args = parse_args()
    workers = max(1, int(args.workers))
    batch_size = int(args.batch_size)
    batch_workers = max(1, int(args.batch_workers))
    symbols = None
    if args.symbols:
        s = [x.strip() for x in args.symbols.split(',') if x.strip()]
        norm = set()
        for x in s:
            if len(x) == 6 and x.isdigit():
                norm.add(('SH' + x) if x.startswith('6') else ('SZ' + x))
            else:
                norm.add(x.upper())
        symbols = norm
    if batch_size and batch_size > 0:
        print(f"工作站资源: {cpu_count()}核，本次批处理模式: 每批{batch_size}只股票，批内分配进程数: {batch_workers}")
    else:
        print(f"工作站资源: {cpu_count()}核，本次日内分配进程数: {workers}")

    if args.date:
        dates = [args.date]
    else:
        files = sorted(glob.glob(str(BAIDUDESK_DIR / "snapshot_*.parquet")))
        dates = []
        for f in files:
            if f.endswith('.downloading'):
                continue
            date_str = Path(f).stem.split('_')[-1]
            deal_files = glob.glob(str(BAIDUDESK_DIR / f"deal_{date_str}.parquet*"))
            if not deal_files or any('.downloading' in x for x in deal_files):
                continue
            dates.append(date_str)

    for d in dates:
        if batch_size and batch_size > 0:
            process_day_batched(d, batch_size=batch_size, batch_workers=batch_workers, symbols=symbols)
        else:
            process_day_parallel(d, workers=workers, symbols=symbols)

    print("ALL DONE: 仅生成 L2 切片，请使用 merge_l1_l2_into_yearly.py 合并回年度宽表。")

if __name__ == "__main__":
    main()
