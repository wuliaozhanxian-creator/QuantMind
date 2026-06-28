"""
公司行为(除权除息)数据完整获取脚本
====================================

相比 v1 的改进:
  1. 支持任意月份范围 (单月 / 跨月连续), 内部按月迭代避免 TQ 缓冲溢出
  2. 断点续传: --resume 跳过已存在的 (symbol, yyyymm) 组合
  3. symbol 格式遵循 AGENTS.md: 大写市场前缀 + 6位数字 (如 SH600000)
  4. 默认排除北交所(BJ)与B股, 与系统其他部分一致
  5. 单只股票失败重试 + 失败清单落盘 (<output>.errors.csv)
  6. 失败异常可记录, 不再静默吞掉

输出字段 (固定列序):
  symbol        - SH600000 形式
  date          - YYYY-MM-DD
  type          - TQ 类型编码
  bonus         - 每股分红(元)
  allot_price   - 配股价(元)
  share_bonus   - 每股送股数
  allotment     - 每股配股比例

使用示例:
  # 拉取 2026 年 6 月数据
  python fetch_corporate_actions.py --start 202606 --end 202606

  # 拉取 2025-2026 两年全量
  python fetch_corporate_actions.py --start 202501 --end 202612 \\
      --output corporate_actions_2025_2026.csv

  # 断点续传 (跨月大范围推荐)
  python fetch_corporate_actions.py --start 202501 --end 202612 \\
      --output corporate_actions.csv --resume
"""
import argparse
import calendar
import os
import re
import sys
import time
from typing import List, Optional, Tuple

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))
from tqcenter import tq


# ---------------------------------------------------------------------------
# 配置
# ---------------------------------------------------------------------------

# 默认市场 — 用 market=5 (全A) + code 前缀推断后缀
# 注: markets 1-4 在当前 TQ 实例返回空, markets 7/8/51/52 是子集
DEFAULT_MARKET_ID = '5'

# B股: 上交所 9xxxxx, 深交所 2xxxxx
B_SHARE_CODE_RE = re.compile(r'^[29]')

OUTPUT_COLUMNS = ['symbol', 'date', 'type', 'bonus',
                  'allot_price', 'share_bonus', 'allotment']

# TQ_COLUMN_MAP 保留备查 (TQ 实际返回的是 'AllotPrice' / 'ShareBonus', 小写后
# 变成 'allotprice' / 'sharebonus', 需 rename 到下划线形式)
TQ_COLUMN_MAP = {
    'AllotPrice': 'allot_price',
    'ShareBonus': 'share_bonus',
}

# B股: symbol 形如 sh900xxx / sz200xxx 末尾 B (不区分大小写, 与 AGENTS.md 一致)
B_SHARE_RE = re.compile(r'^[A-Za-z]+\d+B$', re.IGNORECASE)


# ---------------------------------------------------------------------------
# 工具
# ---------------------------------------------------------------------------

def get_all_stocks(include_bj: bool = False) -> List[Tuple[str, str]]:
    """获取股票列表。

    Returns:
        [(internal_symbol, tdx_code), ...]
          internal_symbol: SH600000 形式 (AGENTS.md 规范)
          tdx_code:        600000.SH 形式 (TQ API 格式)
    """
    markets = list(DEFAULT_MARKETS)
    if include_bj:
        markets.append(BJ_MARKET)

    stocks: List[Tuple[str, str]] = []
    for market_id, suffix, name in markets:
        try:
            raw = tq.get_stock_list(market_id, list_type=1)
            if not raw or not isinstance(raw, list):
                print(f"  [WARN] {name}: 返回空")
                continue
            n = 0
            for s in raw:
                if not isinstance(s, dict):
                    continue
                code = str(s.get('Code', '')).strip()
                if len(code) == 6 and code.isdigit():
                    internal = f"{suffix}{code}"
                    tdx = f"{code}.{suffix}"
                    stocks.append((internal, tdx))
                    n += 1
            print(f"  {name}: {n} 只")
        except Exception as e:
            print(f"  [ERR ] {name}: {e}")
    print(f"  总计: {len(stocks)} 只")
    return stocks


def is_b_share(symbol: str) -> bool:
    """B股判断: 形如 sh900xxxB / sz200xxxB"""
    return bool(B_SHARE_RE.match(symbol or ''))


def iter_months(start_yyyymm: str, end_yyyymm: str) -> List[Tuple[str, str, str]]:
    """生成 [(yyyymm, start_yyyymmdd, end_yyyymmdd), ...]"""
    if len(start_yyyymm) != 6 or len(end_yyyymm) != 6:
        raise ValueError("月份格式必须为 YYYYMM, 例 202606")
    sy, sm = int(start_yyyymm[:4]), int(start_yyyymm[4:6])
    ey, em = int(end_yyyymm[:4]), int(end_yyyymm[4:6])
    if (ey, em) < (sy, sm):
        raise ValueError(f"end {end_yyyymm} 早于 start {start_yyyymm}")

    months = []
    y, m = sy, sm
    while (y, m) <= (ey, em):
        last_day = calendar.monthrange(y, m)[1]
        months.append((
            f"{y:04d}{m:02d}",
            f"{y:04d}{m:02d}01",
            f"{y:04d}{m:02d}{last_day:02d}",
        ))
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return months


def load_existing_index(output_path: str) -> set:
    """加载已存在输出文件的 (symbol, yyyymm) 索引, 用于断点续传"""
    if not os.path.exists(output_path):
        return set()
    try:
        df = pd.read_csv(output_path, dtype={'symbol': str})
        if df.empty or 'symbol' not in df.columns or 'date' not in df.columns:
            return set()
        df['symbol'] = df['symbol'].astype(str).str.upper()
        df['date'] = pd.to_datetime(df['date'], errors='coerce')
        df = df.dropna(subset=['date'])
        df['yyyymm'] = df['date'].dt.strftime('%Y%m')
        return set(zip(df['symbol'], df['yyyymm']))
    except Exception as e:
        print(f"  [WARN] 读取 {output_path} 失败: {e}, 将重新拉取")
        return set()


def fetch_one(tdx_code: str, internal_symbol: str,
              start_yyyymmdd: str, end_yyyymmdd: str,
              retries: int = 2, retry_sleep: float = 1.0):
    """单只单月拉取。

    Returns:
        DataFrame (含 symbol 列) — 成功
        None                      — 该月无数据
        Exception                 — 失败 (已被重试耗尽)
    """
    last_err: Optional[Exception] = None
    for attempt in range(retries + 1):
        try:
            df = tq.get_divid_factors(
                stock_code=tdx_code,
                start_time=start_yyyymmdd,
                end_time=end_yyyymmdd,
            )
            if df is None or df.empty:
                return None
            df = df.reset_index()
            df['symbol'] = internal_symbol
            return df
        except Exception as e:
            last_err = e
            if attempt < retries:
                time.sleep(retry_sleep)
    return last_err


# ---------------------------------------------------------------------------
# 主流程
# ---------------------------------------------------------------------------

def main() -> int:
    p = argparse.ArgumentParser(
        description='公司行为(除权除息)数据获取',
        formatter_class=argparse.RawTextHelpFormatter,
    )
    p.add_argument('--start', required=True, help='起始月份 YYYYMM, 例 202606')
    p.add_argument('--end',   required=True, help='结束月份 YYYYMM, 例 202606')
    p.add_argument('--output', '-o', default='corporate_actions.csv',
                   help='输出 CSV 路径 (默认 corporate_actions.csv)')
    p.add_argument('--resume', action='store_true',
                   help='断点续传: 跳过输出文件中已存在的 (symbol, yyyymm)')
    p.add_argument('--include-bj', action='store_true',
                   help='包含北交所(BJ)股票 (默认排除, 与系统其他部分一致)')
    p.add_argument('--exclude-b-share', dest='exclude_b_share',
                   action='store_true', default=True,
                   help='排除B股 (默认开启)')
    p.add_argument('--no-exclude-b-share', dest='exclude_b_share',
                   action='store_false',
                   help='不禁用B股过滤 (使用此标志关闭)')
    p.add_argument('--retries', type=int, default=2,
                   help='单只股票失败重试次数 (默认 2)')
    p.add_argument('--progress-step', type=int, default=100,
                   help='进度打印步长 (默认 100)')
    args = p.parse_args()

    print('=' * 64)
    print('公司行为(除权除息)数据获取')
    print(f"  时间范围    : {args.start} ~ {args.end}")
    print(f"  输出文件    : {args.output}")
    print(f"  断点续传    : {args.resume}")
    print(f"  包含北交所  : {args.include_bj}")
    print(f"  排除B股     : {args.exclude_b_share}")
    print(f"  重试次数    : {args.retries}")
    print('=' * 64)

    # 1) 初始化
    tq.initialize(__file__)
    print('[OK] 通达信连接成功')

    # 2) 股票列表
    print('\n[1/4] 获取股票列表...')
    all_stocks = get_all_stocks(include_bj=args.include_bj)
    if args.exclude_b_share:
        before = len(all_stocks)
        all_stocks = [(s, c) for s, c in all_stocks if not is_b_share(s)]
        print(f"  排除B股: {before - len(all_stocks)} 只")
    if not all_stocks:
        print('[ERR] 无可用股票, 退出')
        return 1

    # 3) 月份计划
    months = iter_months(args.start, args.end)
    print(f"\n[2/4] 月份计划: {len(months)} 个月")
    for yyyymm, s_dt, e_dt in months:
        print(f"  {yyyymm}: {s_dt} ~ {e_dt}")

    # 4) 续传索引
    existing: set = set()
    if args.resume:
        existing = load_existing_index(args.output)
        print(f"  续传索引: {len(existing)} 条 (symbol, yyyymm)")

    # 5) 拉取
    print('\n[3/4] 拉取数据...')
    records = []
    failed: List[Tuple[str, str, str]] = []   # (symbol, yyyymm, error_str)
    success = 0
    skipped = 0
    total_jobs = len(all_stocks) * len(months)
    done = 0
    t0 = time.time()

    for yyyymm, s_dt, e_dt in months:
        for internal, tdx in all_stocks:
            done += 1
            if args.resume and (internal, yyyymm) in existing:
                skipped += 1
                continue
            if done % args.progress_step == 0 or done == total_jobs:
                elapsed = time.time() - t0
                print(f"  进度: {done}/{total_jobs}  "
                      f"成功 {success} 失败 {len(failed)} 跳过 {skipped}  "
                      f"耗时 {elapsed:.0f}s")

            result = fetch_one(tdx, internal, s_dt, e_dt, retries=args.retries)
            if isinstance(result, Exception):
                failed.append((internal, yyyymm, repr(result)))
            elif result is not None:
                records.append(result)
                success += 1

    # 6) 合并保存
    print(f'\n[4/4] 合并并保存...')
    new_records: Optional[pd.DataFrame] = None
    if records:
        new_records = pd.concat(records, ignore_index=True)
        new_records.columns = [c.lower() for c in new_records.columns]
        # 修正 TQ 列名: allotprice → allot_price, sharebonus → share_bonus
        new_records.rename(columns={
            'allotprice': 'allot_price',
            'sharebonus': 'share_bonus',
        }, inplace=True)
        for col in OUTPUT_COLUMNS:
            if col not in new_records.columns:
                new_records[col] = 0.0
        new_records = new_records[OUTPUT_COLUMNS]
        new_records['date'] = pd.to_datetime(
            new_records['date'], errors='coerce'
        ).dt.strftime('%Y-%m-%d')
        new_records = new_records.dropna(subset=['date'])
        new_records = new_records.sort_values(['date', 'symbol']).reset_index(drop=True)

    # 合并已有文件
    if args.resume and os.path.exists(args.output):
        try:
            old = pd.read_csv(args.output, dtype={'symbol': str})
            if new_records is not None and not new_records.empty:
                final = pd.concat([old, new_records], ignore_index=True)
            else:
                final = old
            before = len(final)
            final = final.drop_duplicates(
                subset=['symbol', 'date', 'type'], keep='last'
            )
            print(f"  去重: {before} -> {len(final)}")
        except Exception as e:
            print(f"  [WARN] 合并旧文件失败: {e}, 仅写本次新增")
            final = new_records if new_records is not None else pd.DataFrame(columns=OUTPUT_COLUMNS)
    else:
        final = new_records if new_records is not None else pd.DataFrame(columns=OUTPUT_COLUMNS)

    final = final.sort_values(['date', 'symbol']).reset_index(drop=True)
    final.to_csv(args.output, index=False, encoding='utf-8-sig')

    print(f"[OK] 已保存: {args.output}")
    print(f"     总记录数  : {len(final)}")
    print(f"     涉及股票数: {final['symbol'].nunique() if 'symbol' in final.columns else 0}")
    print(f"     本次新增  : 成功 {success}  跳过 {skipped}  失败 {len(failed)}")

    # 7) 失败报告
    if failed:
        err_path = args.output + '.errors.csv'
        pd.DataFrame(failed, columns=['symbol', 'yyyymm', 'error']).to_csv(
            err_path, index=False, encoding='utf-8-sig'
        )
        print(f"[ERR] 失败 {len(failed)} 条, 已记录: {err_path}")
        rc = 2
    else:
        rc = 0

    print('\n' + '=' * 64)
    print('执行完毕')
    return rc


if __name__ == '__main__':
    sys.exit(main())
