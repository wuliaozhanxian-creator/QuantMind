"""
从 baostock 拉取 ROE 和利润增长率数据 (季频财务数据)
将季度数据映射到每日数据
"""
import os
import sys
import pandas as pd
import numpy as np
from pathlib import Path
import baostock as bs
import asyncpg
import asyncio
from datetime import datetime

PROJECT_ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(PROJECT_ROOT))

DB_HOST = os.getenv("DB_HOST", "127.0.0.1")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME", "quantmind")
DB_USER = os.getenv("DB_USER", "quantmind")
DB_PASS = os.getenv("DB_PASSWORD", "quantmind2026")


def get_stock_codes():
    """获取A股股票代码列表"""
    lg = bs.login()
    rs = bs.query_all_stock(day=datetime.now().strftime('%Y-%m-%d'))
    data_list = []
    while (rs.error_code == '0') & rs.next():
        data_list.append(rs.get_row_data())
    bs.logout()

    df = pd.DataFrame(data_list, columns=rs.fields)
    # 只保留A股 sh/sz + 6位数字
    df = df[df['code'].str.match(r'^(sh|sz)\d{6}$')]
    return df['code'].tolist()


def get_profit_data(stock_codes):
    """获取盈利能力数据 (ROE) - 最近4个季度"""
    print("获取盈利能力数据 (ROE)...")

    lg = bs.login()
    all_data = []

    # 获取最近4个季度的数据
    quarters = [
        (2024, 1), (2024, 2), (2024, 3), (2024, 4),
        (2025, 1), (2025, 2), (2025, 3), (2025, 4),
        (2026, 1)
    ]

    for year, quarter in quarters:
        print(f"  获取 {year}Q{quarter}...")
        for i, code in enumerate(stock_codes):
            try:
                rs = bs.query_profit_data(code=code, year=year, quarter=quarter)
                while (rs.error_code == '0') & rs.next():
                    row = rs.get_row_data()
                    all_data.append({
                        'code': row[0],
                        'pub_date': row[1],
                        'stat_date': row[2],
                        'roe': row[3],
                        'year': year,
                        'quarter': quarter
                    })
            except:
                pass

            if (i + 1) % 500 == 0:
                print(f"    已处理 {i+1}/{len(stock_codes)}")

    bs.logout()

    df = pd.DataFrame(all_data)
    print(f"  获取到 {len(df):,} 条记录")
    return df


def get_growth_data(stock_codes):
    """获取成长能力数据 (利润增长率) - 最近4个季度"""
    print("获取成长能力数据 (利润增长率)...")

    lg = bs.login()
    all_data = []

    quarters = [
        (2024, 1), (2024, 2), (2024, 3), (2024, 4),
        (2025, 1), (2025, 2), (2025, 3), (2025, 4),
        (2026, 1)
    ]

    for year, quarter in quarters:
        print(f"  获取 {year}Q{quarter}...")
        for i, code in enumerate(stock_codes):
            try:
                rs = bs.query_growth_data(code=code, year=year, quarter=quarter)
                while (rs.error_code == '0') & rs.next():
                    row = rs.get_row_data()
                    all_data.append({
                        'code': row[0],
                        'pub_date': row[1],
                        'stat_date': row[2],
                        'profit_growth': row[5],  # YOYNI 净利润同比增长率
                        'year': year,
                        'quarter': quarter
                    })
            except:
                pass

            if (i + 1) % 500 == 0:
                print(f"    已处理 {i+1}/{len(stock_codes)}")

    bs.logout()

    df = pd.DataFrame(all_data)
    print(f"  获取到 {len(df):,} 条记录")
    return df


async def update_database(conn, df_profit, df_growth):
    """更新数据库 - 将季频数据映射到每日数据"""

    # 获取数据库中的股票代码和日期
    print("\n获取数据库中的股票列表...")
    rows = await conn.fetch("""
        SELECT DISTINCT symbol FROM stock_daily_latest
    """)
    db_symbols = [r['symbol'] for r in rows]
    print(f"  共 {len(db_symbols)} 只股票")

    # 获取日期范围
    rows = await conn.fetch("""
        SELECT DISTINCT trade_date FROM stock_daily_latest ORDER BY trade_date
    """)
    db_dates = [r['trade_date'] for r in rows]
    print(f"  日期范围: {db_dates[0]} ~ {db_dates[-1]}")

    # 处理 ROE 数据
    print("\n处理 ROE 数据...")
    if not df_profit.empty:
        df_profit['symbol'] = df_profit['code'].apply(
            lambda x: f"SH{x[3:]}" if x.startswith('sh') else f"SZ{x[3:]}"
        )
        df_profit['roe'] = pd.to_numeric(df_profit['roe'], errors='coerce')
        df_profit['pub_date'] = pd.to_datetime(df_profit['pub_date'], errors='coerce')

        # 按股票分组，取最新的财务数据
        latest_profit = df_profit.sort_values('pub_date').groupby('symbol').last().reset_index()
        print(f"  有效股票数: {len(latest_profit)}")

        # 批量更新
        await conn.execute("""
            CREATE TEMP TABLE tmp_roe (
                symbol VARCHAR,
                roe DOUBLE PRECISION
            )
        """)

        records = [[row['symbol'], row['roe']] for _, row in latest_profit.iterrows() if pd.notna(row['roe'])]
        await conn.copy_records_to_table("tmp_roe", records=records, columns=['symbol', 'roe'])

        result = await conn.execute("""
            UPDATE stock_daily_latest s
            SET roe = t.roe
            FROM tmp_roe t
            WHERE s.symbol = t.symbol
        """)
        print(f"  更新结果: {result}")

        await conn.execute("DROP TABLE tmp_roe")

    # 处理利润增长率数据
    print("\n处理利润增长率数据...")
    if not df_growth.empty:
        df_growth['symbol'] = df_growth['code'].apply(
            lambda x: f"SH{x[3:]}" if x.startswith('sh') else f"SZ{x[3:]}"
        )
        df_growth['profit_growth'] = pd.to_numeric(df_growth['profit_growth'], errors='coerce')
        df_growth['pub_date'] = pd.to_datetime(df_growth['pub_date'], errors='coerce')

        # 按股票分组，取最新的财务数据
        latest_growth = df_growth.sort_values('pub_date').groupby('symbol').last().reset_index()
        print(f"  有效股票数: {len(latest_growth)}")

        # 批量更新
        await conn.execute("""
            CREATE TEMP TABLE tmp_growth (
                symbol VARCHAR,
                profit_growth DOUBLE PRECISION
            )
        """)

        records = [[row['symbol'], row['profit_growth']] for _, row in latest_growth.iterrows() if pd.notna(row['profit_growth'])]
        await conn.copy_records_to_table("tmp_growth", records=records, columns=['symbol', 'profit_growth'])

        result = await conn.execute("""
            UPDATE stock_daily_latest s
            SET profit_growth = t.profit_growth
            FROM tmp_growth t
            WHERE s.symbol = t.symbol
        """)
        print(f"  更新结果: {result}")

        await conn.execute("DROP TABLE tmp_growth")


async def main():
    print("=" * 60)
    print("从 baostock 拉取 ROE 和利润增长率数据")
    print("=" * 60)

    # 获取股票代码
    print("\n获取股票代码列表...")
    stock_codes = get_stock_codes()
    print(f"共 {len(stock_codes)} 只股票")

    # 获取财务数据
    df_profit = get_profit_data(stock_codes)
    df_growth = get_growth_data(stock_codes)

    # 连接数据库
    print("\n连接数据库...")
    conn = await asyncpg.connect(
        host=DB_HOST, port=DB_PORT, user=DB_USER, password=DB_PASS, database=DB_NAME
    )

    try:
        await update_database(conn, df_profit, df_growth)
    finally:
        await conn.close()

    print("\n完成!")


if __name__ == "__main__":
    asyncio.run(main())
