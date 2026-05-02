import os
import pandas as pd
import akshare as ak
from sqlalchemy import create_engine, text
from datetime import datetime

# 配置数据库连接，优先使用统一 DATABASE_URL
def get_db_url() -> str:
    db_url = os.getenv("DATABASE_URL")
    if db_url:
        return db_url
    host = os.getenv("DB_MASTER_HOST") or os.getenv("DB_HOST")
    port = os.getenv("DB_MASTER_PORT") or os.getenv("DB_PORT")
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    db = os.getenv("DB_NAME")
    missing = [k for k, v in (("DB_HOST", host), ("DB_PORT", port), ("DB_USER", user), ("DB_PASSWORD", password), ("DB_NAME", db)) if not v]
    if missing:
        raise RuntimeError(f"Missing DB env vars: {', '.join(missing)}. Please set DATABASE_URL or DB_* in root .env")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


DB_URL = get_db_url()


def init_table():
    """创建增强版的 stock_daily_latest 表"""
    engine = create_engine(DB_URL)
    # 2026-02-14 修正：先删除旧表以应用新 Schema，防止 CREATE INDEX 因缺失字段报错
    sql = """
    DROP TABLE IF EXISTS stock_daily_latest;
    CREATE TABLE stock_daily_latest (
        trade_date DATE,
        code VARCHAR(20) PRIMARY KEY,
        stock_name VARCHAR(100),
        open FLOAT,
        high FLOAT,
        low FLOAT,
        close FLOAT,
        volume FLOAT,
        turnover FLOAT,
        pct_change FLOAT,
        turnover_rate FLOAT,
        pe_ttm FLOAT,
        pb FLOAT,
        total_mv FLOAT,
        is_st INTEGER,
        is_hs300 INTEGER,
        is_csi1000 INTEGER,
        idx_chinext INTEGER DEFAULT 0,
        listing_market VARCHAR(50),
        industry VARCHAR(100),
        nindnme VARCHAR(200),
        corp_nature VARCHAR(100)
    );
    CREATE INDEX idx_sdl_pe ON stock_daily_latest(pe_ttm);
    CREATE INDEX idx_sdl_industry ON stock_daily_latest(industry);
    CREATE INDEX idx_sdl_nindnme ON stock_daily_latest(nindnme);
    """
    with engine.connect() as conn:
        conn.execute(text(sql))
        conn.commit()
    print("✅ 数据库增强表结构初始化完成")


def get_market_info(code):
    """根据代码判断市场和类型"""
    raw_code = code.replace("SH", "").replace("SZ", "").replace("BJ", "")
    if code.startswith("SH"):
        market = "上海证券交易所"
        if raw_code.startswith("688"):
            m_type = "科创板"
        else:
            m_type = "主板"
    elif code.startswith("SZ"):
        market = "深圳证券交易所"
        if raw_code.startswith("300"):
            m_type = "创业板"
        else:
            m_type = "主板"
    elif code.startswith("BJ") or raw_code.startswith(("8", "4")):
        market = "北京证券交易所"
        m_type = "北交所"
    else:
        market = "未知"
        m_type = "其他"
    return market, m_type


def fetch_and_sync():
    """抓取全量数据并丰富字段"""
    print("正在抓取全市场快照及行业信息 (约需 2-3 分钟)...")
    try:
        # 1. 获取基础行情
        df = ak.stock_zh_a_spot_em()
        mapping = {
            "代码": "raw_code",
            "名称": "stock_name",
            "今开": "open",
            "最高": "high",
            "最低": "low",
            "最新价": "close",
            "成交量": "volume",
            "成交额": "turnover",
            "涨跌幅": "pct_change",
            "换手率": "turnover_rate",
            "市盈率-动态": "pe_ttm",
            "市净率": "pb",
            "总市值": "total_mv",
        }
        processed_df = df[mapping.keys()].rename(columns=mapping)

        def fix_code(c):
            if c.startswith("6") or c.startswith("9"):
                return f"SH{c}"
            if c.startswith("8") or c.startswith("4"):
                return f"BJ{c}"
            return f"SZ{c}"

        processed_df["code"] = processed_df["raw_code"].apply(fix_code)
        processed_df["trade_date"] = datetime.now().date()
        processed_df["is_st"] = processed_df["stock_name"].apply(
            lambda x: 1 if "ST" in x else 0
        )

        # 2. 识别市场和类型
        processed_df["listing_market"] = processed_df.apply(
            lambda x: get_market_info(x["code"])[0], axis=1
        )

        # 3. 抓取行业分类 (通过东财行业板块接口)
        print("正在同步全市场行业分类...")
        try:
            ind_df = ak.stock_board_industry_name_em()
            # 建立 代码 -> 行业 的映射
            code_to_ind = {}
            # 为了速度，我们只取前 50 个主流行业，或全量抓取（此处演示全量逻辑）
            for _, row in ind_df.head(80).iterrows():
                ind_name = row["板块名称"]
                try:
                    cons_df = ak.stock_board_industry_cons_em(symbol=ind_name)
                    for c in cons_df["代码"].tolist():
                        code_to_ind[fix_code(c)] = ind_name
                except:
                    continue

            processed_df["industry"] = (
                processed_df["code"].map(code_to_ind).fillna("未分类")
            )
        except Exception as e:
            print(f"⚠️ 行业同步部分失败: {e}")
            processed_df["industry"] = "未知"

        # 4. 企业性质 (通过概念板块模糊匹配：国企改革、央企背景等)
        print("正在通过概念识别企业性质...")
        processed_df["corp_nature"] = "民营企业"  # 默认值
        try:
            # 获取国企改革成分股
            state_owned = ak.stock_board_concept_cons_em(symbol="国企改革")
            state_codes = set(state_owned["代码"].apply(fix_code).tolist())
            processed_df.loc[processed_df["code"].isin(state_codes), "corp_nature"] = (
                "国有企业"
            )
        except:
            pass

        # 5. 指数标记 (HS300 / CSI1000)
        try:
            hs300 = set(
                ak.index_stock_cons(symbol="000300")["品种代码"]
                .apply(fix_code)
                .tolist()
            )
            csi1000 = set(
                ak.index_stock_cons(symbol="000852")["品种代码"]
                .apply(fix_code)
                .tolist()
            )
            processed_df["is_hs300"] = processed_df["code"].apply(
                lambda x: 1 if x in hs300 else 0
            )
            processed_df["is_csi1000"] = processed_df["code"].apply(
                lambda x: 1 if x in csi1000 else 0
            )
            chinext = set(
                ak.index_stock_cons(symbol="399006")["品种代码"]
                .apply(fix_code)
                .tolist()
            )
            processed_df["idx_chinext"] = processed_df["code"].apply(
                lambda x: 1 if x in chinext else 0
            )
        except:
            processed_df["is_hs300"] = 0
            processed_df["is_csi1000"] = 0
            processed_df["idx_chinext"] = 0

        # 清洗
        processed_df["total_mv"] = processed_df["total_mv"] / 10000.0
        final_df = processed_df.drop(columns=["raw_code"])

        # 写入数据库
        engine = create_engine(DB_URL)
        final_df.to_sql("stock_daily_latest", engine, if_exists="replace", index=False)
        print(f"✅ 成功同步 {len(final_df)} 只股票，含行业、市场类型及企业性质信息")

    except Exception as e:
        print(f"❌ 数据同步总流程失败: {e}")


if __name__ == "__main__":
    init_table()
    fetch_and_sync()
