-- ============================================================
-- QuantMind 数据库升级脚本 v1.4.0
-- ============================================================
-- 标签从 stock_daily_latest 宽表迁移至 PG 长表 stock_tag
--
-- 变更说明：
--   1. 新建 tag_dictionary（标签字典）和 stock_tag（股票-标签成员关系长表）
--   2. 从 stock_daily_latest 删除 16 个标签列：
--      idx_hs300/idx_zz500/idx_zz1000/idx_chinext/idx_margin/idx_all
--      concept_ai/concept_chip/concept_new_energy/concept_pv
--      concept_military/concept_medical/concept_fintech/concept_consumption
--      concept_state_owned/concept_lithium
--   3. 同步删除 4 张月度分区表的标签列（若存在）
--
-- 执行前：
--   1. 备份数据库：pg_dump -U quantmind quantmind > backup_$(date +%Y%m%d).sql
--   2. 先运行回填脚本从 parquet 填充 stock_tag：
--      python scripts/data/migration/backfill_stock_tag_from_parquet.py
--   3. 验证 stock_tag 数据正确后再执行本脚本删列
--
-- 执行：
--   psql -U quantmind -d quantmind -f data/migrations/upgrade_v1.4.0_stock_tag.sql
-- ============================================================

BEGIN;

-- ============================================================
-- 1. 新建 tag_dictionary 表
-- ============================================================
CREATE TABLE IF NOT EXISTS public.tag_dictionary (
    tag_code character varying(64) NOT NULL,
    tag_name character varying(128) NOT NULL,
    tag_category character varying(32) NOT NULL,
    source character varying(64),
    is_active boolean NOT NULL DEFAULT true,
    sort_order integer NOT NULL DEFAULT 0,
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT tag_dictionary_pkey PRIMARY KEY (tag_code)
);

ALTER TABLE public.tag_dictionary OWNER TO quantmind;

COMMENT ON TABLE public.tag_dictionary IS '标签字典：指数/概念/板块标签元数据';
COMMENT ON COLUMN public.tag_dictionary.tag_code IS '标签机器码：hs300/ai/chip';
COMMENT ON COLUMN public.tag_dictionary.tag_name IS '标签中文名';
COMMENT ON COLUMN public.tag_dictionary.tag_category IS '分类：index/concept/board/custom';
COMMENT ON COLUMN public.tag_dictionary.source IS '数据来源：csi/metadata_json/manual';
COMMENT ON COLUMN public.tag_dictionary.is_active IS '软停用';
COMMENT ON COLUMN public.tag_dictionary.sort_order IS '展示排序';

-- ============================================================
-- 2. 新建 stock_tag 表
-- ============================================================
CREATE TABLE IF NOT EXISTS public.stock_tag (
    id bigint NOT NULL,
    symbol character varying(16) NOT NULL,
    tag_code character varying(64) NOT NULL,
    source character varying(64),
    created_at timestamp with time zone NOT NULL DEFAULT now(),
    updated_at timestamp with time zone NOT NULL DEFAULT now(),
    CONSTRAINT stock_tag_pkey PRIMARY KEY (id),
    CONSTRAINT stock_tag_tag_code_fkey FOREIGN KEY (tag_code)
        REFERENCES public.tag_dictionary(tag_code) ON DELETE RESTRICT,
    CONSTRAINT uq_stock_tag_symbol_code UNIQUE (symbol, tag_code)
);

ALTER TABLE public.stock_tag OWNER TO quantmind;

COMMENT ON TABLE public.stock_tag IS '股票-标签成员关系（长表）：一只股票多标签=多行';
COMMENT ON COLUMN public.stock_tag.symbol IS '股票代码 Prefix 格式：SH600191';
COMMENT ON COLUMN public.stock_tag.tag_code IS '标签机器码';
COMMENT ON COLUMN public.stock_tag.source IS '条目级来源';

-- 序列与自增主键
CREATE SEQUENCE IF NOT EXISTS public.stock_tag_id_seq
    AS bigint START WITH 1 INCREMENT BY 1 NO MINVALUE NO MAXVALUE CACHE 1;
ALTER TABLE public.stock_tag_id_seq OWNER TO quantmind;
ALTER SEQUENCE public.stock_tag_id_seq OWNED BY public.stock_tag.id;
ALTER TABLE ONLY public.stock_tag ALTER COLUMN id SET DEFAULT nextval('public.stock_tag_id_seq');

-- 索引
CREATE INDEX IF NOT EXISTS ix_stock_tag_symbol ON public.stock_tag USING btree (symbol);
CREATE INDEX IF NOT EXISTS ix_stock_tag_tag_code ON public.stock_tag USING btree (tag_code);

-- ============================================================
-- 3. 删除 stock_daily_latest 的 16 个标签列
-- ============================================================
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS idx_hs300;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS idx_zz500;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS idx_zz1000;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS idx_chinext;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS idx_margin;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS idx_all;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_ai;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_chip;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_new_energy;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_pv;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_military;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_medical;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_fintech;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_consumption;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_state_owned;
ALTER TABLE public.stock_daily_latest DROP COLUMN IF EXISTS concept_lithium;

-- ============================================================
-- 4. 删除月度分区表的标签列（若存在）
-- ============================================================
DO $$
DECLARE
    tbl text;
    col text;
    cols text[] := ARRAY[
        'idx_hs300','idx_zz500','idx_zz1000','idx_chinext','idx_margin','idx_all',
        'concept_ai','concept_chip','concept_new_energy','concept_pv',
        'concept_military','concept_medical','concept_fintech','concept_consumption',
        'concept_state_owned','concept_lithium'
    ];
BEGIN
    FOREACH tbl IN ARRAY ARRAY[
        'stock_daily_new_2026_01','stock_daily_new_2026_02',
        'stock_daily_new_2026_03','stock_daily_new_2026_04'
    ] LOOP
        FOREACH col IN ARRAY cols LOOP
            EXECUTE format(
                'ALTER TABLE IF EXISTS public.%I DROP COLUMN IF EXISTS %I',
                tbl, col
            );
        END LOOP;
    END LOOP;
END $$;

-- ============================================================
-- 5. 更新统计信息
-- ============================================================
ANALYZE public.stock_daily_latest;
ANALYZE public.stock_tag;
ANALYZE public.tag_dictionary;

COMMIT;

-- ============================================================
-- 验证查询（执行后手动跑一次确认）
-- ============================================================
-- SELECT tag_code, tag_name, tag_category, is_active FROM tag_dictionary ORDER BY sort_order;
-- SELECT tag_code, COUNT(*) FROM stock_tag GROUP BY tag_code ORDER BY tag_code;
-- SELECT COUNT(*) FROM information_schema.columns
--   WHERE table_name='stock_daily_latest'
--   AND (column_name LIKE 'idx_%' OR column_name LIKE 'concept_%');
-- 预期：最后一行返回 0
