--
-- PostgreSQL database dump
--
-- Dumped from database version 15.17
-- Dumped by pg_dump version 15.17
SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;
--
-- Name: orderside; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.orderside AS ENUM (
    'buy',
    'sell'
);
--
-- Name: orderstatus; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.orderstatus AS ENUM (
    'pending',
    'submitted',
    'partially_filled',
    'filled',
    'cancelled',
    'rejected',
    'expired'
);
--
-- Name: ordertype; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.ordertype AS ENUM (
    'market',
    'limit',
    'stop',
    'stop_limit'
);
--
-- Name: positionside; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.positionside AS ENUM (
    'long',
    'short'
);
--
-- Name: simulationstatus; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.simulationstatus AS ENUM (
    'RUNNING',
    'PAUSED',
    'STOPPED',
    'ERROR'
);
--
-- Name: strategystatus; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.strategystatus AS ENUM (
    'DRAFT',
    'REPOSITORY',
    'LIVE_TRADING',
    'ACTIVE',
    'PAUSED',
    'ARCHIVED'
);
--
-- Name: strategytype; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.strategytype AS ENUM (
    'CUSTOM',
    'TECHNICAL',
    'FUNDAMENTAL',
    'QUANTITATIVE',
    'MIXED'
);
--
-- Name: tradeaction; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.tradeaction AS ENUM (
    'buy',
    'sell'
);
--
-- Name: tradingmode; Type: TYPE; Schema: public; Owner: -
--
CREATE TYPE public.tradingmode AS ENUM (
    'BACKTEST',
    'SIMULATION',
    'LIVE',
    'REAL'
);
--
-- Name: auto_populate_id(); Type: FUNCTION; Schema: public; Owner: -
--
CREATE FUNCTION public.auto_populate_id() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
  IF NEW.id IS NULL THEN
    NEW.id := NEW.backtest_id;
  END IF;
  RETURN NEW;
END;
$$;
--
-- Name: cleanup_old_qmt_data(); Type: FUNCTION; Schema: public; Owner: -
--
CREATE FUNCTION public.cleanup_old_qmt_data() RETURNS void
    LANGUAGE plpgsql
    AS $$
BEGIN
    DELETE FROM qmt_account_assets WHERE timestamp < NOW() - INTERVAL '7 days';
    DELETE FROM qmt_positions WHERE timestamp < NOW() - INTERVAL '7 days';
    DELETE FROM qmt_orders WHERE timestamp < NOW() - INTERVAL '7 days';
    DELETE FROM qmt_trades WHERE timestamp < NOW() - INTERVAL '7 days';
    DELETE FROM qmt_sync_logs WHERE timestamp < NOW() - INTERVAL '30 days';
END;
$$;
--
-- Name: maintain_stock_daily_window(); Type: FUNCTION; Schema: public; Owner: -
--
CREATE FUNCTION public.maintain_stock_daily_window() RETURNS trigger
    LANGUAGE plpgsql
    AS $$
BEGIN
    -- 删除该股票超过 30 天的旧数据
    -- 性能优化：仅当插入数据日期较新时触发删除，且不频繁全表扫描
    -- 这里采用简单策略：每次插入后，删除该股票 30 天前的数据
    DELETE FROM public.stock_daily 
    WHERE code = NEW.code AND trade_date < (CURRENT_DATE - INTERVAL '30 days');
    RETURN NEW;
END;
$$;
--
-- Name: qm_import_research_candidate_snapshot(date, text, text, boolean); Type: FUNCTION; Schema: public; Owner: -
--
CREATE FUNCTION public.qm_import_research_candidate_snapshot(p_prediction_trade_date date DEFAULT NULL::date, p_tenant_id text DEFAULT NULL::text, p_user_id text DEFAULT NULL::text, p_force_rebuild boolean DEFAULT false) RETURNS TABLE(imported_rows bigint, imported_prediction_trade_date date, source_mode text, max_source_updated_at timestamp with time zone)
    LANGUAGE plpgsql
    AS $$
DECLARE
  v_last_source_updated_at TIMESTAMPTZ;
  v_has_stock_screener_snapshot BOOLEAN := to_regclass('public.stock_screener_snapshot') IS NOT NULL;
  v_has_stock_selection BOOLEAN := to_regclass('public.stock_selection') IS NOT NULL;
  v_imported_rows BIGINT := 0;
  v_imported_prediction_trade_date DATE := p_prediction_trade_date;
  v_max_source_updated_at TIMESTAMPTZ;
  v_last_run_id TEXT;
  v_source_mode TEXT;
BEGIN
  SELECT last_source_updated_at
    INTO v_last_source_updated_at
    FROM qm_research_import_state
   WHERE job_name = 'research_candidate_snapshot';
  IF p_prediction_trade_date IS NULL THEN
    SELECT MAX(prediction_trade_date)
      INTO v_imported_prediction_trade_date
      FROM qm_model_inference_runs
     WHERE status = 'completed'
       AND (p_tenant_id IS NULL OR tenant_id = p_tenant_id)
       AND (p_user_id IS NULL OR user_id = p_user_id)
       AND (
         p_force_rebuild
         OR updated_at > COALESCE(v_last_source_updated_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')
       );
  END IF;
  v_source_mode := CASE
    WHEN v_has_stock_screener_snapshot THEN 'stock_screener_snapshot'
    WHEN v_has_stock_selection THEN 'stock_selection'
    ELSE 'stock_daily_latest'
  END;
  IF v_imported_prediction_trade_date IS NULL THEN
    RETURN QUERY SELECT 0::BIGINT, NULL::DATE, v_source_mode, NULL::TIMESTAMPTZ;
    RETURN;
  END IF;
  IF p_force_rebuild THEN
    DELETE FROM qm_research_candidate_snapshot
     WHERE prediction_trade_date = v_imported_prediction_trade_date
       AND (p_tenant_id IS NULL OR tenant_id = p_tenant_id)
       AND (p_user_id IS NULL OR user_id = p_user_id);
  END IF;
  IF v_has_stock_screener_snapshot THEN
    WITH candidate_runs AS (
      SELECT
        r.run_id,
        r.tenant_id,
        r.user_id,
        r.model_id,
        r.data_trade_date,
        r.prediction_trade_date,
        r.updated_at AS run_updated_at
      FROM qm_model_inference_runs r
      WHERE r.status = 'completed'
        AND r.prediction_trade_date = v_imported_prediction_trade_date
        AND (p_tenant_id IS NULL OR r.tenant_id = p_tenant_id)
        AND (p_user_id IS NULL OR r.user_id = p_user_id)
        AND (
          p_force_rebuild
          OR r.updated_at > COALESCE(v_last_source_updated_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')
        )
    ),
    signal_rows AS (
      SELECT
        ranked.run_id,
        ranked.tenant_id,
        ranked.user_id,
        ranked.trade_date,
        ranked.symbol_norm,
        ranked.light_score,
        ranked.tft_score,
        ranked.fusion_score,
        ranked.score_rank,
        ranked.universe_tag,
        ranked.signal_side,
        ranked.expected_price,
        ranked.quality,
        ranked.created_at
      FROM (
        SELECT
          s.run_id,
          s.tenant_id,
          s.user_id,
          s.trade_date,
          LPAD(REGEXP_REPLACE(s.symbol, '[^0-9]', '', 'g'), 6, '0') AS symbol_norm,
          s.light_score,
          s.tft_score,
          s.fusion_score,
          s.score_rank,
          s.universe_tag,
          s.signal_side,
          s.expected_price,
          COALESCE(s.quality, '{}'::jsonb) AS quality,
          s.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY s.run_id, s.tenant_id, s.user_id, s.trade_date, LPAD(REGEXP_REPLACE(s.symbol, '[^0-9]', '', 'g'), 6, '0')
            ORDER BY s.created_at DESC NULLS LAST, s.fusion_score DESC NULLS LAST
          ) AS row_num
        FROM engine_signal_scores s
        JOIN candidate_runs r
          ON r.run_id = s.run_id
         AND r.tenant_id = s.tenant_id
         AND r.user_id = s.user_id
         AND r.prediction_trade_date = s.trade_date
      ) ranked
      WHERE ranked.row_num = 1
    ),
    source_rows AS (
      SELECT
        snapshot_date AS trade_date,
        LPAD(REGEXP_REPLACE(symbol, '[^0-9]', '', 'g'), 6, '0') AS symbol_norm,
        short_name AS stock_name,
        industry,
        '[]'::jsonb AS concept_tags,
        close_price,
        change_ratio AS latest_change_pct,
        turnover_rate1 AS turnover_rate,
        amount,
        market_value AS total_mv,
        markettype AS market_type,
        province,
        city,
        NULL::INTEGER AS listed_days,
        COALESCE(continued_rise_days, 0) AS consecutive_limit_up_days,
        LEAST(COALESCE(continued_rise_days, 0), 5) AS recent_limit_up_count_5d,
        COALESCE(vol_3d_up, FALSE) AS volume_trend_3d,
        COALESCE(vol_5d_up, FALSE) AS volume_trend_5d,
        vol_3d_trend AS volume_trend_3d_score,
        vol_3d_sum / 3.0 AS volume_ma_3,
        vol_5d_avg AS volume_ma_5,
        ma5,
        ma10,
        ma20,
        return_5d,
        return_10d,
        close_above_ma5,
        close_above_ma10,
        continued_rise_days,
        continued_fall_days,
        amount_rank,
        amount_3d_sum / 3.0 AS amount_ma_3,
        amount_5d_sum / 5.0 AS amount_ma_5,
        high_5d,
        low_5d,
        life_high_week,
        life_high_month,
        life_high_3month,
        life_high_6month,
        life_high_one_year,
        FALSE AS is_st,
        FALSE AS is_suspended,
        (change_ratio >= 9.80) AS is_limit_up,
        (change_ratio <= -9.80) AS is_limit_down,
        FALSE AS is_hs300,
        FALSE AS is_csi1000
      FROM stock_screener_snapshot
      WHERE snapshot_date = v_imported_prediction_trade_date
    ),
    upserted AS (
      INSERT INTO qm_research_candidate_snapshot (
        tenant_id, user_id, run_id, model_id, data_trade_date, prediction_trade_date, market_snapshot_trade_date,
        symbol, stock_name, industry, concept_tags, close_price, latest_change_pct, turnover_rate, amount, total_mv,
        market_type, province, city, fusion_score, light_score, tft_score, score_rank, signal_side, expected_price,
        universe_tag, quality, confidence_level, confidence_score, listed_days, consecutive_limit_up_days,
        recent_limit_up_count_5d, volume_trend_3d, volume_trend_5d, volume_trend_3d_score, volume_ma_3, volume_ma_5,
        ma5, ma10, ma20, return_5d, return_10d, close_above_ma5, close_above_ma10, continued_rise_days,
        continued_fall_days, amount_rank, amount_ma_3, amount_ma_5, high_5d, low_5d, life_high_week,
        life_high_month, life_high_3month, life_high_6month, life_high_one_year, is_st, is_suspended,
        is_limit_up, is_limit_down, tradable_flag, is_hs300, is_csi1000, hit_reasons, risk_flags,
        thesis_summary, source_updated_at, updated_at
      )
      SELECT
        r.tenant_id,
        r.user_id,
        r.run_id,
        r.model_id,
        r.data_trade_date,
        r.prediction_trade_date,
        sr.trade_date,
        s.symbol_norm,
        sr.stock_name,
        sr.industry,
        COALESCE(sr.concept_tags, '[]'::jsonb),
        sr.close_price,
        sr.latest_change_pct,
        sr.turnover_rate,
        sr.amount,
        sr.total_mv,
        sr.market_type,
        sr.province,
        sr.city,
        s.fusion_score,
        s.light_score,
        s.tft_score,
        s.score_rank,
        s.signal_side,
        s.expected_price,
        s.universe_tag,
        s.quality,
        CASE WHEN s.fusion_score >= 0.80 THEN 'high' WHEN s.fusion_score >= 0.60 THEN 'medium' ELSE 'watch' END,
        s.fusion_score,
        sr.listed_days,
        COALESCE(sr.consecutive_limit_up_days, 0),
        COALESCE(sr.recent_limit_up_count_5d, 0),
        COALESCE(sr.volume_trend_3d, FALSE),
        COALESCE(sr.volume_trend_5d, FALSE),
        sr.volume_trend_3d_score,
        sr.volume_ma_3,
        sr.volume_ma_5,
        sr.ma5,
        sr.ma10,
        sr.ma20,
        sr.return_5d,
        sr.return_10d,
        sr.close_above_ma5,
        sr.close_above_ma10,
        sr.continued_rise_days,
        sr.continued_fall_days,
        sr.amount_rank,
        sr.amount_ma_3,
        sr.amount_ma_5,
        sr.high_5d,
        sr.low_5d,
        sr.life_high_week,
        sr.life_high_month,
        sr.life_high_3month,
        sr.life_high_6month,
        sr.life_high_one_year,
        sr.is_st,
        sr.is_suspended,
        sr.is_limit_up,
        sr.is_limit_down,
        NOT COALESCE(sr.is_limit_up, FALSE) AND NOT COALESCE(sr.is_limit_down, FALSE) AND NOT COALESCE(sr.is_suspended, FALSE),
        sr.is_hs300,
        sr.is_csi1000,
        TO_JSONB(ARRAY_REMOVE(ARRAY[
          CASE WHEN s.fusion_score >= 0.80 THEN '模型高分' END,
          CASE WHEN sr.volume_trend_3d THEN '3日量能递增' END,
          CASE WHEN sr.close_above_ma5 THEN '站上MA5' END,
          CASE WHEN COALESCE(sr.consecutive_limit_up_days, 0) >= 2 THEN '连涨' || sr.consecutive_limit_up_days::TEXT || '日' END,
          CASE WHEN sr.industry IS NOT NULL THEN '行业:' || sr.industry END
        ]::TEXT[], NULL)),
        TO_JSONB(ARRAY_REMOVE(ARRAY[
          CASE WHEN COALESCE(sr.is_limit_up, FALSE) THEN '涨停不可追' END,
          CASE WHEN COALESCE(sr.is_limit_down, FALSE) THEN '跌停流动性受限' END,
          CASE WHEN sr.return_10d IS NOT NULL AND sr.return_10d < -0.12 THEN '近10日回撤较大' END,
          CASE WHEN sr.close_above_ma10 = FALSE THEN '弱于MA10' END
        ]::TEXT[], NULL)),
        CASE WHEN sr.industry IS NOT NULL THEN '模型高分，' || sr.industry || '方向，结合趋势与量能具备进一步研究价值' ELSE '模型高分候选，结合趋势与量能具备进一步研究价值' END,
        GREATEST(COALESCE(s.created_at, TIMESTAMPTZ '1970-01-01 00:00:00+00'), COALESCE(r.run_updated_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')),
        NOW()
      FROM candidate_runs r
      JOIN signal_rows s
        ON s.run_id = r.run_id AND s.tenant_id = r.tenant_id AND s.user_id = r.user_id
      LEFT JOIN source_rows sr
        ON sr.trade_date = r.prediction_trade_date AND sr.symbol_norm = s.symbol_norm
      ON CONFLICT (tenant_id, user_id, run_id, symbol)
      DO UPDATE SET
        model_id = EXCLUDED.model_id,
        data_trade_date = EXCLUDED.data_trade_date,
        prediction_trade_date = EXCLUDED.prediction_trade_date,
        market_snapshot_trade_date = EXCLUDED.market_snapshot_trade_date,
        stock_name = EXCLUDED.stock_name,
        industry = EXCLUDED.industry,
        concept_tags = EXCLUDED.concept_tags,
        close_price = EXCLUDED.close_price,
        latest_change_pct = EXCLUDED.latest_change_pct,
        turnover_rate = EXCLUDED.turnover_rate,
        amount = EXCLUDED.amount,
        total_mv = EXCLUDED.total_mv,
        market_type = EXCLUDED.market_type,
        province = EXCLUDED.province,
        city = EXCLUDED.city,
        fusion_score = EXCLUDED.fusion_score,
        light_score = EXCLUDED.light_score,
        tft_score = EXCLUDED.tft_score,
        score_rank = EXCLUDED.score_rank,
        signal_side = EXCLUDED.signal_side,
        expected_price = EXCLUDED.expected_price,
        universe_tag = EXCLUDED.universe_tag,
        quality = EXCLUDED.quality,
        confidence_level = EXCLUDED.confidence_level,
        confidence_score = EXCLUDED.confidence_score,
        listed_days = EXCLUDED.listed_days,
        consecutive_limit_up_days = EXCLUDED.consecutive_limit_up_days,
        recent_limit_up_count_5d = EXCLUDED.recent_limit_up_count_5d,
        volume_trend_3d = EXCLUDED.volume_trend_3d,
        volume_trend_5d = EXCLUDED.volume_trend_5d,
        volume_trend_3d_score = EXCLUDED.volume_trend_3d_score,
        volume_ma_3 = EXCLUDED.volume_ma_3,
        volume_ma_5 = EXCLUDED.volume_ma_5,
        ma5 = EXCLUDED.ma5,
        ma10 = EXCLUDED.ma10,
        ma20 = EXCLUDED.ma20,
        return_5d = EXCLUDED.return_5d,
        return_10d = EXCLUDED.return_10d,
        close_above_ma5 = EXCLUDED.close_above_ma5,
        close_above_ma10 = EXCLUDED.close_above_ma10,
        continued_rise_days = EXCLUDED.continued_rise_days,
        continued_fall_days = EXCLUDED.continued_fall_days,
        amount_rank = EXCLUDED.amount_rank,
        amount_ma_3 = EXCLUDED.amount_ma_3,
        amount_ma_5 = EXCLUDED.amount_ma_5,
        high_5d = EXCLUDED.high_5d,
        low_5d = EXCLUDED.low_5d,
        life_high_week = EXCLUDED.life_high_week,
        life_high_month = EXCLUDED.life_high_month,
        life_high_3month = EXCLUDED.life_high_3month,
        life_high_6month = EXCLUDED.life_high_6month,
        life_high_one_year = EXCLUDED.life_high_one_year,
        is_st = EXCLUDED.is_st,
        is_suspended = EXCLUDED.is_suspended,
        is_limit_up = EXCLUDED.is_limit_up,
        is_limit_down = EXCLUDED.is_limit_down,
        tradable_flag = EXCLUDED.tradable_flag,
        is_hs300 = EXCLUDED.is_hs300,
        is_csi1000 = EXCLUDED.is_csi1000,
        hit_reasons = EXCLUDED.hit_reasons,
        risk_flags = EXCLUDED.risk_flags,
        thesis_summary = EXCLUDED.thesis_summary,
        source_updated_at = EXCLUDED.source_updated_at,
        updated_at = NOW()
      RETURNING source_updated_at
    )
    SELECT COUNT(*), MAX(source_updated_at)
      INTO v_imported_rows, v_max_source_updated_at
      FROM upserted;
  ELSIF v_has_stock_selection THEN
    WITH candidate_runs AS (
      SELECT
        r.run_id,
        r.tenant_id,
        r.user_id,
        r.model_id,
        r.data_trade_date,
        r.prediction_trade_date,
        r.updated_at AS run_updated_at
      FROM qm_model_inference_runs r
      WHERE r.status = 'completed'
        AND r.prediction_trade_date = v_imported_prediction_trade_date
        AND (p_tenant_id IS NULL OR r.tenant_id = p_tenant_id)
        AND (p_user_id IS NULL OR r.user_id = p_user_id)
        AND (
          p_force_rebuild
          OR r.updated_at > COALESCE(v_last_source_updated_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')
        )
    ),
    signal_rows AS (
      SELECT
        ranked.run_id,
        ranked.tenant_id,
        ranked.user_id,
        ranked.trade_date,
        ranked.symbol_norm,
        ranked.light_score,
        ranked.tft_score,
        ranked.fusion_score,
        ranked.score_rank,
        ranked.universe_tag,
        ranked.signal_side,
        ranked.expected_price,
        ranked.quality,
        ranked.created_at
      FROM (
        SELECT
          s.run_id,
          s.tenant_id,
          s.user_id,
          s.trade_date,
          LPAD(REGEXP_REPLACE(s.symbol, '[^0-9]', '', 'g'), 6, '0') AS symbol_norm,
          s.light_score,
          s.tft_score,
          s.fusion_score,
          s.score_rank,
          s.universe_tag,
          s.signal_side,
          s.expected_price,
          COALESCE(s.quality, '{}'::jsonb) AS quality,
          s.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY s.run_id, s.tenant_id, s.user_id, s.trade_date, LPAD(REGEXP_REPLACE(s.symbol, '[^0-9]', '', 'g'), 6, '0')
            ORDER BY s.created_at DESC NULLS LAST, s.fusion_score DESC NULLS LAST
          ) AS row_num
        FROM engine_signal_scores s
        JOIN candidate_runs r
          ON r.run_id = s.run_id
         AND r.tenant_id = s.tenant_id
         AND r.user_id = s.user_id
         AND r.prediction_trade_date = s.trade_date
      ) ranked
      WHERE ranked.row_num = 1
    ),
    source_rows AS (
      SELECT
        trade_date,
        LPAD(REGEXP_REPLACE(symbol, '[^0-9]', '', 'g'), 6, '0') AS symbol_norm,
        name AS stock_name,
        industry,
        '[]'::jsonb AS concept_tags,
        close AS close_price,
        pct_chg AS latest_change_pct,
        turnover_rate,
        amount,
        market_cap AS total_mv,
        NULL::TEXT AS market_type,
        NULL::TEXT AS province,
        NULL::TEXT AS city,
        CASE WHEN is_listed_over_1y THEN 366 ELSE NULL END AS listed_days,
        0 AS consecutive_limit_up_days,
        0 AS recent_limit_up_count_5d,
        COALESCE(vol_3d_up, FALSE) AS volume_trend_3d,
        COALESCE(vol_5d_up, FALSE) AS volume_trend_5d,
        vol_3d_trend AS volume_trend_3d_score,
        vol_3d_sum / 3.0 AS volume_ma_3,
        vol_5d_avg AS volume_ma_5,
        ma5,
        ma10,
        ma20,
        return_5d,
        return_10d,
        close_above_ma5,
        close_above_ma10,
        NULL::INTEGER AS continued_rise_days,
        NULL::INTEGER AS continued_fall_days,
        amount_rank,
        amount_3d_sum / 3.0 AS amount_ma_3,
        amount_5d_sum / 5.0 AS amount_ma_5,
        high_5d,
        low_5d,
        NULL::DOUBLE PRECISION AS life_high_week,
        NULL::DOUBLE PRECISION AS life_high_month,
        NULL::DOUBLE PRECISION AS life_high_3month,
        NULL::DOUBLE PRECISION AS life_high_6month,
        NULL::DOUBLE PRECISION AS life_high_one_year,
        COALESCE(is_st, FALSE) AS is_st,
        COALESCE(is_suspended, FALSE) AS is_suspended,
        FALSE AS is_limit_up,
        FALSE AS is_limit_down,
        FALSE AS is_hs300,
        FALSE AS is_csi1000
      FROM stock_selection
      WHERE trade_date = v_imported_prediction_trade_date
    ),
    upserted AS (
      INSERT INTO qm_research_candidate_snapshot (
        tenant_id, user_id, run_id, model_id, data_trade_date, prediction_trade_date, market_snapshot_trade_date,
        symbol, stock_name, industry, concept_tags, close_price, latest_change_pct, turnover_rate, amount, total_mv,
        market_type, province, city, fusion_score, light_score, tft_score, score_rank, signal_side, expected_price,
        universe_tag, quality, confidence_level, confidence_score, listed_days, consecutive_limit_up_days,
        recent_limit_up_count_5d, volume_trend_3d, volume_trend_5d, volume_trend_3d_score, volume_ma_3, volume_ma_5,
        ma5, ma10, ma20, return_5d, return_10d, close_above_ma5, close_above_ma10, continued_rise_days,
        continued_fall_days, amount_rank, amount_ma_3, amount_ma_5, high_5d, low_5d, life_high_week,
        life_high_month, life_high_3month, life_high_6month, life_high_one_year, is_st, is_suspended,
        is_limit_up, is_limit_down, tradable_flag, is_hs300, is_csi1000, hit_reasons, risk_flags,
        thesis_summary, source_updated_at, updated_at
      )
      SELECT
        r.tenant_id, r.user_id, r.run_id, r.model_id, r.data_trade_date, r.prediction_trade_date, sr.trade_date,
        s.symbol_norm, sr.stock_name, sr.industry, COALESCE(sr.concept_tags, '[]'::jsonb), sr.close_price, sr.latest_change_pct, sr.turnover_rate,
        sr.amount, sr.total_mv, sr.market_type, sr.province, sr.city, s.fusion_score, s.light_score, s.tft_score,
        s.score_rank, s.signal_side, s.expected_price, s.universe_tag, s.quality,
        CASE WHEN s.fusion_score >= 0.80 THEN 'high' WHEN s.fusion_score >= 0.60 THEN 'medium' ELSE 'watch' END,
        s.fusion_score, sr.listed_days, COALESCE(sr.consecutive_limit_up_days, 0), COALESCE(sr.recent_limit_up_count_5d, 0), COALESCE(sr.volume_trend_3d, FALSE),
        COALESCE(sr.volume_trend_5d, FALSE), sr.volume_trend_3d_score, sr.volume_ma_3, sr.volume_ma_5, sr.ma5, sr.ma10, sr.ma20,
        sr.return_5d, sr.return_10d, sr.close_above_ma5, sr.close_above_ma10, sr.continued_rise_days,
        sr.continued_fall_days, sr.amount_rank, sr.amount_ma_3, sr.amount_ma_5, sr.high_5d, sr.low_5d,
        sr.life_high_week, sr.life_high_month, sr.life_high_3month, sr.life_high_6month, sr.life_high_one_year,
        sr.is_st, sr.is_suspended, sr.is_limit_up, sr.is_limit_down,
        NOT COALESCE(sr.is_st, FALSE) AND NOT COALESCE(sr.is_suspended, FALSE),
        sr.is_hs300, sr.is_csi1000,
        TO_JSONB(ARRAY_REMOVE(ARRAY[
          CASE WHEN s.fusion_score >= 0.80 THEN '模型高分' END,
          CASE WHEN sr.volume_trend_3d THEN '3日量能递增' END,
          CASE WHEN sr.close_above_ma5 THEN '站上MA5' END,
          CASE WHEN sr.industry IS NOT NULL THEN '行业:' || sr.industry END
        ]::TEXT[], NULL)),
        TO_JSONB(ARRAY_REMOVE(ARRAY[
          CASE WHEN COALESCE(sr.is_st, FALSE) THEN 'ST风险' END,
          CASE WHEN COALESCE(sr.is_suspended, FALSE) THEN '停牌' END,
          CASE WHEN sr.close_above_ma10 = FALSE THEN '弱于MA10' END
        ]::TEXT[], NULL)),
        CASE WHEN sr.industry IS NOT NULL THEN '模型高分，' || sr.industry || '方向，结合量能与均线进一步研究' ELSE '模型高分候选，结合量能与均线进一步研究' END,
        GREATEST(COALESCE(s.created_at, TIMESTAMPTZ '1970-01-01 00:00:00+00'), COALESCE(r.run_updated_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')),
        NOW()
      FROM candidate_runs r
      JOIN signal_rows s
        ON s.run_id = r.run_id AND s.tenant_id = r.tenant_id AND s.user_id = r.user_id
      LEFT JOIN source_rows sr
        ON sr.trade_date = r.prediction_trade_date AND sr.symbol_norm = s.symbol_norm
      ON CONFLICT (tenant_id, user_id, run_id, symbol)
      DO UPDATE SET
        model_id = EXCLUDED.model_id,
        data_trade_date = EXCLUDED.data_trade_date,
        prediction_trade_date = EXCLUDED.prediction_trade_date,
        market_snapshot_trade_date = EXCLUDED.market_snapshot_trade_date,
        stock_name = EXCLUDED.stock_name,
        industry = EXCLUDED.industry,
        concept_tags = EXCLUDED.concept_tags,
        close_price = EXCLUDED.close_price,
        latest_change_pct = EXCLUDED.latest_change_pct,
        turnover_rate = EXCLUDED.turnover_rate,
        amount = EXCLUDED.amount,
        total_mv = EXCLUDED.total_mv,
        market_type = EXCLUDED.market_type,
        province = EXCLUDED.province,
        city = EXCLUDED.city,
        fusion_score = EXCLUDED.fusion_score,
        light_score = EXCLUDED.light_score,
        tft_score = EXCLUDED.tft_score,
        score_rank = EXCLUDED.score_rank,
        signal_side = EXCLUDED.signal_side,
        expected_price = EXCLUDED.expected_price,
        universe_tag = EXCLUDED.universe_tag,
        quality = EXCLUDED.quality,
        confidence_level = EXCLUDED.confidence_level,
        confidence_score = EXCLUDED.confidence_score,
        listed_days = EXCLUDED.listed_days,
        consecutive_limit_up_days = EXCLUDED.consecutive_limit_up_days,
        recent_limit_up_count_5d = EXCLUDED.recent_limit_up_count_5d,
        volume_trend_3d = EXCLUDED.volume_trend_3d,
        volume_trend_5d = EXCLUDED.volume_trend_5d,
        volume_trend_3d_score = EXCLUDED.volume_trend_3d_score,
        volume_ma_3 = EXCLUDED.volume_ma_3,
        volume_ma_5 = EXCLUDED.volume_ma_5,
        ma5 = EXCLUDED.ma5,
        ma10 = EXCLUDED.ma10,
        ma20 = EXCLUDED.ma20,
        return_5d = EXCLUDED.return_5d,
        return_10d = EXCLUDED.return_10d,
        close_above_ma5 = EXCLUDED.close_above_ma5,
        close_above_ma10 = EXCLUDED.close_above_ma10,
        continued_rise_days = EXCLUDED.continued_rise_days,
        continued_fall_days = EXCLUDED.continued_fall_days,
        amount_rank = EXCLUDED.amount_rank,
        amount_ma_3 = EXCLUDED.amount_ma_3,
        amount_ma_5 = EXCLUDED.amount_ma_5,
        high_5d = EXCLUDED.high_5d,
        low_5d = EXCLUDED.low_5d,
        life_high_week = EXCLUDED.life_high_week,
        life_high_month = EXCLUDED.life_high_month,
        life_high_3month = EXCLUDED.life_high_3month,
        life_high_6month = EXCLUDED.life_high_6month,
        life_high_one_year = EXCLUDED.life_high_one_year,
        is_st = EXCLUDED.is_st,
        is_suspended = EXCLUDED.is_suspended,
        is_limit_up = EXCLUDED.is_limit_up,
        is_limit_down = EXCLUDED.is_limit_down,
        tradable_flag = EXCLUDED.tradable_flag,
        is_hs300 = EXCLUDED.is_hs300,
        is_csi1000 = EXCLUDED.is_csi1000,
        hit_reasons = EXCLUDED.hit_reasons,
        risk_flags = EXCLUDED.risk_flags,
        thesis_summary = EXCLUDED.thesis_summary,
        source_updated_at = EXCLUDED.source_updated_at,
        updated_at = NOW()
      RETURNING source_updated_at
    )
    SELECT COUNT(*), MAX(source_updated_at)
      INTO v_imported_rows, v_max_source_updated_at
      FROM upserted;
  ELSE
    WITH candidate_runs AS (
      SELECT
        r.run_id,
        r.tenant_id,
        r.user_id,
        r.model_id,
        r.data_trade_date,
        r.prediction_trade_date,
        r.updated_at AS run_updated_at
      FROM qm_model_inference_runs r
      WHERE r.status = 'completed'
        AND r.prediction_trade_date = v_imported_prediction_trade_date
        AND (p_tenant_id IS NULL OR r.tenant_id = p_tenant_id)
        AND (p_user_id IS NULL OR r.user_id = p_user_id)
        AND (
          p_force_rebuild
          OR r.updated_at > COALESCE(v_last_source_updated_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')
        )
    ),
    signal_rows AS (
      SELECT
        ranked.run_id,
        ranked.tenant_id,
        ranked.user_id,
        ranked.trade_date,
        ranked.symbol_norm,
        ranked.light_score,
        ranked.tft_score,
        ranked.fusion_score,
        ranked.score_rank,
        ranked.universe_tag,
        ranked.signal_side,
        ranked.expected_price,
        ranked.quality,
        ranked.created_at
      FROM (
        SELECT
          s.run_id,
          s.tenant_id,
          s.user_id,
          s.trade_date,
          LPAD(REGEXP_REPLACE(s.symbol, '[^0-9]', '', 'g'), 6, '0') AS symbol_norm,
          s.light_score,
          s.tft_score,
          s.fusion_score,
          s.score_rank,
          s.universe_tag,
          s.signal_side,
          s.expected_price,
          COALESCE(s.quality, '{}'::jsonb) AS quality,
          s.created_at,
          ROW_NUMBER() OVER (
            PARTITION BY s.run_id, s.tenant_id, s.user_id, s.trade_date, LPAD(REGEXP_REPLACE(s.symbol, '[^0-9]', '', 'g'), 6, '0')
            ORDER BY s.created_at DESC NULLS LAST, s.fusion_score DESC NULLS LAST
          ) AS row_num
        FROM engine_signal_scores s
        JOIN candidate_runs r
          ON r.run_id = s.run_id AND r.tenant_id = s.tenant_id AND r.user_id = s.user_id AND r.prediction_trade_date = s.trade_date
      ) ranked
      WHERE ranked.row_num = 1
    ),
    source_rows AS (
      SELECT
        mdd.trade_date,
        mdd.symbol AS symbol_norm,
        NULL::TEXT AS stock_name,
        NULL::TEXT AS industry,
        '[]'::jsonb AS concept_tags,
        mdd.raw_close AS close_price,
        NULL::DOUBLE PRECISION AS latest_change_pct,
        NULL::DOUBLE PRECISION AS turnover_rate,
        NULL::DOUBLE PRECISION AS amount,
        NULL::DOUBLE PRECISION AS total_mv,
        NULL::TEXT AS market_type,
        NULL::TEXT AS province,
        NULL::TEXT AS city,
        NULL::INTEGER AS listed_days,
        0 AS consecutive_limit_up_days,
        0 AS recent_limit_up_count_5d,
        FALSE AS volume_trend_3d,
        FALSE AS volume_trend_5d,
        NULL::INTEGER AS volume_trend_3d_score,
        NULL::DOUBLE PRECISION AS volume_ma_3,
        NULL::DOUBLE PRECISION AS volume_ma_5,
        NULL::DOUBLE PRECISION AS ma5,
        NULL::DOUBLE PRECISION AS ma10,
        NULL::DOUBLE PRECISION AS ma20,
        NULL::DOUBLE PRECISION AS return_5d,
        NULL::DOUBLE PRECISION AS return_10d,
        NULL::BOOLEAN AS close_above_ma5,
        NULL::BOOLEAN AS close_above_ma10,
        NULL::INTEGER AS continued_rise_days,
        NULL::INTEGER AS continued_fall_days,
        NULL::INTEGER AS amount_rank,
        NULL::DOUBLE PRECISION AS amount_ma_3,
        NULL::DOUBLE PRECISION AS amount_ma_5,
        NULL::DOUBLE PRECISION AS high_5d,
        NULL::DOUBLE PRECISION AS low_5d,
        NULL::DOUBLE PRECISION AS life_high_week,
        NULL::DOUBLE PRECISION AS life_high_month,
        NULL::DOUBLE PRECISION AS life_high_3month,
        NULL::DOUBLE PRECISION AS life_high_6month,
        NULL::DOUBLE PRECISION AS life_high_one_year,
        FALSE AS is_st,
        FALSE AS is_suspended,
        FALSE AS is_limit_up,
        FALSE AS is_limit_down,
        FALSE AS is_hs300,
        FALSE AS is_csi1000
      FROM market_data_daily mdd
      WHERE mdd.trade_date = v_imported_prediction_trade_date
        AND mdd.raw_close IS NOT NULL
    ),
    upserted AS (
      INSERT INTO qm_research_candidate_snapshot (
        tenant_id, user_id, run_id, model_id, data_trade_date, prediction_trade_date, market_snapshot_trade_date,
        symbol, stock_name, industry, concept_tags, close_price, latest_change_pct, turnover_rate, amount, total_mv,
        market_type, province, city, fusion_score, light_score, tft_score, score_rank, signal_side, expected_price,
        universe_tag, quality, confidence_level, confidence_score, listed_days, consecutive_limit_up_days,
        recent_limit_up_count_5d, volume_trend_3d, volume_trend_5d, volume_trend_3d_score, volume_ma_3, volume_ma_5,
        ma5, ma10, ma20, return_5d, return_10d, close_above_ma5, close_above_ma10, continued_rise_days,
        continued_fall_days, amount_rank, amount_ma_3, amount_ma_5, high_5d, low_5d, life_high_week,
        life_high_month, life_high_3month, life_high_6month, life_high_one_year, is_st, is_suspended,
        is_limit_up, is_limit_down, tradable_flag, is_hs300, is_csi1000, hit_reasons, risk_flags,
        thesis_summary, source_updated_at, updated_at
      )
      SELECT
        r.tenant_id, r.user_id, r.run_id, r.model_id, r.data_trade_date, r.prediction_trade_date, sr.trade_date,
        s.symbol_norm, sr.stock_name, sr.industry, COALESCE(sr.concept_tags, '[]'::jsonb), sr.close_price, sr.latest_change_pct, sr.turnover_rate,
        sr.amount, sr.total_mv, sr.market_type, sr.province, sr.city, s.fusion_score, s.light_score, s.tft_score,
        s.score_rank, s.signal_side, s.expected_price, s.universe_tag, s.quality,
        CASE WHEN s.fusion_score >= 0.80 THEN 'high' WHEN s.fusion_score >= 0.60 THEN 'medium' ELSE 'watch' END,
        s.fusion_score, sr.listed_days, COALESCE(sr.consecutive_limit_up_days, 0), COALESCE(sr.recent_limit_up_count_5d, 0), COALESCE(sr.volume_trend_3d, FALSE),
        COALESCE(sr.volume_trend_5d, FALSE), sr.volume_trend_3d_score, sr.volume_ma_3, sr.volume_ma_5, sr.ma5, sr.ma10, sr.ma20,
        sr.return_5d, sr.return_10d, sr.close_above_ma5, sr.close_above_ma10, sr.continued_rise_days,
        sr.continued_fall_days, sr.amount_rank, sr.amount_ma_3, sr.amount_ma_5, sr.high_5d, sr.low_5d,
        sr.life_high_week, sr.life_high_month, sr.life_high_3month, sr.life_high_6month, sr.life_high_one_year,
        sr.is_st, sr.is_suspended, sr.is_limit_up, sr.is_limit_down,
        NOT COALESCE(sr.is_st, FALSE),
        sr.is_hs300, sr.is_csi1000,
        TO_JSONB(ARRAY_REMOVE(ARRAY[
          CASE WHEN s.fusion_score >= 0.80 THEN '模型高分' END,
          CASE WHEN sr.industry IS NOT NULL THEN '行业:' || sr.industry END
        ]::TEXT[], NULL)),
        TO_JSONB(ARRAY_REMOVE(ARRAY[
          CASE WHEN COALESCE(sr.is_st, FALSE) THEN 'ST风险' END
        ]::TEXT[], NULL)),
        CASE WHEN sr.industry IS NOT NULL THEN '模型高分，' || sr.industry || '方向，待补充更多技术字段' ELSE '模型高分候选，待补充更多技术字段' END,
        GREATEST(COALESCE(s.created_at, TIMESTAMPTZ '1970-01-01 00:00:00+00'), COALESCE(r.run_updated_at, TIMESTAMPTZ '1970-01-01 00:00:00+00')),
        NOW()
      FROM candidate_runs r
      JOIN signal_rows s
        ON s.run_id = r.run_id AND s.tenant_id = r.tenant_id AND s.user_id = r.user_id
      LEFT JOIN source_rows sr
        ON sr.trade_date = r.prediction_trade_date AND sr.symbol_norm = s.symbol_norm
      ON CONFLICT (tenant_id, user_id, run_id, symbol)
      DO UPDATE SET
        model_id = EXCLUDED.model_id,
        data_trade_date = EXCLUDED.data_trade_date,
        prediction_trade_date = EXCLUDED.prediction_trade_date,
        market_snapshot_trade_date = EXCLUDED.market_snapshot_trade_date,
        stock_name = EXCLUDED.stock_name,
        industry = EXCLUDED.industry,
        concept_tags = EXCLUDED.concept_tags,
        close_price = EXCLUDED.close_price,
        latest_change_pct = EXCLUDED.latest_change_pct,
        turnover_rate = EXCLUDED.turnover_rate,
        amount = EXCLUDED.amount,
        total_mv = EXCLUDED.total_mv,
        market_type = EXCLUDED.market_type,
        province = EXCLUDED.province,
        city = EXCLUDED.city,
        fusion_score = EXCLUDED.fusion_score,
        light_score = EXCLUDED.light_score,
        tft_score = EXCLUDED.tft_score,
        score_rank = EXCLUDED.score_rank,
        signal_side = EXCLUDED.signal_side,
        expected_price = EXCLUDED.expected_price,
        universe_tag = EXCLUDED.universe_tag,
        quality = EXCLUDED.quality,
        confidence_level = EXCLUDED.confidence_level,
        confidence_score = EXCLUDED.confidence_score,
        listed_days = EXCLUDED.listed_days,
        consecutive_limit_up_days = EXCLUDED.consecutive_limit_up_days,
        recent_limit_up_count_5d = EXCLUDED.recent_limit_up_count_5d,
        volume_trend_3d = EXCLUDED.volume_trend_3d,
        volume_trend_5d = EXCLUDED.volume_trend_5d,
        volume_trend_3d_score = EXCLUDED.volume_trend_3d_score,
        volume_ma_3 = EXCLUDED.volume_ma_3,
        volume_ma_5 = EXCLUDED.volume_ma_5,
        ma5 = EXCLUDED.ma5,
        ma10 = EXCLUDED.ma10,
        ma20 = EXCLUDED.ma20,
        return_5d = EXCLUDED.return_5d,
        return_10d = EXCLUDED.return_10d,
        close_above_ma5 = EXCLUDED.close_above_ma5,
        close_above_ma10 = EXCLUDED.close_above_ma10,
        continued_rise_days = EXCLUDED.continued_rise_days,
        continued_fall_days = EXCLUDED.continued_fall_days,
        amount_rank = EXCLUDED.amount_rank,
        amount_ma_3 = EXCLUDED.amount_ma_3,
        amount_ma_5 = EXCLUDED.amount_ma_5,
        high_5d = EXCLUDED.high_5d,
        low_5d = EXCLUDED.low_5d,
        life_high_week = EXCLUDED.life_high_week,
        life_high_month = EXCLUDED.life_high_month,
        life_high_3month = EXCLUDED.life_high_3month,
        life_high_6month = EXCLUDED.life_high_6month,
        life_high_one_year = EXCLUDED.life_high_one_year,
        is_st = EXCLUDED.is_st,
        is_suspended = EXCLUDED.is_suspended,
        is_limit_up = EXCLUDED.is_limit_up,
        is_limit_down = EXCLUDED.is_limit_down,
        tradable_flag = EXCLUDED.tradable_flag,
        is_hs300 = EXCLUDED.is_hs300,
        is_csi1000 = EXCLUDED.is_csi1000,
        hit_reasons = EXCLUDED.hit_reasons,
        risk_flags = EXCLUDED.risk_flags,
        thesis_summary = EXCLUDED.thesis_summary,
        source_updated_at = EXCLUDED.source_updated_at,
        updated_at = NOW()
      RETURNING source_updated_at
    )
    SELECT COUNT(*), MAX(source_updated_at)
      INTO v_imported_rows, v_max_source_updated_at
      FROM upserted;
  END IF;
  -- 概念标签回填（导入源无 concept 时兜底）：
  -- 1) 优先使用 industry_classification.stock_codes 映射的行业标签；
  -- 2) 若仍为空且 industry 非空，则使用 industry 作为单标签。
  WITH concept_map AS (
    SELECT
      TRIM(SPLIT_PART(code_item, '.', 1)) AS symbol_norm,
      JSONB_AGG(ic.industry_name ORDER BY ic.industry_name) AS concept_tags
    FROM industry_classification ic
    CROSS JOIN LATERAL regexp_split_to_table(COALESCE(ic.stock_codes, ''), ',') AS code_item
    WHERE COALESCE(ic.industry_name, '') <> ''
      AND COALESCE(TRIM(code_item), '') <> ''
    GROUP BY TRIM(SPLIT_PART(code_item, '.', 1))
  )
  UPDATE qm_research_candidate_snapshot s
     SET concept_tags = cm.concept_tags,
         updated_at = NOW()
    FROM concept_map cm
   WHERE s.prediction_trade_date = v_imported_prediction_trade_date
     AND (p_tenant_id IS NULL OR s.tenant_id = p_tenant_id)
     AND (p_user_id IS NULL OR s.user_id = p_user_id)
     AND (s.concept_tags IS NULL OR s.concept_tags = '[]'::jsonb)
     AND s.symbol = cm.symbol_norm;
  UPDATE qm_research_candidate_snapshot s
     SET concept_tags = jsonb_build_array(s.industry),
         updated_at = NOW()
   WHERE s.prediction_trade_date = v_imported_prediction_trade_date
     AND (p_tenant_id IS NULL OR s.tenant_id = p_tenant_id)
     AND (p_user_id IS NULL OR s.user_id = p_user_id)
     AND (s.concept_tags IS NULL OR s.concept_tags = '[]'::jsonb)
     AND COALESCE(s.industry, '') <> '';
  SELECT run_id
    INTO v_last_run_id
    FROM qm_research_candidate_snapshot
   WHERE prediction_trade_date = v_imported_prediction_trade_date
     AND (p_tenant_id IS NULL OR tenant_id = p_tenant_id)
     AND (p_user_id IS NULL OR user_id = p_user_id)
   ORDER BY source_updated_at DESC NULLS LAST, run_id DESC
   LIMIT 1;
  UPDATE qm_research_import_state
     SET last_source_updated_at = COALESCE(v_max_source_updated_at, last_source_updated_at),
         last_prediction_trade_date = COALESCE(v_imported_prediction_trade_date, last_prediction_trade_date),
         last_run_id = COALESCE(v_last_run_id, last_run_id),
         updated_at = NOW(),
         extra_json = jsonb_build_object('source_mode', v_source_mode, 'force_rebuild', p_force_rebuild)
   WHERE job_name = 'research_candidate_snapshot';
  RETURN QUERY
  SELECT COALESCE(v_imported_rows, 0), v_imported_prediction_trade_date, v_source_mode, v_max_source_updated_at;
END;
$$;
SET default_tablespace = '';
SET default_table_access_method = heap;
--
-- Name: admin_data_files; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.admin_data_files (
    id integer NOT NULL,
    tenant_id character varying(64) NOT NULL,
    data_source_id integer,
    filename character varying(255) NOT NULL,
    file_size integer,
    status character varying(32),
    meta json,
    created_at timestamp without time zone
);
--
-- Name: admin_data_files_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.admin_data_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: admin_data_files_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.admin_data_files_id_seq OWNED BY public.admin_data_files.id;
--
-- Name: admin_models; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.admin_models (
    id integer NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    name character varying(128) NOT NULL,
    description text,
    source_type character varying(32) NOT NULL,
    start_date timestamp without time zone,
    end_date timestamp without time zone,
    config json,
    is_active boolean,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);
--
-- Name: admin_models_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.admin_models_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: admin_models_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.admin_models_id_seq OWNED BY public.admin_models.id;
--
-- Name: admin_training_jobs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.admin_training_jobs (
    id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    status character varying(32),
    instance_id character varying(64),
    request_payload json,
    logs text,
    result json,
    progress integer,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);
--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);
--
-- Name: alembic_version_community; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.alembic_version_community (
    version_num character varying(32) NOT NULL
);
--
-- Name: api_keys; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.api_keys (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    name character varying(128),
    permissions jsonb DEFAULT '[]'::jsonb,
    last_used_at timestamp with time zone,
    expires_at timestamp with time zone,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    access_key character varying(64),
    secret_hash character varying(255)
);
--
-- Name: api_keys_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.api_keys_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: api_keys_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.api_keys_id_seq OWNED BY public.api_keys.id;
--
-- Name: audit_logs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.audit_logs (
    id integer NOT NULL,
    user_id character varying(64),
    tenant_id character varying(64) DEFAULT 'default'::character varying,
    action character varying(100) NOT NULL,
    resource_type character varying(50),
    resource_id character varying(100),
    old_value jsonb,
    new_value jsonb,
    ip_address character varying(64),
    user_agent character varying(255),
    created_at timestamp with time zone DEFAULT now()
);
--
-- Name: audit_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.audit_logs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: audit_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.audit_logs_id_seq OWNED BY public.audit_logs.id;
--
-- Name: backtests; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.backtests (
    id integer NOT NULL,
    strategy_id integer NOT NULL,
    user_id integer NOT NULL,
    start_date timestamp(6) without time zone NOT NULL,
    end_date timestamp(6) without time zone NOT NULL,
    initial_capital double precision NOT NULL,
    final_value double precision,
    total_return double precision,
    annual_return double precision,
    max_drawdown double precision,
    sharpe_ratio double precision,
    win_rate double precision,
    trades json NOT NULL,
    metrics json NOT NULL,
    status character varying(20) NOT NULL,
    error_message text,
    created_at timestamp(6) without time zone NOT NULL,
    completed_at timestamp(6) without time zone,
    code_snapshot text,
    code_hash_snapshot character varying(64)
);
--
-- Name: backtests_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.backtests_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;
--
-- Name: backtests_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.backtests_id_seq OWNED BY public.backtests.id;
--
-- Name: community_audit_logs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.community_audit_logs (
    id bigint NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    action character varying(64) NOT NULL,
    entity_type character varying(64) NOT NULL,
    entity_id character varying(64),
    ip character varying(64),
    user_agent character varying(256),
    meta json,
    created_at timestamp without time zone
);
--
-- Name: community_audit_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.community_audit_logs_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: community_audit_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.community_audit_logs_id_seq OWNED BY public.community_audit_logs.id;
--
-- Name: community_author_follows; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.community_author_follows (
    id bigint NOT NULL,
    tenant_id character varying(64) NOT NULL,
    follower_user_id character varying(64) NOT NULL,
    author_user_id character varying(64) NOT NULL,
    created_at timestamp without time zone
);
--
-- Name: community_author_follows_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.community_author_follows_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: community_author_follows_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.community_author_follows_id_seq OWNED BY public.community_author_follows.id;
--
-- Name: community_comments; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.community_comments (
    id bigint NOT NULL,
    tenant_id character varying(64) NOT NULL,
    post_id bigint NOT NULL,
    author_id character varying(64) NOT NULL,
    content text NOT NULL,
    parent_id bigint,
    reply_to_id bigint,
    likes integer,
    created_at timestamp without time zone,
    updated_at timestamp without time zone
);
--
-- Name: community_comments_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.community_comments_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: community_comments_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.community_comments_id_seq OWNED BY public.community_comments.id;
--
-- Name: community_interactions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.community_interactions (
    id bigint NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    post_id bigint,
    comment_id bigint,
    type character varying(32) NOT NULL,
    created_at timestamp without time zone
);
--
-- Name: community_interactions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.community_interactions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: community_interactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.community_interactions_id_seq OWNED BY public.community_interactions.id;
--
-- Name: community_posts; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.community_posts (
    id bigint NOT NULL,
    tenant_id character varying(64) NOT NULL,
    author_id character varying(64) NOT NULL,
    title character varying(256) NOT NULL,
    content text NOT NULL,
    category character varying(64),
    tags json,
    media json,
    excerpt text,
    views integer,
    likes integer,
    comments integer,
    collections integer,
    pinned boolean,
    featured boolean,
    created_at timestamp without time zone,
    updated_at timestamp without time zone,
    last_comment_at timestamp without time zone
);
--
-- Name: community_posts_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.community_posts_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: community_posts_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.community_posts_id_seq OWNED BY public.community_posts.id;
--
-- Name: data_download_orders; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.data_download_orders (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    order_no character varying(64) NOT NULL,
    amount double precision NOT NULL,
    currency character varying(10),
    status character varying(20),
    download_type character varying(20),
    description character varying(255),
    metadata_info json,
    created_at timestamp with time zone DEFAULT now(),
    completed_at timestamp with time zone,
    expires_at timestamp with time zone,
    download_count integer DEFAULT 0
);
--
-- Name: data_download_orders_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.data_download_orders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: data_download_orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.data_download_orders_id_seq OWNED BY public.data_download_orders.id;
--
-- Name: email_verifications; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.email_verifications (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    email character varying(255) NOT NULL,
    verification_code character varying(128) NOT NULL,
    code_type character varying(32) NOT NULL,
    is_used boolean,
    is_expired boolean,
    created_at timestamp with time zone DEFAULT now(),
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone,
    attempts integer,
    ip_address character varying(64)
);
--
-- Name: email_verifications_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.email_verifications_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: email_verifications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.email_verifications_id_seq OWNED BY public.email_verifications.id;
--
-- Name: engine_dispatch_batches; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.engine_dispatch_batches (
    batch_id character varying(64) NOT NULL,
    run_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) NOT NULL,
    trade_date date NOT NULL,
    strategy_id character varying(64),
    trading_mode character varying(16) DEFAULT 'REAL'::character varying NOT NULL,
    stage character varying(24) DEFAULT 'signal_ready'::character varying NOT NULL,
    stage_updated_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    total_signals integer DEFAULT 0 NOT NULL,
    dispatched_signals integer DEFAULT 0 NOT NULL,
    acked_signals integer DEFAULT 0 NOT NULL,
    order_submitted_count integer DEFAULT 0 NOT NULL,
    order_filled_count integer DEFAULT 0 NOT NULL,
    failed_count integer DEFAULT 0 NOT NULL,
    trace_id character varying(128),
    last_error text,
    created_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    updated_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_engine_dispatch_batches_mode CHECK (((trading_mode)::text = ANY (ARRAY[('REAL'::character varying)::text, ('SHADOW'::character varying)::text, ('SIMULATION'::character varying)::text]))),
    CONSTRAINT ck_engine_dispatch_batches_stage CHECK (((stage)::text = ANY (ARRAY[('signal_ready'::character varying)::text, ('dispatched'::character varying)::text, ('runner_applied'::character varying)::text, ('order_sent'::character varying)::text, ('fill_confirmed'::character varying)::text, ('failed'::character varying)::text])))
);
--
-- Name: engine_dispatch_items; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.engine_dispatch_items (
    id bigint NOT NULL,
    batch_id character varying(64) NOT NULL,
    run_id character varying(64) NOT NULL,
    signal_id character varying(128),
    client_order_id character varying(128),
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) NOT NULL,
    trade_date date NOT NULL,
    symbol character varying(20) NOT NULL,
    action character varying(8) NOT NULL,
    quantity double precision NOT NULL,
    price double precision,
    score double precision,
    dispatch_status character varying(24) DEFAULT 'pending'::character varying NOT NULL,
    order_id uuid,
    exchange_order_id character varying(100),
    exchange_trade_id character varying(100),
    exec_message text,
    created_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    updated_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_engine_dispatch_items_action CHECK (((action)::text = ANY (ARRAY[('BUY'::character varying)::text, ('SELL'::character varying)::text, ('HOLD'::character varying)::text]))),
    CONSTRAINT ck_engine_dispatch_items_status CHECK (((dispatch_status)::text = ANY (ARRAY[('pending'::character varying)::text, ('dispatched'::character varying)::text, ('acked'::character varying)::text, ('order_submitted'::character varying)::text, ('order_filled'::character varying)::text, ('rejected'::character varying)::text, ('failed'::character varying)::text])))
);
--
-- Name: engine_dispatch_items_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.engine_dispatch_items_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: engine_dispatch_items_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.engine_dispatch_items_id_seq OWNED BY public.engine_dispatch_items.id;
--
-- Name: engine_feature_runs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.engine_feature_runs (
    run_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) NOT NULL,
    trade_date date NOT NULL,
    model_name character varying(64) DEFAULT 'model_qlib'::character varying NOT NULL,
    model_version character varying(64) NOT NULL,
    feature_version character varying(64) NOT NULL,
    feature_dim integer NOT NULL,
    window_start timestamp(6) with time zone,
    window_end timestamp(6) with time zone,
    status character varying(24) DEFAULT 'feature_ready'::character varying NOT NULL,
    expected_symbols integer DEFAULT 0 NOT NULL,
    ready_symbols integer DEFAULT 0 NOT NULL,
    missing_symbols integer DEFAULT 0 NOT NULL,
    source character varying(32) DEFAULT 'l2_batch'::character varying NOT NULL,
    checksum character varying(128),
    quality jsonb DEFAULT '{}'::jsonb NOT NULL,
    error_message text,
    created_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    updated_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    factor double precision,
    style_sp_ttm double precision,
    style_cfp_ttm double precision,
    style_ev_ebitda_ttm double precision,
    style_tobin_q double precision,
    mom_ret_1d double precision,
    mom_ret_3d double precision,
    mom_ret_5d double precision,
    mom_ret_10d double precision,
    mom_ret_20d double precision,
    mom_ret_60d double precision,
    mom_ret_120d double precision,
    mom_ma_gap_5 double precision,
    mom_ma_gap_10 double precision,
    mom_ma_gap_20 double precision,
    mom_ma_gap_60 double precision,
    mom_ma_gap_120 double precision,
    mom_ema_gap_12 double precision,
    mom_ema_gap_26 double precision,
    mom_macd_dif double precision,
    mom_macd_dea double precision,
    mom_macd_hist double precision,
    mom_rsi_6 double precision,
    mom_rsi_14 double precision,
    mom_kdj_k double precision,
    mom_kdj_d double precision,
    mom_kdj_j double precision,
    mom_roc_12 double precision,
    mom_breakout_20d double precision,
    vol_std_5 double precision,
    vol_std_10 double precision,
    vol_std_20 double precision,
    vol_std_60 double precision,
    vol_atr_14 double precision,
    vol_atr_20 double precision,
    vol_true_range double precision,
    vol_parkinson_10 double precision,
    vol_parkinson_20 double precision,
    vol_gk_10 double precision,
    vol_gk_20 double precision,
    vol_rs_10 double precision,
    vol_rs_20 double precision,
    vol_downside_20 double precision,
    vol_upside_20 double precision,
    vol_realized_rv double precision,
    vol_realized_rrv double precision,
    vol_realized_rskew double precision,
    vol_realized_rkurt double precision,
    vol_jump_zadj double precision,
    vol_jump_rjv_ratio double precision,
    vol_jump_sjv_ratio double precision,
    liq_turnover_os double precision,
    liq_turnover_tl double precision,
    liq_volume double precision,
    liq_volume_ma_5 double precision,
    liq_volume_ma_10 double precision,
    liq_volume_ma_20 double precision,
    liq_volume_ratio_5 double precision,
    liq_volume_ratio_20 double precision,
    liq_amount double precision,
    liq_amount_ma_5 double precision,
    liq_amount_ma_10 double precision,
    liq_amount_ma_20 double precision,
    liq_amount_ratio_5 double precision,
    liq_amount_ratio_20 double precision,
    liq_trade_count double precision,
    liq_avg_trade_size double precision,
    liq_obv_20 double precision,
    liq_obv_60 double precision,
    liq_mfi_14 double precision,
    liq_accdist_20 double precision,
    liq_amihud_20 double precision,
    liq_amihud_60 double precision,
    flow_net_amount double precision,
    flow_net_amount_ratio double precision,
    flow_large_net_amount double precision,
    flow_large_net_ratio double precision,
    flow_medium_net_amount double precision,
    flow_medium_net_ratio double precision,
    flow_small_net_amount double precision,
    flow_small_net_ratio double precision,
    flow_net_order_count double precision,
    flow_net_order_ratio double precision,
    flow_large_net_order double precision,
    flow_large_order_ratio double precision,
    flow_vpin double precision,
    flow_vpin_ma_5 double precision,
    flow_vpin_ma_20 double precision,
    flow_vpin_delta_5 double precision,
    flow_qsp double precision,
    flow_esp double precision,
    flow_aqsp double precision,
    flow_qsp_time double precision,
    flow_esp_time double precision,
    flow_pressure_index double precision,
    style_ln_mv_total double precision,
    style_ln_mv_float double precision,
    style_bp double precision,
    style_ep_ttm double precision,
    style_smb double precision,
    style_hml double precision,
    style_mkt_premium double precision,
    style_beta_20 double precision,
    style_beta_60 double precision,
    style_beta_120 double precision,
    style_idio_vol_20 double precision,
    style_idio_vol_60 double precision,
    style_residual_ret_20 double precision,
    style_valuation_composite double precision,
    style_size_percentile double precision,
    style_value_percentile double precision,
    ind_ret_1d double precision,
    ind_ret_5d double precision,
    ind_ret_10d double precision,
    ind_ret_20d double precision,
    ind_vol_20 double precision,
    ind_turnover_20 double precision,
    ind_amount_20 double precision,
    ind_strength_20 double precision,
    ind_strength_60 double precision,
    ind_dispersion_20 double precision,
    ind_up_breadth_20 double precision,
    ind_down_breadth_20 double precision,
    ind_relative_volume_20 double precision,
    ind_relative_volatility_20 double precision,
    ind_relative_flow_20 double precision,
    ind_momentum_rank_20 double precision,
    ind_value_rank double precision,
    ind_size_rank double precision,
    ind_code_l1 double precision,
    ind_code_l2 double precision,
    micro_qsp_equal double precision,
    micro_esp_equal double precision,
    micro_aqsp_equal double precision,
    micro_qsp_time double precision,
    micro_esp_time double precision,
    micro_qsp_volume double precision,
    micro_esp_volume double precision,
    micro_qsp_amount double precision,
    micro_esp_amount double precision,
    micro_effective_spread double precision,
    micro_quoted_spread double precision,
    micro_spread_vol_20 double precision,
    micro_imbalance_volume double precision,
    micro_imbalance_amount double precision,
    micro_imbalance_count double precision,
    micro_imbalance_large double precision,
    micro_imbalance_medium double precision,
    micro_imbalance_small double precision,
    micro_jump_flag double precision,
    micro_pressure_score double precision,
    feature_1 double precision,
    feature_2 double precision,
    feature_3 double precision,
    feature_4 double precision,
    feature_5 double precision,
    feature_6 double precision,
    feature_7 double precision,
    feature_8 double precision,
    feature_9 double precision,
    feature_10 double precision,
    feature_11 double precision,
    feature_12 double precision,
    feature_13 double precision,
    feature_14 double precision,
    feature_15 double precision,
    feature_16 double precision,
    feature_17 double precision,
    feature_18 double precision,
    feature_19 double precision,
    feature_20 double precision,
    feature_21 double precision,
    feature_22 double precision,
    feature_23 double precision,
    feature_24 double precision,
    feature_25 double precision,
    feature_26 double precision,
    feature_27 double precision,
    feature_28 double precision,
    feature_29 double precision,
    feature_30 double precision,
    feature_31 double precision,
    feature_32 double precision,
    feature_33 double precision,
    feature_34 double precision,
    feature_35 double precision,
    feature_36 double precision,
    feature_37 double precision,
    feature_38 double precision,
    feature_39 double precision,
    feature_40 double precision,
    feature_41 double precision,
    feature_42 double precision,
    feature_43 double precision,
    feature_44 double precision,
    feature_45 double precision,
    feature_46 double precision,
    feature_47 double precision,
    feature_48 double precision,
    feature_49 double precision,
    feature_50 double precision,
    feature_51 double precision,
    feature_52 double precision,
    feature_53 double precision,
    feature_54 double precision,
    feature_55 double precision,
    feature_56 double precision,
    feature_57 double precision,
    feature_58 double precision,
    feature_59 double precision,
    feature_60 double precision,
    feature_61 double precision,
    feature_62 double precision,
    feature_63 double precision,
    feature_64 double precision,
    feature_65 double precision,
    feature_66 double precision,
    feature_67 double precision,
    feature_68 double precision,
    feature_69 double precision,
    feature_70 double precision,
    feature_71 double precision,
    feature_72 double precision,
    feature_73 double precision,
    feature_74 double precision,
    feature_75 double precision,
    feature_76 double precision,
    feature_77 double precision,
    feature_78 double precision,
    feature_79 double precision,
    feature_80 double precision,
    feature_81 double precision,
    feature_82 double precision,
    feature_83 double precision,
    feature_84 double precision,
    feature_85 double precision,
    feature_86 double precision,
    feature_87 double precision,
    feature_88 double precision,
    feature_89 double precision,
    feature_90 double precision,
    feature_91 double precision,
    feature_92 double precision,
    feature_93 double precision,
    feature_94 double precision,
    feature_95 double precision,
    feature_96 double precision,
    feature_97 double precision,
    feature_98 double precision,
    feature_99 double precision,
    feature_100 double precision,
    feature_101 double precision,
    feature_102 double precision,
    feature_103 double precision,
    feature_104 double precision,
    feature_105 double precision,
    feature_106 double precision,
    feature_107 double precision,
    feature_108 double precision,
    feature_109 double precision,
    feature_110 double precision,
    feature_111 double precision,
    feature_112 double precision,
    feature_113 double precision,
    feature_114 double precision,
    feature_115 double precision,
    feature_116 double precision,
    feature_117 double precision,
    feature_118 double precision,
    feature_119 double precision,
    feature_120 double precision,
    feature_121 double precision,
    feature_122 double precision,
    feature_123 double precision,
    feature_124 double precision,
    feature_125 double precision,
    feature_126 double precision,
    feature_127 double precision,
    feature_128 double precision,
    feature_129 double precision,
    feature_130 double precision,
    feature_131 double precision,
    feature_132 double precision,
    feature_133 double precision,
    feature_134 double precision,
    feature_135 double precision,
    feature_136 double precision,
    feature_137 double precision,
    feature_138 double precision,
    feature_139 double precision,
    feature_140 double precision,
    feature_141 double precision,
    feature_142 double precision,
    feature_143 double precision,
    feature_144 double precision,
    feature_145 double precision,
    feature_146 double precision,
    feature_147 double precision,
    feature_148 double precision,
    feature_149 double precision,
    feature_150 double precision,
    feature_151 double precision,
    CONSTRAINT ck_engine_feature_runs_dim CHECK ((feature_dim > 0)),
    CONSTRAINT ck_engine_feature_runs_status CHECK (((status)::text = ANY (ARRAY[('feature_ready'::character varying)::text, ('signal_ready'::character varying)::text, ('dispatched'::character varying)::text, ('runner_applied'::character varying)::text, ('order_sent'::character varying)::text, ('fill_confirmed'::character varying)::text, ('failed'::character varying)::text])))
);
--
-- Name: engine_feature_snapshots; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.engine_feature_snapshots (
    id bigint NOT NULL,
    run_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) NOT NULL,
    trade_date date NOT NULL,
    symbol character varying(20) NOT NULL,
    model_version character varying(64) NOT NULL,
    feature_version character varying(64) NOT NULL,
    feature_dim integer NOT NULL,
    features jsonb NOT NULL,
    data_source character varying(32) DEFAULT 'l2'::character varying NOT NULL,
    is_valid boolean DEFAULT true NOT NULL,
    missing_ratio numeric(6,4),
    quality jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    CONSTRAINT ck_engine_feature_snapshots_dim CHECK ((feature_dim > 0))
);
--
-- Name: engine_feature_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.engine_feature_snapshots_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: engine_feature_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.engine_feature_snapshots_id_seq OWNED BY public.engine_feature_snapshots.id;
--
-- Name: engine_signal_scores; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.engine_signal_scores (
    id bigint NOT NULL,
    run_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) NOT NULL,
    trade_date date NOT NULL,
    symbol character varying(20) NOT NULL,
    model_version character varying(64) NOT NULL,
    feature_version character varying(64) NOT NULL,
    light_score double precision,
    tft_score double precision,
    fusion_score double precision NOT NULL,
    score_rank integer,
    universe_tag character varying(32),
    signal_side character varying(8),
    expected_price double precision,
    quality jsonb DEFAULT '{}'::jsonb NOT NULL,
    created_at timestamp(6) with time zone DEFAULT now() NOT NULL,
    risk_weight double precision DEFAULT 1.0,
    regime character varying(16) DEFAULT 'normal'::character varying,
    CONSTRAINT ck_engine_signal_scores_side CHECK (((signal_side IS NULL) OR ((signal_side)::text = ANY (ARRAY[('BUY'::character varying)::text, ('SELL'::character varying)::text, ('HOLD'::character varying)::text]))))
);
--
-- Name: engine_signal_scores_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.engine_signal_scores_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: engine_signal_scores_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.engine_signal_scores_id_seq OWNED BY public.engine_signal_scores.id;
--
-- Name: identity_verifications; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.identity_verifications (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    real_name character varying(128) NOT NULL,
    id_number character varying(128) NOT NULL,
    document_type character varying(32),
    front_image_url character varying(512),
    back_image_url character varying(512),
    handheld_image_url character varying(512),
    status character varying(32),
    rejection_reason text,
    submitted_at timestamp(6) with time zone DEFAULT now(),
    verified_at timestamp(6) with time zone,
    verified_by character varying(64)
);
--
-- Name: identity_verifications_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.identity_verifications_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;
--
-- Name: identity_verifications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.identity_verifications_id_seq OWNED BY public.identity_verifications.id;
--
-- Name: index_daily; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.index_daily (
    trade_date date NOT NULL,
    symbol character varying(32) NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    amount double precision,
    adj_factor double precision DEFAULT 1.0,
    created_at timestamp with time zone DEFAULT now()
);
--
-- Name: index_ohlcv_daily; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.index_ohlcv_daily (
    id bigint NOT NULL,
    trade_date date NOT NULL,
    symbol character varying(16) NOT NULL,
    index_name character varying(64),
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    amount double precision,
    pct_change double precision,
    source character varying(64),
    created_at timestamp without time zone DEFAULT now(),
    updated_at timestamp without time zone DEFAULT now()
);
--
-- Name: index_ohlcv_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.index_ohlcv_daily_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: index_ohlcv_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.index_ohlcv_daily_id_seq OWNED BY public.index_ohlcv_daily.id;
--
-- Name: klines; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.klines (
    id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    "interval" character varying(10) NOT NULL,
    "timestamp" timestamp without time zone NOT NULL,
    open_price double precision NOT NULL,
    high_price double precision NOT NULL,
    low_price double precision NOT NULL,
    close_price double precision NOT NULL,
    volume integer NOT NULL,
    amount double precision,
    change double precision,
    change_percent double precision,
    turnover_rate double precision,
    data_source character varying(20),
    created_at timestamp without time zone DEFAULT now()
);
--
-- Name: klines_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
ALTER TABLE public.klines ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.klines_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);
--
-- Name: login_devices; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.login_devices (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    device_id character varying(128) NOT NULL,
    device_name character varying(128),
    device_type character varying(32),
    os character varying(64),
    browser character varying(64),
    ip_address character varying(64),
    location character varying(128),
    is_trusted boolean,
    is_active boolean,
    first_seen_at timestamp with time zone DEFAULT now() NOT NULL,
    last_seen_at timestamp with time zone,
    last_location_change timestamp with time zone
);
--
-- Name: login_devices_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.login_devices_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: login_devices_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.login_devices_id_seq OWNED BY public.login_devices.id;
--
-- Name: market_daily_stats; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.market_daily_stats (
    trade_date date NOT NULL,
    sh_amount double precision,
    sz_amount double precision,
    total_amount double precision,
    created_at timestamp(6) without time zone DEFAULT now()
);
--
-- Name: market_data_daily; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.market_data_daily (
    trade_date date NOT NULL,
    symbol character varying(32) NOT NULL,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    factor double precision,
    mom_ret_1d double precision,
    mom_ret_3d double precision,
    mom_ret_5d double precision,
    mom_ret_10d double precision,
    mom_ret_20d double precision,
    mom_ret_60d double precision,
    mom_ret_120d double precision,
    mom_ma_gap_5 double precision,
    mom_ma_gap_10 double precision,
    mom_ma_gap_20 double precision,
    mom_ma_gap_60 double precision,
    mom_ma_gap_120 double precision,
    mom_ema_gap_12 double precision,
    mom_ema_gap_26 double precision,
    mom_macd_dif double precision,
    mom_macd_dea double precision,
    mom_macd_hist double precision,
    mom_rsi_6 double precision,
    mom_rsi_14 double precision,
    mom_kdj_k double precision,
    mom_kdj_d double precision,
    mom_kdj_j double precision,
    mom_roc_12 double precision,
    mom_breakout_20d double precision,
    vol_std_5 double precision,
    vol_std_10 double precision,
    vol_std_20 double precision,
    vol_std_60 double precision,
    vol_atr_14 double precision,
    vol_atr_20 double precision,
    vol_true_range double precision,
    vol_parkinson_10 double precision,
    vol_parkinson_20 double precision,
    vol_gk_10 double precision,
    vol_gk_20 double precision,
    vol_rs_10 double precision,
    vol_rs_20 double precision,
    vol_downside_20 double precision,
    vol_upside_20 double precision,
    vol_realized_rv double precision,
    vol_realized_rrv double precision,
    vol_realized_rskew double precision,
    vol_realized_rkurt double precision,
    vol_jump_zadj double precision,
    vol_jump_rjv_ratio double precision,
    vol_jump_sjv_ratio double precision,
    liq_turnover_os double precision,
    liq_turnover_tl double precision,
    liq_volume double precision,
    liq_volume_ma_5 double precision,
    liq_volume_ma_10 double precision,
    liq_volume_ma_20 double precision,
    liq_volume_ratio_5 double precision,
    liq_volume_ratio_20 double precision,
    liq_amount double precision,
    liq_amount_ma_5 double precision,
    liq_amount_ma_10 double precision,
    liq_amount_ma_20 double precision,
    liq_amount_ratio_5 double precision,
    liq_amount_ratio_20 double precision,
    liq_trade_count double precision,
    liq_avg_trade_size double precision,
    liq_obv_20 double precision,
    liq_obv_60 double precision,
    liq_mfi_14 double precision,
    liq_accdist_20 double precision,
    liq_amihud_20 double precision,
    liq_amihud_60 double precision,
    flow_net_amount double precision,
    flow_net_amount_ratio double precision,
    flow_large_net_amount double precision,
    flow_large_net_ratio double precision,
    flow_medium_net_amount double precision,
    flow_medium_net_ratio double precision,
    flow_small_net_amount double precision,
    flow_small_net_ratio double precision,
    flow_net_order_count double precision,
    flow_net_order_ratio double precision,
    flow_large_net_order double precision,
    flow_large_order_ratio double precision,
    flow_vpin double precision,
    flow_vpin_ma_5 double precision,
    flow_vpin_ma_20 double precision,
    flow_vpin_delta_5 double precision,
    flow_qsp double precision,
    flow_esp double precision,
    flow_aqsp double precision,
    flow_qsp_time double precision,
    flow_esp_time double precision,
    flow_pressure_index double precision,
    style_ln_mv_total double precision,
    style_ln_mv_float double precision,
    style_bp double precision,
    style_ep_ttm double precision,
    style_smb double precision,
    style_hml double precision,
    style_mkt_premium double precision,
    style_beta_20 double precision,
    style_beta_60 double precision,
    style_beta_120 double precision,
    style_idio_vol_20 double precision,
    style_idio_vol_60 double precision,
    style_residual_ret_20 double precision,
    style_valuation_composite double precision,
    style_size_percentile double precision,
    style_value_percentile double precision,
    ind_ret_1d double precision,
    ind_ret_5d double precision,
    ind_ret_10d double precision,
    ind_ret_20d double precision,
    ind_vol_20 double precision,
    ind_turnover_20 double precision,
    ind_amount_20 double precision,
    ind_strength_20 double precision,
    ind_strength_60 double precision,
    ind_dispersion_20 double precision,
    ind_up_breadth_20 double precision,
    ind_down_breadth_20 double precision,
    ind_relative_volume_20 double precision,
    ind_relative_volatility_20 double precision,
    ind_relative_flow_20 double precision,
    ind_momentum_rank_20 double precision,
    ind_value_rank double precision,
    ind_size_rank double precision,
    ind_code_l1 text,
    ind_code_l2 text,
    micro_qsp_equal double precision,
    micro_esp_equal double precision,
    micro_aqsp_equal double precision,
    micro_qsp_time double precision,
    micro_esp_time double precision,
    micro_qsp_volume double precision,
    micro_esp_volume double precision,
    micro_qsp_amount double precision,
    micro_esp_amount double precision,
    micro_effective_spread double precision,
    micro_quoted_spread double precision,
    micro_spread_vol_20 double precision,
    micro_imbalance_volume double precision,
    micro_imbalance_amount double precision,
    micro_imbalance_count double precision,
    micro_imbalance_large double precision,
    micro_imbalance_medium double precision,
    micro_imbalance_small double precision,
    micro_jump_flag double precision,
    micro_pressure_score double precision,
    label double precision,
    volume_trend_3d boolean
)
PARTITION BY RANGE (trade_date);
--
-- Name: notifications; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.notifications (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    notification_type character varying(50) NOT NULL,
    title character varying(200) NOT NULL,
    content text,
    data jsonb DEFAULT '{}'::jsonb,
    level character varying(16) DEFAULT 'info'::character varying,
    action_url character varying(512),
    is_read boolean DEFAULT false,
    read_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    expires_at timestamp with time zone
);
--
-- Name: notifications_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.notifications_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: notifications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.notifications_id_seq OWNED BY public.notifications.id;
--
-- Name: orders; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.orders (
    id integer NOT NULL,
    order_id uuid NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    portfolio_id integer NOT NULL,
    strategy_id integer,
    symbol character varying(20) NOT NULL,
    symbol_name character varying(50),
    side public.orderside NOT NULL,
    trade_action public.tradeaction,
    position_side public.positionside NOT NULL,
    is_margin_trade boolean NOT NULL,
    order_type public.ordertype NOT NULL,
    trading_mode public.tradingmode NOT NULL,
    status public.orderstatus NOT NULL,
    quantity double precision NOT NULL,
    filled_quantity double precision NOT NULL,
    price double precision,
    stop_price double precision,
    average_price double precision,
    order_value double precision NOT NULL,
    filled_value double precision NOT NULL,
    commission double precision NOT NULL,
    submitted_at timestamp without time zone,
    filled_at timestamp without time zone,
    cancelled_at timestamp without time zone,
    expired_at timestamp without time zone,
    client_order_id character varying(100),
    exchange_order_id character varying(100),
    remarks character varying(500),
    version integer NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: orders_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.orders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.orders_id_seq OWNED BY public.orders.id;
--
-- Name: password_reset_tokens; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.password_reset_tokens (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    email character varying(255) NOT NULL,
    token character varying(128) NOT NULL,
    is_used boolean,
    is_expired boolean,
    created_at timestamp with time zone DEFAULT now(),
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone,
    ip_address character varying(64),
    attempts integer
);
--
-- Name: password_reset_tokens_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.password_reset_tokens_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: password_reset_tokens_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.password_reset_tokens_id_seq OWNED BY public.password_reset_tokens.id;
--
-- Name: payment_methods; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.payment_methods (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    provider character varying(32) NOT NULL,
    provider_token character varying(255),
    last4 character varying(4),
    card_type character varying(32),
    expiry_month integer,
    expiry_year integer,
    is_default integer,
    created_at timestamp(6) with time zone DEFAULT now()
);
--
-- Name: payment_methods_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.payment_methods_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;
--
-- Name: payment_methods_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.payment_methods_id_seq OWNED BY public.payment_methods.id;
--
-- Name: payment_transactions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.payment_transactions (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    amount numeric(10,2) NOT NULL,
    currency character varying(3),
    status character varying(32),
    subscription_id integer,
    payment_method_id integer,
    provider character varying(32) NOT NULL,
    transaction_id character varying(128),
    description character varying(255),
    metadata_info json,
    created_at timestamp(6) with time zone DEFAULT now(),
    updated_at timestamp(6) with time zone,
    completed_at timestamp(6) with time zone
);
--
-- Name: payment_transactions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.payment_transactions_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;
--
-- Name: payment_transactions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.payment_transactions_id_seq OWNED BY public.payment_transactions.id;
--
-- Name: permissions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.permissions (
    id integer NOT NULL,
    name character varying(128) NOT NULL,
    code character varying(128) NOT NULL,
    resource character varying(64) NOT NULL,
    action character varying(32) NOT NULL,
    description text,
    is_active boolean,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone
);
--
-- Name: permissions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.permissions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: permissions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.permissions_id_seq OWNED BY public.permissions.id;
--
-- Name: phone_verifications; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.phone_verifications (
    id integer NOT NULL,
    phone_number character varying(32) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    verification_code character varying(32) NOT NULL,
    code_type character varying(32) NOT NULL,
    is_used boolean,
    is_expired boolean,
    created_at timestamp with time zone DEFAULT now(),
    expires_at timestamp with time zone NOT NULL,
    used_at timestamp with time zone,
    attempts integer,
    ip_address character varying(64)
);
--
-- Name: phone_verifications_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.phone_verifications_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: phone_verifications_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.phone_verifications_id_seq OWNED BY public.phone_verifications.id;
--
-- Name: pipeline_runs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.pipeline_runs (
    run_id text NOT NULL,
    user_id text NOT NULL,
    tenant_id text DEFAULT 'default'::text NOT NULL,
    status text NOT NULL,
    stage text NOT NULL,
    error_message text,
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    request_json jsonb,
    result_json jsonb
);
--
-- Name: portfolio_snapshots; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.portfolio_snapshots (
    id integer NOT NULL,
    portfolio_id integer NOT NULL,
    snapshot_date timestamp without time zone NOT NULL,
    total_value numeric(20,2) NOT NULL,
    available_cash numeric(20,2) NOT NULL,
    market_value numeric(20,2) NOT NULL,
    total_pnl numeric(20,2) NOT NULL,
    total_return numeric(10,4) NOT NULL,
    daily_pnl numeric(20,2) NOT NULL,
    daily_return numeric(10,4) NOT NULL,
    max_drawdown numeric(10,4) NOT NULL,
    sharpe_ratio numeric(10,4),
    volatility numeric(10,4),
    position_count integer NOT NULL,
    is_settlement boolean NOT NULL,
    created_at timestamp without time zone NOT NULL
);
--
-- Name: portfolio_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.portfolio_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: portfolio_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.portfolio_snapshots_id_seq OWNED BY public.portfolio_snapshots.id;
--
-- Name: portfolios; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.portfolios (
    id integer NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    name character varying(100) NOT NULL,
    description text,
    initial_capital numeric(20,2) NOT NULL,
    current_capital numeric(20,2) NOT NULL,
    available_cash numeric(20,2) NOT NULL,
    frozen_cash numeric(20,2) NOT NULL,
    total_value numeric(20,2) NOT NULL,
    total_pnl numeric(20,2) NOT NULL,
    total_return numeric(10,4) NOT NULL,
    daily_pnl numeric(20,2) NOT NULL,
    daily_return numeric(10,4) NOT NULL,
    yesterday_total_value numeric(20,2) NOT NULL,
    max_drawdown numeric(10,4) NOT NULL,
    sharpe_ratio numeric(10,4),
    volatility numeric(10,4),
    status character varying(20) NOT NULL,
    trading_mode public.tradingmode NOT NULL,
    broker_type character varying(32),
    broker_account_id character varying(64),
    broker_params json,
    strategy_id integer,
    real_trading_id character varying(50),
    run_status character varying(20) NOT NULL,
    is_deleted boolean NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    CONSTRAINT check_available_cash_positive CHECK ((available_cash >= (0)::numeric)),
    CONSTRAINT check_initial_capital_positive CHECK ((initial_capital >= (0)::numeric))
);
--
-- Name: portfolios_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.portfolios_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: portfolios_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.portfolios_id_seq OWNED BY public.portfolios.id;
--
-- Name: position_history; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.position_history (
    id integer NOT NULL,
    position_id integer NOT NULL,
    action character varying(20) NOT NULL,
    quantity_change integer NOT NULL,
    price numeric(20,4) NOT NULL,
    amount numeric(20,2) NOT NULL,
    quantity_after integer NOT NULL,
    avg_cost_after numeric(20,4) NOT NULL,
    note text,
    created_at timestamp without time zone NOT NULL
);
--
-- Name: position_history_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.position_history_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: position_history_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.position_history_id_seq OWNED BY public.position_history.id;
--
-- Name: positions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.positions (
    id integer NOT NULL,
    portfolio_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    symbol_name character varying(100),
    exchange character varying(20),
    side character varying(20) NOT NULL,
    quantity integer NOT NULL,
    available_quantity integer NOT NULL,
    frozen_quantity integer NOT NULL,
    avg_cost numeric(20,4) NOT NULL,
    total_cost numeric(20,2) NOT NULL,
    current_price numeric(20,4) NOT NULL,
    market_value numeric(20,2) NOT NULL,
    unrealized_pnl numeric(20,2) NOT NULL,
    unrealized_pnl_rate numeric(10,4) NOT NULL,
    realized_pnl numeric(20,2) NOT NULL,
    weight numeric(10,4) NOT NULL,
    status character varying(20) NOT NULL,
    opened_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL,
    closed_at timestamp without time zone,
    CONSTRAINT check_available_quantity_positive CHECK ((available_quantity >= 0)),
    CONSTRAINT check_quantity_positive CHECK ((quantity >= 0))
);
--
-- Name: positions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.positions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.positions_id_seq OWNED BY public.positions.id;
--
-- Name: qlib_backtest_runs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qlib_backtest_runs (
    id character varying(64),
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    strategy_id character varying(64),
    status character varying(32) DEFAULT 'pending'::character varying NOT NULL,
    config jsonb DEFAULT '{}'::jsonb NOT NULL,
    result jsonb,
    error_message text,
    task_id character varying(64),
    started_at timestamp with time zone,
    completed_at timestamp with time zone,
    execution_time_seconds double precision,
    created_at timestamp with time zone DEFAULT now(),
    result_file_path text,
    result_cos_key text,
    result_cos_url text,
    result_backup_status text DEFAULT 'none'::text NOT NULL,
    result_backup_at timestamp with time zone,
    config_json jsonb,
    result_json jsonb,
    backtest_id text
);
--
-- Name: qlib_backtest_runs_cleanup_backup; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qlib_backtest_runs_cleanup_backup (
    backtest_id text NOT NULL,
    user_id text NOT NULL,
    status text NOT NULL,
    created_at timestamp(6) with time zone NOT NULL,
    completed_at timestamp(6) with time zone,
    config_json jsonb,
    result_json jsonb,
    tenant_id text DEFAULT 'default'::text NOT NULL,
    task_id text,
    result_file_path text,
    result_cos_key text,
    result_cos_url text,
    result_backup_status text DEFAULT 'none'::text NOT NULL,
    result_backup_at timestamp(6) with time zone,
    backup_batch text,
    backup_reason text,
    backup_at timestamp(6) with time zone
);
--
-- Name: qlib_optimization_runs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qlib_optimization_runs (
    optimization_id text NOT NULL,
    task_id text,
    mode text NOT NULL,
    user_id text NOT NULL,
    tenant_id text DEFAULT 'default'::text NOT NULL,
    status text NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    completed_at timestamp with time zone,
    base_request_json jsonb,
    config_snapshot_json jsonb,
    optimization_target text,
    param_ranges_json jsonb,
    total_tasks integer DEFAULT 0 NOT NULL,
    completed_count integer DEFAULT 0 NOT NULL,
    failed_count integer DEFAULT 0 NOT NULL,
    current_params_json jsonb,
    best_params_json jsonb,
    best_metric_value double precision,
    result_summary_json jsonb,
    all_results_json jsonb,
    error_message text
);
--
-- Name: qm_market_calendar_day; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_market_calendar_day (
    market character varying(32) NOT NULL,
    trade_date date NOT NULL,
    is_trading_day boolean NOT NULL,
    timezone character varying(64) DEFAULT 'Asia/Shanghai'::character varying NOT NULL,
    source character varying(64) DEFAULT 'manual'::character varying NOT NULL,
    version character varying(64),
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) DEFAULT '*'::character varying NOT NULL,
    metadata_json jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_market_calendar_exception; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_market_calendar_exception (
    id bigint NOT NULL,
    market character varying(32) NOT NULL,
    trade_date date NOT NULL,
    action character varying(16) NOT NULL,
    reason text,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) DEFAULT '*'::character varying NOT NULL,
    approved_by character varying(128),
    metadata_json jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_market_calendar_exception_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.qm_market_calendar_exception_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: qm_market_calendar_exception_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.qm_market_calendar_exception_id_seq OWNED BY public.qm_market_calendar_exception.id;
--
-- Name: qm_market_calendar_version; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_market_calendar_version (
    market character varying(32) NOT NULL,
    year integer NOT NULL,
    checksum character varying(128) NOT NULL,
    status character varying(32) DEFAULT 'draft'::character varying NOT NULL,
    source character varying(64),
    published_at timestamp with time zone,
    metadata_json jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_market_trading_session; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_market_trading_session (
    market character varying(32) NOT NULL,
    session_name character varying(64) NOT NULL,
    start_time time without time zone NOT NULL,
    end_time time without time zone NOT NULL,
    cross_day boolean DEFAULT false NOT NULL,
    trade_date_rule character varying(64) DEFAULT 'TRADE_DATE'::character varying NOT NULL,
    timezone character varying(64) DEFAULT 'Asia/Shanghai'::character varying NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    user_id character varying(64) DEFAULT '*'::character varying NOT NULL,
    metadata_json jsonb,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_model_inference_runs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_model_inference_runs (
    run_id text NOT NULL,
    tenant_id text DEFAULT 'default'::text NOT NULL,
    user_id text NOT NULL,
    model_id text NOT NULL,
    data_trade_date date NOT NULL,
    prediction_trade_date date NOT NULL,
    status text NOT NULL,
    signals_count integer DEFAULT 0 NOT NULL,
    duration_ms integer,
    fallback_used boolean DEFAULT false NOT NULL,
    fallback_reason text,
    failure_stage text,
    error_message text,
    stdout text,
    stderr text,
    active_model_id text,
    effective_model_id text,
    model_source text,
    active_data_source text,
    request_json jsonb,
    result_json jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
--
-- Name: qm_model_inference_settings; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_model_inference_settings (
    tenant_id text DEFAULT 'default'::text NOT NULL,
    user_id text NOT NULL,
    model_id text NOT NULL,
    enabled boolean DEFAULT false NOT NULL,
    schedule_time text DEFAULT '09:30'::text NOT NULL,
    last_run_id text,
    last_run_json jsonb,
    next_run_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_research_candidate_snapshot; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_research_candidate_snapshot (
    tenant_id text DEFAULT 'default'::text NOT NULL,
    user_id text NOT NULL,
    run_id text NOT NULL,
    model_id text NOT NULL,
    data_trade_date date,
    prediction_trade_date date NOT NULL,
    symbol text NOT NULL,
    fusion_score double precision,
    light_score double precision,
    tft_score double precision,
    score_rank integer,
    signal_side text,
    expected_price double precision,
    universe_tag text,
    quality jsonb DEFAULT '{}'::jsonb NOT NULL,
    confidence_level text,
    confidence_score double precision,
    hit_reasons jsonb DEFAULT '[]'::jsonb NOT NULL,
    risk_flags jsonb DEFAULT '[]'::jsonb NOT NULL,
    thesis_summary text,
    source_updated_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_research_import_state; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_research_import_state (
    job_name text NOT NULL,
    last_source_updated_at timestamp with time zone DEFAULT '1970-01-01 00:00:00+00'::timestamp with time zone NOT NULL,
    last_prediction_trade_date date,
    last_run_id text,
    extra_json jsonb DEFAULT '{}'::jsonb NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_strategy_model_bindings; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_strategy_model_bindings (
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    strategy_id character varying(128) NOT NULL,
    model_id character varying(128) NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: qm_user_models; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_user_models (
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    model_id character varying(128) NOT NULL,
    source_run_id character varying(64),
    status character varying(32) DEFAULT 'candidate'::character varying NOT NULL,
    storage_path text,
    model_file character varying(255),
    metadata_json jsonb,
    metrics_json jsonb,
    is_default boolean DEFAULT false NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    activated_at timestamp with time zone,
    archived_at timestamp with time zone
);
--
-- Name: qm_user_research_pool; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_user_research_pool (
    tenant_id text DEFAULT 'default'::text NOT NULL,
    user_id text NOT NULL,
    symbol text NOT NULL,
    stock_name text,
    added_at timestamp with time zone DEFAULT now() NOT NULL,
    source_run_id text,
    model_id text,
    fusion_score double precision,
    thesis_summary text,
    status text DEFAULT 'pending'::text NOT NULL,
    notes text,
    tags jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    features_snapshot jsonb DEFAULT '{}'::jsonb,
    CONSTRAINT chk_pool_status_valid CHECK ((status = ANY (ARRAY['pending'::text, 'confirmed'::text, 'rejected'::text])))
);
--
-- Name: qm_user_watchlist; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qm_user_watchlist (
    tenant_id text DEFAULT 'default'::text NOT NULL,
    user_id text NOT NULL,
    symbol text NOT NULL,
    stock_name text,
    added_at timestamp with time zone DEFAULT now() NOT NULL,
    source_run_id text,
    notes text,
    tags jsonb DEFAULT '[]'::jsonb NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL,
    features_snapshot jsonb DEFAULT '{}'::jsonb
);
--
-- Name: qmt_agent_bindings; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qmt_agent_bindings (
    id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    api_key_id integer NOT NULL,
    agent_type character varying(32) NOT NULL,
    account_id character varying(64) NOT NULL,
    client_fingerprint character varying(255) NOT NULL,
    hostname character varying(255),
    client_version character varying(64),
    status character varying(32) NOT NULL,
    last_ip character varying(64),
    last_seen_at timestamp with time zone,
    bound_at timestamp with time zone NOT NULL,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
--
-- Name: qmt_agent_sessions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.qmt_agent_sessions (
    id character varying(64) NOT NULL,
    binding_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    token_hash character varying(64) NOT NULL,
    expires_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone,
    last_used_at timestamp with time zone,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL
);
--
-- Name: quote_daily_summaries; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.quote_daily_summaries (
    id integer NOT NULL,
    trade_date date NOT NULL,
    symbol character varying(20) NOT NULL,
    data_source character varying(20) DEFAULT 'remote_redis'::character varying NOT NULL,
    open_price double precision,
    high_price double precision,
    low_price double precision,
    close_price double precision,
    avg_price double precision,
    volume_sum bigint DEFAULT 0,
    amount_sum double precision DEFAULT 0,
    quote_count integer DEFAULT 0,
    first_quote_at timestamp with time zone,
    last_quote_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
--
-- Name: quote_daily_summaries_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
ALTER TABLE public.quote_daily_summaries ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.quote_daily_summaries_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);
--
-- Name: quotes; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.quotes (
    id integer NOT NULL,
    symbol text NOT NULL,
    trade_time timestamp with time zone,
    price numeric(12,4),
    volume bigint,
    bid_price numeric(12,4),
    ask_price numeric(12,4),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    "timestamp" timestamp with time zone DEFAULT now(),
    current_price double precision,
    open_price double precision,
    high_price double precision,
    low_price double precision,
    close_price double precision,
    amount double precision,
    change double precision,
    change_percent double precision,
    data_source character varying(20),
    pre_close double precision,
    bid1_price double precision,
    bid1_volume integer,
    bid2_price double precision,
    bid2_volume integer,
    bid3_price double precision,
    bid3_volume integer,
    bid4_price double precision,
    bid4_volume integer,
    bid5_price double precision,
    bid5_volume integer,
    ask1_price double precision,
    ask1_volume integer,
    ask2_price double precision,
    ask2_volume integer,
    ask3_price double precision,
    ask3_volume integer,
    ask4_price double precision,
    ask4_volume integer,
    ask5_price double precision,
    ask5_volume integer
);
--
-- Name: quotes_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
ALTER TABLE public.quotes ALTER COLUMN id ADD GENERATED BY DEFAULT AS IDENTITY (
    SEQUENCE NAME public.quotes_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1
);
--
-- Name: real_account_baselines; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.real_account_baselines (
    id bigint NOT NULL,
    tenant_id character varying(50) NOT NULL,
    user_id character varying(50) NOT NULL,
    account_id character varying(64) NOT NULL,
    initial_equity double precision NOT NULL,
    first_snapshot_at timestamp without time zone,
    source character varying(32) DEFAULT 'qmt_bridge_first_report'::character varying NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL
);
--
-- Name: real_account_baselines_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.real_account_baselines_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: real_account_baselines_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.real_account_baselines_id_seq OWNED BY public.real_account_baselines.id;
--
-- Name: real_account_ledger_daily_snapshots; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.real_account_ledger_daily_snapshots (
    id integer NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    account_id character varying(64) NOT NULL,
    snapshot_date date NOT NULL,
    last_snapshot_at timestamp without time zone NOT NULL,
    initial_equity double precision NOT NULL,
    day_open_equity double precision NOT NULL,
    month_open_equity double precision NOT NULL,
    total_asset double precision NOT NULL,
    cash double precision NOT NULL,
    market_value double precision NOT NULL,
    today_pnl_raw double precision NOT NULL,
    monthly_pnl_raw double precision NOT NULL,
    total_pnl_raw double precision NOT NULL,
    floating_pnl_raw double precision NOT NULL,
    daily_return_pct double precision NOT NULL,
    total_return_pct double precision NOT NULL,
    position_count integer NOT NULL,
    source character varying(32) NOT NULL,
    payload_json json NOT NULL
);
--
-- Name: real_account_ledger_daily_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.real_account_ledger_daily_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: real_account_ledger_daily_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.real_account_ledger_daily_snapshots_id_seq OWNED BY public.real_account_ledger_daily_snapshots.id;
--
-- Name: real_account_snapshots; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.real_account_snapshots (
    id integer NOT NULL,
    tenant_id character varying(50) NOT NULL,
    user_id character varying(50) NOT NULL,
    account_id character varying(64) NOT NULL,
    snapshot_at timestamp without time zone NOT NULL,
    snapshot_date date NOT NULL,
    snapshot_month character varying(7) NOT NULL,
    total_asset double precision NOT NULL,
    cash double precision NOT NULL,
    market_value double precision NOT NULL,
    today_pnl_raw double precision NOT NULL,
    total_pnl_raw double precision NOT NULL,
    floating_pnl_raw double precision NOT NULL,
    source character varying(32) NOT NULL,
    payload_json json NOT NULL
);
--
-- Name: real_account_snapshot_overview_v; Type: VIEW; Schema: public; Owner: -
--
CREATE VIEW public.real_account_snapshot_overview_v AS
 SELECT real_account_snapshots.id,
    real_account_snapshots.tenant_id,
    real_account_snapshots.user_id,
    real_account_snapshots.account_id,
    real_account_snapshots.snapshot_at,
    real_account_snapshots.snapshot_date,
    real_account_snapshots.snapshot_month,
    real_account_snapshots.total_asset,
    real_account_snapshots.cash,
    real_account_snapshots.market_value,
    real_account_snapshots.today_pnl_raw,
    real_account_snapshots.total_pnl_raw,
    real_account_snapshots.floating_pnl_raw,
    real_account_snapshots.source,
    real_account_snapshots.payload_json,
    COALESCE(( SELECT ras.total_asset
           FROM public.real_account_snapshots ras
          WHERE (((ras.tenant_id)::text = (real_account_snapshots.tenant_id)::text) AND ((ras.user_id)::text = (real_account_snapshots.user_id)::text) AND ((ras.account_id)::text = (real_account_snapshots.account_id)::text))
          ORDER BY ras.snapshot_at
         LIMIT 1), real_account_snapshots.total_asset) AS initial_equity,
    COALESCE(( SELECT ras.total_asset
           FROM public.real_account_snapshots ras
          WHERE (((ras.tenant_id)::text = (real_account_snapshots.tenant_id)::text) AND ((ras.user_id)::text = (real_account_snapshots.user_id)::text) AND ((ras.account_id)::text = (real_account_snapshots.account_id)::text) AND (ras.snapshot_date = real_account_snapshots.snapshot_date))
          ORDER BY ras.snapshot_at
         LIMIT 1), real_account_snapshots.total_asset) AS day_open_equity,
    COALESCE(( SELECT ras.total_asset
           FROM public.real_account_snapshots ras
          WHERE (((ras.tenant_id)::text = (real_account_snapshots.tenant_id)::text) AND ((ras.user_id)::text = (real_account_snapshots.user_id)::text) AND ((ras.account_id)::text = (real_account_snapshots.account_id)::text) AND ((ras.snapshot_month)::text = (real_account_snapshots.snapshot_month)::text))
          ORDER BY ras.snapshot_at
         LIMIT 1), real_account_snapshots.total_asset) AS month_open_equity
   FROM public.real_account_snapshots;
--
-- Name: real_account_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.real_account_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: real_account_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.real_account_snapshots_id_seq OWNED BY public.real_account_snapshots.id;
--
-- Name: real_trading_preflight_snapshots; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.real_trading_preflight_snapshots (
    id integer NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    trading_mode character varying(16) NOT NULL,
    snapshot_date date NOT NULL,
    ready boolean NOT NULL,
    total_checks integer NOT NULL,
    passed_checks integer NOT NULL,
    required_failed_count integer NOT NULL,
    run_count integer NOT NULL,
    failed_required_keys json NOT NULL,
    checks json NOT NULL,
    source character varying(32) NOT NULL,
    last_checked_at timestamp without time zone NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: real_trading_preflight_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.real_trading_preflight_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: real_trading_preflight_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.real_trading_preflight_snapshots_id_seq OWNED BY public.real_trading_preflight_snapshots.id;
--
-- Name: risk_rules; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.risk_rules (
    id integer NOT NULL,
    rule_name character varying(100) NOT NULL,
    rule_type character varying(50) NOT NULL,
    description character varying(500),
    is_active boolean NOT NULL,
    parameters json NOT NULL,
    applies_to_all boolean NOT NULL,
    user_ids json,
    priority integer NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: risk_rules_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.risk_rules_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: risk_rules_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.risk_rules_id_seq OWNED BY public.risk_rules.id;
--
-- Name: role_permissions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.role_permissions (
    role_id integer NOT NULL,
    permission_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);
--
-- Name: roles; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.roles (
    id integer NOT NULL,
    name character varying(64) NOT NULL,
    code character varying(64) NOT NULL,
    description text,
    is_active boolean,
    is_system boolean,
    priority integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone
);
--
-- Name: roles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.roles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: roles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.roles_id_seq OWNED BY public.roles.id;
--
-- Name: sim_orders; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.sim_orders (
    id character varying(64) NOT NULL,
    job_id character varying(64) NOT NULL,
    user_id bigint NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    symbol character varying(20) NOT NULL,
    side public.orderside NOT NULL,
    order_type public.ordertype DEFAULT 'market'::public.ordertype NOT NULL,
    quantity numeric(18,4) NOT NULL,
    price numeric(18,4),
    status public.orderstatus DEFAULT 'pending'::public.orderstatus NOT NULL,
    filled_quantity numeric(18,4) DEFAULT 0,
    filled_price numeric(18,4),
    commission numeric(18,4) DEFAULT 0,
    signal_time timestamp with time zone,
    submit_time timestamp with time zone,
    fill_time timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    order_id uuid DEFAULT gen_random_uuid(),
    portfolio_id integer DEFAULT 0,
    strategy_id integer,
    trading_mode character varying(32) DEFAULT 'simulation'::character varying,
    average_price numeric(18,4),
    order_value numeric(18,4) DEFAULT 0,
    filled_value numeric(18,4) DEFAULT 0,
    submitted_at timestamp with time zone,
    filled_at timestamp with time zone,
    cancelled_at timestamp with time zone,
    execution_model character varying(32) DEFAULT 'synthetic_price'::character varying,
    price_source character varying(64),
    remarks character varying(500),
    version integer DEFAULT 1,
    total_fee numeric(18,4) DEFAULT 0,
    updated_at timestamp with time zone,
    trade_action character varying(32),
    position_side character varying(16) DEFAULT 'long'::character varying,
    is_margin_trade integer DEFAULT 0 NOT NULL
);
--
-- Name: sim_trades; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.sim_trades (
    id character varying(64) NOT NULL,
    job_id character varying(64) NOT NULL,
    order_id character varying(64) NOT NULL,
    user_id bigint NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    symbol character varying(20) NOT NULL,
    side public.orderside NOT NULL,
    quantity numeric(18,4) NOT NULL,
    price numeric(18,4) NOT NULL,
    commission numeric(18,4) DEFAULT 0,
    trade_time timestamp with time zone NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    trade_id uuid,
    portfolio_id integer DEFAULT 0 NOT NULL,
    trading_mode character varying(20) DEFAULT 'SIMULATION'::character varying NOT NULL,
    trade_value numeric(18,4),
    executed_at timestamp with time zone,
    price_source character varying(50),
    updated_at timestamp with time zone DEFAULT now(),
    stamp_duty numeric(18,4) DEFAULT 0,
    transfer_fee numeric(18,4) DEFAULT 0,
    total_fee numeric(18,4) DEFAULT 0,
    trade_action character varying(32),
    position_side character varying(16) DEFAULT 'long'::character varying,
    is_margin_trade integer DEFAULT 0 NOT NULL
);
--
-- Name: simulation_account_daily; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_account_daily (
    id integer NOT NULL,
    account_id character varying(96) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    snapshot_date date NOT NULL,
    snapshot_at timestamp without time zone NOT NULL,
    cash double precision NOT NULL,
    available_cash double precision NOT NULL,
    frozen_cash double precision NOT NULL,
    long_market_value double precision NOT NULL,
    short_market_value double precision NOT NULL,
    total_asset double precision NOT NULL,
    liabilities double precision NOT NULL,
    equity double precision NOT NULL,
    daily_pnl double precision NOT NULL,
    total_pnl double precision NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_account_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_account_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_account_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_account_daily_id_seq OWNED BY public.simulation_account_daily.id;
--
-- Name: simulation_accounts; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_accounts (
    account_id character varying(96) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    base_currency character varying(16) NOT NULL,
    account_type character varying(32) NOT NULL,
    status character varying(32) NOT NULL,
    initial_equity double precision NOT NULL,
    cash double precision NOT NULL,
    available_cash double precision NOT NULL,
    frozen_cash double precision NOT NULL,
    long_market_value double precision NOT NULL,
    short_market_value double precision NOT NULL,
    total_asset double precision NOT NULL,
    liabilities double precision NOT NULL,
    equity double precision NOT NULL,
    maintenance_margin_ratio double precision NOT NULL,
    last_trade_at timestamp without time zone,
    last_projected_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_cash_ledger; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_cash_ledger (
    id integer NOT NULL,
    account_id character varying(96) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    event_type character varying(64) NOT NULL,
    ref_type character varying(32) NOT NULL,
    ref_id character varying(96),
    amount double precision NOT NULL,
    balance_after double precision,
    trade_date timestamp without time zone,
    occurred_at timestamp without time zone NOT NULL,
    currency character varying(16) NOT NULL,
    note character varying(255),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_cash_ledger_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_cash_ledger_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_cash_ledger_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_cash_ledger_id_seq OWNED BY public.simulation_cash_ledger.id;
--
-- Name: simulation_corporate_actions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_corporate_actions (
    id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    action_type character varying(32) NOT NULL,
    ex_date timestamp without time zone,
    effective_date timestamp without time zone,
    cash_dividend_per_share double precision NOT NULL,
    share_ratio double precision NOT NULL,
    rights_price double precision NOT NULL,
    source character varying(64) NOT NULL,
    note character varying(255),
    status character varying(32) NOT NULL,
    applied_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_corporate_actions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_corporate_actions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_corporate_actions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_corporate_actions_id_seq OWNED BY public.simulation_corporate_actions.id;
--
-- Name: simulation_daily_reports; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_daily_reports (
    id integer NOT NULL,
    job_id uuid NOT NULL,
    date date NOT NULL,
    total_asset double precision NOT NULL,
    return_rate double precision NOT NULL,
    holdings_snapshot json,
    created_at timestamp(6) without time zone
);
--
-- Name: simulation_daily_reports_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_daily_reports_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    MAXVALUE 2147483647
    CACHE 1;
--
-- Name: simulation_daily_reports_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_daily_reports_id_seq OWNED BY public.simulation_daily_reports.id;
--
-- Name: simulation_fills; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_fills (
    id integer NOT NULL,
    fill_id uuid NOT NULL,
    order_id uuid NOT NULL,
    legacy_trade_id integer,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(32) NOT NULL,
    account_id character varying(128) NOT NULL,
    strategy_id character varying(64),
    portfolio_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    side character varying(16) NOT NULL,
    position_side character varying(16) NOT NULL,
    trade_action character varying(32),
    fill_price double precision NOT NULL,
    fill_quantity double precision NOT NULL,
    gross_amount double precision NOT NULL,
    commission double precision NOT NULL,
    stamp_duty double precision NOT NULL,
    transfer_fee double precision NOT NULL,
    borrow_fee double precision NOT NULL,
    executed_at timestamp without time zone NOT NULL,
    price_source character varying(64),
    session_phase character varying(32),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_fills_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_fills_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_fills_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_fills_id_seq OWNED BY public.simulation_fills.id;
--
-- Name: simulation_fund_snapshots; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_fund_snapshots (
    id integer NOT NULL,
    tenant_id character varying(50) NOT NULL,
    user_id character varying(50) NOT NULL,
    snapshot_date date NOT NULL,
    total_asset double precision DEFAULT 0.0 NOT NULL,
    available_balance double precision DEFAULT 0.0 NOT NULL,
    frozen_balance double precision DEFAULT 0.0 NOT NULL,
    market_value double precision DEFAULT 0.0 NOT NULL,
    initial_capital double precision DEFAULT 0.0 NOT NULL,
    total_pnl double precision DEFAULT 0.0 NOT NULL,
    today_pnl double precision DEFAULT 0.0 NOT NULL,
    source character varying(64) DEFAULT 'redis_simulation_account'::character varying NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    created_at timestamp without time zone DEFAULT now() NOT NULL,
    account_id character varying(64),
    data jsonb
);
--
-- Name: simulation_fund_snapshots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_fund_snapshots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_fund_snapshots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_fund_snapshots_id_seq OWNED BY public.simulation_fund_snapshots.id;
--
-- Name: simulation_jobs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_jobs (
    id uuid NOT NULL,
    tenant_id character varying NOT NULL,
    user_id integer NOT NULL,
    model_id character varying NOT NULL,
    strategy_config json,
    initial_capital double precision NOT NULL,
    current_cash double precision NOT NULL,
    start_date date NOT NULL,
    last_run_date date,
    status public.simulationstatus,
    execution_logs json,
    created_at timestamp(6) without time zone,
    updated_at timestamp(6) without time zone
);
--
-- Name: simulation_orders; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_orders (
    id integer NOT NULL,
    order_id uuid NOT NULL,
    client_order_id character varying(64),
    tenant_id character varying(64) NOT NULL,
    user_id character varying(32) NOT NULL,
    strategy_id character varying(64),
    account_id character varying(128) NOT NULL,
    portfolio_id integer NOT NULL,
    legacy_order_id integer,
    symbol character varying(20) NOT NULL,
    side character varying(16) NOT NULL,
    position_side character varying(16) NOT NULL,
    trade_action character varying(32),
    order_type character varying(16) NOT NULL,
    time_in_force character varying(16) NOT NULL,
    quantity double precision NOT NULL,
    price double precision,
    trigger_source character varying(32) NOT NULL,
    status character varying(32) NOT NULL,
    rejected_reason character varying(500),
    trading_session_date date,
    submitted_at timestamp without time zone,
    expires_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_orders_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_orders_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_orders_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_orders_id_seq OWNED BY public.simulation_orders.id;
--
-- Name: simulation_position_daily; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_position_daily (
    id integer NOT NULL,
    account_id character varying(96) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    snapshot_date date NOT NULL,
    snapshot_at timestamp without time zone NOT NULL,
    symbol character varying(20) NOT NULL,
    position_side character varying(16) NOT NULL,
    quantity double precision NOT NULL,
    available_quantity double precision NOT NULL,
    frozen_quantity double precision NOT NULL,
    cost_price double precision NOT NULL,
    close_price double precision NOT NULL,
    market_value double precision NOT NULL,
    unrealized_pnl double precision NOT NULL,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_position_daily_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_position_daily_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_position_daily_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_position_daily_id_seq OWNED BY public.simulation_position_daily.id;
--
-- Name: simulation_position_lots; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_position_lots (
    id integer NOT NULL,
    account_id character varying(96) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    symbol character varying(20) NOT NULL,
    position_side character varying(16) NOT NULL,
    open_fill_id character varying(96),
    open_date timestamp without time zone,
    quantity_open double precision NOT NULL,
    quantity_remaining double precision NOT NULL,
    cost_price double precision NOT NULL,
    cost_amount double precision NOT NULL,
    status character varying(32) NOT NULL,
    closed_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_position_lots_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_position_lots_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_position_lots_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_position_lots_id_seq OWNED BY public.simulation_position_lots.id;
--
-- Name: simulation_positions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_positions (
    id integer NOT NULL,
    job_id character varying(64) NOT NULL,
    symbol character varying(20) NOT NULL,
    side public.positionside NOT NULL,
    quantity numeric(18,4) DEFAULT 0 NOT NULL,
    avg_cost numeric(18,4),
    market_value numeric(18,4),
    unrealized_pnl numeric(18,4),
    updated_at timestamp with time zone DEFAULT now()
);
--
-- Name: simulation_positions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_positions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_positions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_positions_id_seq OWNED BY public.simulation_positions.id;
--
-- Name: simulation_rebalance_jobs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.simulation_rebalance_jobs (
    id integer NOT NULL,
    job_id character varying(96) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    strategy_id character varying(96) NOT NULL,
    job_type character varying(32) NOT NULL,
    schedule_type character varying(32) NOT NULL,
    planned_run_at timestamp without time zone,
    window_start_at timestamp without time zone,
    window_end_at timestamp without time zone,
    status character varying(32) NOT NULL,
    attempt_count integer NOT NULL,
    last_error character varying(500),
    idempotency_key character varying(128),
    started_at timestamp without time zone,
    finished_at timestamp without time zone,
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: simulation_rebalance_jobs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.simulation_rebalance_jobs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: simulation_rebalance_jobs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.simulation_rebalance_jobs_id_seq OWNED BY public.simulation_rebalance_jobs.id;
--
-- Name: stock_daily_latest; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.stock_daily_latest (
    trade_date date NOT NULL,
    symbol character varying(32) NOT NULL,
    stock_name text,
    listed_days double precision,
    is_st double precision,
    listing_market character varying(16),
    industry text,
    province text,
    open double precision,
    high double precision,
    low double precision,
    close double precision,
    volume double precision,
    amount double precision,
    pct_change double precision,
    turnover_rate double precision,
    adj_factor double precision,
    pe_ttm double precision,
    pb double precision,
    total_mv double precision,
    float_mv double precision,
    bp double precision,
    ep_ttm double precision,
    ln_mv_total double precision,
    roe double precision,
    return_1d double precision,
    return_3d double precision,
    return_5d double precision,
    return_10d double precision,
    return_20d double precision,
    return_60d double precision,
    ma5 double precision,
    ma10 double precision,
    ma20 double precision,
    ma60 double precision,
    ma_gap_5 double precision,
    ma_gap_10 double precision,
    ma_gap_20 double precision,
    rsi_6 double precision,
    rsi_14 double precision,
    kdj_k double precision,
    kdj_d double precision,
    kdj_j double precision,
    macd_dif double precision,
    macd_dea double precision,
    macd_hist double precision,
    beta_20 double precision,
    vol_std_5 double precision,
    vol_std_20 double precision,
    vol_std_60 double precision,
    vol_atr_14 double precision,
    volume_ratio_5 double precision,
    volume_ratio_20 double precision,
    volume_ma_3 double precision,
    amount_ma_5 double precision,
    volume_trend_3d double precision,
    ind_code_l1 text,
    ind_code_l2 text,
    label double precision,
    concept_ai double precision,
    concept_chip double precision,
    concept_new_energy double precision,
    concept_pv double precision,
    concept_military double precision,
    concept_medical double precision,
    concept_fintech double precision,
    concept_consumption double precision,
    concept_state_owned double precision,
    main_flow double precision,
    inst_ownership double precision,
    lrg_trd_tolbuynum double precision,
    lrg_trd_tolsellnum double precision,
    flow_net_amount double precision,
    b_volume double precision,
    s_volume double precision,
    idx_all double precision,
    idx_hs300 double precision,
    idx_zz1000 double precision,
    idx_margin double precision,
    idx_chinext double precision,
    micro_effective_spread double precision,
    micro_imbalance_volume double precision,
    micro_jump_flag double precision,
    consecutive_limit_up_days double precision,
    limit_up_today double precision,
    limit_down_today double precision,
    profit_growth double precision,
    concept_lithium double precision,
    raw_open double precision,
    raw_high double precision,
    raw_low double precision,
    raw_close double precision,
    raw_volume double precision,
    raw_amount double precision,
    idx_zz500 double precision
);
--
-- Name: stock_pool_files; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.stock_pool_files (
    id integer NOT NULL,
    tenant_id character varying(50) DEFAULT 'default'::character varying,
    user_id character varying(50) NOT NULL,
    pool_name character varying(200),
    session_id character varying(100),
    file_key character varying(500) NOT NULL,
    file_url character varying(1000),
    relative_path character varying(500),
    format character varying(10) DEFAULT 'csv'::character varying,
    file_size integer,
    code_hash character varying(64),
    stock_count integer,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
--
-- Name: stock_pool_files_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.stock_pool_files_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: stock_pool_files_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.stock_pool_files_id_seq OWNED BY public.stock_pool_files.id;
--
-- Name: stock_tag; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.stock_tag (
    id bigint NOT NULL,
    symbol character varying(16) NOT NULL,
    tag_code character varying(64) NOT NULL,
    source character varying(64),
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: stock_tag_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.stock_tag_id_seq
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: stock_tag_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.stock_tag_id_seq OWNED BY public.stock_tag.id;
--
-- Name: stocks; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.stocks (
    id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    name character varying(100),
    exchange character varying(20),
    industry character varying(50),
    sector character varying(50),
    list_date date,
    is_active boolean DEFAULT true,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now()
);
--
-- Name: stocks_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.stocks_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: stocks_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.stocks_id_seq OWNED BY public.stocks.id;
--
-- Name: strategies; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.strategies (
    id integer NOT NULL,
    user_id integer NOT NULL,
    name character varying(200) NOT NULL,
    description text,
    strategy_type public.strategytype NOT NULL,
    status public.strategystatus NOT NULL,
    config json NOT NULL,
    parameters json NOT NULL,
    code text,
    cos_url character varying(500),
    code_hash character varying(64),
    file_size integer,
    validated_backtest_id integer,
    promoted_at timestamp(6) without time zone,
    live_trading_started_at timestamp(6) without time zone,
    is_public boolean NOT NULL,
    shared_users json NOT NULL,
    backtest_count integer NOT NULL,
    view_count integer NOT NULL,
    like_count integer NOT NULL,
    created_at timestamp(6) without time zone NOT NULL,
    updated_at timestamp(6) without time zone NOT NULL,
    version integer DEFAULT 1 NOT NULL,
    parent_id integer,
    change_log text,
    cos_key text,
    is_verified boolean DEFAULT false NOT NULL,
    execution_config jsonb DEFAULT '{}'::jsonb NOT NULL,
    tags text[]
);
--
-- Name: strategies_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.strategies_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: strategies_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.strategies_id_seq OWNED BY public.strategies.id;
--
-- Name: strategy_loop_tasks; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.strategy_loop_tasks (
    task_id text NOT NULL,
    user_id text NOT NULL,
    tenant_id text DEFAULT 'default'::text NOT NULL,
    status text NOT NULL,
    error_message text,
    created_at timestamp(6) with time zone NOT NULL,
    updated_at timestamp(6) with time zone NOT NULL,
    request_json jsonb,
    result_json jsonb
);
--
-- Name: subscription_plans; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.subscription_plans (
    id integer NOT NULL,
    name character varying(100) NOT NULL,
    code character varying(50) NOT NULL,
    description character varying(255),
    price numeric(10,2) NOT NULL,
    currency character varying(3),
    "interval" character varying(20),
    features json,
    is_active boolean,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone
);
--
-- Name: subscription_plans_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.subscription_plans_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: subscription_plans_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.subscription_plans_id_seq OWNED BY public.subscription_plans.id;
--
-- Name: system_settings; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.system_settings (
    key character varying(100) NOT NULL,
    value jsonb NOT NULL,
    description text,
    updated_at timestamp with time zone DEFAULT now()
);
--
-- Name: system_tasks; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.system_tasks (
    task_id character varying(64) NOT NULL,
    task_type character varying(32) NOT NULL,
    status character varying(20) DEFAULT 'PENDING'::character varying,
    progress integer DEFAULT 0,
    logs text,
    result_path text,
    error_message text,
    created_at timestamp(6) with time zone DEFAULT now(),
    finished_at timestamp(6) with time zone
);
--
-- Name: tag_dictionary; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.tag_dictionary (
    tag_code character varying(64) NOT NULL,
    tag_name character varying(128) NOT NULL,
    tag_category character varying(32) NOT NULL,
    source character varying(64),
    is_active boolean DEFAULT true NOT NULL,
    sort_order integer DEFAULT 0 NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    updated_at timestamp with time zone DEFAULT now() NOT NULL
);
--
-- Name: tmp_feature_update; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.tmp_feature_update (
    symbol text,
    trade_date date,
    return_1d double precision,
    return_3d double precision,
    return_5d double precision,
    return_10d double precision,
    return_20d double precision,
    return_60d double precision,
    return_120d double precision,
    ma_gap_5 double precision,
    ma_gap_10 double precision,
    ma_gap_20 double precision,
    rsi_6 double precision,
    rsi_14 double precision,
    kdj_k double precision,
    kdj_d double precision,
    kdj_j double precision,
    macd_dif double precision,
    macd_dea double precision,
    macd_hist double precision,
    vol_std_5 double precision,
    vol_std_20 double precision,
    vol_std_60 double precision,
    vol_atr_14 double precision,
    volume_ratio_5 double precision,
    volume_ratio_20 double precision,
    volume_ma_5 double precision,
    amount_ma_5 double precision,
    beta_20 double precision,
    bp double precision,
    ep_ttm double precision,
    ind_code_l1 text,
    ind_code_l2 text,
    micro_effective_spread double precision,
    micro_imbalance_volume double precision,
    micro_jump_flag double precision,
    adj_factor double precision
);
--
-- Name: trade_manual_execution_tasks; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.trade_manual_execution_tasks (
    task_id text NOT NULL,
    tenant_id text DEFAULT 'default'::text NOT NULL,
    user_id text NOT NULL,
    strategy_id text NOT NULL,
    strategy_name text NOT NULL,
    run_id text NOT NULL,
    model_id text NOT NULL,
    prediction_trade_date date NOT NULL,
    trading_mode text NOT NULL,
    status text NOT NULL,
    stage text DEFAULT 'queued'::text NOT NULL,
    error_stage text,
    error_message text,
    signal_count integer DEFAULT 0 NOT NULL,
    order_count integer DEFAULT 0 NOT NULL,
    success_count integer DEFAULT 0 NOT NULL,
    failed_count integer DEFAULT 0 NOT NULL,
    request_json jsonb,
    result_json jsonb,
    created_at timestamp with time zone NOT NULL,
    updated_at timestamp with time zone NOT NULL,
    progress integer DEFAULT 0 NOT NULL,
    task_type text DEFAULT 'manual'::text NOT NULL,
    task_source text DEFAULT 'manual_page'::text NOT NULL,
    trigger_mode text DEFAULT 'manual'::text NOT NULL,
    trigger_context_json jsonb,
    strategy_snapshot_json jsonb,
    parent_runtime_id text
);
--
-- Name: trades; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.trades (
    id integer NOT NULL,
    trade_id uuid NOT NULL,
    order_id uuid NOT NULL,
    tenant_id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    portfolio_id integer NOT NULL,
    symbol character varying(20) NOT NULL,
    symbol_name character varying(50),
    side public.orderside NOT NULL,
    trade_action public.tradeaction,
    position_side public.positionside NOT NULL,
    is_margin_trade boolean NOT NULL,
    trading_mode public.tradingmode NOT NULL,
    quantity double precision NOT NULL,
    price double precision NOT NULL,
    trade_value double precision NOT NULL,
    commission double precision NOT NULL,
    stamp_duty double precision NOT NULL,
    transfer_fee double precision NOT NULL,
    total_fee double precision NOT NULL,
    executed_at timestamp without time zone NOT NULL,
    exchange_trade_id character varying(100),
    exchange_name character varying(50),
    remarks character varying(500),
    created_at timestamp without time zone NOT NULL,
    updated_at timestamp without time zone NOT NULL
);
--
-- Name: trades_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.trades_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: trades_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.trades_id_seq OWNED BY public.trades.id;
--
-- Name: user_audit_logs; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.user_audit_logs (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    action character varying(64) NOT NULL,
    resource character varying(128),
    resource_id character varying(128),
    description text,
    request_data text,
    response_data text,
    ip_address character varying(64),
    user_agent text,
    request_method character varying(16),
    request_path character varying(255),
    status_code integer,
    success boolean,
    error_message text,
    created_at timestamp with time zone DEFAULT now(),
    duration_ms integer
);
--
-- Name: user_audit_logs_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.user_audit_logs_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: user_audit_logs_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.user_audit_logs_id_seq OWNED BY public.user_audit_logs.id;
--
-- Name: user_profiles; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.user_profiles (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    nickname character varying(128),
    avatar_url character varying(500),
    bio text,
    preferences jsonb DEFAULT '{}'::jsonb,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    display_name character varying(128),
    location character varying(128),
    website character varying(256),
    phone character varying(32),
    trading_experience character varying(32),
    risk_tolerance character varying(32),
    investment_goal character varying(64),
    github_url character varying(256),
    twitter_handle character varying(64),
    linkedin_url character varying(256),
    notification_settings jsonb DEFAULT '{}'::jsonb,
    ai_ide_api_key character varying(128),
    api_key text
);
--
-- Name: user_profiles_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.user_profiles_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: user_profiles_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.user_profiles_id_seq OWNED BY public.user_profiles.id;
--
-- Name: user_roles; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.user_roles (
    user_id character varying(64) NOT NULL,
    role_id integer NOT NULL,
    created_at timestamp with time zone DEFAULT now()
);
--
-- Name: user_sessions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.user_sessions (
    id character varying(64) DEFAULT (gen_random_uuid())::character varying(64),
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    token_hash character varying(255),
    device_info character varying(255),
    ip_address character varying(64),
    expires_at timestamp with time zone NOT NULL,
    revoked_at timestamp with time zone,
    created_at timestamp with time zone DEFAULT now(),
    session_id character varying(64) NOT NULL,
    token_jti character varying(64),
    user_agent character varying(255),
    last_activity_at timestamp with time zone,
    refresh_token character varying(1024),
    refresh_token_expires_at timestamp with time zone,
    last_active_at timestamp with time zone,
    is_active boolean DEFAULT true,
    is_revoked boolean DEFAULT false
);
--
-- Name: user_strategies; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.user_strategies (
    id character varying(64) NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) DEFAULT 'default'::character varying NOT NULL,
    strategy_name character varying(255) NOT NULL,
    description text,
    conditions jsonb DEFAULT '{}'::jsonb,
    stock_pool jsonb DEFAULT '{}'::jsonb,
    position_config jsonb DEFAULT '{}'::jsonb,
    style character varying(64),
    risk_config jsonb DEFAULT '{}'::jsonb,
    cos_url text,
    file_size integer,
    code_hash character varying(128),
    qlib_validated boolean DEFAULT false,
    validation_result jsonb DEFAULT '{}'::jsonb,
    tags text[] DEFAULT ARRAY[]::text[],
    is_public boolean DEFAULT false,
    downloads integer DEFAULT 0,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone DEFAULT now(),
    is_verified boolean DEFAULT false NOT NULL,
    shared_users jsonb DEFAULT '[]'::jsonb NOT NULL
);
--
-- Name: user_subscriptions; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.user_subscriptions (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    plan_id integer NOT NULL,
    status character varying(20),
    start_date timestamp with time zone NOT NULL,
    end_date timestamp with time zone NOT NULL,
    auto_renew boolean,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone,
    alipay_agreement_id character varying(64),
    alipay_agreement_status character varying(20)
);
--
-- Name: user_subscriptions_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.user_subscriptions_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: user_subscriptions_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.user_subscriptions_id_seq OWNED BY public.user_subscriptions.id;
--
-- Name: user_usages; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.user_usages (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    usage_type character varying(20) NOT NULL,
    count integer NOT NULL,
    period character varying(7) NOT NULL,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone
);
--
-- Name: user_usages_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.user_usages_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: user_usages_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.user_usages_id_seq OWNED BY public.user_usages.id;
--
-- Name: users; Type: TABLE; Schema: public; Owner: -
--
CREATE TABLE public.users (
    id integer NOT NULL,
    user_id character varying(64) NOT NULL,
    tenant_id character varying(64) NOT NULL,
    username character varying(128) NOT NULL,
    email character varying(255),
    phone_number character varying(32),
    password_hash character varying(255) NOT NULL,
    is_active boolean,
    is_verified boolean,
    is_admin boolean,
    is_locked boolean,
    last_login_at timestamp with time zone,
    last_login_ip character varying(64),
    login_count integer,
    created_at timestamp with time zone DEFAULT now(),
    updated_at timestamp with time zone,
    is_deleted boolean,
    deleted_at timestamp with time zone
);
--
-- Name: users_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--
CREATE SEQUENCE public.users_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;
--
-- Name: users_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--
ALTER SEQUENCE public.users_id_seq OWNED BY public.users.id;
--
-- Name: admin_data_files id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.admin_data_files ALTER COLUMN id SET DEFAULT nextval('public.admin_data_files_id_seq'::regclass);
--
-- Name: admin_models id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.admin_models ALTER COLUMN id SET DEFAULT nextval('public.admin_models_id_seq'::regclass);
--
-- Name: api_keys id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.api_keys ALTER COLUMN id SET DEFAULT nextval('public.api_keys_id_seq'::regclass);
--
-- Name: audit_logs id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.audit_logs ALTER COLUMN id SET DEFAULT nextval('public.audit_logs_id_seq'::regclass);
--
-- Name: backtests id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.backtests ALTER COLUMN id SET DEFAULT nextval('public.backtests_id_seq'::regclass);
--
-- Name: community_audit_logs id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_audit_logs ALTER COLUMN id SET DEFAULT nextval('public.community_audit_logs_id_seq'::regclass);
--
-- Name: community_author_follows id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_author_follows ALTER COLUMN id SET DEFAULT nextval('public.community_author_follows_id_seq'::regclass);
--
-- Name: community_comments id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_comments ALTER COLUMN id SET DEFAULT nextval('public.community_comments_id_seq'::regclass);
--
-- Name: community_interactions id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_interactions ALTER COLUMN id SET DEFAULT nextval('public.community_interactions_id_seq'::regclass);
--
-- Name: community_posts id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_posts ALTER COLUMN id SET DEFAULT nextval('public.community_posts_id_seq'::regclass);
--
-- Name: data_download_orders id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.data_download_orders ALTER COLUMN id SET DEFAULT nextval('public.data_download_orders_id_seq'::regclass);
--
-- Name: email_verifications id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.email_verifications ALTER COLUMN id SET DEFAULT nextval('public.email_verifications_id_seq'::regclass);
--
-- Name: engine_dispatch_items id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_dispatch_items ALTER COLUMN id SET DEFAULT nextval('public.engine_dispatch_items_id_seq'::regclass);
--
-- Name: engine_feature_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_feature_snapshots ALTER COLUMN id SET DEFAULT nextval('public.engine_feature_snapshots_id_seq'::regclass);
--
-- Name: engine_signal_scores id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_signal_scores ALTER COLUMN id SET DEFAULT nextval('public.engine_signal_scores_id_seq'::regclass);
--
-- Name: identity_verifications id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.identity_verifications ALTER COLUMN id SET DEFAULT nextval('public.identity_verifications_id_seq'::regclass);
--
-- Name: index_ohlcv_daily id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.index_ohlcv_daily ALTER COLUMN id SET DEFAULT nextval('public.index_ohlcv_daily_id_seq'::regclass);
--
-- Name: login_devices id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.login_devices ALTER COLUMN id SET DEFAULT nextval('public.login_devices_id_seq'::regclass);
--
-- Name: notifications id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.notifications ALTER COLUMN id SET DEFAULT nextval('public.notifications_id_seq'::regclass);
--
-- Name: orders id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.orders ALTER COLUMN id SET DEFAULT nextval('public.orders_id_seq'::regclass);
--
-- Name: password_reset_tokens id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.password_reset_tokens ALTER COLUMN id SET DEFAULT nextval('public.password_reset_tokens_id_seq'::regclass);
--
-- Name: payment_methods id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_methods ALTER COLUMN id SET DEFAULT nextval('public.payment_methods_id_seq'::regclass);
--
-- Name: payment_transactions id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_transactions ALTER COLUMN id SET DEFAULT nextval('public.payment_transactions_id_seq'::regclass);
--
-- Name: permissions id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.permissions ALTER COLUMN id SET DEFAULT nextval('public.permissions_id_seq'::regclass);
--
-- Name: phone_verifications id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.phone_verifications ALTER COLUMN id SET DEFAULT nextval('public.phone_verifications_id_seq'::regclass);
--
-- Name: portfolio_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.portfolio_snapshots ALTER COLUMN id SET DEFAULT nextval('public.portfolio_snapshots_id_seq'::regclass);
--
-- Name: portfolios id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.portfolios ALTER COLUMN id SET DEFAULT nextval('public.portfolios_id_seq'::regclass);
--
-- Name: position_history id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.position_history ALTER COLUMN id SET DEFAULT nextval('public.position_history_id_seq'::regclass);
--
-- Name: positions id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.positions ALTER COLUMN id SET DEFAULT nextval('public.positions_id_seq'::regclass);
--
-- Name: qm_market_calendar_exception id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_market_calendar_exception ALTER COLUMN id SET DEFAULT nextval('public.qm_market_calendar_exception_id_seq'::regclass);
--
-- Name: real_account_baselines id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_baselines ALTER COLUMN id SET DEFAULT nextval('public.real_account_baselines_id_seq'::regclass);
--
-- Name: real_account_ledger_daily_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_ledger_daily_snapshots ALTER COLUMN id SET DEFAULT nextval('public.real_account_ledger_daily_snapshots_id_seq'::regclass);
--
-- Name: real_account_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_snapshots ALTER COLUMN id SET DEFAULT nextval('public.real_account_snapshots_id_seq'::regclass);
--
-- Name: real_trading_preflight_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_trading_preflight_snapshots ALTER COLUMN id SET DEFAULT nextval('public.real_trading_preflight_snapshots_id_seq'::regclass);
--
-- Name: risk_rules id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.risk_rules ALTER COLUMN id SET DEFAULT nextval('public.risk_rules_id_seq'::regclass);
--
-- Name: roles id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.roles ALTER COLUMN id SET DEFAULT nextval('public.roles_id_seq'::regclass);
--
-- Name: simulation_account_daily id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_account_daily ALTER COLUMN id SET DEFAULT nextval('public.simulation_account_daily_id_seq'::regclass);
--
-- Name: simulation_cash_ledger id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_cash_ledger ALTER COLUMN id SET DEFAULT nextval('public.simulation_cash_ledger_id_seq'::regclass);
--
-- Name: simulation_corporate_actions id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_corporate_actions ALTER COLUMN id SET DEFAULT nextval('public.simulation_corporate_actions_id_seq'::regclass);
--
-- Name: simulation_daily_reports id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_daily_reports ALTER COLUMN id SET DEFAULT nextval('public.simulation_daily_reports_id_seq'::regclass);
--
-- Name: simulation_fills id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_fills ALTER COLUMN id SET DEFAULT nextval('public.simulation_fills_id_seq'::regclass);
--
-- Name: simulation_fund_snapshots id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_fund_snapshots ALTER COLUMN id SET DEFAULT nextval('public.simulation_fund_snapshots_id_seq'::regclass);
--
-- Name: simulation_orders id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_orders ALTER COLUMN id SET DEFAULT nextval('public.simulation_orders_id_seq'::regclass);
--
-- Name: simulation_position_daily id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_position_daily ALTER COLUMN id SET DEFAULT nextval('public.simulation_position_daily_id_seq'::regclass);
--
-- Name: simulation_position_lots id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_position_lots ALTER COLUMN id SET DEFAULT nextval('public.simulation_position_lots_id_seq'::regclass);
--
-- Name: simulation_positions id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_positions ALTER COLUMN id SET DEFAULT nextval('public.simulation_positions_id_seq'::regclass);
--
-- Name: simulation_rebalance_jobs id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_rebalance_jobs ALTER COLUMN id SET DEFAULT nextval('public.simulation_rebalance_jobs_id_seq'::regclass);
--
-- Name: stock_pool_files id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stock_pool_files ALTER COLUMN id SET DEFAULT nextval('public.stock_pool_files_id_seq'::regclass);
--
-- Name: stock_tag id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stock_tag ALTER COLUMN id SET DEFAULT nextval('public.stock_tag_id_seq'::regclass);
--
-- Name: stocks id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stocks ALTER COLUMN id SET DEFAULT nextval('public.stocks_id_seq'::regclass);
--
-- Name: strategies id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.strategies ALTER COLUMN id SET DEFAULT nextval('public.strategies_id_seq'::regclass);
--
-- Name: subscription_plans id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.subscription_plans ALTER COLUMN id SET DEFAULT nextval('public.subscription_plans_id_seq'::regclass);
--
-- Name: trades id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.trades ALTER COLUMN id SET DEFAULT nextval('public.trades_id_seq'::regclass);
--
-- Name: user_audit_logs id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_audit_logs ALTER COLUMN id SET DEFAULT nextval('public.user_audit_logs_id_seq'::regclass);
--
-- Name: user_profiles id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_profiles ALTER COLUMN id SET DEFAULT nextval('public.user_profiles_id_seq'::regclass);
--
-- Name: user_subscriptions id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_subscriptions ALTER COLUMN id SET DEFAULT nextval('public.user_subscriptions_id_seq'::regclass);
--
-- Name: user_usages id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_usages ALTER COLUMN id SET DEFAULT nextval('public.user_usages_id_seq'::regclass);
--
-- Name: users id; Type: DEFAULT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.users ALTER COLUMN id SET DEFAULT nextval('public.users_id_seq'::regclass);
--
-- Name: admin_data_files admin_data_files_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.admin_data_files
    ADD CONSTRAINT admin_data_files_pkey PRIMARY KEY (id);
--
-- Name: admin_models admin_models_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.admin_models
    ADD CONSTRAINT admin_models_pkey PRIMARY KEY (id);
--
-- Name: admin_training_jobs admin_training_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.admin_training_jobs
    ADD CONSTRAINT admin_training_jobs_pkey PRIMARY KEY (id);
--
-- Name: alembic_version_community alembic_version_community_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.alembic_version_community
    ADD CONSTRAINT alembic_version_community_pkc PRIMARY KEY (version_num);
--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);
--
-- Name: api_keys api_keys_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.api_keys
    ADD CONSTRAINT api_keys_pkey PRIMARY KEY (id);
--
-- Name: audit_logs audit_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.audit_logs
    ADD CONSTRAINT audit_logs_pkey PRIMARY KEY (id);
--
-- Name: backtests backtests_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.backtests
    ADD CONSTRAINT backtests_pkey PRIMARY KEY (id);
--
-- Name: community_audit_logs community_audit_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_audit_logs
    ADD CONSTRAINT community_audit_logs_pkey PRIMARY KEY (id);
--
-- Name: community_author_follows community_author_follows_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_author_follows
    ADD CONSTRAINT community_author_follows_pkey PRIMARY KEY (id);
--
-- Name: community_comments community_comments_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_comments
    ADD CONSTRAINT community_comments_pkey PRIMARY KEY (id);
--
-- Name: community_interactions community_interactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_interactions
    ADD CONSTRAINT community_interactions_pkey PRIMARY KEY (id);
--
-- Name: community_posts community_posts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_posts
    ADD CONSTRAINT community_posts_pkey PRIMARY KEY (id);
--
-- Name: data_download_orders data_download_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.data_download_orders
    ADD CONSTRAINT data_download_orders_pkey PRIMARY KEY (id);
--
-- Name: email_verifications email_verifications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.email_verifications
    ADD CONSTRAINT email_verifications_pkey PRIMARY KEY (id);
--
-- Name: engine_dispatch_batches engine_dispatch_batches_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_dispatch_batches
    ADD CONSTRAINT engine_dispatch_batches_pkey PRIMARY KEY (batch_id);
--
-- Name: engine_dispatch_items engine_dispatch_items_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_dispatch_items
    ADD CONSTRAINT engine_dispatch_items_pkey PRIMARY KEY (id);
--
-- Name: engine_feature_runs engine_feature_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_feature_runs
    ADD CONSTRAINT engine_feature_runs_pkey PRIMARY KEY (run_id);
--
-- Name: engine_feature_snapshots engine_feature_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_feature_snapshots
    ADD CONSTRAINT engine_feature_snapshots_pkey PRIMARY KEY (id);
--
-- Name: engine_signal_scores engine_signal_scores_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_signal_scores
    ADD CONSTRAINT engine_signal_scores_pkey PRIMARY KEY (id);
--
-- Name: identity_verifications identity_verifications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.identity_verifications
    ADD CONSTRAINT identity_verifications_pkey PRIMARY KEY (id);
--
-- Name: index_daily index_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.index_daily
    ADD CONSTRAINT index_daily_pkey PRIMARY KEY (trade_date, symbol);
--
-- Name: index_ohlcv_daily index_ohlcv_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.index_ohlcv_daily
    ADD CONSTRAINT index_ohlcv_daily_pkey PRIMARY KEY (id);
--
-- Name: index_ohlcv_daily index_ohlcv_daily_trade_date_symbol_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.index_ohlcv_daily
    ADD CONSTRAINT index_ohlcv_daily_trade_date_symbol_key UNIQUE (trade_date, symbol);
--
-- Name: klines klines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.klines
    ADD CONSTRAINT klines_pkey PRIMARY KEY (id);
--
-- Name: login_devices login_devices_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.login_devices
    ADD CONSTRAINT login_devices_pkey PRIMARY KEY (id);
--
-- Name: market_daily_stats market_daily_stats_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.market_daily_stats
    ADD CONSTRAINT market_daily_stats_pkey PRIMARY KEY (trade_date);
--
-- Name: market_data_daily market_data_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.market_data_daily
    ADD CONSTRAINT market_data_daily_pkey PRIMARY KEY (trade_date, symbol);
--
-- Name: notifications notifications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.notifications
    ADD CONSTRAINT notifications_pkey PRIMARY KEY (id);
--
-- Name: orders orders_client_order_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_client_order_id_key UNIQUE (client_order_id);
--
-- Name: orders orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.orders
    ADD CONSTRAINT orders_pkey PRIMARY KEY (id);
--
-- Name: password_reset_tokens password_reset_tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.password_reset_tokens
    ADD CONSTRAINT password_reset_tokens_pkey PRIMARY KEY (id);
--
-- Name: payment_methods payment_methods_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_methods
    ADD CONSTRAINT payment_methods_pkey PRIMARY KEY (id);
--
-- Name: payment_transactions payment_transactions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_transactions
    ADD CONSTRAINT payment_transactions_pkey PRIMARY KEY (id);
--
-- Name: permissions permissions_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.permissions
    ADD CONSTRAINT permissions_name_key UNIQUE (name);
--
-- Name: permissions permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.permissions
    ADD CONSTRAINT permissions_pkey PRIMARY KEY (id);
--
-- Name: phone_verifications phone_verifications_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.phone_verifications
    ADD CONSTRAINT phone_verifications_pkey PRIMARY KEY (id);
--
-- Name: pipeline_runs pipeline_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.pipeline_runs
    ADD CONSTRAINT pipeline_runs_pkey PRIMARY KEY (run_id);
--
-- Name: portfolio_snapshots portfolio_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.portfolio_snapshots
    ADD CONSTRAINT portfolio_snapshots_pkey PRIMARY KEY (id);
--
-- Name: portfolios portfolios_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.portfolios
    ADD CONSTRAINT portfolios_pkey PRIMARY KEY (id);
--
-- Name: position_history position_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.position_history
    ADD CONSTRAINT position_history_pkey PRIMARY KEY (id);
--
-- Name: positions positions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_pkey PRIMARY KEY (id);
--
-- Name: qlib_backtest_runs_cleanup_backup qlib_backtest_runs_cleanup_backup_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qlib_backtest_runs_cleanup_backup
    ADD CONSTRAINT qlib_backtest_runs_cleanup_backup_pkey PRIMARY KEY (backtest_id);
--
-- Name: qlib_optimization_runs qlib_optimization_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qlib_optimization_runs
    ADD CONSTRAINT qlib_optimization_runs_pkey PRIMARY KEY (optimization_id);
--
-- Name: qm_market_calendar_day qm_market_calendar_day_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_market_calendar_day
    ADD CONSTRAINT qm_market_calendar_day_pkey PRIMARY KEY (market, trade_date, tenant_id, user_id);
--
-- Name: qm_market_calendar_exception qm_market_calendar_exception_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_market_calendar_exception
    ADD CONSTRAINT qm_market_calendar_exception_pkey PRIMARY KEY (id);
--
-- Name: qm_market_calendar_version qm_market_calendar_version_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_market_calendar_version
    ADD CONSTRAINT qm_market_calendar_version_pkey PRIMARY KEY (market, year);
--
-- Name: qm_market_trading_session qm_market_trading_session_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_market_trading_session
    ADD CONSTRAINT qm_market_trading_session_pkey PRIMARY KEY (market, session_name, tenant_id, user_id);
--
-- Name: qm_model_inference_runs qm_model_inference_runs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_model_inference_runs
    ADD CONSTRAINT qm_model_inference_runs_pkey PRIMARY KEY (run_id);
--
-- Name: qm_model_inference_settings qm_model_inference_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_model_inference_settings
    ADD CONSTRAINT qm_model_inference_settings_pkey PRIMARY KEY (tenant_id, user_id, model_id);
--
-- Name: qm_research_candidate_snapshot qm_research_candidate_snapshot_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_research_candidate_snapshot
    ADD CONSTRAINT qm_research_candidate_snapshot_pkey PRIMARY KEY (tenant_id, user_id, run_id, symbol);
--
-- Name: qm_research_import_state qm_research_import_state_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_research_import_state
    ADD CONSTRAINT qm_research_import_state_pkey PRIMARY KEY (job_name);
--
-- Name: qm_strategy_model_bindings qm_strategy_model_bindings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_strategy_model_bindings
    ADD CONSTRAINT qm_strategy_model_bindings_pkey PRIMARY KEY (tenant_id, user_id, strategy_id);
--
-- Name: qm_user_models qm_user_models_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_user_models
    ADD CONSTRAINT qm_user_models_pkey PRIMARY KEY (tenant_id, user_id, model_id);
--
-- Name: qm_user_research_pool qm_user_research_pool_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_user_research_pool
    ADD CONSTRAINT qm_user_research_pool_pkey PRIMARY KEY (tenant_id, user_id, symbol);
--
-- Name: qm_user_watchlist qm_user_watchlist_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qm_user_watchlist
    ADD CONSTRAINT qm_user_watchlist_pkey PRIMARY KEY (tenant_id, user_id, symbol);
--
-- Name: qmt_agent_bindings qmt_agent_bindings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qmt_agent_bindings
    ADD CONSTRAINT qmt_agent_bindings_pkey PRIMARY KEY (id);
--
-- Name: qmt_agent_sessions qmt_agent_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qmt_agent_sessions
    ADD CONSTRAINT qmt_agent_sessions_pkey PRIMARY KEY (id);
--
-- Name: quote_daily_summaries quote_daily_summaries_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.quote_daily_summaries
    ADD CONSTRAINT quote_daily_summaries_pkey PRIMARY KEY (id);
--
-- Name: quotes quotes_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.quotes
    ADD CONSTRAINT quotes_pkey PRIMARY KEY (id);
--
-- Name: real_account_baselines real_account_baselines_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_baselines
    ADD CONSTRAINT real_account_baselines_pkey PRIMARY KEY (id);
--
-- Name: real_account_ledger_daily_snapshots real_account_ledger_daily_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_ledger_daily_snapshots
    ADD CONSTRAINT real_account_ledger_daily_snapshots_pkey PRIMARY KEY (id);
--
-- Name: real_account_snapshots real_account_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_snapshots
    ADD CONSTRAINT real_account_snapshots_pkey PRIMARY KEY (id);
--
-- Name: real_trading_preflight_snapshots real_trading_preflight_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_trading_preflight_snapshots
    ADD CONSTRAINT real_trading_preflight_snapshots_pkey PRIMARY KEY (id);
--
-- Name: risk_rules risk_rules_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.risk_rules
    ADD CONSTRAINT risk_rules_pkey PRIMARY KEY (id);
--
-- Name: role_permissions role_permissions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.role_permissions
    ADD CONSTRAINT role_permissions_pkey PRIMARY KEY (role_id, permission_id);
--
-- Name: roles roles_name_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_name_key UNIQUE (name);
--
-- Name: roles roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.roles
    ADD CONSTRAINT roles_pkey PRIMARY KEY (id);
--
-- Name: sim_orders sim_orders_order_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.sim_orders
    ADD CONSTRAINT sim_orders_order_id_key UNIQUE (order_id);
--
-- Name: sim_orders sim_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.sim_orders
    ADD CONSTRAINT sim_orders_pkey PRIMARY KEY (id);
--
-- Name: sim_trades sim_trades_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.sim_trades
    ADD CONSTRAINT sim_trades_pkey PRIMARY KEY (id);
--
-- Name: sim_trades sim_trades_trade_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.sim_trades
    ADD CONSTRAINT sim_trades_trade_id_key UNIQUE (trade_id);
--
-- Name: simulation_account_daily simulation_account_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_account_daily
    ADD CONSTRAINT simulation_account_daily_pkey PRIMARY KEY (id);
--
-- Name: simulation_accounts simulation_accounts_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_accounts
    ADD CONSTRAINT simulation_accounts_pkey PRIMARY KEY (account_id);
--
-- Name: simulation_cash_ledger simulation_cash_ledger_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_cash_ledger
    ADD CONSTRAINT simulation_cash_ledger_pkey PRIMARY KEY (id);
--
-- Name: simulation_corporate_actions simulation_corporate_actions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_corporate_actions
    ADD CONSTRAINT simulation_corporate_actions_pkey PRIMARY KEY (id);
--
-- Name: simulation_daily_reports simulation_daily_reports_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_daily_reports
    ADD CONSTRAINT simulation_daily_reports_pkey PRIMARY KEY (id);
--
-- Name: simulation_fills simulation_fills_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_fills
    ADD CONSTRAINT simulation_fills_pkey PRIMARY KEY (id);
--
-- Name: simulation_fund_snapshots simulation_fund_snapshots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_fund_snapshots
    ADD CONSTRAINT simulation_fund_snapshots_pkey PRIMARY KEY (id);
--
-- Name: simulation_jobs simulation_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_jobs
    ADD CONSTRAINT simulation_jobs_pkey PRIMARY KEY (id);
--
-- Name: simulation_orders simulation_orders_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_orders
    ADD CONSTRAINT simulation_orders_pkey PRIMARY KEY (id);
--
-- Name: simulation_position_daily simulation_position_daily_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_position_daily
    ADD CONSTRAINT simulation_position_daily_pkey PRIMARY KEY (id);
--
-- Name: simulation_position_lots simulation_position_lots_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_position_lots
    ADD CONSTRAINT simulation_position_lots_pkey PRIMARY KEY (id);
--
-- Name: simulation_positions simulation_positions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_positions
    ADD CONSTRAINT simulation_positions_pkey PRIMARY KEY (id);
--
-- Name: simulation_rebalance_jobs simulation_rebalance_jobs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_rebalance_jobs
    ADD CONSTRAINT simulation_rebalance_jobs_pkey PRIMARY KEY (id);
--
-- Name: stock_daily_latest stock_daily_latest_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stock_daily_latest
    ADD CONSTRAINT stock_daily_latest_pkey PRIMARY KEY (trade_date, symbol);
--
-- Name: stock_pool_files stock_pool_files_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stock_pool_files
    ADD CONSTRAINT stock_pool_files_pkey PRIMARY KEY (id);
--
-- Name: stock_tag stock_tag_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stock_tag
    ADD CONSTRAINT stock_tag_pkey PRIMARY KEY (id);
--
-- Name: stocks stocks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT stocks_pkey PRIMARY KEY (id);
--
-- Name: stocks stocks_symbol_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stocks
    ADD CONSTRAINT stocks_symbol_key UNIQUE (symbol);
--
-- Name: strategies strategies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.strategies
    ADD CONSTRAINT strategies_pkey PRIMARY KEY (id);
--
-- Name: strategy_loop_tasks strategy_loop_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.strategy_loop_tasks
    ADD CONSTRAINT strategy_loop_tasks_pkey PRIMARY KEY (task_id);
--
-- Name: subscription_plans subscription_plans_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.subscription_plans
    ADD CONSTRAINT subscription_plans_pkey PRIMARY KEY (id);
--
-- Name: system_settings system_settings_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.system_settings
    ADD CONSTRAINT system_settings_pkey PRIMARY KEY (key);
--
-- Name: system_tasks system_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.system_tasks
    ADD CONSTRAINT system_tasks_pkey PRIMARY KEY (task_id);
--
-- Name: tag_dictionary tag_dictionary_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.tag_dictionary
    ADD CONSTRAINT tag_dictionary_pkey PRIMARY KEY (tag_code);
--
-- Name: trade_manual_execution_tasks trade_manual_execution_tasks_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.trade_manual_execution_tasks
    ADD CONSTRAINT trade_manual_execution_tasks_pkey PRIMARY KEY (task_id);
--
-- Name: trades trades_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.trades
    ADD CONSTRAINT trades_pkey PRIMARY KEY (id);
--
-- Name: community_author_follows uq_community_author_follows_model; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_author_follows
    ADD CONSTRAINT uq_community_author_follows_model UNIQUE (tenant_id, follower_user_id, author_user_id);
--
-- Name: community_interactions uq_community_interactions; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.community_interactions
    ADD CONSTRAINT uq_community_interactions UNIQUE (tenant_id, user_id, post_id, comment_id, type);
--
-- Name: engine_dispatch_batches uq_engine_dispatch_batches_run_strategy; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_dispatch_batches
    ADD CONSTRAINT uq_engine_dispatch_batches_run_strategy UNIQUE (tenant_id, user_id, trade_date, run_id, strategy_id, trading_mode);
--
-- Name: engine_feature_snapshots uq_engine_feature_snapshots; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_feature_snapshots
    ADD CONSTRAINT uq_engine_feature_snapshots UNIQUE (tenant_id, user_id, trade_date, symbol, model_version, feature_version, run_id);
--
-- Name: engine_signal_scores uq_engine_signal_scores; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_signal_scores
    ADD CONSTRAINT uq_engine_signal_scores UNIQUE (tenant_id, user_id, trade_date, symbol, model_version, feature_version, run_id);
--
-- Name: real_trading_preflight_snapshots uq_preflight_snapshot_daily; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_trading_preflight_snapshots
    ADD CONSTRAINT uq_preflight_snapshot_daily UNIQUE (tenant_id, user_id, trading_mode, snapshot_date);
--
-- Name: qlib_backtest_runs uq_qlib_backtest_runs_backtest_id; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.qlib_backtest_runs
    ADD CONSTRAINT uq_qlib_backtest_runs_backtest_id UNIQUE (backtest_id);
--
-- Name: quote_daily_summaries uq_quote_daily_summary; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.quote_daily_summaries
    ADD CONSTRAINT uq_quote_daily_summary UNIQUE (trade_date, symbol, data_source);
--
-- Name: real_account_baselines uq_real_account_baseline; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_baselines
    ADD CONSTRAINT uq_real_account_baseline UNIQUE (tenant_id, user_id, account_id);
--
-- Name: real_account_baselines uq_real_account_baselines_scope; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_baselines
    ADD CONSTRAINT uq_real_account_baselines_scope UNIQUE (tenant_id, user_id, account_id);
--
-- Name: real_account_ledger_daily_snapshots uq_real_account_ledger_daily; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.real_account_ledger_daily_snapshots
    ADD CONSTRAINT uq_real_account_ledger_daily UNIQUE (tenant_id, user_id, account_id, snapshot_date);
--
-- Name: simulation_fund_snapshots uq_simulation_fund_snapshots_scope_date; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_fund_snapshots
    ADD CONSTRAINT uq_simulation_fund_snapshots_scope_date UNIQUE (tenant_id, user_id, snapshot_date);
--
-- Name: stock_tag uq_stock_tag_symbol_code; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stock_tag
    ADD CONSTRAINT uq_stock_tag_symbol_code UNIQUE (symbol, tag_code);
--
-- Name: klines uq_symbol_interval_timestamp; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.klines
    ADD CONSTRAINT uq_symbol_interval_timestamp UNIQUE (symbol, "interval", "timestamp");
--
-- Name: user_usages uq_user_usage_period; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_usages
    ADD CONSTRAINT uq_user_usage_period UNIQUE (user_id, tenant_id, usage_type, period);
--
-- Name: users uq_users_tenant_email; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.users
    ADD CONSTRAINT uq_users_tenant_email UNIQUE (tenant_id, email);
--
-- Name: users uq_users_tenant_phone; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.users
    ADD CONSTRAINT uq_users_tenant_phone UNIQUE (tenant_id, phone_number);
--
-- Name: users uq_users_tenant_username; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.users
    ADD CONSTRAINT uq_users_tenant_username UNIQUE (tenant_id, username);
--
-- Name: user_audit_logs user_audit_logs_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_audit_logs
    ADD CONSTRAINT user_audit_logs_pkey PRIMARY KEY (id);
--
-- Name: user_profiles user_profiles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_profiles
    ADD CONSTRAINT user_profiles_pkey PRIMARY KEY (id);
--
-- Name: user_roles user_roles_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_pkey PRIMARY KEY (user_id, role_id);
--
-- Name: user_sessions user_sessions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_sessions
    ADD CONSTRAINT user_sessions_pkey PRIMARY KEY (session_id);
--
-- Name: user_strategies user_strategies_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_strategies
    ADD CONSTRAINT user_strategies_pkey PRIMARY KEY (id);
--
-- Name: user_subscriptions user_subscriptions_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_subscriptions
    ADD CONSTRAINT user_subscriptions_pkey PRIMARY KEY (id);
--
-- Name: user_usages user_usages_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_usages
    ADD CONSTRAINT user_usages_pkey PRIMARY KEY (id);
--
-- Name: users users_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_pkey PRIMARY KEY (id);
--
-- Name: users users_user_id_key; Type: CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.users
    ADD CONSTRAINT users_user_id_key UNIQUE (user_id);
--
-- Name: idx_api_keys_access_key; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_api_keys_access_key ON public.api_keys USING btree (access_key);
--
-- Name: idx_api_keys_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_api_keys_user_id ON public.api_keys USING btree (user_id);
--
-- Name: idx_audit_logs_created_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_audit_logs_created_at ON public.audit_logs USING btree (created_at);
--
-- Name: idx_audit_logs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_audit_logs_user_id ON public.audit_logs USING btree (user_id);
--
-- Name: idx_engine_dispatch_batches_stage; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_dispatch_batches_stage ON public.engine_dispatch_batches USING btree (tenant_id, user_id, trade_date DESC, stage);
--
-- Name: idx_engine_dispatch_items_batch_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_dispatch_items_batch_status ON public.engine_dispatch_items USING btree (batch_id, dispatch_status);
--
-- Name: idx_engine_dispatch_items_symbol_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_dispatch_items_symbol_date ON public.engine_dispatch_items USING btree (symbol, trade_date DESC);
--
-- Name: idx_engine_feature_runs_model; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_feature_runs_model ON public.engine_feature_runs USING btree (model_name, model_version, feature_version, trade_date DESC);
--
-- Name: idx_engine_feature_runs_tenant_user_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_feature_runs_tenant_user_date ON public.engine_feature_runs USING btree (tenant_id, user_id, trade_date DESC);
--
-- Name: idx_engine_feature_snapshots_date_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_feature_snapshots_date_symbol ON public.engine_feature_snapshots USING btree (trade_date, symbol);
--
-- Name: idx_engine_feature_snapshots_run; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_feature_snapshots_run ON public.engine_feature_snapshots USING btree (run_id, symbol);
--
-- Name: idx_engine_signal_scores_date_rank; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_signal_scores_date_rank ON public.engine_signal_scores USING btree (trade_date DESC, score_rank);
--
-- Name: idx_engine_signal_scores_run_rank; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_engine_signal_scores_run_rank ON public.engine_signal_scores USING btree (run_id, score_rank);
--
-- Name: idx_feature_runs_user; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_feature_runs_user ON public.engine_feature_runs USING btree (tenant_id, user_id);
--
-- Name: idx_index_daily_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_index_daily_date ON public.index_daily USING btree (trade_date);
--
-- Name: idx_index_daily_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_index_daily_symbol ON public.index_daily USING btree (symbol);
--
-- Name: idx_index_ohlcv_daily_symbol_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_index_ohlcv_daily_symbol_date ON public.index_ohlcv_daily USING btree (symbol, trade_date);
--
-- Name: idx_index_ohlcv_daily_trade_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_index_ohlcv_daily_trade_date ON public.index_ohlcv_daily USING btree (trade_date);
--
-- Name: idx_mds_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_mds_date ON public.market_daily_stats USING btree (trade_date DESC);
--
-- Name: idx_notifications_tenant_user_created_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_notifications_tenant_user_created_at ON public.notifications USING btree (tenant_id, user_id, created_at DESC);
--
-- Name: idx_notifications_tenant_user_read_created_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_notifications_tenant_user_read_created_at ON public.notifications USING btree (tenant_id, user_id, is_read, created_at DESC);
--
-- Name: idx_notifications_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_notifications_user_id ON public.notifications USING btree (user_id);
--
-- Name: idx_order_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_order_created ON public.orders USING btree (created_at);
--
-- Name: idx_order_portfolio_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_order_portfolio_symbol ON public.orders USING btree (portfolio_id, symbol);
--
-- Name: idx_order_tenant_user_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_order_tenant_user_status ON public.orders USING btree (tenant_id, user_id, status);
--
-- Name: idx_order_user_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_order_user_status ON public.orders USING btree (user_id, status);
--
-- Name: idx_pipeline_runs_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_pipeline_runs_status ON public.pipeline_runs USING btree (status);
--
-- Name: idx_pipeline_runs_tenant_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_pipeline_runs_tenant_created ON public.pipeline_runs USING btree (tenant_id, created_at DESC);
--
-- Name: idx_portfolio_created_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_portfolio_created_at ON public.portfolios USING btree (created_at);
--
-- Name: idx_portfolio_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_portfolio_date ON public.portfolio_snapshots USING btree (portfolio_id, snapshot_date);
--
-- Name: idx_portfolio_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_portfolio_symbol ON public.positions USING btree (portfolio_id, symbol);
--
-- Name: idx_portfolio_tenant_user_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_portfolio_tenant_user_status ON public.portfolios USING btree (tenant_id, user_id, status);
--
-- Name: idx_portfolio_user_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_portfolio_user_status ON public.portfolios USING btree (user_id, status);
--
-- Name: idx_pos_history_created_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_pos_history_created_at ON public.position_history USING btree (created_at);
--
-- Name: idx_post_tenant_category; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_post_tenant_category ON public.community_posts USING btree (tenant_id, category);
--
-- Name: idx_qlib_backtest_runs_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qlib_backtest_runs_status ON public.qlib_backtest_runs USING btree (status);
--
-- Name: idx_qlib_backtest_runs_tenant_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qlib_backtest_runs_tenant_created ON public.qlib_backtest_runs USING btree (tenant_id, created_at DESC);
--
-- Name: idx_qlib_backtest_runs_user_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qlib_backtest_runs_user_created ON public.qlib_backtest_runs USING btree (user_id, created_at DESC);
--
-- Name: idx_qlib_backtest_runs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qlib_backtest_runs_user_id ON public.qlib_backtest_runs USING btree (user_id);
--
-- Name: idx_qlib_optimization_runs_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qlib_optimization_runs_status ON public.qlib_optimization_runs USING btree (status);
--
-- Name: idx_qlib_optimization_runs_tenant_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qlib_optimization_runs_tenant_created ON public.qlib_optimization_runs USING btree (tenant_id, created_at DESC);
--
-- Name: idx_qlib_optimization_runs_user_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qlib_optimization_runs_user_created ON public.qlib_optimization_runs USING btree (user_id, created_at DESC);
--
-- Name: idx_qm_calendar_day_query; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_calendar_day_query ON public.qm_market_calendar_day USING btree (market, tenant_id, user_id, trade_date);
--
-- Name: idx_qm_calendar_exception_query; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_calendar_exception_query ON public.qm_market_calendar_exception USING btree (market, tenant_id, user_id, trade_date);
--
-- Name: idx_qm_model_inference_runs_model_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_model_inference_runs_model_status ON public.qm_model_inference_runs USING btree (tenant_id, user_id, model_id, status, created_at DESC);
--
-- Name: idx_qm_model_inference_runs_owner_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_model_inference_runs_owner_created ON public.qm_model_inference_runs USING btree (tenant_id, user_id, created_at DESC);
--
-- Name: idx_qm_model_inference_runs_target_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_model_inference_runs_target_date ON public.qm_model_inference_runs USING btree (tenant_id, user_id, prediction_trade_date DESC);
--
-- Name: idx_qm_model_inference_settings_owner; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_model_inference_settings_owner ON public.qm_model_inference_settings USING btree (tenant_id, user_id, model_id, updated_at DESC);
--
-- Name: idx_qm_research_candidate_snapshot_model_run; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_research_candidate_snapshot_model_run ON public.qm_research_candidate_snapshot USING btree (tenant_id, user_id, model_id, run_id);
--
-- Name: idx_qm_research_candidate_snapshot_score; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_research_candidate_snapshot_score ON public.qm_research_candidate_snapshot USING btree (tenant_id, user_id, prediction_trade_date DESC, fusion_score DESC);
--
-- Name: idx_qm_research_candidate_snapshot_trade_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_research_candidate_snapshot_trade_date ON public.qm_research_candidate_snapshot USING btree (prediction_trade_date DESC, tenant_id, user_id);
--
-- Name: idx_qm_strategy_model_bindings_model; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_strategy_model_bindings_model ON public.qm_strategy_model_bindings USING btree (tenant_id, user_id, model_id);
--
-- Name: idx_qm_user_models_user_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qm_user_models_user_status ON public.qm_user_models USING btree (tenant_id, user_id, status, updated_at DESC);
--
-- Name: idx_qmt_binding_api_key; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qmt_binding_api_key ON public.qmt_agent_bindings USING btree (api_key_id);
--
-- Name: idx_qmt_binding_tenant_account_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qmt_binding_tenant_account_status ON public.qmt_agent_bindings USING btree (tenant_id, account_id, status);
--
-- Name: idx_qmt_session_binding; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qmt_session_binding ON public.qmt_agent_sessions USING btree (binding_id);
--
-- Name: idx_qmt_session_tenant_user; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_qmt_session_tenant_user ON public.qmt_agent_sessions USING btree (tenant_id, user_id);
--
-- Name: idx_quote_daily_summaries_symbol_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_quote_daily_summaries_symbol_date ON public.quote_daily_summaries USING btree (symbol, trade_date);
--
-- Name: idx_quote_timestamp; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_quote_timestamp ON public.quotes USING btree ("timestamp");
--
-- Name: idx_signal_scores_run_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_signal_scores_run_id ON public.engine_signal_scores USING btree (run_id);
--
-- Name: idx_signal_scores_trade_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_signal_scores_trade_date ON public.engine_signal_scores USING btree (trade_date);
--
-- Name: idx_signal_scores_user; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_signal_scores_user ON public.engine_signal_scores USING btree (tenant_id, user_id);
--
-- Name: idx_sim_account_daily_owner_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_account_daily_owner_date ON public.simulation_account_daily USING btree (tenant_id, user_id, snapshot_date);
--
-- Name: idx_sim_account_daily_owner_time; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_account_daily_owner_time ON public.simulation_account_daily USING btree (tenant_id, user_id, snapshot_at);
--
-- Name: idx_sim_cash_ledger_owner_time; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_cash_ledger_owner_time ON public.simulation_cash_ledger USING btree (tenant_id, user_id, occurred_at);
--
-- Name: idx_sim_corporate_actions_status_effective; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_corporate_actions_status_effective ON public.simulation_corporate_actions USING btree (status, effective_date);
--
-- Name: idx_sim_corporate_actions_symbol_dates; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_corporate_actions_symbol_dates ON public.simulation_corporate_actions USING btree (symbol, ex_date, effective_date);
--
-- Name: idx_sim_orders_job_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_orders_job_id ON public.sim_orders USING btree (job_id);
--
-- Name: idx_sim_position_daily_owner_symbol_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_position_daily_owner_symbol_date ON public.simulation_position_daily USING btree (tenant_id, user_id, symbol, snapshot_date);
--
-- Name: idx_sim_position_daily_owner_symbol_time; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_position_daily_owner_symbol_time ON public.simulation_position_daily USING btree (tenant_id, user_id, symbol, snapshot_at);
--
-- Name: idx_sim_position_lots_owner_symbol_side; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_position_lots_owner_symbol_side ON public.simulation_position_lots USING btree (tenant_id, user_id, symbol, position_side);
--
-- Name: idx_sim_rebalance_jobs_owner_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_rebalance_jobs_owner_status ON public.simulation_rebalance_jobs USING btree (tenant_id, user_id, status);
--
-- Name: idx_sim_trades_job_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_sim_trades_job_id ON public.sim_trades USING btree (job_id);
--
-- Name: idx_simulation_accounts_tenant_user; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX idx_simulation_accounts_tenant_user ON public.simulation_accounts USING btree (tenant_id, user_id);
--
-- Name: idx_simulation_fills_owner_executed; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_fills_owner_executed ON public.simulation_fills USING btree (tenant_id, user_id, executed_at);
--
-- Name: idx_simulation_fills_owner_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_fills_owner_symbol ON public.simulation_fills USING btree (tenant_id, user_id, symbol);
--
-- Name: idx_simulation_fund_snapshots_snapshot_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_fund_snapshots_snapshot_date ON public.simulation_fund_snapshots USING btree (snapshot_date);
--
-- Name: idx_simulation_fund_snapshots_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_fund_snapshots_tenant_id ON public.simulation_fund_snapshots USING btree (tenant_id);
--
-- Name: idx_simulation_fund_snapshots_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_fund_snapshots_user_id ON public.simulation_fund_snapshots USING btree (user_id);
--
-- Name: idx_simulation_jobs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_jobs_user_id ON public.simulation_jobs USING btree (user_id);
--
-- Name: idx_simulation_orders_owner_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_orders_owner_created ON public.simulation_orders USING btree (tenant_id, user_id, created_at);
--
-- Name: idx_simulation_orders_owner_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_simulation_orders_owner_status ON public.simulation_orders USING btree (tenant_id, user_id, status);
--
-- Name: idx_snapshot_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_snapshot_date ON public.portfolio_snapshots USING btree (snapshot_date);
--
-- Name: idx_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_status ON public.positions USING btree (status);
--
-- Name: idx_strategies_cos_key; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_strategies_cos_key ON public.strategies USING btree (cos_key) WHERE (cos_key IS NOT NULL);
--
-- Name: idx_strategies_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_strategies_status ON public.strategies USING btree (status);
--
-- Name: idx_strategies_updated_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_strategies_updated_at ON public.strategies USING btree (updated_at DESC);
--
-- Name: idx_strategies_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_strategies_user_id ON public.strategies USING btree (user_id);
--
-- Name: idx_strategy_loop_tasks_user_tenant_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_strategy_loop_tasks_user_tenant_created ON public.strategy_loop_tasks USING btree (user_id, tenant_id, created_at DESC);
--
-- Name: idx_symbol_interval_timestamp; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_symbol_interval_timestamp ON public.klines USING btree (symbol, "interval", "timestamp");
--
-- Name: idx_symbol_timestamp; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_symbol_timestamp ON public.quotes USING btree (symbol, "timestamp");
--
-- Name: idx_timestamp; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_timestamp ON public.klines USING btree ("timestamp");
--
-- Name: idx_trade_executed; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_executed ON public.trades USING btree (executed_at);
--
-- Name: idx_trade_manual_execution_tasks_owner_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_manual_execution_tasks_owner_created ON public.trade_manual_execution_tasks USING btree (tenant_id, user_id, created_at DESC);
--
-- Name: idx_trade_manual_execution_tasks_status_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_manual_execution_tasks_status_created ON public.trade_manual_execution_tasks USING btree (status, created_at DESC);
--
-- Name: idx_trade_manual_execution_tasks_type_created; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_manual_execution_tasks_type_created ON public.trade_manual_execution_tasks USING btree (task_type, created_at DESC);
--
-- Name: idx_trade_order; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_order ON public.trades USING btree (order_id);
--
-- Name: idx_trade_portfolio; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_portfolio ON public.trades USING btree (portfolio_id, executed_at);
--
-- Name: idx_trade_tenant_user_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_tenant_user_symbol ON public.trades USING btree (tenant_id, user_id, symbol);
--
-- Name: idx_trade_user_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_trade_user_symbol ON public.trades USING btree (user_id, symbol);
--
-- Name: idx_user_research_pool_added_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_user_research_pool_added_at ON public.qm_user_research_pool USING btree (added_at DESC);
--
-- Name: idx_user_research_pool_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_user_research_pool_status ON public.qm_user_research_pool USING btree (status);
--
-- Name: idx_user_research_pool_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_user_research_pool_user_id ON public.qm_user_research_pool USING btree (user_id);
--
-- Name: idx_user_sessions_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_user_sessions_user_id ON public.user_sessions USING btree (user_id);
--
-- Name: idx_user_strategies_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_user_strategies_user_id ON public.user_strategies USING btree (user_id);
--
-- Name: idx_user_watchlist_added_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_user_watchlist_added_at ON public.qm_user_watchlist USING btree (added_at DESC);
--
-- Name: idx_user_watchlist_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_user_watchlist_user_id ON public.qm_user_watchlist USING btree (user_id);
--
-- Name: idx_users_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_users_tenant_id ON public.users USING btree (tenant_id);
--
-- Name: idx_users_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX idx_users_user_id ON public.users USING btree (user_id);
--
-- Name: ix_admin_data_files_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_data_files_id ON public.admin_data_files USING btree (id);
--
-- Name: ix_admin_data_files_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_data_files_tenant_id ON public.admin_data_files USING btree (tenant_id);
--
-- Name: ix_admin_models_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_models_id ON public.admin_models USING btree (id);
--
-- Name: ix_admin_models_name; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_models_name ON public.admin_models USING btree (name);
--
-- Name: ix_admin_models_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_models_tenant_id ON public.admin_models USING btree (tenant_id);
--
-- Name: ix_admin_models_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_models_user_id ON public.admin_models USING btree (user_id);
--
-- Name: ix_admin_training_jobs_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_training_jobs_id ON public.admin_training_jobs USING btree (id);
--
-- Name: ix_admin_training_jobs_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_training_jobs_tenant_id ON public.admin_training_jobs USING btree (tenant_id);
--
-- Name: ix_admin_training_jobs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_admin_training_jobs_user_id ON public.admin_training_jobs USING btree (user_id);
--
-- Name: ix_backtests_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_backtests_id ON public.backtests USING btree (id);
--
-- Name: ix_backtests_strategy_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_backtests_strategy_id ON public.backtests USING btree (strategy_id);
--
-- Name: ix_backtests_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_backtests_user_id ON public.backtests USING btree (user_id);
--
-- Name: ix_community_audit_logs_action; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_audit_logs_action ON public.community_audit_logs USING btree (action);
--
-- Name: ix_community_audit_logs_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_audit_logs_tenant_id ON public.community_audit_logs USING btree (tenant_id);
--
-- Name: ix_community_audit_logs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_audit_logs_user_id ON public.community_audit_logs USING btree (user_id);
--
-- Name: ix_community_author_follows_author_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_author_follows_author_user_id ON public.community_author_follows USING btree (author_user_id);
--
-- Name: ix_community_author_follows_follower_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_author_follows_follower_user_id ON public.community_author_follows USING btree (follower_user_id);
--
-- Name: ix_community_author_follows_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_author_follows_tenant_id ON public.community_author_follows USING btree (tenant_id);
--
-- Name: ix_community_comments_author_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_comments_author_id ON public.community_comments USING btree (author_id);
--
-- Name: ix_community_comments_parent_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_comments_parent_id ON public.community_comments USING btree (parent_id);
--
-- Name: ix_community_comments_post_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_comments_post_id ON public.community_comments USING btree (post_id);
--
-- Name: ix_community_comments_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_comments_tenant_id ON public.community_comments USING btree (tenant_id);
--
-- Name: ix_community_interactions_comment_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_interactions_comment_id ON public.community_interactions USING btree (comment_id);
--
-- Name: ix_community_interactions_post_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_interactions_post_id ON public.community_interactions USING btree (post_id);
--
-- Name: ix_community_interactions_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_interactions_tenant_id ON public.community_interactions USING btree (tenant_id);
--
-- Name: ix_community_interactions_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_interactions_user_id ON public.community_interactions USING btree (user_id);
--
-- Name: ix_community_posts_author_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_posts_author_id ON public.community_posts USING btree (author_id);
--
-- Name: ix_community_posts_category; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_posts_category ON public.community_posts USING btree (category);
--
-- Name: ix_community_posts_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_posts_id ON public.community_posts USING btree (id);
--
-- Name: ix_community_posts_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_community_posts_tenant_id ON public.community_posts USING btree (tenant_id);
--
-- Name: ix_data_download_orders_order_no; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_data_download_orders_order_no ON public.data_download_orders USING btree (order_no);
--
-- Name: ix_data_download_orders_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_data_download_orders_status ON public.data_download_orders USING btree (status);
--
-- Name: ix_data_download_orders_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_data_download_orders_tenant_id ON public.data_download_orders USING btree (tenant_id);
--
-- Name: ix_data_download_orders_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_data_download_orders_user_id ON public.data_download_orders USING btree (user_id);
--
-- Name: ix_email_verifications_email; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_email_verifications_email ON public.email_verifications USING btree (email);
--
-- Name: ix_email_verifications_expires_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_email_verifications_expires_at ON public.email_verifications USING btree (expires_at);
--
-- Name: ix_email_verifications_is_used; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_email_verifications_is_used ON public.email_verifications USING btree (is_used);
--
-- Name: ix_email_verifications_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_email_verifications_tenant_id ON public.email_verifications USING btree (tenant_id);
--
-- Name: ix_email_verifications_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_email_verifications_user_id ON public.email_verifications USING btree (user_id);
--
-- Name: ix_email_verifications_verification_code; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_email_verifications_verification_code ON public.email_verifications USING btree (verification_code);
--
-- Name: ix_identity_verifications_id_number; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_identity_verifications_id_number ON public.identity_verifications USING btree (id_number);
--
-- Name: ix_identity_verifications_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_identity_verifications_status ON public.identity_verifications USING btree (status);
--
-- Name: ix_identity_verifications_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_identity_verifications_tenant_id ON public.identity_verifications USING btree (tenant_id);
--
-- Name: ix_identity_verifications_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_identity_verifications_user_id ON public.identity_verifications USING btree (user_id);
--
-- Name: ix_klines_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_klines_symbol ON public.klines USING btree (symbol);
--
-- Name: ix_login_devices_device_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_login_devices_device_id ON public.login_devices USING btree (device_id);
--
-- Name: ix_login_devices_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_login_devices_tenant_id ON public.login_devices USING btree (tenant_id);
--
-- Name: ix_login_devices_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_login_devices_user_id ON public.login_devices USING btree (user_id);
--
-- Name: ix_orders_order_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_orders_order_id ON public.orders USING btree (order_id);
--
-- Name: ix_orders_portfolio_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_portfolio_id ON public.orders USING btree (portfolio_id);
--
-- Name: ix_orders_position_side; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_position_side ON public.orders USING btree (position_side);
--
-- Name: ix_orders_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_status ON public.orders USING btree (status);
--
-- Name: ix_orders_strategy_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_strategy_id ON public.orders USING btree (strategy_id);
--
-- Name: ix_orders_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_symbol ON public.orders USING btree (symbol);
--
-- Name: ix_orders_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_tenant_id ON public.orders USING btree (tenant_id);
--
-- Name: ix_orders_trade_action; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_trade_action ON public.orders USING btree (trade_action);
--
-- Name: ix_orders_trading_mode; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_trading_mode ON public.orders USING btree (trading_mode);
--
-- Name: ix_orders_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_orders_user_id ON public.orders USING btree (user_id);
--
-- Name: ix_password_reset_tokens_email; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_password_reset_tokens_email ON public.password_reset_tokens USING btree (email);
--
-- Name: ix_password_reset_tokens_expires_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_password_reset_tokens_expires_at ON public.password_reset_tokens USING btree (expires_at);
--
-- Name: ix_password_reset_tokens_is_used; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_password_reset_tokens_is_used ON public.password_reset_tokens USING btree (is_used);
--
-- Name: ix_password_reset_tokens_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_password_reset_tokens_tenant_id ON public.password_reset_tokens USING btree (tenant_id);
--
-- Name: ix_password_reset_tokens_token; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_password_reset_tokens_token ON public.password_reset_tokens USING btree (token);
--
-- Name: ix_password_reset_tokens_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_password_reset_tokens_user_id ON public.password_reset_tokens USING btree (user_id);
--
-- Name: ix_payment_methods_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_payment_methods_tenant_id ON public.payment_methods USING btree (tenant_id);
--
-- Name: ix_payment_methods_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_payment_methods_user_id ON public.payment_methods USING btree (user_id);
--
-- Name: ix_payment_transactions_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_payment_transactions_status ON public.payment_transactions USING btree (status);
--
-- Name: ix_payment_transactions_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_payment_transactions_tenant_id ON public.payment_transactions USING btree (tenant_id);
--
-- Name: ix_payment_transactions_transaction_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_payment_transactions_transaction_id ON public.payment_transactions USING btree (transaction_id);
--
-- Name: ix_payment_transactions_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_payment_transactions_user_id ON public.payment_transactions USING btree (user_id);
--
-- Name: ix_permissions_code; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_permissions_code ON public.permissions USING btree (code);
--
-- Name: ix_permissions_resource; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_permissions_resource ON public.permissions USING btree (resource);
--
-- Name: ix_phone_verifications_expires_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_phone_verifications_expires_at ON public.phone_verifications USING btree (expires_at);
--
-- Name: ix_phone_verifications_is_used; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_phone_verifications_is_used ON public.phone_verifications USING btree (is_used);
--
-- Name: ix_phone_verifications_lookup; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_phone_verifications_lookup ON public.phone_verifications USING btree (tenant_id, phone_number, code_type, verification_code);
--
-- Name: ix_phone_verifications_phone_number; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_phone_verifications_phone_number ON public.phone_verifications USING btree (phone_number);
--
-- Name: ix_phone_verifications_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_phone_verifications_tenant_id ON public.phone_verifications USING btree (tenant_id);
--
-- Name: ix_portfolio_snapshots_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_portfolio_snapshots_id ON public.portfolio_snapshots USING btree (id);
--
-- Name: ix_portfolio_snapshots_portfolio_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_portfolio_snapshots_portfolio_id ON public.portfolio_snapshots USING btree (portfolio_id);
--
-- Name: ix_portfolios_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_portfolios_id ON public.portfolios USING btree (id);
--
-- Name: ix_portfolios_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_portfolios_tenant_id ON public.portfolios USING btree (tenant_id);
--
-- Name: ix_portfolios_trading_mode; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_portfolios_trading_mode ON public.portfolios USING btree (trading_mode);
--
-- Name: ix_portfolios_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_portfolios_user_id ON public.portfolios USING btree (user_id);
--
-- Name: ix_position_history_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_position_history_id ON public.position_history USING btree (id);
--
-- Name: ix_position_history_position_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_position_history_position_id ON public.position_history USING btree (position_id);
--
-- Name: ix_positions_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_positions_id ON public.positions USING btree (id);
--
-- Name: ix_positions_portfolio_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_positions_portfolio_id ON public.positions USING btree (portfolio_id);
--
-- Name: ix_positions_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_positions_symbol ON public.positions USING btree (symbol);
--
-- Name: ix_qmt_agent_bindings_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_bindings_account_id ON public.qmt_agent_bindings USING btree (account_id);
--
-- Name: ix_qmt_agent_bindings_api_key_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_bindings_api_key_id ON public.qmt_agent_bindings USING btree (api_key_id);
--
-- Name: ix_qmt_agent_bindings_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_bindings_status ON public.qmt_agent_bindings USING btree (status);
--
-- Name: ix_qmt_agent_bindings_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_bindings_tenant_id ON public.qmt_agent_bindings USING btree (tenant_id);
--
-- Name: ix_qmt_agent_bindings_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_bindings_user_id ON public.qmt_agent_bindings USING btree (user_id);
--
-- Name: ix_qmt_agent_sessions_binding_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_sessions_binding_id ON public.qmt_agent_sessions USING btree (binding_id);
--
-- Name: ix_qmt_agent_sessions_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_sessions_tenant_id ON public.qmt_agent_sessions USING btree (tenant_id);
--
-- Name: ix_qmt_agent_sessions_token_hash; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_qmt_agent_sessions_token_hash ON public.qmt_agent_sessions USING btree (token_hash);
--
-- Name: ix_qmt_agent_sessions_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_qmt_agent_sessions_user_id ON public.qmt_agent_sessions USING btree (user_id);
--
-- Name: ix_real_account_baselines_scope_time; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_baselines_scope_time ON public.real_account_baselines USING btree (tenant_id, user_id, account_id, first_snapshot_at);
--
-- Name: ix_real_account_ledger_daily_scope_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_ledger_daily_scope_date ON public.real_account_ledger_daily_snapshots USING btree (tenant_id, user_id, account_id, snapshot_date);
--
-- Name: ix_real_account_ledger_daily_snapshots_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_ledger_daily_snapshots_account_id ON public.real_account_ledger_daily_snapshots USING btree (account_id);
--
-- Name: ix_real_account_ledger_daily_snapshots_last_snapshot_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_ledger_daily_snapshots_last_snapshot_at ON public.real_account_ledger_daily_snapshots USING btree (last_snapshot_at);
--
-- Name: ix_real_account_ledger_daily_snapshots_snapshot_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_ledger_daily_snapshots_snapshot_date ON public.real_account_ledger_daily_snapshots USING btree (snapshot_date);
--
-- Name: ix_real_account_ledger_daily_snapshots_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_ledger_daily_snapshots_tenant_id ON public.real_account_ledger_daily_snapshots USING btree (tenant_id);
--
-- Name: ix_real_account_ledger_daily_snapshots_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_ledger_daily_snapshots_user_id ON public.real_account_ledger_daily_snapshots USING btree (user_id);
--
-- Name: ix_real_account_snapshots_account; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_account ON public.real_account_snapshots USING btree (account_id);
--
-- Name: ix_real_account_snapshots_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_account_id ON public.real_account_snapshots USING btree (account_id);
--
-- Name: ix_real_account_snapshots_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_date ON public.real_account_snapshots USING btree (snapshot_date);
--
-- Name: ix_real_account_snapshots_scope_date_time; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_scope_date_time ON public.real_account_snapshots USING btree (tenant_id, user_id, account_id, snapshot_date, snapshot_at);
--
-- Name: ix_real_account_snapshots_scope_month_time; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_scope_month_time ON public.real_account_snapshots USING btree (tenant_id, user_id, account_id, snapshot_month, snapshot_at);
--
-- Name: ix_real_account_snapshots_scope_time; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_scope_time ON public.real_account_snapshots USING btree (tenant_id, user_id, account_id, snapshot_at);
--
-- Name: ix_real_account_snapshots_snapshot_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_snapshot_at ON public.real_account_snapshots USING btree (snapshot_at);
--
-- Name: ix_real_account_snapshots_snapshot_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_snapshot_date ON public.real_account_snapshots USING btree (snapshot_date);
--
-- Name: ix_real_account_snapshots_snapshot_month; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_snapshot_month ON public.real_account_snapshots USING btree (snapshot_month);
--
-- Name: ix_real_account_snapshots_tenant; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_tenant ON public.real_account_snapshots USING btree (tenant_id);
--
-- Name: ix_real_account_snapshots_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_tenant_id ON public.real_account_snapshots USING btree (tenant_id);
--
-- Name: ix_real_account_snapshots_user; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_user ON public.real_account_snapshots USING btree (user_id);
--
-- Name: ix_real_account_snapshots_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_account_snapshots_user_id ON public.real_account_snapshots USING btree (user_id);
--
-- Name: ix_real_trading_preflight_snapshots_snapshot_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_trading_preflight_snapshots_snapshot_date ON public.real_trading_preflight_snapshots USING btree (snapshot_date);
--
-- Name: ix_real_trading_preflight_snapshots_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_trading_preflight_snapshots_tenant_id ON public.real_trading_preflight_snapshots USING btree (tenant_id);
--
-- Name: ix_real_trading_preflight_snapshots_trading_mode; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_trading_preflight_snapshots_trading_mode ON public.real_trading_preflight_snapshots USING btree (trading_mode);
--
-- Name: ix_real_trading_preflight_snapshots_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_real_trading_preflight_snapshots_user_id ON public.real_trading_preflight_snapshots USING btree (user_id);
--
-- Name: ix_risk_rules_is_active; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_risk_rules_is_active ON public.risk_rules USING btree (is_active);
--
-- Name: ix_risk_rules_rule_name; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_risk_rules_rule_name ON public.risk_rules USING btree (rule_name);
--
-- Name: ix_risk_rules_rule_type; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_risk_rules_rule_type ON public.risk_rules USING btree (rule_type);
--
-- Name: ix_roles_code; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_roles_code ON public.roles USING btree (code);
--
-- Name: ix_simulation_account_daily_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_account_daily_account_id ON public.simulation_account_daily USING btree (account_id);
--
-- Name: ix_simulation_account_daily_snapshot_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_account_daily_snapshot_at ON public.simulation_account_daily USING btree (snapshot_at);
--
-- Name: ix_simulation_account_daily_snapshot_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_account_daily_snapshot_date ON public.simulation_account_daily USING btree (snapshot_date);
--
-- Name: ix_simulation_account_daily_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_account_daily_tenant_id ON public.simulation_account_daily USING btree (tenant_id);
--
-- Name: ix_simulation_account_daily_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_account_daily_user_id ON public.simulation_account_daily USING btree (user_id);
--
-- Name: ix_simulation_accounts_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_accounts_tenant_id ON public.simulation_accounts USING btree (tenant_id);
--
-- Name: ix_simulation_accounts_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_accounts_user_id ON public.simulation_accounts USING btree (user_id);
--
-- Name: ix_simulation_cash_ledger_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_cash_ledger_account_id ON public.simulation_cash_ledger USING btree (account_id);
--
-- Name: ix_simulation_cash_ledger_event_type; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_cash_ledger_event_type ON public.simulation_cash_ledger USING btree (event_type);
--
-- Name: ix_simulation_cash_ledger_occurred_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_cash_ledger_occurred_at ON public.simulation_cash_ledger USING btree (occurred_at);
--
-- Name: ix_simulation_cash_ledger_ref_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_cash_ledger_ref_id ON public.simulation_cash_ledger USING btree (ref_id);
--
-- Name: ix_simulation_cash_ledger_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_cash_ledger_tenant_id ON public.simulation_cash_ledger USING btree (tenant_id);
--
-- Name: ix_simulation_cash_ledger_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_cash_ledger_user_id ON public.simulation_cash_ledger USING btree (user_id);
--
-- Name: ix_simulation_corporate_actions_action_type; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_corporate_actions_action_type ON public.simulation_corporate_actions USING btree (action_type);
--
-- Name: ix_simulation_corporate_actions_effective_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_corporate_actions_effective_date ON public.simulation_corporate_actions USING btree (effective_date);
--
-- Name: ix_simulation_corporate_actions_ex_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_corporate_actions_ex_date ON public.simulation_corporate_actions USING btree (ex_date);
--
-- Name: ix_simulation_corporate_actions_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_corporate_actions_status ON public.simulation_corporate_actions USING btree (status);
--
-- Name: ix_simulation_corporate_actions_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_corporate_actions_symbol ON public.simulation_corporate_actions USING btree (symbol);
--
-- Name: ix_simulation_daily_reports_job_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_daily_reports_job_id ON public.simulation_daily_reports USING btree (job_id);
--
-- Name: ix_simulation_fills_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_account_id ON public.simulation_fills USING btree (account_id);
--
-- Name: ix_simulation_fills_fill_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_simulation_fills_fill_id ON public.simulation_fills USING btree (fill_id);
--
-- Name: ix_simulation_fills_legacy_trade_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_legacy_trade_id ON public.simulation_fills USING btree (legacy_trade_id);
--
-- Name: ix_simulation_fills_order_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_order_id ON public.simulation_fills USING btree (order_id);
--
-- Name: ix_simulation_fills_portfolio_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_portfolio_id ON public.simulation_fills USING btree (portfolio_id);
--
-- Name: ix_simulation_fills_strategy_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_strategy_id ON public.simulation_fills USING btree (strategy_id);
--
-- Name: ix_simulation_fills_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_symbol ON public.simulation_fills USING btree (symbol);
--
-- Name: ix_simulation_fills_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_tenant_id ON public.simulation_fills USING btree (tenant_id);
--
-- Name: ix_simulation_fills_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_fills_user_id ON public.simulation_fills USING btree (user_id);
--
-- Name: ix_simulation_jobs_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_jobs_tenant_id ON public.simulation_jobs USING btree (tenant_id);
--
-- Name: ix_simulation_jobs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_jobs_user_id ON public.simulation_jobs USING btree (user_id);
--
-- Name: ix_simulation_orders_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_account_id ON public.simulation_orders USING btree (account_id);
--
-- Name: ix_simulation_orders_legacy_order_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_legacy_order_id ON public.simulation_orders USING btree (legacy_order_id);
--
-- Name: ix_simulation_orders_order_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_simulation_orders_order_id ON public.simulation_orders USING btree (order_id);
--
-- Name: ix_simulation_orders_portfolio_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_portfolio_id ON public.simulation_orders USING btree (portfolio_id);
--
-- Name: ix_simulation_orders_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_status ON public.simulation_orders USING btree (status);
--
-- Name: ix_simulation_orders_strategy_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_strategy_id ON public.simulation_orders USING btree (strategy_id);
--
-- Name: ix_simulation_orders_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_symbol ON public.simulation_orders USING btree (symbol);
--
-- Name: ix_simulation_orders_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_tenant_id ON public.simulation_orders USING btree (tenant_id);
--
-- Name: ix_simulation_orders_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_orders_user_id ON public.simulation_orders USING btree (user_id);
--
-- Name: ix_simulation_position_daily_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_daily_account_id ON public.simulation_position_daily USING btree (account_id);
--
-- Name: ix_simulation_position_daily_position_side; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_daily_position_side ON public.simulation_position_daily USING btree (position_side);
--
-- Name: ix_simulation_position_daily_snapshot_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_daily_snapshot_at ON public.simulation_position_daily USING btree (snapshot_at);
--
-- Name: ix_simulation_position_daily_snapshot_date; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_daily_snapshot_date ON public.simulation_position_daily USING btree (snapshot_date);
--
-- Name: ix_simulation_position_daily_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_daily_symbol ON public.simulation_position_daily USING btree (symbol);
--
-- Name: ix_simulation_position_daily_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_daily_tenant_id ON public.simulation_position_daily USING btree (tenant_id);
--
-- Name: ix_simulation_position_daily_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_daily_user_id ON public.simulation_position_daily USING btree (user_id);
--
-- Name: ix_simulation_position_lots_account_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_lots_account_id ON public.simulation_position_lots USING btree (account_id);
--
-- Name: ix_simulation_position_lots_open_fill_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_lots_open_fill_id ON public.simulation_position_lots USING btree (open_fill_id);
--
-- Name: ix_simulation_position_lots_position_side; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_lots_position_side ON public.simulation_position_lots USING btree (position_side);
--
-- Name: ix_simulation_position_lots_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_lots_symbol ON public.simulation_position_lots USING btree (symbol);
--
-- Name: ix_simulation_position_lots_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_lots_tenant_id ON public.simulation_position_lots USING btree (tenant_id);
--
-- Name: ix_simulation_position_lots_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_position_lots_user_id ON public.simulation_position_lots USING btree (user_id);
--
-- Name: ix_simulation_rebalance_jobs_idempotency_key; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_rebalance_jobs_idempotency_key ON public.simulation_rebalance_jobs USING btree (idempotency_key);
--
-- Name: ix_simulation_rebalance_jobs_job_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_simulation_rebalance_jobs_job_id ON public.simulation_rebalance_jobs USING btree (job_id);
--
-- Name: ix_simulation_rebalance_jobs_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_rebalance_jobs_status ON public.simulation_rebalance_jobs USING btree (status);
--
-- Name: ix_simulation_rebalance_jobs_strategy_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_rebalance_jobs_strategy_id ON public.simulation_rebalance_jobs USING btree (strategy_id);
--
-- Name: ix_simulation_rebalance_jobs_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_rebalance_jobs_tenant_id ON public.simulation_rebalance_jobs USING btree (tenant_id);
--
-- Name: ix_simulation_rebalance_jobs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_simulation_rebalance_jobs_user_id ON public.simulation_rebalance_jobs USING btree (user_id);
--
-- Name: ix_stock_tag_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_stock_tag_symbol ON public.stock_tag USING btree (symbol);
--
-- Name: ix_stock_tag_tag_code; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_stock_tag_tag_code ON public.stock_tag USING btree (tag_code);
--
-- Name: ix_strategies_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_strategies_id ON public.strategies USING btree (id);
--
-- Name: ix_strategies_name; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_strategies_name ON public.strategies USING btree (name);
--
-- Name: ix_strategies_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_strategies_status ON public.strategies USING btree (status);
--
-- Name: ix_strategies_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_strategies_user_id ON public.strategies USING btree (user_id);
--
-- Name: ix_subscription_plans_code; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_subscription_plans_code ON public.subscription_plans USING btree (code);
--
-- Name: ix_system_tasks_status; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_system_tasks_status ON public.system_tasks USING btree (status);
--
-- Name: ix_system_tasks_task_type; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_system_tasks_task_type ON public.system_tasks USING btree (task_type);
--
-- Name: ix_trades_order_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_order_id ON public.trades USING btree (order_id);
--
-- Name: ix_trades_portfolio_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_portfolio_id ON public.trades USING btree (portfolio_id);
--
-- Name: ix_trades_position_side; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_position_side ON public.trades USING btree (position_side);
--
-- Name: ix_trades_symbol; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_symbol ON public.trades USING btree (symbol);
--
-- Name: ix_trades_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_tenant_id ON public.trades USING btree (tenant_id);
--
-- Name: ix_trades_trade_action; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_trade_action ON public.trades USING btree (trade_action);
--
-- Name: ix_trades_trade_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_trades_trade_id ON public.trades USING btree (trade_id);
--
-- Name: ix_trades_trading_mode; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_trading_mode ON public.trades USING btree (trading_mode);
--
-- Name: ix_trades_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_trades_user_id ON public.trades USING btree (user_id);
--
-- Name: ix_user_audit_logs_action; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_audit_logs_action ON public.user_audit_logs USING btree (action);
--
-- Name: ix_user_audit_logs_created_at; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_audit_logs_created_at ON public.user_audit_logs USING btree (created_at);
--
-- Name: ix_user_audit_logs_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_audit_logs_tenant_id ON public.user_audit_logs USING btree (tenant_id);
--
-- Name: ix_user_audit_logs_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_audit_logs_user_id ON public.user_audit_logs USING btree (user_id);
--
-- Name: ix_user_subscriptions_alipay_agreement_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_subscriptions_alipay_agreement_id ON public.user_subscriptions USING btree (alipay_agreement_id);
--
-- Name: ix_user_subscriptions_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_subscriptions_tenant_id ON public.user_subscriptions USING btree (tenant_id);
--
-- Name: ix_user_subscriptions_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_subscriptions_user_id ON public.user_subscriptions USING btree (user_id);
--
-- Name: ix_user_usages_period; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_usages_period ON public.user_usages USING btree (period);
--
-- Name: ix_user_usages_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_usages_tenant_id ON public.user_usages USING btree (tenant_id);
--
-- Name: ix_user_usages_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_user_usages_user_id ON public.user_usages USING btree (user_id);
--
-- Name: ix_users_email; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_users_email ON public.users USING btree (email);
--
-- Name: ix_users_is_deleted; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_users_is_deleted ON public.users USING btree (is_deleted);
--
-- Name: ix_users_phone_number; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_users_phone_number ON public.users USING btree (phone_number);
--
-- Name: ix_users_tenant_id; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_users_tenant_id ON public.users USING btree (tenant_id);
--
-- Name: ix_users_user_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX ix_users_user_id ON public.users USING btree (user_id);
--
-- Name: ix_users_username; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX ix_users_username ON public.users USING btree (username);
--
-- Name: market_data_daily_symbol_idx; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX market_data_daily_symbol_idx ON ONLY public.market_data_daily USING btree (symbol);
--
-- Name: qlib_backtest_runs_cleanup_backup_status_idx; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX qlib_backtest_runs_cleanup_backup_status_idx ON public.qlib_backtest_runs_cleanup_backup USING btree (status);
--
-- Name: qlib_backtest_runs_cleanup_backup_tenant_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX qlib_backtest_runs_cleanup_backup_tenant_id_created_at_idx ON public.qlib_backtest_runs_cleanup_backup USING btree (tenant_id, created_at DESC);
--
-- Name: qlib_backtest_runs_cleanup_backup_user_id_created_at_idx; Type: INDEX; Schema: public; Owner: -
--
CREATE INDEX qlib_backtest_runs_cleanup_backup_user_id_created_at_idx ON public.qlib_backtest_runs_cleanup_backup USING btree (user_id, created_at DESC);
--
-- Name: uq_api_keys_access_key; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX uq_api_keys_access_key ON public.api_keys USING btree (access_key);
--
-- Name: uq_engine_dispatch_items_client_order_id; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX uq_engine_dispatch_items_client_order_id ON public.engine_dispatch_items USING btree (client_order_id) WHERE ((client_order_id IS NOT NULL) AND ((client_order_id)::text <> ''::text));
--
-- Name: uq_qm_user_models_default_per_user; Type: INDEX; Schema: public; Owner: -
--
CREATE UNIQUE INDEX uq_qm_user_models_default_per_user ON public.qm_user_models USING btree (tenant_id, user_id) WHERE (is_default = true);
--
-- Name: qlib_backtest_runs trg_auto_populate_id; Type: TRIGGER; Schema: public; Owner: -
--
CREATE TRIGGER trg_auto_populate_id BEFORE INSERT ON public.qlib_backtest_runs FOR EACH ROW EXECUTE FUNCTION public.auto_populate_id();
--
-- Name: admin_data_files admin_data_files_data_source_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.admin_data_files
    ADD CONSTRAINT admin_data_files_data_source_id_fkey FOREIGN KEY (data_source_id) REFERENCES public.admin_models(id) ON DELETE CASCADE;
--
-- Name: backtests backtests_strategy_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.backtests
    ADD CONSTRAINT backtests_strategy_id_fkey FOREIGN KEY (strategy_id) REFERENCES public.strategies(id);
--
-- Name: engine_dispatch_batches engine_dispatch_batches_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_dispatch_batches
    ADD CONSTRAINT engine_dispatch_batches_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.engine_feature_runs(run_id) ON DELETE CASCADE;
--
-- Name: engine_dispatch_items engine_dispatch_items_batch_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_dispatch_items
    ADD CONSTRAINT engine_dispatch_items_batch_id_fkey FOREIGN KEY (batch_id) REFERENCES public.engine_dispatch_batches(batch_id) ON DELETE CASCADE;
--
-- Name: engine_dispatch_items engine_dispatch_items_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_dispatch_items
    ADD CONSTRAINT engine_dispatch_items_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.engine_feature_runs(run_id) ON DELETE CASCADE;
--
-- Name: engine_feature_snapshots engine_feature_snapshots_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_feature_snapshots
    ADD CONSTRAINT engine_feature_snapshots_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.engine_feature_runs(run_id) ON DELETE CASCADE;
--
-- Name: engine_signal_scores engine_signal_scores_run_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.engine_signal_scores
    ADD CONSTRAINT engine_signal_scores_run_id_fkey FOREIGN KEY (run_id) REFERENCES public.engine_feature_runs(run_id) ON DELETE CASCADE;
--
-- Name: identity_verifications identity_verifications_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.identity_verifications
    ADD CONSTRAINT identity_verifications_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(user_id);
--
-- Name: password_reset_tokens password_reset_tokens_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.password_reset_tokens
    ADD CONSTRAINT password_reset_tokens_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(user_id);
--
-- Name: payment_methods payment_methods_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_methods
    ADD CONSTRAINT payment_methods_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(user_id);
--
-- Name: payment_transactions payment_transactions_payment_method_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_transactions
    ADD CONSTRAINT payment_transactions_payment_method_id_fkey FOREIGN KEY (payment_method_id) REFERENCES public.payment_methods(id);
--
-- Name: payment_transactions payment_transactions_subscription_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_transactions
    ADD CONSTRAINT payment_transactions_subscription_id_fkey FOREIGN KEY (subscription_id) REFERENCES public.user_subscriptions(id);
--
-- Name: payment_transactions payment_transactions_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.payment_transactions
    ADD CONSTRAINT payment_transactions_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(user_id);
--
-- Name: portfolio_snapshots portfolio_snapshots_portfolio_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.portfolio_snapshots
    ADD CONSTRAINT portfolio_snapshots_portfolio_id_fkey FOREIGN KEY (portfolio_id) REFERENCES public.portfolios(id);
--
-- Name: position_history position_history_position_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.position_history
    ADD CONSTRAINT position_history_position_id_fkey FOREIGN KEY (position_id) REFERENCES public.positions(id);
--
-- Name: positions positions_portfolio_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.positions
    ADD CONSTRAINT positions_portfolio_id_fkey FOREIGN KEY (portfolio_id) REFERENCES public.portfolios(id);
--
-- Name: role_permissions role_permissions_permission_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.role_permissions
    ADD CONSTRAINT role_permissions_permission_id_fkey FOREIGN KEY (permission_id) REFERENCES public.permissions(id);
--
-- Name: role_permissions role_permissions_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.role_permissions
    ADD CONSTRAINT role_permissions_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id);
--
-- Name: simulation_daily_reports simulation_daily_reports_job_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.simulation_daily_reports
    ADD CONSTRAINT simulation_daily_reports_job_id_fkey FOREIGN KEY (job_id) REFERENCES public.simulation_jobs(id);
--
-- Name: stock_tag stock_tag_tag_code_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.stock_tag
    ADD CONSTRAINT stock_tag_tag_code_fkey FOREIGN KEY (tag_code) REFERENCES public.tag_dictionary(tag_code) ON DELETE RESTRICT;
--
-- Name: strategies strategies_parent_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.strategies
    ADD CONSTRAINT strategies_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES public.strategies(id);
--
-- Name: strategies strategies_validated_backtest_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.strategies
    ADD CONSTRAINT strategies_validated_backtest_id_fkey FOREIGN KEY (validated_backtest_id) REFERENCES public.backtests(id);
--
-- Name: trades trades_order_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.trades
    ADD CONSTRAINT trades_order_id_fkey FOREIGN KEY (order_id) REFERENCES public.orders(order_id);
--
-- Name: user_roles user_roles_role_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_role_id_fkey FOREIGN KEY (role_id) REFERENCES public.roles(id);
--
-- Name: user_roles user_roles_user_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_roles
    ADD CONSTRAINT user_roles_user_id_fkey FOREIGN KEY (user_id) REFERENCES public.users(user_id);
--
-- Name: user_subscriptions user_subscriptions_plan_id_fkey; Type: FK CONSTRAINT; Schema: public; Owner: -
--
ALTER TABLE ONLY public.user_subscriptions
    ADD CONSTRAINT user_subscriptions_plan_id_fkey FOREIGN KEY (plan_id) REFERENCES public.subscription_plans(id);
--
-- PostgreSQL database dump complete
--

--
-- Seed data: initial admin user (see comments above)
--
--
-- Seed data: initial admin user
-- Username: admin
-- Email: admin@quantmind.local
-- Password: bcrypt-hashed (login and change after first deploy)
-- Note: api_key / ai_ide_api_key are intentionally NULL for security.
--

INSERT INTO public.users (id, user_id, tenant_id, username, email, phone_number, password_hash, is_active, is_verified, is_admin, is_locked, last_login_at, last_login_ip, login_count, created_at, updated_at, is_deleted, deleted_at)
VALUES (10000001, '10000001', 'default', 'admin', 'admin@quantmind.local', NULL, '$2b$12$B/yjK9cT.wx4BlB9j.r/t.dADjCbmutIXoDM7PdKZmV6ypuYiiUvW', true, true, true, false, NULL, NULL, 0, now(), now(), false, NULL);

INSERT INTO public.user_profiles (id, user_id, tenant_id, nickname, avatar_url, bio, preferences, created_at, updated_at, display_name, location, website, phone, trading_experience, risk_tolerance, investment_goal, github_url, twitter_handle, linkedin_url, notification_settings, ai_ide_api_key, api_key)
VALUES (34, '10000001', 'default', 'Administrator', 'data/uploads/default_avatar.png', NULL, '{}'::jsonb, now(), now(), 'Administrator', NULL, NULL, NULL, 'intermediate', 'medium', NULL, NULL, NULL, NULL, '{}'::jsonb, NULL, NULL);

-- Bump sequences past the seeded IDs to avoid future insert collisions
SELECT pg_catalog.setval('public.users_id_seq', GREATEST(10000001, (SELECT COALESCE(MAX(id), 0) FROM public.users)), true);
SELECT pg_catalog.setval('public.user_profiles_id_seq', GREATEST(34, (SELECT COALESCE(MAX(id), 0) FROM public.user_profiles)), true);
