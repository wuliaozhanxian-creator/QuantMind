# PostgreSQL Scripts

## Master Initialization

- `master-init.sh` initializes the primary database and installs required extensions.

## Stock Screener Snapshot

- `stock_screener_snapshot.sql` defines the `stock_screener_snapshot` table and indexes.
- `load_stock_screener_snapshot.py` loads the latest 31 days from `db/stock_new.duckdb` into PostgreSQL.
- `.env` is loaded first, and explicit environment variables override it.
- Derived fields include MA20, BOLL, MACD, KDJ, 20-day volatility, and daily amount rank.
- Symbols are normalized to 6-digit codes during load to align tables.
- Indicators are computed using an extended window, but only the latest N days are stored.
- Adjust factors use the latest available <= snapshot_date, falling back to adjusted price data when needed.
- Numeric fields are rounded to 2 decimals; PE/PB fields are rounded to integers.

Example:

```bash
python scripts/postgres/load_stock_screener_snapshot.py --as-of-date 2026-01-31 --days 31
```

## Stock Daily (Unified)

- `stock_daily.sql` defines the partitioned `stock_daily` table and indexes (range partition by `trade_date`, monthly partitions).
- `load_stock_daily.py` loads daily OHLCV + valuation fields from `db/stock_new.duckdb` and precomputes indicators via pandas/numpy (no external TA lib).
- Indicator lookback uses the last 260 trading days before `start-date` (fallbacks to 400 calendar days if data is sparse) to ensure end-date signals are computed with sufficient history.
- `.env` is loaded first, and explicit environment variables override it.
- `ts_code` is normalized to a 6-digit code when loading (no exchange suffix).
- `total_mv` is stored in **亿元** (converted from source market value), `amount` is stored in **万元**.
- Indicators include MA/EMA/MACD/BOLL/RSI/KDJ/Stoch/CCI/Williams %R/ATR/OBV/VWAP and helper boolean flags.

Example:

```bash
python scripts/postgres/load_stock_daily.py --start-date 2025-12-01 --end-date 2025-12-31
```

## Research Candidate Incremental Import

- [scripts/postgres/research_candidate_incremental.sql](/Users/qusong/git/quantmind/scripts/postgres/research_candidate_incremental.sql) 定义投研平台候选快照表 `qm_research_candidate_snapshot` 及增量导入函数 `qm_import_research_candidate_snapshot(...)`。
  - 2026-04-30：函数末尾新增 `concept_tags` 兜底回填逻辑。若导入源未提供概念标签，会优先按 `industry_classification.stock_codes` 映射填充；仍为空时回退使用 `industry` 作为单标签，避免前端概念筛选全空。
- 导入源为 `qm_model_inference_runs + engine_signal_scores`，优先联接 `stock_screener_snapshot`，其次 `stock_selection`；若两者都未落地，则自动回退到 `stock_daily_latest`。
- 完整字段版会同步沉淀 `continued_rise_days / ma5 / ma10 / ma20 / return_5d / return_10d / amount_rank / province / city` 等研究字段，适合直接给投研平台做排序、过滤和详情展示。

示例：

```sql
\i scripts/postgres/research_candidate_incremental.sql
SELECT * FROM qm_import_research_candidate_snapshot();
SELECT * FROM qm_import_research_candidate_snapshot(DATE '2026-04-30', NULL, NULL, TRUE);
```
