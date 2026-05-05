# QuantMind 数据文件使用说明

本目录包含 QuantMind 投研平台所需的全部离线数据文件，部署前请按以下说明完成数据准备。

---

## 目录结构

```
db/
├── feature_snapshots/    # 年度特征快照（2016-2026）
├── qlib_data/            # Qlib 回测数据（日历/因子/标的）
├── backups/              # 数据库备份
└── README.md
```

---

## 1. feature_snapshots/ — 特征快照

**内容**：2016-2026 年每年的模型特征 Parquet 文件 + 元数据 JSON，共 152 维特征（量价、因子、资金流、微观结构等）。

**使用方式**：将整个 `feature_snapshots/` 目录覆盖到项目 `db/` 目录下：

```bash
cp -r feature_snapshots/* /opt/quantmind/db/feature_snapshots/
```

> 无需额外操作，后端引擎服务启动时会自动扫描该目录加载特征数据。

---

## 2. qlib_data/ — Qlib 回测数据

**内容**：

| 子目录 | 说明 |
|--------|------|
| `calendars/day.txt` | A 股交易日历 |
| `instruments/all.txt` | 全部股票列表 |
| `instruments/csi300.txt` | 沪深 300 成分股 |
| `instruments/csi500.txt` | 中证 500 成分股 |
| `instruments/csi1000.txt` | 中证 1000 成分股 |
| `features/` | ~5500 只股票的日频因子数据（按 `shXXXXXX/` 目录组织） |

**使用方式**：将整个 `qlib_data/` 目录覆盖到项目 `db/` 目录下：

```bash
cp -r qlib_data/* /opt/quantmind/db/qlib_data/
```

> 无需额外操作，Qlib 回测引擎启动时会加载该目录。

---

## 3. backups/ — 数据库备份（需手动导入）

### 3.1 stock_daily_latest 表备份

- **文件**：`stock_daily_latest_20260505.csv.gz`（约 162 MB，解压后约 383 MB，含 ~446,677 行）
- **说明**：`stock_daily_latest` 表是智能策略和投研平台查询的核心数据表，包含全市场股票最新交易日的行情、技术指标、估值、行业标签等字段。
- **重要**：若未导入此表数据，智能策略（AI 选股）和投研平台功能将无法正常使用。

### 3.2 导入步骤

#### 前置条件

确保 Docker 容器已启动且数据库可访问：

```bash
docker-compose up -d db
```

#### 步骤 1：拷贝备份文件到容器

```bash
docker cp backups/stock_daily_latest_20260505.csv.gz quantmind-db:/tmp/
```

#### 步骤 2：解压文件

```bash
docker exec quantmind-db sh -c "gunzip -f /tmp/stock_daily_latest_20260505.csv.gz"
```

#### 步骤 3：确保目标表已创建

```bash
docker exec quantmind-db psql -U quantmind -d quantmind -c "SELECT 1 FROM stock_daily_latest LIMIT 1" 2>/dev/null
```

如果表不存在（报错 `relation "stock_daily_latest" does not exist`），先导入数据库初始化脚本：

```bash
docker cp /opt/quantmind/data/quantmind_init.sql quantmind-db:/tmp/
docker exec quantmind-db psql -U quantmind -d quantmind -f /tmp/quantmind_init.sql
```

#### 步骤 4：导入 CSV 数据

```bash
docker exec quantmind-db psql -U quantmind -d quantmind -c "\COPY stock_daily_latest FROM '/tmp/stock_daily_latest_20260505.csv' WITH (FORMAT csv, HEADER true, NULL '')"
```

#### 步骤 5：验证导入

```bash
docker exec quantmind-db psql -U quantmind -d quantmind -c "SELECT COUNT(*) AS total_rows, MAX(trade_date) AS latest_date, COUNT(DISTINCT symbol) AS symbols FROM stock_daily_latest"
```

预期输出类似：
```
 total_rows | latest_date | symbols
------------|-------------|---------
     446677 | 2026-05-05  |    5331
```

#### 步骤 6：重启服务使缓存生效

```bash
docker-compose restart quantmind
```

---

## 数据依赖关系

```
feature_snapshots/ ──→ 引擎层（特征计算、模型推理）
qlib_data/         ──→ Qlib 回测引擎
stock_daily_latest ──→ AI 智能策略、投研平台查询
```

三者缺一不可，否则对应功能模块将无法启动或返回空数据。
