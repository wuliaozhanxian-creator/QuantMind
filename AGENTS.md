# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

QuantMind is a quantitative trading platform with Python backend (FastAPI) and Electron/React/TypeScript frontend. The OSS edition uses single-container deployment where all backend services run in one container.

## Backend Services (all via `backend/main_oss.py`)

| Service | Port | Responsibility |
|---------|------|----------------|
| api | 8000 | User auth, strategy management, community |
| engine | 8001 | Qlib backtesting, AI strategy generation, model inference |
| trade | 8002 | Order management, positions, risk control |
| stream | 8003 | Real-time quotes, WebSocket push |

## Commands

### Backend
```bash
# Start all services (Docker)
docker-compose up -d

# Run single service locally
SERVICE_MODE=api python backend/main_oss.py

# Tests (run from project root)
python backend/run_tests.py unit        # Unit tests
python backend/run_tests.py integration # Integration tests
python backend/run_tests.py all         # All tests
python backend/run_tests.py trade-long-short  # QMT MVP chain tests

# Lint/format
ruff check backend/
ruff format backend/
```

### Frontend (Electron app in `electron/`)
```bash
npm install              # Install dependencies
npm run dev              # Development (Electron desktop)
npm run dev:web          # Development (Web browser)
npm run typecheck        # Type check
npm run dashboard:build  # Production build
```

## Architecture Notes

- **Feature engineering**: 48-dim features written to `market_data_daily` table by external service
- **Trade service**: Enforces "local-first" order persistence before external submission (three-stage architecture: local-persist → broker-submit → account-sync)
- **Redis DB allocation**: 0=general, 1=auth, 2=trade, 3=market, 4=backtest, 5=cache
- **Shared modules**: `backend/shared/` contains cross-service code (DB manager, Redis client, config, logging)
- **Strategy storage**: `backend/shared/strategy_storage.py` is the single entry point for all strategy CRUD operations
- **Readiness probes**: `/health` (liveness, always 200) vs `/readiness` (checks DB+Redis, 503 if down) — see `backend/shared/readiness.py`
- **Structured logging**: `backend/shared/logging_config.py` injects `service_name` (api/engine/trade/stream) into JSON logs

## Service JWT Authentication (M2 Security)

**认证体系：用户 JWT + Service JWT 双轨制。**

- **用户 JWT**: `backend/shared/auth.py` — `create_access_token` / `verify_token`，用于前端用户认证
- **Service JWT**: `backend/shared/auth.py` — `create_service_token`，用于服务间内部调用认证
- **认证流程图**: `T6.5_service_jwt_flow.md` — 含 15 处调用点矩阵 + 迁移路径
- **Hard constraint**: `INTERNAL_CALL_SECRET` 未配置时 fail-fast，不得降级
- **迁移状态**: Phase 1 已完成（死代码清理 + flow doc），Phase 2-4 计划于 M3 执行

## Order Chain Architecture (M2 Robustness)

**"本地优先"持久化原则：本地写成功才允许外部提交。**

- **三阶段架构** (`backend/services/trade/services/trading_engine.py`):
  1. **阶段1 本地持久化**: `transition_order_status(SUBMITTED)` 落库，commit 失败时 6 字段内存回滚
  2. **阶段2 外部提交**: `_execute_via_broker()`，防御性断言确保 stage1 成功才进入
  3. **阶段3 账户同步**: `_sync_account_to_redis()`，仅非 REJECTED 时执行
- **三层对账扫描** (`backend/services/trade/services/order_timeout_scanner.py`):
  - 5s 短超时: 标记 `[BRIDGE_ACK_TIMEOUT_PENDING_REVIEW]`
  - 60s 对账入队: 标记 `[RECONCILE_QUEUED]` + 记录线索
  - 300s 长超时: 标记 EXPIRED（排除已入队订单）
- **端到端测试**: `backend/tests/test_order_link_robustness.py` — 9 个测试覆盖 4 场景

## Data Source Boundaries (CRITICAL)

数据读取有明确边界，不同模块使用不同数据源：

### 本地数据库 (Local PostgreSQL)
- **表**: `stock_daily_latest`
- **用途**: 智能策略回测、模型训练特征
- **配置**: `backend/shared/db_manager.py` (环境变量 `DB_*`)
- **注意**: 本地数据需要通过 ETL 服务同步维护

### 远程行情服务器 (Remote Market Server - 106.53.100.144)
- **PostgreSQL**: 已废弃（M2 安全加固后 `market_db_manager.py` 改为读取容器内本地 PostgreSQL，远程 PG 仅 Redis 在用）
- **Redis**: `readonly_monitor` 用户 (只读)
- **用途**: 实盘行情推送（远程 PostgreSQL 已由本地库替代）
- **配置文件**:
  - PostgreSQL: `backend/shared/market_db_manager.py`（已改为本地 PG，DB_* 环境变量）
  - Redis: `backend/shared/remote_redis_client.py`
- **连接方式**: 密码通过环境变量注入，运行时读取（M2 安全加固后，Fernet 硬编码加密已废弃）
- **T5.1 只读凭据隔离**:
  - 远程 PG 只读用户白名单：`quantmind_market` / `readonly_monitor` / `quantmind_readonly`（若未来恢复远程 PG 连接，强制要求只读用户）
  - 远程 Redis 只读用户白名单：`readonly_monitor` / `quantmind_readonly`（配置用户名时强制校验，使用读写凭据将 fail-fast）
  - 硬约束：远程行情库仅允许只读访问，禁止 INSERT/UPDATE/DELETE 写入操作

### 密钥管理 (CRITICAL - M2 安全加固)

**强制约束：所有密钥/密码必须通过环境变量注入，严禁硬编码。**

- **环境变量清单**（`.env` 文件配置，`.env.example` 为模板）:
  - `SECRET_KEY` — 应用密钥（空默认值，未配置时 fail-fast 抛 RuntimeError）
  - `JWT_SECRET_KEY` — JWT 签名密钥（空默认值，未配置时 fail-fast）
  - `DB_PASSWORD` — 本地数据库密码
  - `INTERNAL_CALL_SECRET` — 内部服务调用密钥（空默认值，未配置时 fail-fast）
  - `REMOTE_MARKET_DB_HOST/PORT/USER/PASSWORD` — 远程行情 DB 连接
  - `REMOTE_QUOTE_REDIS_HOST/PORT/USER/PASSWORD` — 远程行情 Redis 连接
- **Fernet 加密**: `backend/shared/encryption.py` 已标记 deprecated，新代码不得使用
- **Fail-fast 原则**: 所有序列化密钥在未配置时抛 `RuntimeError`，不得使用默认值降级

## Stock Code Standardization (CRITICAL)

**强制约束：股票代码统一使用 `SH600000` 大写前缀格式，严禁使用 `600000.SH` 后缀格式。**

- **Mandatory Format**: Prefix-based (e.g., `SH600036`). **All internal Redis keys, database fields, and API parameters MUST use this format.**
- **Forbidden Format**: Suffix-based (e.g., `600036.SH`). **Do NOT use this format in any new code or configuration.**
- **Normalization Utilities**: 
  - **Backend**: `backend/shared/stock_utils.py` -> `StockCodeUtil.to_prefix(code)`
  - **Frontend**: `electron/src/utils/portfolioUtils.ts` -> `normalizeStockCode(code)`
- **Redis Key Patterns**:
  - Snapshot: `market:snapshot:sh600036` (lowercase prefix for snapshot keys)
  - SDL Cache: `sdl:2026-04-30:SH600036` (uppercase prefix for SDL keys)
  - Series: `market:series:SH600036` (uppercase prefix for sequences)
- **Market Auto-Identification**:
  - `SH`: 6xxxxx, 9xxxxx
  - `SZ`: 0xxxxx, 3xxxxx, 2xxxxx
  - `BJ`: 4xxxxx, 8xxxxx

## Environment

Required `.env` keys (defaults in `docker-compose.yml`):
- `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`
- `REDIS_HOST`, `REDIS_PORT`
- `SECRET_KEY`, `JWT_SECRET_KEY`
- `STORAGE_MODE=local` for OSS edition

## Code Style

- Python: Line length 88, use ruff for linting/formatting
- TypeScript: Run `npm run typecheck` before committing frontend changes

## Development Constraints (CRITICAL)

- **Single-Machine Deployment**: The current project is a standalone/single-machine project. After completing code modifications, always promptly restart or rebuild the Docker container to apply changes.
- **Database Access Restriction**: Aside from the project's own internal database, the system ONLY supports reading market data. **Do NOT connect to or modify any external databases.**
- **Service Name Standardization**: Always standardize and strictly follow the service names defined above (`api`, `engine`, `trade`, `stream`).

## Deployment Workflow

After making code changes, always:
1. **Commit to git**: Create a commit with descriptive message
2. **Deploy to server**: SSH to `quant-server` and pull/deploy updates

```bash
# Local: commit changes
git add .
git commit -m "descriptive message"

# Deploy to quant-server
ssh quant-server "cd /opt/quantmind && git pull && docker-compose restart"
```

## CI/CD (M2 Quality)

- **Pipeline**: `.github/workflows/ci.yml` — 3 jobs (typecheck / docker-compose validate / ruff advisory)
- **Frontend gate**: `npm run typecheck` must pass before merge
- **Backend lint**: `ruff check backend/` — advisory mode (M2 预存 1166 个 error，计划 M3 末清债，advisory 过期 2026-07-25)
- **Docker validation**: `docker-compose config` validates compose file syntax

## Key Files

- `backend/main_oss.py` - Unified entry point for all backend services
- `backend/run_tests.py` - Test runner with multiple modes
- `backend/shared/` - Shared modules across services
- `docker-compose.yml` - Local deployment configuration
- `backend/shared/readiness.py` - Readiness probe utility (2s timeout, async/sync)
- `backend/shared/auth.py` - Authentication (user JWT + service JWT)
- `backend/shared/logging_config.py` - Structured JSON logging with service_name
- `backend/services/trade/services/trading_engine.py` - Order chain three-stage architecture
- `T6.5_service_jwt_flow.md` - Service JWT migration flow diagram
- `.env.example` - Environment variable template
