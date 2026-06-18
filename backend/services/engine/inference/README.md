# Inference Module

本模块负责生产模型加载与推理，默认目录为 `models/production`。

## 修复记录（2026-05-29，训练-推理强一致契约）

- 推理链路新增 `inference_contract.json` 强校验（模型目录内）：
  - 缺失契约直接失败（历史模型需重训后可推理）；
  - 执行脚本前校验冻结参数（`feature_columns/fill_values/best_iteration/target_horizon_days`）；
  - 对请求 `trade_date` 计算当日数据快照，与契约中的按日清单哈希比对，不一致立即失败。
- 推理结果新增一致性审计字段：`contract_version/contract_hash/manifest_hash/precheck_passed/mismatch_type/mismatch_detail`。
- `inference_parquet.py` 与训练端预处理改为复用共享实现 `backend/shared/feature_preprocess.py`，消除双实现漂移。

## 修复记录（2026-05-25，实时推理 factor 字段映射）

- 问题：`DataAdapter` 曾将实时输入中的 `amount` 映射为 `factor`，导致未显式提供复权因子时，`factor` 可能被错误金额污染。
- 修复：
  - 字段映射改为 `amount -> amount`，并支持 `adj_factor/adjustment_factor/hfq_factor -> factor`；
  - 实时数据未提供 `factor` 时默认回填为 `1.0`；
  - `prepare_features()` 对 `factor<=0` 或缺失值统一回填 `1.0`，避免进入模型的 `factor` 出现 0/负数/空值。
- 影响：实时推理链路不再把成交额误当复权因子，`factor` 字段语义与训练口径保持一致。

## 修复记录（2026-04-26，parquet 推理停牌过滤）

- 问题：平台托管的 parquet `inference.py` 模板会对目标交易日的全量特征行直接打分并排序，未剔除 `volume <= 0` 的不可交易记录；一旦特征快照仍保留停牌股票行，停牌股会直接进入 `engine_signal_scores` 排名。
- 修复：
  - parquet 模板新增不可交易过滤，默认剔除 `close <= 0` 或 `volume <= 0` 的记录；
  - `InferenceScriptRunner` 在识别到平台托管的 parquet 模板时，会在执行前自动同步最新模板，避免旧模板长期残留在用户模型目录；
  - 主模型批次的 `active_data_source` 改为记录真实 parquet 目录，而不是固定写成 qlib 路径。
- 影响：像 `SH600735` 这类在特征快照中仍有行但成交量为 0 的停牌标的，不再进入最新推理榜单。

## 修复记录（2026-04-27，推理批次近30天方向正确率）

- 用户态手动推理与 Celery 自动推理在成功完成后，都会同步计算当前模型的“近30天方向正确率”并回写到 `qm_model_inference_runs.result_json.recent_direction_accuracy`。
- 计算口径保持轻量：
  - 每个历史批次取 Top20 `engine_signal_scores`；
  - 用 `prediction_trade_date` 到下一交易日的收盘方向做真值；
  - 若 Top20 中上涨标的占比超过一半，则该日记为方向正确。
- 目的：为模型推理页面直接提供可解释的近期质量指标，替换原先只反映链路状态的“是否兜底”摘要位。

## 修复记录（2026-04-20，同日多模型推理结果隔离）

- 问题：`InferenceScriptRunner` 写库前会按 `tenant_id + user_id + trade_date + model_version='inference_script'`
  清理旧数据，导致“同一用户同一交易日切换模型再次推理”时，先前模型的信号被误删。
- 修复：写库覆盖粒度从“按日期”改为“按日期 + 模型桶”。
  - 新增模型桶特征版本：`feature_version = script_v1_<model_bucket>`（`model_bucket` 来源于实际生效模型 ID 的规范化值）。
  - `engine_signal_scores` 与 `engine_feature_runs` 的清理语句均增加 `feature_version` 条件。
  - 写入 `engine_feature_runs` 时同步记录 `model_name=<active_model_id>`，便于审计。
- 兼容性：`model_version` 仍保持 `inference_script`，不影响现有按 `model_version` 的查询接口。
- 结果：同日不同模型推理可并存，历史 `run_id` 详情不再因后续推理被清空。

## 修复记录（2026-03-27，自动推理 Celery 任务）

- 修复 `InferenceScriptRunner._query_dimension_readiness` 中 `SessionLocal` 未定义导致的自动任务异常：
  `NameError: name 'SessionLocal' is not defined`。
- 处理方式：在该函数内与写库链路一致，显式基于 `DATABASE_URL` 构造同步 SQLAlchemy 会话（`+asyncpg -> +psycopg2`），仅用于就绪度查询。
- 影响：`engine.tasks.auto_inference_if_needed` 不再因该异常中断，可继续执行日常自动推理流程。

## 修复记录（2026-04-14，自动推理模型解析同步化）

- 自动推理 Celery 路径与 `run_daily_inference_script()` 已统一改为同步模型解析：
  `ModelRegistryService.resolve_effective_model_sync()`。
- 处理方式：在 Celery worker 场景下不再通过 `asyncio.run()` 调用 async 模型解析，避免
  `Task ... got Future ... attached to a different loop`。
- 影响：自动推理在解析策略绑定/用户默认模型时更稳定，推理成功后才会继续写入
  `qm:signal:latest:{tenant_id}:{user_id}` 并触发交易侧托管任务。

## 修复记录（2026-04-16，自动推理来源改为默认模型）

- Celery 自动推理 `engine.tasks.auto_inference_if_needed` 不再按活跃策略逐个带 `strategy_id` 执行。
- 新逻辑改为：
  1. 扫描 `qm_model_inference_settings.enabled = true`
  2. 仅执行 `next_run_at` 已到、且设置中的 `model_id` 仍然是当前默认模型的记录
  3. 执行 `run_daily_inference_script()` 时不传 `strategy_id`，统一沿默认模型链路解析
- 影响：
  - 自动推理产出的 `model_source` 不再被 `strategy_binding` 覆盖；
  - 交易侧自动托管可稳定消费自动推理生成的最新批次；
  - Celery 自动推理结果会同步回写 `qm_model_inference_runs / qm_model_inference_settings.last_run_json`，与用户态推理历史和“自动推理”面板对齐。

## 修复记录（2026-04-16，自动推理改为凌晨排队生成次日批次）

- 自动推理默认计划时间已调整为 `00:00`，目标是“交易日当天 00:00 起进入任务队列，08:00 前等待 146 维数据更新完成，再生成当日可消费批次”。
- Beat 扫描窗口已调整为“交易日当天 00:00-08:00 前”，不再采用收盘后窗口。
- 自动批次失败时，设置中的 `next_run_at` 会推进到下一次 Beat 扫描时点，以便在 08:00 前继续补跑；成功后才推进到下一交易日的凌晨计划时间。

## 修复记录（2026-06-18，自动推理统一到 04:00）

- 用户态自动推理默认 `schedule_time` 已统一为 `04:00`；遗留的 `00:00 / 09:30 / 15:30` 会在读取或执行设置时自动迁移到 `04:00`。
- `engine.tasks.auto_inference_if_needed` 现在会先校验“当天是否为交易日”，确认无误后才继续使用上一交易日数据执行 04:00 自动推理。
- 自动推理 Celery 扫描已改为只消费 `next_run_at` 已到的启用设置，不再把所有 `enabled=true` 的模型全部触发。
- Celery 自动推理结果会同步回写 `qm_model_inference_settings.last_run_json / next_run_at`，前端“自动调度”面板看到的最近执行状态会与后台一致。

## 修复记录（2026-06-18，自动推理升级为独立子任务队列）

- `engine.tasks.auto_inference_if_needed` 现仅负责在交易日 `04:00` 扫描并分发任务，不再在同一个 Celery task 内串行执行全部模型。
- 每条到期自动推理会拆分为独立子任务 `engine.tasks.run_auto_inference_task`，逐条进入 Celery 队列，由 worker 分别执行。
- 子任务会独立完成“重复执行检查、分布式锁、推理、settings 回写、dispatch log 更新”，因此单个慢任务或失败任务不会卡住整批扫描。

## 每日信号生成统一流程（2026-03）

“明日信号生成”支持三种触发方式：

- 管理端手动触发：`POST /api/v1/admin/models/run-inference`
- 策略激活后异步触发：`POST /api/v1/strategies/{strategy_id}/activate` 成功后，按当前 `tenant_id + user_id(8位)` 触发一次推理并发布到对应 Signal Stream
- 自动兜底触发：Celery Beat 定时扫描 `engine.tasks.auto_inference_if_needed`

执行器统一为 `InferenceRouterService + InferenceScriptRunner`，并支持用户模型动态解析：

1. 解析优先级：`显式 model_id > 策略绑定 > 用户默认 > model_qlib > alpha158`
2. 用户模型目录：`models/users/{tenant}/{user}/{model_id}`
3. 系统主模型目录：`models/production/model_qlib`，主数据源：`db/qlib_data`
4. 系统兜底模型目录：`models/production/alpha158`，兜底数据源：`db/qlib_data`
5. 用户模型失败时会回落系统链路，保持 `model_qlib -> alpha158` 兜底
4. 结果统一写入 `engine_feature_runs/engine_signal_scores` 并写 Redis 完成标记 `qm:inference:completed:{prediction_trade_date}`
5. 同步写入最新可消费版本键 `qm:signal:latest:{tenant_id}:{user_id}`，交易侧 runner 会据此丢弃旧 `run_id` 的过期信号，确保只消费最新推理结果
5. 脚本写库链路在 `DATABASE_URL` 为 `postgresql+asyncpg://...` 时，会自动转换为同步驱动 `postgresql+psycopg2://...` 执行写入，避免 `greenlet_spawn` 异常

统一日期口径：

- `data_trade_date`：推理输入数据交易日（用于维度门禁与脚本 `--date`）
- `prediction_trade_date`：信号生效交易日，固定取 `data_trade_date` 的下一交易日（明日）
- `engine_signal_scores.trade_date` 与 `engine_feature_runs.trade_date` 均写入 `prediction_trade_date`
- `qm:signal:latest:{tenant_id}:{user_id}` 保存最近一次可消费的 `run_id`，runner 只接受该版本对应的 stream 消息
- 激活触发、管理员触发、Celery 触发三条链路统一使用上述口径；其中 Celery 自动推理固定沿“默认模型”链路生成最新可消费版本。
- parquet 推理模板在读取 `metadata.fill_values` 时会把 `NaN/Inf/null` 统一兜底为 `0.0`，避免训练产物里个别缺失填充值影响推理预处理。

数据保留策略：

- `inference_script` 写库链路默认只保留最近 30 天预测数据；
- 可通过 `INFERENCE_PREDICTION_RETENTION_DAYS` 调整保留窗口。

维度与覆盖门禁说明：

- 期望特征维度动态解析优先级：
  1. `metadata.json` 的 `feature_count/feature_dim/input_dim/feature_columns/input_spec.tensor_shape`
  2. `feature_schema.json` 的 `features/feature_columns/columns`
  3. 推理脚本注释中的“XX 特征”
  4. `INFERENCE_DEFAULT_FEATURE_DIM`（默认 48）
- 覆盖阈值采用自适应规则（不再固定“>=3000”）：
  - `INFERENCE_MIN_READY_SYMBOLS`（默认 3000）
  - `INFERENCE_MIN_READY_RATIO`（默认 0.9）
  - `INFERENCE_MIN_READY_FLOOR`（默认 100）

统一返回元信息（四条入口一致）：

- `fallback_used`
- `fallback_reason`
- `active_model_id`
- `effective_model_id`
- `model_source`
- `active_data_source`

其中 `/api/v1/inference/predict` 与 `/api/v1/pipeline/runs/{run_id}/result` 均可直接消费这些字段；
`pipeline` 同时会将上述字段落盘到 `result_json` 顶层，便于审计与追踪。

## 已支持模型

- LightGBM（`framework=lightgbm`）
- PyTorch / TFT（`framework=pytorch`）

## TFT Native 接入规范

`tft_native` 模型目录建议包含：

- `metadata.json`（基础信息）
- `inference_metadata.json`（推理专用信息，包含 `framework/model_file/input_spec`）
- `model_best.pth`（state_dict）

加载顺序：

1. 读取 `metadata.json`
2. 若存在 `inference_metadata.json`，补齐缺失键
3. 按 `framework` 选择加载器

### 序列输入格式

当 `input_spec.tensor_shape` 为 3 维（如 `[null, 30, 54]`）时，推理服务按序列模型处理。

支持两种输入：

- 直接序列：`{"sequences": [[[...]]], "symbols": ["SZ000001"]}`
- 行数据：包含 `instrument/symbol` + `date/datetime` + 特征列，系统按标的分组后自动组装最近 `lookback_window` 天序列

输出：

- `predictions`: 每个序列对应一个标量分数
- `symbols`: 与输入序列一一对应

## 运行依赖

若使用 `framework=pytorch`，运行环境必须安装 `torch`。
