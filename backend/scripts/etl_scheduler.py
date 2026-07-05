"""ETL 自动化调度框架 (T5.3)

职责：
1. 统一调度入口：支持 cron 式定时触发（5 字段 unix cron）
2. 任务注册：每个 ETL 脚本注册为独立任务（subprocess 或 callable）
3. 依赖管理：任务间依赖关系，按拓扑序执行
4. 状态追踪：记录每次执行状态（成功/失败/进行中）+ 耗时，持久化到本地 JSON

设计约束：
- 仅依赖 Python 标准库 + 项目内已有依赖（避免引入 APScheduler 等额外包）
- 状态持久化到本地 JSON 文件（不连接外部数据库，符合 T5.3 约束）
- 与现有 ETL 脚本兼容：默认通过 subprocess 调用，保留 callable 模式用于测试
- 线程安全：调度循环在后台线程运行，主线程可注册/查询任务

使用示例：
    from backend.scripts.etl_scheduler import ETLScheduler, TaskSpec

    scheduler = ETLScheduler(state_path="logs/etl_state.json")
    scheduler.register(TaskSpec(
        name="sync_official_data",
        cron="0 18 * * 1-5",  # 每个交易日 18:00
        command=["python", "backend/scripts/sync_official_data_update.py"],
    ))
    scheduler.start()  # 启动后台调度线程
"""

from __future__ import annotations

import json
import logging
import os
import subprocess
import sys
import threading
import time
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Optional

logger = logging.getLogger(__name__)

# 项目根目录，用于解析相对路径的状态文件
_PROJECT_ROOT = Path(__file__).resolve().parents[2]

# 状态枚举
STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_SUCCESS = "success"
STATUS_FAILED = "failed"
STATUS_SKIPPED = "skipped"

# 单次任务最大执行时长（秒），防止僵死
_DEFAULT_TASK_TIMEOUT = int(os.getenv("ETL_TASK_TIMEOUT_SECONDS", "3600"))
# 调度器轮询间隔（秒）
_POLL_INTERVAL = int(os.getenv("ETL_POLL_INTERVAL_SECONDS", "30"))
# 状态文件保留的最近执行记录数
_MAX_HISTORY_PER_TASK = int(os.getenv("ETL_MAX_HISTORY", "50"))

# ============================================================
# Cron 表达式解析（5 字段 unix cron：分 时 日 月 周）
# ============================================================
@dataclass(frozen=True)
class CronSchedule:
    """解析后的 cron 调度，提供 match(datetime) -> bool

    day_of_week 内部统一采用 Python weekday() 约定：0=Monday ... 6=Sunday。
    解析 cron 表达式时会把 cron 标准（0=Sunday, 1=Monday ... 7=Sunday）
    自动转换为 Python weekday()，使 "1-5" 正确表示"周一到周五"。
    """

    minute: set[int]
    hour: set[int]
    day_of_month: set[int]
    month: set[int]
    day_of_week: set[int]  # Python weekday(): 0=Monday ... 6=Sunday

    @staticmethod
    def parse(expr: str) -> "CronSchedule":
        fields = str(expr or "").split()
        if len(fields) != 5:
            raise ValueError(
                f"cron 表达式必须是 5 字段: minute hour day month weekday, got: {expr!r}"
            )
        minute, hour, dom, month, dow = fields
        # cron dow 允许 0-7（0 和 7 都是 Sunday），解析后转为 Python weekday()
        cron_dow = _parse_cron_field(dow, 0, 7)
        python_dow = {(v + 6) % 7 for v in cron_dow}  # cron0/7->6(Sun), cron1->0(Mon)
        return CronSchedule(
            minute=_parse_cron_field(minute, 0, 59),
            hour=_parse_cron_field(hour, 0, 23),
            day_of_month=_parse_cron_field(dom, 1, 31),
            month=_parse_cron_field(month, 1, 12),
            day_of_week=python_dow,
        )

    def matches(self, dt: datetime) -> bool:
        weekday = dt.weekday()  # Monday=0 ... Sunday=6
        return (
            dt.minute in self.minute
            and dt.hour in self.hour
            and dt.day in self.day_of_month
            and dt.month in self.month
            and weekday in self.day_of_week
        )

def _parse_cron_field(expr: str, min_val: int, max_val: int) -> set[int]:
    """解析单个 cron 字段，支持 *, 数字, 逗号, 范围(1-5), 步长(*/2, 1-10/2)"""
    result: set[int] = set()
    for part in str(expr).split(","):
        part = part.strip()
        if not part:
            continue
        step = 1
        if "/" in part:
            base, step_str = part.split("/", 1)
            try:
                step = int(step_str)
            except ValueError as exc:
                raise ValueError(f"非法 cron 步长: {part!r}") from exc
            if step <= 0:
                raise ValueError(f"cron 步长必须 > 0: {part!r}")
        else:
            base = part

        if base == "*":
            lo, hi = min_val, max_val
        elif "-" in base:
            lo_s, hi_s = base.split("-", 1)
            lo, hi = int(lo_s), int(hi_s)
        else:
            lo = hi = int(base)

        if lo < min_val or hi > max_val or lo > hi:
            raise ValueError(
                f"cron 字段越界: {expr!r} (允许 {min_val}-{max_val})"
            )
        result.update(range(lo, hi + 1, step))
    if not result:
        raise ValueError(f"cron 字段解析为空: {expr!r}")
    return result

# ============================================================
# 任务定义与执行记录
# ============================================================
@dataclass
class TaskSpec:
    """ETL 任务规格

    command 与 callable 二选一：
    - command: subprocess 命令列表，如 ["python", "backend/scripts/xxx.py"]
    - callable: 可调用对象，签名为 () -> dict，返回执行结果（用于测试或内置任务）
    """

    name: str
    cron: Optional[str] = None  # None 表示仅手动触发
    command: Optional[list[str]] = None
    callable: Optional[Callable[[], dict]] = None
    depends_on: list[str] = field(default_factory=list)
    timeout_seconds: int = _DEFAULT_TASK_TIMEOUT
    enabled: bool = True
    description: str = ""

    def __post_init__(self):
        if not self.name or not str(self.name).strip():
            raise ValueError("TaskSpec.name 不能为空")
        if self.command is None and self.callable is None:
            raise ValueError(f"任务 {self.name} 必须指定 command 或 callable")
        if self.cron:
            # 预校验 cron 表达式
            CronSchedule.parse(self.cron)
        # 依赖自检：不允许自依赖
        if self.name in self.depends_on:
            raise ValueError(f"任务 {self.name} 不能依赖自身")

@dataclass
class TaskRunRecord:
    """单次任务执行记录"""

    task_name: str
    status: str
    started_at: str  # ISO8601 UTC
    finished_at: Optional[str] = None
    duration_seconds: Optional[float] = None
    return_code: Optional[int] = None
    output: Optional[str] = None
    error: Optional[str] = None
    triggered_by: str = "cron"  # cron / manual / dependency

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

# ============================================================
# 状态持久化
# ============================================================
class StateStore:
    """ETL 执行状态持久化（本地 JSON 文件）

    结构：
    {
      "tasks": {
        "<task_name>": {
          "last_run": <TaskRunRecord dict>,
          "history": [<TaskRunRecord dict>, ...]  # 最近 N 次
        }
      },
      "updated_at": "<ISO8601>"
    }
    """

    def __init__(self, state_path: str | Path):
        self.path = Path(state_path)
        self._lock = threading.Lock()
        self._cache: dict[str, Any] = {"tasks": {}, "updated_at": None}
        self._load()

    def _load(self) -> None:
        if not self.path.exists():
            return
        try:
            self._cache = json.loads(self.path.read_text(encoding="utf-8"))
            if "tasks" not in self._cache:
                self._cache["tasks"] = {}
        except Exception as exc:
            logger.warning("ETL 状态文件加载失败，重置: %s (%s)", self.path, exc)
            self._cache = {"tasks": {}, "updated_at": None}

    def _flush(self) -> None:
        self._cache["updated_at"] = datetime.now(timezone.utc).isoformat()
        try:
            self.path.parent.mkdir(parents=True, exist_ok=True)
            tmp = self.path.with_suffix(self.path.suffix + ".tmp")
            tmp.write_text(
                json.dumps(self._cache, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )
            tmp.replace(self.path)
        except Exception as exc:
            logger.error("ETL 状态文件写入失败: %s (%s)", self.path, exc)

    def record(self, run: TaskRunRecord) -> None:
        with self._lock:
            entry = self._cache["tasks"].setdefault(
                run.task_name, {"last_run": None, "history": []}
            )
            entry["last_run"] = run.to_dict()
            entry["history"].insert(0, run.to_dict())
            # 截断历史，避免无限增长
            entry["history"] = entry["history"][:_MAX_HISTORY_PER_TASK]
            self._flush()

    def get_task_state(self, name: str) -> dict[str, Any]:
        with self._lock:
            return self._cache["tasks"].get(name, {"last_run": None, "history": []})

    def snapshot(self) -> dict[str, Any]:
        with self._lock:
            # 深拷贝避免外部修改
            return json.loads(json.dumps(self._cache))

# ============================================================
# 任务执行器
# ============================================================
def _execute_task(spec: TaskSpec, triggered_by: str) -> TaskRunRecord:
    """同步执行单个任务，返回执行记录"""
    started = datetime.now(timezone.utc)
    record = TaskRunRecord(
        task_name=spec.name,
        status=STATUS_RUNNING,
        started_at=started.isoformat(),
        triggered_by=triggered_by,
    )
    logger.info("ETL 任务开始: %s (trigger=%s)", spec.name, triggered_by)

    try:
        if spec.callable is not None:
            result = spec.callable()
            output = json.dumps(result, ensure_ascii=False) if isinstance(result, dict) else str(result)
            record.output = (output or "")[:8000]
            record.return_code = 0
            record.status = STATUS_SUCCESS
        else:
            assert spec.command is not None
            try:
                proc = subprocess.run(
                    spec.command,
                    capture_output=True,
                    text=True,
                    timeout=spec.timeout_seconds,
                    check=False,
                    cwd=str(_PROJECT_ROOT),
                )
            except subprocess.TimeoutExpired as exc:
                record.status = STATUS_FAILED
                record.error = f"任务超时({spec.timeout_seconds}s): {exc}"
                record.return_code = -1
            except FileNotFoundError as exc:
                record.status = STATUS_FAILED
                record.error = f"命令不存在: {exc}"
                record.return_code = -2
            else:
                record.return_code = proc.returncode
                record.output = (proc.stdout or "")[-8000:]
                record.error = (proc.stderr or "")[-4000:] or None
                record.status = STATUS_SUCCESS if proc.returncode == 0 else STATUS_FAILED
    except Exception as exc:  # pragma: no cover - 兜底
        record.status = STATUS_FAILED
        record.error = f"任务执行异常: {exc}"
        record.return_code = -3
        logger.exception("ETL 任务执行异常: %s", spec.name)

    finished = datetime.now(timezone.utc)
    record.finished_at = finished.isoformat()
    record.duration_seconds = round((finished - started).total_seconds(), 3)
    logger.info(
        "ETL 任务结束: %s status=%s duration=%.3fs rc=%s",
        spec.name,
        record.status,
        record.duration_seconds,
        record.return_code,
    )
    return record

# ============================================================
# 调度器主类
# ============================================================
class ETLScheduler:
    """ETL 调度器

    线程模型：start() 启动后台守护线程，按轮询间隔检查 cron 触发；
    run_task() / run_now() 可在主线程手动触发（含依赖解析）。
    """

    def __init__(self, state_path: str | Path | None = None):
        if state_path is None:
            state_path = _PROJECT_ROOT / "logs" / "etl_state.json"
        self.state = StateStore(state_path)
        self._tasks: dict[str, TaskSpec] = {}
        self._lock = threading.Lock()
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        # 记录本轮调度中已触发的 (task, minute_key)，避免同一分钟内重复触发
        self._fired: dict[str, str] = {}

    # ---------- 任务注册 ----------
    def register(self, spec: TaskSpec) -> None:
        with self._lock:
            if spec.name in self._tasks:
                raise ValueError(f"任务已存在: {spec.name}")
            # 依赖完整性校验（依赖可后续注册，此处仅记录）
            self._tasks[spec.name] = spec
        logger.info("ETL 任务已注册: %s cron=%s deps=%s", spec.name, spec.cron, spec.depends_on)

    def register_many(self, specs: list[TaskSpec]) -> None:
        for spec in specs:
            self.register(spec)

    def get_task(self, name: str) -> Optional[TaskSpec]:
        with self._lock:
            return self._tasks.get(name)

    def list_tasks(self) -> list[TaskSpec]:
        with self._lock:
            return list(self._tasks.values())

    # ---------- 依赖解析 ----------
    def _resolve_order(self, targets: list[str]) -> list[str]:
        """拓扑排序：返回包含 targets 及其依赖的执行顺序"""
        with self._lock:
            tasks = dict(self._tasks)
        visited: set[str] = set()
        order: list[str] = []
        visiting: set[str] = set()

        def visit(name: str):
            if name in visited:
                return
            if name in visiting:
                raise ValueError(f"检测到循环依赖: {name}")
            visiting.add(name)
            spec = tasks.get(name)
            if spec is None:
                raise ValueError(f"依赖的任务未注册: {name}")
            for dep in spec.depends_on:
                visit(dep)
            visiting.discard(name)
            visited.add(name)
            order.append(name)

        for t in targets:
            visit(t)
        return order

    # ---------- 执行 ----------
    def run_now(self, task_name: str, triggered_by: str = "manual") -> TaskRunRecord:
        """立即执行单个任务（含依赖，按拓扑序执行）

        返回该任务自身的执行记录；依赖任务失败时，下游任务标记为 skipped。
        """
        order = self._resolve_order([task_name])
        last_record: Optional[TaskRunRecord] = None
        for name in order:
            spec = self.get_task(name)
            if spec is None:
                continue
            # 依赖任务失败则跳过下游
            if last_record is not None and last_record.status == STATUS_FAILED:
                skip = TaskRunRecord(
                    task_name=name,
                    status=STATUS_SKIPPED,
                    started_at=datetime.now(timezone.utc).isoformat(),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    duration_seconds=0.0,
                    triggered_by="dependency",
                    error=f"上游任务 {last_record.task_name} 失败，跳过",
                )
                self.state.record(skip)
                last_record = skip
                continue
            trigger = triggered_by if name == task_name else "dependency"
            record = _execute_task(spec, trigger)
            self.state.record(record)
            last_record = record
        assert last_record is not None
        return last_record

    def run_all(self, triggered_by: str = "manual") -> dict[str, TaskRunRecord]:
        """立即执行全部已注册任务（按依赖拓扑序）"""
        with self._lock:
            all_names = list(self._tasks.keys())
        order = self._resolve_order(all_names)
        results: dict[str, TaskRunRecord] = {}
        failed_upstream: Optional[str] = None
        for name in order:
            spec = self.get_task(name)
            if spec is None:
                continue
            if failed_upstream is not None:
                skip = TaskRunRecord(
                    task_name=name,
                    status=STATUS_SKIPPED,
                    started_at=datetime.now(timezone.utc).isoformat(),
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    duration_seconds=0.0,
                    triggered_by="dependency",
                    error=f"上游任务 {failed_upstream} 失败，跳过",
                )
                self.state.record(skip)
                results[name] = skip
                continue
            record = _execute_task(spec, triggered_by)
            self.state.record(record)
            results[name] = record
            if record.status == STATUS_FAILED:
                failed_upstream = name
        return results

    # ---------- cron 调度循环 ----------
    def _should_fire(self, spec: TaskSpec, now: datetime) -> bool:
        if not spec.enabled or not spec.cron:
            return False
        schedule = CronSchedule.parse(spec.cron)
        if not schedule.matches(now):
            return False
        minute_key = now.strftime("%Y-%m-%dT%H:%M")
        last = self._fired.get(spec.name)
        if last == minute_key:
            return False
        self._fired[spec.name] = minute_key
        return True

    def _loop(self) -> None:
        logger.info("ETL 调度器后台线程已启动, poll_interval=%ss", _POLL_INTERVAL)
        while not self._stop_event.is_set():
            try:
                now = datetime.now(timezone.utc)
                # 用本地时区触发（A 股收盘后跑 ETL 更符合业务直觉）
                local_now = now.astimezone()
                for spec in self.list_tasks():
                    if self._should_fire(spec, local_now):
                        # 在独立线程执行避免阻塞调度循环
                        t = threading.Thread(
                            target=self._safe_run,
                            args=(spec.name, "cron"),
                            name=f"etl-{spec.name}",
                            daemon=True,
                        )
                        t.start()
            except Exception:  # pragma: no cover
                logger.exception("ETL 调度循环异常")
            self._stop_event.wait(_POLL_INTERVAL)
        logger.info("ETL 调度器后台线程已停止")

    def _safe_run(self, task_name: str, triggered_by: str) -> None:
        try:
            self.run_now(task_name, triggered_by=triggered_by)
        except Exception:  # pragma: no cover
            logger.exception("ETL 调度执行失败: %s", task_name)

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            logger.warning("ETL 调度器已在运行")
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop, name="etl-scheduler", daemon=True
        )
        self._thread.start()

    def stop(self, timeout: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread:
            self._thread.join(timeout=timeout)
            self._thread = None

    # ---------- 状态查询 ----------
    def status_snapshot(self) -> dict[str, Any]:
        """返回调度器整体状态快照（供监控端点使用）"""
        snap = self.state.snapshot()
        tasks_meta: dict[str, Any] = {}
        for spec in self.list_tasks():
            tasks_meta[spec.name] = {
                "cron": spec.cron,
                "enabled": spec.enabled,
                "depends_on": list(spec.depends_on),
                "description": spec.description,
                "has_command": spec.command is not None,
                "has_callable": spec.callable is not None,
            }
        return {
            "scheduler_running": bool(self._thread and self._thread.is_alive()),
            "tasks": tasks_meta,
            "task_states": snap.get("tasks", {}),
            "updated_at": snap.get("updated_at"),
        }

# ============================================================
# 默认任务注册（与现有 ETL 脚本对齐）
# ============================================================
def build_default_scheduler(state_path: str | Path | None = None) -> ETLScheduler:
    """构建预注册了现有 ETL 脚本的调度器

    这些脚本原本是手动执行的，此处仅注册为可被 cron 触发的任务骨架，
    实际参数（如 API key）需通过环境变量或命令行注入。
    """
    scheduler = ETLScheduler(state_path=state_path)
    py = sys.executable or "python"

    # 1. 官方数据同步（增量包）—— 默认每个交易日 18:30
    scheduler.register(
        TaskSpec(
            name="sync_official_data",
            cron="30 18 * * 1-5",
            command=[py, "-m", "backend.scripts.sync_official_data_update"],
            description="拉取并应用官方增量数据包到 stock_daily_latest",
            timeout_seconds=int(os.getenv("ETL_SYNC_OFFICIAL_TIMEOUT", "3600")),
        )
    )

    # 2. 完整管线（技术指标 + K线元数据）—— 默认每个交易日 19:00，依赖官方同步
    scheduler.register(
        TaskSpec(
            name="update_sdl_complete_pipeline",
            cron="0 19 * * 1-5",
            command=[py, "-m", "backend.scripts.update_sdl_complete_pipeline"],
            depends_on=["sync_official_data"],
            description="计算 MACD/KDJ/MA/波动率等技术指标并回写 stock_daily_latest",
        )
    )

    # 3. 行业数据更新 —— 默认每周一 20:00
    scheduler.register(
        TaskSpec(
            name="update_sdl_industry",
            cron="0 20 * * 1",
            command=[py, "-m", "backend.scripts.update_sdl_industry"],
            description="从本地行业 JSON 更新 stock_daily_latest.industry 字段",
        )
    )

    # 4. 特征快照生成 —— 默认每个交易日 20:30，依赖完整管线
    scheduler.register(
        TaskSpec(
            name="generate_feature_snapshots",
            cron="30 20 * * 1-5",
            command=[py, "-m", "backend.scripts.data.generate_feature_snapshots"],
            depends_on=["update_sdl_complete_pipeline"],
            description="从 stock_daily_latest 生成因子特征快照 parquet",
        )
    )

    # 5. 数据缺口检测（T5.3 新增）—— 默认每个交易日 21:00，依赖特征快照
    scheduler.register(
        TaskSpec(
            name="etl_data_quality_check",
            cron="0 21 * * 1-5",
            command=[py, "-m", "backend.scripts.etl_data_quality"],
            depends_on=["generate_feature_snapshots"],
            description="检测 stock_daily_latest 数据缺口并触发补数",
        )
    )

    # 6. 对齐异常告警（T5.3 新增）—— 默认每个交易日 21:30
    scheduler.register(
        TaskSpec(
            name="etl_alignment_monitor",
            cron="30 21 * * 1-5",
            command=[py, "-m", "backend.scripts.etl_alignment_monitor"],
            description="检测股票列表对齐/日期滞后/特征值异常并告警",
        )
    )

    return scheduler

# ============================================================
# CLI 入口
# ============================================================
def _cli() -> int:
    import argparse

    parser = argparse.ArgumentParser(description="ETL 调度器 (T5.3)")
    parser.add_argument(
        "--state-path",
        default=str(_PROJECT_ROOT / "logs" / "etl_state.json"),
        help="状态文件路径",
    )
    sub = parser.add_subparsers(dest="cmd")

    sub.add_parser("start", help="启动后台调度循环（守护进程模式）")
    run_p = sub.add_parser("run", help="立即执行指定任务（含依赖）")
    run_p.add_argument("task_name")
    sub.add_parser("run-all", help="立即执行全部已注册任务")
    sub.add_parser("status", help="打印调度器状态快照")
    sub.add_parser("list", help="列出已注册任务")

    args = parser.parse_args()
    scheduler = build_default_scheduler(state_path=args.state_path)

    if args.cmd == "start":
        scheduler.start()
        logger.info("ETL 调度器已启动，按 Ctrl+C 退出")
        try:
            while True:
                time.sleep(60)
        except KeyboardInterrupt:
            scheduler.stop()
        return 0

    if args.cmd == "run":
        spec = scheduler.get_task(args.task_name)
        if spec is None:
            print(f"任务不存在: {args.task_name}", file=sys.stderr)
            return 1
        record = scheduler.run_now(args.task_name, triggered_by="manual")
        print(json.dumps(record.to_dict(), ensure_ascii=False, indent=2))
        return 0 if record.status == STATUS_SUCCESS else 2

    if args.cmd == "run-all":
        results = scheduler.run_all(triggered_by="manual")
        print(
            json.dumps(
                {k: v.to_dict() for k, v in results.items()},
                ensure_ascii=False,
                indent=2,
            )
        )
        failed = [r for r in results.values() if r.status == STATUS_FAILED]
        return 1 if failed else 0

    if args.cmd in ("status", "list", None):
        snap = scheduler.status_snapshot()
        if args.cmd == "list":
            for name, meta in snap["tasks"].items():
                print(f"- {name}: cron={meta['cron']} deps={meta['depends_on']} enabled={meta['enabled']}")
        else:
            print(json.dumps(snap, ensure_ascii=False, indent=2))
        return 0

    parser.print_help()
    return 1

if __name__ == "__main__":
    sys.exit(_cli())
