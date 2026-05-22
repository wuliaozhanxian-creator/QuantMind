#!/usr/bin/env python3
"""
QuantMind 云端训练脚本 (CVM 容器内运行)
=========================================
参数传递方式：YAML 配置文件（固化在镜像中，参数通过挂载的 config.yaml 传入）

用法：
  docker run -v /host/workspace:/workspace quantmind:latest --config /workspace/config.yaml

config.yaml 结构：
  run_id / job_name
  data.train_start / data.train_end / data.features
  model.type / model.num_boost_round / model.val_ratio / model.params
  output.result_path / output.cos_prefix
  callback.url / callback.secret
"""

from __future__ import annotations

import argparse
import json
import math
import logging
import os
import sys
import time
from datetime import datetime
from datetime import date
from pathlib import Path
from typing import Any
from urllib.parse import urlparse

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    handlers=[logging.StreamHandler(sys.stdout)],
)
logger = logging.getLogger("quantmind.train")
_TRAINING_LOG_FILE = (os.getenv("TRAINING_LOG_FILE") or "").strip() or "/workspace/training.log"


def _attach_training_file_logger(log_path: str) -> None:
    if not log_path:
        return
    path = Path(log_path)
    try:
        path.parent.mkdir(parents=True, exist_ok=True)
    except Exception:
        return
    root_logger = logging.getLogger()
    for handler in root_logger.handlers:
        if isinstance(handler, logging.FileHandler) and Path(getattr(handler, "baseFilename", "")).resolve() == path.resolve():
            return
    try:
        file_handler = logging.FileHandler(path, encoding="utf-8")
        file_handler.setLevel(logging.INFO)
        file_handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
        root_logger.addHandler(file_handler)
    except Exception as exc:
        logger.warning("Failed to attach training log file handler %s: %s", path, exc)


_attach_training_file_logger(_TRAINING_LOG_FILE)

try:
    import lightgbm as lgb
    import numpy as np
    import pandas as pd
    import requests
    import yaml
    from qcloud_cos import CosConfig, CosS3Client
except Exception as exc:  # pragma: no cover - import-time dependency guard
    logger.exception("Failed to import training dependencies: %s", exc)
    raise

# ── LightGBM 默认参数 ────────────────────────────────────────────────────────
DEFAULT_LGB_PARAMS: dict[str, Any] = {
    "objective":         "regression",
    "metric":            "l2",
    "boosting":          "gbdt",
    "num_leaves":        127,
    "learning_rate":     0.05,
    "feature_fraction":  0.8,
    "bagging_fraction":  0.8,
    "bagging_freq":      5,
    "min_child_samples": 50,
    "n_jobs":            -1,
    "verbosity":         -1,
}
_ALLOWED_SHAP_SPLIT = {"valid", "test", "train"}
_DEFAULT_EXPLAIN_CFG: dict[str, Any] = {
    "enable_shap": True,
    "shap_split": "valid",
    "shap_sample_rows": 30000,
}
_DEFAULT_SHAP_SAMPLE_ROWS = 30000
_MIN_SHAP_SAMPLE_ROWS = 1000
_MAX_SHAP_SAMPLE_ROWS = 100000
_SHAP_SAMPLE_RANDOM_STATE = 42
_INFERENCE_TEMPLATE_CANDIDATES = [
    Path(os.getenv("TRAINING_INFERENCE_TEMPLATE_PATH", "")),
    Path("/app/inference_parquet.py"),
    Path("/app/backend/services/engine/inference/templates/inference_parquet.py"),
    Path("/workspace/backend/services/engine/inference/templates/inference_parquet.py"),
]



# ── COS 辅助 ─────────────────────────────────────────────────────────────────
def _cos_client() -> CosS3Client:
    region = os.getenv("TENCENT_REGION", "ap-guangzhou")
    # 训练链路强制使用 COS SDK（Bucket + Key）访问，避免自定义公网域名下载产生流量费。
    # EnableInternalDomain=True 会优先走腾讯云内网解析（同地域场景）。
    return CosS3Client(CosConfig(
        Region=region,
        SecretId=os.getenv("TENCENT_SECRET_ID", ""),
        SecretKey=os.getenv("TENCENT_SECRET_KEY", ""),
        Scheme="https",
        EnableInternalDomain=True,
        AutoSwitchDomainOnRetry=True,
    ))


def _load_local_parquet(local_dir: Path, year: int, columns: list[str] | None = None) -> pd.DataFrame | None:
    file_path = local_dir / f"model_features_{year}.parquet"
    if not file_path.exists():
        return None
    try:
        logger.info(f"Local data hit: {file_path} (reading {len(columns) if columns else 'all'} columns)")
        return pd.read_parquet(file_path, engine="pyarrow", columns=columns)
    except Exception as exc:
        logger.warning(f"  ⚠ Failed to read local parquet {file_path}: {exc}")
        return None


def _upload_bytes(client: CosS3Client, bucket: str, key: str, data: bytes, content_type: str = "application/octet-stream") -> str:
    client.put_object(Bucket=bucket, Key=key, Body=data, ContentType=content_type)
    logger.info(f"Uploaded cos://{bucket}/{key}  ({len(data):,} bytes)")
    return key


def _normalize_cos_key(raw: str) -> str:
    val = (raw or "").strip()
    if not val:
        return ""
    if val.startswith(("http://", "https://")):
        parsed = urlparse(val)
        path_key = parsed.path.lstrip("/")
        logger.warning(
            "config-cos-key is URL (%s), normalized to object key '%s'; "
            "training should use COS key instead of public URL",
            parsed.netloc,
            path_key,
        )
        return path_key
    return val


def _load_inference_template() -> str:
    for candidate in _INFERENCE_TEMPLATE_CANDIDATES:
        if not str(candidate):
            continue
        try:
            if candidate.exists() and candidate.is_file():
                logger.info("Using inference template: %s", candidate)
                return candidate.read_text(encoding="utf-8")
        except Exception as exc:
            logger.warning("Failed to read inference template %s: %s", candidate, exc)
    raise FileNotFoundError(
        "inference_parquet.py template not found; mount it to /app/inference_parquet.py "
        "or set TRAINING_INFERENCE_TEMPLATE_PATH"
    )


def _download_config_from_cos(cos_key: str, dest_path: Path) -> Path:
    bucket = os.getenv("TENCENT_BUCKET", "quantmind-1255718505")
    key = _normalize_cos_key(cos_key)
    if not key:
        raise RuntimeError("empty config cos key")
    client = _cos_client()
    logger.info(f"Downloading config from cos://{bucket}/{key} -> {dest_path}")
    resp = client.get_object(Bucket=bucket, Key=key)
    raw = resp["Body"].get_raw_stream().read()
    dest_path.parent.mkdir(parents=True, exist_ok=True)
    dest_path.write_bytes(raw)
    logger.info(f"Config downloaded: {len(raw):,} bytes")
    return dest_path


def _json_safe_value(value: Any) -> Any:
    """把结果结构中的非有限浮点转换为 JSON 可序列化值。"""
    if isinstance(value, dict):
        return {str(k): _json_safe_value(v) for k, v in value.items()}
    if isinstance(value, (list, set, frozenset)):
        return [_json_safe_value(v) for v in value]
    if isinstance(value, tuple):
        return [_json_safe_value(v) for v in value]
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return _json_safe_value(value.tolist())
        except Exception:
            pass
    if isinstance(value, np.generic):
        return _json_safe_value(value.item())
    if isinstance(value, pd.Series):
        return [_json_safe_value(v) for v in value.tolist()]
    if isinstance(value, pd.Index):
        return [_json_safe_value(v) for v in value.tolist()]
    if isinstance(value, pd.DataFrame):
        return _json_safe_value(value.to_dict(orient="records"))
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, float):
        return value if math.isfinite(value) else None
    return value


def _json_default(value: Any) -> Any:
    """json.dumps 的兜底转换器，保证剩余的日期/路径/标量都能落成原生 JSON。"""
    if isinstance(value, (datetime, date, pd.Timestamp)):
        return value.isoformat()
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, np.generic):
        return _json_safe_value(value)
    if hasattr(value, "tolist") and not isinstance(value, (str, bytes, bytearray)):
        try:
            return _json_safe_value(value.tolist())
        except Exception:
            return str(value)
    return str(value)


def _find_nonfinite_paths(value: Any, path: str = "root", out: list[str] | None = None) -> list[str]:
    """递归定位结构内仍残留的非有限浮点，便于训练日志定位问题字段。"""
    if out is None:
        out = []

    if isinstance(value, dict):
        for key, item in value.items():
            _find_nonfinite_paths(item, f"{path}.{key}", out)
        return out

    if isinstance(value, (list, tuple, set, frozenset)):
        for idx, item in enumerate(list(value)):
            _find_nonfinite_paths(item, f"{path}[{idx}]", out)
        return out

    if isinstance(value, np.generic):
        return _find_nonfinite_paths(value.item(), path, out)

    if isinstance(value, (datetime, date, pd.Timestamp, Path)):
        return out

    if isinstance(value, float) and not math.isfinite(value):
        out.append(path)
        return out

    return out


# ── 评估指标 ─────────────────────────────────────────────────────────────────
def _ic(pred: np.ndarray, label: np.ndarray) -> float:
    mask = np.isfinite(pred) & np.isfinite(label)
    if mask.sum() < 10:
        return float("nan")
    return float(np.corrcoef(pred[mask], label[mask])[0, 1])


def _rank_ic_series(df: pd.DataFrame, pred_col: str, label_col: str) -> list[float]:
    daily = []
    for _, g in df.groupby("trade_date", sort=False):
        g = g[[pred_col, label_col]].dropna()
        if len(g) < 10:
            continue
        rp = g[pred_col].rank(method="average").to_numpy()
        rl = g[label_col].rank(method="average").to_numpy()
        v = _ic(rp, rl)
        if np.isfinite(v):
            daily.append(v)
    return daily


def _compute_metrics(df: pd.DataFrame, y_true: np.ndarray, y_pred: np.ndarray) -> dict:
    ic     = _ic(y_pred, y_true)
    series = _rank_ic_series(df.assign(_pred=y_pred, _label=y_true), "_pred", "_label")
    rank_ic   = float(np.nanmean(series)) if series else float("nan")
    rank_icir = float(np.mean(series) / (np.std(series) + 1e-9)) if series else float("nan")
    rmse = float(np.sqrt(np.mean(np.square(y_pred - y_true)))) if len(y_true) else float("nan")
    labels = (y_true > 0).astype(int)
    pos = int(labels.sum())
    neg = int(len(labels) - pos)
    auc = float("nan")
    if pos > 0 and neg > 0:
        ranks = pd.Series(y_pred).rank(method="average").to_numpy()
        auc = float((ranks[labels == 1].sum() - pos * (pos + 1) / 2.0) / (pos * neg))
    return {"ic": ic, "rank_ic": rank_ic, "rank_icir": rank_icir, "rmse": rmse, "auc": auc}


def _normalize_explain_cfg(raw: Any) -> dict[str, Any]:
    explain = raw if isinstance(raw, dict) else {}
    enable_shap = bool(explain.get("enable_shap", _DEFAULT_EXPLAIN_CFG["enable_shap"]))

    shap_split = str(explain.get("shap_split", _DEFAULT_EXPLAIN_CFG["shap_split"])).strip().lower()
    if shap_split not in _ALLOWED_SHAP_SPLIT:
        logger.warning("Invalid explain.shap_split=%s, fallback to 'valid'", shap_split)
        shap_split = "valid"

    sample_rows_raw = explain.get("shap_sample_rows", _DEFAULT_EXPLAIN_CFG["shap_sample_rows"])
    try:
        sample_rows = int(sample_rows_raw)
    except Exception:
        logger.warning("Invalid explain.shap_sample_rows=%s, fallback to %d", sample_rows_raw, _DEFAULT_SHAP_SAMPLE_ROWS)
        sample_rows = _DEFAULT_SHAP_SAMPLE_ROWS
    sample_rows = max(_MIN_SHAP_SAMPLE_ROWS, min(_MAX_SHAP_SAMPLE_ROWS, sample_rows))

    return {
        "enable_shap": enable_shap,
        "shap_split": shap_split,
        "shap_sample_rows": sample_rows,
    }


def _resolve_shap_source_frame(
    split_frames: dict[str, pd.DataFrame],
    preferred_split: str,
) -> tuple[str, pd.DataFrame]:
    ordered = [preferred_split] + [s for s in ("valid", "test", "train") if s != preferred_split]
    for split in ordered:
        frame = split_frames.get(split)
        if isinstance(frame, pd.DataFrame) and not frame.empty:
            return split, frame
    return "", pd.DataFrame()


def _compute_shap_summary(
    *,
    model: lgb.Booster,
    split_frames: dict[str, pd.DataFrame],
    features: list[str],
    fill_values: dict[str, float],
    explain_cfg: dict[str, Any],
    out_path: Path,
) -> dict[str, Any]:
    shap_info: dict[str, Any] = {
        "enabled": bool(explain_cfg.get("enable_shap", True)),
        "status": "disabled",
        "split": str(explain_cfg.get("shap_split", "valid")),
        "rows_requested": int(explain_cfg.get("shap_sample_rows", _DEFAULT_SHAP_SAMPLE_ROWS)),
        "rows_used": 0,
        "file": "",
        "error": "",
        "elapsed_seconds": 0.0,
    }
    if not shap_info["enabled"]:
        return shap_info

    if not features:
        shap_info["status"] = "skipped"
        shap_info["error"] = "no_feature_columns"
        return shap_info

    start_ts = time.time()
    try:
        preferred_split = str(explain_cfg.get("shap_split", "valid")).strip().lower()
        selected_split, split_df = _resolve_shap_source_frame(split_frames, preferred_split)
        if split_df.empty:
            shap_info["status"] = "skipped"
            shap_info["error"] = "no_rows_for_shap"
            return shap_info

        rows_requested = int(explain_cfg.get("shap_sample_rows", _DEFAULT_SHAP_SAMPLE_ROWS))
        sample_df = split_df
        if len(sample_df) > rows_requested:
            sample_df = sample_df.sample(rows_requested, random_state=_SHAP_SAMPLE_RANDOM_STATE)

        x_df = sample_df[features].copy()
        for c in features:
            fill_v = fill_values.get(c, 0.0)
            if fill_v is None or (isinstance(fill_v, float) and np.isnan(fill_v)):
                fill_v = 0.0
            x_df[c] = x_df[c].astype("float32").fillna(fill_v)
        x = x_df.to_numpy(dtype=np.float32)

        contrib = model.predict(
            x,
            num_iteration=model.best_iteration or None,
            pred_contrib=True,
        )
        if not isinstance(contrib, np.ndarray) or contrib.ndim != 2:
            raise RuntimeError(f"unexpected SHAP contribution shape: {getattr(contrib, 'shape', None)}")
        if contrib.shape[1] < len(features):
            raise RuntimeError(f"contrib columns mismatch: got {contrib.shape[1]}, expect >= {len(features)}")

        shap_values = contrib[:, :len(features)]
        summary_df = pd.DataFrame(
            {
                "feature": features,
                "mean_abs_shap": np.mean(np.abs(shap_values), axis=0),
                "mean_shap": np.mean(shap_values, axis=0),
                "positive_ratio": np.mean(shap_values > 0, axis=0),
            }
        ).sort_values("mean_abs_shap", ascending=False)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        summary_df.to_csv(out_path, index=False)

        shap_info.update(
            {
                "status": "completed",
                "split": selected_split,
                "rows_requested": rows_requested,
                "rows_used": int(len(sample_df)),
                "file": out_path.name,
                "error": "",
            }
        )
        return shap_info
    except Exception as exc:  # noqa: BLE001
        logger.exception("SHAP summary generation failed: %s", exc)
        shap_info["status"] = "failed"
        shap_info["error"] = str(exc)
        return shap_info
    finally:
        shap_info["elapsed_seconds"] = float(time.time() - start_ts)


# ── 数据加载 ──────────────────────────────────────────────────────────────────
def load_data(
    train_start: str,
    train_end: str,
    features: list[str],
    target_horizon_days: int = 1,
    cache_dir: str | None = None,
    valid_end: str | None = None,
    test_end: str | None = None,
    source_mode: str = "LOCAL",
    local_dir: str | None = None,
    limit_up_weight: float = 0.5,
) -> tuple[pd.DataFrame, list[str]]:
    start_year = pd.Timestamp(train_start).year

    # 获取最晚的年份，确保包含 验证/测试 集所需的数据
    ends = [train_end]
    if valid_end: ends.append(valid_end)
    if test_end: ends.append(test_end)
    end_year = max(pd.Timestamp(e).year for e in ends)

    # 自动计算必要的核心列
    core_columns = ["trade_date", "symbol", "open", "close"]
    # 尝试加载当前周期对应的预置收益特征
    _horizon = max(1, int(target_horizon_days or 1))
    
    # 最终合并列：核心列 + 用户请求特征（去重）
    load_cols = list(dict.fromkeys(core_columns + features))
    logger.info(f"Memory optimization: Planning to load {len(load_cols)} columns from {len(range(start_year - 1, end_year + 1))} years")

    local_root = Path(local_dir).expanduser() if local_dir else None
    if local_root is None:
        raise RuntimeError("local_dir must be provided; COS data download has been removed")

    chunks = []
    # 支持更早或更晚的数据年份（由训练起止时间动态决定）
    for year in range(start_year - 1, end_year + 1):
        df_year = _load_local_parquet(local_root, year, columns=load_cols)
        if df_year is not None:
            # 过滤北交所代码（4/8开头），减少后续 concat 后的物理内存占用
            df_year["symbol"] = df_year["symbol"].astype(str).str.zfill(6)
            df_year = df_year[~df_year["symbol"].str.startswith(("4", "8"))]
            chunks.append(df_year)
        else:
            logger.warning(f"No data file found for year {year} in {local_root}, skipping")

    if not chunks:
        raise RuntimeError("No data loaded from local storage")

    df = pd.concat(chunks, axis=0, ignore_index=True)
    del chunks # 及时释放列表引用
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    logger.info(f"Raw concat size: {len(df)} rows. Date range: {df['trade_date'].min()} to {df['trade_date'].max()}")

    # 过滤符号（已在分块加载时完成，此处仅保留逻辑自洽）
    logger.info(f"After symbol filter: {len(df)} rows")

    # 标签：基于 target_horizon_days 构建 N 日远期收益（实盘可交易口径）
    if "open" not in df.columns or "close" not in df.columns:
        raise RuntimeError("Columns 'open' and 'close' are required for tradable label calculation")

    # 从参数读取预测周期（不依赖全局 cfg）
    _horizon = max(1, int(target_horizon_days or 1))

    df = df.sort_values(["symbol", "trade_date"]).reset_index(drop=True)
    
    # 构造实盘可交易收益率：T+1开盘买入，T+H收盘卖出 (Close_{T+H} / Open_{T+1} - 1)
    # 彻底剔除 T收盘 到 T+1开盘 的不可交易隔夜跳空，防止模型过度学习反转并掩盖真实Alpha
    grouped = df.groupby("symbol")
    future_close = grouped["close"].shift(-_horizon)
    next_open = grouped["open"].shift(-1)
    
    # 获取 T 日收盘价，用于判断 T+1 开盘是否一字涨停
    current_close = df["close"]
    
    # 智能识别涨跌幅限制 (科创板/创业板 20%, 北交所 30%, 其他主板 10%)
    # DataFrame 中的 symbol 为大写前缀，如 SH688001, SZ300001, BJ830001
    symbols = df["symbol"].astype(str)
    is_star_gem = symbols.str.startswith("SH68") | symbols.str.startswith("SZ30")
    is_bse = symbols.str.startswith("BJ")
    
    # 设置 T+1 开盘涨停阈值 (留 0.5% 容错余量：20%->19.5%, 30%->29.5%, 10%->9.5%)
    limit_threshold = np.where(is_bse, 0.295, np.where(is_star_gem, 0.195, 0.095))
    
    # 精细化涨跌停识别
    is_limit_up_open = (next_open / current_close - 1) >= limit_threshold
    is_limit_down_open = (next_open / current_close - 1) <= -limit_threshold
    is_extreme = is_limit_up_open | is_limit_down_open
    
    # 避免除以0的情况
    raw_label = np.where(next_open > 0, (future_close / next_open) - 1, np.nan)
    
    # 【改为软降权】：不再把极端样本的 label 设为 NaN，而是保留真实收益率，让模型能够学习到它们的特征分布
    df["label"] = raw_label
    
    # 新增 weight 列：正常样本权重为 1.0，开盘涨跌停的极端样本权重动态配置 (软降权)
    df["weight"] = np.where(is_extreme, limit_up_weight, 1.0)
    
    logger.info(f"Tradable label built with target_horizon_days={_horizon} (Close_T+{_horizon} / Open_T+1 - 1)")
    logger.info(f"Soft weighted {int(is_limit_up_open.sum())} limit-up and {int(is_limit_down_open.sum())} limit-down samples to {limit_up_weight}.")

    valid_count_before = len(df)
    df = df[df["label"].notna()].copy()
    logger.info(f"After label shift & dropna: {len(df)} rows (dropped {valid_count_before - len(df)} rows with missing labels)")

    # 裁剪到请求日期范围 (使用 pd.Timestamp 确保类型一致)
    ts_start = pd.Timestamp(train_start)
    ts_train_end = pd.Timestamp(train_end)
    mask = (df["trade_date"] >= ts_start) & (df["trade_date"] <= ts_train_end)
    # 如果有验证集/测试集，扩大 mask 范围以包含它们
    if valid_end:
        mask = (df["trade_date"] >= ts_start) & (df["trade_date"] <= pd.Timestamp(valid_end))
    if test_end:
        mask = (df["trade_date"] >= ts_start) & (df["trade_date"] <= pd.Timestamp(test_end))

    df = df[mask].copy()
    logger.info(f"After date range clip ({ts_start.date()} to {pd.Timestamp(test_end or valid_end or train_end).date()}): {len(df)} rows")

    # 校验特征列
    missing = [f for f in features if f not in df.columns]
    if missing:
        logger.warning(f"Features not found in parquet (ignored): {missing}")
        features = [f for f in features if f in df.columns]
    if not features:
        raise RuntimeError("No valid feature columns found")

    keep_cols = ["symbol", "trade_date", "label", "weight"] + features
    df = df[keep_cols].reset_index(drop=True)

    # 截面 rank 标准化特征，消除绝对数值随时间漂移的问题
    logger.info("Applying cross-sectional percent rank normalization to features...")
    df[features] = df.groupby("trade_date")[features].rank(pct=True)

    # 截面 rank 标准化标签
    df["label"] = df.groupby("trade_date")["label"].rank(pct=True) - 0.5

    logger.info(
        f"Data ready: {len(df):,} rows, {len(features)} features, "
        f"{df['trade_date'].min().date()} ~ {df['trade_date'].max().date()}"
    )
    return df, features


# ── 训练 ──────────────────────────────────────────────────────────────────────
def train_model(df: pd.DataFrame, features: list[str], cfg: dict) -> tuple:
    model_cfg       = cfg.get("model", {})
    num_boost_round = int(model_cfg.get("num_boost_round", 1000))
    early_stopping_rounds = int(model_cfg.get("early_stopping_rounds", 100) or 100)
    if early_stopping_rounds < 1:
        early_stopping_rounds = 1
    params          = {**DEFAULT_LGB_PARAMS, **model_cfg.get("params", {})}

    def _frame_range_text(frame: pd.DataFrame) -> str:
        if frame.empty:
            return "EMPTY"
        return f"{frame['trade_date'].min().date()}~{frame['trade_date'].max().date()}"

    # 显式 split 优先于 val_ratio（config 有 split.valid 时 val_ratio 为 null）
    split_cfg = cfg.get("split", {})
    if split_cfg.get("valid"):
        valid_start_str, valid_end_str = split_cfg["valid"]
        requested_train = f"{split_cfg['train'][0]}~{split_cfg['train'][1]}"
        requested_val = f"{valid_start_str}~{valid_end_str}"
        train_df = df[
            (df["trade_date"] >= pd.Timestamp(split_cfg["train"][0])) &
            (df["trade_date"] <= pd.Timestamp(split_cfg["train"][1]))
        ].copy()
        val_df   = df[
            (df["trade_date"] >= pd.Timestamp(valid_start_str)) &
            (df["trade_date"] <= pd.Timestamp(valid_end_str))
        ].copy()
        if split_cfg.get("test"):
            test_start_str, test_end_str = split_cfg["test"]
            requested_test = f"{test_start_str}~{test_end_str}"
            test_df = df[
                (df["trade_date"] >= pd.Timestamp(test_start_str)) &
                (df["trade_date"] <= pd.Timestamp(test_end_str))
            ].copy()
        else:
            requested_test = requested_val
            test_df = val_df.copy()
        logger.info(f"Split mode: train~{split_cfg['train'][1]}  val {valid_start_str}~{valid_end_str}")
    else:
        val_ratio = float(model_cfg.get("val_ratio") or 0.15)
        dates     = sorted(df["trade_date"].unique())
        if not dates:
            raise RuntimeError("No rows available for split after preprocessing. 请检查训练时间窗口与特征快照覆盖范围。")
        
        val_idx = int(len(dates) * (1 - val_ratio))
        val_start = dates[val_idx]
        
        # 增加 gap_days，防止训练集尾部标签包含验证集特征导致的数据泄露
        _horizon = max(1, int((cfg.get("label", {}) or {}).get("target_horizon_days") or 1))
        train_end_idx = max(0, val_idx - _horizon)
        actual_train_end = dates[train_end_idx]
        
        train_df  = df[df["trade_date"] <= actual_train_end].copy()
        val_df    = df[df["trade_date"] >= val_start].copy()
        test_df = val_df.copy()
        train_start = pd.Timestamp(df["trade_date"].min()).date()
        train_end = pd.Timestamp(actual_train_end).date()
        requested_train = f"{train_start}~{train_end}"
        requested_val = f"{pd.Timestamp(val_start).date()}~{pd.Timestamp(df['trade_date'].max()).date()}"
        requested_test = requested_val
        logger.info(
            f"val_ratio mode: train~{train_end}"
            f"  val {pd.Timestamp(val_start).date()}~"
        )

    if train_df.empty or val_df.empty or test_df.empty:
        available_range = "EMPTY"
        if not df.empty:
            available_range = f"{df['trade_date'].min().date()}~{df['trade_date'].max().date()}"
        raise RuntimeError(
            "Dataset split contains empty segment. "
            f"available={available_range}; "
            f"train={len(train_df)}({_frame_range_text(train_df)}) requested={requested_train}; "
            f"val={len(val_df)}({_frame_range_text(val_df)}) requested={requested_val}; "
            f"test={len(test_df)}({_frame_range_text(test_df)}) requested={requested_test}. "
            "请调整 train/valid/test 时间窗口，确保三段均与可用数据重叠。"
        )

    # 因为特征已经过截面 Rank (0~1)，缺失值统一填充为 0.5 (中位数)
    fill_values = {c: 0.5 for c in features}

    def _fill(frame: pd.DataFrame) -> np.ndarray:
        x = frame[features].copy()
        for c in features:
            x[c] = x[c].astype("float32").fillna(0.5)
        return x.to_numpy(dtype=np.float32)

    X_train, y_train = _fill(train_df), train_df["label"].astype("float32").to_numpy()
    X_val,   y_val   = _fill(val_df),   val_df["label"].astype("float32").to_numpy()

    w_train = train_df["weight"].astype("float32").to_numpy() if "weight" in train_df.columns else None
    w_val   = val_df["weight"].astype("float32").to_numpy() if "weight" in val_df.columns else None

    ds_train = lgb.Dataset(X_train, label=y_train, weight=w_train, feature_name=features, free_raw_data=True)
    ds_val   = lgb.Dataset(X_val,   label=y_val,   weight=w_val,   feature_name=features, free_raw_data=True)

    callbacks = [
        lgb.early_stopping(stopping_rounds=early_stopping_rounds, verbose=True),
        lgb.log_evaluation(100),
    ]
    model = lgb.train(
        params, ds_train,
        num_boost_round=num_boost_round,
        valid_sets=[ds_train, ds_val],
        valid_names=["train", "valid"],
        callbacks=callbacks,
    )

    y_train_pred = model.predict(_fill(train_df))
    y_val_pred = model.predict(_fill(val_df))
    y_test_pred = model.predict(_fill(test_df))
    train_m = _compute_metrics(train_df, y_train, y_train_pred)
    val_m   = _compute_metrics(val_df,   y_val,   y_val_pred)
    test_m  = _compute_metrics(test_df,  test_df["label"].astype("float32").to_numpy(), y_test_pred)

    logger.info(f"Train IC={train_m['ic']:.4f}  RankIC={train_m['rank_ic']:.4f}")
    logger.info(f"Val   IC={val_m['ic']:.4f}    RankIC={val_m['rank_ic']:.4f}  ICIR={val_m['rank_icir']:.4f}")

    # 生成全窗口预测：覆盖 train/valid/test 三段，供后续完整回测使用
    full_pred_df = df[["symbol", "trade_date", "label"]].copy()
    full_pred_df["pred"] = model.predict(_fill(df))
    full_pred_df["split"] = "train"
    full_pred_df.loc[
        (full_pred_df["trade_date"] >= val_df["trade_date"].min()) &
        (full_pred_df["trade_date"] <= val_df["trade_date"].max()),
        "split",
    ] = "valid"
    full_pred_df.loc[
        (full_pred_df["trade_date"] >= test_df["trade_date"].min()) &
        (full_pred_df["trade_date"] <= test_df["trade_date"].max()),
        "split",
    ] = "test"
    return (
        model,
        fill_values,
        train_m,
        val_m,
        test_m,
        full_pred_df.reset_index(drop=True),
        {
            "train": train_df.reset_index(drop=True),
            "valid": val_df.reset_index(drop=True),
            "test": test_df.reset_index(drop=True),
        },
    )


# ── 主入口 ────────────────────────────────────────────────────────────────────
def main() -> int:
    # 最早期诊断日志：在任何处理之前打印，确保 Batch 环境中一定能看到
    print(f"[BOOT] python={sys.version}", flush=True)
    print(f"[BOOT] argv={sys.argv}", flush=True)
    print(f"[BOOT] TRAIN_CONFIG_COS_KEY={os.getenv('TRAIN_CONFIG_COS_KEY','<unset>')}", flush=True)
    print(f"[BOOT] TENCENT_SECRET_ID len={len(os.getenv('TENCENT_SECRET_ID',''))}", flush=True)

    parser = argparse.ArgumentParser(description="QuantMind Training — YAML config driven")
    parser.add_argument("--config", required=False, help="Path to config.yaml")
    parser.add_argument("--config-cos-key", required=False, help="COS key of config.yaml")
    try:
        args, unknown_args = parser.parse_known_args()
    except SystemExit as exc:
        if int(getattr(exc, "code", 1) or 0) == 0:
            return 0
        # Batch 运行时偶发注入畸形参数（如缺失值的已知 flag）会触发 argparse 退出码 2。
        # 这里降级为环境变量驱动启动，避免任务在入口阶段直接失败。
        logger.warning(f"Argparse failed with argv={sys.argv}; fallback to env-driven args")
        args = argparse.Namespace(config=None, config_cos_key=os.getenv("TRAIN_CONFIG_COS_KEY", ""))
        unknown_args = []
    if unknown_args:
        logger.warning(f"Ignoring unknown CLI args from runtime: {unknown_args}")

    # 完全依赖环境变量（Batch 通过 EnvVars 注入），CLI 参数作为可选覆盖
    cfg_path = Path(args.config) if args.config else Path("/tmp/config.yaml")
    config_cos_key = (args.config_cos_key or os.getenv("TRAIN_CONFIG_COS_KEY", "")).strip()

    run_id     = "unknown"
    cos_prefix = "models/candidates/unknown/"
    result_cos_key = f"{cos_prefix}result.json"
    result: dict = {}
    callback_url    = ""
    callback_secret = ""
    result_path = Path("/workspace/result.json")

    try:
        # config 下载放入 try/except，避免 COS 异常导致 uncaught exit
        if config_cos_key:
            cfg_path = _download_config_from_cos(config_cos_key, cfg_path)

        if not cfg_path.exists():
            raise RuntimeError(f"Config file not found: {cfg_path}")
        cfg = yaml.safe_load(cfg_path.read_text())
        _attach_training_file_logger(str((cfg.get("output") or {}).get("log_path") or _TRAINING_LOG_FILE))

        run_id          = cfg.get("run_id", "unknown")
        job_name        = cfg.get("job_name", "unnamed")
        result_path     = Path(cfg.get("output", {}).get("result_path", "/workspace/result.json"))
        cos_prefix      = cfg.get("output", {}).get("cos_prefix", "models/candidates/{run_id}/").format(run_id=run_id)
        callback_url    = cfg.get("callback", {}).get("url", "")
        callback_secret = cfg.get("callback", {}).get("secret", "")

        logger.info("=== QuantMind Training Start ===")
        logger.info(f"run_id={run_id}  job={job_name}  config={cfg_path}")
        # 数据加载（特征列严格遵循配置）
        features = list(dict.fromkeys([str(item).strip() for item in (cfg["data"].get("features", []) or []) if str(item).strip()]))
        source_mode = str((cfg.get("data", {}) or {}).get("source_mode") or "COS").strip().upper()
        local_data_dir = str((cfg.get("data", {}) or {}).get("local_dir") or "").strip() or None
        explain_cfg = _normalize_explain_cfg((cfg.get("explain") or {}))

        df, valid_features = load_data(
            cfg["data"]["train_start"],
            cfg["data"]["train_end"],
            features,
            target_horizon_days=int((cfg.get("label", {}) or {}).get("target_horizon_days") or 1),
            cache_dir=cfg.get("cache", {}).get("dir"),
            valid_end=cfg.get("split", {}).get("valid", [None, None])[1],
            test_end=cfg.get("split", {}).get("test", [None, None])[1],
            source_mode=source_mode,
            local_dir=local_data_dir,
            limit_up_weight=float(cfg.get("context", {}).get("limit_up_weight", 0.5)),
        )
        cos    = _cos_client()
        bucket = os.getenv("TENCENT_BUCKET", "quantmind-1255718505")
        train_t0 = time.time()
        model, fill_values, train_m, val_m, test_m, pred_df, split_frames = train_model(df, valid_features, cfg)
        elapsed = float(time.time() - train_t0)
        logger.info("Training finished in %.2fs, best_iteration=%s", elapsed, model.best_iteration)

        # 保存模型
        model_path = Path("/workspace/model.lgb")
        model.save_model(str(model_path))
        logger.info(f"Model saved to {model_path}")

        # 保存预测结果（parquet 压缩用于存档，比 pickle 小 ~10x）
        pred_path = Path("/workspace/pred.parquet")
        pred_df.to_parquet(pred_path, engine="pyarrow", compression="zstd", index=False)
        logger.info(f"Predictions saved to {pred_path} ({pred_path.stat().st_size/1024/1024:.1f} MB)")

        # 同时保存回测引擎兼容格式 pred.pkl
        # 回测引擎要求: MultiIndex(datetime, instrument) + 'score' 列
        pred_qlib = (
            pred_df[["trade_date", "symbol", "pred"]]
            .rename(columns={"trade_date": "datetime", "symbol": "instrument", "pred": "score"})
            .assign(datetime=lambda d: pd.to_datetime(d["datetime"]))
            .set_index(["datetime", "instrument"])
            .sort_index()
        )
        pred_pkl_path = Path("/workspace/pred.pkl")
        pred_qlib.to_pickle(pred_pkl_path)
        logger.info(f"Backtest-compatible pred.pkl saved ({pred_pkl_path.stat().st_size/1024/1024:.1f} MB, {len(pred_qlib):,} rows)")

        shap_summary_path = Path("/workspace/shap_summary.csv")
        shap_info = _compute_shap_summary(
            model=model,
            split_frames=split_frames,
            features=valid_features,
            fill_values=fill_values,
            explain_cfg=explain_cfg,
            out_path=shap_summary_path,
        )
        if shap_info.get("status") == "completed":
            logger.info(
                "SHAP summary generated: split=%s rows=%s -> %s",
                shap_info.get("split"),
                shap_info.get("rows_used"),
                shap_summary_path,
            )
        elif shap_info.get("status") == "disabled":
            logger.info("SHAP summary disabled by config")
        elif shap_info.get("status") == "skipped":
            logger.warning("SHAP summary skipped: %s", shap_info.get("error") or "unknown")
        else:
            logger.warning("SHAP summary failed: %s", shap_info.get("error") or "unknown")

        # 构造 metadata
        cos_prefix_str = cos_prefix  # 供定时上传任务使用
        metadata = {
            "run_id": run_id, "job_name": job_name,
            "framework": "lightgbm",
            "model_type": cfg.get("model", {}).get("type", "lightgbm"),
            "model_file": "model.lgb",
            "feature_count": len(valid_features),
            "requested_feature_count": len(features),
            "requested_features": features,
            "auto_appended_feature_count": 0,
            "auto_appended_features": [],
            "features": valid_features,
            "feature_columns": valid_features,
            "fill_values": fill_values,
            "train_start": cfg["data"]["train_start"],
            "train_end":   cfg["data"]["train_end"],
            "val_start":   (cfg.get("split", {}).get("valid") or [None, None])[0] or "",
            "val_end":     (cfg.get("split", {}).get("valid") or [None, None])[1] or "",
            "test_start":  (cfg.get("split", {}).get("test")  or [None, None])[0] or "",
            "test_end":    (cfg.get("split", {}).get("test")  or [None, None])[1] or "",
            "data_source": "parquet",
            "best_iteration": model.best_iteration,
            "target_horizon_days": int((cfg.get("label", {}) or {}).get("target_horizon_days") or 1),
            "target_mode": str((cfg.get("label", {}) or {}).get("target_mode") or "return"),
            "label_formula": str((cfg.get("label", {}) or {}).get("label_formula") or ""),
            "effective_trade_date": str((cfg.get("label", {}) or {}).get("effective_trade_date") or ""),
            "training_window": str((cfg.get("label", {}) or {}).get("training_window") or ""),
            "metrics": {
                "train_ic": train_m["ic"], "train_rank_ic": train_m["rank_ic"], "train_rank_icir": train_m["rank_icir"],
                "val_ic": val_m["ic"], "val_rank_ic": val_m["rank_ic"], "val_rank_icir": val_m["rank_icir"],
                "test_ic": test_m["ic"], "test_rank_ic": test_m["rank_ic"], "test_rank_icir": test_m["rank_icir"],
            },
            "pred_coverage_start": str(pred_df["trade_date"].min().date()) if not pred_df.empty else "",
            "pred_coverage_end": str(pred_df["trade_date"].max().date()) if not pred_df.empty else "",
            "pred_rows": int(len(pred_df)),
            "shap": shap_info,
            "generated_at": datetime.utcnow().isoformat(),
            "elapsed_seconds": elapsed,
        }
        metadata_safe = _json_safe_value(metadata)
        metadata_bytes = json.dumps(metadata_safe, ensure_ascii=False, indent=2).encode()
        Path("/workspace/metadata.json").write_bytes(metadata_bytes)
        logger.info("metadata.json saved locally")

        # 自动生成推理脚本 inference.py
        _INFERENCE_SCRIPT = _load_inference_template()
        Path("/workspace/inference.py").write_text(_INFERENCE_SCRIPT, encoding="utf-8")
        logger.info("inference.py generated in model directory")

        # 写入 cos_upload_pending.json，供凌晨定时任务批量上传
        upload_manifest = {
            "cos_prefix": cos_prefix_str,
            "bucket": bucket,
            "files": [
                {"local": "model.lgb",      "key": cos_prefix_str + "model.lgb",      "content_type": "application/octet-stream"},
                {"local": "pred.parquet",   "key": cos_prefix_str + "pred.parquet",   "content_type": "application/octet-stream"},
                {"local": "pred.pkl",       "key": cos_prefix_str + "pred.pkl",       "content_type": "application/octet-stream"},
                {"local": "metadata.json",  "key": cos_prefix_str + "metadata.json",  "content_type": "application/json"},
                {"local": "config.yaml",    "key": cos_prefix_str + "config.yaml",    "content_type": "application/x-yaml"},
                {"local": "result.json",    "key": cos_prefix_str + "result.json",    "content_type": "application/json"},
            ],
            "created_at": datetime.utcnow().isoformat(),
            "uploaded": False,
        }
        if shap_info.get("status") == "completed" and shap_summary_path.exists():
            upload_manifest["files"].append(
                {
                    "local": "shap_summary.csv",
                    "key": cos_prefix_str + "shap_summary.csv",
                    "content_type": "text/csv",
                }
            )
        Path("/workspace/cos_upload_pending.json").write_text(
            json.dumps(upload_manifest, ensure_ascii=False, indent=2)
        )
        logger.info("cos_upload_pending.json written, artifacts will be uploaded by nightly job")

        result = {
            "status": "completed",
            "run_id": run_id,
            "job_name": job_name,
            "metrics": {
                "train": {"rmse": train_m["rmse"], "auc": train_m["auc"]},
                "val": {"rmse": val_m["rmse"], "auc": val_m["auc"]},
                "test": {"rmse": test_m["rmse"], "auc": test_m["auc"]},
            },
            "artifacts": [
                {"name": "model.lgb",     "local": "/workspace/model.lgb"},
                {"name": "pred.parquet",  "local": "/workspace/pred.parquet"},
                {"name": "metadata.json", "local": "/workspace/metadata.json"},
                {"name": "inference.py",  "local": "/workspace/inference.py"},
                {"name": "config.yaml",   "local": "/workspace/config.yaml"},
                {"name": "result.json",   "local": "/workspace/result.json"},
            ],
            "summary": {
                "status": "训练完成",
                "message": f"训练完成，best_iteration={model.best_iteration}，产物已保存本地，待凌晨3:00上传至COS",
            },
            "metadata": metadata,
            "error": "",
            "logs": f"val_rmse={val_m['rmse']:.6f}, val_auc={val_m['auc']:.6f}",
        }
        if shap_info.get("status") == "completed" and shap_summary_path.exists():
            result["artifacts"].append({"name": "shap_summary.csv", "local": "/workspace/shap_summary.csv"})

    except Exception as e:
        logger.exception(f"Training failed: {e}")
        result = {"status": "failed", "run_id": run_id, "error": str(e)}

    finally:
        result_path.parent.mkdir(parents=True, exist_ok=True)
        result_safe = _json_safe_value(result)
        nonfinite_paths = _find_nonfinite_paths(result_safe)
        if nonfinite_paths:
            logger.warning("Non-finite values remain after sanitization: %s", ", ".join(nonfinite_paths[:20]))
        result_json = json.dumps(result_safe, ensure_ascii=False, indent=2, allow_nan=False, default=_json_default)
        result_path.write_text(result_json)
        logger.info(f"result.json → {result_path}")

        if callback_url:
            try:
                callback_headers = {
                    "X-Internal-Call-Secret": callback_secret,
                    "Content-Type": "application/json",
                }
                resp = requests.post(
                    callback_url,
                    data=result_json.encode("utf-8"),
                    headers=callback_headers,
                    timeout=15,
                )
                logger.info(f"Callback → HTTP {resp.status_code}")
            except Exception as cb_err:
                logger.warning(f"Callback failed (non-fatal): {cb_err}")

    logger.info("=== Training Complete ===")
    return 0 if result.get("status") == "completed" else 1


if __name__ == "__main__":
    sys.exit(main())
