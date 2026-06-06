#!/usr/bin/env python3
"""
=============================================================================
  数据处理与清洗脚本 — process_and_label_data.py

  功能：
  1. 解析 chaos_metrics/ 目录下全部 Prometheus 区间查询结果 JSON 文件
  2. 将多路异构时序数据聚合、对齐到"每秒一行"的标准矩阵
  3. 列名规范化为 <服务名>&<指标名> 格式
  4. 根据 chaos_history.json 的故障时间窗对每一行打标 (fault_type, target_service)
  5. 导出清洗完毕的数据集 → final_dataset_for_algorithm.csv

  输入：
      chaos_metrics/metrics_*.json   — Prometheus range query 原始结果
      chaos_history.json             — 混沌实验元数据（起止时间、故障类型）

  输出：
      final_dataset_for_algorithm.csv — 清洗对齐后的标准 ML 就绪数据集

  使用方法：
      python process_and_label_data.py
      python process_and_label_data.py --metrics-dir chaos_metrics \
                                       --history chaos_history.json \
                                       --output final_dataset_for_algorithm.csv
=============================================================================
"""

import argparse
import json
import sys
import warnings
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd

# 抑制 Pandas 未来警告，保持输出整洁
warnings.filterwarnings("ignore", category=FutureWarning)

# ============================================================================
# 项目根目录
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent


# ============================================================================
# 指标元数据定义
# ============================================================================
# 与 run_chaos_experiment.py 中的 PROMETHEUS_METRICS 保持一致。
# system_metrics: 不带 {service} 占位符的系统全局指标，其列归属为 "system"。
# service_metrics: 带 {service} 占位符的微服务级别指标。
# ============================================================================

METRIC_DEFINITIONS: Dict[str, Dict[str, Any]] = {
    # ── 容器资源指标 ──────────────────────────────────────────────────
    "container_cpu_usage": {
        "query": 'rate(container_cpu_usage_seconds_total{namespace="default",pod=~"{service}-.*"}[5m]) * 100',
        "description": "容器 CPU 使用率 (%)",
        "is_system": False,
    },
    "container_memory_usage": {
        "query": 'container_memory_usage_bytes{namespace="default",pod=~"{service}-.*"}',
        "description": "容器内存使用量 (bytes)",
        "is_system": False,
    },
    "container_memory_working_set": {
        "query": 'container_memory_working_set_bytes{namespace="default",pod=~"{service}-.*"}',
        "description": "容器工作集内存 (bytes)",
        "is_system": False,
    },
    # ── HTTP 请求指标 ─────────────────────────────────────────────────
    "http_request_duration_p99": {
        "query": 'histogram_quantile(0.99, sum(rate(http_server_request_duration_seconds_bucket{job=~".*{service}.*"}[5m])) by (le))',
        "description": "HTTP 请求 P99 延迟 (秒)",
        "is_system": False,
    },
    "http_request_duration_p50": {
        "query": 'histogram_quantile(0.50, sum(rate(http_server_request_duration_seconds_bucket{job=~".*{service}.*"}[5m])) by (le))',
        "description": "HTTP 请求 P50 延迟 (秒)",
        "is_system": False,
    },
    "http_error_rate_5xx": {
        "query": 'sum(rate(http_server_request_duration_seconds_count{job=~".*{service}.*",status_code=~"5.."}[5m])) / sum(rate(http_server_request_duration_seconds_count{job=~".*{service}.*"}[5m]))',
        "description": "HTTP 5xx 错误率",
        "is_system": False,
    },
    # ── gRPC 服务端指标 ───────────────────────────────────────────────
    "grpc_server_latency_p99": {
        "query": 'histogram_quantile(0.99, sum(rate(grpc_server_handling_seconds_bucket{grpc_service=~".*{service}.*"}[5m])) by (le))',
        "description": "gRPC 服务端 P99 延迟 (秒)",
        "is_system": False,
    },
    "grpc_server_latency_p50": {
        "query": 'histogram_quantile(0.50, sum(rate(grpc_server_handling_seconds_bucket{grpc_service=~".*{service}.*"}[5m])) by (le))',
        "description": "gRPC 服务端 P50 延迟 (秒)",
        "is_system": False,
    },
    "grpc_server_error_rate": {
        "query": 'sum(rate(grpc_server_handled_total{grpc_service=~".*{service}.*",grpc_code!="OK"}[5m])) / sum(rate(grpc_server_handled_total{grpc_service=~".*{service}.*"}[5m]))',
        "description": "gRPC 服务端错误率",
        "is_system": False,
    },
    "grpc_server_request_rate": {
        "query": 'sum(rate(grpc_server_handled_total{grpc_service=~".*{service}.*"}[5m]))',
        "description": "gRPC 服务端请求速率 (rps)",
        "is_system": False,
    },
    # ── gRPC 客户端指标 ───────────────────────────────────────────────
    "grpc_client_latency_p99": {
        "query": 'histogram_quantile(0.99, sum(rate(grpc_client_handling_seconds_bucket{grpc_service=~".*{service}.*"}[5m])) by (le))',
        "description": "gRPC 客户端 P99 延迟 (秒)",
        "is_system": False,
    },
    # ── Pod 状态指标 ──────────────────────────────────────────────────
    "pod_restart_count": {
        "query": 'sum(kube_pod_container_status_restarts_total{namespace="default",pod=~"{service}-.*"})',
        "description": "Pod 容器重启次数",
        "is_system": False,
    },
    "pod_ready_status": {
        "query": 'sum(kube_pod_status_ready{namespace="default",pod=~"{service}-.*"})',
        "description": "就绪 Pod 数量",
        "is_system": False,
    },
    # ── 系统全局指标 (不带 {service} 占位符) ──────────────────────────
    "total_request_rate": {
        "query": "sum(rate(http_server_request_duration_seconds_count[5m])) + sum(rate(grpc_server_handled_total[5m]))",
        "description": "系统总请求速率 (rps)",
        "is_system": True,
    },
    "node_cpu_usage": {
        "query": '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
        "description": "节点 CPU 使用率 (%)",
        "is_system": True,
    },
    "node_memory_usage": {
        "query": "(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100",
        "description": "节点内存使用率 (%)",
        "is_system": True,
    },
}

# 预期遍历的在线 Boutique 微服务列表（用于列完整性校验）
ALL_SERVICES = [
    "frontend",
    "cartservice",
    "productcatalogservice",
    "currencyservice",
    "paymentservice",
    "shippingservice",
    "checkoutservice",
    "emailservice",
    "recommendationservice",
    "adservice",
    "reviewservice",
]


# ============================================================================
# 辅助函数
# ============================================================================

def print_stage(msg: str, emoji: str = "▸") -> None:
    """打印处理阶段标题。"""
    print(f"\n{emoji} {msg}", flush=True)


def print_info(msg: str) -> None:
    """打印一般信息。"""
    print(f"    {msg}", flush=True)


def print_warn(msg: str) -> None:
    """打印警告。"""
    print(f"  ⚠ {msg}", flush=True)


def print_ok(msg: str) -> None:
    """打印成功信息。"""
    print(f"  ✓ {msg}", flush=True)


# ============================================================================
# 数据加载层
# ============================================================================

def load_chaos_history(history_path: Path) -> List[Dict[str, Any]]:
    """加载混沌实验历史元数据。

    Args:
        history_path: chaos_history.json 文件路径

    Returns:
        实验记录列表，按 start_time 升序排列
    """
    if not history_path.exists():
        raise FileNotFoundError(
            f"chaos_history.json 未找到: {history_path}\n"
            f"请先运行 run_chaos_experiment.py 采集实验数据。"
        )

    with open(history_path, "r", encoding="utf-8") as f:
        history = json.load(f)

    # 只保留成功完成的实验，并按时间排序
    completed = [r for r in history if r.get("status", "") == "completed"]
    completed.sort(key=lambda r: r.get("start_time", ""))

    if not completed:
        print_warn("chaos_history.json 中没有 status='completed' 的实验记录")

    return completed


def discover_metrics_files(metrics_dir: Path) -> List[Path]:
    """扫描 metrics 目录，返回所有 metrics_*.json 文件路径。

    Args:
        metrics_dir: 指标数据目录

    Returns:
        排序后的文件路径列表
    """
    if not metrics_dir.exists():
        raise FileNotFoundError(
            f"指标目录未找到: {metrics_dir}\n"
            f"请先运行 run_chaos_experiment.py 采集 Prometheus 指标。"
        )

    files = sorted(metrics_dir.glob("metrics_*.json"))
    if not files:
        print_warn(f"目录 {metrics_dir} 中没有找到 metrics_*.json 文件")

    return files


def parse_prometheus_values(
    result_data: Dict[str, Any],
) -> List[Tuple[float, float]]:
    """从单个 Prometheus range query 结果中提取 (timestamp, value) 对。

    处理两种 resultType:
      - "matrix":   result[].values[][] → [(ts, val), ...]
      - "vector":   result[].value[]     → 单点，转为单元素列表
      其他类型视为空。

    若同一时间戳有多个 series（如多个 Pod 副本），取均值。

    Args:
        result_data: Prometheus API 响应中的 data 字段

    Returns:
        [(unix_timestamp_seconds, float_value), ...] 按时间戳排序
    """
    result_type = result_data.get("resultType", "")
    results = result_data.get("result", [])

    if not results:
        return []

    raw_pairs: List[Tuple[float, float]] = []

    if result_type == "matrix":
        # 多时间点序列：values = [[ts, val_string], ...]
        for series in results:
            for ts, val_str in series.get("values", []):
                try:
                    raw_pairs.append((float(ts), float(val_str)))
                except (ValueError, TypeError):
                    continue

    elif result_type == "vector":
        # 瞬时向量：value = [ts, val_string]
        for series in results:
            val_entry = series.get("value", [])
            if len(val_entry) >= 2:
                try:
                    raw_pairs.append((float(val_entry[0]), float(val_entry[1])))
                except (ValueError, TypeError):
                    continue

    # 按时间戳聚合：同一秒多个 series 取均值
    if not raw_pairs:
        return []

    df_temp = pd.DataFrame(raw_pairs, columns=["ts", "val"])
    df_temp = df_temp.groupby("ts")["val"].mean().reset_index()
    df_temp = df_temp.sort_values("ts")

    return list(df_temp.itertuples(index=False, name=None))


def extract_service_from_metric(
    metric_name: str,
    file_service: str,
) -> str:
    """确定某条指标的归属服务名。

    规则：
      - 系统级指标 (is_system=True)  → "system"
      - 服务级指标                   → 使用指标文件记录的 service 字段

    Args:
        metric_name: 指标键名 (如 "container_cpu_usage")
        file_service: 指标 JSON 文件顶层记录的 service

    Returns:
        服务名或 "system"
    """
    meta = METRIC_DEFINITIONS.get(metric_name, {})
    if meta.get("is_system", False):
        return "system"
    return file_service


# ============================================================================
# 核心处理管道
# ============================================================================

class MetricsDataProcessor:
    """Prometheus 多路时序数据聚合、对齐、打标处理器。

    核心管线：
      1. 遍历 metrics JSON → 提取全部 (ts, val) → 按 (service, metric) 分组
      2. 每组构建 pandas Series，合并为宽表 DataFrame
      3. 统一 resample 到 1 秒粒度，forward fill 缺失值
      4. 依据 chaos_history 时间窗口批量打标
      5. 导出为 CSV
    """

    def __init__(
        self,
        metrics_dir: Path,
        history_path: Path,
        output_path: Path,
        resample_rule: str = "1S",
    ):
        """
        Args:
            metrics_dir:  Prometheus 指标 JSON 文件目录
            history_path: chaos_history.json 路径
            output_path:  输出 CSV 路径
            resample_rule: Pandas resample 规则（默认 "1S" = 每秒）
        """
        self.metrics_dir = metrics_dir
        self.history_path = history_path
        self.output_path = output_path
        self.resample_rule = resample_rule

        # 中间状态
        self.history: List[Dict[str, Any]] = []
        self.metrics_files: List[Path] = []
        # {(service, metric_name): [(unix_ts, float_val), ...]}
        self.raw_series: Dict[Tuple[str, str], List[Tuple[float, float]]] = defaultdict(list)
        self.df: Optional[pd.DataFrame] = None

        # 统计
        self.total_values = 0
        self.skipped_files = 0
        self.empty_metrics = 0

    # ── 阶段 1: 数据提取 ──────────────────────────────────────────────

    def extract_all(self) -> None:
        """遍历全部 metrics JSON 文件，提取时序数据到 raw_series 字典。"""
        self.metrics_files = discover_metrics_files(self.metrics_dir)
        self.history = load_chaos_history(self.history_path)

        print_stage(
            f"阶段 1/5: 扫描 {len(self.metrics_files)} 个 metrics 文件，提取时序数据 ...",
            "📂",
        )

        for i, fpath in enumerate(self.metrics_files):
            try:
                with open(fpath, "r", encoding="utf-8") as f:
                    data = json.load(f)
            except (json.JSONDecodeError, IOError) as e:
                print_warn(f"跳过损坏文件 {fpath.name}: {e}")
                self.skipped_files += 1
                continue

            file_service = data.get("service", "unknown")
            experiment_id = data.get("experiment_id", "?")[:8]
            results = data.get("results", {})

            file_value_count = 0
            for metric_name, metric_result in results.items():
                prom_data = metric_result.get("data")
                if not prom_data:
                    self.empty_metrics += 1
                    continue

                pairs = parse_prometheus_values(prom_data)
                if not pairs:
                    self.empty_metrics += 1
                    continue

                service = extract_service_from_metric(metric_name, file_service)
                self.raw_series[(service, metric_name)].extend(pairs)
                file_value_count += len(pairs)

            self.total_values += file_value_count

            # 进度输出（每 10 个文件打印一次，避免刷屏）
            if (i + 1) % 10 == 0 or (i + 1) == len(self.metrics_files):
                print_info(
                    f"已处理 {i + 1}/{len(self.metrics_files)} 文件，"
                    f"累计 {self.total_values:,} 个数据点"
                )

        series_count = len(self.raw_series)
        print_ok(
            f"提取完成: {series_count} 列 (service×metric), "
            f"{self.total_values:,} 个原始数据点, "
            f"跳过 {self.skipped_files} 个损坏文件, "
            f"{self.empty_metrics} 个空指标结果"
        )

    # ── 阶段 2: 列构建 ────────────────────────────────────────────────

    def _build_column_series(
        self,
        pairs: List[Tuple[float, float]],
        col_name: str,
    ) -> pd.Series:
        """将 (unix_ts, val) 对转为以 datetime 为索引的 pandas Series。

        处理要点：
        - Unix 时间戳 → UTC datetime
        - 同一秒内多个值时取均值
        - 按索引排序
        - Series 命名为规范列名

        Args:
            pairs: [(unix_timestamp, value), ...]
            col_name: 列名（如 "cartservice&container_cpu_usage"）

        Returns:
            pd.Series with DatetimeIndex (UTC), sorted
        """
        if not pairs:
            return pd.Series(name=col_name, dtype=np.float64)

        df_col = pd.DataFrame(pairs, columns=["ts", "val"])
        # Unix 秒级时间戳 → UTC datetime（floor 到秒）
        df_col["timestamp"] = pd.to_datetime(df_col["ts"], unit="s", utc=True)
        df_col["timestamp"] = df_col["timestamp"].dt.floor("S")

        # 同一秒内多条记录取均值
        df_col = df_col.groupby("timestamp")["val"].mean()

        # 转为 Series 并命名
        series = df_col.rename(col_name)
        series = series.sort_index()
        return series

    def build_dataframe(self) -> None:
        """将 raw_series 中的全部 (service, metric) 组转为宽表 DataFrame。

        列名格式: <服务名>&<指标名>
        例如: cartservice&container_cpu_usage, system&node_cpu_usage
        """
        if not self.raw_series:
            raise ValueError(
                "未提取到任何时序数据，请检查 metrics 文件是否有效。"
            )

        print_stage(
            f"阶段 2/5: 构建 {len(self.raw_series)} 列的时间对齐 DataFrame ...",
            "🔧",
        )

        columns: Dict[str, pd.Series] = {}
        skipped_empty = 0

        for (service, metric_name), pairs in self.raw_series.items():
            col_name = f"{service}&{metric_name}"
            series = self._build_column_series(pairs, col_name)
            if series.empty:
                skipped_empty += 1
                continue
            columns[col_name] = series

        if not columns:
            raise ValueError("所有列均为空，无法构建 DataFrame。")

        # 合并所有列：pandas 自动按时间戳索引对齐
        self.df = pd.DataFrame(columns)

        if skipped_empty > 0:
            print_info(f"跳过 {skipped_empty} 个空列")

        print_ok(
            f"DataFrame 构建完成: "
            f"{len(self.df.columns)} 列 × {len(self.df):,} 行 "
            f"({self.df.index.min()} → {self.df.index.max()})"
        )

    # ── 阶段 3: 重采样对齐 ────────────────────────────────────────────

    def resample_and_align(self) -> None:
        """将 DataFrame 重采样到统一秒级网格，前向填充缺失值。

        核心操作：
          .resample('1S').mean()  → 聚合到每秒平均值
          .ffill()                → 前一有效值填充 NaN

        系统指标和服务指标的缺失行为：
          - 系统指标（如 node_cpu_usage）在实验覆盖范围内连续存在 → ffill 有效
          - 服务指标仅在对应服务的实验窗口有数据 → ffill 将数据沿时间延伸
        """
        if self.df is None or self.df.empty:
            raise ValueError("DataFrame 为空，无法执行重采样。")

        print_stage(
            f"阶段 3/5: 重采样到 '{self.resample_rule}' 并前向填充 ...",
            "⏱",
        )

        original_rows = len(self.df)
        original_cols = len(self.df.columns)

        # 确保索引为 DatetimeIndex 且已排序
        if not isinstance(self.df.index, pd.DatetimeIndex):
            raise TypeError("DataFrame 索引不是 DatetimeIndex，请检查构建步骤。")

        self.df = self.df.sort_index()

        # 重采样：每秒一个桶，桶内多值取平均
        self.df = self.df.resample(self.resample_rule).mean()

        # 前向填充：用最近一次的有效值填充缺失秒
        before_nan = self.df.isna().sum().sum()
        self.df = self.df.ffill()
        after_nan = self.df.isna().sum().sum()

        new_rows = len(self.df)

        print_info(
            f"重采样前: {original_rows:,} 行 → 重采样后: {new_rows:,} 行 "
            f"(规则: {self.resample_rule})"
        )
        print_info(
            f"前向填充前 NaN 数: {before_nan:,} → "
            f"填充后残留 NaN: {after_nan:,}"
        )
        print_info(
            f"时间跨度: {self.df.index.min()} → {self.df.index.max()}, "
            f"总时长: {self.df.index.max() - self.df.index.min()}"
        )

        print_ok(f"时间对齐完成: {len(self.df):,} 行 × {len(self.df.columns)} 列")

    # ── 阶段 4: 故障区间打标 ──────────────────────────────────────────

    def apply_labels(self) -> None:
        """根据 chaos_history.json 的时间窗口对每一行打标。

        新增列:
          - fault_type:      默认 "normal"，故障窗口内为具体故障类型
          - target_service:  默认 "none"， 故障窗口内为目标服务名

        使用 pandas 布尔掩码逐条匹配，向量化操作保证高效。
        """
        if self.df is None or self.df.empty:
            raise ValueError("DataFrame 为空，无法打标。")

        print_stage(
            f"阶段 4/5: 根据 {len(self.history)} 条故障记录批量打标 ...",
            "🏷",
        )

        # 初始化默认标签列
        self.df["fault_type"] = "normal"
        self.df["target_service"] = "none"

        if not self.history:
            print_warn("chaos_history.json 无有效记录，所有行保持默认标签")
            return

        labeled_rows = 0
        overlapping_warnings = 0

        for i, record in enumerate(self.history):
            experiment_id = record.get("experiment_id", "?")[:8]
            fault_type = record.get("fault_type", "unknown")
            service = record.get("service", "unknown")
            start_str = record.get("start_time", "")
            end_str = record.get("end_time", "")

            if not start_str or not end_str:
                print_warn(
                    f"记录 {experiment_id} 缺少 start_time 或 end_time，跳过"
                )
                continue

            try:
                # 解析 UTC ISO 8601 → pandas Timestamp (UTC)
                start_ts = pd.to_datetime(start_str, utc=True)
                end_ts = pd.to_datetime(end_str, utc=True)
            except (ValueError, TypeError) as e:
                print_warn(f"记录 {experiment_id} 时间解析失败: {e}")
                continue

            # 布尔掩码：时间戳在 [start, end] 之间的行
            mask = (self.df.index >= start_ts) & (self.df.index <= end_ts)

            # 检测是否与已有标签重叠（之前实验已标记过的行）
            already_labeled = mask & (self.df["fault_type"] != "normal")
            if already_labeled.any():
                overlapping_warnings += already_labeled.sum()

            # 批量赋值
            match_count = mask.sum()
            if match_count > 0:
                self.df.loc[mask, "fault_type"] = fault_type
                self.df.loc[mask, "target_service"] = service
                labeled_rows += match_count

            # 每处理完一条记录输出简要信息
            print_info(
                f"[{i + 1}/{len(self.history)}] "
                f"exp={experiment_id}  "
                f"type={fault_type:25s}  "
                f"svc={service:25s}  "
                f"window={start_str} → {end_str}  "
                f"rows={match_count}"
            )

        # 统计标签分布
        normal_count = (self.df["fault_type"] == "normal").sum()
        fault_count = (self.df["fault_type"] != "normal").sum()
        unique_faults = self.df["fault_type"].unique().tolist()

        print_ok(
            f"打标完成: "
            f"normal={normal_count:,} 行, "
            f"fault={fault_count:,} 行, "
            f"故障类型={unique_faults}"
        )
        if overlapping_warnings > 0:
            print_warn(
                f"检测到 {overlapping_warnings} 个时间重叠行 "
                f"(已有标签被覆盖，请检查实验排期是否合理)"
            )

    # ── 阶段 5: 数据质量校验 ──────────────────────────────────────────

    def validate(self) -> None:
        """对最终 DataFrame 执行数据质量校验。"""
        if self.df is None or self.df.empty:
            raise ValueError("DataFrame 为空，无法校验。")

        print_stage("阶段 5/5: 数据质量校验 ...", "🔍")

        checks_passed = 0
        checks_total = 6

        # 1. 检查是否有残留 NaN
        total_nan = self.df.isna().sum().sum()
        if total_nan == 0:
            print_ok(f"NaN 检查通过: 无残留缺失值")
            checks_passed += 1
        else:
            nan_cols = self.df.columns[self.df.isna().any()].tolist()
            print_warn(
                f"残留 {total_nan} 个 NaN 值，涉及列: {nan_cols[:10]}"
                f"{'...' if len(nan_cols) > 10 else ''}"
            )

        # 2. 检查时间索引单调递增
        if self.df.index.is_monotonic_increasing:
            print_ok(f"时间索引单调递增: 通过")
            checks_passed += 1
        else:
            print_warn("时间索引不单调，建议排序后重试")

        # 3. 检查时间步长一致性
        diffs = self.df.index.to_series().diff().dropna()
        expected_step = pd.Timedelta(self.resample_rule)
        # 允许 2 秒误差（浮点取整）
        inconsistent_steps = diffs[
            (diffs < expected_step * 0.5) | (diffs > expected_step * 1.5)
        ]
        if len(inconsistent_steps) == 0:
            print_ok(f"时间步长一致性: 全部为 {expected_step}")
            checks_passed += 1
        else:
            print_warn(
                f"发现 {len(inconsistent_steps)} 处步长不一致: "
                f"min={inconsistent_steps.min()}, max={inconsistent_steps.max()}"
            )

        # 4. 检查标签列存在
        for col in ["fault_type", "target_service"]:
            if col in self.df.columns:
                print_ok(f"标签列 '{col}' 存在: {self.df[col].nunique()} 个唯一值")
                checks_passed += 1
            else:
                print_warn(f"标签列 '{col}' 缺失")

        # 5. 打印列名预览
        svc_cols = [c for c in self.df.columns if c not in ("fault_type", "target_service")]
        print_info(f"数据列总数: {len(svc_cols)}")
        print_info(f"列名示例: {svc_cols[:5]}{'...' if len(svc_cols) > 5 else ''}")

        # 6. 内存使用
        mem_mb = self.df.memory_usage(deep=True).sum() / (1024 * 1024)
        print_info(f"DataFrame 内存占用: {mem_mb:.1f} MB")

        print_ok(f"校验完成: {checks_passed}/{checks_total} 项通过")

    # ── 导出 ──────────────────────────────────────────────────────────

    def export(self) -> None:
        """将最终 DataFrame 导出为 CSV。"""
        if self.df is None or self.df.empty:
            raise ValueError("DataFrame 为空，无法导出。")

        print_stage(f"导出 CSV → {self.output_path}", "💾")

        # 确保输出目录存在
        self.output_path.parent.mkdir(parents=True, exist_ok=True)

        self.df.to_csv(
            self.output_path,
            index=True,            # 保留时间戳索引列
            index_label="timestamp",
            encoding="utf-8",
            float_format="%.6f",   # 控制浮点精度，减小文件体积
        )

        file_size_mb = self.output_path.stat().st_size / (1024 * 1024)
        print_ok(
            f"导出完成: {self.output_path} "
            f"({file_size_mb:.1f} MB, "
            f"{len(self.df):,} 行 × {len(self.df.columns)} 列)"
        )

    # ── 主流程 ────────────────────────────────────────────────────────

    def run(self) -> pd.DataFrame:
        """执行完整处理管线。

        Returns:
            处理完成的 pandas DataFrame
        """
        print("=" * 70)
        print("  数据处理与清洗管道 — MetricsDataProcessor")
        print("=" * 70)
        print_info(f"指标目录:   {self.metrics_dir}")
        print_info(f"历史文件:   {self.history_path}")
        print_info(f"输出文件:   {self.output_path}")
        print_info(f"重采样规则: {self.resample_rule}")

        # 管线执行
        self.extract_all()          # 1. 数据提取
        self.build_dataframe()      # 2. 列构建
        self.resample_and_align()   # 3. 重采样对齐
        self.apply_labels()         # 4. 故障打标
        self.validate()             # 5. 质量校验
        self.export()               # 6. 导出 CSV

        print("\n" + "=" * 70)
        print("  ✓ 全部处理步骤完成！")
        print("=" * 70)
        print(f"  输出: {self.output_path.resolve()}")
        print()

        return self.df


# ============================================================================
# 命令行入口
# ============================================================================

def parse_args() -> argparse.Namespace:
    """解析命令行参数。"""
    parser = argparse.ArgumentParser(
        description="数据处理与清洗脚本 — Prometheus 指标时间对齐 + 故障区间打标",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python process_and_label_data.py
  python process_and_label_data.py --output my_dataset.csv
  python process_and_label_data.py --metrics-dir chaos_metrics \\
                                   --history chaos_history.json \\
                                   --output final_dataset_for_algorithm.csv
        """,
    )
    parser.add_argument(
        "--metrics-dir",
        type=Path,
        default=PROJECT_ROOT / "chaos_metrics",
        help="Prometheus 指标 JSON 目录 (默认: chaos_metrics/)",
    )
    parser.add_argument(
        "--history",
        type=Path,
        default=PROJECT_ROOT / "chaos_history.json",
        help="混沌实验历史文件路径 (默认: chaos_history.json)",
    )
    parser.add_argument(
        "--output",
        type=Path,
        default=PROJECT_ROOT / "final_dataset_for_algorithm.csv",
        help="输出 CSV 文件路径 (默认: final_dataset_for_algorithm.csv)",
    )
    parser.add_argument(
        "--resample",
        default="1S",
        help="Pandas resample 规则 (默认: 1S = 每秒一行)",
    )
    return parser.parse_args()


def main() -> None:
    """主入口。"""
    args = parse_args()

    # Windows 控制台 UTF-8 支持
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    processor = MetricsDataProcessor(
        metrics_dir=args.metrics_dir,
        history_path=args.history,
        output_path=args.output,
        resample_rule=args.resample,
    )

    try:
        processor.run()
    except FileNotFoundError as e:
        print(f"\n❌ 文件缺失: {e}", file=sys.stderr)
        sys.exit(1)
    except ValueError as e:
        print(f"\n❌ 数据错误: {e}", file=sys.stderr)
        sys.exit(2)
    except Exception as e:
        print(f"\n❌ 未预期错误: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(3)


if __name__ == "__main__":
    main()
