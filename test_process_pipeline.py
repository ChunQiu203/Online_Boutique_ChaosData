#!/usr/bin/env python3
"""
=============================================================================
  端到端集成测试 — test_process_pipeline.py

  生成合成 Prometheus 指标数据 + chaos_history.json，然后运行完整的
  process_and_label_data.py 管道，验证输出 CSV 的正确性。

  运行:
      python test_process_pipeline.py
=============================================================================
"""

import json
import os
import sys
import tempfile
import uuid
from datetime import datetime, timedelta, timezone
from pathlib import Path

import numpy as np
import pandas as pd

# 将项目根目录加入 sys.path
PROJECT_ROOT = Path(__file__).resolve().parent
sys.path.insert(0, str(PROJECT_ROOT))

from process_and_label_data import MetricsDataProcessor


def generate_synthetic_metrics_file(
    experiment_id: str,
    service: str,
    fault_type: str,
    start_time: str,
    end_time: str,
    metrics_dir: Path,
) -> Path:
    """生成一个模拟的 Prometheus 指标 JSON 文件。

    为 4 种核心指标生成带噪声的正弦波时序数据，模拟真实监控信号：
      - container_cpu_usage
      - container_memory_usage
      - grpc_server_latency_p99
      - grpc_server_error_rate
    外加 2 个系统级指标：
      - total_request_rate
      - node_cpu_usage

    Args:
        experiment_id: 实验 UUID
        service:       目标微服务名
        fault_type:    故障类型
        start_time:    ISO 8601 UTC 开始时间
        end_time:      ISO 8601 UTC 结束时间
        metrics_dir:   指标文件输出目录

    Returns:
        生成的 JSON 文件路径
    """
    # 解析时间范围，生成每 30s 一个数据点（模拟 Prometheus step=30s）
    start_ts = pd.to_datetime(start_time, utc=True)
    end_ts = pd.to_datetime(end_time, utc=True)

    # 生成时间戳序列（每 30s 一个点）
    timestamps = pd.date_range(start=start_ts, end=end_ts, freq="30s")
    n_points = len(timestamps)
    unix_ts = [t.timestamp() for t in timestamps]

    # 基频 + 噪声
    rng = np.random.default_rng(42)  # 固定种子以保证可复现
    noise = rng.normal(0, 1, n_points)

    # 故障期间人为注入信号偏移（CPU、延迟、错误率升高；内存正常）
    cpu_base = 30.0 + 5 * np.sin(np.linspace(0, 4 * np.pi, n_points))
    cpu_values = cpu_base + noise * 3
    cpu_values = np.clip(cpu_values, 0, 100)  # CPU 使用率 0-100%

    mem_base = 512.0 + 50 * np.sin(np.linspace(0, 3 * np.pi, n_points))
    mem_values = mem_base + noise * 100
    mem_values = np.clip(mem_values, 100, 4096)  # MB

    latency_values = np.abs(
        0.05 + 0.1 * np.sin(np.linspace(0, 6 * np.pi, n_points)) + rng.gamma(2, 0.02, n_points)
    )

    error_values = np.abs(
        0.001 + 0.005 * np.sin(np.linspace(0, 4 * np.pi, n_points)) + rng.exponential(0.002, n_points)
    )
    error_values = np.clip(error_values, 0, 1)

    # 系统级指标
    sys_rps = np.abs(100 + 20 * np.sin(np.linspace(0, 2 * np.pi, n_points)) + rng.normal(0, 5, n_points))
    node_cpu = np.clip(20 + 3 * np.sin(np.linspace(0, 3 * np.pi, n_points)) + rng.normal(0, 2, n_points), 0, 100)

    # 构造 Prometheus range query 响应格式
    def make_metric_result(values: np.ndarray) -> dict:
        """将 numpy array 转为 Prometheus matrix result 格式。"""
        pairs = [[unix_ts[i], f"{float(values[i]):.6f}"] for i in range(n_points)]
        return {
            "data": {
                "resultType": "matrix",
                "result": [
                    {
                        "metric": {
                            "pod": f"{service}-d5f8b-abc12",
                            "namespace": "default",
                        },
                        "values": pairs,
                    }
                ],
            }
        }

    results = {
        "experiment_id": experiment_id,
        "fault_type": fault_type,
        "service": service,
        "query_range": {
            "start": start_time,
            "end": end_time,
        },
        "results": {
            "container_cpu_usage": {
                "description": "容器 CPU 使用率 (%)",
                "unit": "%",
                "query": f'rate(container_cpu_usage_seconds_total{{pod=~"{service}-.*"}}[5m]) * 100',
                **make_metric_result(cpu_values),
            },
            "container_memory_usage": {
                "description": "容器内存使用量 (MB)",
                "unit": "MB",
                "query": f'container_memory_usage_bytes{{pod=~"{service}-.*"}}',
                **make_metric_result(mem_values),
            },
            "grpc_server_latency_p99": {
                "description": "gRPC 服务端 P99 延迟 (秒)",
                "unit": "seconds",
                "query": f'histogram_quantile(0.99, sum(rate(grpc_server_handling_seconds_bucket{{grpc_service=~".*{service}.*"}}[5m])) by (le))',
                **make_metric_result(latency_values),
            },
            "grpc_server_error_rate": {
                "description": "gRPC 服务端错误率",
                "unit": "ratio",
                "query": f'sum(rate(grpc_server_handled_total{{grpc_service=~".*{service}.*",grpc_code!="OK"}}[5m]))',
                **make_metric_result(error_values),
            },
            "total_request_rate": {
                "description": "系统总请求速率 (rps)",
                "unit": "rps",
                "query": "sum(rate(http_server_request_duration_seconds_count[5m]))",
                **make_metric_result(sys_rps),
            },
            "node_cpu_usage": {
                "description": "节点 CPU 使用率 (%)",
                "unit": "%",
                "query": '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[5m])) * 100)',
                **make_metric_result(node_cpu),
            },
        },
    }

    metrics_dir.mkdir(parents=True, exist_ok=True)
    file_path = metrics_dir / f"metrics_{experiment_id}.json"
    with open(file_path, "w", encoding="utf-8") as f:
        json.dump(results, f, ensure_ascii=False, indent=2)

    return file_path


def generate_synthetic_history(
    experiments: list,
    history_path: Path,
) -> Path:
    """生成合成 chaos_history.json。

    Args:
        experiments: [{"fault_type": ..., "service": ..., "start_time": ..., "end_time": ...}, ...]
        history_path: 输出路径
    """
    records = []
    for i, exp in enumerate(experiments):
        records.append({
            "experiment_id": exp.get("experiment_id", str(uuid.uuid4())),
            "fault_category": exp.get("fault_category", "stress test"),
            "fault_type": exp.get("fault_type"),
            "instance_type": "service",
            "service": exp.get("service"),
            "instance": exp.get("service"),
            "source": "chaos-mesh",
            "destination": exp.get("service"),
            "start_time": exp.get("start_time"),
            "end_time": exp.get("end_time"),
            "repetition": i + 1,
            "yaml_file": f"deploy/chaos-experiments/test-{i:02d}.yaml",
            "status": "completed",
        })

    with open(history_path, "w", encoding="utf-8") as f:
        json.dump(records, f, ensure_ascii=False, indent=2)

    return history_path


def run_test():
    """执行端到端测试。"""
    # Windows 控制台 UTF-8 支持
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    print("=" * 70)
    print("  端到端集成测试 — process_and_label_data.py 管道验证")
    print("=" * 70)

    with tempfile.TemporaryDirectory() as tmpdir:
        tmp = Path(tmpdir)
        metrics_dir = tmp / "chaos_metrics"
        history_path = tmp / "chaos_history.json"
        output_path = tmp / "test_output.csv"

        # ── 构造 3 个模拟实验 ──────────────────────────────────────
        base_time = datetime(2026, 6, 6, 10, 0, 0, tzinfo=timezone.utc)

        test_experiments = [
            {
                "experiment_id": str(uuid.uuid4()),
                "fault_type": "cpu stress",
                "service": "cartservice",
                "fault_category": "stress test",
                "start_time": (base_time + timedelta(seconds=60)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_time": (base_time + timedelta(seconds=660)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            {
                "experiment_id": str(uuid.uuid4()),
                "fault_type": "network delay",
                "service": "frontend",
                "fault_category": "network attack",
                "start_time": (base_time + timedelta(seconds=2460)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_time": (base_time + timedelta(seconds=3060)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
            {
                "experiment_id": str(uuid.uuid4()),
                "fault_type": "pod kill",
                "service": "checkoutservice",
                "fault_category": "pod fault",
                "start_time": (base_time + timedelta(seconds=4260)).strftime("%Y-%m-%dT%H:%M:%SZ"),
                "end_time": (base_time + timedelta(seconds=4860)).strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        ]

        # 生成合成数据
        print("\n📂 生成合成测试数据 ...")
        for exp in test_experiments:
            fpath = generate_synthetic_metrics_file(
                experiment_id=exp["experiment_id"],
                service=exp["service"],
                fault_type=exp["fault_type"],
                start_time=exp["start_time"],
                end_time=exp["end_time"],
                metrics_dir=metrics_dir,
            )
            print(f"    ✓ {fpath.name}")

        generate_synthetic_history(test_experiments, history_path)
        print(f"    ✓ {history_path.name}")

        # 运行处理管道
        print("\n🚀 运行数据处理管道 ...\n")
        processor = MetricsDataProcessor(
            metrics_dir=metrics_dir,
            history_path=history_path,
            output_path=output_path,
            resample_rule="1S",
        )
        df = processor.run()

        # ── 断言验证 ──────────────────────────────────────────────
        print("\n🔍 验证断言 ...")
        errors = []

        # 1. 输出文件存在
        if not output_path.exists():
            errors.append("输出 CSV 文件未生成")
        else:
            print(f"  ✓ CSV 文件存在: {output_path.stat().st_size:,} bytes")

        # 2. DataFrame 不为空
        if df.empty:
            errors.append("DataFrame 为空")
        else:
            print(f"  ✓ DataFrame: {len(df)} 行 × {len(df.columns)} 列")

        # 3. 标签列存在
        for col in ["fault_type", "target_service"]:
            if col not in df.columns:
                errors.append(f"缺少列: {col}")
            else:
                print(f"  ✓ 标签列 '{col}' 存在: {df[col].nunique()} 唯一值")

        # 4. 非 normal 标签行数 > 0
        fault_rows = (df["fault_type"] != "normal").sum()
        if fault_rows == 0:
            errors.append("没有故障标签行，打标逻辑可能失败")
        else:
            print(f"  ✓ 故障标签行数: {fault_rows:,}")

        # 5. 时间索引为 DatetimeIndex
        if not isinstance(df.index, pd.DatetimeIndex):
            errors.append(f"索引类型错误: {type(df.index)}")
        else:
            print(f"  ✓ 索引类型: DatetimeIndex (UTC)")

        # 6. 列名格式校验 (service&metric)
        data_cols = [c for c in df.columns if c not in ("fault_type", "target_service")]
        malformed = [c for c in data_cols if "&" not in c]
        if malformed:
            errors.append(f"列名格式不符合 <服务>&<指标>: {malformed}")
        else:
            print(f"  ✓ 全部 {len(data_cols)} 列满足 '服务&指标' 命名规范")

        # 7. 时间步长一致性
        diffs = df.index.to_series().diff().dropna()
        if not (diffs == pd.Timedelta(seconds=1)).all():
            inconsistent = diffs[diffs != pd.Timedelta(seconds=1)]
            errors.append(f"时间步长不一致: {len(inconsistent)} 处异常")
        else:
            print(f"  ✓ 时间步长全部为 1 秒")

        # 8. NaN 检查 — 区分结构 NaN 与间隙 NaN
        #   - 结构 NaN: 服务在其首次实验之前的列（无前值可 ffill）— 正常
        #   - 间隙 NaN: 两段有效数据之间的 NaN（ffill 应对此失效）— 异常
        total_nan = df.isna().sum().sum()
        if total_nan > 0:
            data_cols = [c for c in df.columns if c not in ("fault_type", "target_service")]
            gap_nan_total = 0
            for col in data_cols:
                col_series = df[col]
                valid_mask = col_series.notna()
                if valid_mask.sum() == 0:
                    continue  # 全列空，跳过
                first_valid_idx = valid_mask.idxmax()
                last_valid_idx = valid_mask[valid_mask].index[-1]
                # 检查首尾有效值之间是否有 NaN
                between_mask = (df.index >= first_valid_idx) & (df.index <= last_valid_idx)
                gap_nan = col_series[between_mask].isna().sum()
                gap_nan_total += gap_nan

            if gap_nan_total > 0:
                errors.append(f"数据体有 {gap_nan_total} 个间隙 NaN（ffill 应已填充）")
            else:
                structural_nan = total_nan - gap_nan_total
                print(f"  ✓ 无间隙 NaN，仅有 {structural_nan} 个结构 NaN（服务实验窗口外无数据，正常）")
        else:
            print(f"  ✓ 无 NaN 值")

        # ── 结果汇总 ──────────────────────────────────────────────
        print("\n" + "=" * 70)
        if errors:
            print(f"  ❌ 测试失败 — {len(errors)} 个断言未通过:")
            for e in errors:
                print(f"     • {e}")
            print("=" * 70)
            sys.exit(1)
        else:
            print(f"  ✅ 全部断言通过！")
            print("=" * 70)

        # 打印标签分布
        print("\n📊 标签分布:")
        print(df["fault_type"].value_counts().to_string())
        print(f"\n📊 目标服务分布:")
        print(df["target_service"].value_counts().to_string())

        print(f"\n📋 输出列名 ({len(df.columns)} 列):")
        for i, col in enumerate(df.columns):
            print(f"    {i + 1:3d}. {col}")


if __name__ == "__main__":
    run_test()
