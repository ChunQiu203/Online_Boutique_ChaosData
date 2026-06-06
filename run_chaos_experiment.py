#!/usr/bin/env python3
"""
=============================================================================
  全自动化故障注入与监控采集系统  (v2 — 多服务多指标宽矩阵版)
  run_chaos_experiment.py

  功能：
  - 遍历 5 大类 11 种故障，每种故障连续重复执行 5 次实验
  - 单次实验生命周期：静默期(60s) → 注入故障 → 持续(10min) → 解除故障 → 恢复期(20min)
  - 精确记录每轮实验的 UTC 起止时间 (ISO 8601)
  - 实验元数据追加写入 chaos_history.json
  - ★ 实验结束后：全服务 × 多指标 PromQL 批量查询 → 宽矩阵合并 → 打标 → CSV

  环境要求：
  - Minikube 集群正在运行
  - ChaosMesh 已部署在 chaos-testing 命名空间
  - JMeter 持续压测流量作为背景
  - Prometheus 端口转发至 localhost:9090
  - Python 3.8+，依赖：requests, pandas, numpy

  用法：
      python run_chaos_experiment.py [--dry-run] [--prometheus-url URL]
      python run_chaos_experiment.py --step 15s --rate-window 30s
=============================================================================
"""

import argparse
import json
import os
import signal
import subprocess
import sys
import time
import uuid
from collections import OrderedDict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import numpy as np
import pandas as pd
import requests

# ============================================================================
# 颜色常量 (ANSI escape codes)
# ============================================================================
class Colors:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"
    BLACK = "\033[30m"
    RED = "\033[91m"
    GREEN = "\033[92m"
    YELLOW = "\033[93m"
    BLUE = "\033[94m"
    MAGENTA = "\033[95m"
    CYAN = "\033[96m"
    WHITE = "\033[97m"
    GRAY = "\033[90m"
    BG_YELLOW = "\033[43m"


# ============================================================================
# 项目根目录
# ============================================================================
PROJECT_ROOT = Path(__file__).resolve().parent


# ============================================================================
# 配置常量
# ============================================================================
PROMETHEUS_URL_DEFAULT = "http://localhost:9090"
TARGET_NAMESPACE = "default"
CHAOS_NAMESPACE = "chaos-testing"
DEFAULT_STEP = "15s"            # Prometheus 原生 scrape interval
DEFAULT_RATE_WINDOW = "30s"     # rate() 窗口 ≥ 2× step


# ============================================================================
# 实验生命周期时间常量 (秒)
# ============================================================================
PRE_INJECT_QUIET_SECONDS = 60
FAULT_DURATION_SECONDS = 600    # 10 分钟
COOLDOWN_SECONDS = 1200         # 20 分钟
REPETITIONS_PER_FAULT = 5


# ============================================================================
# Online Boutique 全部微服务
# ============================================================================
ALL_SERVICES = [
    "frontend", "cartservice", "productcatalogservice",
    "currencyservice", "paymentservice", "shippingservice",
    "checkoutservice", "emailservice", "recommendationservice",
    "adservice", "reviewservice",
]


# ============================================================================
# 故障矩阵 (Fault Matrix) — 5 大类 × 11 种故障
# ============================================================================
FAULT_MATRIX: Dict[str, List[Dict[str, str]]] = {
    "stress test": [
        {"fault_type": "cpu stress",    "yaml_file": "experiments/07-stress-cpu-cartservice.yaml",
         "instance_type": "service", "service": "cartservice", "instance": "cartservice",
         "source": "chaos-mesh-stresschaos", "destination": "cartservice"},
        {"fault_type": "memory stress", "yaml_file": "experiments/08-stress-memory-emailservice.yaml",
         "instance_type": "service", "service": "emailservice", "instance": "emailservice",
         "source": "chaos-mesh-stresschaos", "destination": "emailservice"},
    ],
    "network attack": [
        {"fault_type": "network corrupt", "yaml_file": "experiments/02-network-corrupt-productcatalog.yaml",
         "instance_type": "service", "service": "productcatalogservice", "instance": "productcatalogservice",
         "source": "chaos-mesh-networkchaos", "destination": "productcatalogservice"},
        {"fault_type": "network delay",   "yaml_file": "experiments/09-network-delay-cartservice.yaml",
         "instance_type": "service", "service": "cartservice", "instance": "cartservice",
         "source": "chaos-mesh-networkchaos", "destination": "cartservice"},
        {"fault_type": "network loss",    "yaml_file": "experiments/10-network-loss-checkout.yaml",
         "instance_type": "service", "service": "checkoutservice", "instance": "checkoutservice",
         "source": "chaos-mesh-networkchaos", "destination": "checkoutservice"},
    ],
    "pod fault": [
        {"fault_type": "pod failure", "yaml_file": "experiments/01-pod-failure-checkout.yaml",
         "instance_type": "service", "service": "checkoutservice", "instance": "checkoutservice",
         "source": "chaos-mesh-podchaos", "destination": "checkoutservice"},
        {"fault_type": "pod kill",    "yaml_file": "experiments/11-pod-kill-frontend.yaml",
         "instance_type": "service", "service": "frontend", "instance": "frontend",
         "source": "chaos-mesh-podchaos", "destination": "frontend"},
    ],
    "node fault": [
        {"fault_type": "node memory stress", "yaml_file": "experiments/04-node-memory-stress.yaml",
         "instance_type": "node", "service": "minikube", "instance": "minikube",
         "source": "chaos-mesh-nodechaos", "destination": "minikube-node"},
        {"fault_type": "node cpu stress",    "yaml_file": "experiments/03-node-cpu-stress.yaml",
         "instance_type": "node", "service": "minikube", "instance": "minikube",
         "source": "chaos-mesh-nodechaos", "destination": "minikube-node"},
    ],
    "jvm fault": [
        {"fault_type": "jvm cpu",     "yaml_file": "experiments/05-jvm-cpu-recommendation.yaml",
         "instance_type": "service", "service": "recommendationservice", "instance": "recommendationservice",
         "source": "chaos-mesh-jvmchaos", "destination": "recommendationservice"},
        {"fault_type": "jvm latency", "yaml_file": "experiments/06-jvm-latency-recommendation.yaml",
         "instance_type": "service", "service": "recommendationservice", "instance": "recommendationservice",
         "source": "chaos-mesh-jvmchaos", "destination": "recommendationservice"},
    ],
}


# ============================================================================
# PromQL 多路指标生成器 (移植自 chaos_quick_test.py v3)
# ============================================================================

def build_promql_metrics(
    services: List[str],
    rate_window: str = DEFAULT_RATE_WINDOW,
    label_mode: str = "pod",
) -> OrderedDict[str, str]:
    """动态生成全服务 × 多指标的 PromQL 查询字典。

    输出: {"frontend&cpu_usage": "sum(rate(...))", ...}
    """
    if label_mode == "container":
        label_clause = 'container=~".*{service}.*", namespace="{ns}"'
    else:
        label_clause = 'pod=~"{service}-.*", namespace="{ns}"'

    metric_templates: List[Tuple[str, str]] = [
        ("cpu_usage",
         'sum(rate(container_cpu_usage_seconds_total{{{label}}}[{rw}])) * 100'),
        ("mem_usage_mb",
         'sum(container_memory_working_set_bytes{{{label}}}) / 1024 / 1024'),
        ("mem_usage_pct",
         'sum(container_memory_working_set_bytes{{{label}}})'
         ' / sum(kube_pod_container_resource_limits{{resource="memory",{label}}}) * 100'),
        ("grpc_latency_p99",
         'histogram_quantile(0.99, '
         'sum(rate(grpc_server_handling_seconds_bucket{{'
         'grpc_service=~".*{service}.*"}}[{rw}])) by (le))'),
        ("grpc_error_rate",
         'sum(rate(grpc_server_handled_total{{'
         'grpc_service=~".*{service}.*",grpc_code!="OK"}}[{rw}]))'
         ' / sum(rate(grpc_server_handled_total{{'
         'grpc_service=~".*{service}.*"}}[{rw}]))'),
        ("grpc_rps",
         'sum(rate(grpc_server_handled_total{{'
         'grpc_service=~".*{service}.*"}}[{rw}]))'),
        ("pod_restarts",
         'sum(kube_pod_container_status_restarts_total{{'
         'namespace="{ns}",pod=~"{service}-.*"}})'),
    ]

    queries: OrderedDict[str, str] = OrderedDict()
    for svc in services:
        label_str = label_clause.format(service=svc, ns=TARGET_NAMESPACE)
        for suffix, template in metric_templates:
            col_name = f"{svc}&{suffix}"
            promql = template.replace("{label}", label_str)
            promql = promql.format(service=svc, ns=TARGET_NAMESPACE, rw=rate_window)
            promql = promql.replace("{service}", svc)
            queries[col_name] = promql

    # 系统级指标
    queries["system&total_rps"] = (
        'sum(rate(http_server_request_duration_seconds_count[{rw}])) + '
        'sum(rate(grpc_server_handled_total[{rw}]))'
    ).replace("{rw}", rate_window)
    queries["system&node_cpu_pct"] = (
        '100 - (avg by (instance) (rate(node_cpu_seconds_total{{mode="idle"}}[{rw}])) * 100)'
    ).replace("{rw}", rate_window)
    queries["system&node_mem_pct"] = (
        '(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100'
    )

    return queries


# ============================================================================
# 辅助工具函数
# ============================================================================

def cprint(text: str, color: str = "", bold: bool = False,
           prefix: str = "", end: str = "\n") -> None:
    parts = []
    if bold: parts.append(Colors.BOLD)
    if color: parts.append(color)
    parts.append(text)
    parts.append(Colors.RESET)
    full = "".join(parts)
    if prefix: full = f"{prefix} {full}"
    print(full, end=end, flush=True)


def get_utc_now_iso() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def timestamp() -> str:
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"


def countdown(seconds: int, label: str, color: str = Colors.CYAN) -> None:
    for remaining in range(seconds, 0, -1):
        mins, secs = divmod(remaining, 60)
        bar_w = 30
        progress = (seconds - remaining) / seconds if seconds > 0 else 1
        filled = int(bar_w * progress)
        bar = "█" * filled + "░" * (bar_w - filled)
        sys.stdout.write(
            f"\r{timestamp()} {color}{Colors.BOLD}[{label}]{Colors.RESET} "
            f"⏳ {mins:02d}:{secs:02d}  "
            f"│{bar}│ {progress*100:5.1f}%  "
        )
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 120 + "\r")
    sys.stdout.flush()
    cprint(f"[{label}] ✓ 完成 ({seconds}s)", color=Colors.GREEN, bold=True, prefix=timestamp())


def run_kubectl(args: List[str], dry_run: bool = False,
                timeout: int = 120) -> Tuple[bool, str, str]:
    cmd = ["kubectl"] + args
    cmd_str = " ".join(cmd)
    if dry_run:
        cprint(f"  [DRY-RUN] 将执行: {cmd_str}", color=Colors.GRAY)
        return True, "", ""
    try:
        result = subprocess.run(cmd, capture_output=True, text=True,
                                timeout=timeout, cwd=str(PROJECT_ROOT))
        if result.returncode != 0:
            cprint(f"  ✗ kubectl 失败: {cmd_str}", color=Colors.RED, prefix=timestamp())
            cprint(f"    {result.stderr.strip()[:300]}", color=Colors.RED)
            return False, result.stdout, result.stderr
        return True, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        cprint(f"  ✗ kubectl 超时 ({timeout}s): {cmd_str}", color=Colors.RED, prefix=timestamp())
        return False, "", f"Timeout after {timeout}s"
    except FileNotFoundError:
        cprint("  ✗ 未找到 kubectl，请确认已安装并加入 PATH", color=Colors.RED, prefix=timestamp())
        return False, "", "kubectl not found"
    except Exception as e:
        cprint(f"  ✗ kubectl 异常: {e}", color=Colors.RED, prefix=timestamp())
        return False, "", str(e)


def query_prom_range(query: str, start: str, end: str, step: str,
                     prometheus_url: str, timeout: int = 60) -> Optional[Dict[str, Any]]:
    """调用 Prometheus Range Query API，返回 data 字段或 None。"""
    url = f"{prometheus_url.rstrip('/')}/api/v1/query_range"
    params = {"query": query, "start": start, "end": end, "step": step}
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != "success":
            cprint(f"  PromQL error: {body.get('error','?')[:100]}", color=Colors.YELLOW, prefix=timestamp())
            return None
        return body["data"]
    except requests.RequestException as e:
        cprint(f"  Prometheus 请求异常: {e}", color=Colors.YELLOW, prefix=timestamp())
        return None
    except json.JSONDecodeError:
        cprint("  Prometheus 响应非 JSON", color=Colors.YELLOW, prefix=timestamp())
        return None


def prom_data_to_series(data: Dict[str, Any], col_name: str) -> pd.Series:
    """将 Prometheus matrix/vector 结果转为以 UTC datetime 为索引的 Series。"""
    result_type = data.get("resultType", "")
    results = data.get("result", [])
    if not results:
        return pd.Series(name=col_name, dtype=np.float64)

    pairs: List[Tuple[float, float]] = []
    if result_type == "matrix":
        for series in results:
            for ts, val_str in series.get("values", []):
                try:
                    pairs.append((float(ts), float(val_str)))
                except (ValueError, TypeError):
                    continue
    elif result_type == "vector":
        for series in results:
            v = series.get("value", [])
            if len(v) >= 2:
                try:
                    pairs.append((float(v[0]), float(v[1])))
                except (ValueError, TypeError):
                    continue

    if not pairs:
        return pd.Series(name=col_name, dtype=np.float64)

    df = pd.DataFrame(pairs, columns=["ts", "val"])
    df["timestamp"] = pd.to_datetime(df["ts"], unit="s", utc=True).dt.floor("S")
    s = df.groupby("timestamp")["val"].mean().rename(col_name).sort_index()
    return s


# ============================================================================
# ChaosExperimentRunner —— 核心实验编排器
# ============================================================================

class ChaosExperimentRunner:
    """全自动混沌工程实验运行器。

    核心管线:
      1. 遍历 FAULT_MATRIX 执行故障注入（保留原有逻辑）
      2. 记录 chaos_history.json
      3. 实验结束后：全服务 × 多指标 Prometheus 批量查询
      4. 宽矩阵合并 → 1s 对齐 → 打标 → 导出 CSV
    """

    def __init__(
        self,
        prometheus_url: str = PROMETHEUS_URL_DEFAULT,
        dry_run: bool = False,
        skip_prometheus: bool = False,
        step: str = DEFAULT_STEP,
        rate_window: str = DEFAULT_RATE_WINDOW,
        label_mode: str = "pod",
        output_csv: str = "chaos_dataset.csv",
    ):
        self.prometheus_url = prometheus_url
        self.dry_run = dry_run
        self.skip_prometheus = skip_prometheus
        self.data_step = step
        self.rate_window = rate_window
        self.label_mode = label_mode
        self.output_csv = Path(output_csv)

        self.history_file = PROJECT_ROOT / "chaos_history.json"
        self.interrupted = False

        self.total_experiments = sum(
            len(faults) for faults in FAULT_MATRIX.values()
        ) * REPETITIONS_PER_FAULT
        self.completed_experiments = 0
        self.failed_experiments = 0

        # dry-run 模式下缩短倒计时
        self._cd_div = 30 if dry_run else 1

        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _signal_handler(self, signum: int, frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        cprint(f"\n⚠ 收到 {sig_name} 信号，将在当前实验完成后优雅退出...",
               color=Colors.YELLOW, bold=True)
        self.interrupted = True

    # ── chaos_history I/O ────────────────────────────────────────────

    def _load_history(self) -> List[Dict[str, Any]]:
        if self.history_file.exists():
            try:
                with open(self.history_file, "r", encoding="utf-8") as f:
                    return json.load(f)
            except (json.JSONDecodeError, IOError):
                pass
        return []

    def _append_history(self, record: Dict[str, Any]) -> None:
        history = self._load_history()
        history.append(record)
        with open(self.history_file, "w", encoding="utf-8") as f:
            json.dump(history, f, ensure_ascii=False, indent=2)

    def _resolve_yaml_path(self, yaml_file: str) -> Path:
        return PROJECT_ROOT / yaml_file

    # ── 单次实验 ────────────────────────────────────────────────────

    def _run_single_experiment(
        self, fault_category: str, fault_config: Dict[str, str],
        repetition: int, global_index: int,
    ) -> Optional[Dict[str, Any]]:
        fault_type = fault_config["fault_type"]
        yaml_file = fault_config["yaml_file"]
        yaml_path = self._resolve_yaml_path(yaml_file)

        header = (
            f"实验 #{global_index}/{self.total_experiments}  |  "
            f"类别: {fault_category}  |  "
            f"故障: {fault_type}  |  "
            f"第 {repetition}/{REPETITIONS_PER_FAULT} 次"
        )
        print()
        cprint("=" * 80, color=Colors.BLUE)
        cprint(f"  {header}", color=Colors.BOLD, bold=True)
        cprint("=" * 80, color=Colors.BLUE)
        cprint(f"  YAML: {yaml_file}", color=Colors.GRAY, prefix=timestamp())
        cprint(f"  目标: {fault_config['service']} ({fault_config['instance_type']})",
               color=Colors.GRAY, prefix=timestamp())

        # 阶段 1: 注入前静默
        pre_q = PRE_INJECT_QUIET_SECONDS // self._cd_div
        cprint(f"  阶段 1/5: 注入前静默期 ({pre_q}s) — JMeter 持续压测中...",
               color=Colors.CYAN, bold=True, prefix=timestamp())
        countdown(pre_q, "注入前静默", Colors.CYAN)

        # 阶段 2: 注入故障
        cprint("  阶段 2/5: 注入故障 → kubectl apply ...",
               color=Colors.MAGENTA, bold=True, prefix=timestamp())
        if not yaml_path.exists():
            cprint(f"  ✗ YAML 文件不存在: {yaml_path}", color=Colors.RED, prefix=timestamp())
            self.failed_experiments += 1
            return None
        success, _, _ = run_kubectl(["apply", "-f", str(yaml_path)], dry_run=self.dry_run)
        if not success:
            cprint("  ✗ 故障注入失败，跳过本轮", color=Colors.RED, bold=True, prefix=timestamp())
            self.failed_experiments += 1
            return None
        start_time = get_utc_now_iso()
        cprint(f"  ✓ 注入成功 — start_time = {start_time}",
               color=Colors.GREEN, bold=True, prefix=timestamp())

        # 阶段 3: 故障持续
        fault_d = FAULT_DURATION_SECONDS // self._cd_div
        cprint(f"  阶段 3/5: 故障持续中 ({fault_d}s / {fault_d // 60}min) ...",
               color=Colors.YELLOW, bold=True, prefix=timestamp())
        countdown(fault_d, "故障持续", Colors.YELLOW)

        # 阶段 4: 解除故障
        cprint("  阶段 4/5: 解除故障 → kubectl delete ...",
               color=Colors.MAGENTA, bold=True, prefix=timestamp())
        success, _, _ = run_kubectl(
            ["delete", "-f", str(yaml_path), "--ignore-not-found=true"],
            dry_run=self.dry_run)
        if not success:
            cprint("  ⚠ 故障解除命令执行异常（可能已自动过期）",
                   color=Colors.YELLOW, prefix=timestamp())
        end_time = get_utc_now_iso()
        cprint(f"  ✓ 故障已解除 — end_time = {end_time}",
               color=Colors.GREEN, bold=True, prefix=timestamp())

        # 记录元数据
        record = {
            "experiment_id": str(uuid.uuid4()),
            "fault_category": fault_category,
            "fault_type": fault_type,
            "instance_type": fault_config["instance_type"],
            "service": fault_config["service"],
            "instance": fault_config["instance"],
            "source": fault_config["source"],
            "destination": fault_config["destination"],
            "start_time": start_time,
            "end_time": end_time,
            "repetition": repetition,
            "yaml_file": yaml_file,
            "status": "completed",
        }
        self._append_history(record)
        self.completed_experiments += 1
        cprint(f"  ✓ 元数据已写入 chaos_history.json",
               color=Colors.GREEN, prefix=timestamp())

        # 阶段 5: 恢复期
        cool = COOLDOWN_SECONDS // self._cd_div
        cprint(f"  阶段 5/5: 系统恢复期 ({cool}s / {cool // 60}min) — 等待微服务回稳...",
               color=Colors.BLUE, bold=True, prefix=timestamp())
        countdown(cool, "系统恢复", Colors.BLUE)

        return record

    # ── ★ 核心重构: 全量指标采集 + 合并 + 打标 + 导出 ──────────────

    def _collect_and_export_dataset(self) -> None:
        """实验全部结束后:
          1. 构建全服务 × 多指标 PromQL 字典
          2. 确定完整时间范围 (首条记录 start → 末条记录 end + padding)
          3. 逐一查询 Prometheus Range API
          4. 横向合并为宽表 → resample(1s) → ffill → 打标
          5. 导出 CSV
        """
        if self.skip_prometheus:
            cprint("\n⏭ 跳过 Prometheus 指标采集 (--skip-prometheus)",
                   color=Colors.YELLOW, bold=True)
            return

        history = self._load_history()
        completed = [r for r in history if r.get("status") == "completed"]
        if not completed:
            cprint("\n⚠ chaos_history.json 无有效记录，跳过数据采集",
                   color=Colors.YELLOW, bold=True, prefix=timestamp())
            return

        # ── 1. 构建 PromQL 字典 ────────────────────────────────────
        cprint(f"\n{'=' * 80}", color=Colors.MAGENTA, bold=True)
        cprint(f"  全量指标采集: 构建 PromQL 矩阵 + 批量查询 + 宽表合并",
               color=Colors.BOLD, bold=True)
        cprint(f"{'=' * 80}", color=Colors.MAGENTA, bold=True)

        promql_dict = build_promql_metrics(
            services=ALL_SERVICES,
            rate_window=self.rate_window,
            label_mode=self.label_mode,
        )
        total_queries = len(promql_dict)
        cprint(f"  ✓ 生成 {total_queries} 条 PromQL "
               f"({len(ALL_SERVICES)} 服务 × 7 指标 + 3 系统指标)",
               color=Colors.GREEN, prefix=timestamp())

        # ── 2. 确定完整时间范围 ────────────────────────────────────
        all_starts = [r["start_time"] for r in completed]
        all_ends = [r["end_time"] for r in completed]
        full_start = min(all_starts)
        full_end = max(all_ends)
        # 前后各扩展 10 分钟以捕捉充分的故障前/后基线
        pad = pd.Timedelta(minutes=10)
        start_dt = pd.to_datetime(full_start, utc=True) - pad
        end_dt = pd.to_datetime(full_end, utc=True) + pad
        query_start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        query_end = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        cprint(f"  查询窗口: {query_start} → {query_end}",
               color=Colors.GRAY, prefix=timestamp())
        cprint(f"  步长: {self.data_step}  |  rate 窗口: {self.rate_window}",
               color=Colors.GRAY, prefix=timestamp())

        # ── 3. 批量查询 ────────────────────────────────────────────
        cprint(f"\n  正在查询 {total_queries} 路 PromQL ...",
               color=Colors.CYAN, bold=True, prefix=timestamp())

        all_series: Dict[str, pd.Series] = {}
        success_cnt, empty_cnt, error_cnt = 0, 0, 0

        for i, (col_name, promql) in enumerate(promql_dict.items()):
            if i % 20 == 0 or i == total_queries - 1:
                cprint(f"    [{i+1}/{total_queries}] {col_name:50s} ...", color=Colors.DIM)

            if self.dry_run:
                s = self._gen_synthetic(col_name, start_dt, end_dt)
            else:
                data = query_prom_range(promql, query_start, query_end,
                                        step=self.data_step,
                                        prometheus_url=self.prometheus_url)
                if data is None:
                    error_cnt += 1
                    continue
                s = prom_data_to_series(data, col_name)

            if s.empty:
                empty_cnt += 1
                continue
            all_series[col_name] = s
            success_cnt += 1
            # 避免压垮 Prometheus
            if not self.dry_run:
                time.sleep(0.1)

        cprint(f"  ✓ 查询完成: {success_cnt} 成功, {empty_cnt} 空, {error_cnt} 失败",
               color=Colors.GREEN, prefix=timestamp())
        cprint(f"  ✓ 有效列: {len(all_series)} (覆盖 "
               f"{len(set(k.split('&')[0] for k in all_series))} 个实体)",
               color=Colors.GREEN, prefix=timestamp())

        if not all_series:
            cprint("✗ 无任何有效指标数据，跳过 CSV 导出", color=Colors.RED, bold=True)
            return

        # ── 4. 宽矩阵合并 + 对齐 ──────────────────────────────────
        cprint(f"\n  构建宽矩阵 + 秒级对齐 ...", color=Colors.CYAN, bold=True, prefix=timestamp())

        df = pd.DataFrame(all_series).sort_index()
        cprint(f"    合并后: {len(df):,} 行 × {len(df.columns)} 列", color=Colors.WHITE)

        # 重采样到统一网格
        df = df.resample(self.data_step).mean()
        cprint(f"    resample({self.data_step}) 后: {len(df):,} 行", color=Colors.WHITE)

        # 前向填充（仅填充 Prometheus 偶发漏抓点）
        nan_before = df.isna().sum().sum()
        df = df.ffill().bfill()
        nan_after = df.isna().sum().sum()
        if nan_before > 0:
            cprint(f"    ffill: {nan_before:,} NaN → {nan_after:,} NaN", color=Colors.GREEN)
        else:
            cprint(f"    数据完整，无漏抓点", color=Colors.GREEN)

        # ── 5. 区间打标 ───────────────────────────────────────────
        cprint(f"\n  根据 {len(completed)} 条故障记录批量打标 ...",
               color=Colors.CYAN, bold=True, prefix=timestamp())

        df["fault_type"] = "normal"
        df["target_service"] = "none"
        labeled_total = 0

        for i, record in enumerate(completed):
            ftype = record.get("fault_type", "unknown")
            svc = record.get("service", "unknown")
            try:
                t0 = pd.to_datetime(record["start_time"], utc=True)
                t1 = pd.to_datetime(record["end_time"], utc=True)
            except (ValueError, TypeError):
                continue
            mask = (df.index >= t0) & (df.index <= t1)
            n = mask.sum()
            if n > 0:
                df.loc[mask, "fault_type"] = ftype
                df.loc[mask, "target_service"] = svc
                labeled_total += n
            if (i + 1) % 10 == 0 or (i + 1) == len(completed):
                cprint(f"    [{i+1}/{len(completed)}] {ftype:25s} → {svc:25s} ({n} 行)",
                       color=Colors.DIM)

        normal_cnt = (df["fault_type"] == "normal").sum()
        fault_cnt = (df["fault_type"] != "normal").sum()
        unique_ftypes = df["fault_type"].unique().tolist()
        cprint(f"  ✓ 打标完成: normal={normal_cnt:,}  fault={fault_cnt:,}  "
               f"类型={unique_ftypes}",
               color=Colors.GREEN, prefix=timestamp())

        # ── 6. 导出 CSV ───────────────────────────────────────────
        cprint(f"\n  导出 CSV → {self.output_csv} ...",
               color=Colors.CYAN, bold=True, prefix=timestamp())

        self.output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(self.output_csv, index=True, index_label="timestamp",
                  encoding="utf-8", float_format="%.6f")
        size_mb = self.output_csv.stat().st_size / (1024 * 1024)
        cprint(f"  ✓ 导出完成: {self.output_csv} ({size_mb:.1f} MB)",
               color=Colors.GREEN, bold=True, prefix=timestamp())
        cprint(f"    规格: {len(df):,} 行 × {len(df.columns)} 列  |  "
               f"步长: {self.data_step}  |  NaN: {nan_after:,}",
               color=Colors.WHITE, prefix=timestamp())

        # 打印列名一览
        data_cols = [c for c in df.columns if c not in ("fault_type", "target_service")]
        cprint(f"    数据列 ({len(data_cols)}):", color=Colors.WHITE)
        for col in data_cols[:5]:
            cprint(f"      {col}", color=Colors.GRAY)
        if len(data_cols) > 5:
            cprint(f"      ... 共 {len(data_cols)} 列", color=Colors.GRAY)

    def _gen_synthetic(self, col_name: str, start_dt: pd.Timestamp,
                       end_dt: pd.Timestamp) -> pd.Series:
        """dry-run 模式: 生成模拟 Prometheus 采集数据 (仅供管道验证用)。"""
        ts_index = pd.date_range(start=start_dt, end=end_dt, freq=self.data_step)
        n = len(ts_index)
        rng = np.random.default_rng(abs(hash(col_name)) % (2**31))
        parts = col_name.split("&", 1)
        metric = parts[1] if len(parts) > 1 else "unknown"
        t = np.linspace(0, 4 * np.pi, n)
        base = np.sin(t) * 0.3 + 0.5

        if "cpu" in metric:
            vals = 25 + base * 20 + rng.normal(0, 3, n)
        elif "mem" in metric:
            vals = 300 + base * 200 + rng.normal(0, 10, n)
        elif "latency" in metric:
            vals = 0.02 + base * 0.08 + rng.normal(0, 0.005, n)
        elif "error" in metric:
            vals = np.abs(0.001 + base * 0.005 + rng.normal(0, 0.001, n))
        elif "rps" in metric:
            vals = 50 + base * 80 + rng.normal(0, 5, n)
        elif "restart" in metric:
            vals = np.zeros(n)
        elif "node_cpu" in metric:
            vals = 30 + base * 30 + rng.normal(0, 2, n)
        elif "node_mem" in metric:
            vals = 45 + base * 20 + rng.normal(0, 1, n)
        else:
            vals = 50 + base * 30 + rng.normal(0, 3, n)

        # 注入 CPU 尖峰（模拟故障效果）
        if "cpu" in metric and not col_name.startswith("system&"):
            f0 = max(0, n // 3)
            f1 = min(n, f0 + max(1, 120 // 15))
            vals[f0:f1] += rng.uniform(20, 40, size=f1 - f0)

        return pd.Series(vals, index=ts_index, name=col_name)

    # ── 总览信息 ──────────────────────────────────────────────────

    def _print_summary(self) -> None:
        print()
        cprint("╔" + "═" * 78 + "╗", color=Colors.BLUE)
        cprint("║  全自动化混沌工程故障注入实验系统 (v2 宽矩阵版)" + " " * 21 + "║",
               color=Colors.BOLD, bold=True)
        cprint("╠" + "═" * 78 + "╣", color=Colors.BLUE)
        for category, faults in FAULT_MATRIX.items():
            cprint(f"║  ▸ {category.upper()}" + " " * (74 - len(category)),
                   color=Colors.CYAN, bold=True)
            for fault in faults:
                ft = fault["fault_type"]
                svc = fault["service"]
                cprint(f"║     {ft:25s} → {svc:25s} × {REPETITIONS_PER_FAULT} 次", color=Colors.WHITE)
            cprint("║" + " " * 78 + "║", color=Colors.BLUE)
        total = self.total_experiments
        single = PRE_INJECT_QUIET_SECONDS + FAULT_DURATION_SECONDS + COOLDOWN_SECONDS
        total_min = (total * single) / 60
        cprint(f"║  总计: {total} 次实验  |  单次: {single}s  |  预估: {total_min:.0f}min ≈ {total_min/60:.1f}h",
               color=Colors.YELLOW, bold=True)
        cprint(f"║  数据步长: {self.data_step}  |  rate窗口: {self.rate_window}  |  标签模式: {self.label_mode}",
               color=Colors.WHITE)
        cprint("╚" + "═" * 78 + "╝", color=Colors.BLUE)
        if self.dry_run:
            cprint("\n⚠ DRY-RUN 模式 — 不执行 kubectl，倒计时缩短",
                   color=Colors.BG_YELLOW + Colors.BLACK, bold=True)
        print()

    # ── 主循环 ────────────────────────────────────────────────────

    def run(self) -> None:
        self._print_summary()

        if not self.dry_run:
            cprint("⚠ 即将开始全自动故障注入实验，请确认：",
                   color=Colors.YELLOW, bold=True)
            cprint("   1. Minikube 集群正在运行", color=Colors.WHITE)
            cprint("   2. JMeter 压测流量已启动", color=Colors.WHITE)
            cprint("   3. ChaosMesh 已部署到 chaos-testing", color=Colors.WHITE)
            cprint(f"   4. Prometheus: {self.prometheus_url}", color=Colors.WHITE)
            print()

            missing_yamls = []
            for _, faults in FAULT_MATRIX.items():
                for fault in faults:
                    if not self._resolve_yaml_path(fault["yaml_file"]).exists():
                        missing_yamls.append(fault["yaml_file"])
            if missing_yamls:
                cprint("✗ 以下 YAML 文件缺失：", color=Colors.RED, bold=True)
                for mf in missing_yamls:
                    cprint(f"    - {mf}", color=Colors.RED)
                return
            cprint("✓ 所有 YAML 文件校验通过", color=Colors.GREEN, prefix=timestamp())
            print()

            try:
                inp = input(f"{timestamp()} 按 Enter 开始 (或输入 'q' 退出): ").strip()
                if inp.lower() == "q":
                    cprint("用户取消。", color=Colors.YELLOW, bold=True)
                    return
            except (EOFError, KeyboardInterrupt):
                cprint("\n用户取消。", color=Colors.YELLOW, bold=True)
                return

        batch_start = datetime.now()
        cprint(f"\n{'#' * 80}", color=Colors.GREEN, bold=True)
        cprint(f"  实验批次启动 — {batch_start.strftime('%Y-%m-%d %H:%M:%S')}",
               color=Colors.GREEN, bold=True)
        cprint(f"{'#' * 80}\n", color=Colors.GREEN, bold=True)

        global_index = 0
        for category, faults in FAULT_MATRIX.items():
            cprint(f"\n{'▬' * 80}", color=Colors.MAGENTA, bold=True)
            cprint(f"  故障大类: {category.upper()}  ({len(faults)} 种子类型)",
                   color=Colors.MAGENTA, bold=True)
            cprint(f"{'▬' * 80}", color=Colors.MAGENTA, bold=True)

            for fault_config in faults:
                cprint(f"\n  ▸ 子类型: {fault_config['fault_type']}  → 重复 {REPETITIONS_PER_FAULT} 次",
                       color=Colors.CYAN, bold=True, prefix=timestamp())

                for rep in range(1, REPETITIONS_PER_FAULT + 1):
                    if self.interrupted:
                        cprint("\n⚠ 收到中断信号，停止循环",
                               color=Colors.YELLOW, bold=True)
                        break
                    global_index += 1
                    try:
                        self._run_single_experiment(category, fault_config, rep, global_index)
                    except Exception as e:
                        cprint(f"  ✗ 实验异常: {e}", color=Colors.RED, bold=True, prefix=timestamp())
                        self.failed_experiments += 1

                if self.interrupted:
                    break
            if self.interrupted:
                break

        batch_end = datetime.now()
        elapsed = batch_end - batch_start
        print()
        cprint(f"{'#' * 80}", color=Colors.GREEN, bold=True)
        cprint(f"  实验批次结束", color=Colors.GREEN, bold=True)
        cprint(f"  开始: {batch_start.strftime('%Y-%m-%d %H:%M:%S')}", color=Colors.WHITE)
        cprint(f"  结束: {batch_end.strftime('%Y-%m-%d %H:%M:%S')}", color=Colors.WHITE)
        cprint(f"  总耗时: {elapsed}", color=Colors.WHITE)
        cprint(f"  完成: {self.completed_experiments}/{self.total_experiments}",
               color=Colors.GREEN if self.completed_experiments == self.total_experiments else Colors.YELLOW, bold=True)
        if self.failed_experiments > 0:
            cprint(f"  失败: {self.failed_experiments}", color=Colors.RED, bold=True)
        cprint(f"{'#' * 80}", color=Colors.GREEN, bold=True)

        # ── ★ 全量指标采集 + 合并 + 打标 + 导出 ──────────────────
        self._collect_and_export_dataset()

        # ── 最终总结 ───────────────────────────────────────────────
        print()
        cprint("╔" + "═" * 78 + "╗", color=Colors.GREEN)
        cprint("║  实验全部完成！" + " " * 60 + "║", color=Colors.BOLD, bold=True)
        cprint("╠" + "═" * 78 + "╣", color=Colors.GREEN)
        cprint(f"║  元数据文件:  chaos_history.json" + " " * 44 + "║", color=Colors.WHITE)
        if not self.skip_prometheus:
            cprint(f"║  数据集文件:  {self.output_csv}" + " " * (62 - len(str(self.output_csv))) + "║", color=Colors.WHITE)
        cprint("╚" + "═" * 78 + "╝", color=Colors.GREEN)
        print()


# ============================================================================
# 命令行入口
# ============================================================================

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="全自动化故障注入与监控采集系统 (v2 宽矩阵版)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python run_chaos_experiment.py                              # 正式运行
  python run_chaos_experiment.py --dry-run                    # 空跑验证管道
  python run_chaos_experiment.py --step 15s --rate-window 30s # 自定义精度
  python run_chaos_experiment.py --output my_dataset.csv      # 自定义输出
        """,
    )
    parser.add_argument("--dry-run", action="store_true",
                        help="空跑模式: 跳过 kubectl, 倒计时缩短, 生成模拟数据")
    parser.add_argument("--prometheus-url", default=PROMETHEUS_URL_DEFAULT,
                        help=f"Prometheus 地址 (默认: {PROMETHEUS_URL_DEFAULT})")
    parser.add_argument("--skip-prometheus", action="store_true",
                        help="跳过 Prometheus 数据采集与 CSV 导出")
    parser.add_argument("--step", default=DEFAULT_STEP,
                        help=f"Prometheus 采集步长 (默认: {DEFAULT_STEP})")
    parser.add_argument("--rate-window", default=DEFAULT_RATE_WINDOW,
                        help=f"PromQL rate() 窗口 (默认: {DEFAULT_RATE_WINDOW})")
    parser.add_argument("--label-mode", default="pod", choices=["pod", "container"],
                        help="PromQL 标签匹配方式 (默认: pod)")
    parser.add_argument("--output", default="chaos_dataset.csv",
                        help="输出 CSV 文件名 (默认: chaos_dataset.csv)")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    runner = ChaosExperimentRunner(
        prometheus_url=args.prometheus_url,
        dry_run=args.dry_run,
        skip_prometheus=args.skip_prometheus,
        step=args.step,
        rate_window=args.rate_window,
        label_mode=args.label_mode,
        output_csv=args.output,
    )
    runner.run()


if __name__ == "__main__":
    main()
