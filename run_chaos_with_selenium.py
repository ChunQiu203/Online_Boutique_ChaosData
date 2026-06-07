#!/usr/bin/env python3
"""
=============================================================================
  run_chaos_with_selenium.py  —  Selenium 流量引擎 + 混沌实验 全自动闭环流水线
  v3.0  (chaos-engineering 主执行脚本)

  功能亮点:
    1. 后台异步拉起 Selenium (Headless Chrome)，死循环执行首页→浏览→加购→下单
    2. 扫描 experiments/*.yaml 自动发现故障，支持 --categories / --faults / --services 过滤
    3. 每种故障可指定 --repeat N 次重复
       生命周期: 静默(60s) → 注入故障(记录 start_time) → 持续(10min) → 解除(记录 end_time) → 冷却(20min)
    3. try...finally + signal/atexit 多层防护，确保 Selenium 无孤儿进程残留
    4. 实验结束后统一捞取 Prometheus 80 路指标 → outer join → 15s 重采样 → ffill/bfill → 打标 → CSV

  环境要求:
    - Minikube 集群运行中，Online Boutique 已部署
    - ChaosMesh 已部署至 chaos-testing 命名空间
    - Prometheus 端口转发至 localhost:9090
    - kubectl 端口转发 frontend → localhost:8080 (Selenium 需要)
    - Python 3.8+, Chrome 浏览器, 依赖见 requirements

  用法:
    cd chaos-engineering
    python run_chaos_with_selenium.py                           # 正式运行
    python run_chaos_with_selenium.py --dry-run                 # 空跑验证管道
    python run_chaos_with_selenium.py --skip-selenium           # 跳过 Selenium (仅混沌)
    python run_chaos_with_selenium.py --output my_dataset.csv   # 自定义输出路径
=============================================================================
"""

from __future__ import annotations

import argparse
import atexit
import json
import os
import platform
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

# ═══════════════════════════════════════════════════════════════════════════
# 项目路径常量
# ═══════════════════════════════════════════════════════════════════════════

SCRIPT_ROOT = Path(__file__).resolve().parent                              # chaos-engineering/
SELENIUM_DIR = SCRIPT_ROOT.parent / "online-boutique-course" / "test" / "selenium"  # Selenium 测试根目录
CHAOS_YAML_DIR = SCRIPT_ROOT / "experiments"                              # 故障 YAML 目录

# ═══════════════════════════════════════════════════════════════════════════
# 颜色常量 (ANSI)
# ═══════════════════════════════════════════════════════════════════════════

class Colors:
    RESET      = "\033[0m"
    BOLD       = "\033[1m"
    DIM        = "\033[2m"
    BLACK      = "\033[30m"
    RED        = "\033[91m"
    GREEN      = "\033[92m"
    YELLOW     = "\033[93m"
    BLUE       = "\033[94m"
    MAGENTA    = "\033[95m"
    CYAN       = "\033[96m"
    WHITE      = "\033[97m"
    GRAY       = "\033[90m"
    BG_RED     = "\033[41m"
    BG_GREEN   = "\033[42m"
    BG_YELLOW  = "\033[43m"

# ═══════════════════════════════════════════════════════════════════════════
# 配置常量
# ═══════════════════════════════════════════════════════════════════════════

PROMETHEUS_URL      = "http://localhost:9090"
FRONTEND_BASE_URL   = "http://127.0.0.1:8080"
TARGET_NAMESPACE    = "default"
CHAOS_NAMESPACE     = "chaos-testing"

# 实验生命周期
PRE_INJECT_QUIET_SECONDS  = 60       # 静默期
FAULT_DURATION_SECONDS    = 600      # 故障持续 (10min)
COOLDOWN_SECONDS          = 1200     # 冷却恢复 (20min)
REPETITIONS_PER_FAULT     = 1        # 默认每种故障重复次数 (--repeat 可覆盖)

# 数据参数
PROMETHEUS_STEP     = "5s"
PROMETHEUS_RATE_WIN = "30s"

# Selenium 参数
SELENIUM_BROWSER        = "edge"     # edge (Windows 自带) / chrome / firefox
SELENIUM_HEADLESS       = True
SELENIUM_LOG_FILE       = "selenium_traffic.log"
SELENIUM_DRIVER_PATH    = r"E:\edgedriver_win64"  # EdgeDriver 目录, 空字符串=自动查找

# ═══════════════════════════════════════════════════════════════════════════
# 微服务列表
# ═══════════════════════════════════════════════════════════════════════════

ALL_SERVICES = [
    "frontend", "cartservice", "productcatalogservice",
    "currencyservice", "paymentservice", "shippingservice",
    "checkoutservice", "emailservice", "recommendationservice",
    "adservice", "reviewservice",
]

# ═══════════════════════════════════════════════════════════════════════════
# 故障模板 — 定义故障类型 & 运行时随机选服务注入
# ═══════════════════════════════════════════════════════════════════════════

# 通用目标服务池 (排除 node/minikube，只选微服务)
TARGET_SERVICE_POOL = [
    "frontend", "cartservice", "productcatalogservice",
    "currencyservice", "paymentservice", "shippingservice",
    "checkoutservice", "emailservice", "recommendationservice",
    "adservice", "reviewservice",
]

# JVM 专属目标 (JVMChaos 只能打 Java 服务)
JVM_SERVICE_POOL = ["adservice"]

# 故障模板: 每项定义一个故障类型
# - yaml_template 中 {service} 会被替换为目标服务
# - 支持 "service" 字段: 若指定, 固定打该服务 (JVM 类); 否则从服务池随机选
FAULT_TEMPLATES: List[Dict[str, Any]] = [
    # ── Pod 类 ──
    {
        "fault_type": "pod-kill",
        "category": "pod-fault",
        "chaos_kind": "PodChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: PodChaos\n"
            "metadata:\n"
            "  name: pod-kill-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: pod-kill\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
        ),
    },
    {
        "fault_type": "pod-failure",
        "category": "pod-fault",
        "chaos_kind": "PodChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: PodChaos\n"
            "metadata:\n"
            "  name: pod-failure-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: pod-failure\n"
            "  mode: one\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
        ),
    },
    # ── 网络类 ──
    {
        "fault_type": "network-delay",
        "category": "network-attack",
        "chaos_kind": "NetworkChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: NetworkChaos\n"
            "metadata:\n"
            "  name: network-delay-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: delay\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  delay:\n"
            "    latency: \"500ms\"\n"
            "    jitter: \"100ms\"\n"
        ),
    },
    {
        "fault_type": "network-loss",
        "category": "network-attack",
        "chaos_kind": "NetworkChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: NetworkChaos\n"
            "metadata:\n"
            "  name: network-loss-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: loss\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  loss:\n"
            "    loss: \"10%\"\n"
        ),
    },
    {
        "fault_type": "network-corrupt",
        "category": "network-attack",
        "chaos_kind": "NetworkChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: NetworkChaos\n"
            "metadata:\n"
            "  name: network-corrupt-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: corrupt\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  corrupt:\n"
            "    corrupt: \"5%\"\n"
        ),
    },
    # ── 压力类 ──
    {
        "fault_type": "cpu-stress",
        "category": "stress-test",
        "chaos_kind": "StressChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: StressChaos\n"
            "metadata:\n"
            "  name: cpu-stress-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  stressors:\n"
            "    cpu:\n"
            "      workers: 2\n"
            "      load: 80\n"
        ),
    },
    {
        "fault_type": "memory-stress",
        "category": "stress-test",
        "chaos_kind": "StressChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: StressChaos\n"
            "metadata:\n"
            "  name: memory-stress-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  stressors:\n"
            "    memory:\n"
            "      workers: 1\n"
            "      size: \"256M\"\n"
        ),
    },
    # ── DNS 类 ──
    {
        "fault_type": "dns-error",
        "category": "dns-attack",
        "chaos_kind": "DNSChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: DNSChaos\n"
            "metadata:\n"
            "  name: dns-error-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: error\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  patterns:\n"
            "    - \"*.default.svc.cluster.local\"\n"
        ),
    },
    # ── 容器级 Pod 类 ──
    {
        "fault_type": "container-kill",
        "category": "pod-fault",
        "chaos_kind": "PodChaos",
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: PodChaos\n"
            "metadata:\n"
            "  name: container-kill-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: container-kill\n"
            "  mode: one\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  containerNames:\n"
            "    - server\n"
        ),
    },
    # ── JVM 类 (只能打 Java 服务, 固定 adservice) ──
    {
        "fault_type": "jvm-cpu",
        "category": "jvm-fault",
        "chaos_kind": "JVMChaos",
        "service": "adservice",   # JVMChaos 只能打 Java 服务
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: JVMChaos\n"
            "metadata:\n"
            "  name: jvm-cpu-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: stress\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  cpuCount: 2\n"
        ),
    },
    {
        "fault_type": "jvm-latency",
        "category": "jvm-fault",
        "chaos_kind": "JVMChaos",
        "service": "adservice",   # JVMChaos 只能打 Java 服务
        "yaml_template": (
            "apiVersion: chaos-mesh.org/v1alpha1\n"
            "kind: JVMChaos\n"
            "metadata:\n"
            "  name: jvm-latency-{service}\n"
            "  namespace: {chaos_ns}\n"
            "spec:\n"
            "  action: latency\n"
            "  mode: all\n"
            "  duration: {duration}\n"
            "  selector:\n"
            "    namespaces:\n"
            "      - {target_ns}\n"
            "    labelSelectors:\n"
            "      app: {service}\n"
            "  latency: 2000\n"
            "  class: hipstershop.AdService\n"
            "  method: getAds\n"
        ),
    },
]

# 故障持续时间 (ISO 8601 duration 格式，用于 YAML)
_FAULT_YAML_DURATION = "600s"


def build_fault_matrix(
    templates: List[Dict[str, Any]],
    services: Optional[List[str]] = None,
    rng: Optional[np.random.Generator] = None,
) -> Dict[str, List[Dict[str, Any]]]:
    """根据模板 + 目标服务池构建故障矩阵。

    每个模板运行一次:
      - 若模板指定了 "service" 固定值 (JVM 类), 直接使用
      - 否则从服务池中随机选取一个目标服务

    返回: { category: [{fault_type, service, yaml_content, ...}, ...], ... }
    """
    if rng is None:
        rng = np.random.default_rng()

    default_pool = services if services else list(TARGET_SERVICE_POOL)
    faults_by_cat: Dict[str, List[Dict[str, Any]]] = OrderedDict()

    for tmpl in templates:
        # 模板有固定 service → 直接用; 否则随机选
        if "service" in tmpl:
            svc = tmpl["service"]
        else:
            pool = default_pool
            svc = str(rng.choice(pool))
        yaml_content = tmpl["yaml_template"].format(
            service=svc,
            target_ns=TARGET_NAMESPACE,
            chaos_ns=CHAOS_NAMESPACE,
            duration=_FAULT_YAML_DURATION,
        )
        cat = tmpl["category"]
        entry = {
            "fault_type": tmpl["fault_type"],
            "service": svc,
            "chaos_kind": tmpl["chaos_kind"],
            "yaml_content": yaml_content,
            "instance_type": "service",
            "instance": svc,
            "source": f"chaos-mesh-{tmpl['chaos_kind'].lower()}",
            "destination": svc,
        }
        faults_by_cat.setdefault(cat, []).append(entry)

    return faults_by_cat


def list_available_faults() -> None:
    """--list: 打印故障模板列表和目标服务池。"""
    total = len(FAULT_TEMPLATES)
    random_cnt = sum(1 for t in FAULT_TEMPLATES if "service" not in t)
    fixed_cnt = total - random_cnt
    print(f"\n故障模板 ({total} 种):")
    if random_cnt:
        print(f"  (其中 {random_cnt} 种从服务池随机选目标)")
    if fixed_cnt:
        print(f"  (其中 {fixed_cnt} 种固定目标服务)\n")
    for t in FAULT_TEMPLATES:
        target = t.get("service", "🎲随机")
        print(f"  [{t['category']:16s}] {t['fault_type']:22s} → {target:25s} (kind={t['chaos_kind']})")
    print(f"\n通用目标服务池 ({len(TARGET_SERVICE_POOL)} 个):")
    print(f"  {', '.join(TARGET_SERVICE_POOL)}")
    print(f"\nJVM 专属 ({len(JVM_SERVICE_POOL)} 个, JVMChaos 只能打 Java):")
    print(f"  {', '.join(JVM_SERVICE_POOL)}")
    print(f"\n运行时: 随机类故障每次从服务池随机选, 固定类直接用指定服务。\n"
          f"  --services X,Y  限定通用目标服务池\n"
          f"  --repeat N      每种故障重复 N 次 (每次重新随机选)\n")

# ═══════════════════════════════════════════════════════════════════════════
# 80 路 PromQL 指标生成器
# ═══════════════════════════════════════════════════════════════════════════

def build_promql_metrics(
    services: List[str],
    rate_window: str = PROMETHEUS_RATE_WIN,
    label_mode: str = "pod",
) -> OrderedDict[str, str]:
    """动态生成全服务 × 多指标的 PromQL 查询字典 (~80 路)。

    每服务 7 个指标 + 3 个系统指标 = 11×7 + 3 = 80 路。
    """
    if label_mode == "container":
        label_clause = 'container=~".*{service}.*", namespace="{ns}"'
    else:
        label_clause = 'pod=~"{service}-.*", namespace="{ns}"'

    metric_templates: List[Tuple[str, str]] = [
        (
            "cpu_usage",
            'sum(rate(container_cpu_usage_seconds_total{{{label}}}[{rw}])) * 100',
        ),
        (
            "mem_usage_mb",
            'sum(container_memory_working_set_bytes{{{label}}}) / 1024 / 1024',
        ),
        (
            "mem_usage_pct",
            'sum(container_memory_working_set_bytes{{{label}}})'
            ' / sum(kube_pod_container_resource_limits{{resource="memory",{label}}}) * 100',
        ),
        (
            "grpc_latency_p99",
            'histogram_quantile(0.99, '
            'sum(rate(grpc_server_handling_seconds_bucket{{'
            'grpc_service=~".*{service}.*"}}[{rw}])) by (le))',
        ),
        (
            "grpc_error_rate",
            'sum(rate(grpc_server_handled_total{{'
            'grpc_service=~".*{service}.*",grpc_code!="OK"}}[{rw}]))'
            ' / sum(rate(grpc_server_handled_total{{'
            'grpc_service=~".*{service}.*"}}[{rw}]))',
        ),
        (
            "grpc_rps",
            'sum(rate(grpc_server_handled_total{{'
            'grpc_service=~".*{service}.*"}}[{rw}]))',
        ),
        (
            "pod_restarts",
            'sum(kube_pod_container_status_restarts_total{{'
            'namespace="{ns}",pod=~"{service}-.*"}})',
        ),
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

    # ── 3 路系统级全局指标 ──
    queries["system&total_rps"] = (
        'sum(rate(http_server_request_duration_seconds_count[{rw}])) + '
        'sum(rate(grpc_server_handled_total[{rw}]))'
    ).replace("{rw}", rate_window)
    queries["system&node_cpu_pct"] = (
        '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[{rw}])) * 100)'
    ).replace("{rw}", rate_window)
    queries["system&node_mem_pct"] = (
        '(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100'
    )

    return queries


# ═══════════════════════════════════════════════════════════════════════════
# 辅助工具函数
# ═══════════════════════════════════════════════════════════════════════════

def _ts() -> str:
    """当前本地时间戳字符串。"""
    return f"[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}]"


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
    """返回当前 UTC ISO 8601 时间字符串。"""
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def countdown(seconds: int, label: str, color: str = Colors.CYAN) -> None:
    """终端进度条倒计时。"""
    if seconds <= 0:
        return
    for remaining in range(seconds, 0, -1):
        mins, secs = divmod(remaining, 60)
        bar_w = 30
        progress = (seconds - remaining) / seconds if seconds > 0 else 1
        filled = int(bar_w * progress)
        bar = "█" * filled + "░" * (bar_w - filled)
        sys.stdout.write(
            f"\r{_ts()} {color}{Colors.BOLD}[{label}]{Colors.RESET} "
            f"⏳ {mins:02d}:{secs:02d}  "
            f"│{bar}│ {progress*100:5.1f}%  "
        )
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 120 + "\r")
    sys.stdout.flush()
    cprint(f"[{label}] ✓ 完成 ({seconds}s)", color=Colors.GREEN, bold=True, prefix=_ts())


def run_kubectl(args: List[str], dry_run: bool = False,
                timeout: int = 180) -> Tuple[bool, str, str]:
    """封装 kubectl 命令行调用，返回 (success, stdout, stderr)。"""
    cmd = ["kubectl"] + args
    cmd_str = " ".join(cmd)
    if dry_run:
        cprint(f"  [DRY-RUN] 将执行: {cmd_str}", color=Colors.GRAY)
        return True, "", ""
    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout,
            cwd=str(SCRIPT_ROOT),
        )
        if result.returncode != 0:
            cprint(f"  ✗ kubectl 失败: {cmd_str}", color=Colors.RED, prefix=_ts())
            cprint(f"    {result.stderr.strip()[:400]}", color=Colors.RED)
            return False, result.stdout, result.stderr
        return True, result.stdout, result.stderr
    except subprocess.TimeoutExpired:
        cprint(f"  ✗ kubectl 超时 ({timeout}s): {cmd_str}", color=Colors.RED, prefix=_ts())
        return False, "", f"Timeout after {timeout}s"
    except FileNotFoundError:
        cprint("  ✗ 未找到 kubectl，请确认已安装并加入 PATH", color=Colors.RED, prefix=_ts())
        return False, "", "kubectl not found"
    except Exception as e:
        cprint(f"  ✗ kubectl 异常: {e}", color=Colors.RED, prefix=_ts())
        return False, "", str(e)


# ═══════════════════════════════════════════════════════════════════════════
# Prometheus 区间查询
# ═══════════════════════════════════════════════════════════════════════════

def query_prom_range(query: str, start: str, end: str, step: str,
                     prometheus_url: str = PROMETHEUS_URL,
                     timeout: int = 60) -> Optional[Dict[str, Any]]:
    """调用 Prometheus Range Query API，返回 data 字段或 None。"""
    url = f"{prometheus_url.rstrip('/')}/api/v1/query_range"
    params = {"query": query, "start": start, "end": end, "step": step}
    try:
        resp = requests.get(url, params=params, timeout=timeout)
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != "success":
            cprint(f"  PromQL error: {body.get('error','?')[:120]}", color=Colors.YELLOW, prefix=_ts())
            return None
        return body["data"]
    except requests.RequestException as e:
        cprint(f"  Prometheus 请求异常: {e}", color=Colors.YELLOW, prefix=_ts())
        return None
    except json.JSONDecodeError:
        cprint("  Prometheus 响应非 JSON", color=Colors.YELLOW, prefix=_ts())
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

    df_tmp = pd.DataFrame(pairs, columns=["ts", "val"])
    df_tmp["timestamp"] = pd.to_datetime(df_tmp["ts"], unit="s", utc=True).dt.floor("s")
    s = df_tmp.groupby("timestamp")["val"].mean().rename(col_name).sort_index()
    return s


# ═══════════════════════════════════════════════════════════════════════════
# Selenium 流量引擎管理器
# ═══════════════════════════════════════════════════════════════════════════

class SeleniumTrafficEngine:
    """后台 Selenium Headless Chrome 流量引擎。

    通过 subprocess.Popen 拉起一个无限循环的 pytest 进程，
    模拟真实用户的"首页 → 浏览 → 加购 → 下单"完整业务链路。
    故障期间 Selenium 必然报错，外部通过 try-except 吞掉即可。
    """

    def __init__(
        self,
        base_url: str = FRONTEND_BASE_URL,
        browser: str = SELENIUM_BROWSER,
        headless: bool = SELENIUM_HEADLESS,
        log_file: str = SELENIUM_LOG_FILE,
        driver_path: str = SELENIUM_DRIVER_PATH,
    ):
        self.base_url = base_url
        self.browser = browser
        self.headless = headless
        self.driver_path = driver_path
        self.log_path = SCRIPT_ROOT / log_file
        self._process: Optional[subprocess.Popen] = None
        self._shutdown_flag = False

    # ── 构建 Selenium 启动命令 ──────────────────────────────────────

    def _build_cmd(self) -> List[str]:
        """构建启动 Selenium pytest 后台循环的命令 (纯 Python, 跨平台, 不依赖 bash)。"""
        driver_arg = ""
        if self.driver_path:
            driver_arg = f" --driver-path={self.driver_path}"
        pytest_args = (
            f"-v --browser={self.browser}{driver_arg} "
            f"--headless --base-url={self.base_url} "
            f"--tb=short --no-header -p no:warnings"
        )

        # 用 Python 自身做 while-true 循环，不依赖 bash
        # subprocess.run 可吞掉 pytest 的非零退出码
        loop_code = (
            f"import subprocess, sys, time, os\n"
            f"os.chdir(r'{SELENIUM_DIR}')\n"
            f'pytest_args = "{pytest_args}"\n'
            f"while True:\n"
            f"    try:\n"
            f"        subprocess.run(\n"
            f"            [sys.executable, '-m', 'pytest'] + pytest_args.split(),\n"
            f"            stderr=subprocess.STDOUT, timeout=900)\n"
            f"    except subprocess.TimeoutExpired:\n"
            f"        pass\n"
            f"    except Exception:\n"
            f"        pass\n"
            f"    print('[SELENIUM] done, restarting in 2s...', flush=True)\n"
            f"    time.sleep(2)\n"
        )
        return [sys.executable, "-c", loop_code]

    # ── 启动流量 ────────────────────────────────────────────────────

    def start(self) -> None:
        """后台启动 Selenium 流量引擎。"""
        if self._process is not None:
            cprint("⚠ Selenium 流量引擎已在运行中，跳过重复启动",
                   color=Colors.YELLOW, prefix=_ts())
            return

        cmd = self._build_cmd()
        cprint("🚀 启动 Selenium Headless 流量引擎 ...", color=Colors.CYAN, bold=True, prefix=_ts())
        cprint(f"   浏览器: {self.browser}  |  Headless: {self.headless}  |  "
               f"Base URL: {self.base_url}", color=Colors.GRAY, prefix=_ts())
        if self.driver_path:
            cprint(f"   驱动路径: {self.driver_path}", color=Colors.GRAY, prefix=_ts())
        cprint(f"   日志文件: {self.log_path}", color=Colors.GRAY, prefix=_ts())

        try:
            log_fh = open(self.log_path, "w", encoding="utf-8")
            log_fh.write(f"=== Selenium Traffic Engine Log ===\n")
            log_fh.write(f"Started: {get_utc_now_iso()}\n")
            log_fh.write(f"Browser: {self.browser}  Headless: {self.headless}\n")
            log_fh.write(f"Driver Path: {self.driver_path}\n")
            log_fh.write(f"Base URL: {self.base_url}\n")
            log_fh.write(f"Working Dir: {SELENIUM_DIR}\n")
            log_fh.write(f"Command: {' '.join(cmd)}\n")
            log_fh.write(f"{'=' * 60}\n\n")
            log_fh.flush()

            # Windows 下创建新进程组防止 Ctrl+C 级联杀死
            if platform.system() == "Windows":
                self._process = subprocess.Popen(
                    cmd,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    creationflags=subprocess.CREATE_NEW_PROCESS_GROUP,
                )
            else:
                self._process = subprocess.Popen(
                    cmd,
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    stdin=subprocess.DEVNULL,
                    preexec_fn=os.setsid,
                )

            # 给 pytest 一点时间初始化
            time.sleep(5)

            # 检查进程是否存活
            if self._process.poll() is not None:
                cprint("✗ Selenium 流量引擎启动后立即退出！请检查：", color=Colors.RED, prefix=_ts())
                cprint(f"    1. kubectl port-forward deployment/frontend 8080:8080 是否保持",
                       color=Colors.RED)
                cprint(f"    2. {self.browser} 浏览器是否已安装，驱动是否正确", color=Colors.RED)
                if self.driver_path:
                    cprint(f"       当前驱动路径: {self.driver_path}", color=Colors.RED)
                cprint(f"    3. 查看日志: {self.log_path}", color=Colors.RED)
            else:
                cprint(f"✓ Selenium 流量引擎已启动 (PID={self._process.pid})",
                       color=Colors.GREEN, bold=True, prefix=_ts())

        except FileNotFoundError:
            cprint("✗ 未找到 python，请确认环境配置",
                   color=Colors.RED, bold=True, prefix=_ts())
            self._process = None
        except Exception as e:
            cprint(f"✗ Selenium 启动异常: {e}", color=Colors.RED, bold=True, prefix=_ts())
            self._process = None

    # ── 停止流量 ────────────────────────────────────────────────────

    def stop(self) -> None:
        """安全停止 Selenium 流量引擎，确保无 Headless Chrome 孤儿进程。"""
        if self._process is None:
            return

        pid = self._process.pid
        cprint(f"🛑 正在关闭 Selenium 流量引擎 (PID={pid}) ...",
               color=Colors.YELLOW, bold=True, prefix=_ts())

        try:
            # 第一步: 礼貌请求 (SIGTERM)
            if platform.system() == "Windows":
                self._process.terminate()
            else:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, OSError):
            pass

        # 等待最多 15 秒
        try:
            self._process.wait(timeout=15)
        except subprocess.TimeoutExpired:
            cprint("  ⚠ 子进程未在 15s 内响应 SIGTERM，强制 kill ...",
                   color=Colors.YELLOW, prefix=_ts())
            try:
                if platform.system() == "Windows":
                    self._process.kill()
                else:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                self._process.wait(timeout=10)
            except (ProcessLookupError, OSError, subprocess.TimeoutExpired):
                pass

        # 第三步: 清理可能残留的 chromedriver / chrome 进程
        self._cleanup_orphans()

        self._process = None
        cprint("✓ Selenium 流量引擎已安全关闭", color=Colors.GREEN, bold=True, prefix=_ts())

    def _cleanup_orphans(self) -> None:
        """扫尾：杀死可能残留的 WebDriver 和 headless 浏览器进程 (Edge / Chrome 兼容)。"""
        sys_name = platform.system()
        # 根据浏览器类型选择要清理的进程
        if self.browser == "edge":
            driver_procs = ["msedgedriver.exe"]
            browser_procs = ["msedge.exe"]
            nix_driver = "msedgedriver"
            nix_browser = "msedge.*headless"
        else:
            driver_procs = ["chromedriver.exe"]
            browser_procs = ["chrome.exe"]
            nix_driver = "chromedriver"
            nix_browser = "chrome.*headless"

        try:
            if sys_name == "Windows":
                for proc in driver_procs:
                    subprocess.run(
                        ["taskkill", "/F", "/IM", proc],
                        capture_output=True, timeout=10,
                    )
                for proc in browser_procs:
                    subprocess.run(
                        ["taskkill", "/F", "/IM", proc],
                        capture_output=True, timeout=10,
                    )
            else:
                subprocess.run(
                    ["pkill", "-f", nix_driver],
                    capture_output=True, timeout=10,
                )
                subprocess.run(
                    ["pkill", "-f", nix_browser],
                    capture_output=True, timeout=10,
                )
        except Exception:
            pass  # 最后一层防护，吞掉所有异常

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None


# ═══════════════════════════════════════════════════════════════════════════
# ChaosWithSeleniumRunner — 核心实验编排器
# ═══════════════════════════════════════════════════════════════════════════

class ChaosWithSeleniumRunner:
    """Selenium 流量 + 混沌实验 全自动闭环编排器。

    核心管线:
      1. 后台启动 Selenium 流量引擎
      2. 遍历 self.fault_matrix 执行故障注入实验
      3. 优雅关闭 Selenium
      4. Prometheus 全量指标采集 (80 路) → outer join → 15s 对齐 → 打标 → CSV
    """

    def __init__(
        self,
        dry_run: bool = False,
        skip_selenium: bool = False,
        skip_prometheus: bool = False,
        prometheus_url: str = PROMETHEUS_URL,
        step: str = PROMETHEUS_STEP,
        rate_window: str = PROMETHEUS_RATE_WIN,
        label_mode: str = "pod",
        output_csv: str = "final_dataset_for_algorithm.csv",
        categories: Optional[List[str]] = None,
        fault_types: Optional[List[str]] = None,
        services: Optional[List[str]] = None,
        repeat: int = REPETITIONS_PER_FAULT,
    ):
        self.dry_run = dry_run
        self.skip_selenium = skip_selenium
        self.skip_prometheus = skip_prometheus
        self.prometheus_url = prometheus_url
        self.data_step = step
        self.rate_window = rate_window
        self.label_mode = label_mode
        self.output_csv = SCRIPT_ROOT / output_csv
        self.repeat = repeat
        self.target_services = services  # 限定服务池 (None = 全量)

        # ★ 从模板动态构建故障矩阵 (每种故障随机选服务)
        self.rng = np.random.default_rng()
        self.fault_matrix = self._build_matrix()

        # 实验状态
        self.history_file = SCRIPT_ROOT / "chaos_history.json"
        self.interrupted = False
        self.selenium: Optional[SeleniumTrafficEngine] = None

        self.total_experiments = sum(
            len(faults) for faults in self.fault_matrix.values()
        ) * self.repeat
        self.completed_experiments = 0
        self.failed_experiments = 0

        # dry-run 模式下缩短倒计时
        self._cd_div = 30 if dry_run else 1

        # 注册多层安全网
        atexit.register(self._atexit_cleanup)
        signal.signal(signal.SIGINT, self._signal_handler)
        signal.signal(signal.SIGTERM, self._signal_handler)

    def _build_matrix(self) -> Dict[str, List[Dict[str, Any]]]:
        """(重新)构建故障矩阵 — 每次调用都会重新随机选服务。"""
        return build_fault_matrix(
            FAULT_TEMPLATES,
            services=self.target_services,
            rng=self.rng,
        )

    # ── 安全网 ──────────────────────────────────────────────────────

    def _signal_handler(self, signum: int, frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        cprint(f"\n⚠ 收到 {sig_name} 信号，将在当前实验完成后优雅退出...",
               color=Colors.YELLOW, bold=True)
        self.interrupted = True

    def _atexit_cleanup(self) -> None:
        """进程退出时的最后防线：确保 Selenium 被终止。"""
        if self.selenium is not None and self.selenium.is_running:
            try:
                self.selenium.stop()
            except Exception:
                pass

    # ── chaos_history I/O ──────────────────────────────────────────

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

    # ── YAML 路径解析 ──────────────────────────────────────────────

    def _resolve_yaml_path(self, yaml_file: str) -> Path:
        """保留兼容性，但动态生成的 YAML 用不到此方法。"""
        return CHAOS_YAML_DIR / yaml_file

    # ── 单次实验执行 ───────────────────────────────────────────────

    def _run_single_experiment(
        self,
        fault_category: str,
        fault_config: Dict[str, Any],
        repetition: int,
        global_index: int,
    ) -> Optional[Dict[str, Any]]:
        fault_type = fault_config["fault_type"]
        svc = fault_config["service"]
        yaml_content = fault_config.get("yaml_content", "")

        header = (
            f"实验 #{global_index}/{self.total_experiments}  |  "
            f"类别: {fault_category}  |  "
            f"故障: {fault_type}  |  "
            f"目标: {svc}  |  "
            f"第 {repetition}/{self.repeat} 次"
        )
        print()
        cprint("=" * 80, color=Colors.BLUE)
        cprint(f"  {header}", color=Colors.BOLD, bold=True)
        cprint("=" * 80, color=Colors.BLUE)
        cprint(f"  故障类型: {fault_type}  →  随机目标: {svc}",
               color=Colors.GRAY, prefix=_ts())

        # ── 阶段 1: 注入前静默期 ──────────────────────────────────
        pre_q = PRE_INJECT_QUIET_SECONDS // self._cd_div
        cprint(f"  阶段 1/5: 注入前静默期 ({pre_q}s) — Selenium 流量持续施压，建立健康基线 ...",
               color=Colors.CYAN, bold=True, prefix=_ts())
        countdown(pre_q, "注入前静默", Colors.CYAN)

        # ── 阶段 2: 写入临时 YAML 并注入 ──────────────────────────
        cprint(f"  阶段 2/5: 生成临时 YAML → kubectl apply ...",
               color=Colors.MAGENTA, bold=True, prefix=_ts())

        tmp_yaml = SCRIPT_ROOT / ".tmp_chaos.yaml"
        if not self.dry_run:
            tmp_yaml.write_text(yaml_content, encoding="utf-8")
            cprint(f"    临时 YAML: {tmp_yaml}", color=Colors.GRAY, prefix=_ts())

        success, _, _ = run_kubectl(
            ["apply", "-f", str(tmp_yaml)], dry_run=self.dry_run,
        )
        if not success:
            cprint("  ✗ 故障注入失败，跳过本轮", color=Colors.RED, bold=True, prefix=_ts())
            self.failed_experiments += 1
            self._cleanup_tmp_yaml(tmp_yaml)
            return None

        start_time = get_utc_now_iso()
        cprint(f"  ✓ 注入成功 — start_time = {start_time}",
               color=Colors.GREEN, bold=True, prefix=_ts())

        # ── 阶段 3: 故障持续 ──────────────────────────────────────
        fault_d = FAULT_DURATION_SECONDS // self._cd_div
        cprint(f"  阶段 3/5: 故障持续中 ({fault_d}s / {fault_d // 60}min) — "
               f"Selenium 预期大量报错，容错吞之 ...",
               color=Colors.YELLOW, bold=True, prefix=_ts())
        countdown(fault_d, "故障持续", Colors.YELLOW)

        # ── 阶段 4: 解除故障 ──────────────────────────────────────
        cprint("  阶段 4/5: 解除故障 → kubectl delete ...",
               color=Colors.MAGENTA, bold=True, prefix=_ts())
        success, _, _ = run_kubectl(
            ["delete", "-f", str(tmp_yaml), "--ignore-not-found=true"],
            dry_run=self.dry_run,
        )
        if not success:
            cprint("  ⚠ 故障解除命令执行异常（可能已自动过期）",
                   color=Colors.YELLOW, prefix=_ts())

        self._cleanup_tmp_yaml(tmp_yaml)
        end_time = get_utc_now_iso()
        cprint(f"  ✓ 故障已解除 — end_time = {end_time}",
               color=Colors.GREEN, bold=True, prefix=_ts())

        # ── 记录元数据 ────────────────────────────────────────────
        record = {
            "experiment_id": str(uuid.uuid4()),
            "fault_category": fault_category,
            "fault_type": fault_type,
            "instance_type": fault_config.get("instance_type", "service"),
            "service": svc,
            "instance": fault_config.get("instance", svc),
            "source": fault_config.get("source", ""),
            "destination": fault_config.get("destination", svc),
            "start_time": start_time,
            "end_time": end_time,
            "repetition": repetition,
            "yaml_file": ".tmp_chaos.yaml",
            "status": "completed",
        }
        self._append_history(record)
        self.completed_experiments += 1
        cprint(f"  ✓ 元数据已写入 chaos_history.json",
               color=Colors.GREEN, prefix=_ts())

        # ── 阶段 5: 系统恢复冷却期 ────────────────────────────────
        cool = COOLDOWN_SECONDS // self._cd_div
        cprint(f"  阶段 5/5: 系统恢复冷却期 ({cool}s / {cool // 60}min) — "
               f"Selenium 流量持续，等待微服务自愈回稳 ...",
               color=Colors.BLUE, bold=True, prefix=_ts())
        countdown(cool, "系统冷却", Colors.BLUE)

        return record

    @staticmethod
    def _cleanup_tmp_yaml(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass

    # ════════════════════════════════════════════════════════════════
    # 全量指标采集 + outer join + 15s 对齐 + 打标 + CSV 导出
    # ════════════════════════════════════════════════════════════════

    def _collect_and_export_dataset(self) -> None:
        """实验全部结束后:

        1. 构建 80 路 PromQL 字典
        2. 确定完整时间范围 (首条 start → 末条 end + padding)
        3. 逐路查询 Prometheus Range API
        4. pd.concat(axis=1, join='outer') 横向合并为宽表
        5. .resample('15S').mean().ffill().bfill() 等距对齐
        6. 依据 chaos_history 时间窗口区间掩码打标
        7. 导出 final_dataset_for_algorithm.csv
        """
        if self.skip_prometheus:
            cprint("\n⏭ 跳过 Prometheus 指标采集 (--skip-prometheus)",
                   color=Colors.YELLOW, bold=True)
            return

        history = self._load_history()
        completed = [r for r in history if r.get("status") == "completed"]
        if not completed:
            cprint("\n⚠ chaos_history.json 无有效记录，跳过数据采集",
                   color=Colors.YELLOW, bold=True, prefix=_ts())
            return

        # ── 1. 构建 PromQL 字典 ──────────────────────────────────
        cprint(f"\n{'=' * 80}", color=Colors.MAGENTA, bold=True)
        cprint(f"  全量指标采集: 构建 80 路 PromQL 矩阵 + 批量查询 + 宽表合并",
               color=Colors.BOLD, bold=True)
        cprint(f"{'=' * 80}", color=Colors.MAGENTA, bold=True)

        promql_dict = build_promql_metrics(
            services=ALL_SERVICES,
            rate_window=self.rate_window,
            label_mode=self.label_mode,
        )
        total_queries = len(promql_dict)
        cprint(f"  ✓ 生成 {total_queries} 条 PromQL "
               f"({len(ALL_SERVICES)} 服务 × 7 指标 + 3 系统指标 = 80 路)",
               color=Colors.GREEN, prefix=_ts())

        # ── 2. 确定完整时间范围 ──────────────────────────────────
        all_starts = [r["start_time"] for r in completed]
        all_ends = [r["end_time"] for r in completed]
        full_start = min(all_starts)
        full_end = max(all_ends)
        # 前后各扩展 10 分钟以捕捉充分的前后基线
        pad = pd.Timedelta(minutes=10)
        start_dt = pd.to_datetime(full_start, utc=True) - pad
        end_dt = pd.to_datetime(full_end, utc=True) + pad
        query_start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        query_end = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        cprint(f"  查询窗口: {query_start} → {query_end}",
               color=Colors.GRAY, prefix=_ts())
        cprint(f"  步长: {self.data_step}  |  rate 窗口: {self.rate_window}",
               color=Colors.GRAY, prefix=_ts())

        # ── 3. 批量查询 ──────────────────────────────────────────
        cprint(f"\n  正在查询 {total_queries} 路 PromQL ...",
               color=Colors.CYAN, bold=True, prefix=_ts())

        all_series: List[pd.Series] = []
        success_cnt, empty_cnt, error_cnt = 0, 0, 0

        for i, (col_name, promql) in enumerate(promql_dict.items()):
            if (i + 1) % 20 == 0 or (i + 1) == total_queries:
                cprint(f"    [{i+1}/{total_queries}] {col_name:50s} ...", color=Colors.DIM)

            if self.dry_run:
                s = self._gen_synthetic(col_name, start_dt, end_dt)
            else:
                data = query_prom_range(
                    promql, query_start, query_end,
                    step=self.data_step, prometheus_url=self.prometheus_url,
                )
                if data is None:
                    error_cnt += 1
                    continue
                s = prom_data_to_series(data, col_name)

            if s.empty:
                empty_cnt += 1
                continue
            all_series.append(s)
            success_cnt += 1
            # 避免压垮 Prometheus
            if not self.dry_run:
                time.sleep(0.1)

        cprint(f"  ✓ 查询完成: {success_cnt} 成功, {empty_cnt} 空, {error_cnt} 失败",
               color=Colors.GREEN, prefix=_ts())
        cprint(f"  ✓ 有效列: {len(all_series)} (覆盖 "
               f"{len(set(s.name.split('&')[0] for s in all_series))} 个实体)",
               color=Colors.GREEN, prefix=_ts())

        if not all_series:
            cprint("✗ 无任何有效指标数据，跳过 CSV 导出", color=Colors.RED, bold=True)
            return

        # ── 4. 宽矩阵合并 (outer join) + 15s 对齐 ─────────────────
        cprint(f"\n  构建宽矩阵 (pd.concat axis=1 join='outer') + 15s 对齐 ...",
               color=Colors.CYAN, bold=True, prefix=_ts())

        # ★ 使用 outer join 合并所有时序列
        df = pd.concat(all_series, axis=1, join="outer").sort_index()
        cprint(f"    outer join 合并后: {len(df):,} 行 × {len(df.columns)} 列",
               color=Colors.WHITE)

        # ★ 15 秒等距网格对齐 → mean 聚合 → ffill → bfill
        df = df.resample(self.data_step).mean()
        cprint(f"    resample({self.data_step}) 后: {len(df):,} 行", color=Colors.WHITE)

        nan_before = df.isna().sum().sum()
        df = df.ffill().bfill()
        nan_after = df.isna().sum().sum()
        if nan_before > 0:
            cprint(f"    ffill/bfill: {nan_before:,} NaN → {nan_after:,} NaN",
                   color=Colors.GREEN)
        else:
            cprint(f"    数据完整，无漏抓点", color=Colors.GREEN)

        # ── 5. 区间掩码打标 ───────────────────────────────────────
        cprint(f"\n  根据 {len(completed)} 条故障记录批量打标 ...",
               color=Colors.CYAN, bold=True, prefix=_ts())

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
               color=Colors.GREEN, prefix=_ts())

        # ── 6. 导出 CSV ───────────────────────────────────────────
        cprint(f"\n  导出 CSV → {self.output_csv} ...",
               color=Colors.CYAN, bold=True, prefix=_ts())

        self.output_csv.parent.mkdir(parents=True, exist_ok=True)
        df.to_csv(
            self.output_csv, index=True, index_label="timestamp",
            encoding="utf-8", float_format="%.6f",
        )
        size_mb = self.output_csv.stat().st_size / (1024 * 1024)
        cprint(f"  ✓ 导出完成: {self.output_csv} ({size_mb:.1f} MB)",
               color=Colors.GREEN, bold=True, prefix=_ts())
        cprint(f"    规格: {len(df):,} 行 × {len(df.columns)} 列  |  "
               f"步长: {self.data_step}  |  NaN: {nan_after:,}",
               color=Colors.WHITE, prefix=_ts())

        # 打印列名一览
        data_cols = [c for c in df.columns if c not in ("fault_type", "target_service")]
        cprint(f"    数据列 ({len(data_cols)}):", color=Colors.WHITE)
        for col in data_cols[:6]:
            cprint(f"      {col}", color=Colors.GRAY)
        if len(data_cols) > 6:
            cprint(f"      ... 共 {len(data_cols)} 列", color=Colors.GRAY)

    def _gen_synthetic(
        self, col_name: str, start_dt: pd.Timestamp, end_dt: pd.Timestamp,
    ) -> pd.Series:
        """dry-run 模式: 生成模拟 Prometheus 时序数据 (仅供管道验证用)。"""
        ts_index = pd.date_range(start=start_dt, end=end_dt, freq=self.data_step)
        n = len(ts_index)
        rng = np.random.default_rng(abs(hash(col_name)) % (2**31))
        metric = col_name.split("&", 1)[-1] if "&" in col_name else "unknown"
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

        # 注入尖峰（模拟故障效果）
        if "cpu" in metric and not col_name.startswith("system&"):
            f0 = max(0, n // 3)
            f1 = min(n, f0 + max(1, 120 // 15))
            vals[f0:f1] += rng.uniform(20, 40, size=f1 - f0)

        return pd.Series(vals, index=ts_index, name=col_name)

    # ── 总览信息 ──────────────────────────────────────────────────

    def _print_summary(self) -> None:
        total_fault_types = sum(len(v) for v in self.fault_matrix.values())
        pool = self.target_services if self.target_services else TARGET_SERVICE_POOL
        print()
        cprint("╔" + "═" * 78 + "╗", color=Colors.BLUE)
        cprint("║  Selenium 流量引擎 + 混沌实验 全自动闭环流水线 (v3.0) 随机版" + " " * 10 + "║",
               color=Colors.BOLD, bold=True)
        cprint("╠" + "═" * 78 + "╣", color=Colors.BLUE)
        for category, faults in self.fault_matrix.items():
            cprint(f"║  ▸ {category.upper()}" + " " * (74 - len(category)),
                   color=Colors.CYAN, bold=True)
            for fault in faults:
                ft = fault["fault_type"]
                svc = fault["service"]
                cprint(f"║     {ft:22s} → 🎯{svc:25s} (随机)" + " " * 9 + f"× {self.repeat}", color=Colors.WHITE)
            cprint("║" + " " * 78 + "║", color=Colors.BLUE)
        total = self.total_experiments
        single = PRE_INJECT_QUIET_SECONDS + FAULT_DURATION_SECONDS + COOLDOWN_SECONDS
        total_min = (total * single) / 60
        cprint(f"║  故障类型: {total_fault_types}  |  重复: {self.repeat} 次  |  总计: {total} 次实验",
               color=Colors.YELLOW, bold=True)
        cprint(f"║  目标池: {len(pool)} 个服务  |  每次随机选 1 个注入",
               color=Colors.YELLOW)
        cprint(f"║  单次: {single // 60}min  |  预估: {total_min:.0f}min ≈ {total_min/60:.1f}h",
               color=Colors.YELLOW)
        cprint(f"║  数据步长: {self.data_step}  |  rate窗口: {self.rate_window}  |  标签模式: {self.label_mode}",
               color=Colors.WHITE)
        cprint("║  Selenium: " + ("已启用 (Headless Chrome)" if not self.skip_selenium else "已禁用"),
               color=Colors.WHITE)
        cprint("╚" + "═" * 78 + "╝", color=Colors.BLUE)
        if self.dry_run:
            cprint("\n⚠ DRY-RUN 模式 — 不执行 kubectl，倒计时缩短",
                   color=Colors.BG_YELLOW + Colors.BLACK, bold=True)
        print()

    # ════════════════════════════════════════════════════════════════
    # ★ 主入口
    # ════════════════════════════════════════════════════════════════

    def run(self) -> None:
        self._print_summary()

        # ── 前置校验 ──────────────────────────────────────────────
        if not self.dry_run:
            cprint("⚠ 即将开始全自动混沌工程实验，请确认以下前置条件：",
                   color=Colors.YELLOW, bold=True)
            cprint("   1. Minikube 集群正在运行", color=Colors.WHITE)
            cprint("   2. Online Boutique 已部署", color=Colors.WHITE)
            if not self.skip_selenium:
                cprint("   3. kubectl port-forward deployment/frontend 8080:8080 保持运行",
                       color=Colors.WHITE)
            cprint("   4. ChaosMesh 已部署到 chaos-testing", color=Colors.WHITE)
            cprint(f"   5. Prometheus: {self.prometheus_url}", color=Colors.WHITE)
            if not self.skip_selenium:
                cprint("   6. Chrome 浏览器已安装 (Selenium)", color=Colors.WHITE)
            print()

            pool = self.target_services if self.target_services else TARGET_SERVICE_POOL
            cprint(f"  目标服务池: {pool}", color=Colors.GREEN, prefix=_ts())
            cprint("✓ 故障模板就绪 (运行时动态生成 YAML, 随机选目标)", color=Colors.GREEN, prefix=_ts())
            print()

            try:
                inp = input(f"{_ts()} 按 Enter 开始 (或输入 'q' 退出): ").strip()
                if inp.lower() == "q":
                    cprint("用户取消。", color=Colors.YELLOW, bold=True)
                    return
            except (EOFError, KeyboardInterrupt):
                cprint("\n用户取消。", color=Colors.YELLOW, bold=True)
                return

        # ── ★ 后台拉起 Selenium 流量引擎 ──────────────────────────
        if not self.skip_selenium:
            self.selenium = SeleniumTrafficEngine()
            self.selenium.start()
            if not self.selenium.is_running and not self.dry_run:
                cprint("\n✗ Selenium 流量引擎启动失败，实验终止",
                       color=Colors.RED, bold=True)
                return

        try:
            # ── 故障大循环 ────────────────────────────────────────
            batch_start = datetime.now()
            cprint(f"\n{'#' * 80}", color=Colors.GREEN, bold=True)
            cprint(f"  实验批次启动 — {batch_start.strftime('%Y-%m-%d %H:%M:%S')}",
                   color=Colors.GREEN, bold=True)
            cprint(f"  Selenium 流量: {'运行中' if (self.selenium and self.selenium.is_running) else '已禁用'}",
                   color=Colors.GREEN, bold=True)
            cprint(f"{'#' * 80}\n", color=Colors.GREEN, bold=True)

            global_index = 0
            for category, faults in self.fault_matrix.items():
                cprint(f"\n{'▬' * 80}", color=Colors.MAGENTA, bold=True)
                cprint(f"  故障大类: {category.upper()}  ({len(faults)} 种子类型)",
                       color=Colors.MAGENTA, bold=True)
                cprint(f"{'▬' * 80}", color=Colors.MAGENTA, bold=True)

                for fault_config in faults:
                    cprint(f"\n  ▸ 子类型: {fault_config['fault_type']}  → 重复 {self.repeat} 次",
                           color=Colors.CYAN, bold=True, prefix=_ts())

                    for rep in range(1, self.repeat + 1):
                        if self.interrupted:
                            cprint("\n⚠ 收到中断信号，停止循环",
                                   color=Colors.YELLOW, bold=True)
                            break
                        global_index += 1
                        try:
                            self._run_single_experiment(
                                category, fault_config, rep, global_index,
                            )
                        except Exception as e:
                            # ★ 容错: 捕获所有异常，绝对不能让单体失败终止 55 次大循环
                            cprint(f"  ✗ 实验异常 (已捕获，继续下一轮): {e}",
                                   color=Colors.RED, bold=True, prefix=_ts())
                            self.failed_experiments += 1

                    if self.interrupted:
                        break
                if self.interrupted:
                    break

            # ── 批次结束统计 ──────────────────────────────────────
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

            # ── 全量指标采集 + 合并 + 打标 + 导出 ────────────────
            self._collect_and_export_dataset()

            # ── 最终总结 ─────────────────────────────────────────
            print()
            cprint("╔" + "═" * 78 + "╗", color=Colors.GREEN)
            cprint("║  实验全部完成！" + " " * 60 + "║", color=Colors.BOLD, bold=True)
            cprint("╠" + "═" * 78 + "╣", color=Colors.GREEN)
            cprint(f"║  元数据文件:  chaos_history.json" + " " * 44 + "║", color=Colors.WHITE)
            if not self.skip_prometheus:
                out_name = str(self.output_csv.name)
                cprint(f"║  数据集文件:  {out_name}" + " " * (62 - len(out_name)) + "║",
                       color=Colors.WHITE)
            if not self.skip_selenium:
                log_name = SELENIUM_LOG_FILE
                cprint(f"║  流量日志:    {log_name}" + " " * (62 - len(log_name)) + "║",
                       color=Colors.WHITE)
            cprint("╚" + "═" * 78 + "╝", color=Colors.GREEN)
            print()

        finally:
            # ── ★ 优雅关闭 Selenium (try...finally 保证) ──────────
            if self.selenium is not None and self.selenium.is_running:
                print()
                cprint("=" * 60, color=Colors.YELLOW)
                cprint("  执行 finally 块: 安全关闭 Selenium 流量引擎 ...",
                       color=Colors.YELLOW, bold=True)
                cprint("=" * 60, color=Colors.YELLOW)
                self.selenium.stop()
                cprint("✓ finally 块完成: Selenium 已关闭, 无孤儿进程残留",
                       color=Colors.GREEN, bold=True)


# ═══════════════════════════════════════════════════════════════════════════
# 命令行入口
# ═══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Selenium 流量引擎 + 混沌实验 全自动闭环流水线 (v3.0)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  cd chaos-engineering

  # 查看故障模板和服务池
  python run_chaos_with_selenium.py --list

  # 正式运行 (9种故障, 每种从11个服务中随机选1个注入)
  python run_chaos_with_selenium.py

  # 限定服务池, 每次重复3次 (每次重新随机选服务)
  python run_chaos_with_selenium.py --services cartservice,frontend,checkoutservice --repeat 3

  # 跳过 Selenium, 仅混沌实验
  python run_chaos_with_selenium.py --skip-selenium

  # 空跑验证
  python run_chaos_with_selenium.py --dry-run
        """,
    )
    parser.add_argument(
        "--list", action="store_true",
        help="列出故障模板和目标服务池，然后退出",
    )
    parser.add_argument(
        "--services", default="",
        help="限定目标服务池, 逗号分隔 (如: frontend,cartservice,checkoutservice)。默认: 全部11个微服务",
    )
    parser.add_argument(
        "--repeat", type=int, default=REPETITIONS_PER_FAULT,
        help=f"每种故障重复次数, 每次重新随机选服务 (默认: {REPETITIONS_PER_FAULT})",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="空跑模式: 跳过 kubectl, 倒计时缩短, 生成模拟 Prometheus 数据",
    )
    parser.add_argument(
        "--skip-selenium", action="store_true",
        help="跳过 Selenium 后台流量 (仅执行混沌实验)",
    )
    parser.add_argument(
        "--skip-prometheus", action="store_true",
        help="跳过 Prometheus 数据采集与 CSV 导出",
    )
    parser.add_argument(
        "--prometheus-url", default=PROMETHEUS_URL,
        help=f"Prometheus 地址 (默认: {PROMETHEUS_URL})",
    )
    parser.add_argument(
        "--step", default=PROMETHEUS_STEP,
        help=f"Prometheus 采集步长 (默认: {PROMETHEUS_STEP})",
    )
    parser.add_argument(
        "--rate-window", default=PROMETHEUS_RATE_WIN,
        help=f"PromQL rate() 窗口 (默认: {PROMETHEUS_RATE_WIN})",
    )
    parser.add_argument(
        "--label-mode", default="pod", choices=["pod", "container"],
        help="PromQL 标签匹配方式 (默认: pod)",
    )
    parser.add_argument(
        "--output", default="final_dataset_for_algorithm.csv",
        help="输出 CSV 文件名 (默认: final_dataset_for_algorithm.csv)",
    )
    return parser.parse_args()


def main() -> None:
    args = parse_args()

    # Windows 控制台 UTF-8
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    # --list: 列印故障模板并退出
    if args.list:
        list_available_faults()
        return

    # 解析服务池限定
    services = [s.strip() for s in args.services.split(",") if s.strip()] or None

    runner = ChaosWithSeleniumRunner(
        dry_run=args.dry_run,
        skip_selenium=args.skip_selenium,
        skip_prometheus=args.skip_prometheus,
        prometheus_url=args.prometheus_url,
        step=args.step,
        rate_window=args.rate_window,
        label_mode=args.label_mode,
        output_csv=args.output,
        services=services,
        repeat=args.repeat,
    )
    runner.run()


if __name__ == "__main__":
    main()
