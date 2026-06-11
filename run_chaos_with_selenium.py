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
    4. 实验结束后统一捞取 Prometheus 80 路指标 → outer join → 10s 重采样 → ffill/bfill → 打标 → CSV

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
# ★ v3.2 调整: 静默/故障/冷却各 5min
#   单次实验 15min，11 种故障 ≈ 2.8h (原 5.7h)
#   有效恢复窗口 = 冷却 5min + 下一轮静默 5min = 10min，覆盖 pod-kill 恢复
#   normal:fault = (5+5):5 = 2:1，满足异常检测样本平衡
PRE_INJECT_QUIET_SECONDS  = 300      # 静默期 (5min) — 30 个 baseline 采样点
FAULT_DURATION_SECONDS    = 300      # 故障持续 (5min) — 30 个 fault 采样点
COOLDOWN_SECONDS          = 300      # 冷却恢复 (5min) + 下轮静默 5min = 10min 有效恢复
REPETITIONS_PER_FAULT     = 1        # 默认每种故障重复次数 (--repeat 可覆盖)

# Quick 模式 (--quick): 快速验证管道
QUICK_QUIET_SECONDS  = 60            # 静默期 1min
QUICK_FAULT_SECONDS  = 120           # 故障持续 2min
QUICK_COOLDOWN_SECONDS = 120         # 冷却恢复 2min

# 数据参数
PROMETHEUS_STEP     = "10s"
PROMETHEUS_RATE_WIN = "60s"

# Selenium 参数
SELENIUM_BROWSER        = "edge"     # edge (Windows 自带) / chrome / firefox
SELENIUM_HEADLESS       = True
SELENIUM_LOG_FILE       = "selenium_traffic.log"
SELENIUM_DRIVER_PATH    = r"E:\edgedriver_win64"  # EdgeDriver 目录, 空字符串=自动查找

# ═══════════════════════════════════════════════════════════════════════════
# JMeter 配置常量
# ═══════════════════════════════════════════════════════════════════════════

JMETER_HOME      = r"D:\Application\apache-jmeter-5.6.3"     # JMeter 安装目录
JMETER_BIN_DIR   = str(Path(JMETER_HOME) / "bin")
JMETER_JMX       = str(SCRIPT_ROOT / "jmeter" / "online-boutique.jmx")
JMETER_BASE_DIR  = str(SCRIPT_ROOT / "jmeter")                 # data/products.csv, tools/ 等相对路径的基准
JMETER_TOOLS_DIR = str(SCRIPT_ROOT / "jmeter" / "tools")
JMETER_USERS     = 30                                          # 默认并发用户数
JMETER_SCENARIO  = "mixed"                                     # 默认场景: shopping | mixed
JMETER_RAMPUP    = 30                                          # 爬坡时间 (秒)

# ═══════════════════════════════════════════════════════════════════════════
# 微服务列表
# ═══════════════════════════════════════════════════════════════════════════

ALL_SERVICES = [
    "frontend", "cartservice", "productcatalogservice",
    "currencyservice", "paymentservice", "shippingservice",
    "checkoutservice", "emailservice", "recommendationservice",
    "adservice", "reviewservice",
]

# 各微服务的 metrics 端口映射 (Prometheus 抓取注解用)
# 大部分 Go 服务通过 OpenCensus Prometheus exporter 在应用端口暴露 /metrics
SERVICE_METRICS_PORTS: Dict[str, int] = {
    "frontend":                8080,
    "cartservice":             7070,
    "productcatalogservice":   3550,
    "currencyservice":         7000,
    "paymentservice":          50051,
    "shippingservice":         50051,
    "checkoutservice":         5050,
    "emailservice":            8080,
    "recommendationservice":   8080,
    "adservice":               9555,
    "reviewservice":           8080,
}

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
            "    loss: \"10\"\n"
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
            "    corrupt: \"5\"\n"
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
    label_mode: str = "container",
) -> OrderedDict[str, str]:
    """动态生成全服务 × 多指标的 PromQL 查询字典。

    每服务 9 个指标 + 3 个系统指标 = 11×9 + 3 = 102 路。

    label_mode:
      - "container": container=~".*{service}.*"  (默认, 更可靠)
      - "pod":       pod=~"{service}-.*"

    指标说明:
      - cpu_usage / mem_usage_mb: cAdvisor 容器指标 (kubelet 内置, 始终存在)
      - mem_usage_pct: 容器内存使用率 = working_set / spec_limit × 100
      - grpc_latency_p99: gRPC 服务端 P99 延迟 (需 Prometheus 注解)
      - grpc_error_rate:  gRPC 非 OK 响应占比 (需 Prometheus 注解)
      - grpc_rps:         gRPC 服务端每秒请求数 (需 Prometheus 注解)
      - pod_restarts:     Pod 重启次数 (kubelet 内置指标)
    """
    if label_mode == "pod":
        label_clause = 'pod=~"{service}-.*", namespace="{ns}"'
    else:
        # container 模式: 容器名包含服务名
        label_clause = 'container=~".*{service}.*", namespace="{ns}"'

    # ── 7 个指标模板 ──
    # ★ mem_usage_pct: 使用 cAdvisor 的 container_spec_memory_limit_bytes 做分母
    #   避免依赖 kube-state-metrics (kube_pod_container_resource_limits)
    # ★ pod_restarts: 优先使用 kubelet 的 kube_pod_container_status_restarts_total
    # ★ gRPC 指标: 主数据源 = Istio sidecar telemetry (istio_*),
    #   fallback = 应用自身暴露的 grpc_server_* / rpc_server_* / grpc_io_*
    #   Istio 指标在 port 15020 /stats/prometheus 上暴露，由 Prometheus kubernetes-pods job 抓取
    #   标签: destination_workload=<deployment-name>, reporter="destination"=服务端视角
    metric_templates: List[Tuple[str, str]] = [
        (
            "cpu_usage",
            'sum(rate(container_cpu_usage_seconds_total{{label}}[{rw}])) * 100',
        ),
        (
            "mem_usage_mb",
            'sum(container_memory_working_set_bytes{{label}}) / 1024 / 1024',
        ),
        (
            "mem_usage_pct",
            # ★ 精确匹配 pod，分母用 cAdvisor spec_limit (kubelet 内置)
            'sum(container_memory_working_set_bytes{'
            'pod=~"{service}-.*", namespace="{ns}"})'
            ' / sum(container_spec_memory_limit_bytes{'
            'pod=~"{service}-.*", namespace="{ns}"}) * 100',
        ),
        (
            "grpc_latency_p99",
            # ★ 主数据源: Istio sidecar 延迟直方图 (ms → s)
            'histogram_quantile(0.99, '
            'sum(rate(istio_request_duration_milliseconds_bucket{'
            'reporter="destination", destination_workload="{service}"}[{rw}])) by (le)) / 1000'
            ' or '
            # fallback 1: gRPC Prometheus 指标 (应用自身暴露)
            'histogram_quantile(0.99, '
            'sum(rate(grpc_server_handling_seconds_bucket{'
            'grpc_service=~".*{service}.*"}[{rw}])) by (le))'
            ' or '
            # fallback 2: OpenTelemetry gRPC
            'histogram_quantile(0.99, '
            'sum(rate(rpc_server_duration_bucket{'
            'grpc_service=~".*{service}.*"}[{rw}])) by (le))'
            ' or '
            # fallback 3: OpenCensus
            'histogram_quantile(0.99, '
            'sum(rate(grpc_io_server_server_latency_bucket{'
            'grpc_service=~".*{service}.*"}[{rw}])) by (le))',
        ),
        (
            "grpc_error_rate",
            # ★ 主数据源: Istio sidecar — 非 2xx HTTP + 非 OK gRPC 合并
            '(('
            'sum(rate(istio_requests_total{'
            'reporter="destination", destination_workload="{service}",'
            'response_code!~"2..", response_code!=""}[{rw}])) or vector(0)'
            ' + '
            'sum(rate(istio_requests_total{'
            'reporter="destination", destination_workload="{service}",'
            'grpc_response_status!="OK", grpc_response_status!=""}[{rw}])) or vector(0)'
            ') / '
            'sum(rate(istio_requests_total{'
            'reporter="destination", destination_workload="{service}"}[{rw}])))'
            ' or '
            # fallback 1: 应用 gRPC Prometheus
            '(sum(rate(grpc_server_handled_total{'
            'grpc_service=~".*{service}.*",grpc_code!="OK"}[{rw}]))'
            ' / sum(rate(grpc_server_handled_total{'
            'grpc_service=~".*{service}.*"}[{rw}])))'
            ' or '
            # fallback 2: OpenTelemetry
            '(sum(rate(rpc_server_duration_count{'
            'grpc_service=~".*{service}.*",grpc_status_code!="0"}[{rw}]))'
            ' / sum(rate(rpc_server_duration_count{'
            'grpc_service=~".*{service}.*"}[{rw}])))',
        ),
        (
            "grpc_rps",
            # ★ 主数据源: Istio sidecar 请求计数
            'sum(rate(istio_requests_total{'
            'reporter="destination", destination_workload="{service}"}[{rw}]))'
            ' or '
            # fallback 1: 应用 gRPC Prometheus
            'sum(rate(grpc_server_handled_total{'
            'grpc_service=~".*{service}.*"}[{rw}]))'
            ' or '
            # fallback 2: OpenTelemetry
            'sum(rate(rpc_server_duration_count{'
            'grpc_service=~".*{service}.*"}[{rw}]))'
            ' or '
            # fallback 3: OpenCensus
            'sum(rate(grpc_io_server_completed_rpcs_count{'
            'grpc_service=~".*{service}.*"}[{rw}]))',
        ),
        (
            "pod_restarts",
            # kubelet 内置的 Pod 重启计数 (不需要 kube-state-metrics)
            'sum(kube_pod_container_status_restarts_total{'
            'namespace="{ns}",pod=~"{service}-.*"})',
        ),
    ]

    queries: OrderedDict[str, str] = OrderedDict()
    for svc in services:
        label_str = label_clause.format(service=svc, ns=TARGET_NAMESPACE)
        for suffix, template in metric_templates:
            col_name = f"{svc}&{suffix}"
            # 根据指标类型选用不同的 label 注入方式
            if any(kw in suffix for kw in ("grpc_",)):
                # gRPC 指标: 用 grpc_service label 匹配
                promql = template.replace("{rw}", rate_window)
                promql = promql.replace("{service}", svc)
            elif suffix in ("pod_restarts", "mem_usage_pct"):
                # pod_restarts / mem_usage_pct: 用 pod label, 精确匹配 container="server" 排除 sidecar
                promql = template.replace("{ns}", TARGET_NAMESPACE)
                promql = promql.replace("{service}", svc)
            else:
                # cpu_usage / mem_usage_mb: 用 container/pod label
                promql = template.replace("{label}", label_str)
                promql = promql.replace("{rw}", rate_window)
            queries[col_name] = promql

    # ── 3 路系统级全局指标 ──
    # total_rps: 集群全局请求速率 (Istio mesh 全量)
    queries["system&total_rps"] = (
        'sum(rate(istio_requests_total{reporter="destination"}[{rw}]))'
    ).replace("{rw}", rate_window)
    # node_cpu_pct: 节点 CPU (kubelet 内置)
    queries["system&node_cpu_pct"] = (
        '100 - (avg by (instance) (rate(node_cpu_seconds_total{mode="idle"}[{rw}])) * 100)'
    ).replace("{rw}", rate_window)
    # node_mem_pct: 节点内存 (kubelet 内置)
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
            # 正斜杠避免 \e 等转义字符问题
            driver_path_clean = self.driver_path.replace("\\", "/")
            driver_arg = f" --driver-path={driver_path_clean}"
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
        cprint(f"🚀 启动 Selenium Headless {self.browser.title()} 流量引擎 ...", color=Colors.CYAN, bold=True, prefix=_ts())
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

        # 第三步: 清理可能残留的 WebDriver / 浏览器 孤儿进程
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
# JMeter 流量引擎管理器
# ═══════════════════════════════════════════════════════════════════════════

class JMeterTrafficEngine:
    """后台 JMeter HTTP 流量引擎。

    通过 subprocess.Popen 拉起 JMeter CLI（非 GUI 模式），
    模拟多用户并发 HTTP 请求，覆盖完整业务链路：
    首页 → 商品详情 → 加购 → 购物车 → 结算 → 评论。

    与 Selenium 的关键差异：
      - 协议级 HTTP 请求，不渲染浏览器，资源开销极低（~100MB vs 500MB+）
      - 可精确控制并发数（-Jusers=N）和持续时间（-Jduration=S）
      - 输出结构化 JTL 结果文件（每请求一条记录），可按时间戳与 Prometheus 对齐
      - 支持 events.csv 事件打标，汇总工具自动按 normal/fault/recovery 分阶段统计
    """

    def __init__(
        self,
        run_id: str,
        duration_seconds: int,
        users: int = JMETER_USERS,
        scenario: str = JMETER_SCENARIO,
        rampup: int = JMETER_RAMPUP,
        host: str = "127.0.0.1",
        port: int = 8080,
        jmx_path: str = JMETER_JMX,
        jmeter_base_dir: str = JMETER_BASE_DIR,
        checkout_percent: int = 30,
        currency_percent: int = 20,
        review_write_percent: int = 10,
    ):
        self.run_id = run_id
        self.duration_seconds = duration_seconds
        self.users = users
        self.scenario = scenario
        self.rampup = rampup
        self.host = host
        self.port = port
        self.jmx_path = jmx_path
        self.jmeter_base_dir = jmeter_base_dir
        self.checkout_percent = checkout_percent
        self.currency_percent = currency_percent
        self.review_write_percent = review_write_percent

        # 实验目录与文件路径
        self._experiment_dir = Path(jmeter_base_dir) / "experiments" / run_id
        self._jmeter_dir = self._experiment_dir / "jmeter"
        self._jtl_path = self._jmeter_dir / "result.jtl"
        self._jmeter_log_path = self._jmeter_dir / "jmeter.log"
        self._events_path = self._experiment_dir / "events.csv"

        self._process: Optional[subprocess.Popen] = None
        self._shutdown_flag = False

    # ── 构建 JMeter 命令行 ─────────────────────────────────────────

    def _build_cmd(self) -> List[str]:
        """构建 JMeter CLI 命令行 (非 GUI 模式)。

        JMeter 参数说明:
          -n              非 GUI 模式
          -t <jmx>       测试计划文件
          -Jkey=value    覆盖 JMX 中的 ${__P(key,default)} 属性
          -q <props>     加载额外的 properties 文件 (可选)
          -l <jtl>       输出 CSV JTL 结果文件
          -j <log>       输出 JMeter 日志文件
        """
        # 检测 jmeter 可执行文件
        jmeter_bin = self._find_jmeter_bin()

        return [
            jmeter_bin,
            "-n",                           # 非 GUI
            "-t", self.jmx_path,            # 测试计划
            "-Jjmeter_base_dir=" + self.jmeter_base_dir,
            "-Jrun_id=" + self.run_id,
            "-Jscenario=" + self.scenario,
            "-Jusers=" + str(self.users),
            "-Jduration=" + str(self.duration_seconds),
            "-Jrampup=" + str(self.rampup),
            "-Jprotocol=http",
            "-Jhost=" + self.host,
            "-Jport=" + str(self.port),
            "-Jbase_path=",
            "-Jcheckout_percent=" + str(self.checkout_percent),
            "-Jcurrency_percent=" + str(self.currency_percent),
            "-Jreview_write_percent=" + str(self.review_write_percent),
            "-l", str(self._jtl_path),      # JTL 结果
            "-j", str(self._jmeter_log_path),  # JMeter 日志
        ]

    @staticmethod
    def _find_jmeter_bin() -> str:
        """查找 JMeter 可执行文件。

        优先使用 JMETER_HOME/bin/jmeter.bat (Windows) 或 jmeter (Unix)，
        其次使用 PATH 中的 jmeter。
        """
        sys_name = platform.system()
        # Windows: 优先 jmeter.bat, 其次 jmeter
        if sys_name == "Windows":
            candidates = [
                str(Path(JMETER_BIN_DIR) / "jmeter.bat"),
                str(Path(JMETER_BIN_DIR) / "jmeter"),
                "jmeter.bat",
                "jmeter",
            ]
        else:
            candidates = [
                str(Path(JMETER_BIN_DIR) / "jmeter"),
                "jmeter",
            ]
        for c in candidates:
            # 对于带路径的候选，检查文件是否存在
            if "/" in c or "\\" in c:
                if Path(c).exists():
                    return c
            else:
                # PATH 中的命令，用 shutil.which 检查
                import shutil
                found = shutil.which(c)
                if found:
                    return found
        # 最后的 fallback
        return "jmeter"

    # ── 启动流量 ───────────────────────────────────────────────────

    def start(self) -> bool:
        """后台启动 JMeter 流量引擎。

        Returns:
            True 如果 JMeter 成功启动，False 如果启动失败。
        """
        if self._process is not None:
            cprint("⚠ JMeter 流量引擎已在运行中，跳过重复启动",
                   color=Colors.YELLOW, prefix=_ts())
            return True

        # 创建实验目录
        self._experiment_dir.mkdir(parents=True, exist_ok=True)
        self._jmeter_dir.mkdir(parents=True, exist_ok=True)
        (self._experiment_dir / "monitoring" / "prometheus").mkdir(parents=True, exist_ok=True)
        (self._experiment_dir / "chaos").mkdir(parents=True, exist_ok=True)

        # 初始化 events.csv
        self._init_events_csv()

        # 写入 manifest.csv
        self._write_manifest()

        cmd = self._build_cmd()
        cprint(f"🚀 启动 JMeter HTTP 流量引擎 ...", color=Colors.CYAN, bold=True, prefix=_ts())
        cprint(f"   JMX: {self.jmx_path}", color=Colors.GRAY, prefix=_ts())
        cprint(f"   场景: {self.scenario}  |  并发: {self.users}  |  "
               f"爬坡: {self.rampup}s  |  持续: {self.duration_seconds}s ({self.duration_seconds / 60:.0f}min)",
               color=Colors.GRAY, prefix=_ts())
        cprint(f"   目标: http://{self.host}:{self.port}", color=Colors.GRAY, prefix=_ts())
        cprint(f"   JTL: {self._jtl_path}", color=Colors.GRAY, prefix=_ts())
        cprint(f"   日志: {self._jmeter_log_path}", color=Colors.GRAY, prefix=_ts())

        try:
            log_fh = open(self._jmeter_log_path, "w", encoding="utf-8")
            log_fh.write(f"=== JMeter Traffic Engine Log ===\n")
            log_fh.write(f"Run ID: {self.run_id}\n")
            log_fh.write(f"Started: {get_utc_now_iso()}\n")
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

            # 等待 JMeter 初始化 (Java 启动较慢)
            time.sleep(8)

            # 检查进程是否存活
            if self._process.poll() is not None:
                cprint("✗ JMeter 流量引擎启动后立即退出！请检查：", color=Colors.RED, prefix=_ts())
                cprint(f"    1. JMeter 是否已安装: {JMETER_BIN_DIR}", color=Colors.RED)
                cprint(f"    2. Java 是否可用: java -version", color=Colors.RED)
                cprint(f"    3. JMX 文件是否存在: {self.jmx_path}", color=Colors.RED)
                cprint(f"    4. kubectl port-forward deployment/frontend 8080:8080 是否保持", color=Colors.RED)
                cprint(f"    5. 查看日志: {self._jmeter_log_path}", color=Colors.RED)
                return False
            else:
                cprint(f"✓ JMeter 流量引擎已启动 (PID={self._process.pid})",
                       color=Colors.GREEN, bold=True, prefix=_ts())
                return True

        except FileNotFoundError:
            cprint(f"✗ 未找到 JMeter 可执行文件，请确认 JMETER_HOME 配置",
                   color=Colors.RED, bold=True, prefix=_ts())
            cprint(f"   当前 JMETER_BIN_DIR: {JMETER_BIN_DIR}", color=Colors.RED)
            self._process = None
            return False
        except Exception as e:
            cprint(f"✗ JMeter 启动异常: {e}", color=Colors.RED, bold=True, prefix=_ts())
            self._process = None
            return False

    # ── 停止流量 ───────────────────────────────────────────────────

    def stop(self) -> None:
        """安全停止 JMeter 流量引擎。

        JMeter 支持两种关闭方式:
          1. 优雅关闭: shutdown port (默认 4445, 需在 JMX 中配置)
          2. 强制终止: SIGTERM / taskkill

        这里使用 SIGTERM → wait → SIGKILL 三层策略。
        """
        if self._process is None:
            return

        pid = self._process.pid
        cprint(f"🛑 正在关闭 JMeter 流量引擎 (PID={pid}) ...",
               color=Colors.YELLOW, bold=True, prefix=_ts())

        try:
            # 第一步: 礼貌请求 (SIGTERM / Ctrl+C)
            if platform.system() == "Windows":
                self._process.terminate()
            else:
                os.killpg(os.getpgid(pid), signal.SIGTERM)
        except (ProcessLookupError, OSError):
            pass

        # 等待最多 30 秒 (JMeter 可能需要时间写 JTL 和清理)
        try:
            self._process.wait(timeout=30)
        except subprocess.TimeoutExpired:
            cprint("  ⚠ 子进程未在 30s 内响应 SIGTERM，强制 kill ...",
                   color=Colors.YELLOW, prefix=_ts())
            try:
                if platform.system() == "Windows":
                    self._process.kill()
                else:
                    os.killpg(os.getpgid(pid), signal.SIGKILL)
                self._process.wait(timeout=10)
            except (ProcessLookupError, OSError, subprocess.TimeoutExpired):
                pass

        self._process = None
        cprint("✓ JMeter 流量引擎已安全关闭", color=Colors.GREEN, bold=True, prefix=_ts())

    # ── 事件打标 ───────────────────────────────────────────────────

    def _init_events_csv(self) -> None:
        """初始化 events.csv 文件头。"""
        if not self._events_path.exists():
            self._events_path.write_text("run_id,event,timestamp_utc,details\n", encoding="utf-8")

    def _write_manifest(self) -> None:
        """写入 manifest.csv (与 run-test.sh 兼容的格式)。"""
        import csv as csv_module
        manifest_path = self._experiment_dir / "manifest.csv"
        if not manifest_path.exists():
            with open(manifest_path, "w", newline="", encoding="utf-8") as f:
                writer = csv_module.writer(f)
                writer.writerow([
                    "run_id", "scenario", "start_utc", "end_utc",
                    "users", "rampup_s", "duration_s", "host", "port",
                    "checkout_percent", "currency_percent", "review_write_percent",
                    "target_service", "fault_type", "fault_parameters", "operator",
                ])
                writer.writerow([
                    self.run_id, self.scenario, get_utc_now_iso(), "",
                    self.users, self.rampup, self.duration_seconds,
                    self.host, self.port,
                    self.checkout_percent, self.currency_percent, self.review_write_percent,
                    "", "", "", "",
                ])

    def mark_event(self, event: str, details: str = "") -> None:
        """向 events.csv 写入事件记录 (Python 原生实现，不依赖 bash)。

        Args:
            event: 事件名 — TEST_START, WARMUP_END, FAULT_START, FAULT_END, TEST_END, NOTE
            details: 事件详情 (如故障类型和目标服务)
        """
        import csv as csv_module
        valid_events = {"TEST_START", "WARMUP_END", "FAULT_START", "FAULT_END", "TEST_END", "NOTE"}
        if event not in valid_events:
            cprint(f"  ⚠ 无效事件名 '{event}', 跳过 (有效: {sorted(valid_events)})",
                   color=Colors.YELLOW, prefix=_ts())
            return

        # CSV 转义: 包含逗号或引号时用双引号包裹
        def csv_quote(val: str) -> str:
            if ',' in val or '"' in val:
                return '"' + val.replace('"', '""') + '"'
            return val

        timestamp = get_utc_now_iso()
        line = f"{csv_quote(self.run_id)},{csv_quote(event)},{csv_quote(timestamp)},{csv_quote(details)}\n"

        with open(self._events_path, "a", encoding="utf-8") as f:
            f.write(line)

        cprint(f"  📌 JMeter 事件: {event} — {details}", color=Colors.DIM, prefix=_ts())

    # ── 结果汇总 ───────────────────────────────────────────────────

    def summarize(self) -> bool:
        """调用 summarize_results.py 生成 summary.csv 和 summary.md。

        要求 JTL 文件存在且 events.csv 中包含 FAULT_START/FAULT_END 事件。

        Returns:
            True 如果汇总成功。
        """
        if not self._jtl_path.exists():
            cprint("  ⚠ JTL 文件不存在，跳过结果汇总", color=Colors.YELLOW, prefix=_ts())
            return False

        summarizer = Path(JMETER_TOOLS_DIR) / "summarize_results.py"
        if not summarizer.exists():
            cprint(f"  ⚠ 汇总脚本不存在: {summarizer}", color=Colors.YELLOW, prefix=_ts())
            return False

        cprint(f"\n  📊 运行 JMeter 结果汇总 ...", color=Colors.CYAN, bold=True, prefix=_ts())
        try:
            result = subprocess.run(
                [
                    sys.executable, str(summarizer),
                    "--jtl", str(self._jtl_path),
                    "--events", str(self._events_path),
                    "--output-dir", str(self._jmeter_dir),
                ],
                capture_output=True, text=True, timeout=60,
            )
            if result.returncode == 0:
                cprint(f"  ✓ 汇总完成 → {self._jmeter_dir}/summary.csv 和 summary.md",
                       color=Colors.GREEN, prefix=_ts())
                return True
            else:
                cprint(f"  ⚠ 汇总脚本返回非零: {result.stderr.strip()[:200]}",
                       color=Colors.YELLOW, prefix=_ts())
                return False
        except subprocess.TimeoutExpired:
            cprint("  ⚠ 汇总脚本超时", color=Colors.YELLOW, prefix=_ts())
            return False
        except Exception as e:
            cprint(f"  ⚠ 汇总异常: {e}", color=Colors.YELLOW, prefix=_ts())
            return False

    @property
    def is_running(self) -> bool:
        return self._process is not None and self._process.poll() is None

    @property
    def jtl_path(self) -> Path:
        return self._jtl_path

    @property
    def events_path(self) -> Path:
        return self._events_path


# ═══════════════════════════════════════════════════════════════════════════
# ChaosWithSeleniumRunner — 核心实验编排器
# ═══════════════════════════════════════════════════════════════════════════

class ChaosWithSeleniumRunner:
    """Selenium 流量 + 混沌实验 全自动闭环编排器。

    核心管线:
      1. 后台启动 Selenium 流量引擎
      2. 遍历 self.fault_matrix 执行故障注入实验
      3. 优雅关闭 Selenium
      4. Prometheus 全量指标采集 (80 路) → outer join → 10s 对齐 → 打标 → CSV
    """

    def __init__(
        self,
        dry_run: bool = False,
        quick: bool = False,
        skip_selenium: bool = False,
        load_engine: str = "selenium",       # ★ "selenium" | "jmeter" | "none"
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
        # JMeter 专属参数
        jmeter_users: int = JMETER_USERS,
        jmeter_scenario: str = JMETER_SCENARIO,
        jmeter_rampup: int = JMETER_RAMPUP,
    ):
        self.dry_run = dry_run
        self.quick = quick
        # ★ backward compat: --skip-selenium 等价于 --load-engine=none
        self.load_engine = "none" if skip_selenium else load_engine
        self.skip_selenium = (self.load_engine == "none")
        self.skip_prometheus = skip_prometheus

        # JMeter 专属参数
        self.jmeter_users = jmeter_users
        self.jmeter_scenario = jmeter_scenario
        self.jmeter_rampup = jmeter_rampup
        self.prometheus_url = prometheus_url
        self.data_step = step
        self.rate_window = rate_window
        self.label_mode = label_mode
        self.output_csv = SCRIPT_ROOT / output_csv
        self.incremental_csv = self.output_csv.with_name(
            self.output_csv.stem + "_incremental" + self.output_csv.suffix
        )
        self.repeat = repeat
        self.target_services = services  # 限定服务池 (None = 全量)

        # ★ Quick 模式: 缩到 1/10，快速验证
        self.pre_inject_quiet = QUICK_QUIET_SECONDS if quick else PRE_INJECT_QUIET_SECONDS
        self.fault_duration = QUICK_FAULT_SECONDS if quick else FAULT_DURATION_SECONDS
        self.cooldown = QUICK_COOLDOWN_SECONDS if quick else COOLDOWN_SECONDS

        # ★ 预构建 PromQL 字典 (80 路)，各实验窗口查询复用
        self.promql_dict = build_promql_metrics(
            services=ALL_SERVICES,
            rate_window=self.rate_window,
            label_mode=self.label_mode,
        )
        self.data_columns = list(self.promql_dict.keys())

        # ★ 从模板动态构建故障矩阵 (每种故障随机选服务)
        self.rng = np.random.default_rng()
        self.fault_matrix = self._build_matrix()

        # 实验状态
        self.history_file = SCRIPT_ROOT / "chaos_history.json"
        self.interrupted = False
        self.selenium: Optional[SeleniumTrafficEngine] = None
        self.jmeter: Optional[JMeterTrafficEngine] = None

        self.total_experiments = len(FAULT_TEMPLATES) * self.repeat
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

    def _init_run_state(self) -> None:
        """每次运行前重置状态：清空增量 CSV 和 chaos_history，避免多轮运行数据混淆。"""
        if not self.skip_prometheus:
            try:
                if self.incremental_csv.exists():
                    self.incremental_csv.unlink()
            except OSError:
                pass
            cprint(f"  增量 CSV 就绪: {self.incremental_csv.name} (crash-safe 逐实验追加)",
                   color=Colors.GRAY, prefix=_ts())
        # 清空历史文件，只记录本次运行
        try:
            if self.history_file.exists():
                self.history_file.unlink()
        except OSError:
            pass
        cprint(f"  历史文件已重置: {self.history_file.name}",
               color=Colors.GRAY, prefix=_ts())

    # ── 安全网 ──────────────────────────────────────────────────────

    def _signal_handler(self, signum: int, frame: Any) -> None:
        sig_name = signal.Signals(signum).name
        cprint(f"\n⚠ 收到 {sig_name} 信号，将在当前实验完成后优雅退出...",
               color=Colors.YELLOW, bold=True)
        self.interrupted = True

    def _atexit_cleanup(self) -> None:
        """进程退出时的最后防线：确保流量引擎被终止。"""
        # Selenium 清理
        if self.selenium is not None and self.selenium.is_running:
            try:
                self.selenium.stop()
            except Exception:
                pass
        # JMeter 清理
        if self.jmeter is not None and self.jmeter.is_running:
            try:
                self.jmeter.stop()
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

        # 流量引擎文案
        engine_name = {"selenium": "Selenium", "jmeter": "JMeter", "none": "无"}.get(self.load_engine, "流量")

        # ── 阶段 1: 注入前静默期 ──────────────────────────────────
        quiet_start_time = get_utc_now_iso()
        pre_q = self.pre_inject_quiet // self._cd_div
        cprint(f"  阶段 1/5: 注入前静默期 ({pre_q}s) — {engine_name} 流量持续施压，建立健康基线 ...",
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
            ["apply", "-f", str(tmp_yaml), "--validate=false"], dry_run=self.dry_run,
        )
        if not success:
            cprint("  ✗ 故障注入失败，跳过本轮", color=Colors.RED, bold=True, prefix=_ts())
            self.failed_experiments += 1
            self._cleanup_tmp_yaml(tmp_yaml)
            return None

        start_time = get_utc_now_iso()
        cprint(f"  ✓ 注入成功 — start_time = {start_time}",
               color=Colors.GREEN, bold=True, prefix=_ts())

        # ★ JMeter: 标记 FAULT_START 事件
        if self.jmeter is not None and self.jmeter.is_running:
            self.jmeter.mark_event("FAULT_START",
                                   f"{fault_type} → {svc} (rep {repetition}/{self.repeat})")

        # ── 阶段 3: 故障持续 ──────────────────────────────────────
        fault_d = self.fault_duration // self._cd_div
        cprint(f"  阶段 3/5: 故障持续中 ({fault_d}s / {fault_d // 60}min) — "
               f"{engine_name} 预期大量报错，容错吞之 ...",
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

        # ★ JMeter: 标记 FAULT_END 事件
        if self.jmeter is not None and self.jmeter.is_running:
            self.jmeter.mark_event("FAULT_END",
                                   f"{fault_type} → {svc} (rep {repetition}/{self.repeat})")

        # ── 阶段 5: 系统恢复冷却期 ────────────────────────────────
        cool = self.cooldown // self._cd_div
        cprint(f"  阶段 5/5: 系统恢复冷却期 ({cool}s / {cool // 60}min) — "
               f"{engine_name} 流量持续，等待微服务自愈回稳 ...",
               color=Colors.BLUE, bold=True, prefix=_ts())
        countdown(cool, "系统冷却", Colors.BLUE)
        cooldown_end_time = get_utc_now_iso()

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
            "quiet_start_time": quiet_start_time,
            "cooldown_end_time": cooldown_end_time,
            "repetition": repetition,
            "yaml_file": ".tmp_chaos.yaml",
            "status": "completed",
        }
        self._append_history(record)
        self.completed_experiments += 1
        cprint(f"  ✓ 元数据已写入 chaos_history.json",
               color=Colors.GREEN, prefix=_ts())

        return record

    @staticmethod
    def _cleanup_tmp_yaml(path: Path) -> None:
        try:
            if path.exists():
                path.unlink()
        except OSError:
            pass

    # ════════════════════════════════════════════════════════════════
    # ★ 增量采集：每个实验结束后立刻查询 Prometheus → 打标 → 追加 CSV
    # ════════════════════════════════════════════════════════════════

    def _collect_experiment_window(self, record: Dict[str, Any]) -> None:
        """增量采集单个实验完整生命周期的 80 路 Prometheus 指标。

        每个实验完成后立即调用：
          1. 取 record 的 quiet_start_time / cooldown_end_time 覆盖完整周期
             (静默期 → 故障注入 → 故障持续 → 解除 → 冷却恢复)
          2. 前后各扩 1min 缓冲，确保相邻实验窗口重叠、无缺口
          3. 逐路查询 Prometheus Range API（80 路，复用 self.promql_dict）
          4. outer join → resample → 补全缺失列
          5. 依据故障窗口打标 fault_type / target_service
          6. 追加写入增量 CSV（crash-safe）
        """
        if self.skip_prometheus:
            return

        start_time = record["start_time"]
        end_time = record["end_time"]
        fault_type = record["fault_type"]
        service = record.get("service", "unknown")

        # ★ 使用 quiet_start_time / cooldown_end_time 覆盖完整实验生命周期
        #   避免冷却期出现数据缺口
        q_start = record.get("quiet_start_time", start_time)
        q_end = record.get("cooldown_end_time", end_time)
        start_dt = pd.to_datetime(q_start, utc=True) - pd.Timedelta(minutes=1)
        end_dt = pd.to_datetime(q_end, utc=True) + pd.Timedelta(minutes=1)
        query_start = start_dt.strftime("%Y-%m-%dT%H:%M:%SZ")
        query_end = end_dt.strftime("%Y-%m-%dT%H:%M:%SZ")

        cprint(f"\n  📊 增量采集: {fault_type} → {service} "
               f"[完整周期: {q_start} .. {q_end}]",
               color=Colors.CYAN, bold=True, prefix=_ts())

        total = len(self.promql_dict)
        all_series: List[pd.Series] = []
        success_cnt, empty_cnt, error_cnt = 0, 0, 0

        for i, (col_name, promql) in enumerate(self.promql_dict.items()):
            if (i + 1) % 20 == 0 or (i + 1) == total:
                cprint(f"    [{i+1}/{total}] {col_name:50s} ...", color=Colors.DIM)

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
            if not self.dry_run:
                time.sleep(0.05)

        if not all_series:
            cprint(f"  ⚠ 窗口内无有效数据，跳过本次追加", color=Colors.YELLOW, prefix=_ts())
            return

        # 构建 mini 宽矩阵
        df = pd.concat(all_series, axis=1, join="outer").sort_index()
        df = df.resample(self.data_step).mean()

        # 补全缺失列（确保追加时列序与 header 一致）
        for col in self.data_columns:
            if col not in df.columns:
                df[col] = np.nan
        df = df[self.data_columns]

        # 打标
        df["fault_type"] = "normal"
        df["target_service"] = "none"
        try:
            t0 = pd.to_datetime(start_time, utc=True)
            t1 = pd.to_datetime(end_time, utc=True)
            mask = (df.index >= t0) & (df.index <= t1)
            if mask.sum() > 0:
                df.loc[mask, "fault_type"] = fault_type
                df.loc[mask, "target_service"] = service
        except (ValueError, TypeError):
            pass

        # 追加写入增量 CSV
        write_header = (
            not self.incremental_csv.exists()
            or self.incremental_csv.stat().st_size == 0
        )
        df.to_csv(
            self.incremental_csv, mode='a', index=True, index_label="timestamp",
            header=write_header, encoding="utf-8", float_format="%.6f",
        )
        size_kb = self.incremental_csv.stat().st_size / 1024

        # ── 数据质量快速诊断 ──
        empty_cols = [c for c in self.data_columns if c in df.columns and df[c].isna().all()]
        if empty_cols:
            # 按类别统计空列
            empty_grpc = [c for c in empty_cols if "grpc" in c or "rps" in c or "restart" in c]
            empty_cpu = [c for c in empty_cols if "cpu" in c]
            empty_pct = [c for c in empty_cols if "mem_usage_pct" in c]
            parts = []
            if empty_grpc:
                parts.append(f"gRPC/RPS: {len(empty_grpc)}列为空")
            if empty_cpu:
                parts.append(f"CPU: {len(empty_cpu)}列为空")
            if empty_pct:
                parts.append(f"mem_pct: {len(empty_pct)}列为空")
            if parts:
                cprint(f"  ⚠ 空列警告: {' | '.join(parts)}  "
                       f"(可能是 Prometheus 未抓取应用指标或 kube-state-metrics 未安装)",
                       color=Colors.YELLOW, prefix=_ts())

        cprint(f"  ✓ 已追加 {len(df)} 行 → {self.incremental_csv.name} "
               f"({size_kb:.0f} KB) [{success_cnt}/{total} 列有效]",
               color=Colors.GREEN, prefix=_ts())

    # ════════════════════════════════════════════════════════════════
    # 最终化: 读取增量 CSV → 去重 → 重采样 → ffill/bfill → 重新打标 → 导出
    # ════════════════════════════════════════════════════════════════

    def _finalize_dataset(self) -> None:
        """实验全部结束后：读取增量 CSV → 去重 → 重采样 → ffill/bfill → 重新打标 → 导出最终 CSV。

        增量 CSV 各窗口可能存在重叠时间戳（前后缓冲），需按优先级去重：
        fault > normal，若同一时间戳有故障标记则保留故障版本。
        """
        if self.skip_prometheus:
            return

        if not self.incremental_csv.exists() or self.incremental_csv.stat().st_size == 0:
            cprint("\n⚠ 增量 CSV 为空，跳过最终导出",
                   color=Colors.YELLOW, bold=True, prefix=_ts())
            return

        cprint(f"\n{'=' * 80}", color=Colors.MAGENTA, bold=True)
        cprint(f"  数据集最终化: 读取增量 CSV → 去重 → 重采样 → 重新打标 → 导出",
               color=Colors.BOLD, bold=True)
        cprint(f"{'=' * 80}", color=Colors.MAGENTA, bold=True)

        # ── 1. 读取增量 CSV ────────────────────────────────────
        cprint(f"\n  读取增量 CSV: {self.incremental_csv.name} ...",
               color=Colors.CYAN, bold=True, prefix=_ts())
        df = pd.read_csv(self.incremental_csv, index_col=0, parse_dates=True)
        n_raw = len(df)
        cprint(f"    原始: {n_raw:,} 行 × {len(df.columns)} 列", color=Colors.WHITE)

        # ── 2. 去重 ────────────────────────────────────────────
        # 优先级: fault_type != "normal" > "normal"
        priority_map = {"normal": 0}
        for ft in df["fault_type"].unique():
            if ft != "normal":
                priority_map[ft] = 1
        df["_prio"] = df["fault_type"].map(priority_map).fillna(0).astype(int)
        df = df.sort_values("_prio", ascending=False)
        dups = df.index.duplicated(keep="first").sum()
        df = df[~df.index.duplicated(keep="first")]
        df = df.drop(columns=["_prio"])
        df = df.sort_index()
        cprint(f"    去重: {n_raw:,} → {len(df):,} 行 (移除 {dups:,} 条重复时间戳, fault > normal)",
               color=Colors.GREEN)

        # ── 3. 剥离标签列，只对数值列做 resample ──────────────
        # resample().mean() 不能处理字符串列，先取出标签，resample 后再加回来
        label_backup = {}
        for lc in ["fault_type", "target_service"]:
            if lc in df.columns:
                label_backup[lc] = df[lc].copy()
                df = df.drop(columns=[lc])

        # ── 4. 重采样对齐 (仅数值列) ──────────────────────────
        df = df.resample(self.data_step).mean()
        cprint(f"    resample({self.data_step}) 后: {len(df):,} 行", color=Colors.WHITE)

        # ── 5. 缺失值修复: interpolate → ffill → bfill ──────────
        # ★ interpolate() 线性插值处理偶发的 Prometheus scrape 超时 NaN
        #   然后 ffill/bfill 兜底处理首尾残留
        nan_before = df.isna().sum().sum()
        # 先线性插值 (limit=5 避免跨大缺口插值)
        df = df.interpolate(method="linear", limit=5, limit_direction="both")
        # 再 ffill/bfill 清理首尾残留
        df = df.ffill().bfill()
        nan_after = df.isna().sum().sum()
        if nan_before > 0:
            cprint(f"    interpolate+ffill+bfill: {nan_before:,} NaN → {nan_after:,} NaN", color=Colors.GREEN)
        else:
            cprint(f"    数据完整，无漏抓点", color=Colors.GREEN)

        # ── 5b. Clip mem_usage_pct 到 [0, 100] ──────────────────
        #   防止容器 spec_limit 匹配异常导致百分比爆炸
        pct_cols = [c for c in df.columns if "mem_usage_pct" in c]
        if pct_cols:
            over_count = (df[pct_cols] > 100).sum().sum()
            under_count = (df[pct_cols] < 0).sum().sum()
            if over_count > 0 or under_count > 0:
                df[pct_cols] = df[pct_cols].clip(0, 100)
                cprint(f"    mem_usage_pct clamp: {over_count}个>100, {under_count}个<0 已修正到 [0, 100]",
                       color=Colors.YELLOW, prefix=_ts())

        # ── 5. 重新打标 (以 chaos_history 为准) ────────────────
        # 先剥离增量阶段的标签列，再按所有 completed 记录重打
        for lc in ["fault_type", "target_service"]:
            if lc in df.columns:
                df = df.drop(columns=[lc])

        history = self._load_history()
        completed = [r for r in history if r.get("status") == "completed"]
        df["fault_type"] = "normal"
        df["target_service"] = "none"

        for record in completed:
            ftype = record.get("fault_type", "unknown")
            svc = record.get("service", "unknown")
            try:
                t0 = pd.to_datetime(record["start_time"], utc=True)
                t1 = pd.to_datetime(record["end_time"], utc=True)
                mask = (df.index >= t0) & (df.index <= t1)
                if mask.sum() > 0:
                    df.loc[mask, "fault_type"] = ftype
                    df.loc[mask, "target_service"] = svc
            except (ValueError, TypeError):
                continue

        normal_cnt = (df["fault_type"] == "normal").sum()
        fault_cnt = (df["fault_type"] != "normal").sum()
        cprint(f"    重新打标: normal={normal_cnt:,}  fault={fault_cnt:,}  "
               f"类型={sorted(set(df['fault_type'].unique()))}",
               color=Colors.GREEN, prefix=_ts())

        # ── 6. 导出最终 CSV ────────────────────────────────────
        cprint(f"\n  导出最终 CSV → {self.output_csv} ...",
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

        # ── 数据质量总览 ────────────────────────────────────────
        # 按指标类别统计覆盖率
        metric_cats = {
            "cpu_usage": [],
            "mem_usage_mb": [],
            "mem_usage_pct": [],
            "grpc_latency": [],
            "grpc_error": [],
            "grpc_rps": [],
            "pod_restarts": [],
            "system": [],
        }
        for col in self.data_columns:
            if col.startswith("system&"):
                metric_cats["system"].append(col)
            elif "cpu_usage" in col:
                metric_cats["cpu_usage"].append(col)
            elif "mem_usage_mb" in col:
                metric_cats["mem_usage_mb"].append(col)
            elif "mem_usage_pct" in col:
                metric_cats["mem_usage_pct"].append(col)
            elif "grpc_latency" in col:
                metric_cats["grpc_latency"].append(col)
            elif "grpc_error" in col:
                metric_cats["grpc_error"].append(col)
            elif "grpc_rps" in col:
                metric_cats["grpc_rps"].append(col)
            elif "pod_restarts" in col:
                metric_cats["pod_restarts"].append(col)

        cprint(f"\n  📊 数据质量报告:", color=Colors.CYAN, bold=True, prefix=_ts())
        for cat_name, cols in metric_cats.items():
            if not cols:
                continue
            non_empty = sum(1 for c in cols if c in df.columns and not df[c].isna().all())
            pct = non_empty / len(cols) * 100 if cols else 0
            bar = "█" * int(pct / 10) + "░" * (10 - int(pct / 10))
            color = Colors.GREEN if pct >= 80 else (Colors.YELLOW if pct >= 30 else Colors.RED)
            cprint(f"    {cat_name:18s}  {bar}  {non_empty}/{len(cols)}  ({pct:.0f}%)", color=color)
            if pct == 0 and "grpc" in cat_name:
                cprint(f"       ↳ gRPC 指标为空: 请确认 Pod 有 prometheus.io/scrape 注解 + /metrics 端点",
                       color=Colors.DIM)
            elif pct == 0 and "pod_restart" in cat_name:
                cprint(f"       ↳ 重启指标为空: 请确认 kube-state-metrics 已部署",
                       color=Colors.DIM)

        # 时间连续性检查
        if len(df) >= 2:
            diffs = df.index.to_series().diff().dropna()
            large_gaps = (diffs > pd.Timedelta(seconds=30)).sum()
            if large_gaps > 0:
                cprint(f"    ⚠ 时间缺口: {large_gaps} 个 > 30s 的缺口", color=Colors.YELLOW)

        # 故障注入覆盖检查
        svc_counts = df[df["fault_type"] != "normal"]["target_service"].value_counts()
        if len(svc_counts) > 0:
            cprint(f"    故障覆盖服务: {len(svc_counts)}/{len(ALL_SERVICES)} — "
                   f"{', '.join(sorted(svc_counts.index))}", color=Colors.WHITE, prefix=_ts())

        # 清理增量文件
        try:
            self.incremental_csv.unlink()
            cprint(f"    已清理增量文件: {self.incremental_csv.name}", color=Colors.GRAY)
        except OSError:
            pass

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
        total_fault_types = len(FAULT_TEMPLATES)
        pool = self.target_services if self.target_services else TARGET_SERVICE_POOL
        print()
        cprint("╔" + "═" * 78 + "╗", color=Colors.BLUE)
        header_text = {
            "selenium": "Selenium 流量引擎 + 混沌实验 全自动闭环流水线 (v3.2)",
            "jmeter":   "JMeter 流量引擎 + 混沌实验 全自动闭环流水线 (v3.2)",
            "none":     "混沌实验 全自动闭环流水线 (v3.2, 无流量引擎)",
        }.get(self.load_engine, "混沌实验 全自动闭环流水线 (v3.2)")
        header_text += " " * max(0, 76 - len(header_text))
        cprint(f"║  {header_text}║", color=Colors.BOLD, bold=True)
        cprint("╠" + "═" * 78 + "╣", color=Colors.BLUE)
        # 按类别展示故障模板，每个模板的服务为"轮转"(运行时轮转覆盖所有服务)
        last_cat = None
        for tmpl in FAULT_TEMPLATES:
            cat = tmpl["category"]
            if cat != last_cat:
                if last_cat is not None:
                    cprint("║" + " " * 78 + "║", color=Colors.BLUE)
                cprint(f"║  ▸ {cat.upper()}" + " " * (74 - len(cat)),
                       color=Colors.CYAN, bold=True)
                last_cat = cat
            svc_info = tmpl.get("service", "🎲轮转")
            cprint(f"║     {tmpl['fault_type']:22s} → 🎯{svc_info:25s}" + " " * 11 + f"× {self.repeat}",
                   color=Colors.WHITE)
        total = self.total_experiments
        single = self.pre_inject_quiet + self.fault_duration + self.cooldown
        total_min = (total * single) / 60
        cprint(f"║  故障类型: {total_fault_types}  |  重复: {self.repeat} 次  |  总计: {total} 次实验",
               color=Colors.YELLOW, bold=True)
        cprint(f"║  目标池: {len(pool)} 个服务  |  每次随机选 1 个注入  |  顺序: 随机打乱",
               color=Colors.YELLOW)
        cprint(f"║  单次: {single // 60}min  |  预估: {total_min:.0f}min ≈ {total_min/60:.1f}h",
               color=Colors.YELLOW)
        cprint(f"║  数据步长: {self.data_step}  |  rate窗口: {self.rate_window}  |  标签模式: {self.label_mode}",
               color=Colors.WHITE)
        # 流量引擎信息
        if self.load_engine == "selenium":
            browser_label = SELENIUM_BROWSER.title()
            cprint(f"║  流量引擎: Selenium (Headless {browser_label})", color=Colors.WHITE)
        elif self.load_engine == "jmeter":
            cprint(f"║  流量引擎: JMeter ({self.jmeter_scenario}, {self.jmeter_users} users, "
                   f"rampup {self.jmeter_rampup}s)", color=Colors.WHITE)
        else:
            cprint("║  流量引擎: 已禁用", color=Colors.WHITE)
        cprint("╚" + "═" * 78 + "╝", color=Colors.BLUE)
        if self.quick:
            cprint(f"\n⚡ QUICK 模式 — 静默{self.pre_inject_quiet}s + 故障{self.fault_duration}s + 冷却{self.cooldown}s",
                   color=Colors.BG_YELLOW + Colors.BLACK, bold=True)
        if self.dry_run:
            cprint("\n⚠ DRY-RUN 模式 — 不执行 kubectl，倒计时缩短",
                   color=Colors.BG_YELLOW + Colors.BLACK, bold=True)
        print()

    # ════════════════════════════════════════════════════════════════
    # Prometheus 注解自动确保 — 没有注解 Prometheus 不会抓取 Pod
    # ════════════════════════════════════════════════════════════════

    def _ensure_prometheus_annotations(self) -> None:
        """确保所有 Online Boutique 微服务 Deployment 的 Pod template 带有
        Prometheus 抓取注解。

        原因: Prometheus 的 kubernetes-pods scrape job 只抓取带有
        ``prometheus.io/scrape: 'true'`` 注解的 Pod。没有这些注解，
        所有应用级指标 (gRPC / HTTP) 都不会出现在 Prometheus 中。
        """
        if self.dry_run:
            cprint("  [DRY-RUN] 将 patch Deployment 添加 Prometheus 注解",
                   color=Colors.GRAY, prefix=_ts())
            return

        cprint("\n📌 确保 Prometheus Pod 抓取注解 ...",
               color=Colors.CYAN, bold=True, prefix=_ts())

        ok_cnt, skip_cnt, fail_cnt = 0, 0, 0
        for svc in ALL_SERVICES:
            port = SERVICE_METRICS_PORTS.get(svc)
            if port is None:
                cprint(f"  ⚠ {svc}: 无 metrics 端口配置，跳过", color=Colors.YELLOW, prefix=_ts())
                skip_cnt += 1
                continue

            patch = (
                '{{"spec":{{"template":{{"metadata":{{"annotations":{{'
                '"prometheus.io/scrape":"true",'
                '"prometheus.io/port":"{port}",'
                '"prometheus.io/path":"/metrics"'
                '}}}}}}}}}}'
            ).format(port=port)

            success, stdout, stderr = run_kubectl(
                ["patch", "deployment", svc, "-p", patch],
                dry_run=False, timeout=30,
            )
            if success:
                ok_cnt += 1
            else:
                # 不存在的 Deployment 不算致命错误
                if "not found" in stderr.lower():
                    cprint(f"  ⚠ {svc}: Deployment 不存在，跳过", color=Colors.YELLOW, prefix=_ts())
                    skip_cnt += 1
                else:
                    cprint(f"  ✗ {svc}: patch 失败 — {stderr.strip()[:120]}", color=Colors.RED, prefix=_ts())
                    fail_cnt += 1

        if ok_cnt > 0:
            cprint(f"  ✓ 已 patch {ok_cnt} 个 Deployment ({skip_cnt} 跳过, {fail_cnt} 失败)",
                   color=Colors.GREEN, bold=True, prefix=_ts())
            cprint(f"  ⏳ 等待 rollout 完成 (旧 Pod 替换为新 Pod，获得注解) ...",
                   color=Colors.CYAN, prefix=_ts())
            # 等待新 Pod 滚动更新就绪
            time.sleep(15)
        elif fail_cnt > 0:
            cprint(f"  ⚠ {fail_cnt} 个 patch 失败，请手动检查 Prometheus 抓取配置",
                   color=Colors.YELLOW, bold=True, prefix=_ts())

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
                if self.load_engine == "selenium":
                    cprint("   3. kubectl port-forward deployment/frontend 8080:8080 保持运行",
                           color=Colors.WHITE)
                    cprint(f"   6. {SELENIUM_BROWSER.title()} 浏览器已安装 (Selenium)", color=Colors.WHITE)
                elif self.load_engine == "jmeter":
                    cprint("   3. kubectl port-forward deployment/frontend 8080:8080 保持运行",
                           color=Colors.WHITE)
                    cprint("   6. JMeter 已安装且 jmeter 命令可用", color=Colors.WHITE)
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

        # ── ★ 确保 Pod 有 Prometheus 抓取注解 ────────────────────
        if not self.skip_prometheus:
            self._ensure_prometheus_annotations()

        # ── ★ 后台拉起流量引擎 ──────────────────────────────────────
        batch_start = datetime.now()  # 提前初始化，供 JMeter run_id 使用
        if self.load_engine == "selenium":
            self.selenium = SeleniumTrafficEngine()
            self.selenium.start()
            if not self.selenium.is_running and not self.dry_run:
                cprint("\n✗ Selenium 流量引擎启动失败，实验终止",
                       color=Colors.RED, bold=True)
                return
        elif self.load_engine == "jmeter":
            # 计算 JMeter 运行总时长 (覆盖所有实验 + 缓冲)
            single_exp_seconds = self.pre_inject_quiet + self.fault_duration + self.cooldown
            total_est_seconds = self.total_experiments * single_exp_seconds
            jmeter_duration = total_est_seconds + 600  # 额外 10min 缓冲
            jmeter_run_id = f"CHAOS-{batch_start.strftime('%Y%m%d-%H%M%S')}"
            self.jmeter = JMeterTrafficEngine(
                run_id=jmeter_run_id,
                duration_seconds=jmeter_duration,
                users=self.jmeter_users,
                scenario=self.jmeter_scenario,
                rampup=self.jmeter_rampup,
            )
            ok = self.jmeter.start()
            if not ok and not self.dry_run:
                cprint("\n✗ JMeter 流量引擎启动失败，实验终止",
                       color=Colors.RED, bold=True)
                return
            # 写入 JMeter TEST_START 事件
            if self.jmeter.is_running:
                self.jmeter.mark_event("TEST_START", f"JMeter {self.jmeter_scenario} {self.jmeter_users} users")

        # ★ 初始化运行状态 (清理旧 CSV 和历史文件)
        self._init_run_state()

        try:
            # ── 故障大循环 ────────────────────────────────────────
            cprint(f"\n{'#' * 80}", color=Colors.GREEN, bold=True)
            cprint(f"  实验批次启动 — {batch_start.strftime('%Y-%m-%d %H:%M:%S')}",
                   color=Colors.GREEN, bold=True)
            engine_status = "已禁用"
            if self.load_engine == "selenium":
                engine_status = "Selenium 运行中" if (self.selenium and self.selenium.is_running) else "Selenium 启动失败"
            elif self.load_engine == "jmeter":
                engine_status = "JMeter 运行中" if (self.jmeter and self.jmeter.is_running) else "JMeter 启动失败"
            cprint(f"  流量引擎: {engine_status}", color=Colors.GREEN, bold=True)
            cprint(f"{'#' * 80}\n", color=Colors.GREEN, bold=True)

            # ★ 构建实验队列 — 轮转选服务，保证全部11个服务都被覆盖
            # 每种故障 × repeat 次，每次轮转到不同服务 (而非重复同样服务)
            default_pool = self.target_services if self.target_services else list(TARGET_SERVICE_POOL)
            cat_rotations: Dict[str, Dict[str, Any]] = {}
            for tmpl in FAULT_TEMPLATES:
                cat = tmpl["category"]
                if cat not in cat_rotations:
                    pool = list(default_pool)
                    self.rng.shuffle(pool)
                    cat_rotations[cat] = {"pool": pool, "idx": 0}

            experiment_queue: List[Tuple[str, Dict[str, Any], int]] = []
            for tmpl in FAULT_TEMPLATES:
                cat = tmpl["category"]
                for rep in range(1, self.repeat + 1):
                    if "service" in tmpl:
                        svc = tmpl["service"]
                    else:
                        rot = cat_rotations[cat]
                        svc = rot["pool"][rot["idx"] % len(rot["pool"])]
                        rot["idx"] += 1
                    yaml_content = tmpl["yaml_template"].format(
                        service=svc, target_ns=TARGET_NAMESPACE,
                        chaos_ns=CHAOS_NAMESPACE, duration=_FAULT_YAML_DURATION,
                    )
                    fault_config = {
                        "fault_type": tmpl["fault_type"],
                        "service": svc,
                        "chaos_kind": tmpl["chaos_kind"],
                        "yaml_content": yaml_content,
                        "instance_type": "service",
                        "instance": svc,
                        "source": f"chaos-mesh-{tmpl['chaos_kind'].lower()}",
                        "destination": svc,
                    }
                    experiment_queue.append((cat, fault_config, rep))
            self.rng.shuffle(experiment_queue)

            total_planned = len(experiment_queue)
            cprint(f"  实验队列: {total_planned} 次实验, 已随机打乱顺序",
                   color=Colors.CYAN, bold=True, prefix=_ts())
            # 预览前 5 个
            preview = ", ".join(f"{fc['fault_type']}→{fc['service']}" for _, fc, _ in experiment_queue[:5])
            cprint(f"  前 5 个: {preview} ...", color=Colors.DIM, prefix=_ts())
            print()

            global_index = 0
            for category, fault_config, rep in experiment_queue:
                if self.interrupted:
                    cprint("\n⚠ 收到中断信号，停止循环",
                           color=Colors.YELLOW, bold=True)
                    break
                global_index += 1
                try:
                    record = self._run_single_experiment(
                        category, fault_config, rep, global_index,
                    )
                    if record is not None:
                        self._collect_experiment_window(record)
                except Exception as e:
                    # ★ 容错: 捕获所有异常，绝对不能让单体失败终止 55 次大循环
                    cprint(f"  ✗ 实验异常 (已捕获，继续下一轮): {e}",
                           color=Colors.RED, bold=True, prefix=_ts())
                    self.failed_experiments += 1

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

            # ── 最终化: 去重 + 重采样 + 重新打标 + 导出 ────────
            self._finalize_dataset()

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
                if self.load_engine == "selenium":
                    log_name = SELENIUM_LOG_FILE
                    cprint(f"║  流量日志:    {log_name}" + " " * (62 - len(log_name)) + "║",
                           color=Colors.WHITE)
                elif self.load_engine == "jmeter" and self.jmeter is not None:
                    jtl_name = str(self.jmeter.jtl_path.name)
                    events_name = str(self.jmeter.events_path.name)
                    cprint(f"║  JMeter JTL:  {jtl_name}" + " " * (62 - len(jtl_name)) + "║",
                           color=Colors.WHITE)
                    cprint(f"║  JMeter 事件: {events_name}" + " " * (62 - len(events_name)) + "║",
                           color=Colors.WHITE)
            cprint("╚" + "═" * 78 + "╝", color=Colors.GREEN)
            print()

        finally:
            # ── ★ 优雅关闭流量引擎 (try...finally 保证) ──────────
            # Selenium 清理
            if self.selenium is not None and self.selenium.is_running:
                print()
                cprint("=" * 60, color=Colors.YELLOW)
                cprint("  执行 finally 块: 安全关闭 Selenium 流量引擎 ...",
                       color=Colors.YELLOW, bold=True)
                cprint("=" * 60, color=Colors.YELLOW)
                self.selenium.stop()
                cprint("✓ finally 块完成: Selenium 已关闭, 无孤儿进程残留",
                       color=Colors.GREEN, bold=True)

            # JMeter 清理 (含事件标记 + 结果汇总)
            if self.jmeter is not None and self.jmeter.is_running:
                print()
                cprint("=" * 60, color=Colors.YELLOW)
                cprint("  执行 finally 块: 安全关闭 JMeter 流量引擎 ...",
                       color=Colors.YELLOW, bold=True)
                cprint("=" * 60, color=Colors.YELLOW)
                self.jmeter.mark_event("TEST_END", "JMeter run completed")
                self.jmeter.stop()
                cprint("✓ finally 块完成: JMeter 已关闭",
                       color=Colors.GREEN, bold=True)
                # 运行结果汇总
                self.jmeter.summarize()


# ═══════════════════════════════════════════════════════════════════════════
# 命令行入口
# ═══════════════════════════════════════════════════════════════════════════

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="JMeter / Selenium 双引擎 + 混沌实验 全自动闭环流水线 (v3.2)",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
使用示例:
  cd chaos-engineering

  # 查看故障模板和服务池
  python run_chaos_with_selenium.py --list

  # 正式运行 (Selenium 模式, 9种故障, 每种从11个服务中随机选1个注入)
  python run_chaos_with_selenium.py

  # ★ JMeter 模式 (HTTP 协议级压测, 30 并发, mixed 场景)
  python run_chaos_with_selenium.py --load-engine jmeter

  # ★ JMeter 模式 + 自定义参数
  python run_chaos_with_selenium.py --load-engine jmeter --jmeter-users 50 --jmeter-scenario shopping

  # 限定服务池, 每次重复3次 (每次重新随机选服务)
  python run_chaos_with_selenium.py --services cartservice,frontend,checkoutservice --repeat 3

  # 跳过流量引擎, 仅混沌实验 (等价于 --load-engine=none)
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
        "--quick", action="store_true",
        help="快速验证模式: 静默30s + 故障1min + 冷却2min, 总时长大幅缩短",
    )
    parser.add_argument(
        "--dry-run", action="store_true",
        help="空跑模式: 跳过 kubectl, 倒计时缩短, 生成模拟 Prometheus 数据",
    )
    parser.add_argument(
        "--skip-selenium", action="store_true",
        help="跳过流量引擎 (等价于 --load-engine=none, 保留向后兼容)",
    )
    parser.add_argument(
        "--load-engine", default="selenium", choices=["selenium", "jmeter", "none"],
        help="流量引擎选择: selenium (默认, Headless 浏览器), jmeter (HTTP 协议级压测), none (跳过)",
    )
    parser.add_argument(
        "--jmeter-users", type=int, default=JMETER_USERS,
        help=f"JMeter 并发用户数 (默认: {JMETER_USERS})",
    )
    parser.add_argument(
        "--jmeter-scenario", default=JMETER_SCENARIO, choices=["shopping", "mixed"],
        help=f"JMeter 场景: shopping (购物流程) / mixed (购物+评论) (默认: {JMETER_SCENARIO})",
    )
    parser.add_argument(
        "--jmeter-rampup", type=int, default=JMETER_RAMPUP,
        help=f"JMeter 爬坡时间(秒) (默认: {JMETER_RAMPUP})",
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
        quick=args.quick,
        skip_selenium=args.skip_selenium,
        load_engine=args.load_engine,
        skip_prometheus=args.skip_prometheus,
        prometheus_url=args.prometheus_url,
        step=args.step,
        rate_window=args.rate_window,
        label_mode=args.label_mode,
        output_csv=args.output,
        services=services,
        repeat=args.repeat,
        jmeter_users=args.jmeter_users,
        jmeter_scenario=args.jmeter_scenario,
        jmeter_rampup=args.jmeter_rampup,
    )
    runner.run()


if __name__ == "__main__":
    main()
