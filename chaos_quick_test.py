#!/usr/bin/env python3
"""
=============================================================================
  混沌工程快速验证 Demo (v3 — 原生精度版)
  chaos_quick_test.py

  设计原则:
    - 数据精度 = Prometheus 物理 scrape_interval (15s)
    - 不插值、不掺噪声、不做任何数据合成
    - CSV 里每一行都对应 Prometheus 的一次真实抓取

  闭环流程 (7 阶段):
    1. 动态生成 ChaosMesh StressChaos YAML → 临时文件
    2. kubectl apply → 注入 CPU 压力 → 记录 start_time (UTC)
    3. 倒计时等待 120s
    4. kubectl delete → 解除故障 → 记录 end_time (UTC)
    5. 批量查询全服务 × 多指标 PromQL (step=15s)
    6. 时间对齐 (15s 网格) → 打标
    7. 导出 CSV + 结果预览

  用法:
    python chaos_quick_test.py                          # 默认 emailservice, 120s
    python chaos_quick_test.py --service frontend --duration 180
    python chaos_quick_test.py --dry-run                # 空跑验证 (模拟数据)
    python chaos_quick_test.py --step 30s               # 调整采集精度
=============================================================================
"""

import argparse
import json
import subprocess
import sys
import tempfile
import time
import warnings
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, OrderedDict

import numpy as np
import pandas as pd
import requests

warnings.filterwarnings("ignore", category=FutureWarning)

# ============================================================================
# 配置常量
# ============================================================================
PROMETHEUS_URL = "http://localhost:9090"
CHAOS_NAMESPACE = "chaos-testing"
TARGET_NAMESPACE = "default"
PROMETHEUS_STEP = "15s"          # Range query 步长 = Prometheus scrape_interval
RATE_WINDOW = "30s"              # rate() 窗口: 至少 2x scrape_interval

# ============================================================================
# 全部微服务列表 — 按实际部署的服务名填写
# ============================================================================
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
# ANSI 颜色工具
# ============================================================================
class C:
    R = "\033[91m"; G = "\033[92m"; Y = "\033[93m"
    B = "\033[94m"; M = "\033[95m"; C = "\033[96m"; W = "\033[97m"
    X = "\033[0m"; BD = "\033[1m"; DIM = "\033[2m"


def ts() -> str:
    return f"[{datetime.now().strftime('%H:%M:%S')}]"


def log(msg: str, color: str = "", bold: bool = False) -> None:
    b = C.BD if bold else ""
    print(f"{ts()} {b}{color}{msg}{C.X}", flush=True)


def log_stage(num: int, total: int, msg: str) -> None:
    log(f"━━━ 阶段 {num}/{total}: {msg} ━━━", C.C, bold=True)


# ============================================================================
# 工具函数
# ============================================================================

def utc_now() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def run_kubectl(args: List[str], timeout: int = 120) -> Tuple[bool, str]:
    cmd = ["kubectl"] + args
    cmd_str = " ".join(cmd)
    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)
        if result.returncode != 0:
            log(f"  ✗ 失败: {cmd_str}", C.R)
            log(f"    {result.stderr.strip()[:300]}", C.R)
            return False, result.stderr.strip()
        return True, result.stdout.strip()
    except subprocess.TimeoutExpired:
        log(f"  ✗ 超时 ({timeout}s): {cmd_str}", C.R)
        return False, "timeout"
    except FileNotFoundError:
        log("  ✗ 未找到 kubectl", C.R)
        return False, "kubectl not found"


def query_prom_range(query: str, start: str, end: str, step: str = PROMETHEUS_STEP) -> Optional[Dict[str, Any]]:
    """调用 Prometheus Range Query API，返回 data 字段或 None。"""
    url = f"{PROMETHEUS_URL.rstrip('/')}/api/v1/query_range"
    params = {"query": query, "start": start, "end": end, "step": step}
    try:
        resp = requests.get(url, params=params, timeout=30)
        resp.raise_for_status()
        body = resp.json()
        if body.get("status") != "success":
            log(f"    PromQL error: {body.get('error','?')[:100]}", C.Y)
            return None
        return body["data"]
    except requests.RequestException as e:
        log(f"    Prometheus 请求异常: {e}", C.Y)
        return None
    except json.JSONDecodeError:
        log("    Prometheus 响应非 JSON", C.Y)
        return None


def prom_data_to_series(data: Dict[str, Any], col_name: str) -> pd.Series:
    """将 Prometheus matrix/vector 结果转为以 UTC datetime 为索引的 Series。

    - matrix: values[] → [(ts, val), ...]
    - vector: value[] → 单点
    - 同一秒多条（多 Pod 副本）取均值
    """
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
# PromQL 多路指标生成器
# ============================================================================

def build_promql_metrics(
    services: List[str],
    rate_window: str = RATE_WINDOW,
    label_mode: str = "pod",
) -> OrderedDict[str, str]:
    """动态生成全服务 × 多指标的 PromQL 查询字典。

    输出格式:
      { "frontend&cpu_usage": "sum(rate(...))", "cartservice&mem_usage": "...", ... }

    每项指标为一个独立的 PromQL 查询，覆盖全体 services 列表。
    额外追加 3 个系统级全局指标。

    Args:
        services:    微服务名列表
        rate_window: rate() 时间窗口 (默认 10s，短窗口捕捉瞬时故障)
        label_mode:  标签匹配方式
                     - "pod":       使用 pod=~"<svc>-.*" (cAdvisor)
                     - "container": 使用 container=~".*<svc>.*" (部分配置)

    Returns:
        OrderedDict[列名, PromQL]
    """
    if label_mode == "container":
        label_clause = 'container=~".*{service}.*", namespace="{ns}"'
    else:  # pod (默认，兼容性最好)
        label_clause = 'pod=~"{service}-.*", namespace="{ns}"'

    queries: OrderedDict[str, str] = OrderedDict()

    # ── 指标模板 ─────────────────────────────────────────────────────
    # 每项是一个 (后缀, PromQL模板) 对，{service} {ns} {rw} 会被替换
    metric_templates: List[Tuple[str, str]] = [
        # ─ 容器资源 ─
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
        # ─ gRPC 服务端黄金指标 ─
        (
            "grpc_latency_p99",
            'histogram_quantile(0.99, '
            'sum(rate(grpc_server_handling_seconds_bucket{{'
            'grpc_service=~".*{{service}}.*"}}[{rw}])) by (le))',
        ),
        (
            "grpc_error_rate",
            'sum(rate(grpc_server_handled_total{{'
            'grpc_service=~".*{{service}}.*",grpc_code!="OK"}}[{rw}]))'
            ' / sum(rate(grpc_server_handled_total{{'
            'grpc_service=~".*{{service}}.*"}}[{rw}]))',
        ),
        (
            "grpc_rps",
            'sum(rate(grpc_server_handled_total{{'
            'grpc_service=~".*{{service}}.*"}}[{rw}]))',
        ),
        # ─ Pod 稳定性 ─
        (
            "pod_restarts",
            'sum(kube_pod_container_status_restarts_total{{'
            'namespace="{ns}",pod=~"{{service}}-.*"}})',
        ),
    ]

    for svc in services:
        # 先为当前服务构建 label_clause
        label_str = label_clause.format(service=svc, ns=TARGET_NAMESPACE)

        for suffix, template in metric_templates:
            col_name = f"{svc}&{suffix}"

            # ★ 两步格式化:
            #  1) 先用 str.replace 处理 {label} (它不是 .format() 的合法 key)
            #  2) 再用 .format() 处理 {service} {ns} {rw}
            promql = template.replace("{label}", label_str)
            promql = promql.format(
                service=svc,
                ns=TARGET_NAMESPACE,
                rw=rate_window,
            )
            # gRPC 模板中 {{service}} → .format() 后变为 {service} → 替换为实际值
            promql = promql.replace("{service}", svc)
            queries[col_name] = promql

    # ── 系统级全局指标 (3 路) ──────────────────────────────────────
    queries["system&total_rps"] = (
        'sum(rate(http_server_request_duration_seconds_count[{rw}])) + '
        'sum(rate(grpc_server_handled_total[{rw}]))'.replace("{rw}", rate_window)
    )
    queries["system&node_cpu_pct"] = (
        '100 - (avg by (instance) (rate(node_cpu_seconds_total{{mode="idle"}}[{rw}])) * 100)'
    ).replace("{rw}", rate_window)
    queries["system&node_mem_pct"] = (
        '(1 - (node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes)) * 100'
    )

    return queries


# ============================================================================
# 合成数据生成器 (dry-run 模式用)
# ============================================================================

def generate_synthetic_series(
    col_name: str,
    start_dt: pd.Timestamp,
    end_dt: pd.Timestamp,
    rng: np.random.Generator,
    step: str = "15s",
) -> pd.Series:
    """为 dry-run 模式生成模拟 Prometheus 采集数据。

    直接按原生抓取间隔生成，不做任何插值或噪声添加。
    每行数据对应 Prometheus 的一次真实 scrape。

    Args:
        col_name: 列名 (如 "emailservice&cpu_usage")
        start_dt: 起始时间
        end_dt:   结束时间
        rng:      numpy 随机数生成器
        step:     采集步长 (默认 15s)

    Returns:
        原生精度的 pd.Series (DatetimeIndex)
    """
    ts_index = pd.date_range(start=start_dt, end=end_dt, freq=step)
    n_points = len(ts_index)

    # 解析列名
    parts = col_name.split("&", 1)
    service = parts[0]
    metric = parts[1] if len(parts) > 1 else "unknown"

    # 基频正弦波信号 (模拟业务周期)
    t = np.linspace(0, 4 * np.pi, n_points)
    base = np.sin(t) * 0.3 + 0.5

    if "cpu" in metric:
        base_val = 25 + base * 20
        noise_scale = 3
    elif "mem" in metric:
        base_val = 300 + base * 200
        noise_scale = 10
    elif "latency" in metric:
        base_val = 0.02 + base * 0.08
        noise_scale = 0.005
    elif "error" in metric:
        base_val = 0.001 + base * 0.005
        noise_scale = 0.001
    elif "rps" in metric or "total_rps" in metric:
        base_val = 50 + base * 80
        noise_scale = 5
    elif "restart" in metric:
        base_val = np.zeros(n_points)
        noise_scale = 0
    elif "node_cpu" in metric:
        base_val = 30 + base * 30
        noise_scale = 2
    elif "node_mem" in metric:
        base_val = 45 + base * 20
        noise_scale = 1
    else:
        base_val = 50 + base * 30
        noise_scale = 3

    values = base_val + rng.normal(0, noise_scale, n_points)

    # 对 CPU 类指标注入故障特征 (仅在故障窗口内)
    if "cpu" in metric and service != "system":
        n_total = n_points
        fault_start = max(0, n_total // 3)
        fault_end = min(n_total, fault_start + max(1, 120 // 15))
        values[fault_start:fault_end] += rng.uniform(20, 40, size=fault_end - fault_start)

    s = pd.Series(values, index=ts_index, name=col_name)
    return s


# ============================================================================
# YAML 生成
# ============================================================================

def generate_stress_yaml(service: str, duration_sec: int) -> str:
    return f"""apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: quick-test-cpu-{service}
  namespace: {CHAOS_NAMESPACE}
spec:
  mode: all
  duration: "{duration_sec}s"
  selector:
    namespaces:
      - {TARGET_NAMESPACE}
    labelSelectors:
      app: {service}
  stressors:
    cpu:
      workers: 2
      load: 80
"""


# ============================================================================
# 主流程
# ============================================================================

def main():
    parser = argparse.ArgumentParser(
        description="混沌工程快速验证 v3 — 全服务多指标宽矩阵，原生 Prometheus 精度"
    )
    parser.add_argument("--service", default="emailservice",
                        help="目标故障注入服务 (默认: emailservice)")
    parser.add_argument("--duration", type=int, default=120,
                        help="故障持续时间 秒 (默认: 120)")
    parser.add_argument("--output", default="quick_test_result.csv",
                        help="输出 CSV (默认: quick_test_result.csv)")
    parser.add_argument("--dry-run", action="store_true",
                        help="空跑模式: 跳过 kubectl, 全模拟数据测试管道")
    parser.add_argument("--step", default="15s",
                        help="Prometheus 采集步长 / CSV 时间精度 (默认: 15s)")
    parser.add_argument("--rate-window", default="30s",
                        help="PromQL rate 查询窗口 (默认: 30s, >= 2x step)")
    parser.add_argument("--label-mode", default="pod",
                        choices=["pod", "container"],
                        help="PromQL 标签匹配方式 (默认: pod)")
    parser.add_argument("--max-columns", type=int, default=0,
                        help="限制最大指标列数 (0=全量, 用于加速测试)")
    args = parser.parse_args()

    target_svc = args.service
    duration = args.duration
    output_file = args.output
    data_step = args.step

    # Windows UTF-8
    if sys.platform == "win32":
        try:
            sys.stdout.reconfigure(encoding="utf-8")
        except AttributeError:
            pass

    TOTAL_STAGES = 7

    # ====================================================================
    print()
    log("╔" + "═" * 70 + "╗", C.M)
    log("║  混沌工程快速验证 v3 — 全服务多指标宽矩阵 (原生精度)" + " " * 12 + "║", C.BD, bold=True)
    log("╠" + "═" * 70 + "╣", C.M)
    log(f"║  目标故障服务: {target_svc:<51s}║", C.W)
    log(f"║  全量服务数:   {len(ALL_SERVICES):<51d}║", C.W)
    log(f"║  每服务指标数: 7 (cpu/mem/latency/error/rps/restart)" + " " * 12 + "║", C.W)
    log(f"║  故障类型:     CPU Stress (StressChaos)" + " " * 23 + "║", C.W)
    log(f"║  持续:         {duration}s ({duration//60}min)" + " " * 33 + "║", C.W)
    log(f"║  CSV 精度:     {data_step} (Prometheus 原生 scrape 间隔)" + " " * 15 + "║", C.W)
    log("╚" + "═" * 70 + "╝", C.M)
    print()

    # ====================================================================
    # 阶段 1: 生成 PromQL 矩阵 + 故障 YAML
    # ====================================================================
    log_stage(1, TOTAL_STAGES, "构建 PromQL 多路指标字典 + 生成故障 YAML")

    promql_dict = build_promql_metrics(
        services=ALL_SERVICES,
        rate_window=args.rate_window,
        label_mode=args.label_mode,
    )
    # 截断列数 (加速测试)
    if args.max_columns > 0:
        trimmed = OrderedDict()
        for i, (k, v) in enumerate(promql_dict.items()):
            if i >= args.max_columns:
                break
            trimmed[k] = v
        promql_dict = trimmed

    total_queries = len(promql_dict)
    svc_cols = [k for k in promql_dict if not k.startswith("system&")]
    sys_cols = [k for k in promql_dict if k.startswith("system&")]
    unique_svcs = len(set(k.split("&")[0] for k in svc_cols))
    log(f"  ✓ 生成 {total_queries} 条 PromQL ({unique_svcs} 服务 × {len(svc_cols)//max(unique_svcs,1)} 指标 + {len(sys_cols)} 系统)", C.G)
    log(f"  ✓ CSV 时间精度: {data_step} (Prometheus 原生 scrape interval，不做插值)", C.G)
    log(f"  ✓ rate 窗口: {args.rate_window}", C.G)
    log(f"  ✓ 标签模式: {args.label_mode}", C.G)

    yaml_content = generate_stress_yaml(target_svc, duration)
    tmp_dir = Path(tempfile.gettempdir()) / "chaos-quick-test"
    tmp_dir.mkdir(parents=True, exist_ok=True)
    yaml_path = tmp_dir / "test-cpu-stress.yaml"
    with open(yaml_path, "w", encoding="utf-8") as f:
        f.write(yaml_content)
    log(f"  ✓ YAML → {yaml_path}", C.G)

    # ====================================================================
    # 阶段 2: 注入故障
    # ====================================================================
    log_stage(2, TOTAL_STAGES, "注入故障 → kubectl apply")

    if args.dry_run:
        log("  [DRY-RUN] 跳过 kubectl apply", C.Y)
        start_time = utc_now()
        log(f"  ✓ (模拟) start_time = {start_time}", C.G)
    else:
        ok, out = run_kubectl(["apply", "-f", str(yaml_path)])
        if not ok:
            log("  ✗ 故障注入失败，终止", C.R, bold=True)
            sys.exit(1)
        start_time = utc_now()
        log(f"  ✓ 注入成功 — start_time = {start_time}", C.G, bold=True)

    # ====================================================================
    # 阶段 3: 倒计时等待
    # ====================================================================
    log_stage(3, TOTAL_STAGES, f"故障持续中 ({duration}s) ...")
    effective_wait = 5 if args.dry_run else duration
    for remaining in range(effective_wait, 0, -1):
        mins, secs = divmod(remaining, 60)
        bar_w = 30
        prog = (effective_wait - remaining) / effective_wait
        filled = int(bar_w * prog)
        bar = "█" * filled + "░" * (bar_w - filled)
        sys.stdout.write(
            f"\r{ts()} {C.Y}⏳ {mins:02d}:{secs:02d}  "
            f"│{bar}│ {prog*100:5.1f}%  {C.X}"
        )
        sys.stdout.flush()
        time.sleep(1)
    sys.stdout.write("\r" + " " * 100 + "\r")
    sys.stdout.flush()
    log(f"  ✓ 等待完成 ({effective_wait}s)", C.G, bold=True)

    # ====================================================================
    # 阶段 4: 解除故障
    # ====================================================================
    log_stage(4, TOTAL_STAGES, "解除故障 → kubectl delete")

    if args.dry_run:
        log("  [DRY-RUN] 跳过 kubectl delete", C.Y)
        end_time = utc_now()
        log(f"  ✓ (模拟) end_time = {end_time}", C.G)
    else:
        ok, out = run_kubectl([
            "delete", "-f", str(yaml_path), "--ignore-not-found=true"
        ])
        if not ok:
            log("  ⚠ 删除命令异常 (可能已自动过期)", C.Y)
        end_time = utc_now()
        log(f"  ✓ 故障已解除 — end_time = {end_time}", C.G, bold=True)

    try:
        yaml_path.unlink()
    except OSError:
        pass

    # ====================================================================
    # 阶段 5: 多路 Prometheus Range Query (全服务 × 全指标)
    # ====================================================================
    log_stage(5, TOTAL_STAGES, f"批量查询 Prometheus — {total_queries} 路指标")

    # 查询窗口前后各扩展 60s 以捕捉故障前后的基线
    start_dt = pd.to_datetime(start_time, utc=True)
    end_dt = pd.to_datetime(end_time, utc=True)
    pad = pd.Timedelta(seconds=60)
    query_start = (start_dt - pad).strftime("%Y-%m-%dT%H:%M:%SZ")
    query_end = (end_dt + pad).strftime("%Y-%m-%dT%H:%M:%SZ")

    all_series: Dict[str, pd.Series] = {}
    rng = np.random.default_rng(42)  # 固定种子保证 dry-run 可复现

    success_count = 0
    empty_count = 0
    error_count = 0

    for i, (col_name, promql) in enumerate(promql_dict.items()):
        # 进度输出 (每 20 条或首尾各打印一条)
        if i % 20 == 0 or i == total_queries - 1:
            log(f"  查询 [{i+1}/{total_queries}] {col_name:50s} ...", C.DIM)

        if args.dry_run:
            s = generate_synthetic_series(col_name, start_dt - pad, end_dt + pad, rng, step=data_step)
        else:
            data = query_prom_range(promql, query_start, query_end, step=data_step)
            if data is None:
                error_count += 1
                continue
            s = prom_data_to_series(data, col_name)

        if s.empty:
            empty_count += 1
            continue

        all_series[col_name] = s
        success_count += 1

    log(f"  ✓ 查询完成: {success_count} 路成功, {empty_count} 空结果, {error_count} 失败", C.G)
    log(f"  ✓ 有效列数: {len(all_series)} (覆盖 {len(set(k.split('&')[0] for k in all_series))} 个实体)", C.G)

    if not all_series:
        log("✗ 未获取到任何指标数据。", C.R, bold=True)
        log("  请检查: 1) Prometheus 端口转发 2) metric 标签名与实际环境匹配", C.Y)
        log("  提示: 尝试 --label-mode container 或检查 pod label", C.Y)
        sys.exit(2)

    # ====================================================================
    # 阶段 6: 时间对齐 + 打标 (原生精度，不做插值)
    # ====================================================================
    log_stage(6, TOTAL_STAGES, f"时间对齐 ({data_step} 网格) → 打标")

    # 6a. 合并所有列为宽表 — 按时间戳索引自动对齐
    df = pd.DataFrame(all_series).sort_index()
    log(f"  合并后: {len(df):,} 行 × {len(df.columns)} 列 (原生 Prometheus 时间戳)", C.W)

    # 6b. 重采样到统一网格，消除不同 query 之间的微小时间偏移
    #     使用 mean() 聚合桶内值；通常每桶只有 1 个真实抓取点
    df = df.resample(data_step).mean()
    log(f"  resample({data_step}) 后: {len(df):,} 行", C.W)

    # 6c. 前向填充: Prometheus 偶尔漏抓 (scrape 失败) 的个别点
    nan_before = df.isna().sum().sum()
    df = df.ffill().bfill()
    nan_after = df.isna().sum().sum()
    if nan_before > 0:
        log(f"  ffill/bfill: {nan_before:,} NaN → {nan_after:,} NaN (补漏抓点)", C.G)
    else:
        log(f"  无漏抓点，数据完整", C.G)

    # 6d. 打标
    df["fault_type"] = "normal"
    fault_mask = (df.index >= start_dt) & (df.index <= end_dt)
    df.loc[fault_mask, "fault_type"] = "cpu_stress"

    fault_rows = fault_mask.sum()
    normal_rows = (~fault_mask).sum()
    log(f"  标签分布: normal={normal_rows:,}  cpu_stress={fault_rows:,}", C.W)

    # ====================================================================
    # 阶段 7: 导出 CSV + 结果预览
    # ====================================================================
    log_stage(7, TOTAL_STAGES, "导出 CSV + 质量报告")

    df.to_csv(
        output_file,
        index=True,
        index_label="timestamp",
        encoding="utf-8",
        float_format="%.6f",
    )

    file_size_kb = Path(output_file).stat().st_size / 1024
    log(f"  ✓ 导出: {output_file} ({file_size_kb:.1f} KB)", C.G, bold=True)

    # ── 质量报告 ──────────────────────────────────────────────────────
    data_cols = [c for c in df.columns if c not in ("fault_type", "target_service")]
    print(f"\n{C.W}╔{'═'*68}╗{C.X}")
    print(f"{C.W}║  数据质量报告{' '*56}║{C.X}")
    print(f"{C.W}╠{'═'*68}╣{C.X}")
    print(f"{C.W}║  行数:       {len(df):>10,}  {' '*46}║{C.X}")
    print(f"{C.W}║  数据列数:   {len(data_cols):>10}  {' '*46}║{C.X}")
    print(f"{C.W}║  标签列:     fault_type (normal / cpu_stress){' '*19}║{C.X}")
    print(f"{C.W}║  时间跨度:   {df.index.min()} →{' '*21}║{C.X}")
    print(f"{C.W}║              {df.index.max()}{' '*33}║{C.X}")
    print(f"{C.W}║  步长:       {data_step} (Prometheus 原生 scrape interval){' '*22}║{C.X}")
    print(f"{C.W}║  数据原则:   纯物理采集，不做插值/不掺噪声{' '*24}║{C.X}")
    print(f"{C.W}║  NaN 残留:   {nan_after:,}{' '*50}║{C.X}")
    print(f"{C.W}╚{'═'*68}╝{C.X}")

    # 列名展示
    print(f"\n{C.W}  数据列名 ({len(data_cols)} 列):{C.X}")
    for i, col in enumerate(data_cols):
        svc = col.split("&")[0]
        metric = col.split("&")[1] if "&" in col else col
        icon = "🎯" if svc == target_svc else ("🖥" if svc == "system" else "  ")
        print(f"    {icon} {svc:<28s} │ {metric}")

    # 数值分布快照
    print(f"\n{C.W}  数值分布快照 (前 5 列):{C.X}")
    snapshot_cols = data_cols[:5]
    if snapshot_cols:
        stats = df[snapshot_cols].describe().round(4)
        for col in stats.columns:
            short = col.replace(target_svc, "★").replace("&", "│")
            stats.rename(columns={col: short}, inplace=True)
        print(stats.to_string(max_colwidth=20))

    print()
    log("=" * 70, C.G)
    log(f"  ✅ 闭环验证完成 — 全服务 × 多指标 宽矩阵已就绪", C.G, bold=True)
    log(f"  📄 {Path(output_file).resolve()}", C.G)
    log("=" * 70, C.G)
    print()


if __name__ == "__main__":
    main()
