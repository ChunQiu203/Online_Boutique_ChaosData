#!/usr/bin/env python3
"""快速验证修改后的 5 种弱故障是否生效 —— 每种的第一个 rep 只跑一个，用 quick 模式。"""

from __future__ import annotations
import subprocess, sys, time, json
from datetime import datetime, timezone
from pathlib import Path

SCRIPT_ROOT = Path(__file__).resolve().parent

# 只测试这 5 个修改过的故障
TARGET_FAULTS = ["pod-failure", "network-corrupt", "dns-error", "jvm-cpu", "jvm-latency"]

# Quick 模式参数
QUIET_SEC  = 60    # 静默 1 min
FAULT_SEC  = 120   # 故障 2 min
COOLDOWN_SEC = 90  # 冷却 1.5 min

def run(cmd, dry=False):
    print(f"  $ {' '.join(cmd)}")
    if dry:
        return True, "", ""
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    return r.returncode == 0, r.stdout, r.stderr

def main():
    print("=" * 70)
    print("  修改后故障快速验证 (5 种，各 1 次)")
    print("=" * 70)

    # 1. 检查前置条件
    ok, out, err = run(["kubectl", "get", "pods", "-n", "default"])
    if not ok:
        print("❌ kubectl 连不上集群，先 minikube start")
        sys.exit(1)

    ok, out, err = run(["kubectl", "get", "pods", "-n", "chaos-testing"])
    if not ok:
        print("❌ chaos-testing namespace 不存在，ChaosMesh 没部署")
        sys.exit(1)

    # 2. 检查 Prometheus 端口转发
    import requests
    try:
        r = requests.get("http://localhost:9090/api/v1/status/runtimeinfo", timeout=5)
        prom_ok = r.status_code == 200
    except Exception:
        prom_ok = False
    if not prom_ok:
        print("⚠ Prometheus :9090 不可达，将跳过指标采集 (仅验证故障注入)")
        print("  如需采集: kubectl port-forward -n monitoring svc/prometheus 9090:9090 &")
    else:
        print("✓ Prometheus 可达")

    # 3. 导入故障模板
    sys.path.insert(0, str(SCRIPT_ROOT))
    from run_chaos_with_selenium import (
        FAULT_TEMPLATES, TARGET_NAMESPACE, CHAOS_NAMESPACE,
        TARGET_SERVICE_POOL, JVM_SERVICE_POOL,
        build_promql_metrics, query_prom_range, prom_data_to_series,
        Colors, cprint, get_utc_now_iso, countdown,
        PROMETHEUS_STEP, PROMETHEUS_RATE_WIN, ALL_SERVICES, PROMETHEUS_URL,
    )

    test_templates = [t for t in FAULT_TEMPLATES if t["fault_type"] in TARGET_FAULTS]
    print(f"\n待测试故障: {len(test_templates)} 种")
    for t in test_templates:
        svc = t.get("service", "🎲随机")
        print(f"  {t['fault_type']:20s} → {svc:25s}  (kind={t['chaos_kind']})")

    # 4. 逐故障测试
    results = []
    timeline = []  # 保存每个实验的时间窗口
    for i, tmpl in enumerate(test_templates):
        ft = tmpl["fault_type"]
        svc = tmpl.get("service")
        if svc is None:
            import numpy as np
            svc = str(np.random.default_rng().choice(TARGET_SERVICE_POOL))

        print(f"\n{'─' * 70}")
        print(f"  [{i+1}/{len(test_templates)}] 测试: {ft} → {svc}")
        print(f"{'─' * 70}")

        # 生成 YAML
        yaml = tmpl["yaml_template"].format(
            service=svc,
            target_ns=TARGET_NAMESPACE,
            chaos_ns=CHAOS_NAMESPACE,
            duration=f"{FAULT_SEC}s",
        )

        # 记录时间
        quiet_start = get_utc_now_iso()
        print(f"  静默基线期 ({QUIET_SEC}s) ...")
        countdown(QUIET_SEC, "静默", Colors.CYAN)

        # 注入
        tmp_yaml = SCRIPT_ROOT / ".tmp_test_fault.yaml"
        tmp_yaml.write_text(yaml, encoding="utf-8")
        start_time = get_utc_now_iso()
        print(f"  注入故障 (start={start_time[:19]}) ...")
        ok, out, err = run(["kubectl", "apply", "-f", str(tmp_yaml), "--validate=false"])
        if not ok:
            print(f"  ❌ kubectl apply 失败:\n{err}")
            results.append((ft, svc, "apply_failed"))
            continue

        # 等待故障持续
        countdown(FAULT_SEC, "故障中", Colors.MAGENTA)

        # 删除故障
        end_time = get_utc_now_iso()
        print(f"  删除故障 (end={end_time[:19]}) ...")
        ok, out, err = run(["kubectl", "delete", "-f", str(tmp_yaml), "--ignore-not-found=true"])
        tmp_yaml.unlink(missing_ok=True)

        # 冷却
        print(f"  冷却恢复期 ({COOLDOWN_SEC}s) ...")
        countdown(COOLDOWN_SEC, "冷却", Colors.CYAN)
        cooldown_end = get_utc_now_iso()

        # 记录时间窗口
        timeline.append({
            "fault_type": ft,
            "service": svc,
            "quiet_start": quiet_start,
            "start_time": start_time,
            "end_time": end_time,
            "cooldown_end": cooldown_end,
        })
        # 每个实验跑完就写一次，crash-safe
        with open(SCRIPT_ROOT / ".test_timeline.json", "w") as f:
            json.dump(timeline, f, indent=2, ensure_ascii=False)

        # 采集 Prometheus 指标
        if prom_ok:
            print("  采集 Prometheus 指标 ...")
            from datetime import timedelta
            import pandas as pd

            promql_dict = build_promql_metrics(ALL_SERVICES, PROMETHEUS_RATE_WIN)
            q_start = (pd.to_datetime(start_time, utc=True) - timedelta(seconds=QUIET_SEC + 60))
            q_end = (pd.to_datetime(end_time, utc=True) + timedelta(seconds=COOLDOWN_SEC + 60))

            series_list = []
            for col, promql in list(promql_dict.items())[:20]:  # 只采 20 列做快速验证
                data = query_prom_range(promql,
                    q_start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    q_end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    step=PROMETHEUS_STEP)
                if data:
                    s = prom_data_to_series(data, col)
                    if not s.empty:
                        series_list.append(s)

            if series_list:
                df = pd.concat(series_list, axis=1, join="outer")
                # 简单对比 baseline vs fault
                t0 = pd.to_datetime(start_time, utc=True)
                t1 = pd.to_datetime(end_time, utc=True)
                before = df[df.index < t0]
                during = df[(df.index >= t0) & (df.index <= t1)]

                # 只看目标服务
                for metric in ["cpu_usage", "grpc_latency_p99", "grpc_error_rate"]:
                    col = f"{svc}&{metric}"
                    if col in df.columns:
                        b = before[col].mean()
                        d = during[col].mean()
                        if b and d and b > 0:
                            chg = (d - b) / b * 100
                            impact = "✅ 显著" if abs(chg) > 50 else ("⚠ 微弱" if abs(chg) > 10 else "❌ 无效")
                            print(f"    {metric:20s}: {b:.3f} → {d:.3f}  ({chg:+.0f}%)  {impact}")
                        else:
                            print(f"    {metric:20s}: 无数据")

        results.append((ft, svc, "done"))
        print(f"  ✅ {ft} → {svc} 完成")

    # 5. 汇总
    print(f"\n{'=' * 70}")
    print("  验证汇总")
    print(f"{'=' * 70}")
    for ft, svc, status in results:
        icon = "✅" if status == "done" else "❌"
        print(f"  {icon} {ft:20s} → {svc:25s}  ({status})")
    print(f"\n成功: {sum(1 for r in results if r[2]=='done')}/{len(results)}")

if __name__ == "__main__":
    main()
