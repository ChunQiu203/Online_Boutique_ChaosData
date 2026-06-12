#!/usr/bin/env python3
"""读 .test_timeline.json 的时间窗口，去 Prometheus 拉数据验证故障效果"""
import json, requests, pandas as pd, numpy as np
from pathlib import Path

timeline_file = Path(__file__).resolve().parent / ".test_timeline.json"
if not timeline_file.exists():
    print("❌ 找不到 .test_timeline.json，先运行 test_modified_faults.py")
    exit(1)

with open(timeline_file) as f:
    timeline = json.load(f)

PROM = "http://localhost:9090"
STEP = "10s"
RW   = "60s"
NS   = "default"

def query(promql, start, end):
    r = requests.get(f"{PROM}/api/v1/query_range",
        params={"query": promql, "start": start, "end": end, "step": STEP}, timeout=30)
    data = r.json().get("data", {})
    vals = []
    for series in data.get("result", []):
        for ts, v in series.get("values", []):
            vals.append({"ts": pd.to_datetime(float(ts), unit="s", utc=True), "val": float(v)})
    if not vals:
        return pd.Series(dtype=float, name="val")
    df = pd.DataFrame(vals).set_index("ts")
    return df["val"]

def label(svc, metric):
    if metric == "cpu":
        return f'sum(rate(container_cpu_usage_seconds_total{{container=~".*{svc}.*", namespace="{NS}"}}[{RW}])) * 100'
    if metric == "p99":
        return (
            f'histogram_quantile(0.99, sum(rate(istio_request_duration_milliseconds_bucket{{'
            f'reporter="destination", destination_workload="{svc}"}}[{RW}])) by (le)) / 1000'
        )
    if metric == "error":
        return (
            f'(sum(rate(istio_requests_total{{reporter="destination", destination_workload="{svc}",'
            f'response_code!~"2..", response_code!=""}}[{RW}])) or vector(0))'
            f' / sum(rate(istio_requests_total{{reporter="destination", destination_workload="{svc}"}}[{RW}]))'
        )
    if metric == "rps":
        return f'sum(rate(istio_requests_total{{reporter="destination", destination_workload="{svc}"}}[{RW}]))'

print("=" * 75)
print("  故障效果验证（精确时间窗口）")
print("=" * 75)

for exp in timeline:
    ft, svc = exp["fault_type"], exp["service"]
    quiet_start = exp["quiet_start"]
    start_time  = exp["start_time"]
    end_time    = exp["end_time"]
    cooldown_end = exp["cooldown_end"]

    print(f"\n{'─' * 75}")
    print(f"  {ft} → {svc}")
    print(f"    基线: {quiet_start[:19]} ~ {start_time[:19]}")
    print(f"    故障: {start_time[:19]} ~ {end_time[:19]}")
    print(f"    冷却: {end_time[:19]} ~ {cooldown_end[:19]}")
    print(f"{'─' * 75}")

    for metric, name in [("cpu","CPU%"), ("p99","P99(s)"), ("error","Error%")]:
        promql = label(svc, metric)
        try:
            s = query(promql, quiet_start, cooldown_end)
        except Exception as e:
            print(f"    {name:10s}: 查询失败 {e}")
            continue

        if len(s) < 3:
            print(f"    {name:10s}: 无数据")
            continue

        before = s[s.index < pd.to_datetime(start_time, utc=True)]
        during = s[(s.index >= pd.to_datetime(start_time, utc=True)) &
                   (s.index <= pd.to_datetime(end_time, utc=True))]

        if before.empty or during.empty:
            print(f"    {name:10s}: 窗口内无数据点")
            continue

        b_mean, d_mean = before.mean(), during.mean()
        if b_mean and b_mean != 0:
            chg = (d_mean - b_mean) / b_mean * 100
        else:
            chg = float('nan')

        if pd.isna(chg):
            flag = "—"
        elif abs(chg) > 50:
            flag = "✅ 显著"
        elif abs(chg) > 15:
            flag = "⚠ 微弱"
        else:
            flag = "❌ 无效"

        print(f"    {name:10s}: {b_mean:.4f} → {d_mean:.4f}  ({chg:+.0f}%)  {flag}")

    # 系统 RPS
    try:
        s_sys = query(
            f'sum(rate(istio_requests_total{{reporter="destination"}}[{RW}]))',
            quiet_start, cooldown_end)
        if len(s_sys) > 3:
            before_sys = s_sys[s_sys.index < pd.to_datetime(start_time, utc=True)]
            during_sys = s_sys[(s_sys.index >= pd.to_datetime(start_time, utc=True)) &
                               (s_sys.index <= pd.to_datetime(end_time, utc=True))]
            if not before_sys.empty and not during_sys.empty and before_sys.mean() > 0:
                chg_sys = (during_sys.mean() - before_sys.mean()) / before_sys.mean() * 100
                print(f"    系统RPS   : {before_sys.mean():.1f} → {during_sys.mean():.1f}  ({chg_sys:+.0f}%)")
    except Exception:
        pass

print(f"\n{'=' * 75}")
print("  |变化|>50%=✅显著  >15%=⚠微弱  <15%=❌无效")
print("=" * 75)
