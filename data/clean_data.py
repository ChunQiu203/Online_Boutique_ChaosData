"""
数据清洗脚本
输入: final_dataset_for_algorithm.csv, chaos_history.json
输出: final_dataset_clean.csv, chaos_history_clean.json

清洗内容:
  1. timestamp UTC时间 -> Unix秒级时间戳
  2. pod_restarts 递减处理（Pod重建时计数器归零，转换为真实累计值）
  3. 数据质量报告
"""

import pandas as pd
import numpy as np
import json
import sys
from pathlib import Path


# ============================================================
# 配置
# ============================================================
BASE_DIR = Path(__file__).parent
CSV_INPUT = BASE_DIR / "final_dataset_for_algorithm.csv"
JSON_INPUT = BASE_DIR / "chaos_history.json"
CSV_OUTPUT = BASE_DIR / "final_dataset_clean.csv"
JSON_OUTPUT = BASE_DIR / "chaos_history_clean.json"
REPORT_OUTPUT = BASE_DIR / "clean_report.txt"


def clean_csv():
    """清洗 CSV 文件"""
    print("=" * 60)
    print("清洗 CSV: final_dataset_for_algorithm.csv")
    print("=" * 60)

    df = pd.read_csv(CSV_INPUT)
    report_lines = []
    changes = 0

    # ----------------------------------------------------------
    # Step 1: UTC时间 -> Unix秒级时间戳
    # ----------------------------------------------------------
    print("\n[Step 1] 转换 UTC 时间为 Unix 秒级时间戳...")
    # pandas datetime64 内部是纳秒，整除 10^9 得到秒，原地替换保持列位置不变
    ts_dt = pd.to_datetime(df['timestamp'])
    df['timestamp'] = (ts_dt.astype('int64') // 10**9).astype('int64')
    report_lines.append(f"Step 1: UTC datetime -> Unix timestamp (seconds)")
    report_lines.append(f"  时间范围: {df['timestamp'].min()} ~ {df['timestamp'].max()}")

    # ----------------------------------------------------------
    # Step 2: pod_restarts 递减处理
    # ----------------------------------------------------------
    print("[Step 2] 处理 pod_restarts 递减（Pod重建导致计数器归零）...")

    service_names = sorted(set(
        c.split('&')[0] for c in df.columns
        if '&' in c and c not in ('system&total_rps', 'system&node_cpu_pct', 'system&node_mem_pct')
    ))

    rebuild_events = []

    for svc in service_names:
        col = f'{svc}&pod_restarts'
        if col not in df.columns:
            continue

        original = df[col].copy()
        diffs = original.diff().fillna(0)

        # 检测递减点（Pod重建）
        decrease_mask = diffs < 0
        n_decreases = decrease_mask.sum()

        if n_decreases > 0:
            # 修复策略：当值递减时，意味着Pod重建，计数器归零
            # 将当前值 + 上一时刻的累计值，恢复为真实累计重启次数
            fixed = original.copy()
            accumulator = 0

            for i in range(len(fixed)):
                if decrease_mask.iloc[i]:
                    # Pod重建：用修复后的前一行值作为新的累计基线
                    accumulator = fixed.iloc[i - 1]
                    fixed.iloc[i] = accumulator + original.iloc[i]
                else:
                    fixed.iloc[i] = accumulator + original.iloc[i]

            df[col] = fixed

            for idx in diffs[decrease_mask].index:
                ts_unix = df.loc[idx, 'timestamp']
                ts_readable = pd.to_datetime(ts_unix, unit='s', utc=True)
                rebuild_events.append({
                    'service': svc,
                    'row': idx,
                    'timestamp_unix': int(ts_unix),
                    'timestamp_utc': str(ts_readable),
                    'old_value': original.loc[idx - 1],
                    'new_value_after_reset': original.loc[idx],
                    'fixed_value': fixed.loc[idx],
                })
                report_lines.append(
                    f"  [Pod重建] {svc} @ row {idx} (unix={ts_unix}, {ts_readable}): "
                    f"{original.loc[idx - 1]:.0f} -> {original.loc[idx]:.0f} "
                    f"(fixed: {fixed.loc[idx]:.0f})"
                )
            changes += n_decreases

    if not rebuild_events:
        report_lines.append("  无Pod重建事件")
    else:
        report_lines.append(f"  共修复 {len(rebuild_events)} 处递减: 转为累计值")

    # ----------------------------------------------------------
    # Step 3: 保存
    # ----------------------------------------------------------
    print("[Step 3] 保存清洗后的 CSV...")
    df.to_csv(CSV_OUTPUT, index=False)
    report_lines.append(f"\n输出: {CSV_OUTPUT}")
    report_lines.append(f"行数: {len(df)}, 列数: {len(df.columns)}")

    return df, report_lines, rebuild_events


def clean_json():
    """清洗 JSON 文件"""
    print("\n" + "=" * 60)
    print("清洗 JSON: chaos_history.json")
    print("=" * 60)

    with open(JSON_INPUT) as f:
        experiments = json.load(f)

    report_lines = []

    # ----------------------------------------------------------
    # 校验 + 补充字段
    # ----------------------------------------------------------
    print("[Step 1] 校验时间逻辑 & 补充 derived 字段...")

    time_errors = []
    for i, exp in enumerate(experiments):
        # 解析时间
        start = pd.to_datetime(exp['start_time'])
        end = pd.to_datetime(exp['end_time'])
        quiet = pd.to_datetime(exp['quiet_start_time'])
        cooldown = pd.to_datetime(exp['cooldown_end_time'])

        # 校验时间顺序: quiet < start < end < cooldown
        if not (quiet < start < end < cooldown):
            time_errors.append({
                'index': i,
                'experiment_id': exp['experiment_id'],
                'quiet': str(quiet),
                'start': str(start),
                'end': str(end),
                'cooldown': str(cooldown),
            })

        # 补充 computed 字段
        exp['duration_seconds'] = (end - start).total_seconds()
        exp['quiet_duration_seconds'] = (start - quiet).total_seconds()
        exp['cooldown_duration_seconds'] = (cooldown - end).total_seconds()

    if time_errors:
        for e in time_errors:
            report_lines.append(
                f"  [TIME ERROR] exp #{e['index']} ({e['experiment_id'][:8]}...): "
                f"时间顺序异常"
            )
    else:
        report_lines.append("  所有33条实验时间逻辑正确 (quiet < start < end < cooldown)")

    # 统计
    avg_dur = np.mean([e['duration_seconds'] for e in experiments])
    report_lines.append(f"  平均故障注入时长: {avg_dur:.0f}s ({avg_dur/60:.1f}min)")

    # ----------------------------------------------------------
    # Step 2: 检查实验间隔是否重叠
    # ----------------------------------------------------------
    print("[Step 2] 检查实验时间窗口重叠...")

    # 按 start_time 排序
    sorted_exps = sorted(experiments, key=lambda e: e['start_time'])
    overlaps = []
    for i in range(len(sorted_exps) - 1):
        prev = sorted_exps[i]
        curr = sorted_exps[i + 1]
        prev_cooldown = pd.to_datetime(prev['cooldown_end_time'])
        curr_quiet = pd.to_datetime(curr['quiet_start_time'])

        if prev_cooldown > curr_quiet:
            overlaps.append({
                'prev_id': prev['experiment_id'][:8],
                'curr_id': curr['experiment_id'][:8],
                'prev_cooldown': str(prev_cooldown),
                'curr_quiet': str(curr_quiet),
            })

    if overlaps:
        for o in overlaps:
            report_lines.append(
                f"  [OVERLAP] {o['prev_id']} cooldown={o['prev_cooldown']} "
                f"overlaps {o['curr_id']} quiet={o['curr_quiet']}"
            )
    else:
        report_lines.append("  所有实验窗口无重叠")

    # ----------------------------------------------------------
    # Step 3: 保存
    # ----------------------------------------------------------
    print("[Step 3] 保存清洗后的 JSON...")
    with open(JSON_OUTPUT, 'w') as f:
        json.dump(experiments, f, indent=2, ensure_ascii=False)

    report_lines.append(f"\n输出: {JSON_OUTPUT}")
    report_lines.append(f"记录数: {len(experiments)}")

    return experiments, report_lines


def main():
    print("开始数据清洗...")
    print(f"输入目录: {BASE_DIR}\n")

    # ---- 清洗 CSV ----
    df_clean, csv_report, rebuild_events = clean_csv()

    # ---- 清洗 JSON ----
    exp_clean, json_report = clean_json()

    # ---- 生成报告 ----
    print("\n" + "=" * 60)
    print("生成清洗报告...")

    report = []
    report.append("=" * 60)
    report.append("数据清洗报告")
    report.append("=" * 60)
    report.append(f"清洗时间: {pd.Timestamp.now()}")
    report.append("")

    report.append("--- final_dataset_for_algorithm.csv ---")
    report.extend(csv_report)
    report.append("")

    report.append("--- chaos_history.json ---")
    report.extend(json_report)
    report.append("")

    report.append("=" * 60)
    report.append("Pod重建事件详情:")
    report.append("=" * 60)
    if rebuild_events:
        for ev in rebuild_events:
            report.append(
                f"  {ev['service']:25s} | row {ev['row']:4d} | "
                f"unix={ev['timestamp_unix']} | {ev['timestamp_utc']} | "
                f"{ev['old_value']:.0f} -> {ev['new_value_after_reset']:.0f} | "
                f"fixed: {ev['fixed_value']:.0f}"
            )
    else:
        report.append("  无")
    report.append("")

    report.append("=" * 60)
    report.append("清洗后数据摘要:")
    report.append("=" * 60)
    report.append(f"  CSV: {len(df_clean)} 行 x {len(df_clean.columns)} 列")
    report.append(f"  JSON: {len(exp_clean)} 条实验记录")
    report.append(f"  Pod重建事件: {len(rebuild_events)} 处")
    report.append("")

    report_text = "\n".join(report)

    with open(REPORT_OUTPUT, 'w', encoding='utf-8') as f:
        f.write(report_text)

    print(report_text)
    print(f"\n清洗完成!")
    print(f"  CSV -> {CSV_OUTPUT}")
    print(f"  JSON -> {JSON_OUTPUT}")
    print(f"  报告 -> {REPORT_OUTPUT}")


if __name__ == "__main__":
    main()
