"""
监控指标可视化脚本
读取清洗后的指标数据与混沌实验记录，绘制：
  1. 单服务仪表盘 —— 指定服务的全部指标 + 故障区间着色
  2. 全部服务概览 —— 11个服务的核心指标热力图
  3. 故障影响对比 —— 某个故障前后各服务指标变化

用法：
  python plot_metrics.py                  # 生成全部图
  python plot_metrics.py --service adservice  # 只看某个服务
  python plot_metrics.py --fault network-delay # 只看某类故障的影响
"""

import pandas as pd
import numpy as np
import json
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
from matplotlib.patches import Patch
from pathlib import Path
import argparse
import sys
import warnings

# 忽略 tight_layout 与 colorbar 的兼容性警告（不影响出图）
warnings.filterwarnings('ignore', message='.*tight_layout.*')

# ============================================================
# 配置
# ============================================================
BASE_DIR = Path(__file__).parent / "data" / "cleandata"
CSV_PATH = BASE_DIR / "final_dataset_clean.csv"
JSON_PATH = BASE_DIR / "chaos_history_clean.json"
OUTPUT_DIR = Path(__file__).parent / "plots"
OUTPUT_DIR.mkdir(exist_ok=True)

# 中文字体设置
plt.rcParams['font.sans-serif'] = ['SimHei', 'Microsoft YaHei', 'DejaVu Sans']
plt.rcParams['axes.unicode_minus'] = False

# 服务颜色映射（固定，方便识别）
SERVICE_COLORS = {
    'frontend': '#1f77b4',
    'cartservice': '#ff7f0e',
    'productcatalogservice': '#2ca02c',
    'currencyservice': '#d62728',
    'paymentservice': '#9467bd',
    'shippingservice': '#8c564b',
    'checkoutservice': '#e377c2',
    'emailservice': '#7f7f7f',
    'recommendationservice': '#bcbd22',
    'adservice': '#17becf',
    'reviewservice': '#aec7e8',
}

# 故障类型颜色
FAULT_COLORS = {
    'network-delay': '#ff6b6b',
    'network-corrupt': '#ffa502',
    'network-loss': '#ff6348',
    'cpu-stress': '#e056a0',
    'memory-stress': '#9b59b6',
    'pod-failure': '#e74c3c',
    'pod-kill': '#c0392b',
    'container-kill': '#d63031',
    'jvm-latency': '#fdcb6e',
    'jvm-cpu': '#f39c12',
    'dns-error': '#6c5ce7',
    'normal': '#dfe6e9',
}

METRIC_LABELS = {
    'cpu_usage': 'CPU Usage (cores)',
    'mem_usage_mb': 'Memory (MB)',
    'mem_usage_pct': 'Memory (%)',
    'grpc_latency_p99': 'gRPC P99 Latency (ms)',
    'grpc_error_rate': 'gRPC Error Rate',
    'grpc_rps': 'gRPC Requests/s',
    'pod_restarts': 'Pod Restarts (cumulative)',
    'total_rps': 'Total RPS',
    'node_cpu_pct': 'Node CPU (%)',
    'node_mem_pct': 'Node Memory (%)',
}


def load_data():
    """加载清洗后的数据"""
    df = pd.read_csv(CSV_PATH)
    # timestamp 是 Unix 秒，转为 datetime
    df['datetime'] = pd.to_datetime(df['timestamp'], unit='s', utc=True)
    # 也转成本地时间便于看图
    df['datetime_local'] = df['datetime'].dt.tz_convert('Asia/Shanghai')

    with open(JSON_PATH) as f:
        experiments = json.load(f)

    # 解析实验时间
    for exp in experiments:
        exp['start_dt'] = pd.to_datetime(exp['start_time'])
        exp['end_dt'] = pd.to_datetime(exp['end_time'])
        exp['quiet_dt'] = pd.to_datetime(exp['quiet_start_time'])
        exp['cooldown_dt'] = pd.to_datetime(exp['cooldown_end_time'])

    return df, experiments


def get_service_names(df):
    """从列名提取所有微服务名称"""
    svc = set()
    for c in df.columns:
        if '&' in c:
            s = c.split('&')[0]
            if s != 'system':
                svc.add(s)
    return sorted(svc)


# ============================================================
# 绘图函数 1: 单服务仪表盘
# ============================================================
def plot_service_dashboard(df, experiments, service, save=True):
    """
    绘制指定服务的完整仪表盘
    包含: CPU / Memory / P99 Latency / Error Rate / RPS + 故障区间着色
    """
    fig, axes = plt.subplots(5, 1, figsize=(20, 24), sharex=True)

    metrics = ['cpu_usage', 'mem_usage_pct', 'grpc_latency_p99', 'grpc_error_rate', 'grpc_rps']
    colors = ['#3498db', '#9b59b6', '#e74c3c', '#e67e22', '#2ecc71']

    for ax, metric, color in zip(axes, metrics, colors):
        col = f'{service}&{metric}'
        if col not in df.columns:
            ax.set_title(f'{metric} - column not found')
            continue

        ax.plot(df['datetime_local'], df[col], color=color, linewidth=0.6, alpha=0.9)
        ax.set_ylabel(METRIC_LABELS.get(metric, metric), fontsize=11)
        ax.set_title(f'{service} — {METRIC_LABELS.get(metric, metric)}', fontsize=12, fontweight='bold')
        ax.grid(True, alpha=0.3)

        # 标记故障区间
        for exp in experiments:
            if exp['fault_type'] == 'normal':
                continue
            fault_color = FAULT_COLORS.get(exp['fault_type'], '#ff0000')
            ax.axvspan(exp['start_dt'].tz_convert('Asia/Shanghai'),
                       exp['end_dt'].tz_convert('Asia/Shanghai'),
                       alpha=0.12, facecolor=fault_color,edgecolor='none')

            # 如果该故障作用于当前服务，加粗标记
            if exp['service'] == service:
                ax.axvspan(exp['start_dt'].tz_convert('Asia/Shanghai'),
                           exp['end_dt'].tz_convert('Asia/Shanghai'),
                           alpha=0.3, facecolor=fault_color,edgecolor='red', linewidth=1.5, linestyle='--')

    # x轴格式
    axes[-1].set_xlabel('Time (Asia/Shanghai)', fontsize=12)
    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    axes[-1].xaxis.set_major_locator(mdates.HourLocator(interval=1))
    plt.xticks(rotation=0)

    # 图例
    legend_patches = []
    fault_types_in_plot = set()
    for exp in experiments:
        ft = exp['fault_type']
        if ft != 'normal' and ft not in fault_types_in_plot:
            fault_types_in_plot.add(ft)
            legend_patches.append(
                Patch(facecolor=FAULT_COLORS.get(ft, '#ff0000'), alpha=0.4,
                      label=f'{ft} ({exp["service"]})' if exp['service'] == service else ft)
            )

    fig.legend(handles=legend_patches, loc='lower center', ncol=6, fontsize=9,
               bbox_to_anchor=(0.5, -0.01))

    fig.suptitle(f'{service} — Metrics Dashboard with Fault Injection Overlay',
                 fontsize=16, fontweight='bold', y=1.01)

    plt.tight_layout()

    if save:
        path = OUTPUT_DIR / f'dashboard_{service}.png'
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f'  Saved: {path}')

    plt.close(fig)
    return fig


# ============================================================
# 绘图函数 2: 全部服务概览热力图
# ============================================================
def plot_all_services_heatmap(df, experiments, metric='grpc_latency_p99', save=True):
    """
    所有服务的单个指标热力图，一眼看出哪些服务在什么时间异常
    """
    services = get_service_names(df)

    fig, axes = plt.subplots(len(services), 1, figsize=(20, 2.2 * len(services)), sharex=True)

    if len(services) == 1:
        axes = [axes]

    for ax, svc in zip(axes, services):
        col = f'{svc}&{metric}'
        if col not in df.columns:
            continue

        # 用颜色映射数值大小
        values = df[col].values
        # 归一化到 0-1
        vmin, vmax = np.percentile(values, [1, 99])
        if vmax == vmin:
            vmax = vmin + 1
        norm_values = np.clip((values - vmin) / (vmax - vmin), 0, 1)

        # 画散点热力条
        times = df['datetime_local']
        scatter = ax.scatter(times, [svc] * len(times), c=norm_values,
                             cmap='YlOrRd', s=15, edgecolors='none', alpha=0.8,
                             vmin=0, vmax=1)

        ax.set_ylabel(svc, fontsize=9, rotation=30, ha='right', va='center')

        # 故障区间
        for exp in experiments:
            if exp['fault_type'] == 'normal':
                continue
            color = FAULT_COLORS.get(exp['fault_type'], '#ff0000')
            ax.axvspan(exp['start_dt'].tz_convert('Asia/Shanghai'),
                       exp['end_dt'].tz_convert('Asia/Shanghai'),
                       alpha=0.2, facecolor=color, edgecolor='none')

    axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    axes[-1].xaxis.set_major_locator(mdates.HourLocator(interval=1))

    # colorbar
    cbar = fig.colorbar(scatter, ax=axes, shrink=0.5, aspect=30, pad=0.02)
    cbar.set_label(f'{METRIC_LABELS.get(metric, metric)} (normalized)', fontsize=10)

    fig.suptitle(f'All Services — {METRIC_LABELS.get(metric, metric)} Heatmap',
                 fontsize=14, fontweight='bold')
    plt.tight_layout()

    if save:
        path = OUTPUT_DIR / f'heatmap_{metric}.png'
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f'  Saved: {path}')

    return fig


# ============================================================
# 绘图函数 3: 故障前后对比（系统级）
# ============================================================
def plot_fault_impact_overview(df, experiments, save=True):
    """
    系统级总览：Total RPS + Node CPU + Node Memory + 告警服务热度
    """
    fig = plt.figure(figsize=(22, 14))
    gs = fig.add_gridspec(4, 2, hspace=0.3, wspace=0.25)

    # 3.1 系统总览
    ax1 = fig.add_subplot(gs[0, :])
    ax1.plot(df['datetime_local'], df['system&total_rps'], color='#2c3e50', linewidth=0.8)
    ax1.set_ylabel('Total RPS', fontsize=11)
    ax1.set_title('System Overview: Total Requests Per Second', fontsize=13, fontweight='bold')
    ax1.grid(True, alpha=0.3)

    # 3.2 Node CPU
    ax2 = fig.add_subplot(gs[1, 0])
    ax2.plot(df['datetime_local'], df['system&node_cpu_pct'], color='#e74c3c', linewidth=0.8)
    ax2.set_ylabel('Node CPU (%)', fontsize=11)
    ax2.set_title('Node CPU Usage', fontsize=12, fontweight='bold')
    ax2.grid(True, alpha=0.3)

    # 3.3 Node Memory
    ax3 = fig.add_subplot(gs[1, 1])
    ax3.plot(df['datetime_local'], df['system&node_mem_pct'], color='#8e44ad', linewidth=0.8)
    ax3.set_ylabel('Node Memory (%)', fontsize=11)
    ax3.set_title('Node Memory Usage', fontsize=12, fontweight='bold')
    ax3.grid(True, alpha=0.3)

    # 3.4 各服务错误率总览
    ax4 = fig.add_subplot(gs[2, :])
    services = get_service_names(df)
    for svc in services:
        col = f'{svc}&grpc_error_rate'
        if col in df.columns:
            color = SERVICE_COLORS.get(svc, '#999999')
            ax4.plot(df['datetime_local'], df[col], color=color, linewidth=0.5, alpha=0.8, label=svc)
    ax4.set_ylabel('Error Rate', fontsize=11)
    ax4.set_title('All Services — gRPC Error Rate', fontsize=12, fontweight='bold')
    ax4.legend(loc='upper left', ncol=6, fontsize=7)
    ax4.grid(True, alpha=0.3)

    # 3.5 各服务延迟总览
    ax5 = fig.add_subplot(gs[3, :])
    for svc in services:
        col = f'{svc}&grpc_latency_p99'
        if col in df.columns:
            color = SERVICE_COLORS.get(svc, '#999999')
            # 裁剪极端值
            clipped = df[col].clip(upper=df[col].quantile(0.99))
            ax5.plot(df['datetime_local'], clipped, color=color, linewidth=0.5, alpha=0.8, label=svc)
    ax5.set_ylabel('P99 Latency (ms)', fontsize=11)
    ax5.set_title('All Services — gRPC P99 Latency (clipped at 99th percentile)', fontsize=12, fontweight='bold')
    ax5.legend(loc='upper left', ncol=6, fontsize=7)
    ax5.grid(True, alpha=0.3)

    # 给所有子图加故障区间着色
    all_axes = [ax1, ax2, ax3, ax4, ax5]
    for ax in all_axes:
        for exp in experiments:
            if exp['fault_type'] == 'normal':
                continue
            color = FAULT_COLORS.get(exp['fault_type'], '#ff0000')
            ax.axvspan(exp['start_dt'].tz_convert('Asia/Shanghai'),
                       exp['end_dt'].tz_convert('Asia/Shanghai'),
                       alpha=0.08, facecolor=color, edgecolor='none')
        ax.xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
        ax.xaxis.set_major_locator(mdates.HourLocator(interval=1))

    # 图例
    legend_patches = []
    seen = set()
    for exp in experiments:
        ft = exp['fault_type']
        if ft != 'normal' and ft not in seen:
            seen.add(ft)
            legend_patches.append(
                Patch(facecolor=FAULT_COLORS.get(ft, '#ff0000'), alpha=0.4, label=f'{ft}')
            )
    fig.legend(handles=legend_patches, loc='lower center', ncol=6, fontsize=9,
               bbox_to_anchor=(0.5, -0.01))

    fig.suptitle('System-Wide Monitoring Dashboard', fontsize=16, fontweight='bold')
    plt.tight_layout()

    if save:
        path = OUTPUT_DIR / 'system_overview.png'
        fig.savefig(path, dpi=150, bbox_inches='tight')
        print(f'  Saved: {path}')

    return fig


# ============================================================
# 绘图函数 4: 单故障影响分析
# ============================================================
def plot_per_experiment(df, experiments, save=True):
    """
    每个故障实验一个文件夹，每个服务一张图。
    每张图 = 该服务的全部指标，7个子图纵向排列。
    x轴: quiet_start_time ~ cooldown_end_time
    红框: start_time ~ end_time（故障注入期）
    """
    results = []

    fault_exps = [e for e in experiments if e['fault_type'] != 'normal']

    for exp_idx, exp in enumerate(fault_exps):
        start = exp['start_dt']
        end = exp['end_dt']
        quiet = exp['quiet_dt']
        cooldown = exp['cooldown_dt']
        svc_target = exp['service']

        # ---- 时间窗口 ----
        mask = (df['datetime'] >= quiet) & (df['datetime'] <= cooldown)
        wdf = df[mask].copy()
        if len(wdf) == 0:
            print(f'  [SKIP] {exp["fault_type"]} -> {svc_target}: empty window')
            continue

        # ---- 实验文件夹 ----
        exp_name = f'{exp["fault_type"]}_{svc_target}_{exp["experiment_id"][:8]}'
        exp_dir = OUTPUT_DIR / exp_name
        exp_dir.mkdir(parents=True, exist_ok=True)

        # ---- tz ----
        tz = 'Asia/Shanghai'
        fs = start.tz_convert(tz)
        fe = end.tz_convert(tz)
        qs = quiet.tz_convert(tz)
        ce = cooldown.tz_convert(tz)

        # ---- 服务级指标定义 ----
        svc_metrics = [
            ('cpu_usage',       'CPU Usage (cores)',        '#3498db'),
            ('mem_usage_mb',    'Memory Usage (MB)',        '#9b59b6'),
            ('mem_usage_pct',   'Memory Usage (%)',         '#8e44ad'),
            ('grpc_latency_p99','gRPC P99 Latency (ms)',    '#e74c3c'),
            ('grpc_error_rate', 'gRPC Error Rate',          '#e67e22'),
            ('grpc_rps',        'gRPC Requests/s',          '#2ecc71'),
            ('pod_restarts',    'Pod Restarts (cumulative)','#7f8c8d'),
        ]

        sys_metrics = [
            ('system&total_rps',    'Total RPS',           '#2c3e50'),
            ('system&node_cpu_pct', 'Node CPU (%)',        '#e74c3c'),
            ('system&node_mem_pct', 'Node Memory (%)',     '#8e44ad'),
        ]

        services = get_service_names(df)

        # ========================================================
        # 每个微服务一张图
        # ========================================================
        for svc in services:
            svc_dir = exp_dir / svc
            svc_dir.mkdir(parents=True, exist_ok=True)

            n_metrics = 7
            fig, axes = plt.subplots(n_metrics, 1, figsize=(16, 2.5 * n_metrics), sharex=True)

            for i, (metric, label, color) in enumerate(svc_metrics):
                ax = axes[i]
                col = f'{svc}&{metric}'
                if col not in wdf.columns:
                    ax.text(0.5, 0.5, f'{metric}: N/A', transform=ax.transAxes,
                            ha='center', va='center', fontsize=14, color='gray')
                    continue

                ax.plot(wdf['datetime_local'], wdf[col],
                        color=color, linewidth=0.8, alpha=0.95)

                # 故障注入期红框
                ax.axvspan(fs, fe, alpha=0.15, facecolor='#e74c3c',
                           edgecolor='#c0392b', linewidth=1.5, linestyle='-', zorder=10)

                # 静默/冷却浅色背景
                ax.axvspan(qs, fs, alpha=0.04, facecolor='#2ecc71', edgecolor='none')
                ax.axvspan(fe, ce, alpha=0.04, facecolor='#3498db', edgecolor='none')

                ax.set_ylabel(label, fontsize=10, color=color)
                ax.tick_params(axis='y', labelcolor=color, labelsize=8)
                ax.grid(True, alpha=0.2)
                ax.set_ylim(bottom=0)

            # x轴格式
            axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
            axes[-1].xaxis.set_major_locator(mdates.MinuteLocator(interval=2))
            axes[-1].set_xlabel('Time (Asia/Shanghai)', fontsize=11)
            plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=30, ha='right')

            # 标题
            is_target = ' [TARGET]' if svc == svc_target else ''
            fig.suptitle(
                f'{svc}{is_target}  |  '
                f'Fault: {exp["fault_type"]} → {svc_target}  |  '
                f'{quiet.strftime("%H:%M")} — {cooldown.strftime("%H:%M")} UTC',
                fontsize=13, fontweight='bold', y=1.01
            )
            plt.tight_layout()

            path = svc_dir / '1_all.png'
            fig.savefig(path, dpi=150, bbox_inches='tight')
            plt.close(fig)

        # ========================================================
        # 系统级指标一张图
        # ========================================================
        sys_dir = exp_dir / 'system'
        sys_dir.mkdir(parents=True, exist_ok=True)

        n_sys = 3
        fig, axes = plt.subplots(n_sys, 1, figsize=(16, 2.5 * n_sys), sharex=True)
        for i, (col_name, label, color) in enumerate(sys_metrics):
            ax = axes[i]
            if col_name in wdf.columns:
                ax.plot(wdf['datetime_local'], wdf[col_name],
                        color=color, linewidth=0.8, alpha=0.95)
            ax.axvspan(fs, fe, alpha=0.15, facecolor='#e74c3c',
                       edgecolor='#c0392b', linewidth=1.5, linestyle='-', zorder=10)
            ax.axvspan(qs, fs, alpha=0.04, facecolor='#2ecc71', edgecolor='none')
            ax.axvspan(fe, ce, alpha=0.04, facecolor='#3498db', edgecolor='none')
            ax.set_ylabel(label, fontsize=10, color=color)
            ax.tick_params(axis='y', labelcolor=color, labelsize=8)
            ax.grid(True, alpha=0.2)
        axes[-1].xaxis.set_major_formatter(mdates.DateFormatter('%H:%M:%S'))
        axes[-1].xaxis.set_major_locator(mdates.MinuteLocator(interval=2))
        axes[-1].set_xlabel('Time (Asia/Shanghai)', fontsize=11)
        plt.setp(axes[-1].xaxis.get_majorticklabels(), rotation=30, ha='right')

        fig.suptitle(
            f'System Metrics  |  '
            f'Fault: {exp["fault_type"]} → {svc_target}  |  '
            f'{quiet.strftime("%H:%M")} — {cooldown.strftime("%H:%M")} UTC',
            fontsize=13, fontweight='bold', y=1.01
        )
        plt.tight_layout()
        fig.savefig(sys_dir / '1_all.png', dpi=150, bbox_inches='tight')
        plt.close(fig)

        results.append(exp_name)
        print(f'  [{exp_idx+1}/{len(fault_exps)}] Saved: {exp_name}/')

    return results


# ============================================================
# 主函数
# ============================================================
def main():
    parser = argparse.ArgumentParser(description='Chaos Engineering Metrics Visualization')
    parser.add_argument('--service', type=str, default=None,
                        help='Target service for dashboard plot')
    parser.add_argument('--fault', type=str, default=None,
                        help='Fault type for impact analysis')
    parser.add_argument('--all', action='store_true', default=True,
                        help='Generate all plots (default)')
    args = parser.parse_args()

    print('Loading data...')
    df, experiments = load_data()
    print(f'  CSV: {len(df)} rows, time range: {df["datetime_local"].iloc[0]} ~ {df["datetime_local"].iloc[-1]}')
    print(f'  JSON: {len(experiments)} experiments')

    # ----------------------------------------------------------
    # 图1: 系统级总览
    # ----------------------------------------------------------
    print('\n[1/4] Generating system overview...')
    plot_fault_impact_overview(df, experiments)

    # ----------------------------------------------------------
    # 图2: 各服务热力图 (两个核心指标)
    # ----------------------------------------------------------
    print('\n[2/4] Generating service heatmaps...')
    for metric in ['grpc_error_rate', 'grpc_latency_p99']:
        plot_all_services_heatmap(df, experiments, metric=metric)

    # ----------------------------------------------------------
    # 图3: 单服务仪表盘
    # ----------------------------------------------------------
    print('\n[3/4] Generating service dashboards...')
    if args.service:
        services = [args.service]
    else:
        # 只画被注入了故障的服务
        targeted = set(exp['service'] for exp in experiments if exp['fault_type'] != 'normal')
        services = sorted(targeted)

    for svc in services:
        print(f'  Plotting {svc}...')
        plot_service_dashboard(df, experiments, svc)

    # ----------------------------------------------------------
    # 图4: 每个实验的故障影响分析
    # ----------------------------------------------------------
    print('\n[4/4] Generating per-experiment fault analysis...')
    # ---- 每个实验画图 ----
    plot_per_experiment(df, experiments, save=True)

    print(f'\nAll plots saved to: {OUTPUT_DIR}')
    print(f'Total files: {len(list(OUTPUT_DIR.glob("*.png")))}')
    plt.close('all')


if __name__ == '__main__':
    main()
