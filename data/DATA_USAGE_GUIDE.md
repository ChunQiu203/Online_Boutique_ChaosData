# 混沌工程数据集使用说明

> 本文档描述 `final_dataset_for_algorithm.csv` 和 `chaos_history.json` 两个数据文件的结构、字段含义、以及如何使用它们进行混沌工程分析和算法研究。

---

## 目录

1. [文件概览](#文件概览)
2. [final_dataset_for_algorithm.csv —— 指标时序数据集](#final_dataset_for_algorithmcsv--指标时序数据集)
   - [整体参数](#整体参数)
   - [列结构说明](#列结构说明)
   - [故障类型与分布](#故障类型与分布)
   - [典型使用场景](#典型使用场景)
3. [chaos_history.json —— 混沌实验记录](#chaos_historyjson--混沌实验记录)
   - [整体参数](#整体参数-1)
   - [字段说明](#字段说明)
   - [故障类别与故障类型映射](#故障类别与故障类型映射)
   - [典型使用场景](#典型使用场景-1)
4. [两个文件的联合使用](#两个文件的联合使用)
5. [注意事项](#注意事项)

---

## 文件概览

| 文件 | 类型 | 行数/记录数 | 时间范围 | 核心内容 |
|------|------|------------|----------|----------|
| `final_dataset_for_algorithm.csv` | 时序指标数据 | 3006 行 | 2026-06-08 20:20 ~ 2026-06-09 04:40 | 11个微服务的性能指标，每10秒一条 |
| `chaos_history.json` | 实验元数据 | 33 条 | 2026-06-08 20:26 ~ 2026-06-09 04:29 | 混沌实验的注入参数与生命周期时间 |

**数据关系**: CSV 是"果"（系统在各故障下的表现），JSON 是"因"（何时对哪个服务注入了何种故障）。两者通过 `fault_type` 和 `target_service` / `service` 字段关联。

---

## final_dataset_for_algorithm.csv —— 指标时序数据集

### 整体参数

| 参数 | 值 |
|------|-----|
| 总行数（数据） | 3006 |
| 总列数 | 83 |
| 采样间隔 | **10 秒** |
| 时间范围 | 2026-06-08 20:20:00 ~ 2026-06-09 04:40:50 |
| 监控服务数 | 11 个 |
| 正常样本 | 2010 条（66.9%） |
| 故障样本 | 996 条（33.1%） |

### 列结构说明

#### 第1列：`timestamp`
- **类型**: `datetime`（UTC时区，ISO 8601格式）
- **示例**: `2026-06-08 20:20:00+00:00`
- **说明**: 每条指标记录的采样时间戳，间隔为10秒

#### 第2~78列：服务指标（11个服务 × 7项指标 = 77列）

每个服务有7项指标，命名格式为 `<service>&<metric_name>`：

| 指标名 | 含义 | 单位 |
|--------|------|------|
| `cpu_usage` | CPU使用率 | 核数（cores） |
| `mem_usage_mb` | 内存使用量 | MB |
| `mem_usage_pct` | 内存使用百分比 | % |
| `grpc_latency_p99` | gRPC请求P99延迟 | 毫秒（ms） |
| `grpc_error_rate` | gRPC错误率 | 比例（0~1） |
| `grpc_rps` | gRPC每秒请求数 | req/s |
| `pod_restarts` | Pod重启次数 | 次（累计值） |

**11个被监控的微服务**:

| # | 服务名 | 说明 |
|---|--------|------|
| 1 | `frontend` | 前端网关服务 |
| 2 | `cartservice` | 购物车服务 |
| 3 | `productcatalogservice` | 产品目录服务 |
| 4 | `currencyservice` | 货币兑换服务 |
| 5 | `paymentservice` | 支付服务 |
| 6 | `shippingservice` | 配送服务 |
| 7 | `checkoutservice` | 结账服务 |
| 8 | `emailservice` | 邮件服务 |
| 9 | `recommendationservice` | 推荐服务 |
| 10 | `adservice` | 广告服务 |
| 11 | `reviewservice` | 评论服务 |

#### 第79~81列：系统级指标

| 列名 | 含义 | 单位 |
|------|------|------|
| `system&total_rps` | 系统总请求速率 | req/s |
| `system&node_cpu_pct` | 节点CPU使用率 | % |
| `system&node_mem_pct` | 节点内存使用率 | % |

#### 第82~83列：标签列

| 列名 | 含义 | 取值范围 |
|------|------|----------|
| `fault_type` | 当前注入的故障类型 | `normal` 或 11种故障类型之一 |
| `target_service` | 故障注入的目标服务 | `none` 或 10个服务之一 |

### 故障类型与分布

| 故障类型 | 样本数 | 占比 |
|----------|--------|------|
| `normal`（无故障） | 2010 | 66.9% |
| `memory-stress` | 92 | 3.1% |
| `network-delay` | 91 | 3.0% |
| `network-corrupt` | 91 | 3.0% |
| `pod-failure` | 91 | 3.0% |
| `container-kill` | 91 | 3.0% |
| `cpu-stress` | 90 | 3.0% |
| `network-loss` | 90 | 3.0% |
| `jvm-latency` | 90 | 3.0% |
| `jvm-cpu` | 90 | 3.0% |
| `pod-kill` | 90 | 3.0% |
| `dns-error` | 90 | 3.0% |

> **设计特点**: 除 `normal` 外，11种故障类型各约90条样本，覆盖8.3小时实验时长。样本分布相对均衡，适合训练分类/检测模型。

### 典型使用场景

#### 1. 故障检测（二分类/多分类）

```python
import pandas as pd
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier

# 加载数据
df = pd.read_csv('final_dataset_for_algorithm.csv')

# 特征列（所有服务指标 + 系统指标）
feature_cols = [c for c in df.columns if c not in ('timestamp', 'fault_type', 'target_service')]
X = df[feature_cols]
y = df['fault_type']

# 二分类：正常 vs 异常
y_binary = (y != 'normal').astype(int)

# 划分训练/测试集（注意时间顺序）
split_idx = int(len(df) * 0.8)
X_train, X_test = X.iloc[:split_idx], X.iloc[split_idx:]
y_train, y_test = y_binary.iloc[:split_idx], y_binary.iloc[split_idx:]

# 训练模型
clf = RandomForestClassifier(n_estimators=100)
clf.fit(X_train, y_train)
print(f'Accuracy: {clf.score(X_test, y_test):.4f}')
```

#### 2. 故障类型识别（多分类）

```python
# 直接使用 fault_type 做多分类（12类：normal + 11种故障）
from sklearn.metrics import classification_report

y_multi = df['fault_type']
clf_multi = RandomForestClassifier(n_estimators=100)
clf_multi.fit(X_train, y_multi.iloc[:split_idx])
y_pred = clf_multi.predict(X_test)

print(classification_report(y_multi.iloc[split_idx:], y_pred))
```

#### 3. 故障定位（识别受影响的微服务）

```python
# 使用 target_service 作为标签，训练定位模型
df_fault = df[df['fault_type'] != 'normal']  # 仅故障样本
X_fault = df_fault[feature_cols]
y_service = df_fault['target_service']

# 二阶段方法：
# 阶段1: 先检测是否有故障
# 阶段2: 若检测到故障，再定位到具体服务
```

#### 4. 根因分析（Root Cause Analysis）

```python
# 使用故障期间各服务的指标变化来推断根因服务
# 例如：对比故障前后各服务 grpc_error_rate / grpc_latency_p99 的变化幅度

def analyze_fault_impact(df, fault_start_time, fault_type):
    """分析某个故障对各服务的影响程度"""
    fault_window = df[
        (df['timestamp'] >= fault_start_time) &
        (df['fault_type'] == fault_type)
    ]
    normal_window = df[df['fault_type'] == 'normal']

    impacts = {}
    for svc in ['frontend', 'cartservice', 'productcatalogservice', ...]:
        err_col = f'{svc}&grpc_error_rate'
        lat_col = f'{svc}&grpc_latency_p99'

        err_increase = fault_window[err_col].mean() - normal_window[err_col].mean()
        lat_increase = fault_window[lat_col].mean() - normal_window[lat_col].mean()

        impacts[svc] = {
            'error_rate_delta': err_increase,
            'latency_delta': lat_increase
        }
    return impacts
```

#### 5. 时间序列异常检测

```python
# 每10秒一条，适合时序模型
# LSTM、Transformer、AutoEncoder 等深度学习方法

import numpy as np

def create_sequences(X, y, seq_length=30):
    """创建滑动窗口序列（30步 = 5分钟的历史窗口）"""
    X_seq, y_seq = [], []
    for i in range(len(X) - seq_length):
        X_seq.append(X[i:i+seq_length])
        y_seq.append(y[i+seq_length])  # 预测下一步的故障状态
    return np.array(X_seq), np.array(y_seq)
```

---

## chaos_history.json —— 混沌实验记录

### 整体参数

| 参数 | 值 |
|------|-----|
| 总实验数 | 33 条 |
| 故障类别数 | 5 种 |
| 故障类型数 | 11 种 |
| 目标服务数 | 10 个 |
| 实验状态 | 全部 `completed` |
| 单个实验时长 | 约 5 分钟 |
| 实验间隔 | 约 10 分钟（含 quiet 和 cooldown） |

### 字段说明

| 字段名 | 类型 | 说明 | 示例 |
|--------|------|------|------|
| `experiment_id` | UUID | 实验唯一标识符 | `"a3740185-9aa8-4db7-abe3-7fee89792316"` |
| `fault_category` | string | 故障大类 | `"network-attack"` |
| `fault_type` | string | 具体故障类型 | `"network-delay"` |
| `instance_type` | string | 注入粒度 | `"service"` |
| `service` | string | 目标服务名 | `"adservice"` |
| `instance` | string | 目标实例名 | `"adservice"` |
| `source` | string | Chaos Mesh 故障源CRD | `"chaos-mesh-networkchaos"` |
| `destination` | string | 故障目标 | `"adservice"` |
| `start_time` | ISO 8601 | 故障实际开始注入时间 | `"2026-06-08T20:26:09Z"` |
| `end_time` | ISO 8601 | 故障结束时间 | `"2026-06-08T20:31:10Z"` |
| `quiet_start_time` | ISO 8601 | 静默期开始（故障前5分钟） | `"2026-06-08T20:21:09Z"` |
| `cooldown_end_time` | ISO 8601 | 冷却期结束（故障后5分钟） | `"2026-06-08T20:36:10Z"` |
| `repetition` | int | 实验重复次数 | `2` |
| `yaml_file` | string | 使用的 Chaos Mesh YAML 文件 | `".tmp_chaos.yaml"` |
| `status` | string | 实验执行状态 | `"completed"` |

#### 时间线模型

```
  quiet_start        start_time         end_time       cooldown_end
      |<--- 静默期 --->|<--- 故障注入期 --->|<--- 冷却期 --->|
      |     (5 min)    |     (~5 min)       |    (5 min)     |
```

- **静默期**（quiet → start）: 故障注入前5分钟的基线采集窗口，此时系统正常运行
- **故障注入期**（start → end）: 实际故障作用的时间窗口，约5分钟
- **冷却期**（end → cooldown）: 故障结束后5分钟的恢复观察窗口

### 故障类别与故障类型映射

| 故障类别 (fault_category) | 包含的故障类型 (fault_type) | Chaos Mesh CRD |
|---------------------------|---------------------------|----------------|
| `network-attack` | `network-delay`, `network-corrupt`, `network-loss` | NetworkChaos |
| `pod-fault` | `pod-failure`, `pod-kill`, `container-kill` | PodChaos |
| `stress-test` | `cpu-stress`, `memory-stress` | StressChaos |
| `jvm-fault` | `jvm-latency`, `jvm-cpu` | JVMChaos |
| `dns-attack` | `dns-error` | DNSChaos |

### 典型使用场景

#### 1. 实验回溯与审计

```python
import json
from datetime import datetime

with open('chaos_history.json') as f:
    experiments = json.load(f)

# 查询某个时间段内的实验
target_time = datetime.fromisoformat("2026-06-08T20:30:00Z")
active_experiments = []
for exp in experiments:
    start = datetime.fromisoformat(exp['start_time'])
    end = datetime.fromisoformat(exp['end_time'])
    if start <= target_time <= end:
        active_experiments.append(exp)

print(f"在 {target_time} 活跃的实验: {len(active_experiments)} 个")
for exp in active_experiments:
    print(f"  - {exp['fault_type']} -> {exp['service']}")
```

#### 2. 生成实验报告

```python
from collections import Counter

# 统计每个服务的故障注入次数
service_counter = Counter(exp['service'] for exp in experiments)
print("各服务被注入故障次数:")
for svc, count in service_counter.most_common():
    print(f"  {svc}: {count} 次")

# 统计故障类型分布
type_counter = Counter(exp['fault_type'] for exp in experiments)
print("\n故障类型分布:")
for ft, count in type_counter.most_common():
    print(f"  {ft}: {count} 次")

# 实验总时长
total_duration = sum(
    (datetime.fromisoformat(e['end_time']) -
     datetime.fromisoformat(e['start_time'])).total_seconds()
    for e in experiments
)
print(f"\n总故障注入时长: {total_duration/60:.0f} 分钟")
```

#### 3. 复现混沌实验

```python
def generate_chaos_yaml(experiment):
    """根据实验记录生成 Chaos Mesh YAML 用于复现"""
    yaml_template = f"""
apiVersion: chaos-mesh.org/v1alpha1
kind: {experiment['source'].replace('chaos-mesh-', '').title()}
metadata:
  name: replay-{experiment['experiment_id'][:8]}
spec:
  action: {experiment['fault_type']}
  mode: one
  selector:
    namespaces:
      - default
    labelSelectors:
      app: {experiment['service']}
  duration: "5m"
"""
    return yaml_template
```

---

## 两个文件的联合使用

### 核心关联方式

CSV 的 `fault_type` 和 `target_service` 列与 JSON 的 `fault_type` 和 `service` 字段一一对应。联合使用可以实现：

#### 1. 精确标注：用 JSON 的实验时间窗口标注 CSV 数据

```python
import pandas as pd
import json

df = pd.read_csv('final_dataset_for_algorithm.csv')
df['timestamp'] = pd.to_datetime(df['timestamp'])

with open('chaos_history.json') as f:
    experiments = json.load(f)

# 用实验记录精确标记每一行的故障状态
def get_fault_label(ts):
    """根据时间戳判断属于哪个实验/故障类型"""
    for exp in experiments:
        start = pd.to_datetime(exp['start_time'])
        end = pd.to_datetime(exp['end_time'])
        if start <= ts <= end:
            return exp['fault_type'], exp['service']
    return 'normal', 'none'

# 验证CSV标签与JSON的一致性
mismatches = 0
for i, row in df.iterrows():
    expected_type, expected_svc = get_fault_label(row['timestamp'])
    if row['fault_type'] != expected_type:
        mismatches += 1
print(f'标签不一致行数: {mismatches}/{len(df)}')
```

#### 2. 提取故障前/中/后三阶段数据

```python
def extract_fault_phases(df, experiment, before_sec=120, after_sec=120):
    """
    提取单个实验的三个阶段数据
    - before:  故障前 N 秒（基线）
    - during:  故障期间
    - after:   故障后 N 秒（恢复）
    """
    start = pd.to_datetime(experiment['start_time'])
    end = pd.to_datetime(experiment['end_time'])
    quiet_start = pd.to_datetime(experiment['quiet_start_time'])
    cooldown_end = pd.to_datetime(experiment['cooldown_end_time'])

    baseline = df[(df['timestamp'] >= quiet_start) & (df['timestamp'] < start)]
    during   = df[(df['timestamp'] >= start) & (df['timestamp'] <= end)]
    recovery = df[(df['timestamp'] > end) & (df['timestamp'] <= cooldown_end)]

    return baseline, during, recovery

# 示例：分析 adservice network-delay 故障
ad_delay_exp = [e for e in experiments
                if e['service'] == 'adservice'
                and e['fault_type'] == 'network-delay'][0]

baseline, during, recovery = extract_fault_phases(df, ad_delay_exp)

print(f"基线样本: {len(baseline)}, 故障样本: {len(during)}, 恢复样本: {len(recovery)}")
print(f"故障期 adservice P99延迟: {during['adservice&grpc_latency_p99'].mean():.2f}ms")
print(f"基线期 adservice P99延迟: {baseline['adservice&grpc_latency_p99'].mean():.2f}ms")
```

#### 3. 构建训练数据集（带精确时间窗口）

```python
def build_labeled_dataset(df, experiments):
    """
    使用 JSON 精确标注，构建三分类训练集：
    0 = 正常, 1 = 故障期, 2 = 恢复期
    """
    df = df.copy()
    df['fault_phase'] = 'normal'  # 0

    for exp in experiments:
        start = pd.to_datetime(exp['start_time'])
        end = pd.to_datetime(exp['end_time'])
        cooldown = pd.to_datetime(exp['cooldown_end_time'])

        # 故障期
        mask_during = (df['timestamp'] >= start) & (df['timestamp'] <= end)
        df.loc[mask_during, 'fault_phase'] = 'fault'  # 1

        # 恢复期
        mask_recovery = (df['timestamp'] > end) & (df['timestamp'] <= cooldown)
        df.loc[mask_recovery, 'fault_phase'] = 'recovery'  # 2

        # 同时记录具体故障信息
        df.loc[mask_during | mask_recovery, 'fault_type_detail'] = exp['fault_type']
        df.loc[mask_during | mask_recovery, 'fault_service'] = exp['service']
        df.loc[mask_during | mask_recovery, 'fault_category'] = exp['fault_category']

    return df

labeled_df = build_labeled_dataset(df, experiments)
print(labeled_df['fault_phase'].value_counts())
```

#### 4. 实验级特征提取（用于算法评估）

```python
def extract_experiment_features(df, experiment):
    """为一个实验提取统计特征（对比基线与故障期）"""
    baseline, during, recovery = extract_fault_phases(df, experiment)

    features = {
        'fault_type': experiment['fault_type'],
        'target_service': experiment['service'],
        'fault_category': experiment['fault_category'],
        'repetition': experiment['repetition'],
    }

    # 对每个服务计算故障期 vs 基线期的指标变化
    services = ['frontend', 'cartservice', 'productcatalogservice',
                'currencyservice', 'paymentservice', 'shippingservice',
                'checkoutservice', 'emailservice', 'recommendationservice',
                'adservice', 'reviewservice']

    for svc in services:
        for metric in ['cpu_usage', 'grpc_latency_p99', 'grpc_error_rate']:
            col = f'{svc}&{metric}'
            baseline_mean = baseline[col].mean()
            during_mean = during[col].mean()
            # 变化率（避免除零）
            if baseline_mean > 0:
                features[f'{svc}_{metric}_change'] = (during_mean - baseline_mean) / baseline_mean
            else:
                features[f'{svc}_{metric}_change'] = during_mean - baseline_mean

    return features

# 为所有实验提取特征，构建实验级数据集
all_exp_features = [extract_experiment_features(df, exp) for exp in experiments]
exp_df = pd.DataFrame(all_exp_features)
print(f"实验级特征矩阵: {exp_df.shape}")
```

---

## 注意事项

1. **时间对齐**: CSV 时间戳使用 `+00:00` 时区格式（带空格分隔），JSON 使用 ISO 8601 格式（带 `T` 分隔，`Z` 结尾）。联合分析时需统一格式。

2. **采样间隔**: CSV 数据每10秒采集一次。每个约5分钟的故障实验对应约30条故障采样点。

3. **标签一致性**: CSV 自带的 `fault_type` 和 `target_service` 标签与 JSON 实验记录已对齐。但如需更精确的窗口（如静默期/恢复期标记），建议使用 JSON 的 `start_time`/`end_time`/`cooldown_end_time` 重新标注。

4. **数据量**: 总计约8.3小时的连续监控数据，覆盖33个混沌实验。适合中小规模的算法验证与概念验证（PoC）。

5. **服务依赖**: Online Boutique 是一个微服务电商系统，服务间存在调用链（如 `frontend → checkoutservice → cartservice/paymentservice/shippingservice`）。注入故障到下游服务时，上游服务的指标也可能出现异常，这为根因分析增加了复杂性。

6. **pod_restarts 指标**: 该指标为累计值（从Pod启动算起），不是每秒/每窗口的增量。使用时建议做差分处理。

7. **所有实验 status 为 completed**: 表示所有实验均已正常执行完毕，数据完整。

8. **重复实验**: 相同 `fault_type + service` 组合可能有多次重复（`repetition` 字段），可用于验证故障模式的稳定性和一致性。

---

## 快速开始 Checklist

- [ ] 用 `pandas.read_csv()` 加载 CSV，用 `json.load()` 加载 JSON
- [ ] 将 CSV 的 `timestamp` 转为 `datetime` 类型
- [ ] 检查两类文件的 `fault_type` 标签一致性（见[联合使用示例1](#1-精确标注用-json-的实验时间窗口标注-csv-数据)）
- [ ] 根据任务选择标签：二分类用 `normal` vs 其他，多分类直接用 `fault_type`
- [ ] 注意时间序列的顺序性，避免随机打乱（用时间顺序切分训练/测试集）
- [ ] 特征工程时可考虑：滑动窗口统计量、服务间指标相关性、一阶/二阶差分
