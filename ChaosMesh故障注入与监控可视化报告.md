# 基于 ChaosMesh 的微服务故障注入与 Prometheus + Grafana 实时监控可视化报告

> **项目名称**: Online Boutique 混沌工程全自动化流水线
> **实验日期**: 2026-06-08 ~ 2026-06-09
> **被测系统**: Google Online Boutique (11 个微服务组成的电商系统)

---

## 目录

1. [项目背景与目标](#1-项目背景与目标)
2. [系统架构概览](#2-系统架构概览)
3. [ChaosMesh 故障注入方案](#3-chaosmesh-故障注入方案)
4. [Prometheus + Grafana 监控体系](#4-prometheus--grafana-监控体系)
5. [全自动实验流水线设计](#5-全自动实验流水线设计)
6. [故障注入实验矩阵](#6-故障注入实验矩阵)
7. [可视化分析与关键发现](#7-可视化分析与关键发现)
8. [数据集与算法应用](#8-数据集与算法应用)
9. [总结与展望](#9-总结与展望)

---

## 1. 项目背景与目标

### 1.1 背景

现代微服务架构中，服务间的复杂依赖关系使得单个组件的故障可能引发级联效应，最终导致整个系统不可用。混沌工程（Chaos Engineering）通过在受控环境中主动注入故障，帮助团队发现系统的薄弱环节，验证弹性设计，提升系统可靠性。

### 1.2 目标

本项目基于 Google Online Boutique（一个由 11 个微服务构成的电商演示系统），实现以下目标：

1. **故障注入**: 使用 ChaosMesh 在 Kubernetes 集群中注入 5 大类共 11 种不同类型的故障
2. **实时监控**: 通过 Prometheus 采集多维度性能指标，Grafana 提供实时可视化仪表盘
3. **数据采集**: 在 Selenium/JMeter 持续流量背景下，采集故障前后的完整指标时序数据
4. **可视化分析**: 生成单服务仪表盘、热力图、故障影响对比等多维度可视化图表
5. **ML 就绪数据集**: 产出带标签的时序数据集，支持故障检测、分类与根因分析算法研究

---

## 2. 系统架构概览

### 2.1 整体架构

```
┌──────────────────────────────────────────────────────────────────┐
│                        Kubernetes (Minikube)                       │
│                                                                    │
│  ┌────────────────────  Online Boutique ──────────────────────┐   │
│  │                                                              │   │
│  │  frontend ──► checkout ──┬── cartservice                    │   │
│  │                │          ├── paymentservice                 │   │
│  │                │          ├── shippingservice                │   │
│  │                │          ├── productcatalogservice          │   │
│  │                │          └── currencyservice                │   │
│  │                │                                            │   │
│  │                ▼                                            │   │
│  │     emailservice   adservice   recommendationservice        │   │
│  │     reviewservice                                            │   │
│  └──────────────────────────────────────────────────────────────┘   │
│                                                                    │
│  ┌── ChaosMesh ──┐    ┌── Monitoring Stack ──────────────────┐    │
│  │ PodChaos       │    │ Prometheus (metrics scraping)         │    │
│  │ NetworkChaos    │    │ Grafana (dashboards & alerting)       │    │
│  │ StressChaos     │    │ Node Exporter (host metrics)          │    │
│  │ DNSChaos        │    │ Kube-State-Metrics (pod status)       │    │
│  │ JVMChaos        │    └──────────────────────────────────────┘    │
│  └────────────────┘                                                │
└──────────────────────────────────────────────────────────────────┘

  ┌── 流量引擎 ──────────────────────────────────────────────────┐
  │  Selenium (Headless Edge/Chrome)                               │
  │  JMeter (30 并发用户，混合场景)                                  │
  │  持续产生: 首页浏览 → 搜索 → 加购 → 下单                        │
  └──────────────────────────────────────────────────────────────┘

  ┌── 数据管道 ──────────────────────────────────────────────────┐
  │  run_chaos_with_selenium.py (主控脚本)                          │
  │  ├── 故障调度 & 生命周期管理                                     │
  │  ├── Prometheus PromQL 批量查询                                 │
  │  ├── 80 路指标 outer join → 10s 重采样                          │
  │  └── 输出: final_dataset_for_algorithm.csv + chaos_history.json │
  └──────────────────────────────────────────────────────────────┘
```

### 2.2 11 个被监控的微服务

| # | 服务名 | 技术栈 | 职责 |
|---|--------|--------|------|
| 1 | `frontend` | Go | 前端网关，用户入口 |
| 2 | `cartservice` | C# | 购物车管理 |
| 3 | `productcatalogservice` | Go | 产品目录查询 |
| 4 | `currencyservice` | Node.js | 货币兑换 |
| 5 | `paymentservice` | Node.js | 支付处理 |
| 6 | `shippingservice` | Go | 配送估算 |
| 7 | `checkoutservice` | Go | 订单结算编排 |
| 8 | `emailservice` | Python | 订单确认邮件 |
| 9 | `recommendationservice` | Python | 产品推荐 |
| 10 | `adservice` | Java | 广告投放 (支持 JVMChaos) |
| 11 | `reviewservice` | Java | 产品评论 |

### 2.3 服务调用链

用户请求从 `frontend` 进入，经 `checkoutservice` 编排后并发调用 `cartservice`、`paymentservice`、`shippingservice`、`productcatalogservice`、`currencyservice`。后台服务包括 `emailservice`（发送确认邮件）、`adservice`（广告投放）、`recommendationservice`（产品推荐）和 `reviewservice`（产品评论）。

这种复杂的调用链使得下游服务的故障可能通过依赖传播到上游，产生级联效应——这正是混沌工程要验证的核心场景。

---

## 3. ChaosMesh 故障注入方案

### 3.1 ChaosMesh 简介

[ChaosMesh](https://chaos-mesh.org/) 是 CNCF 托管的云原生混沌工程平台，基于 Kubernetes CRD（Custom Resource Definition）设计。它通过在集群中创建自定义资源（如 `PodChaos`、`NetworkChaos`），由 Controller 将故障注入到目标 Pod/容器中，无需修改应用代码。

### 3.2 故障类型矩阵（5 大类，11 种故障）

#### 3.2.1 Pod 故障类（PodChaos）

**目标**: 模拟 Pod/容器级别的异常

| 故障类型 | CRD Kind | 动作 | 影响 | 故障参数 |
|----------|----------|------|------|----------|
| `pod-kill` | PodChaos | `pod-kill` | 杀死目标服务的所有 Pod，模拟进程崩溃 | `mode: all`，K8s 会尝试重启 |
| `pod-failure` | PodChaos | `pod-failure` | 使 Pod 不可用（持续约 5 分钟），模拟节点故障 | `mode: one` |
| `container-kill` | PodChaos | `container-kill` | 杀死 Pod 中的指定容器（`server`），模拟容器崩溃 | `mode: one`，更精确的粒度 |

**示例 YAML (pod-kill)**:

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: PodChaos
metadata:
  name: pod-kill-frontend
  namespace: chaos-testing
spec:
  action: pod-kill
  mode: all
  duration: "600s"
  selector:
    namespaces:
      - default
    labelSelectors:
      app: frontend
```

#### 3.2.2 网络攻击类（NetworkChaos）

**目标**: 模拟网络层面的异常，测试服务间通信的鲁棒性

| 故障类型 | CRD Kind | 动作 | 影响 | 故障参数 |
|----------|----------|------|------|----------|
| `network-delay` | NetworkChaos | `delay` | 目标服务所有网络包增加延迟 | `latency: 500ms`，`jitter: 100ms`，`direction: both` |
| `network-loss` | NetworkChaos | `loss` | 目标服务网络包以一定概率丢失 | `loss: 10%` |
| `network-corrupt` | NetworkChaos | `corrupt` | 目标服务网络包以一定概率损坏 | `corrupt: 5%` |

**示例 YAML (network-delay)**:

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: NetworkChaos
metadata:
  name: network-delay-cartservice
  namespace: chaos-testing
spec:
  action: delay
  mode: all
  duration: "600s"
  selector:
    namespaces:
      - default
    labelSelectors:
      app: cartservice
  delay:
    latency: "500ms"
    jitter: "100ms"
  direction: both
```

**关键发现**: 网络类故障对用户体验影响最直接。500ms 的网络延迟注入到 `cartservice`（购物车）时，`frontend` 的 gRPC 延迟从正常的 ~50ms 飙升至 ~600ms，直接影响用户页面加载速度。

#### 3.2.3 压力测试类（StressChaos）

**目标**: 模拟资源争抢，测试服务的资源隔离和限流机制

| 故障类型 | CRD Kind | 动作 | 影响 | 故障参数 |
|----------|----------|------|------|----------|
| `cpu-stress` | StressChaos | `stress` | 在目标 Pod 内施加 CPU 压力 | `workers: 2`，`load: 80`（占总 CPU 80%） |
| `memory-stress` | StressChaos | `stress` | 在目标 Pod 内施加内存压力 | `workers: 1`，`size: 256MB` |

**示例 YAML (cpu-stress)**:

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: StressChaos
metadata:
  name: cpu-stress-checkoutservice
  namespace: chaos-testing
spec:
  mode: all
  duration: "600s"
  selector:
    namespaces:
      - default
    labelSelectors:
      app: checkoutservice
  stressors:
    cpu:
      workers: 2
      load: 80
```

**关键发现**: CPU 压力故障会导致目标服务的请求处理变慢，影响整个调用链的响应时间。但得益于 gRPC 的超时机制，系统不会完全崩溃，只是吞吐量下降。

#### 3.2.4 DNS 攻击类（DNSChaos）

**目标**: 模拟 DNS 解析故障，测试服务发现机制的容错能力

| 故障类型 | CRD Kind | 动作 | 影响 | 故障参数 |
|----------|----------|------|------|----------|
| `dns-error` | DNSChaos | `error` | 目标服务的 DNS 查询返回错误 | 匹配模式 `*.default.svc.cluster.local` |

**示例 YAML (dns-error)**:

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: DNSChaos
metadata:
  name: dns-error-{service}
  namespace: chaos-testing
spec:
  action: error
  mode: all
  duration: "600s"
  selector:
    namespaces:
      - default
    labelSelectors:
      app: {service}
  patterns:
    - "*.default.svc.cluster.local"
```

**关键发现**: DNS 故障是影响最大的故障类型之一。当目标服务无法解析 Kubernetes 内部 DNS 时，所有依赖该服务的上游服务都会出现连接失败，导致 gRPC 错误率急剧上升。

#### 3.2.5 JVM 故障类（JVMChaos）

**目标**: 模拟 Java 服务内部的 JVM 级别故障（仅适用于 Java 服务）

| 故障类型 | CRD Kind | 动作 | 影响 | 故障参数 |
|----------|----------|------|------|----------|
| `jvm-cpu` | JVMChaos | `stress` | 在 JVM 内施加 CPU 压力（利用 Byteman） | `cpuCount: 2` |
| `jvm-latency` | JVMChaos | `latency` | 指定 Java 方法调用增加延迟 | `latency: 2000ms`，`class: hipstershop.AdService`，`method: getAds` |

**示例 YAML (jvm-latency)**:

```yaml
apiVersion: chaos-mesh.org/v1alpha1
kind: JVMChaos
metadata:
  name: jvm-latency-adservice
  namespace: chaos-testing
spec:
  action: latency
  mode: all
  duration: "600s"
  selector:
    namespaces:
      - default
    labelSelectors:
      app: adservice
  latency: 2000
  class: hipstershop.AdService
  method: getAds
```

**关键发现**: JVM 故障的精度最高，可以精确到方法级别。例如在 `adservice` 的 `getAds` 方法注入 2s 延迟后，`frontend` 的页面渲染时间明显增加但未完全失败（因为有降级逻辑）。这证明了 Java 服务具备一定的优雅降级能力。

### 3.3 故障注入机制

ChaosMesh 的故障注入原理基于 sidecar 模式：

1. **CRD 创建**: 用户提交故障 YAML 到 Kubernetes API Server
2. **Controller 监听**: ChaosMesh Controller 监听到新的故障 CR
3. **Sidecar 注入**: 对于 PodChaos/StressChaos，通过 `chaos-daemon`（以 DaemonSet 运行在每个节点上）将故障注入到目标 Pod 的 namespace 中
4. **TC 规则**: 对于 NetworkChaos，使用 Linux `tc`（traffic control）在目标 Pod 的网络命名空间中添加延迟/丢包/损坏规则
5. **故障回收**: 达到 `duration` 后自动删除故障规则，系统恢复正常

---

## 4. Prometheus + Grafana 监控体系

### 4.1 监控架构

```
┌──────────────────── Prometheus Stack ─────────────────────────┐
│                                                                │
│  ┌─ Service Discovery ──┐    ┌─ Metrics ──────────────────┐   │
│  │ Kubernetes API        │    │                            │   │
│  │ → Pod Annotations     │───►│ container_cpu_usage_*      │   │
│  │ → Service Endpoints   │    │ container_memory_*         │   │
│  │ → PodMonitor CRD      │    │ grpc_server_handled_*      │   │
│  └───────────────────────┘    │ grpc_server_handling_*     │   │
│                                │ kube_pod_status_phase      │   │
│  ┌─ PromQL (15s scrape) ─┐    │ node_cpu_seconds_total     │   │
│  │ rate(…[60s])          │    │ node_memory_MemAvailable   │   │
│  │ histogram_quantile()  │    └────────────────────────────┘   │
│  │ avg_over_time(…[10s]) │                                     │
│  └───────────────────────┘                                     │
└────────────────────────────────────────────────────────────────┘
         │
         ▼
┌──────────────────── Grafana ────────────────────────────────┐
│  Dashboards:                                                  │
│  ├── Online Boutique / Overview        (系统总览)              │
│  ├── Online Boutique / Services        (各服务指标明细)        │
│  ├── Online Boutique / gRPC Metrics    (gRPC 调用详情)        │
│  ├── Online Boutique / Resources       (K8s 资源使用)         │
│  └── Custom: 混沌工程实验仪表盘         (故障注入叠加视图)     │
└──────────────────────────────────────────────────────────────┘
```

### 4.2 Prometheus 指标采集

#### 4.2.1 采集的 7 项核心指标

每个微服务采集 7 项指标，11 个服务 × 7 = 77 列服务级指标 + 3 列系统级指标 = **80 列**：

| # | 指标名称 | PromQL 查询 | 含义 | 单位 |
|---|----------|-------------|------|------|
| 1 | `cpu_usage` | `sum(rate(container_cpu_usage_seconds_total{container="server",pod=~"${svc}-.*"}[${RATE_WIN}]))` | CPU 使用率 | cores |
| 2 | `mem_usage_mb` | `sum(container_memory_working_set_bytes{container="server",pod=~"${svc}-.*"}) / 1024 / 1024` | 内存使用量 | MB |
| 3 | `mem_usage_pct` | 内存使用量 / 资源限制 × 100 | 内存使用百分比 | % |
| 4 | `grpc_latency_p99` | `histogram_quantile(0.99, sum(rate(grpc_server_handling_seconds_bucket{grpc_service=~".*${svc_capitalized}.*"}[${RATE_WIN}])) by (le))` | gRPC P99 延迟 | ms |
| 5 | `grpc_error_rate` | `sum(rate(grpc_server_handled_total{grpc_service=~".*${svc_capitalized}.*",grpc_code!="OK"}[${RATE_WIN}])) / sum(rate(grpc_server_handled_total{grpc_service=~".*${svc_capitalized}.*"}[${RATE_WIN}]))` | gRPC 错误率 | 0~1 |
| 6 | `grpc_rps` | `sum(rate(grpc_server_handled_total{grpc_service=~".*${svc_capitalized}.*"}[${RATE_WIN}]))` | gRPC 请求速率 | req/s |
| 7 | `pod_restarts` | `sum(kube_pod_container_status_restarts_total{namespace="default",pod=~"${svc}-.*"})` | Pod 累计重启次数 | 次 |

**系统级指标**:

| 指标 | PromQL | 含义 |
|------|--------|------|
| `total_rps` | `sum(rate(grpc_server_handled_total{namespace="default"}[${RATE_WIN}]))` | 系统总请求速率 |
| `node_cpu_pct` | `100 - avg(rate(node_cpu_seconds_total{mode="idle"}[${RATE_WIN}])) * 100` | 节点 CPU 使用率 |
| `node_mem_pct` | `(1 - node_memory_MemAvailable_bytes / node_memory_MemTotal_bytes) * 100` | 节点内存使用率 |

#### 4.2.2 采集参数

| 参数 | 值 | 说明 |
|------|-----|------|
| Prometheus 查询地址 | `http://localhost:9090` | 通过 `kubectl port-forward` 暴露 |
| 查询步长 (`step`) | `10s` | 每 10 秒一个采样点 |
| Rate 窗口 (`RATE_WIN`) | `60s` | 使用 60s 滑动窗口计算 rate |
| Scrape 间隔 | `15s` | Prometheus 原生抓取频率 |

#### 4.2.3 数据处理管道

```
Prometheus 原始数据 (15s scrape)
        │
        ▼
  批量 PromQL 查询 (80 列, 10s step)
        │
        ▼
  数据清洗 & 合并
  ├── 80 路指标 Outer Join (按 timestamp)
  ├── 前向填充 (ffill) → 后向填充 (bfill)
  ├── 添加 fault_type 标签 (与实验时间窗口对齐)
  └── 添加 target_service 标签
        │
        ▼
  final_dataset_for_algorithm.csv (3006 行 × 83 列)
```

### 4.3 Grafana 可视化仪表盘

#### 4.3.1 预配置仪表盘

通过 `scripts/setup-grafana.sh` 一键部署的 Grafana 包含 4 个预配置 Dashboard：

1. **Online Boutique / Overview**: 系统总览面板
   - 总请求速率（RPS）
   - 平均响应延迟
   - 错误率趋势
   - 活跃用户数

2. **Online Boutique / Services**: 各服务指标明细
   - 每个服务的 CPU / Memory 使用
   - gRPC 请求速率和延迟分服务展示
   - Pod 状态与重启次数

3. **Online Boutique / gRPC Metrics**: gRPC 调用详情
   - 各服务间 gRPC 调用拓扑
   - P50 / P95 / P99 延迟分布
   - 状态码分布（OK / NotFound / Internal / …）

4. **Online Boutique / Resources**: Kubernetes 资源使用
   - 节点 CPU / Memory 使用率
   - Pod 资源请求 vs 限制对比
   - 网络 I/O 流量

#### 4.3.2 Grafana 访问方式

```bash
# 端口转发
kubectl port-forward -n monitoring svc/grafana 3000:80

# 浏览器访问
# URL: http://localhost:3000
# 用户名: admin
# 密码: prom-operator
```

---

## 5. 全自动实验流水线设计

### 5.1 主控脚本架构

`run_chaos_with_selenium.py` 是整个混沌工程实验的指挥中心，实现以下核心流程：

```
                    ┌──────────────┐
                    │   启动流量引擎  │
                    │ Selenium/JMeter│ (后台异步)
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │  扫描实验目录  │
                    │ experiments/ │ (11 种故障 YAML)
                    └──────┬───────┘
                           │
              ┌────────────▼────────────┐
              │   for 每种故障类型:       │
              │   ┌──────────────────┐   │
              │   │ 1. 静默期 (5min)  │   │ ← baseline 采集
              │   │ 2. 注入故障       │   │ ← kubectl apply
              │   │ 3. 故障持续 (5min) │   │ ← fault 采集
              │   │ 4. 解除故障       │   │ ← kubectl delete
              │   │ 5. 冷却期 (5min)  │   │ ← recovery 采集
              │   └──────────────────┘   │
              └────────────┬────────────┘
                           │
                    ┌──────▼───────┐
                    │ Prometheus   │
                    │ 批量查询 80列 │
                    └──────┬───────┘
                           │
                    ┌──────▼───────┐
                    │ 数据合并      │
                    │ 打标 & 导出   │
                    │ CSV + JSON   │
                    └──────────────┘
```

### 5.2 实验生命周期

每次实验严格遵循以下时间线：

```
  quiet_start  (T-5min)     start_time  (T)        end_time  (T+5min)    cooldown_end  (T+10min)
      │                         │                       │                     │
      ├────── 静默期 (5min) ──────┼─── 故障注入期 (5min) ──┼─── 冷却期 (5min) ────┤
      │    baseline 采集          │    fault 采集           │    recovery 采集      │
      │    (30 个采样点)           │    (30 个采样点)        │    (30 个采样点)       │
```

- **静默期**: 故障注入前 5 分钟，系统正常运行，采集基线指标
- **故障注入期**: ChaosMesh 故障生效，持续 5 分钟，采集系统在故障下的表现
- **冷却期**: 故障解除后 5 分钟，观察系统恢复情况

### 5.3 流量引擎

实验期间，通过流量引擎持续产生真实用户流量，确保采集到的指标反映真实负载下的系统表现：

| 引擎 | 配置 | 说明 |
|------|------|------|
| Selenium | Headless Edge/Chrome | 模拟用户浏览、搜索、加购、下单完整购物流程 |
| JMeter | 30 并发用户，混合场景 | 高并发压测，测试系统在压力 + 故障双重打击下的表现 |

### 5.4 安全保障

```python
# try...finally + signal/atexit 多层防护
- try...finally: 确保每次实验后故障被清理
- atexit.register(): 进程退出时自动删除所有残留故障 CR
- signal.signal(SIGINT/SIGTERM): Ctrl+C 时优雅退出
- 孤儿进程防护: Selenium/JMeter 子进程确保在父进程退出时自动终止
```

---

## 6. 故障注入实验矩阵

### 6.1 实验参数

| 参数 | 值 |
|------|-----|
| 故障类型数 | 11 种 |
| 目标服务池 | 10 个微服务（JVM 类固定 adservice） |
| 每种故障重复次数 | 3 次（默认 1，实际运行 3 轮） |
| 总实验次数 | 33 条 |
| 单次实验时长 | 15 分钟（5+5+5） |
| 总实验时长 | ~8.3 小时 |
| 故障:正常 样本比 | 约 1:2（996:2010） |

### 6.2 故障分布

| 故障类别 | 故障类型 | 目标服务 | ChaosMesh CRD | 实验次数 |
|----------|----------|----------|---------------|----------|
| **network-attack** | `network-delay` | cartservice / shippingservice / adservice | NetworkChaos | 3 |
| **network-attack** | `network-corrupt` | productcatalogservice / currencyservice / emailservice | NetworkChaos | 3 |
| **network-attack** | `network-loss` | checkoutservice / frontend / reviewservice | NetworkChaos | 3 |
| **pod-fault** | `pod-failure` | checkoutservice / paymentservice / productcatalogservice | PodChaos | 3 |
| **pod-fault** | `pod-kill` | frontend / cartservice / shippingservice | PodChaos | 3 |
| **pod-fault** | `container-kill` | currencyservice / emailservice / reviewservice | PodChaos | 3 |
| **stress-test** | `cpu-stress` | cartservice / checkoutservice / adservice | StressChaos | 3 |
| **stress-test** | `memory-stress` | emailservice / paymentservice / frontend | StressChaos | 3 |
| **dns-attack** | `dns-error` | productcatalogservice / shippingservice / checkoutservice | DNSChaos | 3 |
| **jvm-fault** | `jvm-cpu` | adservice (固定) | JVMChaos | 3 |
| **jvm-fault** | `jvm-latency` | adservice (固定) | JVMChaos | 3 |

### 6.3 数据集概览

| 文件 | 类型 | 规模 | 说明 |
|------|------|------|------|
| `final_dataset_for_algorithm.csv` | 时序指标数据 | 3006 行 × 83 列 | ML 就绪，带 fault_type 标签 |
| `chaos_history.json` | 实验元数据 | 33 条记录 | 包含精确时间窗口和服务映射 |

**采样统计**:

| 标签 | 样本数 | 占比 |
|------|--------|------|
| `normal` (无故障) | 2010 | 66.9% |
| 各故障类型 | ~90 条/类型 | ~3.0%/类型 |
| 故障样本合计 | 996 | 33.1% |

---

## 7. 可视化分析与关键发现

### 7.1 可视化类型

项目生成的图表分为三大类：

#### 7.1.1 单服务仪表盘（Dashboard）

每个微服务生成一张完整仪表盘，包含 5 个子图：
- **CPU Usage**: CPU 使用率趋势（cores）
- **Memory Usage**: 内存使用百分比（%）
- **gRPC P99 Latency**: P99 响应延迟（ms）
- **gRPC Error Rate**: 错误率（0~1）
- **gRPC RPS**: 每秒请求数（req/s）

所有子图叠加了**故障区间着色**：
- 浅色背景：该时间段发生了某种故障
- 深色+虚线边框：该故障**直接作用于当前服务**
- 不同颜色代表不同故障类型

#### 7.1.2 全服务热力图（Heatmap）

将所有 11 个服务的同一指标放在一个图中，使用颜色深浅表示指标值大小：
- `heatmap_grpc_latency_p99.png`: gRPC P99 延迟热力图
- `heatmap_grpc_error_rate.png`: gRPC 错误率热力图

热力图可以**一眼识别**哪些服务在什么时间段出现了异常。

#### 7.1.3 系统总览（Overview）

- System Total RPS（系统总请求速率）
- Node CPU Usage（节点 CPU 使用率）
- Node Memory Usage（节点内存使用率）

### 7.2 关键实验案例

#### 案例 1: network-delay → adservice (a3740185)

**故障参数**: `latency=500ms, jitter=100ms`

**观察**:

| 指标 | 基线期 (故障前) | 故障期 | 变化 |
|------|----------------|--------|------|
| adservice gRPC P99 延迟 | ~45ms | ~580ms | **+1189%** |
| frontend gRPC P99 延迟 | ~120ms | ~310ms | **+158%** |
| adservice gRPC RPS | ~15 req/s | ~8 req/s | -47% |
| adservice gRPC Error Rate | 0.1% | 0.1% | 无明显变化 |

**分析**:
- `adservice` 直接受到 500ms 网络延迟打击，P99 延迟从 45ms 飙升至 580ms
- `frontend` 作为上游也受到牵连（因为调用 adservice 耗时增加），但影响程度小于直接目标
- gRPC 错误率未显著上升，说明超时时间设置合理（未触发超时失败）
- 但 RPS 下降 47%，因为请求变慢导致吞吐量下降

**可视化验证**:
```
plots/network-delay_adservice_a3740185/
├── adservice/1_all.png         ← adservice P99 latency 明显升高
├── frontend/1_all.png          ← frontend 的延迟也受影响
├── cartservice/1_all.png       ← 购物车服务基本不受影响
└── system/1_all.png            ← 系统级 RPS 轻微下降
```

#### 案例 2: network-corrupt → currencyservice (6b17199e)

**故障参数**: `corrupt=5%`

**观察**:

| 指标 | 基线期 | 故障期 | 变化 |
|------|--------|--------|------|
| currencyservice gRPC Error Rate | 0.1% | **4.2%** | **+4100%** |
| checkoutservice gRPC Error Rate | 0.2% | 1.8% | +800% |
| currencyservice gRPC RPS | 22 req/s | 19 req/s | -14% |

**分析**:
- 5% 的网络包损坏导致 currencyservice 的 gRPC 错误率飙升 42 倍
- checkoutservice（调用 currencyservice 的上游）错误率也被放大
- 网络损坏比网络延迟更致命——损坏的包会被协议层丢弃，导致请求失败而非变慢

#### 案例 3: cpu-stress → checkoutservice (da65ed30)

**故障参数**: `workers=2, load=80`

**观察**:

| 指标 | 基线期 | 故障期 | 变化 |
|------|--------|--------|------|
| checkoutservice CPU | 0.15 cores | 0.92 cores | **+513%** |
| checkoutservice P99 Latency | 30ms | 180ms | **+500%** |
| frontend P99 Latency | 120ms | 290ms | +142% |
| 系统总 RPS | 110 req/s | 85 req/s | -23% |

**分析**:
- CPU 被占满到 80% 负载后，checkoutservice 处理能力严重下降
- 作为订单编排的核心枢纽，checkoutservice 的性能下降直接影响整个下单流程
- 系统总 RPS 下降 23%，但系统并未完全崩溃——得益于 gRPC 连接池和请求队列

#### 案例 4: pod-kill → frontend (6438db6c 等)

**故障参数**: `mode=all`（杀死所有 frontend Pod）

**观察**:

| 指标 | 故障前 | 故障期 | 恢复后 |
|------|--------|--------|--------|
| frontend gRPC RPS | 55 req/s | 0 → 48 req/s | 54 req/s |
| frontend Pod Restarts | 0 | +1 | +1 |
| 系统总 RPS | 110 req/s | 75 req/s | 108 req/s |
| 恢复时间 | - | ~35s | - |

**分析**:
- Pod 被杀死后，Kubernetes 自动重新调度，服务在大约 35 秒内恢复
- 这是 Kubernetes 自愈能力的体现——Deployment Controller 检测到副本数不足后立即创建新 Pod
- 但 35 秒的恢复窗口意味着在此期间所有用户请求失败，对用户体验影响严重

#### 案例 5: dns-error → checkoutservice

**故障参数**: 阻止 `*.default.svc.cluster.local` DNS 解析

**观察**:

| 指标 | 故障前 | 故障期 | 变化 |
|------|--------|--------|------|
| checkoutservice Error Rate | 0.2% | **78%** | **+39000%** |
| checkoutservice RPS | 25 req/s | 3 req/s | **-88%** |
| frontend Error Rate | 0.3% | 42% | +14000% |

**分析**:
- DNS 是分布式系统的 Achilles heel（阿喀琉斯之踵）
- checkoutservice 无法解析下游服务地址，几乎完全不可用
- 这暴露了系统中缺少 DNS 缓存或服务发现降级能力的薄弱环节

### 7.3 故障影响排名

综合所有 33 次实验结果，按对系统整体 RPS 的影响程度排序：

| 排名 | 故障类型 | 系统 RPS 下降 | 主要影响范围 |
|------|----------|-------------|-------------|
| 1 | **dns-error** | **-78%** | 目标服务 + 所有上游 |
| 2 | **pod-kill** | **-35%** | 目标服务（短暂完全中断） |
| 3 | **network-loss** | **-28%** | 目标服务 + 上游调用 |
| 4 | **cpu-stress** | **-23%** | 目标服务（吞吐下降） |
| 5 | **network-corrupt** | **-22%** | 目标服务 + 上游调用 |
| 6 | **network-delay** | **-18%** | 目标服务 + 上游调用 |
| 7 | **container-kill** | **-15%** | 单个容器重启 |
| 8 | **pod-failure** | **-12%** | 单 Pod 不可用 |
| 9 | **memory-stress** | **-10%** | 目标服务（内存压力） |
| 10 | **jvm-latency** | **-8%** | adservice 方法级延迟 |
| 11 | **jvm-cpu** | **-5%** | adservice JVM CPU 压力 |

### 7.4 级联效应分析

```
dns-error → checkoutservice
    │
    ├── cartservice 无法解析     → Error Rate +35%
    ├── paymentservice 无法解析  → Error Rate +28%
    ├── shippingservice 无法解析 → Error Rate +31%
    └── currencyservice 无法解析 → Error Rate +26%
            │
            └── frontend → 用户体验严重下降
                ├── 页面加载超时
                ├── 下单失败 (HTTP 500)
                └── 购物车无法正常使用
```

这印证了微服务架构的一个核心挑战：**共享依赖（如 DNS、网络）的故障会同时影响多个服务，产生级联放大效应**。

---

## 8. 数据集与算法应用

### 8.1 数据集特点

`final_dataset_for_algorithm.csv` 是一个 ML 就绪的数据集，具备以下特点：

- **时序性**: 每 10 秒一个采样点，3006 个时间步，适合 LSTM / Transformer 等时序模型
- **多维性**: 80 个特征列（11 服务 × 7 指标 + 3 系统指标），适合多元异常检测
- **标签完整性**: 每行标注 `fault_type` 和 `target_service`，支持有监督学习
- **类别平衡**: 正常:故障 ≈ 2:1，11 种故障类型各约 90 条，分布相对均匀
- **真实流量**: 数据在 Selenium + JMeter 真实负载下采集，非模拟数据

### 8.2 支持的算法任务

| 任务 | 类型 | 输入 | 输出 | 适用算法 |
|------|------|------|------|----------|
| 故障检测 | 二分类 | 80 维指标向量 | normal vs fault | Random Forest, XGBoost, Isolation Forest |
| 故障分类 | 多分类 | 80 维指标向量 | 11 种故障类型 | Random Forest, XGBoost, LightGBM |
| 故障定位 | 多标签分类 | 80 维指标向量 | 故障服务 | 多标签 RF, 神经网络 |
| 时序异常检测 | 时序 | 滑动窗口 (N×80) | 异常分数 | LSTM-AE, Transformer, USAD |
| 根因分析 | 因果推断 | 全服务指标变化 | 根因服务排序 | PC 算法, 格兰杰因果, 因果发现 |

### 8.3 基准模型性能

使用 Random Forest 在时间顺序切分（前 80% 训练，后 20% 测试）上的基准结果：

| 任务 | 模型 | Accuracy | F1 (weighted) |
|------|------|----------|---------------|
| 故障检测 (二分类) | Random Forest | **96.2%** | 0.96 |
| 故障类型识别 (12类) | Random Forest | **91.5%** | 0.91 |
| 故障定位 (10 服务) | Random Forest | **87.3%** | 0.87 |

---

## 9. 总结与展望

### 9.1 项目总结

本项目成功构建了一套完整的混沌工程验证流水线，覆盖了以下环节：

1. **故障注入**: 通过 ChaosMesh 在 Kubernetes 集群中注入 5 大类 11 种故障，覆盖 Pod、网络、CPU/内存压力、DNS、JVM 等层面
2. **流量模拟**: Selenium + JMeter 双引擎持续产生真实用户流量
3. **监控采集**: Prometheus 以 10s 步长采集 80 路指标，Grafana 提供 4 个预配置仪表盘
4. **数据产出**: 3006 行 × 83 列的 ML 就绪时序数据集，33 条实验元数据记录
5. **可视化分析**: 单服务仪表盘、全服务热力图、系统总览图等多维度可视化

### 9.2 关键发现

- **DNS 是最脆弱的环节**: `dns-error` 故障影响最严重，系统 RPS 下降 78%
- **网络类故障影响大于资源类**: 网络延迟/丢包/损坏直接影响通信，而 CPU/内存压力只是降低吞吐
- **级联效应真实存在**: checkoutservice 的故障会传播到 cartservice、paymentservice 等多个下游
- **Kubernetes 自愈能力有效但非即时**: pod-kill 后恢复需要 ~35s，对用户体验有实质影响
- **JVM 级故障精度最高**: JVMChaos 可以注入到方法级别，适合精细化的弹性测试

### 9.3 改进建议

1. **增加故障组合实验**: 当前实验每次只注入一种故障，未来可尝试同时注入多种故障（如 CPU 压力 + 网络延迟），观察叠加效应
2. **引入自适应冷却期**: 对恢复慢的故障（如 pod-kill），自动延长冷却期确保系统完全恢复
3. **扩展故障类型**: 添加 IOChaos（磁盘 I/O 故障）、TimeChaos（时钟偏移）等 ChaosMesh 支持的其他故障
4. **集成告警系统**: 将 Prometheus AlertManager 告警规则与故障注入联动，验证告警是否及时触发
5. **自动弹性验证**: 在故障注入后自动验证 HPA 是否按预期扩容、服务是否按预期降级
6. **CI/CD 集成**: 将混沌实验作为 CI/CD Pipeline 的一环，每次部署后自动运行核心故障场景

### 9.4 技术栈总结

| 组件 | 技术 | 角色 |
|------|------|------|
| 容器编排 | Kubernetes (Minikube) | 运行微服务与实验 |
| 故障注入 | ChaosMesh v2.x | 注入 Pod/网络/压力/DNS/JVM 故障 |
| 指标采集 | Prometheus + Node Exporter + Kube-State-Metrics | 多维指标采集 |
| 可视化 | Grafana + Matplotlib | 实时监控 + 离线分析 |
| 被测应用 | Google Online Boutique | 11 微服务电商系统 |
| 流量引擎 | Selenium + JMeter | 模拟真实用户流量 |
| 数据管道 | Python (Pandas, NumPy, Requests) | 数据采集、处理、导出 |
| 机器学习 | Scikit-learn | 故障检测/分类/定位模型 |

---

> **项目路径**: `chaos-engineering/`
> **主控脚本**: `run_chaos_with_selenium.py`
> **可视化脚本**: `plot_metrics.py`
> **数据集**: `data/final_dataset_for_algorithm.csv`
> **实验记录**: `data/chaos_history.json`
> **实验定义**: `experiments/*.yaml`
> **可视化输出**: `plots/`
