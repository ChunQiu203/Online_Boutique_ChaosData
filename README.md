# Chaos Engineering — 全自动化故障注入与监控采集

独立的混沌工程实验模块，与微服务部署代码完全解耦。

## 文件说明

| 文件 | 用途 |
|------|------|
| `run_chaos_experiment.py` | **主实验脚本** — 5大类11种故障 × 5次重复 = 55次全量实验，自动注入/解除 + Prometheus指标采集 + 宽表CSV导出 |
| `chaos_quick_test.py` | **快速验证Demo** — 单次CPU压力注入 + 全服务多指标采集 + CSV导出，用于快速验证闭环是否打通 |
| `process_and_label_data.py` | **数据处理脚本** — 解析Prometheus原始JSON，时间对齐、列名规范化、故障区间打标 |
| `test_process_pipeline.py` | **管道集成测试** — 用合成数据验证数据处理全流程 |
| `experiments/` | 11个ChaosMesh YAML定义文件 |

## 快速开始

```bash
# 1. 先跑一次快速验证 (3分钟)
cd chaos-engineering
python chaos_quick_test.py --dry-run    # 空跑，验证管道
python chaos_quick_test.py              # 正式注入 + 采集

# 2. 跑全量实验 (55次, ~28小时)
python run_chaos_experiment.py --dry-run   # 空跑，检查YAML和配置
python run_chaos_experiment.py             # 正式运行

# 3. 数据处理 (可选，主脚本已内置导出)
python process_and_label_data.py
```

## 依赖

```bash
pip install requests pandas numpy pyyaml
```

## 前置条件

- Minikube 集群运行中
- ChaosMesh 已部署至 `chaos-testing` 命名空间
- Prometheus 端口转发至 `localhost:9090`
- JMeter 持续压测流量

## 输出

- `chaos_history.json` — 实验元数据（故障类型、起止时间、目标服务）
- `chaos_dataset.csv` — 全服务 × 多指标 宽表（~6840行 × 82列，带 `fault_type` 标签）

## 完整文档

详见 `CHAOS_EXPERIMENT_DOCS.txt`
