# JMeter Experiments

每次正式实验使用唯一 `RUN_ID`，由 `scripts/run-test.*` 自动创建：

```text
experiments/<RUN_ID>/
├── manifest.csv
├── events.csv
├── jmeter/
│   ├── result.jtl
│   ├── jmeter.log
│   ├── summary.csv
│   ├── summary.md
│   └── report/
├── monitoring/
│   ├── prometheus/
│   └── grafana/
└── chaos/
```

`experiments/<RUN_ID>/` 是运行产物目录，不应提交。监控、Grafana 截图和 Chaos Mesh YAML 使用同一个 `RUN_ID` 归档，故障开始和结束时间写入 `events.csv`。
