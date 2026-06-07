# Chaos Engineering — 全自动化故障注入流水线

---

## 1. 启动微服务

```bash
# 启动 Minikube
minikube start --cpus=4 --memory=8192 --disk-size=40g

# 部署 Online Boutique
cd online-boutique-course
skaffold run
# 或者: kubectl apply -f ./release/kubernetes-manifests.yaml

# 等待全部 Pod 就绪
kubectl get pods -w
```

---

## 2. 启动 Prometheus + Grafana

```bash
# ★ 一键部署 Prometheus + Grafana + 4 个预配置 Dashboard
cd ..
bash scripts/setup-grafana.sh

# 或手动步骤:
kubectl apply -f ./deploy/kubernetes/manifests-monitoring/

# 等待 Pod 就绪
kubectl get pods -n monitoring -w

# 终端 1: Prometheus 端口转发
kubectl port-forward -n monitoring svc/prometheus 9090:9090

# 终端 2: Grafana 端口转发
kubectl port-forward -n monitoring svc/grafana 3000:80
# 浏览器打开 http://localhost:3000 (admin/prom-operator)
# 预配置 4 个 Dashboard → Dashboards → Online Boutique 文件夹
```

📖 详细文档: [docs/GRAFANA_VISUALIZATION_GUIDE.md](../docs/GRAFANA_VISUALIZATION_GUIDE.md)

---

## 3. 启动 Selenium 流量引擎 (前置准备)

```bash
# 终端 3: 前端端口转发 (Selenium 需要)
kubectl port-forward deployment/frontend 8080:8080

# 验证前端可达
curl -s -o /dev/null -w "%{http_code}" http://localhost:8080
# 应返回: 200

# 安装 Selenium 依赖 (只需一次)
cd online-boutique-course/test/selenium
python -m venv .venv
source .venv/Scripts/activate  # Windows Git Bash
# source .venv/bin/activate    # Linux/Mac
pip install -r requirements.txt
cd ../../chaos-engineering
```

---

## 4. 运行全自动混沌实验

```bash
# 安装脚本依赖 (只需一次)
pip install requests pandas numpy

# 进入混沌工程目录
cd chaos-engineering

# ★ 空跑验证 (不执行 kubectl, 倒计时缩短, 生成模拟数据验证管道)
python run_chaos_with_selenium.py --dry-run

# ★ 正式运行 (11种故障 × 5次重复 = 55次实验, 约28小时)
python run_chaos_with_selenium.py
```

**可选参数：**

```
--skip-selenium          跳过 Selenium, 仅执行故障注入 + 指标采集
--skip-prometheus        跳过 Prometheus 指标采集和 CSV 导出
--output my_data.csv     自定义输出 CSV 文件名
--dry-run                空跑模式
```

---

## 5. 输出文件

| 文件 | 说明 |
|------|------|
| `chaos_history.json` | 55 条实验元数据（故障类型、目标服务、起止时间） |
| `final_dataset_for_algorithm.csv` | ML 就绪宽表 (~80 列, 15s 间隔, 带 fault_type 标签) |
| `selenium_traffic.log` | Selenium 后台流量引擎运行日志 |
