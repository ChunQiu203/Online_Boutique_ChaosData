# Online Boutique JMeter 性能测试方案

本目录提供一套 Apache JMeter 自动化性能测试包，只负责 Online Boutique 前端、购物车、结算和新增评论功能的性能测试与结果汇总。

## 1. 测试目标

- 验证首页、商品详情、购物车、币种切换、结算和评论功能在不同并发下的端到端表现。
- 单独统计评论读取、评论写入、评论展示验证和负面评论校验。
- 为 Prometheus、Grafana 和 Chaos Mesh 成员提供统一 Run ID、事件时间线和结果目录。
- 不伪造 JTL、吞吐量、响应时间、通过率或 HTML 报告。

正式测试必须使用 JMeter CLI。GUI 只用于检查和调试 `online-boutique.jmx`。

## 2. 项目架构

当前项目基于 Google Online Boutique，使用 Kubernetes/Minikube 部署。主要部署文件是仓库根目录的 `deploy-all.yaml`。

与本测试包直接相关的服务:

| 业务 | 服务 |
|---|---|
| 首页 | frontend, productcatalogservice, currencyservice, cartservice, adservice |
| 商品详情和评论读取 | frontend, productcatalogservice, recommendationservice, currencyservice, reviewservice |
| 评论提交 | frontend, reviewservice |
| 购物车 | frontend, cartservice, redis-cart, productcatalogservice |
| 结算 | frontend, checkoutservice, paymentservice, shippingservice, emailservice, cartservice |

## 3. 评论功能说明

frontend 通过 `REVIEW_SERVICE_ADDR=reviewservice:8080` 调用 reviewservice。评论创建通过 frontend `POST /reviews` 代理到 reviewservice。商品详情页读取 reviewservice 的 `/reviews?product_id=...` 和 `/reviews/stats?product_id=...`。

当前源码确认 `GET /reviews/{product_id}` 路由存在，但 `src/frontend/templates/` 中未发现 `reviews` 模板定义。`T07_Verify_Review_Fragment` 会保留断言以暴露该问题，不会通过删除断言让测试假通过。

## 4. 环境要求

- Apache JMeter 5.6.3 或更高版本
- Java 运行环境，版本需满足 JMeter 要求
- Python 3.10+
- Windows 10/11 PowerShell 或 Git Bash
- kubectl
- Minikube
- 已部署 Online Boutique，且 frontend 和 reviewservice 使用本地镜像

验证:

```bash
jmeter --version
java -version
python --version
kubectl config current-context
```

## 5. 部署检查

部署和镜像切换由项目部署流程负责，本测试包不修改 Kubernetes 部署逻辑。

正式 JMeter 压测前建议执行:

```bash
kubectl get nodes
kubectl get deployment frontend reviewservice
kubectl get service frontend frontend-external reviewservice
kubectl get pods
```

确认:

- frontend Deployment 名称: `frontend`
- reviewservice Deployment 名称: `reviewservice`
- frontend Service 名称: `frontend`, `frontend-external`
- reviewservice Service 名称: `reviewservice`
- frontend 镜像: `frontend:local`
- reviewservice 镜像: `reviewservice:local`
- frontend 环境变量: `REVIEW_SERVICE_ADDR=reviewservice:8080`

## 6. 关闭内置 loadgenerator

正式压测前关闭项目内置 loadgenerator，避免叠加负载:

```bash
kubectl scale deployment/loadgenerator --replicas=0
```

环境检查脚本会检测 loadgenerator 副本数。如果不为 0，只输出警告，默认不会修改集群。

## 7. 端口转发

frontend:

```bash
kubectl port-forward deployment/frontend 8080:8080
```

可选 reviewservice 直连，用于诊断或按 `review_id` 清理本轮评论:

```bash
kubectl port-forward service/reviewservice 8081:8080
```

默认目标 Base URL:

```text
http://127.0.0.1:8080
```

## 8. 环境检查脚本

PowerShell:

```powershell
cd test\jmeter
.\scripts\check-environment.ps1 -Host 127.0.0.1 -Port 8080
```

Git Bash:

```bash
cd test/jmeter
./scripts/check-environment.sh --host 127.0.0.1 --port 8080
```

检查内容包括 Kubernetes 连通性、Deployment/Service、Pod、镜像、`REVIEW_SERVICE_ADDR`、Base URL、`/_healthz`、商品详情页评论区域和 loadgenerator 状态。

## 9. 场景与 JMX

只维护一个主测试计划:

```text
online-boutique.jmx
```

通过属性 `scenario` 选择场景:

| scenario | 说明 |
|---|---|
| `shopping` | 首页、商品详情、加购、购物车、可选币种切换、可选结算 |
| `mixed` | shopping 流程加评论写入和展示验证 |
| `review-read` | 商品详情和评论片段读取，不创建评论 |
| `review-write` | 商品详情、评论提交、评论片段验证、商品页验证 |
| `review-negative` | 评论负面校验，预期 4xx 单独统计 |

主要 Sample Label:

```text
T01_Home
T02_Product_Detail_With_Reviews
T03_Add_To_Cart
T04_View_Cart
T05_Change_Currency
T06_Submit_Review
T07_Verify_Review_Fragment
T08_Verify_Review_Product_Page
T09_Checkout
NEG_Review_Invalid_JSON
NEG_Review_Missing_Product
NEG_Review_Missing_User
NEG_Review_Invalid_Rating_Low
NEG_Review_Invalid_Rating_High
NEG_Review_Missing_Title
NEG_Review_Missing_Content
```

## 10. 公共参数

所有关键参数都可通过 `-J` 覆盖，命令行 `-J` 优先于 properties 文件。

| 参数 | 默认值 |
|---|---|
| `protocol` | `http` |
| `host` | `127.0.0.1` |
| `port` | `8080` |
| `base_path` | 空 |
| `scenario` | `shopping` |
| `users` | `1` |
| `rampup` | `1` |
| `duration` | `60` |
| `run_id` | `LOCAL-UNSET` |
| `checkout_percent` | `30` |
| `currency_percent` | `20` |
| `review_write_percent` | `10` |
| `think_time_min_ms` | `1000` |
| `think_time_max_ms` | `3000` |
| `connect_timeout_ms` | `5000` |
| `response_timeout_ms` | `15000` |
| `review_rating` | `5` |
| `review_user_prefix` | `jmeter-user` |
| `review_title_prefix` | `jmeter-review` |
| `review_content_prefix` | `automated-performance-test` |
| `review_submit_path` | `/reviews` |
| `review_fragment_path_template` | `/reviews/${product_id}` |
| `product_detail_path_template` | `/product/${product_id}` |
| `review_direct_enabled` | `false` |
| `review_direct_host` | `127.0.0.1` |
| `review_direct_port` | `8081` |
| `review_cleanup_enabled` | `false` |

## 11. 运行冒烟测试

PowerShell:

```powershell
cd test\jmeter
.\scripts\run-test.ps1 `
  -Config .\config\smoke.properties `
  -RunId OB-SMOKE-R01 `
  -Host 127.0.0.1 `
  -Port 8080
```

Git Bash:

```bash
cd test/jmeter
./scripts/run-test.sh \
  --config ./config/smoke.properties \
  --run-id OB-SMOKE-R01 \
  --host 127.0.0.1 \
  --port 8080
```

冒烟目标:

- 首页成功
- 商品详情成功
- 评论区域存在
- 评论提交返回 201
- 评论片段和商品页展示验证成功
- 加入购物车成功
- 查看购物车成功
- 结算成功
- JTL、HTML Dashboard、`summary.csv`、`summary.md` 生成

## 12. 基准测试

```powershell
.\scripts\run-test.ps1 -Config .\config\baseline-10.properties -RunId OB-BASELINE-10U-R01
.\scripts\run-test.ps1 -Config .\config\baseline-30.properties -RunId OB-BASELINE-30U-R01
.\scripts\run-test.ps1 -Config .\config\baseline-50.properties -RunId OB-BASELINE-50U-R01
```

Git Bash:

```bash
./scripts/run-test.sh --config ./config/baseline-10.properties --run-id OB-BASELINE-10U-R01
./scripts/run-test.sh --config ./config/baseline-30.properties --run-id OB-BASELINE-30U-R01
./scripts/run-test.sh --config ./config/baseline-50.properties --run-id OB-BASELINE-50U-R01
```

## 13. 评论读取测试

不持续创建新评论:

```bash
./scripts/run-test.sh \
  --config ./config/review-read-30.properties \
  --run-id OB-REVIEW-READ-30U-R01
```

## 14. 评论写入测试

受 `review_write_percent` 和思考时间控制，避免无限快速写库:

```bash
./scripts/run-test.sh \
  --config ./config/review-write-10.properties \
  --run-id OB-REVIEW-WRITE-10U-R01
```

## 15. 评论负面测试

```bash
./scripts/run-test.sh \
  --config ./config/negative-review.properties \
  --run-id OB-REVIEW-NEGATIVE-R01
```

`NEG_Review_*` 的预期 4xx 会由汇总工具单独统计，不纳入正常业务错误率。

## 16. 故障实验

本测试包不部署 Prometheus、Grafana、Chaos Mesh，也不注入故障。JMeter 在 13 分钟内持续运行，故障由其他成员操作。

混合故障实验:

```bash
./scripts/run-test.sh \
  --config ./config/fault-30.properties \
  --run-id OB-FAULT-30U-R01
```

reviewservice 故障实验:

```bash
./scripts/run-test.sh \
  --config ./config/review-fault-30.properties \
  --run-id OB-REVIEW-DELAY-30U-R01
```

建议时间线:

```text
0-2 分钟: 预热
2-5 分钟: 正常阶段
5-10 分钟: 故障阶段
10-13 分钟: 恢复阶段
```

故障开始和结束时立即标记事件。

PowerShell:

```powershell
.\scripts\mark-event.ps1 `
  -RunId OB-REVIEW-DELAY-30U-R01 `
  -Event FAULT_START `
  -Details "reviewservice network delay 200ms"
```

Git Bash:

```bash
./scripts/mark-event.sh \
  --run-id OB-REVIEW-DELAY-30U-R01 \
  --event FAULT_END \
  --details "reviewservice network delay removed"
```

支持事件:

```text
TEST_START
WARMUP_END
FAULT_START
FAULT_END
TEST_END
NOTE
```

## 17. 结果目录

每次运行创建:

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

脚本会拒绝覆盖已存在的 Run ID。

HTML Dashboard 位置:

```text
experiments/<RUN_ID>/jmeter/report/index.html
```

## 18. 汇总工具

手动汇总:

```bash
python tools/summarize_results.py \
  --jtl experiments/RUN_ID/jmeter/result.jtl \
  --events experiments/RUN_ID/events.csv \
  --output-dir experiments/RUN_ID/jmeter
```

输出:

- `summary.csv`
- `summary.md`

统计内容:

- 总体样本数、成功数、失败数、错误率
- 平均、最小、最大、中位数、P90、P95、P99
- 吞吐量、接收速率、发送速率
- 按 Sample Label 分组
- 评论读取、提交、展示验证专项
- 4xx、5xx、连接错误、超时、JSON 断言失败、页面内容断言失败
- 负面评论用例单独统计
- 如果 `events.csv` 有有效 `FAULT_START` 和 `FAULT_END`，按 normal/fault/recovery 阶段统计

## 19. 配置文件用途

| 文件 | 用途 |
|---|---|
| `smoke.properties` | 1 用户混合冒烟，评论和结算都强制执行 |
| `baseline-10.properties` | 10 用户混合基准 |
| `baseline-30.properties` | 30 用户混合基准 |
| `baseline-50.properties` | 50 用户混合基准 |
| `review-read-30.properties` | 30 用户评论读取 |
| `review-write-10.properties` | 10 用户评论写入 |
| `fault-30.properties` | 13 分钟混合故障实验负载 |
| `review-fault-30.properties` | 13 分钟评论写入故障实验负载 |
| `negative-review.properties` | 评论负面校验 |

## 20. 与监控成员交接

所有成员使用相同 Run ID，例如:

```text
OB-BASELINE-10U-R01
OB-BASELINE-30U-R01
OB-BASELINE-50U-R01
OB-REVIEW-READ-30U-R01
OB-REVIEW-WRITE-10U-R01
OB-REVIEW-DELAY-30U-R01
```

监控成员保存:

- Prometheus CSV 到 `experiments/<RUN_ID>/monitoring/prometheus/`
- Grafana 截图到 `experiments/<RUN_ID>/monitoring/grafana/`
- Chaos Mesh YAML 和事件说明到 `experiments/<RUN_ID>/chaos/`
- 故障时间写入同一个 `events.csv`

至少采集:

```text
Pod CPU
Pod 内存
网络接收速率
网络发送速率
Pod 重启次数
Pod Ready 状态
Deployment 可用副本数
节点 CPU
节点内存
应用请求数量
应用延迟
应用错误率
```

reviewservice 故障实验重点观察:

```text
reviewservice CPU
reviewservice 内存
reviewservice 网络
reviewservice Ready
reviewservice Pod 重启
frontend 错误
评论提交错误率
商品详情 P95
评论是否丢失
恢复时间
```

## 21. 评论数据污染和清理

reviewservice 使用:

```text
SQLITE_DB_PATH=/data/reviews.db
Volume=emptyDir
```

注意:

- 评论写入会改变测试环境。
- 同一个 Pod 内容器重启时数据通常仍在。
- Pod 被重新创建后 `emptyDir` 数据会消失。
- Pod Kill、Deployment 重建可能导致评论数据丢失。
- 评论数据丢失本身可作为故障实验观察结果。
- 不要随意删除其他成员评论。

默认不清理:

```properties
review_cleanup_enabled=false
```

如需清理当前 JMeter run 创建且已提取 `review_id` 的评论:

```bash
kubectl port-forward service/reviewservice 8081:8080
./scripts/run-test.sh \
  --config ./config/review-write-10.properties \
  --run-id OB-REVIEW-WRITE-CLEAN-R01 \
  --jmeter-property review_direct_enabled=true \
  --jmeter-property review_cleanup_enabled=true
```

PowerShell 等价参数:

```powershell
.\scripts\run-test.ps1 `
  -Config .\config\review-write-10.properties `
  -RunId OB-REVIEW-WRITE-CLEAN-R01 `
  -JMeterProperty review_direct_enabled=true,review_cleanup_enabled=true
```

清理失败不应掩盖正式测试结果。

## 22. 常见错误排查

| 现象 | 可能原因 | 处理 |
|---|---|---|
| `jmeter` 未找到 | JMeter 未安装或未加入 PATH | 安装 JMeter 5.6.3+ 并重开终端 |
| `python` 未找到 | Python 未安装或未加入 PATH | 安装 Python 3.10+ |
| `kubectl get nodes` 失败 | Minikube 未启动或 context 错误 | `minikube start`，确认 context |
| Base URL 不通 | 未启动 port-forward | `kubectl port-forward deployment/frontend 8080:8080` |
| 商品详情没有评论区域 | frontend 镜像不是 `frontend:local` 或部署未更新 | 检查 Deployment 镜像和环境变量 |
| `T07_Verify_Review_Fragment` 失败 | 当前 frontend 缺少 `reviews` 模板或路由片段为空 | 查看 frontend 日志并修复业务模板后重跑 |
| 结算失败 | 后端服务未 Ready 或测试卡字段被修改 | 检查 checkout/payment/shipping/email/cart 服务 |
| HTML Dashboard 生成失败 | 报告目录已存在且非空 | 使用唯一 Run ID |
| 结果错误率受内置负载影响 | loadgenerator 未关闭 | `kubectl scale deployment/loadgenerator --replicas=0` |

## 23. 测试结果局限性

- JMeter 响应时间是客户端观察到的端到端时间，不是服务端纯处理时间。
- JMeter 不执行页面 JavaScript，因此评论提交后必须显式请求验证接口和商品页。
- 样本量少时百分位数仅作参考。
- 商品页 200 不等于 reviewservice 正常，因为源码中评论读取失败为非致命降级。
- 当前评论片段路由存在模板缺口，相关断言失败应作为真实缺陷或待修复项处理。
- 本测试包不包含 Prometheus、Grafana 或 Chaos Mesh 的安装和故障注入逻辑。

## 24. 文件说明

```text
test/jmeter/
├── online-boutique.jmx
├── README.md
├── API_ANALYSIS.md
├── data/products.csv
├── config/*.properties
├── scripts/run-test.ps1
├── scripts/run-test.sh
├── scripts/check-environment.ps1
├── scripts/check-environment.sh
├── scripts/mark-event.ps1
├── scripts/mark-event.sh
├── tools/summarize_results.py
├── experiments/manifest-template.csv
├── results/.gitkeep
├── reports/.gitkeep
└── logs/.gitkeep
```
