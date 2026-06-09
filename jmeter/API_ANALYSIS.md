# API Analysis

本文档基于当前仓库源码和 `deploy-all.yaml` 静态确认，不使用预期值替代源码事实。

## Git 和源码范围

- 当前分支: `main`
- 当前提交: `8f3d3119a17d051a6f54cbd95c94a3587fce3a80`
- 主要部署清单: `deploy-all.yaml`
- 主要源码:
  - `src/frontend/main.go`
  - `src/frontend/handlers.go`
  - `src/frontend/templates/*.html`
  - `src/loadgenerator/locustfile.py`
  - `src/productcatalogservice/products.json`
  - `src/reviewservice/*.go`

## Frontend 路由

| 方法 | 路径 | 处理器 | 说明 |
|---|---|---|---|
| GET/HEAD | `/` | `homeHandler` | 首页 |
| GET/HEAD | `/product/{id}` | `productHandler` | 商品详情页，读取评论和评论统计 |
| GET/HEAD | `/cart` | `viewCartHandler` | 查看购物车 |
| POST | `/cart` | `addToCartHandler` | 加入购物车，成功返回 302 到 `/cart` |
| POST | `/cart/empty` | `emptyCartHandler` | 清空购物车，成功返回 302 到 `/` |
| POST | `/setCurrency` | `setCurrencyHandler` | 设置币种 Cookie，成功返回 302 到 Referer 或 `/` |
| POST | `/cart/checkout` | `placeOrderHandler` | 结算，下单成功渲染订单页 |
| GET | `/logout` | `logoutHandler` | 清除 Cookie |
| GET | `/_healthz` | inline handler | 返回 `ok` |
| GET | `/product-meta/{ids}` | `getProductByID` | 返回单个商品 JSON |
| POST | `/bot` | `chatBotHandler` | 聊天助手代理 |
| POST | `/reviews` | `postReviewHandler` | 代理评论创建到 reviewservice |
| GET | `/reviews/{product_id}` | `getReviewsHandler` | 读取评论后渲染 `reviews` 模板 |

`getReviewsHandler` 当前调用 `templates.ExecuteTemplate(w, "reviews", ...)`，但 `src/frontend/templates/` 中未发现 `{{ define "reviews" }}`。因此该路由存在，但 HTML 片段渲染很可能失败并产生空响应或服务端日志错误。JMeter 保留 `T07_Verify_Review_Fragment` 断言以暴露该问题，不通过删除断言规避。

## Reviewservice 路由

| 方法 | 路径 | 处理器 | 状态码 |
|---|---|---|---|
| GET | `/_healthz` | `healthHandler` | 200，正文 `ok` |
| POST | `/reviews` | `createReviewHandler` | 成功 201，校验失败 400，写入失败 500 |
| GET | `/reviews?product_id={id}` | `getReviewsHandler` | 成功 200，缺少 product_id 为 400 |
| GET | `/reviews/stats?product_id={id}` | `getReviewStatsHandler` | 成功 200，缺少 product_id 为 400 |
| GET | `/reviews/{id}` | `getReviewHandler` | 成功 200，不存在 404，缺少 id 为 400 |
| DELETE | `/reviews/{id}` | `deleteReviewHandler` | 成功 204，不存在或删除失败按当前代码返回 500 |

错误响应格式:

```json
{
  "error": "message",
  "timestamp": "UTC timestamp"
}
```

## 请求字段

### 添加购物车

`POST /cart` 使用表单字段:

| 字段 | 来源 | 约束 |
|---|---|---|
| `product_id` | `handlers.go` / `product.html` | 必填 |
| `quantity` | `handlers.go` / `product.html` | 1 到 10 |

成功响应为 302，`Location: /cart`。

### 币种切换

`POST /setCurrency` 使用表单字段:

| 字段 | 源码 |
|---|---|
| `currency_code` | `handlers.go` / `header.html` |

源码白名单和模板可见币种: `USD`, `EUR`, `CAD`, `JPY`, `GBP`, `TRY`。成功时写入 `shop_currency` Cookie 并返回 302。

### 结算

`POST /cart/checkout` 使用表单字段:

| 字段 |
|---|
| `email` |
| `street_address` |
| `zip_code` |
| `city` |
| `state` |
| `country` |
| `credit_card_number` |
| `credit_card_expiration_month` |
| `credit_card_expiration_year` |
| `credit_card_cvv` |

`validator.PlaceOrderPayload` 要求邮箱合法、地址字段必填、信用卡号通过 `credit_card` 校验、月份为 1 到 12、年份和 CVV 必填。订单成功页面稳定文本包括 `Your order is complete!`、`Confirmation #`、`Total Paid`。

### 评论创建

`POST /reviews` 通过 frontend 代理到 reviewservice，JMeter 使用 `Content-Type: application/json`。

请求 JSON 字段:

```json
{
  "product_id": "OLJCESPC7Z",
  "user_name": "jmeter-user",
  "rating": 5,
  "title": "jmeter-review",
  "content": "automated-performance-test"
}
```

成功响应字段:

| 字段 | 类型 |
|---|---|
| `id` | string |
| `product_id` | string |
| `user_name` | string |
| `rating` | number |
| `title` | string |
| `content` | string |
| `created_at` | timestamp |

评论创建成功状态码为 201。校验失败状态码为 400。frontend 代理不会完整转发上游 `Content-Type`，因此 JMeter 不把响应 `Content-Type` 作为硬性成功条件，而是校验状态码、JSON 可解析性和业务字段。

负面评论校验:

| 用例 | 预期 |
|---|---|
| 非法 JSON | 400 JSON 错误 |
| 空 `product_id` | 400 JSON 错误 |
| 空 `user_name` | 400 JSON 错误 |
| `rating=0` | 400 JSON 错误 |
| `rating=6` | 400 JSON 错误 |
| 空 `title` | 400 JSON 错误 |
| 空 `content` | 400 JSON 错误 |

## Cookie 行为

frontend 使用:

- `shop_session-id`: `ensureSessionID` 生成，一个浏览器会话对应一个购物车用户 ID。
- `shop_currency`: `setCurrencyHandler` 写入。

JMeter 使用 HTTP Cookie Manager，不手写 `shop_session-id`。同一线程内 Cookie 持续有效，不同线程不共享 Cookie。

## 稳定断言文本

| 页面 | 稳定文本 |
|---|---|
| 首页 | `Hot Products` |
| 商品详情 | `Customer Reviews`, `Write a Review`, `Add To Cart`, 商品 ID |
| 无评论商品详情 | `No reviews yet. Be the first to review this product!` |
| 购物车 | `Cart (`, `SKU #`, `Quantity:`, `Total` |
| 空购物车 | `Your shopping cart is empty!` |
| 结算成功 | `Your order is complete!`, `Confirmation #`, `Total Paid` |
| 错误模板 | `Uh, oh!`, `Something has failed` |

## 真实商品 ID

来自 `src/loadgenerator/locustfile.py` 和 `src/productcatalogservice/products.json`:

- `OLJCESPC7Z`
- `66VCHSJNUP`
- `1YMWWN1N4O`
- `L9ECAV7KIM`
- `2ZYFJ3GM2N`
- `0PUK6V6EV0`
- `LS4PSXUNUM`
- `9SIQT8TOJO`
- `6E92ZMYYFZ`

## 服务依赖

| 业务步骤 | 主要服务 |
|---|---|
| 首页 | frontend, productcatalogservice, currencyservice, cartservice, adservice |
| 商品详情和评论读取 | frontend, productcatalogservice, recommendationservice, currencyservice, reviewservice |
| 评论提交 | frontend, reviewservice |
| 购物车 | frontend, cartservice, redis-cart, productcatalogservice |
| 结算 | frontend, checkoutservice, paymentservice, shippingservice, emailservice, cartservice |

`productHandler` 调用 `getProductReviews`。评论读取和统计读取失败时仅写 warning 并返回 `nil`，商品详情页仍可能 200。因此商品页 200 不能单独证明 reviewservice 正常。

## Kubernetes 部署

| 对象 | 名称 | 镜像 |
|---|---|---|
| frontend Deployment | `frontend` | `frontend:local` |
| frontend Service | `frontend`, `frontend-external` | 不适用 |
| reviewservice Deployment | `reviewservice` | `reviewservice:local` |
| reviewservice Service | `reviewservice` | 不适用 |

frontend 环境变量:

- `REVIEW_SERVICE_ADDR=reviewservice:8080`

reviewservice 环境变量:

- `SQLITE_DB_PATH=/data/reviews.db`

## SQLite 数据生命周期

`deploy-all.yaml` 中 reviewservice 挂载:

```yaml
volumeMounts:
- name: data
  mountPath: /data
volumes:
- name: data
  emptyDir: {}
```

结论:

- 评论写入会改变测试环境。
- 同一个 Pod 内容器重启时，`emptyDir` 中的数据通常仍在。
- Pod 被删除并重新创建后，`emptyDir` 数据会消失。
- Deployment 重建、Pod Kill 或调度迁移可能导致评论丢失。
- 评论丢失可作为故障实验观察结果。
- 不应删除其他成员创建的评论。JMeter 可选清理只按当前运行提取到的 `review_id` 删除。
