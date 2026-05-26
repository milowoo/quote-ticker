# Quote-Ticker

消费撮合引擎的成交推送，生成多周期 K 线数据，通过 WebSocket 实时推送成交，并提供 HTTP 接口查询 K 线。

---

## 架构总览

```
┌─────────────────────────────────────────────────────────────┐
│                      match_aeron (撮合引擎)                   │
│                        Kafka PbTradeTick                     │
└──────────────────────────────────────────┬──────────────────┘
                                           │
                                           ▼
┌─────────────────────────────────────────────────────────────┐
│                    quote-ticker (Go Service)                  │
│                                                              │
│  ┌──────────┐   ┌───────────┐   ┌──────────────────────┐    │
│  │  Kafka   │──▶│  Kline    │──▶│  Asnyc Flush Worker  │───▶│──▶ TiDB
│  │ Consumer │   │Aggregator │   │  (goroutine pool)    │    │   (t_kline_{symbol})
│  │(N workers)│  │(64 shards)│   └──────────────────────┘    │
│  └──────────┘   └─────┬─────┘                                │
│                        │                                      │
│                        ▼                                      │
│  ┌─────────────────────────────────────────┐                │
│  │          WebSocket Hub                    │                │
│  │  (per-symbol fan-out goroutine)          │──▶ 前端订阅者   │
│  └─────────────────────────────────────────┘                │
│                                                              │
│  ┌─────────────────────────────────────────┐                │
│  │          HTTP API                        │                │
│  │  GET /api/klines?symbol=...&interval=.. │◀── 查询历史 K 线 │
│  └─────────────────────────────────────────┘                │
│                                                              │
│  ┌─────────────────────────────────────────┐                │
│  │          Continuity Checker              │                │
│  │  (每 10min 检测 + backfill 长周期 K 线)    │                │
│  └─────────────────────────────────────────┘                │
│                                                              │
│  ┌─────────────────────────────────────────┐                │
│  │          etcd Elector                    │                │
│  │  (多实例选主，单实例可关闭)                │                │
│  └─────────────────────────────────────────┘                │
└─────────────────────────────────────────────────────────────┘
```

---

## 数据流

### 核心路径（逐笔成交 → K 线）

完整调用链：

```
Kafka PbTradeTick (protobuf)
  │
  ▼
Kafka Consumer Worker Pool (config.parallel 个 goroutine)     ← internal/kafka/consumer.go
  │ proto.Unmarshal(data, &pb.PbTradeTick{})
  │ NewTradeFromTick(tick) → Price/Quantity 转换为 int64 定点数 (decimal.D)
  │
  └→ c.handler(ctx, trade)        ← main.go 定义的 TradeHandler 闭包
       │                            cmd/server/main.go:126
       │
       ├→ agg.ProcessTrade(ctx, trade)        ← internal/kline/aggregator.go
       │    │
       │    ├── shard[fnv32(symbol) % 64].Lock()    ← 64 分片锁，不同 symbol 完全并行
       │    │    └── 首次则为该 symbol 执行 recoverLocked (从 TiDB 读 checkpoint)
       │    │
       │    ├── for 7 intervals (1m/10m/30m/1h/1w/1mon/1y):
       │    │    ├── iv.AlignFn(t) 计算当前窗口起始时间
       │    │    ├── cur.Update(price, qty, amount)  ← 更新 OHLCV
       │    │    │    └── 纯 int64 定点数运算，零 heap alloc
       │    │    └── 窗口关闭 ? → 标记为 completed kline
       │    │
       │    └── completed > 0 ?
       │         └── flushWorker.Enqueue(symbol, klines)
       │              └── 后台 goroutine 执行 BatchSave → TiDB (REPLACE INTO t_kline_{symbol})
       │                   └── k.ComputeAvg()  ← 加权平均在 flush 时计算，非每笔
       │
       └→ hub.BroadcastTrade(trade)              ← internal/ws/hub.go
            │
            └── ensureBroadcaster(symbol) → per-symbol channel (buf 256)
                 └── broadcastLoop goroutine (每个 symbol 独立)
                      ├── json.Marshal(trade)   ← 一次 marshal，多连接共享
                      └── for each subscriber:
                           └── c.send <- buf    ← 慢消费者 drop，不阻塞
```

### WebSocket 推送

```
前端订阅 → WS: {"action":"subscribe","symbol":"BTCUSDT"}
  │
  └→ hub.subscribe(conn, "BTCUSDT")             ← internal/ws/hub.go
       │
       └→ hub.subs["BTCUSDT"] = {conn1, conn2, ...}

撮合引擎新成交 → Kafka → agg.ProcessTrade
  │
  └→ hub.BroadcastTrade(trade)
       │
       └→ trade → per-symbol chan("BTCUSDT")
            │
            └→ broadcastLoop goroutine (per-symbol, 独立 goroutine)
                 ├── json.Marshal({"type":"trade","data":{...}})
                 └── for each subscriber:
                      ├── c.send <- buf (非阻塞)
                      └── writePump → conn.ws.WriteMessage
```

### 启动与恢复路径

```
服务启动 (cmd/server/main.go)
  │
  ├── db.Ping()
  ├── agg.StartCheckpoint(ctx, 5s)              ← 每 5s 持久化 open bucket
  ├── (可选) elect.Run(ctx)                      ← etcd 选主
  ├── consumer.Run(ctx)                          ← 开始消费 Kafka
  │
  └── 首个 Trade 到达 symbol="BTCUSDT"
       │
       └── agg.recoverLocked(ctx, "BTCUSDT", t)  ← 查 DB 恢复 checkpoint
            │
            ├── LoadKline("BTCUSDT", "1m",  windowStart)
            ├── LoadKline("BTCUSDT", "10m", windowStart)
            ├── LoadKline("BTCUSDT", "30m", windowStart)
            ├── LoadKline("BTCUSDT", "1h",  windowStart)
            ├── LoadKline("BTCUSDT", "1w",  windowStart)
            ├── LoadKline("BTCUSDT", "1mon",windowStart)
            ├── LoadKline("BTCUSDT", "1y",  windowStart)
            └── 命中任一 → 恢复为 bucket 初始状态；未命中 → 从零开始

---

## 性能设计

### 1. int64 定点数（零 GC 压力）

所有价格、成交量字段使用 `internal/decimal.D`（`int64` 类型，精度 10⁻⁸）：

| 操作 | big.Float | decimal.D |
|------|-----------|-----------|
| 解析 | heap alloc + string parse | 纯整数运算，零 alloc |
| 比较 | `Cmp` 方法调用 | 原生 `>` / `<` |
| 加法 | `Add` 方法 | 内联优化 |
| 乘法 | heap alloc | `a*b/Scale`（大数回退 big.Int） |
| 每笔 GC | 21 次 heap alloc | **0 次** |

### 2. Per-Symbol 分片锁

```go
const numShards = 64
shard[fnv32(symbol) % numShards]  // 独立 sync.Mutex
```

- 不同交易对完全不互斥
- 500 交易对 × 10 TPS 时无锁竞争

### 3. 异步 DB Flush

Trade 处理与 DB 写入解耦：
```
ProcessTrade → 只更新内存桶 + 入队 flush channel
FlushWorker → 后台 goroutine 批量 BatchSave
```

- 生产端永不等待 DB
- Buffer 10000，满时 log 告警但不阻塞
- 关闭时 drain 500ms 保证最终写入

### 4. Kafka 多 Worker

```
reader goroutine → chan → worker 1
                         → worker 2
                         → worker N (config.parallel)
```

- 适合多 partition topic
- 处理与 fetch/commit 分离

### 5. WebSocket Per-Symbol Fan-out

每个 symbol 一个独立 goroutine：
- JSON 一次 marshal，多连接共享
- BroadcastTrade 非阻塞（drop 慢消费者）
- 不同 symbol 互不影响

---

## 容错设计

### 1. 幂等消费（防重复处理）

```
Kafka message → proto.Unmarshal
                  │
                  ├── deduper.Seen(tradeID) == true → 跳过（重复消息）
                  │
                  └── deduper.Seen(tradeID) == false → 正常处理并记录 tradeID
                       │
                       └── 处理成功后 CommitMessages（at-least-once 交付）
                            │
                            └── 若 commit 失败，rebalance 后重新消费该消息
                                 → dedup 拦截，不会重复计入 K 线
```

- 使用 ring buffer 记录最近 100 万条 TradeID（≈ 16MB，覆盖 ~5 分钟窗口）
- 超出窗口的旧 ID 自动淘汰，不影响高频交易
- 滑窗大小超过 Kafka rebalance 时间窗口，确保 rebalance 期间的重复消息被拦截

### 2. Periodic Checkpoint（防 Crash 丢失）

```
每 5 秒 → 遍历所有 shard → 收集非空 bucket → REPLACE INTO 写入 TiDB
```

| 场景 | 结果 |
|------|------|
| 实例 crash | 最多丢 5 秒的 open kline 数据 |
| 窗口关闭时 crash | REPLACE INTO 覆盖旧 checkpoint |
| 无数据时 crash | TradeCount==0 跳过，不写空行 |

### 2. 重启恢复

首个 Trade 到达某交易对时自动恢复：
```
recoverLocked:
  for 7 intervals:
    LoadKline(symbol, interval, currentWindowStart)
    命中 → 作为 bucket 初始状态
    未命中 → 从零开始
```

### 3. Continuity Check（后台数据一致性校验）

Leader 实例上每 10 分钟运行：

| 检查对象 | 检测方式 | 修复方式 |
|---------|---------|---------|
| 1m K 线 | 相邻 start_time 连续性 | 仅日志告警 |
| 10m/30m/1h K 线 | 扫描预期窗口是否存在 | 从 1m 数据 Re-aggregate 并 REPLACE |

```
缺失 10:00~10:09 的 10m 窗口
  → Query 1m klines WHERE st>=10:00 AND st<10:10
  → mergeOneMKlines: Open=首条, High=max, Low=min, Close=末条
  → BatchSave → REPLACE INTO
```

### 4. Final Flush on Shutdown

收到 SIGINT/SIGTERM 时：
1. 取消 checkpoint goroutine
2. 遍历所有 shard 的所有 open bucket
3. 非空 bucket 入队 flush worker
4. 等待 500ms drain
5. 关闭连接

---

## 高可用设计

### etcd Leader Election

多实例部署下，通过 etcd `concurrency.Mutex` 选主：

```
所有实例 → 对 /quote-ticker/leader 争夺分布式锁
   ├── 持有锁 = Leader → 写 DB + Continuity Check
   │                   ← 5s lease，异常断开后自动释放
   └── 未持有 = Follower → 仅消费 + WS 推送
                           ← mutex.Lock() 阻塞等待，锁释放后立即抢锁
```

| 角色 | Kafka 消费 | WS 推送 | DB 写入 | Checkpoint | Continuity |
|------|-----------|---------|---------|-----------|-----------|
| Leader | ✅ | ✅ | ✅ | ✅ | ✅ |
| Follower | ✅ | ✅ | ❌ | ❌ | ❌ |

Leader 切换时：
- etcd session lease TTL = 5s（Leader crash 后最多 5s 完成切换）
- 新 Leader 的首个 Trade 自动触发 `recoverLocked`，从 DB 恢复 checkpoint
- 旧 Leader 的 etcd lease 自动过期，锁被释放

### 单实例模式

```yaml
etcd:
  enabled: false    # 默认关闭，退化单实例
```

`leaderFn` 恒返回 `true`，所有 DB 操作正常运行。

---

## 数据分表设计

### 按交易对分表

```
t_kline_{symbol}  例如：t_kline_btcusdt, t_kline_ethusdt, t_kline_solusdt
```

### 表结构

```sql
CREATE TABLE IF NOT EXISTS t_kline_btcusdt (
    iv     VARCHAR(8)      NOT NULL COMMENT 'interval',
    st     BIGINT          NOT NULL COMMENT 'start_time(ms)',
    ct     BIGINT          NOT NULL COMMENT 'close_time(ms)',
    o      DECIMAL(40,20)  NOT NULL COMMENT 'open',
    h      DECIMAL(40,20)  NOT NULL COMMENT 'high',
    l      DECIMAL(40,20)  NOT NULL COMMENT 'low',
    c      DECIMAL(40,20)  NOT NULL COMMENT 'close',
    v      DECIMAL(40,20)  NOT NULL DEFAULT 0,
    q      DECIMAL(40,20)  NOT NULL DEFAULT 0,
    n      INT UNSIGNED    NOT NULL DEFAULT 0,
    bv     DECIMAL(40,20)  NOT NULL DEFAULT 0,
    bq     DECIMAL(40,20)  NOT NULL DEFAULT 0,
    wavg   DECIMAL(40,20)  NOT NULL DEFAULT 0,
    created_at BIGINT      NOT NULL DEFAULT 0,
    updated_at BIGINT      NOT NULL DEFAULT 0,
    PRIMARY KEY (iv, st) CLUSTERED
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_bin
```

- 主键 `(iv, st)` 支持聚簇索引，时间范围查询走 PK Scan
- TiDB 自动按 Region 分片，无需手动分区
- 新交易对由 `TableManager` 在 Kafka 消费到时自动 `CREATE TABLE IF NOT EXISTS`

### 7 种 K 线周期

| Interval | 窗口宽度 | 对齐方式 |
|----------|---------|---------|
| 1m | 60s | 整分钟 |
| 10m | 600s | 整 10 分钟 |
| 30m | 1800s | 整 30 分钟 |
| 1h | 3600s | 整小时 |
| 1w | 7d | 周一 00:00 UTC |
| 1mon | 日历月 | 每月 1 日 00:00 UTC |
| 1y | 日历年 | 1 月 1 日 00:00 UTC |

---

## API 参考

### HTTP

| 方法 | 路径 | 说明 |
|------|------|------|
| GET | `/api/klines?symbol=X&interval=X&startTime=&endTime=&limit=` | 查询 K 线 |
| GET | `/api/kline/{symbol}/{interval}?startTime=&endTime=&limit=` | 同上（path 风格） |
| GET | `/health` | 健康检查 |
| GET | `/metrics` | Prometheus 监控指标 |
| WS | `/ws` | WebSocket |

### WebSocket 订阅

```json
// 请求
{"action": "subscribe", "symbol": "BTCUSDT"}
{"action": "unsubscribe", "symbol": "BTCUSDT"}

// 推送
{"type": "trade", "data": {"Symbol":"BTCUSDT","TradeID":12345,...}}
```

---

## 配置

```yaml
server:
  port: 8082

kafka:
  brokers:
    - localhost:9092
  topic: match-result
  group_id: quote-ticker-group
  parallel: 4                    # 消费 worker 数，建议 = partition 数

database:
  dsn: "root:@tcp(localhost:4000)/quote_ticker?charset=utf8mb4&parseTime=true&loc=UTC"

etcd:
  servers:
    - localhost:2379
  path: /quote-ticker/leader
  enabled: false                 # 多实例时改为 true

continuity:
  check_interval: 10m
```

---

## 监控指标

`GET /metrics` 暴露 Prometheus 格式的指标，包含：

### 业务指标

| 指标 | 类型 | 标签 | 说明 |
|------|------|------|------|
| `quote_ticker_kafka_messages_total` | Counter | `status` (received/processed/deduped/error) | Kafka 消息处理计数 |
| `quote_ticker_kafka_processing_duration_seconds` | Histogram | — | 单条消息处理耗时（1µs~16ms） |
| `quote_ticker_trades_total` | Counter | `symbol` | 各交易对接收的成交笔数 |
| `quote_ticker_klines_written_total` | Counter | `interval` | 写入 DB 的 K 线数 |
| `quote_ticker_processing_duration_seconds` | Histogram | `symbol` | 单笔成交多 interval 更新耗时（1µs~16ms） |
| `quote_ticker_open_buckets` | Gauge | `symbol`, `interval` | 当前内存中 open bucket 的 trade 数 |
| `quote_ticker_ws_connections` | Gauge | — | 当前 WebSocket 连接数 |
| `quote_ticker_ws_subscriptions` | Gauge | `symbol` | 各交易对订阅数 |
| `quote_ticker_leader` | Gauge | — | 1=Leader, 0=Follower |
| `quote_ticker_checkpoint_duration_seconds` | Histogram | — | 一次 checkpoint flush 耗时（100µs~1.6s） |
| `quote_ticker_checkpoint_klines` | Summary | — | 每次 checkpoint 写入的 kline 数 |

### Go Runtime 指标（prometheus/client_golang 内置）

| 指标 | 说明 |
|------|------|
| `go_goroutines` | Goroutine 数量 |
| `go_memstats_alloc_bytes` | 当前堆分配 |
| `go_memstats_heap_objects` | 堆对象数 |
| `go_gc_duration_seconds` | GC 暂停耗时 |
| `process_cpu_seconds_total` | 进程 CPU 时间 |
| `process_resident_memory_bytes` | 常驻内存 RSS |

### 告警建议

| 条件 | 说明 |
|------|------|
| `rate(kafka_messages_total{status="error"}[5m]) > 0` | Kafka 消费异常 |
| `leader == 0` | 无 Leader（etcd 模式） |
| `go_goroutines > 50000` | Goroutine 泄漏 |
| `process_resident_memory_bytes > 2e9` | 内存超过 2GB |
| `rate(checkpoint_duration_seconds_sum[5m]) / rate(checkpoint_duration_seconds_count[5m]) > 1` | Checkpoint 写入过慢 |

---

## 项目结构

```
├── cmd/server/main.go              # 入口
├── config.yaml                     # 配置
├── proto/matching.proto            # PbTradeTick 定义
├── internal/
│   ├── config/config.go            # 配置解析
│   ├── decimal/decimal.go          # int64 定点数
│   ├── model/
│   │   ├── clock.go                # 时间工具
│   │   ├── kline.go                # K 线领域模型
│   │   ├── trade.go                # 成交模型
│   │   └── pb/matching.pb.go       # Proto 生成代码
│   ├── kline/
│   │   ├── interval.go             # 7 种周期定义
│   │   ├── aggregator.go           # K 线聚合引擎
│   │   └── continuity.go          # 连续性检测 + 补数
│   ├── repository/
│   │   ├── table_manager.go        # 自动建表
│   │   └── kline_repo.go          # TiDB 读写
│   ├── kafka/consumer.go           # Kafka 消费 (protobuf)
│   ├── elector/elector.go          # etcd 选主
│   ├── ws/hub.go                   # WebSocket 推送
│   └── api/
│       ├── handler.go              # HTTP 查询
│       └── router.go               # 路由
└── go.mod / go.sum
```
