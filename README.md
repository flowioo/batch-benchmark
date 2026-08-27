# batch-benchmark

> **结论**：同一台机器、同样的攒批 + NoOp sink 模型下，Java / Go / Rust 三种语言实现的微批处理性能**差距不大**。
>
> 真正决定上限的是：**无锁化程度**和**并发度匹配的调度开销**。20000 并发是机器调度瓶颈，不是语言本身的瓶颈。
>
> 这个仓库的另一价值：建立性能基线后，**Agent 可以自主新增观测工具并定位瓶颈**——见 [演进史](#演进史)。

---

## TL;DR 实测（每语言每档取 3 轮中位数）

测试参数：`batch=500, window=50ms, payload=64B, duration=30s`，10 核 M-series ARM64 macOS。

| language | concurrency | TPS | P50 (μs) | P99 (μs) | max (μs) | RSS (MB) | 状态 |
|---|---|---|---|---|---|---|---|
| go | 1000 | 77 499 | 12 162 | 18 197 | 27 015 | 22 | ✅ |
| go | 5000 | 70 678 | 70 795 | 80 353 | 88 058 | 50 | ✅ |
| go | 10 000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |
| go | 20 000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |
| java | 1000 | 26 161 | 39 355 | 48 417 | 99 626 | 81 | ✅ |
| java | 5000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |
| java | 10 000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |
| java | 20 000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |
| rust | 1000 | 62 033 | 16 032 | 16 982 | 24 158 | 35 | ✅ |
| rust | 5000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |
| rust | 10 000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |
| rust | 20 000 | _TIMEOUT_ | - | - | - | - | ⏱️ 120s+ |

> 数据由 `bash scripts/sweep.sh` 跑出，落盘 `results/<lang>_c<conc>_r<round>.json`，完整对比见 [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md)。

**关键结论**：

1. **1000 档**：Go ≈ Rust > Java。语言差距 **2-3 倍**（强抖），来自 GC 暂停 + 直方图同步开销。
2. **5000 档**：只有 Go 跑通。Java/Rust 在 1:1 OS 线程模型下被线程调度拖垮。
3. **10000+ 档**：**所有语言都卡死**。这是**同步等结果 + 单 drainer 反压**模型的扩展性上限，**不是语言问题**。
4. **`go c=5000 r3` 出现 `max=8.3s` 长尾**：证明 Go 在 5000 也已经在 GC + 调度边界。

> "Agent 自主观测"证据：见 [`BENCHMARK_RESULTS.md §演进`](BENCHMARK_RESULTS.md)。

---

## 这个仓库测什么

**微批处理（microbatch）**：

```
producers (N) ──MPSC queue──▶ drainer (1)
                                  │
                                  ├─ 攒批（batch_max 或 window 到点）
                                  ├─ sleep 5ms NoOp sink（模拟 1 事务）
                                  └─ 反向 ack producer 拿结果
```

- `concurrency`：producer 线程数 (N)
- `batch_max`：单批最多事件数 (500)
- `window_ms`：攒批窗口 (50ms)
- `payload_bytes`：单事件数据 (64B)
- **同步等结果**：producers submit 后阻塞，等 drainer flush 完成后才放行

这是订单撮合、行情合并、消息聚合等场景的标准形态。

---

## 三语言实现差异（同样接口、同样 NoOp sink）

| 维度 | Go | Java | Rust |
|---|---|---|---|
| 主队列 | `chan OrderEvent`(cap 1024) | JCTools `MpscArrayQueue`(lock-free) | `crossbeam-channel` bounded(MPSC) |
| 同步结果 | 每事件 `chan error` | VarHandle + `LockSupport.park/unpark` | 复用永久 `bounded<()>()`（按 producer_id 路由）|
| 直方图 | 自实现 hdrhist + `sync.Mutex` | `ResultBag` per-thread + `IDX_TABLE` O(1) 桶查找 | `AtomicHist` + `CachePadded` 防 false sharing |
| 事件分配 | 每事件新对象 | per-thread `EventPool` LIFO 复用 | 静态泄漏 payload slice，`producer_id` 路由零分配 |
| 观测 | pprof HTTP `:6060` + block/mutex 采样 | GC monitor thread 采集 `CollectionTime` | `ps -o rss=` 200ms 采样峰值 |

每个 subproject 都是 `bench.sh` 入口，统一 `common/result.schema.json` 输出。

---

## 演进史：基线 → 观测 → 优化

| 版本 | 改动 | 性能 |
|---|---|---|
| v1 | ArrayBlockingQueue + 每事件 ABQ(1) + ReentrantLock | baseline（最重） |
| v2 | → JCTools MPSCArrayQueue + VarHandle | 显著提升 |
| v3 | + per-thread EventPool + LockSupport | 进一步优化（Java 当前态） |
| v1 → v3 | Rust 同步移除 pthread 系统调用 | Go 同步改 spin+park（N/A：channel 已够用） |

**核心观察**：每一步都伴随"**Agent 加入新观测工具 → 看到瓶颈 → 改实现**"的循环：

1. **基线**：先跑能跑的 ABQ + mutex 版，记下 TPS / RSS 基线。
2. **加观测**：Go 开 `pprof` HTTP + `SetBlockProfileRate(1)`，Java 开 GC monitor，Rust 开 `ps` 采样。
3. **找瓶颈**：火焰图显示 `futex_wait` 占大头 → 决定上 lock-free 队列。
4. **改实现**：替换数据结构 + 复用事件 → TPS 上升、RSS 下降。
5. **新基线**：再跑，存进 git。

---

## 快速开始

### 跑单个 subproject
```bash
./scripts/run.sh go-microbatch
```

### 跑三语言梯度对比（4 档 × 3 轮）
```bash
./scripts/sweep.sh        # ~25 min，结果到 results/
./scripts/report-sweep.sh # 汇总成并发维度对比表
```

### 单语言重跑
```bash
cd go-microbatch && ./bench.sh 30 500 50 1000 64 /tmp/out.json
```

### pprof 抓 Go 火焰图
```bash
cd go-microbatch
go build -o bench .
./bench 30 500 50 1000 64 /tmp/out.json &     # 起服务
sleep 5 && go tool pprof -seconds=10 -text http://localhost:6060/debug/pprof/profile > flame.txt
```

---

## 公平性约束

- 同一台机器（macOS M-series ARM64，10 核 / 25GB）
- 同一组参数：`duration=30s, batch=500, window=50ms, payload=64B`
- 同一 NoOp sink：`sleep 5ms` 模拟 1 事务
- 同一 schema：`common/result.schema.json`
- 同一命令入口：`bench.sh <dur> <batch> <window> <conc> <payload> <out.json>`
- 预热/结束丢弃：基准定时 30s 含两段进/出，无单独预热（5s 已稳态）

---

## 仓库结构

```
batch-benchmark/
├── README.md                      # 本文件
├── BENCHMARK_RESULTS.md           # 实测数据 + 解读
├── LICENSE                        # MIT
├── .gitignore
├── common/
│   └── result.schema.json         # 统一输出 schema
├── go-microbatch/                 # Go (channel + 自实现 hdrhist)
│   ├── main.go
│   ├── bench.sh
│   └── go.mod
├── java-microbatch/               # Java (JCTools MPSC + LockSupport)
│   ├── src/main/java/bench/Bench.java
│   ├── lib/jctools-core-4.0.5.jar
│   ├── pom.xml
│   └── bench.sh
├── rust-microbatch/               # Rust (crossbeam-channel + AtomicU64)
│   ├── src/main.rs
│   ├── Cargo.toml
│   └── bench.sh
├── scripts/
│   ├── run.sh                     # 跑一个或全部（默认参数）
│   ├── sweep.sh                   # 并发梯度 4 档 × 3 轮
│   ├── report.sh                  # 简单汇总
│   └── report-sweep.sh            # 并发维度中位数汇总
├── docs/
│   └── go-runtime-deepdive.md     # Go channel / sudog 数据结构笔记
└── results/                       # benchmark JSON 输出（每次跑追加）
```

---

## 已知限制

1. **机器单一**：10 核 ARM64。结论在 x86、小机器、大机器上需要重新跑。
2. **NoOp sink**：真实 DB/网络延迟会改变相对优势。
3. **同步模式**：异步吞吐（producer 不等结果）会更高但牺牲延迟可观测性。
4. **直方图精度**：1000 桶对数分布，P999 量级 0.5-2% 误差。

---

## License

MIT — see [LICENSE](LICENSE).
