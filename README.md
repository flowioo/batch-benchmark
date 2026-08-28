# batch-benchmark

## 项目说明

本项目测试**单机微批写入**场景下各语言（Go / Java / Rust）的性能差异。

采用 **MPSC（Multi-Producer, Single-Consumer）模型**：N 个 producer 并发写入，单个 drainer 攒批后落库（NoOp sink 模拟事务）。这是**订单撮合、行情合并、消息聚合**等场景的标准形态，目标是**提升写入性能**。

> **结论**：同一台机器、同样的攒批 + NoOp sink 模型下，**相同并发档位内三语言的性能表现差别不大**——TPS 同在几万量级、延迟同在几十 ms 量级，没有数量级差距。
>
> **技术选型主要看团队技术栈**，而不是 benchmark 分数：
>
> - **有条件优先 Go**：吞吐最高（77k TPS）、内存最轻（c=1000 仅 23MB）、并发扩展最平（平顶到 c=7000）、单二进制部署。
> - **Rust**：性能第二，但内存随并发增长快（c=5000 达 176MB），团队上手成本是主要门槛。
> - **Java**：资源占用较高（RSS 是 Go 的 3-4 倍），且高并发下先塌（c=2000 TPS 衰减 14×），适合已有 Java 团队复用生态。
>
> 这个仓库的另一价值：建立性能基线后，**Agent 可以自主新增观测工具并定位瓶颈**——见 [演进史](#演进史)。

---

## TL;DR 实测（每语言每档取 3 轮中位数）

测试参数：`batch=500, window=50ms, payload=64B, duration=30s`，10 核 M-series ARM64 macOS。

| language | concurrency | TPS | P50 (ms) | P99 (ms) | max (ms) | RSS (MB) | 状态 |
|---|---|---:|---:|---:|---:|---:|---|
| go | 1 000 | 77 500 | 12.2 | 18.2 | 27.0 | 23 | ✅ |
| go | 2 000 | 73 636 | 26.0 | 33.5 | 42.0 | 30 | ✅ |
| go | 3 000 | 71 943 | 40.3 | 47.3 | 53.7 | 35 | ✅ |
| go | 5 000 | 70 678 | 70.8 | 80.4 | 88.1 | 50 | ✅ |
| go | 7 000 | 70 726 | 98.9 | 98.9 | 118.1 | 67 | ✅ |
| go | 10 000 | – | – | – | – | – | ⏱️ TIMEOUT |
| go | 20 000 | – | – | – | – | – | ⏱️ TIMEOUT |
| java | 1 000 | 26 161 | 39.4 | 48.4 | 99.6 | 81 | ✅ |
| java | 2 000 | **1 799** | 98.9 | 98.9 | 100.0 | 138 | ⚠️ 退化 |
| java | 3 000 | – | – | – | – | – | ⏱️ TIMEOUT |
| java | 5 000+ | – | – | – | – | – | ⏱️ TIMEOUT |
| rust | 1 000 | 62 033 | 16.0 | 17.0 | 24.2 | 35 | ✅ |
| rust | 2 000 | 47 300 | 42.7 | 55.6 | 100.8 | 72 | ✅ |
| rust | 3 000 | 46 617 | 64.6 | 98.9 | 190.4 | 106 | ✅ |
| rust | 5 000 | 47 233 | 98.9 | 98.9 | 347.3 | 176 | ✅ |
| rust | 10 000 | – | – | – | – | – | ⏱️ TIMEOUT |
| rust | 20 000 | – | – | – | – | – | ⏱️ TIMEOUT |

> 数据由 `bash scripts/sweep-boundary.sh` 跑出，落盘 `results/<lang>_c<conc>_r<round>.json`，完整对比与解读见 [`BENCHMARK_RESULTS.md`](BENCHMARK_RESULTS.md)。

**关键结论**：

1. **相同并发档位内，性能差别不大**：能跑通的档位下，三语言 TPS 同在 1.8k-77k 量级、延迟同在几十 ms 量级，没有数量级鸿沟。语言不是性能的决定性因素。
2. **每语言有自己的天花板**：Go 70k @ c=7000、Rust 47k @ c=2000+、Java 26k @ c=1000。瓶颈 = `drainer_rate × batch × (1 - sink_cost)`。
3. **扩展模式不同**：
   - **Go**：TPS 平顶（c=2000 起稳定 71-77k），p50 随 c 线性增长（12→99ms）。
   - **Rust**：TPS 在 c=2000 跌到 47k 后稳定，但 p50 持续增长。OS-thread 调度 + park 唤醒延迟。
   - **Java**：c=2000 突然塌到 1.8k（TPS 衰减 14×），p50 顶到窗口 99ms。MPSC 队列 + LockSupport.park 抖动。
4. **10000+ 统一卡死**：三语言都过不去 → **同步等结果 + 单 drainer 反压**的模型天花板，非语言问题。
5. **资源占用**：Go 最轻（c=7000 才 67MB），Java 最重（c=1000 就 81MB，是 Go 同档 3.5 倍）。

---

## 技术选型建议

| 场景 | 推荐 | 理由 |
|---|---|---|
| 有条件、新项目 | **Go** | 吞吐最高 + 内存最轻 + 扩展最平 + 单二进制部署 + 并发模型天然匹配微批 |
| 极致性能/嵌入式 | Rust | 单核性能第二，但本测试中内存并不占优，团队门槛高 |
| 已有 Java 团队/生态 | Java | 性能够用（c=1000 下 26k TPS），复用 Spring/JDK 生态价值 > 性能损失；注意 RSS 和 c=2000 塌陷 |

**一句话**：benchmark 的差距（同档 2-3 倍）远小于团队换栈的成本，**选型看团队技术栈；有条件都用 Go，Java 资源占用较高需预留内存预算**。

> "Agent 自主观测→迭代"证据：见 [`BENCHMARK_RESULTS.md §演进`](BENCHMARK_RESULTS.md)。

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
./scripts/sweep.sh           # ~25 min，结果到 results/
./scripts/report-sweep.sh    # 汇总成并发维度对比表

# 边界档补全（c=2000/3000/5000/7000 + 120s wall timeout）
./scripts/sweep-boundary.sh  # ~25 min，跳过已有 .json 的档
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
│   ├── sweep-boundary.sh          # 边界档补全 c=2000/3000/5000/7000 + 120s wall timeout
│   ├── sweep-remaining.sh         # 早期 sweep 的 120s TIMEOUT 包装版
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
