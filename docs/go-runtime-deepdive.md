# Go Runtime 深度：Channel / G 栈 / Goroutine 数据结构

时间: 2026-08-26

> 注意：`session_default.md` 已被覆盖为 Java 优化方案，本文件单独保存 Go runtime 讨论。

---

# 第一部分：Go Channel 设计

## 1. 设计目标
- goroutine 间通信 + 同步（"share memory by communicating"）
- 类型安全、编译期检查
- select 多路复用
- 高性能：低延迟、高并发、可伸缩

## 2. 数据结构（runtime/chan.go）

```go
type hchan struct {
    qcount   uint           // 当前队列中元素数
    dataqsiz uint           // 环形队列容量（make 时指定）
    buf      unsafe.Pointer // 环形队列指针
    elemsize uint16         // 单元素大小
    closed   uint32         // 是否已 close
    elemtype *_type         // 元素类型（用于 copy）
    sendx    uint           // 发送 index
    recvx    uint           // 接收 index
    recvq    waitq          // 等待 recv 的 G 队列（sudog 链表）
    sendq    waitq          // 等待 send 的 G 队列
    lock     mutex          // 保护 chan 的全局锁
}

type waitq struct {
    first *sudog
    last  *sudog
}

type sudog struct {
    g          *g
    next       *sudog
    prev       *sudog
    elem       unsafe.Pointer // 指向 G 栈上 elem 槽位
    ticket     uint32         // select 公平性
    isSelect   bool
    success    bool           // channel close 标志
    parent     *sudog         // select 内部链表
    c          *hchan
}
```

**关键**：`recvq`/`sendq` 存的是**指向 G 栈上 elem 槽位**的指针，recv 时直接**从 sender 栈 copy 到 receiver 栈**，零中间分配。

## 3. 创建 makechan

```go
func makechan(t *chantype, size int) *hchan {
    c := (*hchan)(hchan_malloc(mem))  // hchan + buf 一次 malloc
    c.buf = add(unsafe.Pointer(c), hchanSize)
    c.elemsize = uint16(elem.Size_)
    c.elemtype = elem
    c.dataqsiz = uint(size)
    return c
}
```

hchan + buf 一次分配。size==0 不分配 buf。

## 4. 发送 chansend1 → chansend

```go
func chansend1(c *hchan, elem unsafe.Pointer) {
    chansend(c, elem, true, getcallerpc())
}

func chansend(c *hchan, ep unsafe.Pointer, block bool, callerpc uintptr) bool {
    // 1. 快速路径：无锁检查 → 锁 → copy 到 buf[sendx] → unlock
    if c.qcount < c.dataqsiz { return true }

    // 2. 同步匹配（不经过 buf）
    if !c.closed && c.qcount == 0 && c.recvq.first != nil {
        sendDirect(c.elemtype, c, sg, ep)  // 栈到栈 copy
        goready(gp, skip+1)
        return true
    }
    // 3. close → panic
    if c.closed != 0 { panic("send on closed channel") }

    // 4. 阻塞：挂到 sendq → gopark
    mysg := acquireSudog()
    mysg.elem = ep
    c.sendq.enqueue(mysg)
    gopark(...)
    return mysg.success
}
```

### sendDirect 栈到栈拷贝

```go
func sendDirect(t *_type, c *hchan, sg *sudog, ep unsafe.Pointer) {
    // sender 上下文不能直接写 receiver 栈
    // 借用 c.buf[0] 作中转（_type 对齐保证）
    memmove(chanBuf(c, 0), ep, t.Size_)       // buf[0] = *ep
    memmove(sg.elem, chanBuf(c, 0), t.Size_)  // receiver 栈 = buf[0]
}
```

**为什么先到 buf[0]**：sendDirect 在 sender 上下文执行，跨 G 写栈不安全，借 buf[0] 中转。

## 5. 接收 chanrecv1

```go
func chanrecv(c *hchan, ep unsafe.Pointer, block bool) (selected, received bool) {
    if c.qcount > 0 {
        // copy buf[recvx] → *ep → recvx++ → qcount-- → unlock
        return true, true
    }
    if c.closed != 0 {
        if ep != nil { typedmemclr(c.elemtype, ep) }  // 返回零值
        return true, false
    }
    if sg := c.sendq.first; sg != nil {
        recvDirect(c.elemtype, c, sg, ep)  // 同步传递
        goready(sg.g, skip+1)
        return true, true
    }
    // 阻塞 → 挂到 recvq
}
```

## 6. close closechan

```go
func closechan(c *hchan) {
    if c == nil { panic("close of nil channel") }
    if c.closed != 0 { panic("close of closed channel") }
    c.closed = 1
    // 唤醒所有 recvq（让它们收到零值）
    for sg := c.recvq.dequeue(); sg != nil; sg = c.recvq.dequeue() {
        sg.elem = nil
        sg.success = false
        goready(sg.g, 3)
    }
    // 唤醒所有 sendq（让它们 panic）
    for sg := c.sendq.dequeue(); sg != nil; sg = c.sendq.dequeue() {
        sg.success = false
        goready(sg.g, 3)
    }
}
```

## 7. select selectgo

```go
func selectgo(cas0 *scase, order0 *uint16, ncases int) (int, bool) {
    // 1. 第一遍：乱序扫所有 case，看有无就绪
    for i := range cas0 {
        switch casi.kind {
        case caseRecv:
            if c.qcount > 0 || c.closed != 0 { return i, true }
        case caseSend:
            if c.qcount < c.dataqsiz { return i, true }
        }
    }
    // 2. 乱序把所有 sudog 挂到各 chan 等待队列
    //    ticket + isSelect 用于多 case 公平性
    // 3. gopark → 当前 G 挂起
    // 4. 醒来后：cas0[i].success 判断哪个 case 被选中
    // 5. cleanup：所有 sudog 从各 chan 摘下
}
```

**公平性**：`fastrandn(ncases)` 伪随机选 case，避免饥饿。

**多 case 唤醒**：sudog.parent 链 select 内部所有 sudog，ticket 全局计数器保证多 chan 同时就绪时只 1 个 case 真正触发。

## 8. 关键设计点

### 8.1 sudog 池化
- 每次 park 新建 sudog 太贵（GC 压力）
- runtime 维护全局 sudog pool + P 本地池
- select 临时用完立即归还

### 8.2 同步传递 synchronous hand-off
- sender + receiver 同时就绪，**不经过 buf**，栈到栈 copy
- 节省一次 memory copy + 一次 cache 失效
- unbuffered chan 比看起来快的真正原因

### 8.3 锁粒度
- 一个 chan 一把 mutex
- 争用激烈时退化为 futex
- **经验**：高 TPS 通信用 buffered channel，锁竞争小

### 8.4 nil channel 语义
- `var c chan int; c <- 1` 永久阻塞
- `var c chan int; <-c` 永久阻塞
- `var c chan int; close(c)` panic
- **用途**：select 里用 nil channel 动态禁用某个 case

```go
ch2 = nil  // 禁用
select {
case x := <-ch1: ...
case ch2 <- y: ...  // 永久阻塞，跳过
}
```

## 9. 性能数据

| 场景 | 延迟 | 备注 |
|---|---|---|
| buffered chan send/recv（不阻塞） | ~50-100ns | fast path，几乎无锁 |
| unbuffered chan send/recv（配对） | ~150-300ns | mutex + park/wake |
| select (2 case 就绪) | ~200-400ns | fastrandn 选 case |
| select (N case 全阻塞) | ~500-1000ns | 创建 N sudog + park |

---

# 第二部分：G 栈

## 1. 是什么

G 栈 = goroutine 专用的、连续、动态可增长的调用栈。

```go
type g struct {
    stack       stack
    stackguard0 uintptr
    stackguard1 uintptr
    // ...
}

type stack struct {
    lo uintptr   // 栈底（低地址）
    hi uintptr   // 栈顶（高地址）
}
```

## 2. 关键特征

1. **每个 goroutine 一个独立栈**（不共享）
2. **初始 2KB**，按需增长，最大 1GB（64-bit）
3. **连续内存**：lo..hi 一段连续虚拟地址
4. **栈分裂**：2KB → 4KB → 8KB → ... 翻倍增长

## 3. 栈增长 morestack

```go
// 函数 prologue
if SP < stackguard0 → runtime.morestack_noctxt
                        → 分配 2 倍大小新栈
                        → memmove 旧栈内容到新栈
                        → 调整 stack.lo/hi
                        → 跳回函数
```

**Go runtime 的核心魔法**：让 goroutine 轻量（2KB 起）又能调用深递归。

## 4. 跟 OS 线程栈对比

| 维度 | G 栈 | OS 线程栈 |
|---|---|---|
| 大小 | 2KB~1GB，动态 | 固定 8MB (Linux) |
| 数量 | 几万~百万 | 几千到瓶颈 |
| 调度 | 用户态 | 内核 |
| 切换 | 几条 asm 指令 | syscall 进出 |

---

# 第三部分：Goroutine 数据结构（G/M/P）

## 1. 三种核心对象

| 类型 | 含义 | 数量级 |
|---|---|---|
| **G (goroutine)** | 用户态协程 | 几万~百万 |
| **M (machine)** | OS 线程 | = GOMAXPROCS |
| **P (processor)** | 逻辑处理器，调度上下文 | = GOMAXPROCS |

**关系**：`M` 必须持有 `P` 才能执行 `G`。

## 2. G 结构（src/runtime/runtime2.go）

```go
type g struct {
    stack       stack        // 内存栈：lo..hi
    stackguard0 uintptr
    stackguard1 uintptr

    _panic    *_panic
    _defer    *_defer
    m         *m             // 当前关联的 M
    sched     gobuf          // 调度上下文（PC/SP/regs）
    atomicstatus atomicuint32 // _Gidle/_Grunnable/_Grunning/_Gsyscall/_Gwaiting/_Gdead
    goid      int64
    schedlink guintptr       // 下一个 G（runqueue 链表）
    waitreason string
    preempt   bool
    parkingOnChan atomic.Bool

    schedwhen int64
    syscallsp uintptr
    syscallpc uintptr

    timer     *timer

    param     unsafe.Pointer
    gopc      uintptr
    ancestors *[]ancestorInfo

    msignal   *m
    sig       uint32
    sigcode0  uintptr
    sigcode1  uintptr
    sigpc     uintptr

    gstart    func()

    racectx   uintptr
    waiting   *sudog
}
```

### gobuf 调度上下文

```go
type gobuf struct {
    sp   uintptr
    pc   uintptr
    g    guintptr
    ctxt unsafe.Pointer
    ret  uintptr
    bp   uintptr
}
```

**G 切换本质** = 保存当前 gobuf → 加载目标 gobuf → jmp 到目标 sp/pc。

## 3. 状态机 atomicstatus

```go
const (
    _Gidle      = iota  // 0: 刚分配，未初始化
    _Grunnable          // 1: 在 runqueue 等执行
    _Grunning           // 2: 正在 M 上跑
    _Gsyscall           // 3: 在 syscall
    _Gwaiting           // 4: 阻塞（chan、select、lock）
    _Gmoribund          // 5: 不可恢复
    _Gdead              // 6: 已退出，可复用
    _Genqueue_wait      // 7
    _Gcopystack         // 8
    _Gpreempted         // 9: 被抢占
)
```

## 4. M 结构（OS 线程）

```go
type m struct {
    g0      *g              // 调度栈 G（每个 M 一个，固定 8KB）
    curg    *g              // 当前跑的 G
    p       puintptr        // 关联的 P
    nextp   puintptr
    oldp    puintptr
    id      int64

    parked  bool
    park    note            // futex 用的 note
    alllink *m
    schedlink muintptr

    gsignal *g              // 信号处理 G
    tls     [tlsSlots]uintptr
    mstartfn func()
}
```

**g0 栈**：M 的固定 8KB 栈，runtime 调度自己用（gopark/goready 切到 g0）。

## 5. P 结构（逻辑处理器）

```go
type p struct {
    id      int32
    status  uint32          // _Pidle/_Prunning/_Psyscall/_Pgcstop/_Pdead
    m       muintptr
    mcache  *mcache         // per-P 内存缓存

    runq     [256]guintptr  // 本地 runqueue（数组，固定 256）
    runqhead uint32
    runqtail uint32
    runnext  guintptr       // 下一个优先跑的 G（无锁 steal）

    gFree gList

    preempt bool

    timers *timersBucket    // per-P timer 堆
}
```

**本地 runqueue 固定 256**，CAS 头尾指针做无锁 push/pop。**满了丢 1/2 到全局 queue**。

## 6. schedt 全局调度器

```go
type schedt struct {
    lock mutex
    midle        muintptr  // 空闲 M 链表
    nmidle       int32
    pidle        puintptr  // 空闲 P 链表
    npidle       uint32
    runq     gQueue        // 全局 runqueue
    runqsize int32
    allgs        []*g
    sudoglock  mutex
    sudogcache *sudog
}

type gQueue struct {
    head guintptr
    tail guintptr
}
```

**全局变量**：`var sched schedt`、`var allp []*p`、`var allm []*m`。

## 7. G 生命周期

### 7.1 创建 go func()

```go
func newproc(siz int32, fn *funcval) {
    newg := gfget(_g_.m.p)  // 从 gFree 链拿（复用优先）
    casgstatus(newg, _Gdead, _Grunnable)
    newg.gopc = getcallerpc()
    newg.startpc = uintptr(fn)

    stackinit()                  // 分配 2KB 栈
    newg.stack = stack{lo: ..., hi: ...}
    newg.sched.sp = newg.stack.hi - siz
    newg.sched.pc = abi.FuncPCABIInternal(goexit) + ...
    newg.sched.g = guintptr(unsafe.Pointer(newg))

    runqput(_g_.m.p, newg, true)  // 放 P 的 runq
    wakep()
}
```

### 7.2 schedule

```go
func schedule() {
    gp := findrunnable()  // g0 栈上跑
    execute(gp, true)
}

func execute(gp *g, inheritTime bool) {
    gogo(&gp.sched)  // 汇编：恢复 sp/pc/regs
}
```

### 7.3 gopark（chan/lock 等待）

```go
func gopark(unlockf func(*g, unsafe.Pointer) bool, lock unsafe.Pointer, reason waitReason, ...) {
    casgstatus(gp, _Grunning, _Gwaiting)
    dropg()
    if unlockf != nil && !unlockf(gp, lock) {}
    schedule()  // 切走
}
```

### 7.4 goready（唤醒）

```go
func goready(gp *g, traceskip int) {
    casgstatus(gp, _Gwaiting, _Grunnable)
    runqput(_g_.m.p, gp, true)
    if _g_.m.p.ptr().nmspinning.Load() == 0 {
        wakep()
    }
}
```

## 8. Work Stealing

```go
func findrunnable() (gp *g) {
top:
    if gp := _g_.m.p.ptr().runq.pop(); gp != nil { return gp }  // 本地 runq

    if _g_.m.p.ptr().schedtick%61 == 0 {  // 每 61 次
        if gp := globrunqget(_g_.m.p, 1); gp != nil { return gp }
    }

    for i := 0; i < 4; i++ {  // 偷别的 P
        for enum := stealOrder.start; ...; enum = stealOrder.next(enum) {
            if gp := runqsteal(_g_.m.p, allp[enum], ...); gp != nil { return gp }
        }
    }

    stopm()  // 全部空 → park M
    goto top
}
```

**stealOrder** 伪随机洗牌数组，避免热点 P。

## 9. Sysmon（系统监控线程）

```go
func sysmon() {
    for {
        // 1. 抢占 G
        //    - retake：long syscall (>10ms) 抢 P
        //    - preemptall：长 G running (>10ms) 注入 SIGURG
        // 2. 强制 GC
        // 3. scan timers
        usleep(delay)
    }
}
```

**Go 1.14 异步抢占**：sysmon 注入 SIGURG → signal handler 调 `asyncPreempt` → 保存 PC 调 `schedule()`。

## 10. G 状态转换图

```
       newproc()
         ↓
       _Gdead → _Grunnable
                    ↓
                 _Grunning ←──────┐
                    ↓             │
        ┌──────────┼──────────┐   │
        ↓          ↓          ↓   │
    _Gsyscall  _Gwaiting  _Gpreempted
        ↓          ↓          ↓   │
        └──────────┴──────────┘   │
                    ↓             │
                 _Grunnable ──────┘
                    ↓ (goexit)
                 _Gdead
```

## 11. 性能数据

| 操作 | 延迟 |
|---|---|
| `go func()` 启动 | ~500ns |
| goroutine 上下文切换 | ~50-200ns（用户态） |
| 调度（schedule） | ~200ns |
| 唤醒（goready） | ~100ns |
| park + 唤醒（chan） | ~300-500ns |

vs OS 线程：
- 启动线程：~50-100μs
- 切换线程：~1-5μs

## 12. 关键优化

### 12.1 P 本地 runq
- 固定 256，无锁 CAS
- 大多数 G 调度无需争抢

### 12.2 work stealing
- 忙 P 上的 G 被闲 P 偷走
- 随机起点避免热点

### 12.3 异步抢占
- Go 1.14 前：依赖 G 主动让出
- 现在：sysmon 注入信号强制抢占
- 防止长 G 霸占 M

### 12.4 gFree 缓存
- G 退出后不立刻释放，放回 gFree 链
- 下次 `go func()` 优先复用（不 alloc）

### 12.5 per-P 资源
- `mcache`（per-P 内存缓存）
- `timers`（per-P timer 堆）

## 13. 调优 GOMAXPROCS

```go
runtime.GOMAXPROCS(N)  // 设 P 数
```

- **默认** = `runtime.NumCPU()`
- **I/O 密集型**：可略高于 CPU 数
- **CPU 密集型**：= CPU 数最理想

## 14. 经典踩坑

### Goroutine 泄漏
```go
// 错误：永远没人 send
for i := 0; i < 1000; i++ {
    go func() { ch <- 1 }()  // 1 个 send，其他 999 个永远阻塞
}
```
- 症状：pprof 看 goroutine 数 >1000，全是 channel send
- 修：context 取消 / done channel

### 限流模式
```go
sem := make(chan struct{}, N)  // 最多 N 个并发
for task := range tasks {
    sem <- struct{}{}
    go func(t Task) {
        defer func() { <-sem }()
        process(t)
    }(task)
}
```

## 15. 源码定位

- `src/runtime/runtime2.go` - g/m/p/schedt 定义
- `src/runtime/proc.go` - newproc/schedule/gopark/goready
- `src/runtime/chan.go` - channel 实现
- `src/runtime/stack.go` - 栈增长 morestack
- `src/runtime/asm_amd64.s` - gogo schedule 汇编入口
- `src/runtime/lock_futex.go` - futex 封装

## 16. 一句话总结
> Go runtime 三件套：**channel = 环形 buf + sudog + 栈到栈 copy**；**G 栈 = 2KB 动态增长**；**goroutine = G/M/P 三层抽象 + work stealing + async preemption**。让百万 goroutine 跑在几千线程上，用户态调度无 syscall 进出。

---

# 第四部分：Channel 加锁策略 & 无锁化

## 1. 加锁：是的（hchan.lock mutex）

```go
type hchan struct {
    qcount   uint
    dataqsiz uint
    buf      unsafe.Pointer
    sendx, recvx uint
    recvq, sendq waitq
    lock     mutex        // ← chan 锁
    // ...
}
```

## 2. 加锁策略：3 路径

```go
func chansend(c *hchan, ep unsafe.Pointer, block bool) bool {
    // 路径 1: fast path（无锁快查）
    if c.qcount < c.dataqsiz {  // atomic load
        return chansendbuf(c, ep)  // 内部才 lock
    }

    // 路径 2: 同步配对（lock 一次，栈到栈 copy）
    if !c.closed && c.qcount == 0 && c.recvq.first != nil {
        sendDirect(...)
    }

    // 路径 3: slow path（lock + sudog 入队 + gopark）
    mysg := acquireSudog()
    c.sendq.enqueue(mysg)
    gopark(...)
}
```

| 路径 | 锁次数 | 阻塞 | 用途 |
|---|---|---|---|
| fast path | 1 | 不 | 99% buffered send |
| 配对路径 | 1 | 不 | unbuffered send 匹配 recv |
| slow path | 2 | 是 | chan 满时 |

## 3. Go mutex 特殊性

Go runtime 自带 `runtime.mutex`，**不是 pthread_mutex**：

- 短持锁：自旋 + CAS（用户态，~10-50ns），**不进 syscall**
- 真正阻塞：fallback 到 futex（仅这时进内核）
- 自适应：探测对手是 spin 还是 park，动态调整策略

**跟 Java 的对比**：
- Java `ReentrantLock`：默认非公平，CAS + LockSupport.park（跟 Go 类似）
- Go mutex 更激进 spin，热点更优

## 4. 锁内临界区极小

send 临界区只做 3 件事：
1. memmove(elem → buf[sendx])
2. sendx++（if 满则 recvx）
3. unlock

**不在锁内做**：业务逻辑、I/O、内存分配（除非 `acquireSudog` 池空）。

## 5. 能无锁化吗？—— 能，但有限制

### 5.1 为什么 Go chan 不能直接无锁

chan 设计是 **MPMC（多写多读）**：
- 多个 producer 写 → head 竞争
- 多个 consumer 读 → tail 竞争
- head + tail 都 atomic CAS → CAS 链爆炸
- **结果**：必须 mutex 串行化

### 5.2 MPSC ring buffer（无锁可行）

**应用场景**：多 producer 单 consumer（本项目 drainer 单线程就是这种）。

```
   head (atomic, producers 竞争 CAS)  tail (only consumer)
   ↓                                 ↓
   [ ][ ][ ][ ][ ][ ][ ][ ]
```

**push（无锁 CAS）**：
```go
for {
    h := atomic.LoadUint32(&q.head)
    t := atomic.LoadUint32(&q.tail)
    if h-t >= uint32(cap(q.buf)) { /* 满 */ }

    // 关键：CAS 抢一个槽位，但写入 buf 不在 CAS 临界区
    slot := &q.buf[h%uint32(cap(q.buf))]
    if atomic.CompareAndSwapUint32(&q.head, h, h+1) {
        *slot = elem
        atomic.StoreUint32(&q.commit[h%uint32(cap(q.buf))], 1)  // 发布
        break
    }
    // CAS 失败：自旋重试
}
```

**pop（消费者独占，无锁）**：
```go
t := q.tail
slot := &q.buf[t%uint32(cap(q.buf))]
if atomic.LoadUint32(&q.commit[t%uint32(cap(q.buf))]) == 0 {
    return empty
}
v := *slot
atomic.StoreUint32(&q.commit[t%uint32(cap(q.buf))], 0)
q.tail = t + 1
return v
```

**关键技巧**：用 `commit` 位表示"已发布"。CAS 只抢槽位，写入 + commit 不在 CAS 临界区。

### 5.3 工业级 MPSC 实现

| 名称 | 特点 | 性能 |
|---|---|---|
| LMAX Disruptor | Java 经典，cache-line padding | ~30-50ns/op |
| Linux io_uring | 内核 MPSC，submission queue | syscall 进出 |
| Vyukov MPSC | 学术经典，bounded + lock-free | ~25-40ns/op |
| 1024cores.net lock-free | Dmitry Vyukov 系列 | 同上 |

### 5.4 Go 自己写 MPSC 的 trade-off

| 维度 | 用 chan | 自己写 MPSC |
|---|---|---|
| 性能 | ~100ns/op | ~30-50ns/op |
| 实现成本 | 0 | 100~200 行 + 边界测试 |
| ABA 风险 | 无 | 有（要用 sequence/tag） |
| 维护 | 标准库 | 自己 review |
| 适用 | 任意 | 仅 MPSC（单 consumer） |

### 5.5 MPSC 在本项目可行性

**结论：可以，但没必要**。

理由：
1. 测得 chan 延迟 ~100ns，占端到端 5ms 不到 0.002%
2. 当前 P99 瓶颈在 `flush()` 内的 `time.Sleep(5ms)` 和 `e.result <- nil` 逐条回
3. 无锁优化后理论 P99 改善 < 0.1%
4. 增加维护成本

**优化优先级**（P99 改善幅度）：
1. flush 内的 `time.Sleep(5ms)` 改成并行或去抖 → 100×
2. 逐条 `e.result <- nil` 改成 `resultCh <- error` 批量通知 → 5-10×
3. chan 改 MPSC ring buffer → 0.1%
4. 别的 → 测了再说

## 6. 无锁的代价

- **复杂**：边界条件多（满/空/ABA/sleeping producer）
- **难 debug**：bug 只在高并发下出现
- **可移植性差**：依赖 CPU memory model（x86 是 TSO，ARM/Apple Silicon 是弱序）
- **不一定更快**：高争用时 CAS 自旋烧 CPU，反而比 mutex 慢

**经验法则**：
- 低争用：mutex 够用
- 高争用：考虑 MPSC ring + batch + backoff
- 极致：lock-free，但要先 profile 证明是热点

## 7. 一句话总结

> Go chan 用 **runtime.mutex（自旋 + futex）**，fast path 极短，几乎无锁感。无锁 MPSC 能实现（CAS + commit 位），但 Go chan 是 MPMC 必须 mutex。本项目瓶颈不在 chan，无锁化收益 < 0.1%，不建议改。

---

# 第五部分：MPMC / MPSC / SPMC / SPSC 模式详解

## 1. 4 种模式定义

**按 producer（写）数量 + consumer（读）数量分类**：

| 模式 | 写者 | 读者 | 典型场景 |
|---|---|---|---|
| **MPMC** | 多 | 多 | 通用 channel、worker pool、任务分发 |
| **MPSC** | 多 | 单 | 日志收集、metrics 上报、批处理（**本项目**） |
| **SPMC** | 单 | 多 | 配置广播、信号通知、订阅 |
| **SPSC** | 单 | 单 | actor 内部、pipeline 阶段、ring buffer |

## 2. 每种模式详解

### 2.1 MPMC（多写多读）

**定义**：N 个 goroutine 写，M 个 goroutine 读，N ≥ 2 且 M ≥ 2。

**实现复杂度**：⭐⭐⭐⭐⭐（最高）

**Go chan 实现**：
```go
ch := make(chan T, size)
// 任意 G 可 ch <- x
// 任意 G 可 <-ch
```

**runtime 必须处理**：
- 多个写者竞争 buf 槽位 → mutex
- 多个读者竞争 buf 槽位 → mutex
- 一条消息只能被一个 reader 接收 → CAS + mutex
- close 后所有 reader 收到零值 → broadcast

**典型场景**：
```go
// 1. Work queue
tasks := make(chan Task, 100)
for w := 0; w < numWorkers; w++ { go worker(tasks) }
for _, t := range tasks_ { tasks <- t }
close(tasks)

// 2. Result aggregation
results := make(chan Result, 100)
for w := 0; w < numWorkers; w++ { go process(tasks, results) }
go collect(results)
```

**性能瓶颈**：mutex 争用随 N+M 增长，N=8 + M=8 时锁冲突明显。

### 2.2 MPSC（多写单读）

**定义**：N 个 goroutine 写，1 个 goroutine 读，N ≥ 2。

**实现复杂度**：⭐⭐（无锁可行）

**Go chan 实现**：
```go
queue := make(chan OrderEvent, 1024)
// 50 个 producer goroutine 写
// 1 个 drainer goroutine 读（本项目 go-microbatch）
```

**无锁实现**：
```
head (atomic, producers CAS 抢)  tail (only consumer)
↓                                ↓
[ ][ ][ ][ ][ ][ ][ ][ ]

producer 路径：
  1. atomic.Load(head) → h
  2. if h-tail < cap: 
       CAS(head, h, h+1)        // 抢槽位
       buf[h%cap] = elem         // 写数据（不在 CAS 临界区）
       commit[h%cap] = 1         // 发布

consumer 路径（无锁）：
  1. t = tail
  2. if commit[t%cap] == 1:
       v = buf[t%cap]
       commit[t%cap] = 0
       tail = t + 1
       return v
```

**关键技巧**：
- `commit` 位把"抢槽位"和"发布数据"解耦
- consumer 读 commit 位判断"producer 是否已写完"
- consumer 单线程独占 tail，无需 atomic

**典型场景**：
```go
// 1. 微批处理（本项目）
queue := make(chan OrderEvent, 1024)
for i := 0; i < 50; i++ { go produce(queue) }
go drain(queue)  // 1 个 consumer 攒批

// 2. 日志/指标聚合
metrics := make(chan Metric, 1000)
for _, c := range collectors { go c.run(metrics) }
go aggregator(metrics)  // 1 个聚合

// 3. 事件总线
events := make(chan Event, 100)
for _, p := range producers { go p.emit(events) }
go dispatcher(events)  // 1 个分发
```

**工业实现**：
- LMAX Disruptor（Java）
- Linux io_uring submission queue
- Vyukov MPSC bounded queue
- 1024cores.net lock-free MPSC

**性能**：~25-50ns/op（vs chan ~100ns/op）

### 2.3 SPMC（单写多读）

**定义**：1 个 goroutine 写，N 个 goroutine 读。

**实现复杂度**：⭐⭐⭐（mutex 或 copy-on-write）

**Go chan 实现**：
```go
// 1 个 config writer
// N 个 worker reader
config := make(chan Config)
go func() {
    cfg := loadConfig()
    config <- cfg
    close(config)
}()
for i := 0; i < N; i++ {
    go func() { cfg := <-config }()  // 实际只 1 个能拿到，其他零值
}()
```

**坑**：chan 不真"广播"。多个 reader 时只 1 个能拿到。

**正确做法**：用 `close()` 广播
```go
// 方式 1: close + 每个 reader 自己读
config := make(chan Config, 1)
go func() {
    config <- loadConfig()
    close(config)  // 所有 reader 收到零值
}()
for i := 0; i < N; i++ {
    go func() {
        cfg := <-config  // 1 个拿到 cfg，其他拿到零值 → 不可靠
    }()
}

// 方式 2: 每个 reader 独立 chan
chans := make([]chan Config, N)
for i := range chans { chans[i] = make(chan Config, 1) }
go func() {
    cfg := loadConfig()
    for _, ch := range chans { ch <- cfg }
}()

// 方式 3: atomic.Value 共享读
var config atomic.Pointer[Config]
go func() { config.Store(loadConfig()) }()
for i := 0; i < N; i++ {
    go func() {
        cfg := config.Load()  // 每次都拿最新
    }()
}
```

**无锁实现**：
- seqlock（顺序锁）：writer 用 sequence counter，reader 检测是否冲突
- RCU（Read-Copy-Update）：Linux 内核用
- copy-on-write：每次写复制一份

**典型场景**：
```go
// 1. 配置热加载
var cfg atomic.Pointer[Config]  // 多 reader
go watchConfig(&cfg)            // 单 writer

// 2. 订阅发布（每订阅者独立 chan）
subs := []chan Event{}
go publisher(subs)  // 1 个 publisher
// 多个 subscriber 各自有 chan

// 3. 状态广播
var state atomic.Value  // 单写多读
go stateUpdater(&state)
go reader1(&state)
go reader2(&state)
```

**Go 推荐**：`atomic.Value` / `atomic.Pointer` 是 SPMC 的最佳实践。

### 2.4 SPSC（单写单读）

**定义**：1 个 goroutine 写，1 个 goroutine 读。

**实现复杂度**：⭐（最简单，无锁最易）

**Go chan 实现**：
```go
ch := make(chan T, size)  // 任何 size 都行
// 1 个 producer 写
// 1 个 consumer 读
```

**无锁 ring buffer**：
```
head (单写独占)  tail (单读独占)
↓                ↓
[ ][ ][ ][ ][ ][ ][ ][ ]
```

**实现**（~30 行 Go）：
```go
type SPSC[T any] struct {
    buf  []T
    mask uint64
    head uint64  // producer 独占
    tail uint64  // consumer 独占
}

func (q *SPSC[T]) Push(v T) bool {
    h := q.head
    if h-q.tail >= uint64(len(q.buf)) { return false }
    q.buf[h&q.mask] = v
    atomic.StoreUint64(&q.head, h+1)  // 发布
    return true
}

func (q *SPSC[T]) Pop() (T, bool) {
    t := q.tail
    if t == atomic.LoadUint64(&q.head) { return *new(T), false }
    v := q.buf[t&q.mask]
    q.tail = t + 1
    return v, true
}
```

**关键**：
- `head`/`tail` 各自独占，无 CAS
- `atomic.Store(head)` 是发布语义，确保 `buf[h&mask] = v` 对 consumer 可见
- `mask = len-1`，要求 len 是 2 的幂

**cache line padding**（避免 false sharing）：
```go
type SPSC[T any] struct {
    _pad0 [64]byte
    head uint64
    _pad1 [56]byte  // 64 - 8
    tail uint64
    _pad2 [56]byte
    buf  []T
}
```

**典型场景**：
```go
// 1. Actor 模型内部 mailbox
type Actor struct {
    inbox chan Msg  // 1 个 sender（外部）→ 1 个 receiver（自己）
}

// 2. Pipeline 阶段
stage1 → chan → stage2 → chan → stage3
// 每条 chan 是 SPSC

// 3. 生产者-消费者对
producer := func(out chan<- T) { ... }
consumer := func(in <-chan T) { ... }
```

**性能**：~10-20ns/op（最快，无任何争用）

**工业实现**：
- LMAX Disruptor 单消费者模式
- Linux kfifo（内核 ring buffer）
- 学术：Lamport 队列

## 3. 4 种模式对比表

| 模式 | 写 | 读 | Go chan | 无锁难度 | 无锁性能 | 典型应用 |
|---|---|---|---|---|---|---|
| MPMC | N | M | ✅ 直接用 | 极难 | 50-100ns | 通用 channel |
| MPSC | N | 1 | ✅ 直接用 | 中 | 25-50ns | 批处理、日志聚合 |
| SPMC | 1 | M | ⚠️ 慎用（不广播） | 中 | 30-60ns | 配置广播、状态共享 |
| SPSC | 1 | 1 | ✅ 直接用 | 易 | 10-20ns | actor、pipeline |

## 4. 本项目（go-microbatch）是哪种？

```go
// 写
for i := 0; i < cfg.Concurrency; i++ {  // 50 个
    go func() {
        queue <- evt  // 50 个 goroutine 写
    }()
}

// 读
go func() {           // 1 个
    for e := range queue {  // 1 个 drainer 读
        ...
    }
}()
```

**答：MPSC（50 写 1 读）**。

理论上可换无锁 MPSC ring buffer，但：
- 当前 chan 延迟 ~100ns 占端到端 5ms 的 0.002%
- 改无锁需 100-200 行代码 + 边界测试
- 维护成本 > 性能收益

**建议**：保持 chan，先优化 flush 内部瓶颈。

## 5. 选型决策树

```
需要多个 goroutine 通信？
├─ 否 → 直接变量传（无并发）
└─ 是 → 几个写，几个读？
        ├─ 1 写 1 读 → SPSC chan 或无锁 ring buffer
        ├─ 1 写 多读 → atomic.Value / 每个 reader 独立 chan
        ├─ 多写 1读 → MPSC chan 或无锁 MPSC ring
        └─ 多写 多读 → MPMC chan（Go 原生）
```

**优化顺序**：
1. 先用 Go chan（最简单）
2. profile 证明 chan 是瓶颈
3. 按模式换无锁实现

**不要过早优化**：
- 90% 情况下 Go chan 够用
- 无锁实现 bug 难调
- 维护成本高

## 6. 一句话总结

> **4 种模式对应不同争用拓扑**：MPMC（最复杂，chan 唯一选项）/ MPSC（无锁可行，批处理典型）/ SPMC（用 atomic.Value 替代）/ SPSC（无锁最易，最快）。Go chan 全按 MPMC 实现，简单通用但非最优。本项目 MPSC 可优化但收益小，先优化 flush 内部。