// rust-microbatch: 无锁版微批处理 benchmark（v3 优化）
// 改动（相对 v2）：
// 1. 复用 result channel：每个 producer 启动时一次性创建永久 channel，drainer 按 producer_id 路由
//    消除每次请求 pthread_mutex_init/cond_init 系统调用（5-15μs/op）
// 2. OrderEvent 去掉 result_tx 字段，改为 producer_id（usize）
// 3. 退出：drainer flush 最后 batch 时 send 给已退出的 result_rx，send 返回 Err 被忽略（无害）

use crossbeam_channel::{bounded, Sender, Receiver, RecvTimeoutError};
use crossbeam_utils::CachePadded;
use std::env;
use std::fs;
use std::process;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, Ordering};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

struct OrderEvent {
    producer_id: usize,
    payload: &'static [u8],
}

#[derive(Clone)]
struct Config {
    duration_sec: u64,
    batch_max: usize,
    window_ms: u64,
    concurrency: usize,
    payload_bytes: usize,
}

struct Metrics {
    tps: f64,
    p50_us: u64,
    p99_us: u64,
    p999_us: u64,
    p9999_us: u64,
    max_us: u64,
    rss_mb: f64,
    gc_pause_p99_us: u64,
    errors: u64,
    success: u64,
}

// 无锁 histogram：每桶独立 AtomicU64 + CachePadded 防 false sharing
struct AtomicHist {
    buckets: [CachePadded<AtomicU64>; 1000],
    max_us: AtomicU64,
    total: AtomicU64,
}

impl AtomicHist {
    fn new() -> Self {
        const ZERO: CachePadded<AtomicU64> = CachePadded::new(AtomicU64::new(0));
        AtomicHist {
            buckets: [ZERO; 1000],
            max_us: AtomicU64::new(0),
            total: AtomicU64::new(0),
        }
    }
    #[inline]
    fn add(&self, us: u64) {
        if us > 0 {
            let decade = ((us as f64).log10() as i32).max(0).min(4) as usize;
            let rel = ((us as f64).log10() - decade as f64).max(0.0).min(1.0);
            let idx = (decade * 200 + (rel * 200.0) as usize).min(999);
            self.buckets[idx].fetch_add(1, Ordering::Relaxed);
        } else {
            self.buckets[0].fetch_add(1, Ordering::Relaxed);
        }
        self.total.fetch_add(1, Ordering::Relaxed);
        let mut cur = self.max_us.load(Ordering::Relaxed);
        while us > cur {
            match self.max_us.compare_exchange_weak(cur, us, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => break,
                Err(c) => cur = c,
            }
        }
    }
    fn snapshot(&self) -> Histogram {
        let mut h = Histogram { buckets: [0; 1000], max_us: 0, total: 0 };
        for i in 0..1000 {
            h.buckets[i] = self.buckets[i].load(Ordering::Relaxed);
        }
        h.max_us = self.max_us.load(Ordering::Relaxed);
        h.total = self.total.load(Ordering::Relaxed);
        h
    }
}

struct Histogram {
    buckets: [u64; 1000],
    max_us: u64,
    total: u64,
}

impl Histogram {
    fn percentile(&self, q: f64) -> u64 {
        if self.total == 0 { return 0; }
        let target = (self.total as f64 * q) as u64;
        let mut cum = 0u64;
        for (i, &b) in self.buckets.iter().enumerate() {
            cum += b;
            if cum >= target {
                let decade = i / 200;
                let rel_idx = i % 200;
                let lo = 10f64.powi(decade as i32);
                let rel = rel_idx as f64 / 200.0;
                let val = 10f64.powi(decade as i32) * (10f64).powf(rel);
                let val = val.max(lo).min(lo * 10.0);
                return val as u64;
            }
        }
        self.max_us
    }
}

fn now_nanos() -> u64 {
    SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos() as u64
}

fn read_rss_mb() -> f64 {
    let output = std::process::Command::new("ps")
        .args(&["-o", "rss=", "-p", &process::id().to_string()])
        .output();
    if let Ok(out) = output {
        if let Ok(s) = String::from_utf8(out.stdout) {
            if let Ok(kb) = s.trim().parse::<u64>() {
                return kb as f64 / 1024.0;
            }
        }
    }
    0.0
}

fn now_iso() -> String {
    if let Ok(out) = std::process::Command::new("date").args(&["+%Y-%m-%dT%H:%M:%S%z"]).output() {
        if let Ok(s) = String::from_utf8(out.stdout) {
            return s.trim().to_string();
        }
    }
    String::from("unknown")
}

fn main() {
    let args: Vec<String> = env::args().collect();
    if args.len() < 7 {
        eprintln!("usage: rust-microbatch <duration_sec> <batch_max> <window_ms> <concurrency> <payload_bytes> <output_json>");
        process::exit(2);
    }
    let cfg = Config {
        duration_sec:   args[1].parse().unwrap(),
        batch_max:      args[2].parse().unwrap(),
        window_ms:      args[3].parse().unwrap(),
        concurrency:    args[4].parse().unwrap(),
        payload_bytes:  args[5].parse().unwrap(),
    };
    let out_path = &args[6];

    let (metrics, rss_mb) = run(cfg.clone());
    let json = format_result(&cfg, &metrics, rss_mb);
    fs::write(out_path, &json).unwrap();
    println!("{}", json);
}

fn run(cfg: Config) -> (Metrics, f64) {
    let (tx, rx): (Sender<OrderEvent>, Receiver<OrderEvent>) = bounded(1024);

    // payload 一次性泄漏到堆，所有 producer 共享指针（零 refcount 零拷贝）
    let payload: &'static [u8] = Box::leak(vec![0u8; cfg.payload_bytes].into_boxed_slice());

    // 每个 producer 一个永久 result channel：避免每次请求 pthread_mutex/cond_init
    let mut result_txs: Vec<Sender<()>> = Vec::with_capacity(cfg.concurrency);

    let hist = Arc::new(AtomicHist::new());
    let stop = Arc::new(AtomicBool::new(false));

    // RSS 采样
    let rss_stop = Arc::new(AtomicBool::new(false));
    let rss_peak_x100 = Arc::new(AtomicU64::new(0));
    let rss_sampler = thread::spawn({
        let rss_stop = rss_stop.clone();
        let rss_peak_clone = rss_peak_x100.clone();
        move || {
            while !rss_stop.load(Ordering::Relaxed) {
                let mb = read_rss_mb();
                let x100 = (mb * 100.0) as u64;
                let cur = rss_peak_clone.load(Ordering::Relaxed);
                if x100 > cur { rss_peak_clone.store(x100, Ordering::Relaxed); }
                thread::sleep(Duration::from_millis(200));
            }
        }
    });

    // producer：复用 result_rx，循环发送 → 等 ack
    let mut handles = vec![];
    for producer_id in 0..cfg.concurrency {
        let (result_tx, result_rx) = bounded::<()>(2048);
        result_txs.push(result_tx);

        let tx = tx.clone();
        let hist = hist.clone();
        let stop = stop.clone();
        handles.push(thread::spawn(move || {
            loop {
                if stop.load(Ordering::Relaxed) { break; }
                let submit_nanos = now_nanos();
                let evt = OrderEvent {
                    producer_id,
                    payload,  // 零拷贝：直接传 &'static [u8]
                };
                if tx.send(evt).is_err() { break; }
                if result_rx.recv().is_err() { break; }
                let us = (now_nanos() - submit_nanos) / 1000;
                hist.add(us);
            }
        }));
    }
    drop(tx);

    // drainer：按 producer_id 路由 ack
    let drainer = thread::spawn(move || {
        let mut batch: Vec<OrderEvent> = Vec::with_capacity(cfg.batch_max);
        let window = Duration::from_millis(cfg.window_ms);
        let mut deadline = std::time::Instant::now() + window;
        loop {
            let now = std::time::Instant::now();
            let timeout = if now >= deadline {
                Duration::from_millis(0)
            } else {
                deadline - now
            };
            match rx.recv_timeout(timeout) {
                Ok(evt) => {
                    batch.push(evt);
                    if batch.len() >= cfg.batch_max {
                        flush(&mut batch, &result_txs);
                        deadline = std::time::Instant::now() + window;
                    }
                }
                Err(RecvTimeoutError::Timeout) => {
                    if !batch.is_empty() { flush(&mut batch, &result_txs); }
                    deadline = std::time::Instant::now() + window;
                }
                Err(RecvTimeoutError::Disconnected) => {
                    if !batch.is_empty() { flush(&mut batch, &result_txs); }
                    break;
                }
            }
        }
    });

    thread::sleep(Duration::from_secs(cfg.duration_sec));

    // 退出顺序：stop → producer.join → drainer 收 Disconnected → flush + break
    stop.store(true, Ordering::Relaxed);
    for h in handles { h.join().unwrap(); }
    drainer.join().unwrap();
    rss_stop.store(true, Ordering::Relaxed);
    rss_sampler.join().unwrap();

    let h = hist.snapshot();
    let total = h.total;
    let tps = total as f64 / cfg.duration_sec as f64;
    let m = Metrics {
        tps,
        p50_us: h.percentile(0.50),
        p99_us: h.percentile(0.99),
        p999_us: h.percentile(0.999),
        p9999_us: h.percentile(0.9999),
        max_us: h.max_us,
        rss_mb: 0.0,
        gc_pause_p99_us: 0,
        errors: 0,
        success: total,
    };
    let rss_mb = rss_peak_x100.load(Ordering::Relaxed) as f64 / 100.0;
    (m, rss_mb)
}

fn flush(batch: &mut Vec<OrderEvent>, result_txs: &[Sender<()>]) {
    if batch.is_empty() { return; }
    thread::sleep(Duration::from_millis(5));
    for e in batch.drain(..) {
        let _ = result_txs[e.producer_id].send(());
    }
}

fn format_result(cfg: &Config, m: &Metrics, rss_mb: f64) -> String {
    format!(
        r#"{{"name":"rust-microbatch","language":"rust","version":"stable","category":"microbatch","config":{{"duration_sec":{},"batch_max":{},"window_ms":{},"concurrency":{},"payload_bytes":{}}},"metrics":{{"tps":{:.2},"p50_us":{},"p99_us":{},"p999_us":{},"p9999_us":{},"max_us":{},"rss_mb":{:.2},"alloc_mb_per_sec":0,"gc_pause_p99_us":{},"errors":{},"success":{}}},"timestamp":"{}"}}"#,
        cfg.duration_sec, cfg.batch_max, cfg.window_ms, cfg.concurrency, cfg.payload_bytes,
        m.tps, m.p50_us, m.p99_us, m.p999_us, m.p9999_us, m.max_us, rss_mb,
        m.gc_pause_p99_us, m.errors, m.success, now_iso()
    )
}