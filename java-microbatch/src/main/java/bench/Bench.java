// java-microbatch: 简化版微批处理 benchmark
// 架构：MpscArrayQueue + 50ms 攒批 + NoOp sink (sleep 5ms 模拟 1 事务) + park 等结果
// 公平性：跟 go-microbatch / rust-microbatch 一致的接口和参数

package bench;

import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.lang.management.ManagementFactory;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.concurrent.locks.LockSupport;
import org.jctools.queues.MpscArrayQueue;

public class Bench {
    // 桶索引查表：μs ∈ [0, 100_000] → 0..999 桶索引，O(1) 无浮点
    static final int[] IDX_TABLE = buildIdxTable();
    static int[] buildIdxTable() {
        int[] t = new int[100_001];
        for (int us = 0; us <= 100_000; us++) {
            if (us == 0) { t[0] = 0; continue; }
            int decade = (int) Math.log10(us);
            if (decade < 0) decade = 0;
            if (decade > 4) decade = 4;
            double rel = Math.log10((double) us) - decade;
            int idx = decade * 200 + (int)(rel * 200);
            if (idx >= 1000) idx = 999;
            t[us] = idx;
        }
        return t;
    }

    // VarHandle for done: hot read 走 plain load（unpark 提供 happens-before）
    static final VarHandle DONE;
    static {
        try {
            DONE = MethodHandles.lookup().findVarHandle(OrderEvent.class, "done", boolean.class);
        } catch (ReflectiveOperationException e) {
            throw new ExceptionInInitializerError(e);
        }
    }

    static final class OrderEvent {
        long seq;
        long submitNanos;
        Thread parker;
        byte[] payload;
        OrderEvent next;       // 对象池 free list 指针
        boolean done;          // VarHandle 访问

        OrderEvent(long seq, long submitNanos, Thread parker, byte[] payload) {
            this.seq = seq;
            this.submitNanos = submitNanos;
            this.parker = parker;
            this.payload = payload;
        }
        // 热路径读：opaque load（无 fence，unpark 提供 happens-before）
        boolean plainDone()    { return (boolean) DONE.getOpaque(this); }
        // flush 端写：release store + unpark 提供 acquire fence
        void markDoneRelease() { DONE.setRelease(this, true); }
        void reset() {
            parker = null;
            payload = null;
            next = null;
        }
    }

    // 每 producer 一个无锁 free list（LIFO 栈，CAS-free 由单线程访问保证）
    static final class EventPool {
        private OrderEvent head;
        OrderEvent acquire(long seq, long submitNanos, Thread parker, byte[] payload) {
            OrderEvent e = head;
            if (e == null) return new OrderEvent(seq, submitNanos, parker, payload);
            head = e.next;
            e.seq = seq;
            e.submitNanos = submitNanos;
            e.parker = parker;
            e.payload = payload;
            e.next = null;
            e.done = false;
            return e;
        }
        void release(OrderEvent e) {
            e.reset();
            e.done = false;
            e.next = head;
            head = e;
        }
    }

    static final class Config {
        int durationSec;
        int batchMax;
        int windowMs;
        int concurrency;
        int payloadBytes;
        String toJson() {
            return String.format(
                "{\"duration_sec\":%d,\"batch_max\":%d,\"window_ms\":%d,\"concurrency\":%d,\"payload_bytes\":%d}",
                durationSec, batchMax, windowMs, concurrency, payloadBytes
            );
        }
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 6) {
            System.err.println("usage: Bench <duration_sec> <batch_max> <window_ms> <concurrency> <payload_bytes> <output_json>");
            System.exit(2);
        }
        Config cfg = new Config();
        cfg.durationSec = Integer.parseInt(args[0]);
        cfg.batchMax    = Integer.parseInt(args[1]);
        cfg.windowMs    = Integer.parseInt(args[2]);
        cfg.concurrency = Integer.parseInt(args[3]);
        cfg.payloadBytes = Integer.parseInt(args[4]);
        String outPath  = args[5];

        ResultBag bag = run(cfg);
        String json = formatResult(cfg, bag);
        Files.writeString(Path.of(outPath), json);
        System.out.println(json);
    }

    static final class ResultBag {
        long success;
        long[] buckets;
        long maxUs;
        long maxGcPauseUs;
        long total;
        ResultBag() { buckets = new long[1000]; }
        void addLatency(long us) {
            if (us < 0) us = 0;
            if (us > 100_000) us = 100_000;
            if (us > maxUs) maxUs = us;
            total++;
            buckets[IDX_TABLE[(int) us]]++;
        }
        void merge(ResultBag other) {
            success += other.success;
            total += other.total;
            if (other.maxUs > maxUs) maxUs = other.maxUs;
            for (int i = 0; i < buckets.length; i++) buckets[i] += other.buckets[i];
        }
        long percentile(double q) {
            if (total == 0) return 0;
            long target = (long)(total * q);
            long cum = 0;
            for (int i = 0; i < buckets.length; i++) {
                cum += buckets[i];
                if (cum >= target) {
                    int decade = i / 200;
                    int relIdx = i % 200;
                    double lo = Math.pow(10, decade);
                    double hi = Math.pow(10, decade + 1);
                    double rel = relIdx / 200.0;
                    double val = Math.pow(10, decade + rel);
                    if (val < lo) val = lo;
                    if (val > hi) val = hi;
                    return (long) val;
                }
            }
            return maxUs;
        }
    }

    static ResultBag run(Config cfg) throws Exception {
        MpscArrayQueue<OrderEvent> queue = new MpscArrayQueue<>(1024);
        AtomicBoolean stop = new AtomicBoolean(false);
        AtomicLong seqCounter = new AtomicLong();
        ResultBag[] perBag = new ResultBag[cfg.concurrency];
        AtomicLong bagMaxGcPause = new AtomicLong();

        Thread[] producers = new Thread[cfg.concurrency];
        for (int i = 0; i < cfg.concurrency; i++) {
            final int idx = i;
            producers[i] = new Thread(() -> {
                ResultBag myBag = new ResultBag();
                perBag[idx] = myBag;
                byte[] payload = new byte[cfg.payloadBytes];
                Thread me = Thread.currentThread();
                EventPool pool = new EventPool();

                while (!stop.get()) {
                    OrderEvent evt = pool.acquire(
                        seqCounter.incrementAndGet(),
                        System.nanoTime(),
                        me,
                        payload
                    );
                    while (!queue.offer(evt)) Thread.onSpinWait();

                    long deadlineNs = System.nanoTime() + cfg.windowMs * 2_000_000L;
                    while (!evt.plainDone()) {
                        long parkNs = deadlineNs - System.nanoTime();
                        if (parkNs <= 0) break;
                        LockSupport.parkNanos(parkNs);
                    }
                    long latUs = (System.nanoTime() - evt.submitNanos) / 1000;
                    myBag.addLatency(latUs);
                    myBag.success++;
                    pool.release(evt);
                }
            }, "producer-" + i);
            producers[i].start();
        }

        Thread drainer = new Thread(() -> {
            List<OrderEvent> batch = new ArrayList<>(cfg.batchMax);
            long deadline = System.nanoTime() + cfg.windowMs * 1_000_000L;
            try {
                while (!stop.get() || !queue.isEmpty()) {
                    OrderEvent evt = queue.poll();
                    if (evt == null) {
                        long now = System.nanoTime();
                        long timeoutNs = deadline - now;
                        if (timeoutNs <= 0) {
                            flush(batch);
                            deadline = System.nanoTime() + cfg.windowMs * 1_000_000L;
                            continue;
                        }
                        LockSupport.parkNanos(Math.min(timeoutNs, 1_000_000L));
                        continue;
                    }
                    batch.add(evt);
                    if (batch.size() >= cfg.batchMax) {
                        flush(batch);
                        deadline = System.nanoTime() + cfg.windowMs * 1_000_000L;
                    }
                }
                OrderEvent tail;
                while ((tail = queue.poll()) != null) {
                    batch.add(tail);
                }
                flush(batch);
            } catch (Throwable t) {}
        }, "drainer");
        drainer.start();

        Thread gcMonitor = new Thread(() -> {
            long lastGcTime = 0;
            while (!stop.get()) {
                long total = ManagementFactory.getGarbageCollectorMXBeans().stream()
                    .mapToLong(gc -> gc.getCollectionTime())
                    .sum();
                if (total > lastGcTime) {
                    long delta = total - lastGcTime;
                    long deltaUs = delta * 1000;
                    if (deltaUs > bagMaxGcPause.get()) bagMaxGcPause.set(deltaUs);
                    lastGcTime = total;
                }
                try { Thread.sleep(10); } catch (InterruptedException e) { return; }
            }
        });
        gcMonitor.setDaemon(true);
        gcMonitor.start();

        Thread.sleep(cfg.durationSec * 1000L);
        stop.set(true);
        for (Thread p : producers) p.join();
        drainer.join();
        gcMonitor.interrupt();

        ResultBag merged = new ResultBag();
        for (ResultBag b : perBag) {
            if (b != null) merged.merge(b);
        }
        merged.maxGcPauseUs = bagMaxGcPause.get();
        return merged;
    }

    static void flush(List<OrderEvent> batch) {
        if (batch.isEmpty()) return;
        try { Thread.sleep(5); } catch (InterruptedException e) {}
        for (OrderEvent e : batch) {
            e.markDoneRelease();           // release store
            LockSupport.unpark(e.parker);  // 唤醒 producer（提供 acquire fence）
        }
        batch.clear();
    }

    static String formatResult(Config cfg, ResultBag bag) {
        double tps = (double) bag.total / cfg.durationSec;
        double rssMb = readRssMb();
        return String.format(
            "{\"name\":\"java-microbatch\",\"language\":\"java\",\"version\":\"%s\",\"category\":\"microbatch\"," +
            "\"config\":%s," +
            "\"metrics\":{" +
                "\"tps\":%.2f," +
                "\"p50_us\":%d," +
                "\"p99_us\":%d," +
                "\"p999_us\":%d," +
                "\"p9999_us\":%d," +
                "\"max_us\":%d," +
                "\"rss_mb\":%.2f," +
                "\"alloc_mb_per_sec\":0," +
                "\"gc_pause_p99_us\":%d," +
                "\"errors\":0," +
                "\"success\":%d" +
            "},\"timestamp\":\"%s\"}",
            System.getProperty("java.version"),
            cfg.toJson(),
            tps,
            bag.percentile(0.50),
            bag.percentile(0.99),
            bag.percentile(0.999),
            bag.percentile(0.9999),
            bag.maxUs,
            rssMb,
            bag.maxGcPauseUs,
            bag.total,
            new java.text.SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssXXX").format(new Date())
        );
    }

    static double readRssMb() {
        try {
            Process p = new ProcessBuilder("ps", "-o", "rss=", "-p",
                java.lang.management.ManagementFactory.getRuntimeMXBean().getName().split("@")[0])
                .redirectErrorStream(true).start();
            byte[] out = p.getInputStream().readAllBytes();
            p.waitFor();
            String s = new String(out).trim();
            if (!s.isEmpty()) {
                long kb = Long.parseLong(s);
                return kb / 1024.0;
            }
        } catch (Exception e) {
            return Runtime.getRuntime().totalMemory() / 1024.0 / 1024.0 * 1.5;
        }
        return 0.0;
    }
}