// go-microbatch: 简化版微批处理 benchmark
// 架构：MPSC chan + 50ms 攒批 + NoOp sink (sleep 5ms 模拟 1 事务) + 同步等结果
// 公平性：跟 java-microbatch / rust-microbatch 一致的接口和参数

package main

import (
	"encoding/json"
	"fmt"
	"math"
	"net/http"
	_ "net/http/pprof"
	"os"
	"runtime"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

// HDR Hist: 5 个数量级 × 200 桶 = 1000 桶
// 1μs-10μs, 10-100μs, 100μs-1ms, 1ms-10ms, 10ms-100ms
// 桶宽度 = decade_size / 200
type hdrHist struct {
	buckets [1000]uint64
	total   uint64
	max     int64
}

func newHdrHist() *hdrHist { return &hdrHist{} }

func (h *hdrHist) add(us int64) {
	if us < 0 {
		us = 0
	}
	if us > h.max {
		h.max = us
	}
	h.total++
	if us == 0 {
		h.buckets[0]++
		return
	}
	// 找到数量级（每 10x 一档，5 档 = 0..4）
	decade := int(math.Log10(float64(us)))
	if decade < 0 {
		decade = 0
	}
	if decade > 4 {
		decade = 4
	}
	// 该档内的相对位置（对数刻度，0..1）
	rel := (math.Log10(float64(us)) - float64(decade)) // 0..1
	idx := decade*200 + int(rel*200)
	if idx >= 1000 {
		idx = 999
	}
	h.buckets[idx]++
}

func (h *hdrHist) percentile(q float64) float64 {
	if h.total == 0 {
		return 0
	}
	target := uint64(float64(h.total) * q)
	cum := uint64(0)
	for i, b := range h.buckets {
		cum += b
		if cum >= target {
			// 反算 μs
			decade := i / 200
			relIdx := i % 200
			lo := math.Pow(10, float64(decade))
			hi := math.Pow(10, float64(decade+1))
			rel := float64(relIdx) / 200.0
			// 桶中心值
			val := math.Pow(10, float64(decade)+rel) // 几何中心
			if val < lo {
				val = lo
			}
			if val > hi {
				val = hi
			}
			return val
		}
	}
	return float64(h.max)
}

type OrderEvent struct {
	seq         uint64
	submitNanos int64
	result      chan error
	payload     []byte
}

type Config struct {
	DurationSec  int
	BatchMax     int
	WindowMs     int
	Concurrency  int
	PayloadBytes int
}

type Metrics struct {
	TPS           float64 `json:"tps"`
	P50Us         float64 `json:"p50_us"`
	P99Us         float64 `json:"p99_us"`
	P999Us        float64 `json:"p999_us"`
	P9999Us       float64 `json:"p9999_us"`
	MaxUs         float64 `json:"max_us"`
	RSSMb         float64 `json:"rss_mb"`
	AllocMbPerSec float64 `json:"alloc_mb_per_sec"`
	GcPauseP99Us  float64 `json:"gc_pause_p99_us"`
	Errors        int     `json:"errors"`
	Success       uint64  `json:"success"`
}

type Result struct {
	Name      string  `json:"name"`
	Language  string  `json:"language"`
	Version   string  `json:"version"`
	Category  string  `json:"category"`
	Config    Config  `json:"config"`
	Metrics   Metrics `json:"metrics"`
	Timestamp string  `json:"timestamp"`
}

func main() {
	if len(os.Args) < 7 {
		fmt.Fprintln(os.Stderr, "usage: main <duration_sec> <batch_max> <window_ms> <concurrency> <payload_bytes> <output_json>")
		os.Exit(2)
	}

	// 可观测：开 pprof HTTP 端点 + block/mutex profile 全采样
	runtime.SetBlockProfileRate(1)
	runtime.SetMutexProfileFraction(5)
	go func() {
		_ = http.ListenAndServe("localhost:6060", nil)
	}()
	duration, _ := strconv.Atoi(os.Args[1])
	batchMax, _ := strconv.Atoi(os.Args[2])
	windowMs, _ := strconv.Atoi(os.Args[3])
	concurrency, _ := strconv.Atoi(os.Args[4])
	payloadBytes, _ := strconv.Atoi(os.Args[5])
	outPath := os.Args[6]

	cfg := Config{
		DurationSec:  duration,
		BatchMax:     batchMax,
		WindowMs:     windowMs,
		Concurrency:  concurrency,
		PayloadBytes: payloadBytes,
	}

	res := run(cfg)

	bytes, _ := json.MarshalIndent(res, "", "  ")
	if err := os.WriteFile(outPath, bytes, 0644); err != nil {
		fmt.Fprintln(os.Stderr, "write result:", err)
		os.Exit(1)
	}
	fmt.Println(string(bytes))
}

func run(cfg Config) Result {
	var memStats runtime.MemStats
	runtime.ReadMemStats(&memStats)
	allocStart := memStats.TotalAlloc
	startTime := time.Now()

	queue := make(chan OrderEvent, 1024)
	stop := make(chan struct{})

	var histMu sync.Mutex
	hist := newHdrHist()
	addLatency := func(us int64) {
		histMu.Lock()
		hist.add(us)
		histMu.Unlock()
	}

	var seqCounter atomic.Uint64
	var submitSuccess atomic.Uint64

	// producer
	var wgProducer sync.WaitGroup
	for i := 0; i < cfg.Concurrency; i++ {
		wgProducer.Add(1)
		go func() {
			defer wgProducer.Done()
			payload := make([]byte, cfg.PayloadBytes)
			for {
				select {
				case <-stop:
					return
				default:
				}
				resultCh := make(chan error, 1)
				evt := OrderEvent{
					seq:         seqCounter.Add(1),
					submitNanos: time.Now().UnixNano(),
					result:      resultCh,
					payload:     payload,
				}
				queue <- evt
				<-resultCh // 同步等结果
				addLatency(int64(time.Since(time.Unix(0, evt.submitNanos)).Microseconds()))
				submitSuccess.Add(1)
			}
		}()
	}

	// drainer
	drainerDone := make(chan struct{})
	go func() {
		defer close(drainerDone)
		ticker := time.NewTicker(time.Duration(cfg.WindowMs) * time.Millisecond)
		defer ticker.Stop()
		batch := make([]OrderEvent, 0, cfg.BatchMax)
		flush := func() {
			if len(batch) == 0 {
				return
			}
			time.Sleep(5 * time.Millisecond) // NoOp sink
			for _, e := range batch {
				e.result <- nil
			}
			batch = batch[:0]
		}
		for {
			select {
			case e := <-queue:
				batch = append(batch, e)
				if len(batch) >= cfg.BatchMax {
					flush()
				}
			case <-ticker.C:
				flush()
			case <-stop:
				for {
					select {
					case e := <-queue:
						batch = append(batch, e)
					default:
						flush()
						return
					}
				}
			}
		}
	}()

	time.Sleep(time.Duration(cfg.DurationSec) * time.Second)
	close(stop)
	wgProducer.Wait()
	<-drainerDone
	elapsed := time.Since(startTime)

	runtime.ReadMemStats(&memStats)
	allocEnd := memStats.TotalAlloc
	allocRate := float64(allocEnd-allocStart) / elapsed.Seconds() / 1024 / 1024

	// GC 暂停（最近 256 次总和）
	var gcPauseTotalUs uint64
	for i := 0; i < 256; i++ {
		gcPauseTotalUs += memStats.PauseNs[i] / 1000
	}

	return Result{
		Name:     "go-microbatch",
		Language: "go",
		Version:  runtime.Version(),
		Category: "microbatch",
		Config:   cfg,
		Metrics: Metrics{
			TPS:           float64(submitSuccess.Load()) / elapsed.Seconds(),
			P50Us:         hist.percentile(0.50),
			P99Us:         hist.percentile(0.99),
			P999Us:        hist.percentile(0.999),
			P9999Us:       hist.percentile(0.9999),
			MaxUs:         float64(hist.max),
			RSSMb:         float64(memStats.Sys) / 1024 / 1024,
			AllocMbPerSec: allocRate,
			GcPauseP99Us:  float64(gcPauseTotalUs),
			Errors:        0,
			Success:       submitSuccess.Load(),
		},
		Timestamp: time.Now().Format(time.RFC3339),
	}
}
