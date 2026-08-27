#!/usr/bin/env bash
# pprof-go.sh - 单独跑一轮 Go 20000 并发，期间采集 CPU / mutex / block profile
# 与 sweep.sh 错开：pprof 需要独占 HTTP :6060
# 输出：results/pprof/cpu.txt、mutex.txt、block.txt
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT/go-microbatch"
mkdir -p "$ROOT/results/pprof"

echo "▶️  go build..."
go build -o bench . 2>/dev/null

DURATION="${DURATION:-30}"
CONC="${CONC:-20000}"
OUT="$ROOT/results/pprof/go_c${CONC}_runtime.json"

echo "▶️  ./bench $DURATION ... $CONC producers"
./bench "$DURATION" 500 50 "$CONC" 64 "$OUT" >/dev/null &
BENCH_PID=$!

# 等服务起
sleep 3

echo "▶️  采 CPU profile 10s"
go tool pprof -seconds=10 -text http://localhost:6060/debug/pprof/profile > "$ROOT/results/pprof/cpu.txt" 2>&1 || true

echo "▶️  采 mutex profile"
go tool pprof -text http://localhost:6060/debug/pprof/mutex > "$ROOT/results/pprof/mutex.txt" 2>&1 || true

echo "▶️  采 block profile"
go tool pprof -text http://localhost:6060/debug/pprof/block > "$ROOT/results/pprof/block.txt" 2>&1 || true

echo "▶️  goroutine dump"
curl -s http://localhost:6060/debug/pprof/goroutine?debug=2 > "$ROOT/results/pprof/goroutines.txt" 2>&1 || true

echo "▶️  heap"
go tool pprof -text http://localhost:6060/debug/pprof/heap > "$ROOT/results/pprof/heap.txt" 2>&1 || true

# 等 bench 跑完
wait "$BENCH_PID"
echo "✅ done: $ROOT/results/pprof/{cpu,mutex,block,heap,goroutines}.txt + $OUT"
