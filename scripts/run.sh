#!/usr/bin/env bash
# run.sh - 顶层 benchmark runner
# 自动发现所有 subproject（包含 bench.sh 的子目录）并跑

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"
mkdir -p "$RESULTS"

# 默认参数
DURATION="${DURATION:-30}"
BATCH="${BATCH:-500}"
WINDOW="${WINDOW:-50}"
CONCURRENCY="${CONCURRENCY:-8}"
PAYLOAD="${PAYLOAD:-64}"

# 单个 subproject 还是全部
TARGET="${1:-all}"

run_one() {
    local name="$1"
    local dir="$ROOT/$name"
    if [[ ! -x "$dir/bench.sh" ]]; then
        echo "⏭️  skip: $name (no bench.sh)"
        return
    fi
    echo "▶️  running: $name ($DURATION s, batch=$BATCH, window=${WINDOW}ms, concurrency=$CONCURRENCY, payload=${PAYLOAD}B)"
    local out="$RESULTS/${name}.json"
    local start=$(date +%s)
    "$dir/bench.sh" "$DURATION" "$BATCH" "$WINDOW" "$CONCURRENCY" "$PAYLOAD" "$out" >/dev/null
    local end=$(date +%s)
    echo "✅ done: $name ($((end - start))s) → $out"
}

if [[ "$TARGET" == "all" ]]; then
    # 串行跑所有 subproject（避免 CPU 争抢影响数据）
    for d in "$ROOT"/*/; do
        [[ -x "$d/bench.sh" ]] || continue
        name=$(basename "$d")
        run_one "$name"
    done
else
    run_one "$TARGET"
fi

echo
echo "📊 汇总："
"$ROOT/scripts/report.sh" || true
