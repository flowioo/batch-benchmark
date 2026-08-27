#!/usr/bin/env bash
# sweep.sh - 并发梯度压测：4 档 × 3 语言 × 3 轮 = 36 次
# 用途：观察 TPS / 延迟 / RSS 随 producer 并发度的扩展曲线

set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"
mkdir -p "$RESULTS"

# 默认参数
DURATION="${DURATION:-30}"
BATCH="${BATCH:-500}"
WINDOW="${WINDOW:-50}"
PAYLOAD="${PAYLOAD:-64}"
ROUNDS="${ROUNDS:-3}"

# 并发梯度（用户确认 4 档）
CONCURRENCIES_DEFAULT="1000 5000 10000 20000"
CONCURRENCIES="${CONCURRENCIES:-$CONCURRENCIES_DEFAULT}"

LANGS_DEFAULT="go-microbatch java-microbatch rust-microbatch"
LANGS="${LANGS:-$LANGS_DEFAULT}"

run_one() {
    local name="$1" conc="$2" round="$3"
    local dir="$ROOT/$name"
    [[ -x "$dir/bench.sh" ]] || { echo "⏭️  skip: $name"; return; }
    local out="$RESULTS/${name}_c${conc}_r${round}.json"
    echo "▶️  ${name} c=${conc} r=${round}/${ROUNDS}"
    local t0=$(date +%s)
    "$dir/bench.sh" "$DURATION" "$BATCH" "$WINDOW" "$conc" "$PAYLOAD" "$out" >/dev/null
    local t1=$(date +%s)
    echo "   ✅ $((t1 - t0))s → $out"
}

for conc in $CONCURRENCIES; do
    for lang in $LANGS; do
        for round in $(seq 1 "$ROUNDS"); do
            run_one "$lang" "$conc" "$round"
        done
    done
done

echo
echo "📊 汇总（按 concurrency × language）："
"$ROOT/scripts/report-sweep.sh" || true
