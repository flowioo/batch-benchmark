#!/usr/bin/env bash
# sweep-boundary.sh - 边界档位补全（c=2000/3000/5000/7000），bash 自实现 wall timeout
set -uo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"
mkdir -p "$RESULTS"

DURATION="${DURATION:-30}"
BATCH="${BATCH:-500}"
WINDOW="${WINDOW:-50}"
PAYLOAD="${PAYLOAD:-64}"
ROUNDS="${ROUNDS:-3}"
WALL_TIMEOUT="${WALL_TIMEOUT:-120}"

TARGETS=(
    "go-microbatch 2000"
    "go-microbatch 3000"
    "go-microbatch 7000"
    "java-microbatch 2000"
    "java-microbatch 3000"
    "java-microbatch 5000"
    "rust-microbatch 2000"
    "rust-microbatch 3000"
    "rust-microbatch 5000"
)

run_one() {
    local name="$1" conc="$2" round="$3"
    local dir="$ROOT/$name"
    local out="$RESULTS/${name}_c${conc}_r${round}.json"
    local tag="$RESULTS/${name}_c${conc}_r${round}.TIMEOUT"
    [[ -f "$out" ]] && { echo "⏭️  skip ${name} c=${conc} r=${round}"; return; }
    echo "▶️  ${name} c=${conc} r=${round}/${ROUNDS}"
    local t0=$(date +%s)
    # bash 自实现 wall timeout: 后台跑 + sleep + kill
    ( "$dir/bench.sh" "$DURATION" "$BATCH" "$WINDOW" "$conc" "$PAYLOAD" "$out" >/dev/null 2>&1 ) &
    local pid=$!
    ( sleep "$WALL_TIMEOUT"; kill -9 "$pid" 2>/dev/null ) &
    local watchdog=$!
    wait "$pid" 2>/dev/null
    local rc=$?
    kill -9 "$watchdog" 2>/dev/null
    wait "$watchdog" 2>/dev/null
    local dt=$(($(date +%s) - t0))
    if [[ -f "$out" ]] && [[ $rc -eq 0 ]]; then
        echo "   ✅ ${dt}s → $out"
        rm -f "$tag"
    else
        echo "   ⏱️  TIMEOUT/TIMEOUT (${dt}s, rc=$rc)"
        pkill -9 -f "$name/bench" 2>/dev/null || true
        pkill -9 -f "bench.Bench" 2>/dev/null || true
        echo "{\"name\":\"$name\",\"language\":\"$(echo $name | cut -d- -f1)\",\"timeout\":true,\"concurrency\":$conc}" > "$tag"
        rm -f "$out"
    fi
}

for target in "${TARGETS[@]}"; do
    read -r name conc <<< "$target"
    for round in $(seq 1 "$ROUNDS"); do
        run_one "$name" "$conc" "$round"
    done
done

echo
echo "✅ boundary sweep done"
