#!/usr/bin/env bash
# sweep-remaining.sh - 跑剩余档，每档带 120s wall timeout
# 原因：java 5000+ OS-thread 已触调度墙；rust 同样风险；go 默认 M:N 调度
# 输出：跑通的写 JSON，timeout 的写 results/<lang>_c<conc>_r<round>.TIMEOUT
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

# 跑这些档
TARGETS=(
    "go-microbatch 1000"
    "go-microbatch 5000"
    "go-microbatch 10000"
    "go-microbatch 20000"
    "rust-microbatch 1000"
    "rust-microbatch 5000"
    "rust-microbatch 10000"
    "rust-microbatch 20000"
    "java-microbatch 5000"
    "java-microbatch 10000"
    "java-microbatch 20000"
)

run_one() {
    local name="$1" conc="$2" round="$3"
    local dir="$ROOT/$name"
    [[ -x "$dir/bench.sh" ]] || return
    local out="$RESULTS/${name}_c${conc}_r${round}.json"
    local tag="$RESULTS/${name}_c${conc}_r${round}.TIMEOUT"
    echo "▶️  ${name} c=${conc} r=${round}/${ROUNDS}"
    local t0=$(date +%s)
    if timeout "$WALL_TIMEOUT" "$dir/bench.sh" "$DURATION" "$BATCH" "$WINDOW" "$conc" "$PAYLOAD" "$out" >/dev/null 2>&1; then
        echo "   ✅ $(($(date +%s) - t0))s → $out"
        rm -f "$tag"
    else
        echo "   ⏱️  TIMEOUT (>${WALL_TIMEOUT}s) → $tag"
        # 标记该档为卡死
        pkill -9 -f "$name/bench" 2>/dev/null || true
        pkill -9 -f "bench.Bench" 2>/dev/null || true
        # 写一个空 marker 表示这一档卡死
        echo "{\"name\":\"$name\",\"language\":\"$(echo $name | cut -d- -f1)\",\"timeout\":true}" > "$tag"
        rm -f "$out"  # 确保半成品 json 被清掉
    fi
}

for target in "${TARGETS[@]}"; do
    read -r name conc <<< "$target"
    # 如果该 (lang,conc) 全部 3 轮都已存在，跳过
    existing=$(ls "$RESULTS/${name}_c${conc}_r"*.json 2>/dev/null | wc -l)
    if [[ $existing -ge $ROUNDS ]]; then
        echo "⏭️  skip ${name} c=${conc} (已有 $existing 轮)"
        continue
    fi
    for round in $(seq 1 "$ROUNDS"); do
        # 已有的话跳过
        if [[ -f "$RESULTS/${name}_c${conc}_r${round}.json" ]]; then
            echo "⏭️  skip ${name} c=${conc} r=${round}"
            continue
        fi
        run_one "$name" "$conc" "$round"
    done
done

echo
echo "📊 汇总："
ls "$RESULTS"/*_c*_r*.json 2>/dev/null | wc -l
echo "TIMEOUT marker:"
ls "$RESULTS"/*.TIMEOUT 2>/dev/null
