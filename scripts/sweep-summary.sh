#!/usr/bin/env bash
# sweep-summary.sh - 解析 4 档×3 轮 JSON，输出 markdown 表格
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"

command -v jq >/dev/null 2>&1 || { echo "❌ 需要 jq"; exit 1; }

echo "# 并发梯度对比（中位数）"
echo
echo "| language | concurrency | TPS | P50 (μs) | P99 (μs) | P999 (μs) | max (μs) | RSS (MB) | alloc MB/s | status |"
echo "|---|---|---|---|---|---|---|---|---|---|"

emit_row() {
    local lang="$1" conc="$2"
    local files
    files=$(ls "$RESULTS/${lang}_c${conc}_r"*.json 2>/dev/null || true)
    if [[ -z "$files" ]]; then
        # 检查 TIMEOUT
        if ls "$RESULTS/${lang}_c${conc}_r"*.TIMEOUT >/dev/null 2>&1; then
            printf "| %s | %s | _TIMEOUT_ | | | | | | | os-thread 反压卡死 |\n" "$lang" "$conc"
        else
            printf "| %s | %s | _缺数据_ | | | | | | | |\n" "$lang" "$conc"
        fi
        return
    fi
    # jq 一行输出 7 个数（每行一个），用 mapfile
    local vals
    vals=$(jq -r -s '
        map(.metrics) |
        [
            ((map(.tps)            | sort | .[length/2|floor]) | tostring),
            ((map(.p50_us)         | sort | .[length/2|floor]) | tostring),
            ((map(.p99_us)         | sort | .[length/2|floor]) | tostring),
            ((map(.p999_us)        | sort | .[length/2|floor]) | tostring),
            ((map(.max_us)         | sort | .[length/2|floor]) | tostring),
            ((map(.rss_mb)         | sort | .[length/2|floor]) | tostring),
            ((map(.alloc_mb_per_sec // 0) | sort | .[length/2|floor]) | tostring)
        ] | .[]
    ' $files)
    local tps p50 p99 p999 max rss alloc
    { read -r tps; read -r p50; read -r p99; read -r p999; read -r max; read -r rss; read -r alloc; } <<< "$vals"
    # status: TIMEOUT 标记
    local status="✅"
    if ls "$RESULTS/${lang}_c${conc}_r"*.TIMEOUT >/dev/null 2>&1; then
        status="partial-TIMEOUT"
    fi
    printf "| %s | %s | %s | %s | %s | %s | %s | %s | %s | %s |\n" \
        "$lang" "$conc" "${tps%.*}" "$p50" "$p99" "$p999" "$max" "${rss%.*}" "$alloc" "$status"
}

for lang in go-microbatch java-microbatch rust-microbatch; do
    for conc in 1000 5000 10000 20000; do
        emit_row "$lang" "$conc"
    done
done

echo
echo "# 完整文件清单"
ls -1 "$RESULTS"/*_c*_r*.json "$RESULTS"/*.TIMEOUT 2>/dev/null | sed 's|.*/||' | sort
