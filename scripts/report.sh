#!/usr/bin/env bash
# report.sh - 汇总 results/ 下的 JSON 结果成对比表
set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"

if ! command -v jq >/dev/null 2>&1; then
    echo "❌ 需要安装 jq: brew install jq"
    exit 1
fi

# 表头（11 列）
printf "%-22s | %-8s | %-10s | %-8s | %-8s | %-8s | %-8s | %-8s | %-8s | %-9s | %-8s\n" \
    "name" "lang" "tps" "p50_us" "p99_us" "p999_us" "p99.99_us" "max_us" "rss_mb" "success" "errors"
printf -- "-----------------------+----------+------------+----------+----------+----------+----------+----------+----------+-----------+----------\n"

# 按文件名排序输出
for f in "$RESULTS"/*.json; do
    [[ -f "$f" ]] || continue
    name=$(basename "$f" .json)
    lang=$(jq -r '.language' "$f")
    tps=$(jq -r '.metrics.tps' "$f")
    p50=$(jq -r '.metrics.p50_us' "$f")
    p99=$(jq -r '.metrics.p99_us' "$f")
    p999=$(jq -r '.metrics.p999_us' "$f")
    p9999=$(jq -r '.metrics.p9999_us' "$f")
    max=$(jq -r '.metrics.max_us' "$f")
    rss=$(jq -r '.metrics.rss_mb' "$f")
    # 兼容旧 json：success/errors 可能缺失，默认 0
    success=$(jq -r '.metrics.success // 0' "$f")
    errors=$(jq -r '.metrics.errors // 0' "$f")
    printf "%-22s | %-8s | %10.1f | %8.0f | %8.0f | %8.0f | %8.0f | %8.0f | %8.1f | %9d | %8d\n" \
        "$name" "$lang" "$tps" "$p50" "$p99" "$p999" "$p9999" "$max" "$rss" "$success" "$errors"
done
