#!/usr/bin/env bash
# report-sweep.sh - 对 sweep.sh 结果做并发档对比
# 输出：每个 (language, concurrency) 取 3 轮中位数

set -euo pipefail
ROOT="$(cd "$(dirname "$0")/.." && pwd)"
RESULTS="$ROOT/results"

command -v jq >/dev/null 2>&1 || { echo "❌ 需要 jq"; exit 1; }

# 表头
printf "%-16s | %-6s | %-8s | %-10s | %-10s | %-10s | %-10s | %-10s | %-10s\n" \
    "language" "conc" "rss_mb" "tps_med" "p50_us_med" "p99_us_med" "p999_us_med" "max_us_med" "alloc_mb/s"
printf -- "-----------------+--------+----------+------------+-------------+-------------+--------------+--------------+--------------\n"

# 从文件名 <lang>_c<conc>_r<round>.json 抽出 lang 和 conc
for f in $(ls "$RESULTS"/*_c*_r*.json 2>/dev/null | sort); do
    [[ -f "$f" ]] || continue
    base=$(basename "$f" .json)
    # 截到最后一个 _r 之前
    prefix="${base%_r*}"
    lang="${prefix%_c*}"
    conc="${prefix#*_c}"
    # 算该 (lang,conc) 的 3 轮中位数
    metrics=$(jq -s '
        map(.metrics) |
        {
            rss: (map(.rss_mb) | sort | .[length/2|floor]),
            tps: (map(.tps) | sort | .[length/2|floor]),
            p50: (map(.p50_us) | sort | .[length/2|floor]),
            p99: (map(.p99_us) | sort | .[length/2|floor]),
            p999: (map(.p999_us) | sort | .[length/2|floor]),
            max: (map(.max_us) | sort | .[length/2|floor]),
            alloc: (map(.alloc_mb_per_sec // 0) | sort | .[length/2|floor])
        }
    ' $(ls "$RESULTS/${lang}_c${conc}_r"*.json 2>/dev/null) 2>/dev/null)
    [[ -z "$metrics" ]] && continue
    printf "%-16s | %-6s | %8.2f | %10.1f | %11.0f | %11.0f | %12.0f | %12.0f | %12.3f\n" \
        "$lang" "$conc" \
        "$(echo "$metrics" | jq -r '.rss')" \
        "$(echo "$metrics" | jq -r '.tps')" \
        "$(echo "$metrics" | jq -r '.p50')" \
        "$(echo "$metrics" | jq -r '.p99')" \
        "$(echo "$metrics" | jq -r '.p999')" \
        "$(echo "$metrics" | jq -r '.max')" \
        "$(echo "$metrics" | jq -r '.alloc')"
done | awk '!seen[$1" "$2]++'  # 去重：每个 (lang,conc) 只输出一次
