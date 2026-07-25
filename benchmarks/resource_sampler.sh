#!/usr/bin/env bash
set -euo pipefail

# Samples system CPU and memory usage every second and writes CSV
OUT=${1:-benchmarks/results/$(date -u +%Y%m%dT%H%M%SZ)/resource_usage.csv}
DIR=$(dirname "$OUT")
mkdir -p "$DIR"

echo "timestamp,cpu_percent,mem_used_kb,mem_free_kb" > "$OUT"
while true; do
  ts=$(date -u +%Y-%m-%dT%H:%M:%SZ)
  # macOS: use vm_stat and top. Linux users: replace with /proc or top -b
  if command -v vm_stat >/dev/null 2>&1; then
    # approximate memory usage
    mem_free_kb=$(vm_stat | awk '/Pages free/ {print $3}' | sed 's/\.|//' )
    mem_used_kb=0
    cpu_percent=$(top -l 1 -n 0 | awk '/CPU usage/ {print $3+0}')
  else
    # Linux
    mem_used_kb=$(awk '/MemTotal/ {total=$2} /MemAvailable/ {avail=$2} END {print total-avail}' /proc/meminfo)
    mem_free_kb=$(awk '/MemAvailable/ {print $2}' /proc/meminfo)
    cpu_percent=$(top -b -n1 | awk '/Cpu\(s\)/ {print $2+$4}')
  fi
  echo "$ts,$cpu_percent,$mem_used_kb,$mem_free_kb" >> "$OUT"
  sleep 1
done
