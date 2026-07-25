Plotting quickstart

1) Install Python deps (prefer virtualenv):

```bash
python3 -m venv .venv
source .venv/bin/activate
pip install -r benchmarks/requirements.txt
```

2) Run plot script on a sweep output (example):

```bash
python3 benchmarks/plot.py --input benchmarks/results/<timestamp>/aggregate.csv
```

3) Outputs are placed in `benchmarks/results/<timestamp>/figs/` and include:
- `throughput_vs_requests.png`
- `latency_boxplot.png`
- `summary.csv`

Notes:
- `plot.py` expects the aggregated CSV to contain the header:
  `scenario,totalRequests,throughput,avgLatencyMs,p99LatencyMs,maxLatencyMs,memoryUsedBytes`
- Modify `plot.py` to add additional plots (CDFs) if per-request latencies are exported.
