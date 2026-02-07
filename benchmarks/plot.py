#!/usr/bin/env python3
"""Simple analysis and plotting for benchmark aggregate CSVs.

Usage:
  python3 benchmarks/plot.py --input benchmarks/results/<ts>/aggregate.csv --outdir benchmarks/results/<ts>/figs
"""
import argparse
import os
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np

sns.set(style="whitegrid")


def load_csv(path):
    df = pd.read_csv(path)
    # normalize column names if needed
    return df


def ensure_outdir(path):
    os.makedirs(path, exist_ok=True)


def plot_throughput_vs_requests(df, outdir):
    # convert totalRequests to numeric
    df['totalRequests'] = pd.to_numeric(df['totalRequests'], errors='coerce')
    grouped = df.groupby(['scenario','totalRequests'])['throughput'].agg(['mean','std']).reset_index()
    # compute 95% CI using t-distribution
    from scipy import stats
    agg = df.groupby(['scenario','totalRequests']).agg(
        mean_throughput=('throughput','mean'),
        std_throughput=('throughput','std'),
        n=('throughput','count')
    ).reset_index()
    agg['ci95'] = agg.apply(lambda r: (stats.t.ppf(0.975, r['n']-1) * r['std_throughput'] / (r['n']**0.5)) if r['n']>1 else 0.0, axis=1)

    plt.figure(figsize=(9,6))
    for scenario, g in agg.groupby('scenario'):
        plt.errorbar(g['totalRequests'], g['mean_throughput'], yerr=g['ci95'], marker='o', label=scenario, capsize=4)
    plt.xlabel('Total Requests', fontsize=12)
    plt.ylabel('Throughput (ops/sec)', fontsize=12)
    plt.title('Throughput vs Total Requests', fontsize=14)
    plt.legend()
    plt.grid(alpha=0.25)
    plt.tight_layout()
    out_svg = os.path.join(outdir, 'throughput_vs_requests.svg')
    out_png = os.path.join(outdir, 'throughput_vs_requests.png')
    plt.savefig(out_svg)
    plt.savefig(out_png, dpi=300)
    plt.close()


def plot_latency_box(df, outdir):
    plt.figure(figsize=(8,5))
    sns.boxplot(x='scenario', y='avgLatencyMs', data=df)
    plt.xlabel('Scenario', fontsize=12)
    plt.ylabel('Average Latency (ms)', fontsize=12)
    plt.title('Average Latency by Scenario', fontsize=14)
    plt.grid(alpha=0.2)
    plt.tight_layout()
    out_svg = os.path.join(outdir, 'latency_boxplot.svg')
    out_png = os.path.join(outdir, 'latency_boxplot.png')
    plt.savefig(out_svg)
    plt.savefig(out_png, dpi=300)
    plt.close()


def plot_latency_cdf(lat_csv_paths, outdir):
    # Create cleaner plots: one CDF per scenario per request-size, aggregated across durations/repeats
    # Parse filenames expecting: SCENARIO_reqXXX_durYY_repZ_latencies.csv
    groups = {}
    for path in lat_csv_paths:
        base = os.path.basename(path)
        parts = base.split('_')
        if len(parts) < 4:
            continue
        scenario = parts[0]
        req = parts[1]  # e.g., req100
        key = f"{scenario}_{req}"
        groups.setdefault(key, []).append(path)

    # produce one figure per request-size (req100, req500, ...)
    by_req = {}
    for key, files in groups.items():
        scenario, req = key.split('_')
        by_req.setdefault(req, {}).setdefault(scenario, []).extend(files)

    percentiles_rows = []
    for req, scen_map in by_req.items():
        plt.figure(figsize=(9,6))
        for scenario, files in scen_map.items():
            all_vals = []
            for f in files:
                try:
                    lat = pd.read_csv(f, header=None)
                    vals = lat.iloc[:,1].astype(float).values
                    all_vals.append(vals)
                except Exception:
                    continue
            if not all_vals:
                continue
            vals_concat = np.concatenate(all_vals)
            vals_sorted = np.sort(vals_concat)
            cdf = np.arange(1, len(vals_sorted)+1) / len(vals_sorted)
            plt.plot(vals_sorted, cdf, label=scenario, alpha=0.9)
            # compute percentiles for table
            p50 = np.percentile(vals_concat, 50)
            p90 = np.percentile(vals_concat, 90)
            p95 = np.percentile(vals_concat, 95)
            p99 = np.percentile(vals_concat, 99)
            percentiles_rows.append({
                'scenario': scenario,
                'request_group': req,
                'p50_ms': round(p50,3),
                'p90_ms': round(p90,3),
                'p95_ms': round(p95,3),
                'p99_ms': round(p99,3),
            })

        plt.xlabel('Latency (ms)', fontsize=12)
        plt.ylabel('CDF', fontsize=12)
        plt.title(f'Latency CDFs (aggregated) — {req}', fontsize=14)
        plt.legend(fontsize='small')
        plt.grid(alpha=0.2)
        plt.tight_layout()
        out_svg = os.path.join(outdir, f'latency_cdfs_{req}.svg')
        out_png = os.path.join(outdir, f'latency_cdfs_{req}.png')
        plt.savefig(out_svg)
        plt.savefig(out_png, dpi=300)
        plt.close()

        # tail zoom plot (focus on top percentiles)
        plt.figure(figsize=(8,4))
        for scenario, files in scen_map.items():
            all_vals = []
            for f in files:
                try:
                    lat = pd.read_csv(f, header=None)
                    vals = lat.iloc[:,1].astype(float).values
                    all_vals.append(vals)
                except Exception:
                    continue
            if not all_vals:
                continue
            vals_concat = np.concatenate(all_vals)
            vals_sorted = np.sort(vals_concat)
            cdf = np.arange(1, len(vals_sorted)+1) / len(vals_sorted)
            plt.plot(vals_sorted, cdf, label=scenario, alpha=0.9)
        # set xlim to show tail (e.g., up to 99.9th percentile across all scenarios)
        all_vals_all = np.concatenate([np.concatenate([pd.read_csv(f,header=None).iloc[:,1].astype(float).values for f in files]) for files in scen_map.values()])
        xmax = np.percentile(all_vals_all, 99.9)
        plt.xlim(0, max(1.0, xmax))
        plt.ylim(0.9, 1.0)
        plt.xlabel('Latency (ms)', fontsize=12)
        plt.ylabel('CDF', fontsize=12)
        plt.title(f'Latency CDF tail (p90-p100) — {req}', fontsize=14)
        plt.legend(fontsize='small')
        plt.grid(alpha=0.2)
        plt.tight_layout()
        out_svg = os.path.join(outdir, f'latency_cdfs_tail_{req}.svg')
        out_png = os.path.join(outdir, f'latency_cdfs_tail_{req}.png')
        plt.savefig(out_svg)
        plt.savefig(out_png, dpi=300)
        plt.close()

    # write percentiles table
    if percentiles_rows:
        pct_df = pd.DataFrame(percentiles_rows).drop_duplicates()
        pct_csv = os.path.join(outdir, 'percentiles.csv')
        pct_df.to_csv(pct_csv, index=False)
        # append to RESULTS.md if present
        results_md = os.path.join(outdir, 'RESULTS.md')
        with open(results_md, 'a') as f:
            f.write('\n## Percentiles per scenario and request group\n')
            f.write(pct_df.to_markdown(index=False))
            f.write('\n')

def plot_resources(resource_csv, outdir):
    try:
        df = pd.read_csv(resource_csv)
        df['timestamp'] = pd.to_datetime(df['timestamp'])
        plt.figure(figsize=(10,4))
        plt.plot(df['timestamp'], df['cpu_percent'])
        plt.xlabel('Time')
        plt.ylabel('CPU %')
        plt.title('CPU over time')
        plt.tight_layout()
        plt.savefig(os.path.join(outdir, 'cpu_time.png'), dpi=200)
        plt.close()
    except Exception:
        pass


def write_results_md(df, outdir):
    # summary table with mean/median/p90/p95/p99 and throughput CI
    from scipy import stats
    grouped = df.groupby(['scenario','totalRequests']).agg(
        runs=('throughput','count'),
        throughput_mean=('throughput','mean'),
        throughput_std=('throughput','std'),
        latency_mean=('avgLatencyMs','mean'),
        latency_std=('avgLatencyMs','std'),
        p99_mean=('p99LatencyMs','mean')
    ).reset_index()
    grouped['ci95'] = grouped.apply(lambda r: (stats.t.ppf(0.975, r['runs']-1) * r['throughput_std'] / (r['runs']**0.5)) if r['runs']>1 else 0.0, axis=1)
    summary_csv = os.path.join(outdir, 'summary.csv')
    grouped.to_csv(summary_csv, index=False)

    md = os.path.join(outdir, 'RESULTS.md')
    with open(md, 'w') as f:
        f.write('# Benchmark Results Summary\n\n')
        f.write('This file summarizes the benchmark sweep. Figures are in this directory.\n\n')
        f.write('## Summary table\n\n')
        f.write(grouped.to_markdown(index=False))
        f.write('\n\n')
        f.write('## Notes\n')
        f.write('- Throughput values show mean and 95% CI where applicable.\n')
        f.write('- Latency percentiles are annotated on the CDF plots.\n')



def write_summary(df, outdir):
    agg = df.groupby(['scenario','totalRequests']).agg(
        runs=('throughput','count'),
        throughput_mean=('throughput','mean'),
        throughput_std=('throughput','std'),
        latency_mean=('avgLatencyMs','mean'),
        latency_std=('avgLatencyMs','std'),
        p99_mean=('p99LatencyMs','mean')
    ).reset_index()
    out = os.path.join(outdir, 'summary.csv')
    agg.to_csv(out, index=False)


def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True, help='Path to aggregate.csv')
    p.add_argument('--outdir', required=False, help='Output directory for figures', default=None)
    args = p.parse_args()

    df = load_csv(args.input)
    outdir = args.outdir or os.path.dirname(args.input)
    figs = os.path.join(outdir, 'figs')
    ensure_outdir(figs)

    # Basic validation
    required = ['scenario','totalRequests','throughput','avgLatencyMs','p99LatencyMs']
    for r in required:
        if r not in df.columns:
            raise SystemExit(f'Missing required column: {r} in {args.input}')

    plot_throughput_vs_requests(df, figs)
    plot_latency_box(df, figs)
    # plot CDFs if per-run latency CSVs exist
    lat_files = []
    for f in os.listdir(os.path.dirname(args.input)):
        if f.endswith('_latencies.csv'):
            lat_files.append(os.path.join(os.path.dirname(args.input), f))
    if lat_files:
        plot_latency_cdf(lat_files, figs)
    # resource sampler file (optional)
    resource_csv = os.path.join(os.path.dirname(args.input), 'resource_usage.csv')
    if os.path.exists(resource_csv):
        plot_resources(resource_csv, figs)
    write_summary(df, figs)

    print('Plots and summary written to', figs)


if __name__ == '__main__':
    main()
