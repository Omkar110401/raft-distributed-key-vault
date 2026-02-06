package com.omkar.distributed_key_vault.benchmark;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.stream.Collectors;

public class BenchmarkResult {
    public String scenario;
    public int totalRequests;
    public double throughput;
    public double avgLatencyMs;
    public double p99LatencyMs;
    public double maxLatencyMs;
    public long memoryUsedBytes;
    public List<Double> latencies = new ArrayList<>();

    public String toCsv() {
        return String.join(",",
            scenario,
            String.valueOf(totalRequests),
            String.format("%.2f", throughput),
            String.format("%.2f", avgLatencyMs),
            String.format("%.2f", p99LatencyMs),
            String.format("%.2f", maxLatencyMs),
            String.valueOf(memoryUsedBytes)
        );
    }

    public static String csvHeader() {
        return "scenario,totalRequests,throughput,avgLatencyMs,p99LatencyMs,maxLatencyMs,memoryUsedBytes";
    }

    public void computeAggregates() {
        this.totalRequests = latencies.size();
        if (latencies.isEmpty()) return;
        double sum = 0.0;
        double max = Double.MIN_VALUE;
        for (double v : latencies) {
            sum += v;
            if (v > max) max = v;
        }
        this.avgLatencyMs = sum / latencies.size();
        List<Double> sorted = latencies.stream().sorted().collect(Collectors.toList());
        this.maxLatencyMs = max;
        int idx99 = (int) Math.ceil(0.99 * sorted.size()) - 1;
        idx99 = Math.max(0, Math.min(idx99, sorted.size() - 1));
        this.p99LatencyMs = sorted.get(idx99);
        // throughput placeholder: requests per second over test duration; set by runner
    }
}
