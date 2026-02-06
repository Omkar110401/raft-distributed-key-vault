package com.omkar.distributed_key_vault.benchmark;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

// Runs benchmarks using the workload generator
public class BenchmarkRunner {
    private final WorkloadGenerator generator;
    private final BenchmarkConfig config;

    public BenchmarkRunner(WorkloadGenerator generator, BenchmarkConfig config) {
        this.generator = generator;
        this.config = config;
    }

    // Runs the benchmark and returns the results
    public List<BenchmarkResult> run() throws InterruptedException {
        List<BenchmarkResult> results = new ArrayList<>();
        BenchmarkResult runResult = new BenchmarkResult();
        long start = System.currentTimeMillis();

        generator.run(() -> {
            long reqStart = System.nanoTime();
            boolean ok = sendRequest();
            long reqEnd = System.nanoTime();
            double latencyMs = (reqEnd - reqStart) / 1_000_000.0;
            runResult.latencies.add(latencyMs);
            MetricService.recordRequest((long) latencyMs, ok);
        });

        long end = System.currentTimeMillis();
        // finalize aggregates
        runResult.scenario = config.getPattern();
        runResult.computeAggregates();
        double durationSec = (end - start) / 1000.0;
        runResult.throughput = runResult.totalRequests / durationSec;
        results.add(runResult);
        return results;
    }

    // Replace with actual request logic
    private boolean sendRequest() {
        try {
            Thread.sleep(5 + (int)(Math.random() * 10));
            return Math.random() > 0.05;
        } catch (InterruptedException e) {
            return false;
        }
    }
}
