package com.omkar.distributed_key_vault.benchmark;

import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

// Generates different types of workloads for benchmarking
public class WorkloadGenerator {
    private final Random random = new Random();

    public enum Pattern {
        CONSTANT, BURST, RAMP_UP
    }

    private final Pattern pattern;
    private final int totalRequests;
    private final int durationSeconds;
    private final int burstSize;
    private final int rampSteps;

    public WorkloadGenerator(Pattern pattern, int totalRequests, int durationSeconds, int burstSize, int rampSteps) {
        this.pattern = pattern;
        this.totalRequests = totalRequests;
        this.durationSeconds = durationSeconds;
        this.burstSize = burstSize;
        this.rampSteps = rampSteps;
    }

    // Runs the workload, calling the provided Runnable for each request
    public void run(Runnable requestTask) throws InterruptedException {
        switch (pattern) {
            case CONSTANT:
                runConstant(requestTask);
                break;
            case BURST:
                runBurst(requestTask);
                break;
            case RAMP_UP:
                runRampUp(requestTask);
                break;
        }
    }

    private void runConstant(Runnable requestTask) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(8);
        long interval = (durationSeconds * 1000L) / totalRequests;
        for (int i = 0; i < totalRequests; i++) {
            executor.submit(requestTask);
            Thread.sleep(interval);
        }
        executor.shutdown();
        executor.awaitTermination(durationSeconds + 5, TimeUnit.SECONDS);
    }

    private void runBurst(Runnable requestTask) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(16);
        int bursts = Math.max(1, totalRequests / burstSize);
        for (int b = 0; b < bursts; b++) {
            for (int i = 0; i < burstSize && (b * burstSize + i) < totalRequests; i++) {
                executor.submit(requestTask);
            }
            Thread.sleep((durationSeconds * 1000L) / bursts);
        }
        executor.shutdown();
        executor.awaitTermination(durationSeconds + 5, TimeUnit.SECONDS);
    }

    private void runRampUp(Runnable requestTask) throws InterruptedException {
        ExecutorService executor = Executors.newFixedThreadPool(16);
        int stepRequests = totalRequests / rampSteps;
        for (int s = 1; s <= rampSteps; s++) {
            for (int i = 0; i < stepRequests; i++) {
                executor.submit(requestTask);
            }
            Thread.sleep((durationSeconds * 1000L) / rampSteps);
        }
        executor.shutdown();
        executor.awaitTermination(durationSeconds + 5, TimeUnit.SECONDS);
    }
}
