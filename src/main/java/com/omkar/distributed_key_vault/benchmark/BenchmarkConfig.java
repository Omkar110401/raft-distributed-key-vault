package com.omkar.distributed_key_vault.benchmark;

// Simple DTO for benchmark parameters with getters expected by controller
public class BenchmarkConfig {
    private String pattern = "CONSTANT";
    private int totalRequests = 100;
    private int durationSeconds = 10;
    private int burstSize = 10;
    private int rampSteps = 5;

    public String getPattern() {
        return pattern;
    }

    public void setPattern(String pattern) {
        this.pattern = pattern;
    }

    public int getTotalRequests() {
        return totalRequests;
    }

    public void setTotalRequests(int totalRequests) {
        this.totalRequests = totalRequests;
    }

    public int getDurationSeconds() {
        return durationSeconds;
    }

    public void setDurationSeconds(int durationSeconds) {
        this.durationSeconds = durationSeconds;
    }

    public int getBurstSize() {
        return burstSize;
    }

    public void setBurstSize(int burstSize) {
        this.burstSize = burstSize;
    }

    public int getRampSteps() {
        return rampSteps;
    }

    public void setRampSteps(int rampSteps) {
        this.rampSteps = rampSteps;
    }
}
