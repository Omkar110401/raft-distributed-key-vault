package com.omkar.distributed_key_vault.benchmark;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.Counter;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class MetricService {
    private static MeterRegistry registry;
    private final Counter requestCounter;
    private final Timer latencyTimer;

    public MetricService(MeterRegistry registry) {
        MetricService.registry = registry;
        this.requestCounter = registry.counter("benchmark.requests.total");
        this.latencyTimer = registry.timer("benchmark.request.latency.ms");
    }

    public static void recordRequest(long durationMs, boolean success) {
        if (registry == null) return;
        registry.counter("benchmark.requests.total").increment();
        registry.timer("benchmark.request.latency.ms").record(durationMs, TimeUnit.MILLISECONDS);
        if (!success) registry.counter("benchmark.requests.failures").increment();
    }
}
