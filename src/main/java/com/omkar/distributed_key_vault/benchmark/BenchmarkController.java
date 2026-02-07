package com.omkar.distributed_key_vault.benchmark;

import org.springframework.web.bind.annotation.*;
import org.springframework.http.ResponseEntity;
import java.util.List;

// REST controller for benchmarking
@RestController
@RequestMapping("/api/benchmark")
public class BenchmarkController {
    @PostMapping("/run")
    public ResponseEntity<List<BenchmarkResult>> runBenchmark(@RequestBody BenchmarkConfig config) throws InterruptedException {
        WorkloadGenerator generator = new WorkloadGenerator(
                WorkloadGenerator.Pattern.valueOf(config.getPattern()),
                config.getTotalRequests(),
                config.getDurationSeconds(),
                config.getBurstSize(),
                config.getRampSteps()
        );
        BenchmarkRunner runner = new BenchmarkRunner(generator, config);
        List<BenchmarkResult> results = runner.run();
        return ResponseEntity.ok(results);
    }

    @PostMapping("/export")
    public ResponseEntity<String> exportCsv(@RequestBody List<BenchmarkResult> results) {
        String csv = BenchmarkCsvExporter.export(results);
        return ResponseEntity.ok(csv);
    }
}
