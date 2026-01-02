package com.omkar.distributed_key_vault.metrics;

import lombok.RequiredArgsConstructor;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/metrics")
@RequiredArgsConstructor
public class MetricsController {
    private final MetricsCollector metricsCollector;

    @GetMapping("/events")
    public ResponseEntity<?> getEvents() {
        return ResponseEntity.ok(metricsCollector.getAllEvents());
    }

    @GetMapping("/export")
    public ResponseEntity<String> exportCSV() {
        String csv = metricsCollector.exportAsCSV();
        return ResponseEntity.ok()
                .header(HttpHeaders.CONTENT_DISPOSITION, "attachment; filename=\"metrics.csv\"")
                .contentType(MediaType.parseMediaType("text/csv"))
                .body(csv);
    }

    @GetMapping("/status")
    public ResponseEntity<?> getMetricsStatus() {
        return ResponseEntity.ok(new Object() {
            public int bufferSize = metricsCollector.getBufferSize();
            public int maxSize = 10000;
        });
    }

    @DeleteMapping("/clear")
    public ResponseEntity<?> clearMetrics() {
        metricsCollector.clearBuffer();
        return ResponseEntity.ok("Metrics cleared");
    }
}
