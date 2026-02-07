package com.omkar.distributed_key_vault.benchmark;

import java.io.FileWriter;
import java.io.IOException;
import java.util.List;

public class BenchmarkCsvExporter {
    // Export aggregate CSV (one line per run)
    public static String export(List<BenchmarkResult> results) {
        StringBuilder sb = new StringBuilder();
        sb.append(BenchmarkResult.csvHeader()).append("\n");
        for (BenchmarkResult r : results) {
            sb.append(r.toCsv()).append("\n");
        }
        return sb.toString();
    }

    // Write per-request latencies to a CSV file (run-specific)
    public static void writeLatenciesCsv(BenchmarkResult result, String filePath) throws IOException {
        try (FileWriter writer = new FileWriter(filePath)) {
            writer.write("requestIndex,latencyMs\n");
            int i = 0;
            for (double l : result.latencies) {
                writer.write(i + "," + String.format("%.3f", l) + "\n");
                i++;
            }
        }
    }
}
