package com.omkar.distributed_key_vault.metrics;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;

import java.time.Instant;

@Data
@Builder
@AllArgsConstructor
public class MetricsEvent {
    private Instant timestamp;
    private String nodeId;
    private String eventType;          // LEADERSHIP_CHANGE, ELECTION_START, WRITE_REQUEST, etc.
    private Integer term;
    private String previousValue;
    private String newValue;
    private Long latencyMs;
    private String details;

    public String toCSV() {
        return String.format("%s,%s,%s,%s,%s,%s,%s,%s",
                timestamp,
                nodeId,
                eventType,
                term != null ? term : "",
                previousValue != null ? previousValue : "",
                newValue != null ? newValue : "",
                latencyMs != null ? latencyMs : "",
                details != null ? details : ""
        );
    }

    public static String csvHeader() {
        return "timestamp,node_id,event_type,term,previous_value,new_value,latency_ms,details";
    }
}
