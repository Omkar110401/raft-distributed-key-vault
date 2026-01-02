package com.omkar.distributed_key_vault.metrics;

import com.omkar.distributed_key_vault.config.NodeConfig;
import com.omkar.distributed_key_vault.raft.LogEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.LinkedList;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

@Slf4j
@Component
@RequiredArgsConstructor
public class MetricsCollector {
    private final NodeConfig nodeConfig;

    private final Queue<MetricsEvent> eventBuffer = new ConcurrentLinkedQueue<>();
    private static final int MAX_BUFFER_SIZE = 10000;

    // Leadership events
    public void recordLeadershipChange(String previousLeader, String newLeader, int term) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("LEADERSHIP_CHANGE")
                .term(term)
                .previousValue(previousLeader)
                .newValue(newLeader)
                .build());
    }

    public void recordElectionStart(int term, String reason) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("ELECTION_START")
                .term(term)
                .details(reason)
                .build());
    }

    public void recordElectionEnd(int term, String result, Integer votesReceived) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("ELECTION_END")
                .term(term)
                .details(result + (votesReceived != null ? " (votes: " + votesReceived + ")" : ""))
                .build());
    }

    public void recordRoleChange(String oldRole, String newRole, int term) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("ROLE_CHANGE")
                .term(term)
                .previousValue(oldRole)
                .newValue(newRole)
                .build());
    }

    public void recordTermChange(int oldTerm, int newTerm, String reason) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("TERM_CHANGE")
                .previousValue(String.valueOf(oldTerm))
                .newValue(String.valueOf(newTerm))
                .details(reason)
                .build());
    }

    // Write request events
    public void recordWriteRequest(String key, String result, long latencyMs) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("WRITE_REQUEST")
                .previousValue(key)
                .newValue(result)
                .latencyMs(latencyMs)
                .build());
    }

    // Read request events
    public void recordReadRequest(String key, String result, long latencyMs) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("READ_REQUEST")
                .previousValue(key)
                .newValue(result)
                .latencyMs(latencyMs)
                .build());
    }

    // State machine apply events
    public void recordStateApply(LogEntry entry, long durationMs) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("STATE_MACHINE_APPLY")
                .term(entry.getTerm())
                .latencyMs(durationMs)
                .details("index=" + entry.getIndex() + ",type=" + entry.getCommandType())
                .build());
    }

    // Log replication events
    public void recordLogReplicationStart(int entryIndex, int entryTerm) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("LOG_REPLICATION_START")
                .term(entryTerm)
                .details("index=" + entryIndex)
                .build());
    }

    public void recordLogReplicationAck(String followerId, int entryIndex, long latencyMs) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("LOG_REPLICATION_ACK")
                .latencyMs(latencyMs)
                .details("follower=" + followerId + ",index=" + entryIndex)
                .build());
    }

    public void recordLogCommit(int entryIndex, int entryTerm, long timeSinceCreationMs, int followersAckedCount) {
        addEvent(MetricsEvent.builder()
                .timestamp(Instant.now())
                .nodeId(nodeConfig.getNodeId())
                .eventType("LOG_COMMIT")
                .term(entryTerm)
                .latencyMs(timeSinceCreationMs)
                .details("index=" + entryIndex + ",followers_acked=" + followersAckedCount)
                .build());
    }

    // Private helper
    private void addEvent(MetricsEvent event) {
        if (eventBuffer.size() >= MAX_BUFFER_SIZE) {
            eventBuffer.poll(); // Remove oldest if buffer full
        }
        eventBuffer.offer(event);
    }

    // Export methods
    public List<MetricsEvent> getAllEvents() {
        return new LinkedList<>(eventBuffer);
    }

    public String exportAsCSV() {
        StringBuilder csv = new StringBuilder();
        csv.append(MetricsEvent.csvHeader()).append("\n");

        for (MetricsEvent event : eventBuffer) {
            csv.append(event.toCSV()).append("\n");
        }

        return csv.toString();
    }

    public void clearBuffer() {
        eventBuffer.clear();
    }

    public int getBufferSize() {
        return eventBuffer.size();
    }
}
