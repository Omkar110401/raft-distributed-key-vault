package com.omkar.distributed_key_vault.raft;

import org.springframework.stereotype.Component;
import java.util.concurrent.ConcurrentHashMap;
import java.util.Map;
import java.util.LongSummaryStatistics;
import java.util.stream.Collectors;

/**
 * Tracks replication performance metrics for Phase 3.2
 * Monitors log replication lag, success rates, and commit progress
 */
@Component
public class ReplicationMetrics {
    
    // Track replication lag per follower in milliseconds
    private final Map<String, Long> replicationLag = new ConcurrentHashMap<>();
    
    // Track match index (how many entries each follower has)
    private final Map<String, Integer> matchIndexProgress = new ConcurrentHashMap<>();
    
    // Count successful replications per follower
    private final Map<String, Long> replicationSuccessCount = new ConcurrentHashMap<>();
    
    // Track failures/retries per follower
    private final Map<String, Long> replicationFailureCount = new ConcurrentHashMap<>();
    
    // Count how many times commitIndex advanced
    private volatile long commitIndexAdvances = 0;
    
    // Count total commands applied to state machine
    private volatile long commandsApplied = 0;
    
    // Track average replication latency across all operations
    private volatile double averageReplicationLatency = 0.0;
    
    /**
     * Record when an entry is sent for replication
     */
    public void recordReplicationSent(String nodeId, int entryIndex, long timestamp) {
        // Will calculate lag when ACK received
    }
    
    /**
     * Record when replication ACK is received from follower
     */
    public void recordReplicationAck(String nodeId, int matchIndex, long latencyMs) {
        matchIndexProgress.put(nodeId, matchIndex);
        replicationLag.put(nodeId, latencyMs);
        replicationSuccessCount.merge(nodeId, 1L, Long::sum);
        
        // Update running average
        updateAverageLatency(latencyMs);
    }
    
    /**
     * Record replication failure (log divergence)
     */
    public void recordReplicationFailure(String nodeId) {
        replicationFailureCount.merge(nodeId, 1L, Long::sum);
    }
    
    /**
     * Record when commitIndex advances (majority has entries)
     */
    public void recordCommitIndexAdvance(int oldCommitIndex, int newCommitIndex) {
        commitIndexAdvances++;
    }
    
    /**
     * Record when command is applied to state machine
     */
    public void recordCommandApplied(String commandType) {
        commandsApplied++;
    }
    
    /**
     * Get current replication lag for a specific follower
     */
    public long getReplicationLag(String nodeId) {
        return replicationLag.getOrDefault(nodeId, 0L);
    }
    
    /**
     * Get average replication lag across all followers
     */
    public double getAverageReplicationLag() {
        if (replicationLag.isEmpty()) return 0.0;
        return replicationLag.values().stream()
            .mapToLong(Long::longValue)
            .average()
            .orElse(0.0);
    }
    
    /**
     * Get max replication lag (slowest follower)
     */
    public long getMaxReplicationLag() {
        return replicationLag.values().stream()
            .mapToLong(Long::longValue)
            .max()
            .orElse(0L);
    }
    
    /**
     * Get match index (replication progress) for a follower
     */
    public int getMatchIndex(String nodeId) {
        return matchIndexProgress.getOrDefault(nodeId, 0);
    }
    
    /**
     * Get all followers' replication progress
     */
    public Map<String, Integer> getAllMatchIndices() {
        return new ConcurrentHashMap<>(matchIndexProgress);
    }
    
    /**
     * Get replication success count for a follower
     */
    public long getReplicationSuccessCount(String nodeId) {
        return replicationSuccessCount.getOrDefault(nodeId, 0L);
    }
    
    /**
     * Get replication failure count for a follower
     */
    public long getReplicationFailureCount(String nodeId) {
        return replicationFailureCount.getOrDefault(nodeId, 0L);
    }
    
    /**
     * Get total times commitIndex advanced
     */
    public long getCommitIndexAdvances() {
        return commitIndexAdvances;
    }
    
    /**
     * Get total commands applied to state machine
     */
    public long getCommandsApplied() {
        return commandsApplied;
    }
    
    /**
     * Get average replication latency
     */
    public double getAverageReplicationLatency() {
        return averageReplicationLatency;
    }
    
    /**
     * Get replication health summary
     */
    public String getHealthSummary() {
        StringBuilder sb = new StringBuilder();
        sb.append("Replication Health:\n");
        sb.append("  Avg Lag: ").append(String.format("%.2f", getAverageReplicationLag())).append("ms\n");
        sb.append("  Max Lag: ").append(getMaxReplicationLag()).append("ms\n");
        sb.append("  Commit Advances: ").append(commitIndexAdvances).append("\n");
        sb.append("  Commands Applied: ").append(commandsApplied).append("\n");
        sb.append("  Per Follower:\n");
        
        matchIndexProgress.forEach((nodeId, matchIndex) -> {
            long lag = getReplicationLag(nodeId);
            long success = getReplicationSuccessCount(nodeId);
            long failures = getReplicationFailureCount(nodeId);
            sb.append("    ").append(nodeId).append(": ")
              .append("matchIndex=").append(matchIndex)
              .append(" lag=").append(lag).append("ms")
              .append(" success=").append(success)
              .append(" failures=").append(failures).append("\n");
        });
        
        return sb.toString();
    }
    
    /**
     * Reset all metrics
     */
    public void reset() {
        replicationLag.clear();
        matchIndexProgress.clear();
        replicationSuccessCount.clear();
        replicationFailureCount.clear();
        commitIndexAdvances = 0;
        commandsApplied = 0;
        averageReplicationLatency = 0.0;
    }
    
    private void updateAverageLatency(long latencyMs) {
        // Simple running average
        if (averageReplicationLatency == 0.0) {
            averageReplicationLatency = latencyMs;
        } else {
            averageReplicationLatency = (averageReplicationLatency * 0.9) + (latencyMs * 0.1);
        }
    }
}
