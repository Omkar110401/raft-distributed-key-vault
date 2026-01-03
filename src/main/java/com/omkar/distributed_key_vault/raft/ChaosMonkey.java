package com.omkar.distributed_key_vault.raft;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Phase 3.3 - Chaos Monkey for Failure Injection
 * Deliberately introduces failures to test resilience:
 * - Node crashes
 * - Network partitions
 * - Latency/jitter
 * - Packet loss
 * - Message delays
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class ChaosMonkey {
    
    private final Random random = new Random();
    
    // Failure state tracking
    private final Map<String, NodeFailureState> nodeFailureState = new ConcurrentHashMap<>();
    private final Map<String, Long> crashStartTime = new ConcurrentHashMap<>();
    private final Map<String, Long> recoverTime = new ConcurrentHashMap<>();
    
    // Configuration
    private volatile int packetDropRate = 0;      // 0-100 percent
    private volatile int latencyJitterMs = 0;     // Added to RTT
    private volatile boolean networkPartitioned = false;
    private final Set<String> partitionGroup = new HashSet<>();
    
    // Metrics
    private volatile long totalCrashes = 0;
    private volatile long totalRecoveries = 0;
    private volatile long droppedMessages = 0;
    private volatile long latencyInjections = 0;
    
    /**
     * Node failure state enum
     */
    public enum NodeFailureState {
        HEALTHY,        // Node operating normally
        CRASHED,        // Node down (not responding)
        RECOVERING,     // Node restarting
        DEGRADED,       // Node slow/unreliable
        PARTITIONED     // Network isolated
    }
    
    // ============ CRASH SCENARIOS ============
    
    /**
     * Crash a node (immediate failure)
     */
    public void crashNode(String nodeId, long durationMs) {
        nodeFailureState.put(nodeId, NodeFailureState.CRASHED);
        crashStartTime.put(nodeId, System.currentTimeMillis());
        recoverTime.put(nodeId, System.currentTimeMillis() + durationMs);
        totalCrashes++;
        
        log.warn("CHAOS: Node {} crashed - will recover in {}ms", nodeId, durationMs);
    }
    
    /**
     * Recover a crashed node
     */
    public void recoverNode(String nodeId) {
        if (nodeFailureState.getOrDefault(nodeId, NodeFailureState.HEALTHY) == NodeFailureState.CRASHED) {
            nodeFailureState.put(nodeId, NodeFailureState.HEALTHY);
            crashStartTime.remove(nodeId);
            recoverTime.remove(nodeId);
            totalRecoveries++;
            
            long crashDuration = System.currentTimeMillis() - crashStartTime.getOrDefault(nodeId, System.currentTimeMillis());
            log.info("CHAOS: Node {} recovered - was down for {}ms", nodeId, crashDuration);
        }
    }
    
    /**
     * Automatically recover crashed nodes when their duration expires
     */
    public void checkAndRecoverNodes() {
        long now = System.currentTimeMillis();
        
        nodeFailureState.forEach((nodeId, state) -> {
            if (state == NodeFailureState.CRASHED) {
                Long recoveryTime = recoverTime.get(nodeId);
                if (recoveryTime != null && now >= recoveryTime) {
                    recoverNode(nodeId);
                }
            }
        });
    }
    
    // ============ NETWORK PARTITION SCENARIOS ============
    
    /**
     * Create network partition: split cluster into isolated groups
     * E.g., partition("node1,node2", "node3") = node1,node2 can talk to each other
     *       but cannot reach node3
     */
    public void partitionNetwork(String group1, String group2) {
        networkPartitioned = true;
        partitionGroup.addAll(Arrays.asList(group1.split(",")));
        
        log.warn("CHAOS: Network partition created");
        log.warn("  Group A (can communicate): {}", group1);
        log.warn("  Group B (isolated): {}", group2);
    }
    
    /**
     * Heal network partition
     */
    public void healNetworkPartition() {
        if (networkPartitioned) {
            networkPartitioned = false;
            partitionGroup.clear();
            log.info("CHAOS: Network partition healed");
        }
    }
    
    /**
     * Check if two nodes can communicate (for message routing)
     */
    public boolean canCommunicate(String fromNode, String toNode) {
        // Check if nodes are in crashed state
        if (isNodeCrashed(fromNode) || isNodeCrashed(toNode)) {
            return false;
        }
        
        // Check if network is partitioned
        if (networkPartitioned) {
            boolean fromInGroup = partitionGroup.contains(fromNode.trim());
            boolean toInGroup = partitionGroup.contains(toNode.trim());
            
            // Can only communicate if both in same group
            if (fromInGroup != toInGroup) {
                return false;
            }
        }
        
        return true;
    }
    
    // ============ LATENCY & JITTER INJECTION ============
    
    /**
     * Simulate network latency by adding delay
     */
    public long injectNetworkLatency() {
        if (latencyJitterMs > 0) {
            long delay = (long) (random.nextDouble() * latencyJitterMs);
            latencyInjections++;
            return delay;
        }
        return 0;
    }
    
    /**
     * Set packet drop rate (0-100%)
     */
    public void setPacketDropRate(int rate) {
        if (rate < 0 || rate > 100) {
            throw new IllegalArgumentException("Rate must be 0-100");
        }
        this.packetDropRate = rate;
        log.warn("CHAOS: Packet drop rate set to {}%", rate);
    }
    
    /**
     * Check if packet should be dropped
     */
    public boolean shouldDropPacket() {
        if (packetDropRate > 0) {
            int roll = random.nextInt(100);
            if (roll < packetDropRate) {
                droppedMessages++;
                return true;
            }
        }
        return false;
    }
    
    /**
     * Set latency jitter (ms added to messages)
     */
    public void setLatencyJitter(int jitterMs) {
        this.latencyJitterMs = jitterMs;
        log.warn("CHAOS: Latency jitter set to {}ms", jitterMs);
    }
    
    // ============ DEGRADATION SCENARIOS ============
    
    /**
     * Mark node as degraded (slow/unreliable)
     */
    public void degradeNode(String nodeId) {
        nodeFailureState.put(nodeId, NodeFailureState.DEGRADED);
        log.warn("CHAOS: Node {} degraded - slow responses", nodeId);
    }
    
    /**
     * Restore node from degraded state
     */
    public void restoreNode(String nodeId) {
        nodeFailureState.put(nodeId, NodeFailureState.HEALTHY);
        log.info("CHAOS: Node {} restored", nodeId);
    }
    
    /**
     * Check if node is degraded
     */
    public boolean isNodeDegraded(String nodeId) {
        return nodeFailureState.getOrDefault(nodeId, NodeFailureState.HEALTHY) == NodeFailureState.DEGRADED;
    }
    
    // ============ STATE QUERIES ============
    
    /**
     * Check if node is crashed
     */
    public boolean isNodeCrashed(String nodeId) {
        return nodeFailureState.getOrDefault(nodeId, NodeFailureState.HEALTHY) == NodeFailureState.CRASHED;
    }
    
    /**
     * Get node failure state
     */
    public NodeFailureState getNodeState(String nodeId) {
        return nodeFailureState.getOrDefault(nodeId, NodeFailureState.HEALTHY);
    }
    
    /**
     * Check how long node has been crashed
     */
    public long getCrashDuration(String nodeId) {
        if (isNodeCrashed(nodeId)) {
            return System.currentTimeMillis() - crashStartTime.getOrDefault(nodeId, System.currentTimeMillis());
        }
        return 0;
    }
    
    /**
     * Reset all chaos (return to healthy state)
     */
    public void reset() {
        nodeFailureState.clear();
        crashStartTime.clear();
        recoverTime.clear();
        networkPartitioned = false;
        partitionGroup.clear();
        packetDropRate = 0;
        latencyJitterMs = 0;
        
        log.info("CHAOS: All chaos reset - cluster returned to healthy state");
    }
    
    // ============ METRICS ============
    
    /**
     * Get comprehensive chaos metrics
     */
    public ChaosMetrics getMetrics() {
        return ChaosMetrics.builder()
                .totalCrashes(totalCrashes)
                .totalRecoveries(totalRecoveries)
                .currentlyFailedNodes(countFailedNodes())
                .droppedMessages(droppedMessages)
                .latencyInjections(latencyInjections)
                .packetDropRate(packetDropRate)
                .latencyJitterMs(latencyJitterMs)
                .networkPartitioned(networkPartitioned)
                .failedNodeStates(new HashMap<>(nodeFailureState))
                .build();
    }
    
    /**
     * Count currently failed/degraded nodes
     */
    private int countFailedNodes() {
        return (int) nodeFailureState.values().stream()
                .filter(state -> state != NodeFailureState.HEALTHY)
                .count();
    }
    
    /**
     * Get chaos status summary
     */
    @lombok.Data
    @lombok.Builder
    public static class ChaosMetrics {
        private long totalCrashes;
        private long totalRecoveries;
        private int currentlyFailedNodes;
        private long droppedMessages;
        private long latencyInjections;
        private int packetDropRate;
        private int latencyJitterMs;
        private boolean networkPartitioned;
        private Map<String, NodeFailureState> failedNodeStates;
    }
}
