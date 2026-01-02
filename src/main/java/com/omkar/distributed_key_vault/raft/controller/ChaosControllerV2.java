package com.omkar.distributed_key_vault.raft.controller;

import com.omkar.distributed_key_vault.raft.ChaosMonkey;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.web.bind.annotation.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Phase 3.3 - Chaos Control API
 * REST endpoints to inject failures during testing
 * Useful for failure scenario demonstrations
 */
@Slf4j
@RestController
@RequestMapping("/chaos")
@RequiredArgsConstructor
public class ChaosControllerV2 {
    
    private final ChaosMonkey chaosMonkey;
    
    // ============ NODE CRASH OPERATIONS ============
    
    @PostMapping("/crash")
    public Map<String, Object> crashNode(
            @RequestParam String nodeId,
            @RequestParam(defaultValue = "5000") long durationMs
    ) {
        chaosMonkey.crashNode(nodeId, durationMs);
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "CRASH");
        response.put("nodeId", nodeId);
        response.put("durationMs", durationMs);
        response.put("message", "Node " + nodeId + " will crash for " + durationMs + "ms");
        
        return response;
    }
    
    @PostMapping("/recover")
    public Map<String, Object> recoverNode(@RequestParam String nodeId) {
        chaosMonkey.recoverNode(nodeId);
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "RECOVER");
        response.put("nodeId", nodeId);
        response.put("message", "Node " + nodeId + " recovered");
        
        return response;
    }
    
    @PostMapping("/crash-cascade")
    public Map<String, Object> cascadingCrash(
            @RequestParam String nodeIds,
            @RequestParam(defaultValue = "5000") long durationMs,
            @RequestParam(defaultValue = "500") long delayBetweenMs
    ) {
        String[] nodes = nodeIds.split(",");
        
        for (int i = 0; i < nodes.length; i++) {
            final int index = i;
            new Thread(() -> {
                try {
                    Thread.sleep(index * delayBetweenMs);
                    chaosMonkey.crashNode(nodes[index].trim(), durationMs);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }).start();
        }
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "CASCADE_CRASH");
        response.put("nodeIds", nodes);
        response.put("message", "Cascading crash initiated for: " + nodeIds);
        
        return response;
    }
    
    // ============ NETWORK PARTITION OPERATIONS ============
    
    @PostMapping("/partition")
    public Map<String, Object> partitionNetwork(
            @RequestParam String group1,
            @RequestParam String group2
    ) {
        chaosMonkey.partitionNetwork(group1, group2);
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "PARTITION");
        response.put("group1", group1);
        response.put("group2", group2);
        response.put("message", "Network partitioned: " + group1 + " <-> " + group2);
        
        return response;
    }
    
    @PostMapping("/partition/heal")
    public Map<String, Object> healPartition() {
        chaosMonkey.healNetworkPartition();
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "HEAL_PARTITION");
        response.put("message", "Network partition healed");
        
        return response;
    }
    
    @GetMapping("/partition/status")
    public Map<String, Object> getPartitionStatus() {
        ChaosMonkey.ChaosMetrics metrics = chaosMonkey.getMetrics();
        
        Map<String, Object> response = new HashMap<>();
        response.put("networkPartitioned", metrics.isNetworkPartitioned());
        response.put("failedNodes", metrics.getCurrentlyFailedNodes());
        
        return response;
    }
    
    // ============ LATENCY & PACKET LOSS ============
    
    @PostMapping("/latency")
    public Map<String, Object> setLatency(@RequestParam int jitterMs) {
        chaosMonkey.setLatencyJitter(jitterMs);
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "SET_LATENCY");
        response.put("latencyJitterMs", jitterMs);
        response.put("message", "Latency jitter set to " + jitterMs + "ms");
        
        return response;
    }
    
    @PostMapping("/packet-loss")
    public Map<String, Object> setPacketLoss(@RequestParam int dropRate) {
        if (dropRate < 0 || dropRate > 100) {
            Map<String, Object> error = new HashMap<>();
            error.put("error", "Drop rate must be 0-100");
            return error;
        }
        
        chaosMonkey.setPacketDropRate(dropRate);
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "SET_PACKET_LOSS");
        response.put("dropRate", dropRate);
        response.put("message", "Packet drop rate set to " + dropRate + "%");
        
        return response;
    }
    
    // ============ NODE DEGRADATION ============
    
    @PostMapping("/degrade")
    public Map<String, Object> degradeNode(@RequestParam String nodeId) {
        chaosMonkey.degradeNode(nodeId);
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "DEGRADE");
        response.put("nodeId", nodeId);
        response.put("message", "Node " + nodeId + " degraded (slow)");
        
        return response;
    }
    
    @PostMapping("/restore")
    public Map<String, Object> restoreNode(@RequestParam String nodeId) {
        chaosMonkey.restoreNode(nodeId);
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "RESTORE");
        response.put("nodeId", nodeId);
        response.put("message", "Node " + nodeId + " restored");
        
        return response;
    }
    
    // ============ STATE & METRICS ============
    
    @GetMapping("/status")
    public Map<String, Object> getChaosStatus() {
        ChaosMonkey.ChaosMetrics metrics = chaosMonkey.getMetrics();
        
        Map<String, Object> response = new HashMap<>();
        response.put("totalCrashes", metrics.getTotalCrashes());
        response.put("totalRecoveries", metrics.getTotalRecoveries());
        response.put("currentlyFailedNodes", metrics.getCurrentlyFailedNodes());
        response.put("droppedMessages", metrics.getDroppedMessages());
        response.put("latencyInjections", metrics.getLatencyInjections());
        response.put("packetDropRate", metrics.getPacketDropRate());
        response.put("latencyJitterMs", metrics.getLatencyJitterMs());
        response.put("networkPartitioned", metrics.isNetworkPartitioned());
        
        return response;
    }
    
    @GetMapping("/metrics")
    public Map<String, Object> getMetrics() {
        return getChaosStatus();
    }
    
    @GetMapping("/node-states")
    public Map<String, Object> getNodeStates() {
        ChaosMonkey.ChaosMetrics metrics = chaosMonkey.getMetrics();
        
        Map<String, Object> response = new HashMap<>();
        response.put("nodeStates", metrics.getFailedNodeStates());
        
        return response;
    }
    
    // ============ RESET ============
    
    @PostMapping("/reset")
    public Map<String, Object> reset() {
        chaosMonkey.reset();
        
        Map<String, Object> response = new HashMap<>();
        response.put("action", "RESET");
        response.put("message", "All chaos cleared - cluster returned to healthy state");
        
        return response;
    }
    
    // ============ HELPER: CHECK COMMUNICATION ============
    
    @GetMapping("/can-communicate")
    public Map<String, Object> canCommunicate(
            @RequestParam String fromNode,
            @RequestParam String toNode
    ) {
        boolean canComm = chaosMonkey.canCommunicate(fromNode, toNode);
        
        Map<String, Object> response = new HashMap<>();
        response.put("fromNode", fromNode);
        response.put("toNode", toNode);
        response.put("canCommunicate", canComm);
        
        return response;
    }
}
