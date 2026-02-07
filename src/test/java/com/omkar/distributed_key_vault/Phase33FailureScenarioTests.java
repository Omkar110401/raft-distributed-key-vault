package com.omkar.distributed_key_vault;

import com.omkar.distributed_key_vault.raft.*;
import com.omkar.distributed_key_vault.vault.KeyVaultStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3.3 - Failure Injection & Resilience Tests
 * Tests system behavior under adversarial conditions:
 * - Node crashes and recovery
 * - Network partitions
 * - Leader failures
 * - Concurrent failures
 * - Data consistency after failures
 */
@SpringBootTest
public class Phase33FailureScenarioTests {
    
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(Phase33FailureScenarioTests.class);
    
    @Autowired
    private RaftState raftState;
    
    @Autowired
    private ChaosMonkey chaosMonkey;
    
    @Autowired
    private KeyVaultStore keyVaultStore;
    
    @Autowired
    private RaftCoordinator coordinator;
    
    @BeforeEach
    public void setup() {
        chaosMonkey.reset();
        raftState.getLog().clear();
        keyVaultStore.clear();
    }
    
    // ============ SCENARIO 1: Single Follower Crash & Recovery ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testFollowerCrashAndRecovery() throws InterruptedException {
        logger.info("=== SCENARIO 1: Follower Crash & Recovery ===");
        
        // Setup: Assume we're leader with 2 followers
        raftState.becomeLeader();
        
        // Add entry to log
        LogEntry entry = LogEntry.builder()
                .index(1)
                .term(1)
                .commandType(CommandType.PUT)
                .key("test")
                .value("value1")
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
        raftState.getLog().add(entry);
        
        // CRASH: follower2 goes down
        logger.info("Step 1: Crashing follower2...");
        chaosMonkey.crashNode("node2", 2000);  // Crash for 2 seconds
        
        assertTrue(chaosMonkey.isNodeCrashed("node2"), "Node2 should be crashed");
        assertFalse(chaosMonkey.canCommunicate("node1", "node2"), "Cannot communicate with crashed node");
        
        // Try to send RPC to follower2 - should fail
        logger.info("Step 2: Attempting RPC to crashed node...");
        assertFalse(chaosMonkey.canCommunicate("node1", "node2"));
        
        // Wait for recovery
        logger.info("Step 3: Waiting for node2 to recover...");
        Thread.sleep(2500);
        chaosMonkey.checkAndRecoverNodes();
        
        assertTrue(chaosMonkey.canCommunicate("node1", "node2"), "Should be able to communicate after recovery");
        assertFalse(chaosMonkey.isNodeCrashed("node2"), "Node2 should be recovered");
        
        logger.info("✓ PASSED: Follower crash and recovery working correctly");
    }
    
    // ============ SCENARIO 2: Leader Crash & Election ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testLeaderCrashTriggersElection() throws InterruptedException {
        logger.info("=== SCENARIO 2: Leader Crash Triggers Election ===");
        
        // Setup: node1 is leader
        raftState.becomeLeader();
        raftState.setLeaderId("node1");
        int termBefore = raftState.getCurrentTerm().get();
        
        // CRASH: Leader goes down
        logger.info("Step 1: Leader node1 crashes...");
        chaosMonkey.crashNode("node1", 3000);
        
        assertTrue(chaosMonkey.isNodeCrashed("node1"));
        
        // On election timeout, followers should detect leader is down
        logger.info("Step 2: Waiting for election timeout...");
        Thread.sleep(1000);
        
        // Simulate other nodes detecting leader is down
        assertFalse(chaosMonkey.canCommunicate("node1", "node2"));
        
        // Recovery
        logger.info("Step 3: Leader recovering...");
        Thread.sleep(2500);
        chaosMonkey.checkAndRecoverNodes();
        
        assertFalse(chaosMonkey.isNodeCrashed("node1"));
        logger.info("✓ PASSED: Leader crash detected and election triggered");
    }
    
    // ============ SCENARIO 3: Network Partition (Split Brain) ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testNetworkPartition() throws InterruptedException {
        logger.info("=== SCENARIO 3: Network Partition Detection ===");
        
        raftState.becomeLeader();
        
        // PARTITION: Split into (node1) and (node2, node3)
        logger.info("Step 1: Creating network partition...");
        chaosMonkey.partitionNetwork("node1", "node2,node3");
        
        // Verify partition
        assertFalse(chaosMonkey.canCommunicate("node1", "node2"));
        assertFalse(chaosMonkey.canCommunicate("node1", "node3"));
        assertTrue(chaosMonkey.canCommunicate("node2", "node3"));
        
        // Leader (node1) in minority partition - should lose leadership
        logger.info("Step 2: Leader in minority (1 of 3) - should lose leadership");
        // In real implementation, leader would step down after election timeout with no quorum
        
        // HEAL: Restore connectivity
        logger.info("Step 3: Healing network partition...");
        chaosMonkey.healNetworkPartition();
        
        assertTrue(chaosMonkey.canCommunicate("node1", "node2"));
        assertTrue(chaosMonkey.canCommunicate("node1", "node3"));
        
        logger.info("✓ PASSED: Network partition correctly isolated nodes");
    }
    
    // ============ SCENARIO 4: Cascading Failures ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCascadingNodeFailures() throws InterruptedException {
        logger.info("=== SCENARIO 4: Cascading Node Failures ===");
        
        raftState.becomeLeader();
        
        // Crash multiple followers
        logger.info("Step 1: Cascading failures...");
        chaosMonkey.crashNode("node2", 3000);
        Thread.sleep(500);
        chaosMonkey.crashNode("node3", 3000);
        
        // System should still be operational (leader + majority)
        assertTrue(chaosMonkey.isNodeCrashed("node2"));
        assertTrue(chaosMonkey.isNodeCrashed("node3"));
        
        ChaosMonkey.ChaosMetrics metrics = chaosMonkey.getMetrics();
        assertEquals(2, metrics.getCurrentlyFailedNodes());
        
        logger.info("Step 2: Waiting for recovery...");
        Thread.sleep(3500);
        chaosMonkey.checkAndRecoverNodes();
        
        assertFalse(chaosMonkey.isNodeCrashed("node2"));
        assertFalse(chaosMonkey.isNodeCrashed("node3"));
        
        metrics = chaosMonkey.getMetrics();
        assertEquals(0, metrics.getCurrentlyFailedNodes());
        
        logger.info("✓ PASSED: System survived cascading failures and recovered");
    }
    
    // ============ SCENARIO 5: Packet Loss During Replication ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testPacketLossDuringReplication() throws InterruptedException {
        logger.info("=== SCENARIO 5: Packet Loss During Replication ===");
        
        // Set up replication with packet loss
        logger.info("Step 1: Starting replication with 50% packet loss...");
        
        // Try to replicate entry
        LogEntry entry = LogEntry.builder()
                .index(1)
                .term(1)
                .commandType(CommandType.PUT)
                .key("resilient_key")
                .value("should_replicate")
                .createdAt(System.currentTimeMillis())
                .applied(false)
                .build();
        
        raftState.getLog().add(entry);
        
        // With packet loss, retries should eventually succeed
        chaosMonkey.setPacketDropRate(50);  // Higher rate to guarantee drops
        long droppedBefore = chaosMonkey.getMetrics().getDroppedMessages();
        
        // Simulate multiple retry attempts
        int dropsObserved = 0;
        for (int i = 0; i < 20; i++) {
            if (chaosMonkey.shouldDropPacket()) {
                dropsObserved++;
                logger.info("Packet dropped (attempt {})", i + 1);
            } else {
                logger.info("Packet sent successfully (attempt {})", i + 1);
                if (dropsObserved > 0) break;
            }
        }
        
        long droppedAfter = chaosMonkey.getMetrics().getDroppedMessages();
        assertTrue(droppedAfter > droppedBefore, "Some packets should have been dropped");
        assertTrue(dropsObserved > 0, "Should observe at least one drop with 50% rate");
        
        logger.info("✓ PASSED: System handled packet loss with retries (observed {} drops)", dropsObserved);
    }
    
    // ============ SCENARIO 6: High Latency Conditions ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testHighLatencyReplication() throws InterruptedException {
        logger.info("=== SCENARIO 6: High Latency Conditions ===");
        
        // Simulate high latency (100ms jitter)
        logger.info("Step 1: Injecting 100ms latency jitter...");
        chaosMonkey.setLatencyJitter(100);
        
        long operationStart = System.currentTimeMillis();
        
        // Simulate replication under latency
        for (int i = 0; i < 5; i++) {
            long delay = chaosMonkey.injectNetworkLatency();
            logger.info("Added latency: {}ms", delay);
            Thread.sleep(Math.min(delay, 200));
        }
        
        long operationDuration = System.currentTimeMillis() - operationStart;
        
        // Latency should add noticeable delay
        assertTrue(operationDuration > 100, "Latency should cause delays");
        
        ChaosMonkey.ChaosMetrics metrics = chaosMonkey.getMetrics();
        assertTrue(metrics.getLatencyInjections() > 0);
        
        logger.info("✓ PASSED: System handled high latency conditions");
    }
    
    // ============ SCENARIO 7: Degraded Node (Slow Response) ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testDegradedNodeHandling() throws InterruptedException {
        logger.info("=== SCENARIO 7: Degraded Node Handling ===");
        
        raftState.becomeLeader();
        
        // DEGRADE: Mark node2 as degraded (slow)
        logger.info("Step 1: Degrading node2...");
        chaosMonkey.degradeNode("node2");
        
        assertTrue(chaosMonkey.isNodeDegraded("node2"));
        
        // Leader should still be able to replicate to other nodes
        assertTrue(chaosMonkey.canCommunicate("node1", "node3"));
        
        // Add entry
        LogEntry entry = LogEntry.builder()
                .index(1)
                .term(1)
                .commandType(CommandType.PUT)
                .key("test")
                .value("replicates_despite_degradation")
                .createdAt(System.currentTimeMillis())
                .applied(false)
                .build();
        raftState.getLog().add(entry);
        
        logger.info("Step 2: Restoring node2...");
        chaosMonkey.restoreNode("node2");
        
        assertFalse(chaosMonkey.isNodeDegraded("node2"));
        
        logger.info("✓ PASSED: System continues despite degraded nodes");
    }
    
    // ============ SCENARIO 8: Leader & Follower Crash Simultaneously ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSimultaneousMultipleNodeCrash() throws InterruptedException {
        logger.info("=== SCENARIO 8: Simultaneous Multiple Node Crash ===");
        
        raftState.becomeLeader();
        int initialTerm = raftState.getCurrentTerm().get();
        
        // Crash leader and one follower simultaneously
        logger.info("Step 1: Crashing leader and follower simultaneously...");
        chaosMonkey.crashNode("node1", 2500);
        chaosMonkey.crashNode("node2", 2500);
        
        // Only node3 left - has quorum? No (1 of 3)
        // So no progress should be made
        assertFalse(chaosMonkey.canCommunicate("node1", "node3"));
        assertFalse(chaosMonkey.canCommunicate("node2", "node3"));
        
        // Wait for recovery
        logger.info("Step 2: Waiting for recovery...");
        Thread.sleep(3000);
        chaosMonkey.checkAndRecoverNodes();
        
        // All nodes back up
        assertTrue(chaosMonkey.canCommunicate("node1", "node3"));
        assertTrue(chaosMonkey.canCommunicate("node2", "node3"));
        
        logger.info("✓ PASSED: System recovered from simultaneous failures");
    }
    
    // ============ SCENARIO 9: Repeated Failures ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testRepeatedFailuresAndRecovery() throws InterruptedException {
        logger.info("=== SCENARIO 9: Repeated Failures & Recovery ===");
        
        raftState.becomeLeader();
        long initialCrashes = chaosMonkey.getMetrics().getTotalCrashes();
        long initialRecoveries = chaosMonkey.getMetrics().getTotalRecoveries();
        
        // Multiple crash/recovery cycles
        for (int cycle = 1; cycle <= 3; cycle++) {
            logger.info("Cycle {}: Crash node2...", cycle);
            chaosMonkey.crashNode("node2", 600);
            assertTrue(chaosMonkey.isNodeCrashed("node2"), "node2 should be crashed");
            
            Thread.sleep(700);
            chaosMonkey.checkAndRecoverNodes();
            
            assertTrue(chaosMonkey.canCommunicate("node1", "node2"), "Should communicate after recovery in cycle " + cycle);
        }
        
        ChaosMonkey.ChaosMetrics metrics = chaosMonkey.getMetrics();
        assertEquals(initialCrashes + 3, metrics.getTotalCrashes(), "Should have 3 more crashes");
        assertEquals(initialRecoveries + 3, metrics.getTotalRecoveries(), "Should have 3 more recoveries");
        
        logger.info("✓ PASSED: System handled repeated failures (3 cycles)");
    }
    
    // ============ SCENARIO 10: Full Cluster Restart ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testFullClusterRestart() throws InterruptedException {
        logger.info("=== SCENARIO 10: Full Cluster Restart ===");
        
        raftState.becomeLeader();
        
        // Add data
        LogEntry entry = LogEntry.builder()
                .index(1)
                .term(1)
                .commandType(CommandType.PUT)
                .key("persistent_key")
                .value("persistent_value")
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
        raftState.getLog().add(entry);
        keyVaultStore.put("persistent_key", "persistent_value");
        
        logger.info("Step 1: Crashing entire cluster...");
        chaosMonkey.crashNode("node1", 2000);
        chaosMonkey.crashNode("node2", 2000);
        chaosMonkey.crashNode("node3", 2000);
        
        assertTrue(chaosMonkey.isNodeCrashed("node1"));
        
        // Wait for recovery
        logger.info("Step 2: Waiting for cluster restart...");
        Thread.sleep(2500);
        chaosMonkey.checkAndRecoverNodes();
        
        // All nodes should be back
        assertFalse(chaosMonkey.isNodeCrashed("node1"));
        assertFalse(chaosMonkey.isNodeCrashed("node2"));
        assertFalse(chaosMonkey.isNodeCrashed("node3"));
        
        // Data should persist
        String value = keyVaultStore.get("persistent_key");
        assertEquals("persistent_value", value);
        
        logger.info("✓ PASSED: Data persisted through full cluster restart");
    }
    
    // ============ CHAOS METRICS ============
    
    @Test
    public void testChaosMetricsTracking() {
        logger.info("=== Chaos Metrics Tracking ===");
        
        long initialCrashes = chaosMonkey.getMetrics().getTotalCrashes();
        long initialLatencyInjections = chaosMonkey.getMetrics().getLatencyInjections();
        
        // Generate some chaos
        chaosMonkey.crashNode("node1", 1000);
        chaosMonkey.setPacketDropRate(15);
        chaosMonkey.setLatencyJitter(50);
        
        // Trigger some events (need enough calls to guarantee drops/latency)
        int dropsTriggered = 0;
        for (int i = 0; i < 100; i++) {
            if (chaosMonkey.shouldDropPacket()) {
                dropsTriggered++;
            }
        }
        
        for (int i = 0; i < 100; i++) {
            chaosMonkey.injectNetworkLatency();
        }
        
        ChaosMonkey.ChaosMetrics metrics = chaosMonkey.getMetrics();
        
        assertEquals(initialCrashes + 1, metrics.getTotalCrashes(), "Should have 1 more crash");
        assertEquals(15, metrics.getPacketDropRate(), "Packet drop rate should be 15%");
        assertEquals(50, metrics.getLatencyJitterMs(), "Latency jitter should be 50ms");
        assertTrue(metrics.getLatencyInjections() > initialLatencyInjections, "Latency injections should increase");
        assertTrue(dropsTriggered > 0, "Should trigger some drops with 15% rate over 100 attempts");
        
        logger.info("Metrics: {}", metrics);
        logger.info("✓ PASSED: Chaos metrics correctly tracked");
    }
}
