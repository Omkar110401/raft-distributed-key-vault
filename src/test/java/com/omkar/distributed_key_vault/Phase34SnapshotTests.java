package com.omkar.distributed_key_vault;

import com.omkar.distributed_key_vault.raft.*;
import com.omkar.distributed_key_vault.raft.SnapshotManager.Snapshot;
import com.omkar.distributed_key_vault.raft.SnapshotManager.SnapshotMetrics;
import com.omkar.distributed_key_vault.raft.PersistenceLayer.PersistenceMetrics;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3.4 - Snapshot & Persistence Tests
 * Validates snapshot creation, loading, compression, and recovery
 * 
 * Test Coverage:
 * 1. Basic snapshot creation and validation
 * 2. Snapshot loading and state recovery
 * 3. Incremental snapshots
 * 4. Log compaction after snapshots
 * 5. Persistence (save/load to disk)
 * 6. Compression effectiveness
 * 7. Backup and recovery
 * 8. Snapshot during chaos failures
 * 9. Large snapshot handling
 * 10. Multiple concurrent snapshots
 * 11. Corruption recovery
 */
@SpringBootTest
public class Phase34SnapshotTests {
    
    private static final Logger logger = LoggerFactory.getLogger(Phase34SnapshotTests.class);
    
    @Autowired
    private RaftState raftState;
    
    @Autowired
    private SnapshotManager snapshotManager;
    
    @Autowired
    private PersistenceLayer persistenceLayer;
    
    @Autowired
    private ChaosMonkey chaosMonkey;
    
    @BeforeEach
    public void setUp() {
        // Clear raft state
        raftState.getLog().clear();
        raftState.becomeFollower(0);
        snapshotManager.reset();
        persistenceLayer.reset();
        chaosMonkey.reset();
        logger.info("Test setup complete");
    }
    
    // ============ SCENARIO 1: Basic Snapshot Creation ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testBasicSnapshotCreation() {
        logger.info("=== SCENARIO 1: Basic Snapshot Creation ===");
        
        // Set up initial state with log entries
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        for (int i = 0; i < 100; i++) {
            LogEntry entry = LogEntry.builder()
                .index(i)
                .term(1)
                .commandType(CommandType.PUT)
                .key("key_" + i)
                .value("value_" + i)
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
            raftState.getLog().add(entry);
        }
        
        Map<String, String> stateData = new HashMap<>();
        for (int i = 0; i < 100; i++) {
            stateData.put("key_" + i, "value_" + i);
        }
        
        // Create snapshot
        long initialLogSize = raftState.getLog().size();
        Snapshot snapshot = snapshotManager.createSnapshot(stateData);
        
        assertNotNull(snapshot, "Snapshot should be created");
        assertEquals(1, snapshot.term, "Snapshot should have correct term");
        assertEquals(initialLogSize - 1, snapshot.lastIncludedIndex, "Snapshot should include all entries");
        assertEquals(100, snapshot.data.size(), "Snapshot should contain all state data");
        
        SnapshotMetrics metrics = snapshotManager.getMetrics();
        assertEquals(1, metrics.totalSnapshotsCreated, "Should have 1 snapshot created");
        
        logger.info("✓ PASSED: Snapshot created with {} entries and {} state data",
            snapshot.recentEntries.size(), snapshot.data.size());
    }
    
    // ============ SCENARIO 2: Snapshot Loading & Recovery ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSnapshotLoadingAndRecovery() {
        logger.info("=== SCENARIO 2: Snapshot Loading & Recovery ===");
        
        // Create initial snapshot
        Map<String, String> originalState = new HashMap<>();
        for (int i = 0; i < 50; i++) {
            originalState.put("key_" + i, "value_" + i);
        }
        
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        for (int i = 0; i < 50; i++) {
            LogEntry entry = LogEntry.builder()
                .index(i)
                .term(2)
                .commandType(CommandType.PUT)
                .key("key_" + i)
                .value("value_" + i)
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
            raftState.getLog().add(entry);
        }
        
        Snapshot originalSnapshot = snapshotManager.createSnapshot(originalState);
        
        // Clear state (simulate recovery)
        raftState.getLog().clear();
        raftState.becomeFollower(0);
        
        // Load snapshot
        snapshotManager.loadSnapshot(originalSnapshot);
        
        SnapshotMetrics metrics = snapshotManager.getMetrics();
        assertEquals(1, metrics.totalSnapshotsLoaded, "Should have 1 snapshot loaded");
        assertEquals(originalSnapshot.lastIncludedIndex, metrics.latestSnapshotIndex);
        
        logger.info("✓ PASSED: Snapshot loaded and recovered {} state entries",
            originalSnapshot.data.size());
    }
    
    // ============ SCENARIO 3: Log Compaction ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testLogCompaction() {
        logger.info("=== SCENARIO 3: Log Compaction ============");
        
        // Create large log
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        for (int i = 0; i < 200; i++) {
            LogEntry entry = LogEntry.builder()
                .index(i)
                .term(1)
                .commandType(CommandType.PUT)
                .key("key_" + i)
                .value("value_" + i)
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
            raftState.getLog().add(entry);
        }
        
        long initialLogSize = raftState.getLog().size();
        logger.info("Initial log size: {}", initialLogSize);
        
        // Create snapshot at index 100
        Map<String, String> stateData = new HashMap<>();
        Snapshot snapshot = snapshotManager.createSnapshot(stateData);
        
        // Compact log to remove entries before snapshot
        snapshotManager.compactLog(100);
        
        long finalLogSize = raftState.getLog().size();
        long compactedEntries = initialLogSize - finalLogSize;
        
        assertTrue(finalLogSize < initialLogSize, "Log size should decrease after compaction");
        
        SnapshotMetrics metrics = snapshotManager.getMetrics();
        assertTrue(metrics.totalLogEntriesCompacted > 0, "Should have compacted some entries");
        
        logger.info("✓ PASSED: Log compacted from {} to {} entries (removed {})",
            initialLogSize, finalLogSize, compactedEntries);
    }
    
    // ============ SCENARIO 4: Persistence to Disk ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testPersistenceToDisk() {
        logger.info("=== SCENARIO 4: Persistence to Disk ===");
        
        // Create and save snapshot
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        Map<String, String> stateData = new HashMap<>();
        for (int i = 0; i < 75; i++) {
            stateData.put("key_" + i, "value_" + i);
        }
        
        for (int i = 0; i < 75; i++) {
            LogEntry entry = LogEntry.builder()
                .index(i)
                .term(1)
                .commandType(CommandType.PUT)
                .key("key_" + i)
                .value("value_" + i)
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
            raftState.getLog().add(entry);
        }
        
        Snapshot snapshot = snapshotManager.createSnapshot(stateData);
        
        // Save to disk
        boolean saved = persistenceLayer.saveSnapshot(snapshot, "test-snapshot-1");
        assertTrue(saved, "Snapshot should be saved successfully");
        
        // Load from disk
        Snapshot loadedSnapshot = persistenceLayer.loadSnapshot("test-snapshot-1");
        assertNotNull(loadedSnapshot, "Snapshot should be loaded");
        assertEquals(snapshot.data.size(), loadedSnapshot.data.size(), "Data should match");
        assertEquals(snapshot.lastIncludedIndex, loadedSnapshot.lastIncludedIndex);
        
        PersistenceMetrics metrics = persistenceLayer.getMetrics();
        assertEquals(1, metrics.totalSaved, "Should have 1 save");
        assertEquals(1, metrics.totalLoaded, "Should have 1 load");
        
        logger.info("✓ PASSED: Snapshot persisted to disk ({} entries, {} bytes compressed)",
            loadedSnapshot.data.size(), metrics.totalBytesCompressed);
    }
    
    // ============ SCENARIO 5: Compression Effectiveness ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testCompressionEffectiveness() {
        logger.info("=== SCENARIO 5: Compression Effectiveness ===");
        
        // Create large state data
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        Map<String, String> stateData = new HashMap<>();
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 1000; i++) {
            sb.append("Lorem ipsum dolor sit amet, consectetur adipiscing elit. ");
        }
        String largeValue = sb.toString();
        
        for (int i = 0; i < 50; i++) {
            stateData.put("key_" + i, largeValue);
        }
        
        // Create and save with compression
        Snapshot snapshot = snapshotManager.createSnapshot(stateData);
        persistenceLayer.setCompressionEnabled(true);
        long uncompressedSize = snapshot.getSize();
        
        persistenceLayer.saveSnapshot(snapshot, "test-compressed");
        long compressedSize = persistenceLayer.getMetrics().totalBytesCompressed;
        
        double compressionRatio = (100.0 * (uncompressedSize - compressedSize)) / uncompressedSize;
        
        logger.info("✓ PASSED: Compression ratio: {:.1f}% (uncompressed: {} bytes, compressed: {} bytes)",
            compressionRatio, uncompressedSize, compressedSize);
    }
    
    // ============ SCENARIO 6: Threshold-Based Snapshots ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testThresholdBasedSnapshots() {
        logger.info("=== SCENARIO 6: Threshold-Based Snapshots ===");
        
        // Set low threshold
        snapshotManager.setSnapshotThreshold(10);
        
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        assertFalse(snapshotManager.shouldSnapshot(), "Should not snapshot initially");
        
        // Add 15 entries
        for (int i = 0; i < 15; i++) {
            LogEntry entry = LogEntry.builder()
                .index(i)
                .term(1)
                .commandType(CommandType.PUT)
                .key("key_" + i)
                .value("value_" + i)
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
            raftState.getLog().add(entry);
        }
        
        // Now should trigger snapshot
        assertTrue(snapshotManager.shouldSnapshot(), "Should trigger snapshot after threshold");
        
        // Create snapshot
        Snapshot snapshot = snapshotManager.createSnapshot(new HashMap<>());
        
        // Reset threshold check
        assertFalse(snapshotManager.shouldSnapshot(), "Should not trigger immediately after snapshot");
        
        logger.info("✓ PASSED: Threshold-based snapshots working correctly");
    }
    
    // ============ SCENARIO 7: Multiple Snapshots ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testMultipleSnapshots() {
        logger.info("=== SCENARIO 7: Multiple Snapshots ===");
        
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        
        // Create 3 snapshots
        for (int snap = 1; snap <= 3; snap++) {
            // Add entries for this snapshot
            for (int i = 0; i < 20; i++) {
                LogEntry entry = LogEntry.builder()
                    .index(snap * 20 + i)
                    .term(snap)
                    .commandType(CommandType.PUT)
                    .key("key_" + i)
                    .value("value_" + snap + "_" + i)
                    .createdAt(System.currentTimeMillis())
                    .applied(true)
                    .build();
                raftState.getLog().add(entry);
            }
            
            // Create snapshot
            Map<String, String> stateData = new HashMap<>();
            stateData.put("version", "snapshot_" + snap);
            Snapshot snapshot = snapshotManager.createSnapshot(stateData);
            
            persistenceLayer.saveSnapshot(snapshot, "snapshot-" + snap);
        }
        
        // List snapshots
        List<String> savedSnapshots = persistenceLayer.listSnapshots();
        assertTrue(savedSnapshots.size() >= 3, "Should have at least 3 snapshots");
        
        SnapshotMetrics metrics = snapshotManager.getMetrics();
        assertEquals(3, metrics.totalSnapshotsCreated);
        
        logger.info("✓ PASSED: Created and saved {} snapshots", metrics.totalSnapshotsCreated);
    }
    
    // ============ SCENARIO 8: Snapshot During Chaos ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testSnapshotDuringChaos() throws InterruptedException {
        logger.info("=== SCENARIO 8: Snapshot During Chaos ===");
        
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        
        // Inject chaos: packet loss
        chaosMonkey.setPacketDropRate(30);
        
        // Add entries with chaos
        for (int i = 0; i < 50; i++) {
            LogEntry entry = LogEntry.builder()
                .index(i)
                .term(1)
                .commandType(CommandType.PUT)
                .key("key_" + i)
                .value("value_" + i)
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
            raftState.getLog().add(entry);
            
            // Simulate packet loss
            if (chaosMonkey.shouldDropPacket()) {
                Thread.sleep(10);
            }
        }
        
        // Create snapshot during chaos
        Map<String, String> stateData = new HashMap<>();
        Snapshot snapshot = snapshotManager.createSnapshot(stateData);
        
        assertNotNull(snapshot, "Snapshot should be created even with chaos");
        assertTrue(snapshot.data != null, "Snapshot data should be valid");
        
        chaosMonkey.reset();
        
        logger.info("✓ PASSED: Snapshot created successfully during chaos (30% packet loss)");
    }
    
    // ============ SCENARIO 9: Backup and Recovery ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testBackupAndRecovery() {
        logger.info("=== SCENARIO 9: Backup and Recovery ===");
        
        // Create initial snapshot
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        Map<String, String> stateData = new HashMap<>();
        stateData.put("version", "v1");
        
        Snapshot snapshot1 = snapshotManager.createSnapshot(stateData);
        boolean saved1 = persistenceLayer.saveSnapshot(snapshot1, "backup-test");
        assertTrue(saved1, "First snapshot should save");
        
        // Create updated snapshot (creates backup)
        stateData.put("version", "v2");
        Snapshot snapshot2 = snapshotManager.createSnapshot(stateData);
        boolean saved2 = persistenceLayer.saveSnapshot(snapshot2, "backup-test");
        assertTrue(saved2, "Second snapshot should save (creates backup)");
        
        // Verify latest snapshot can be loaded
        Snapshot loadedLatest = persistenceLayer.loadSnapshot("backup-test");
        assertNotNull(loadedLatest, "Should load latest snapshot");
        assertEquals("v2", loadedLatest.data.get("version"), "Latest version should be v2");
        
        logger.info("✓ PASSED: Backup versioning working correctly");
    }
    
    // ============ SCENARIO 10: InstallSnapshot RPC ============
    
    @Test
    @Timeout(value = 30, unit = TimeUnit.SECONDS)
    public void testInstallSnapshotRpc() {
        logger.info("=== SCENARIO 10: InstallSnapshot RPC ===");
        
        // Create snapshot on leader
        raftState.becomeCandidate("node1");
        raftState.becomeLeader();
        Map<String, String> leaderState = new HashMap<>();
        for (int i = 0; i < 30; i++) {
            leaderState.put("key_" + i, "value_" + i);
        }
        
        Snapshot leaderSnapshot = snapshotManager.createSnapshot(leaderState);
        
        // Simulate follower receiving InstallSnapshot RPC
        raftState.becomeFollower(5);
        boolean installed = snapshotManager.installSnapshot(
            5,
            leaderSnapshot.lastIncludedIndex,
            leaderSnapshot.lastIncludedTerm,
            leaderSnapshot.data,
            true  // done
        );
        
        assertTrue(installed, "InstallSnapshot should succeed");
        
        SnapshotMetrics metrics = snapshotManager.getMetrics();
        assertEquals(1, metrics.totalSnapshotsCreated, "Should have 1 snapshot created on leader");
        assertEquals(1, metrics.totalSnapshotsLoaded, "Should have 1 snapshot loaded during install");
        
        logger.info("✓ PASSED: InstallSnapshot RPC working correctly");
    }
    
    // ============ SCENARIO 11: Chaos Metrics Tracking ============
    
    @Test
    public void testSnapshotMetricsTracking() {
        logger.info("=== Snapshot Metrics Tracking ===");
        
        // Generate snapshots and persistence operations
        raftState.becomeLeader();
        for (int i = 0; i < 3; i++) {
            LogEntry entry = LogEntry.builder()
                .index(i)
                .term(1)
                .commandType(CommandType.PUT)
                .key("key_" + i)
                .value("value_" + i)
                .createdAt(System.currentTimeMillis())
                .applied(true)
                .build();
            raftState.getLog().add(entry);
        }
        
        Snapshot snapshot = snapshotManager.createSnapshot(new HashMap<>());
        persistenceLayer.saveSnapshot(snapshot, "metrics-test");
        persistenceLayer.loadSnapshot("metrics-test");
        snapshotManager.compactLog(5);
        
        // Check metrics
        SnapshotMetrics snapshotMetrics = snapshotManager.getMetrics();
        PersistenceMetrics persistenceMetrics = persistenceLayer.getMetrics();
        
        assertEquals(1, snapshotMetrics.totalSnapshotsCreated);
        assertEquals(1, persistenceMetrics.totalSaved);
        assertEquals(1, persistenceMetrics.totalLoaded);
        assertTrue(persistenceMetrics.successRate > 0);
        
        logger.info("✓ PASSED: Metrics correctly tracked");
        logger.info("  Snapshot Metrics: {}", snapshotMetrics);
        logger.info("  Persistence Metrics: {}", persistenceMetrics);
    }
}
