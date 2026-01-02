package com.omkar.distributed_key_vault;

import com.omkar.distributed_key_vault.raft.*;
import com.omkar.distributed_key_vault.vault.KeyVaultStore;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Phase 3.2 - Multi-Entry Log Replication Tests
 * Validates command replication, execution, and consistency
 */
@SpringBootTest
public class Phase32ReplicationTests {
    
    @Autowired
    private RaftState raftState;
    
    @Autowired
    private KeyVaultStore keyVaultStore;
    
    @Autowired
    private CommandExecutor commandExecutor;
    
    @Autowired
    private RepairReplicaLog repairReplicaLog;
    
    @Autowired
    private ReplicationMetrics replicationMetrics;
    
    private LogEntry testEntry;
    
    @BeforeEach
    public void setUp() {
        keyVaultStore.clear();
        replicationMetrics.reset();
        
        // Create test entry
        testEntry = new LogEntry();
        testEntry.setIndex(1);
        testEntry.setTerm(1);
        testEntry.setCommandType(CommandType.PUT);
        testEntry.setKey("testKey");
        testEntry.setValue("testValue");
    }
    
    /**
     * Test that single command entry is executed correctly
     */
    @Test
    public void testSingleCommandExecution() {
        // Execute command
        boolean result = commandExecutor.executeCommand(testEntry);
        
        // Verify success
        assertTrue(result, "Command should execute successfully");
        assertTrue(testEntry.isApplied(), "Entry should be marked as applied");
        assertNotNull(testEntry.getAppliedAt(), "Applied timestamp should be set");
        
        // Verify state machine updated
        String value = keyVaultStore.get("testKey");
        assertEquals("testValue", value, "State machine should have the value");
    }
    
    /**
     * Test that multiple entries execute in order
     */
    @Test
    public void testMultiEntryExecution() {
        List<LogEntry> entries = new ArrayList<>();
        
        // Create 3 PUT entries
        for (int i = 1; i <= 3; i++) {
            LogEntry entry = new LogEntry();
            entry.setIndex(i);
            entry.setTerm(1);
            entry.setCommandType(CommandType.PUT);
            entry.setKey("key" + i);
            entry.setValue("value" + i);
            entries.add(entry);
        }
        
        // Execute all
        for (LogEntry entry : entries) {
            boolean result = commandExecutor.executeCommand(entry);
            assertTrue(result, "Each command should execute successfully");
        }
        
        // Verify all in state machine
        assertEquals("value1", keyVaultStore.get("key1"));
        assertEquals("value2", keyVaultStore.get("key2"));
        assertEquals("value3", keyVaultStore.get("key3"));
    }
    
    /**
     * Test DELETE command execution
     */
    @Test
    public void testDeleteCommandExecution() {
        // First write
        LogEntry putEntry = new LogEntry();
        putEntry.setIndex(1);
        putEntry.setTerm(1);
        putEntry.setCommandType(CommandType.PUT);
        putEntry.setKey("deleteMe");
        putEntry.setValue("temp");
        
        commandExecutor.executeCommand(putEntry);
        assertEquals("temp", keyVaultStore.get("deleteMe"));
        
        // Then delete
        LogEntry delEntry = new LogEntry();
        delEntry.setIndex(2);
        delEntry.setTerm(1);
        delEntry.setCommandType(CommandType.DELETE);
        delEntry.setKey("deleteMe");
        
        commandExecutor.executeCommand(delEntry);
        assertNull(keyVaultStore.get("deleteMe"), "Key should be deleted");
    }
    
    /**
     * Test NOOP command (should not modify state machine)
     */
    @Test
    public void testNoopCommandExecution() {
        LogEntry noopEntry = new LogEntry();
        noopEntry.setIndex(1);
        noopEntry.setTerm(1);
        noopEntry.setCommandType(CommandType.NOOP);
        
        boolean result = commandExecutor.executeCommand(noopEntry);
        
        assertTrue(result, "NOOP should execute successfully");
        assertTrue(noopEntry.isApplied(), "NOOP should be marked applied");
        assertEquals(0, keyVaultStore.size(), "State machine should be unchanged");
    }
    
    /**
     * Test idempotency: applying same command twice
     */
    @Test
    public void testIdempotency() {
        // First application
        commandExecutor.executeCommand(testEntry);
        assertEquals("testValue", keyVaultStore.get("testKey"));
        
        // Reset applied flag (simulate retry)
        testEntry.setApplied(false);
        
        // Second application (idempotent)
        commandExecutor.executeCommand(testEntry);
        assertEquals("testValue", keyVaultStore.get("testKey"));
        assertEquals(1, keyVaultStore.size(), "Should still have 1 entry");
    }
    
    /**
     * Test log divergence detection
     */
    @Test
    public void testLogDivergenceDetection() {
        List<LogEntry> leaderLog = new ArrayList<>();
        List<LogEntry> followerLog = new ArrayList<>();
        
        // Both have entries 1-3
        for (int i = 1; i <= 3; i++) {
            LogEntry entry = new LogEntry();
            entry.setIndex(i);
            entry.setTerm(1);
            leaderLog.add(entry);
            followerLog.add(entry);
        }
        
        // Leader has entries 4-6 with term 2
        // Follower has different entry 4 with term 1
        LogEntry leaderEntry4 = new LogEntry();
        leaderEntry4.setIndex(4);
        leaderEntry4.setTerm(2);
        leaderLog.add(leaderEntry4);
        
        LogEntry followerEntry4 = new LogEntry();
        followerEntry4.setIndex(4);
        followerEntry4.setTerm(1);
        followerLog.add(followerEntry4);
        
        // Find conflict
        int conflictIndex = repairReplicaLog.findConflictIndex(leaderLog, followerLog);
        
        assertEquals(4, conflictIndex, "Conflict should be at index 4");
    }
    
    /**
     * Test replication metrics tracking
     */
    @Test
    public void testReplicationMetricsTracking() {
        // Record replication ACK
        replicationMetrics.recordReplicationAck("node1", 10, 25);
        
        assertEquals(25, replicationMetrics.getReplicationLag("node1"));
        assertEquals(1, replicationMetrics.getReplicationSuccessCount("node1"));
        
        // Record command applied
        replicationMetrics.recordCommandApplied("PUT");
        
        assertEquals(1, replicationMetrics.getCommandsApplied());
        
        // Record commit advance
        replicationMetrics.recordCommitIndexAdvance(0, 5);
        
        assertEquals(1, replicationMetrics.getCommitIndexAdvances());
    }
    
    /**
     * Test command validation
     */
    @Test
    public void testCommandValidation() {
        // Valid PUT
        LogEntry validPut = new LogEntry();
        validPut.setCommandType(CommandType.PUT);
        validPut.setKey("key");
        validPut.setValue("value");
        
        assertTrue(commandExecutor.validateCommand(validPut));
        
        // Invalid PUT (missing key)
        LogEntry invalidPut = new LogEntry();
        invalidPut.setCommandType(CommandType.PUT);
        invalidPut.setValue("value");
        
        assertFalse(commandExecutor.validateCommand(invalidPut));
        
        // Valid DELETE
        LogEntry validDel = new LogEntry();
        validDel.setCommandType(CommandType.DELETE);
        validDel.setKey("key");
        
        assertTrue(commandExecutor.validateCommand(validDel));
        
        // Valid NOOP
        LogEntry noop = new LogEntry();
        noop.setCommandType(CommandType.NOOP);
        
        assertTrue(commandExecutor.validateCommand(noop));
    }
    
    /**
     * Test state machine consistency
     */
    @Test
    public void testStateConsistency() {
        // Write initial value
        LogEntry entry1 = new LogEntry();
        entry1.setIndex(1);
        entry1.setTerm(1);
        entry1.setCommandType(CommandType.PUT);
        entry1.setKey("key");
        entry1.setValue("value1");
        
        commandExecutor.executeCommand(entry1);
        assertEquals("value1", keyVaultStore.get("key"));
        
        // Overwrite value
        LogEntry entry2 = new LogEntry();
        entry2.setIndex(2);
        entry2.setTerm(1);
        entry2.setCommandType(CommandType.PUT);
        entry2.setKey("key");
        entry2.setValue("value2");
        
        commandExecutor.executeCommand(entry2);
        assertEquals("value2", keyVaultStore.get("key"), "Latest value should be stored");
    }
}
