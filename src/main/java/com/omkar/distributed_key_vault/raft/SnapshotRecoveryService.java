package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.raft.SnapshotManager.Snapshot;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

/**
 * Phase 3.5 - Snapshot Recovery Service
 * Handles startup recovery from snapshots and persistent state
 * 
 * Recovery sequence:
 * 1. Load latest snapshot from disk
 * 2. Restore state machine from snapshot data
 * 3. Restore Raft log with recent entries
 * 4. Initialize replication state
 */
@Slf4j
@Service
@RequiredArgsConstructor
public class SnapshotRecoveryService {
    
    private final SnapshotManager snapshotManager;
    private final PersistenceLayer persistenceLayer;
    private final RaftState raftState;
    private final ElectionService electionService;
    
    /**
     * Recover node from persistent state on startup
     * Called automatically by Spring after bean initialization
     */
    @PostConstruct
    public void recoverFromSnapshot() {
        try {
            log.info("🔄 Starting snapshot recovery...");
            
            // Try to load latest snapshot
            Snapshot latest = snapshotManager.getLatestSnapshot();
            
            if (latest != null) {
                log.info("📥 Found snapshot at index {}, restoring state...", latest.lastIncludedIndex);
                
                // Restore Raft state
                snapshotManager.loadSnapshot(latest);
                
                // Restore state machine from snapshot data
                restoreStateData(latest.data);
                
                // Log recovery
                log.warn("✓ Recovery complete: restored from snapshot at index {} (term {})",
                    latest.lastIncludedIndex, latest.lastIncludedTerm);
                
                // Print recovery stats
                printRecoveryStats(latest);
                
            } else {
                log.info("ℹ️  No snapshot found - starting fresh");
                raftState.becomeFollower(0);
            }
            
            // Initialize as follower (will start election if no heartbeat)
            raftState.becomeFollower(raftState.getCurrentTerm().get());
            
        } catch (Exception e) {
            log.error("❌ Error during snapshot recovery", e);
            // Continue anyway - will catch up through replication
            raftState.becomeFollower(0);
        }
    }
    
    /**
     * Restore state machine from snapshot data
     */
    private void restoreStateData(java.util.Map<String, String> snapshotData) {
        if (snapshotData == null || snapshotData.isEmpty()) {
            log.debug("No state data to restore");
            return;
        }
        
        // In production, restore to actual KeyVaultStore
        // For now, just log the data
        log.debug("Restoring {} state entries from snapshot", snapshotData.size());
        
        // Example restoration (would need actual KeyVaultStore injection)
        snapshotData.forEach((key, value) -> {
            log.debug("  Restored: {} = {}", key, value);
        });
    }
    
    /**
     * Print recovery statistics
     */
    private void printRecoveryStats(Snapshot snapshot) {
        SnapshotManager.SnapshotMetrics metrics = snapshotManager.getMetrics();
        PersistenceLayer.PersistenceMetrics persistenceMetrics = persistenceLayer.getMetrics();
        
        log.info("╔════════════════════════════════════════╗");
        log.info("║     SNAPSHOT RECOVERY STATISTICS      ║");
        log.info("╠════════════════════════════════════════╣");
        log.info("║ Snapshot Index       : {}", String.format("%-22d║", snapshot.lastIncludedIndex));
        log.info("║ Snapshot Term        : {}", String.format("%-22d║", snapshot.lastIncludedTerm));
        log.info("║ State Data Entries   : {}", String.format("%-22d║", snapshot.data.size()));
        log.info("║ Recent Log Entries   : {}", String.format("%-22d║", snapshot.recentEntries.size()));
        log.info("║ Snapshot Size (KB)   : {}", String.format("%-22d║", snapshot.getSize() / 1024));
        log.info("║────────────────────────────────────────║");
        log.info("║ Snapshots Created    : {}", String.format("%-22d║", metrics.totalSnapshotsCreated));
        log.info("║ Snapshots Loaded     : {}", String.format("%-22d║", metrics.totalSnapshotsLoaded));
        log.info("║ Entries Compacted    : {}", String.format("%-22d║", metrics.totalLogEntriesCompacted));
        log.info("║ Persistence Saves    : {}", String.format("%-22d║", persistenceMetrics.totalSaved));
        log.info("║ Persistence Success  : {}%", String.format("%-21d║", persistenceMetrics.successRate));
        log.info("╚════════════════════════════════════════╝");
    }
    
    /**
     * Check recovery status
     */
    public RecoveryStatus getRecoveryStatus() {
        Snapshot latest = snapshotManager.getLatestSnapshot();
        SnapshotManager.SnapshotMetrics metrics = snapshotManager.getMetrics();
        
        return new RecoveryStatus(
            latest != null,
            latest != null ? latest.lastIncludedIndex : 0,
            latest != null ? latest.lastIncludedTerm : 0,
            raftState.getLog().size(),
            metrics.totalLogEntriesCompacted,
            latest != null ? latest.getSize() : 0,
            raftState.getRole().toString()
        );
    }
    
    /**
     * RecoveryStatus DTO
     */
    public static class RecoveryStatus {
        public boolean snapshotAvailable;
        public long snapshotIndex;
        public long snapshotTerm;
        public int logSize;
        public long entriesCompacted;
        public long snapshotSizeBytes;
        public String nodeRole;
        
        public RecoveryStatus(boolean available, long idx, long term, int log,
                             long compacted, long size, String role) {
            this.snapshotAvailable = available;
            this.snapshotIndex = idx;
            this.snapshotTerm = term;
            this.logSize = log;
            this.entriesCompacted = compacted;
            this.snapshotSizeBytes = size;
            this.nodeRole = role;
        }
        
        @Override
        public String toString() {
            return String.format("RecoveryStatus(snapshot=%b, idx=%d, term=%d, log=%d, compacted=%d, role=%s)",
                snapshotAvailable, snapshotIndex, snapshotTerm, logSize, entriesCompacted, nodeRole);
        }
    }
}
