package com.omkar.distributed_key_vault.raft.controller;

import com.omkar.distributed_key_vault.raft.*;
import com.omkar.distributed_key_vault.raft.SnapshotManager.Snapshot;
import com.omkar.distributed_key_vault.raft.SnapshotManager.SnapshotMetrics;
import com.omkar.distributed_key_vault.raft.PersistenceLayer.PersistenceMetrics;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.*;

/**
 * Phase 3.5 - Snapshot Controller
 * REST API for snapshot management, creation, and metrics
 */
@Slf4j
@RestController
@RequestMapping("/snapshots")
@RequiredArgsConstructor
public class SnapshotController {
    
    private final SnapshotManager snapshotManager;
    private final PersistenceLayer persistenceLayer;
    private final RaftState raftState;
    
    // ============ SNAPSHOT QUERIES ============
    
    /**
     * Get latest snapshot metadata
     */
    @GetMapping("/latest")
    public ResponseEntity<SnapshotResponse> getLatestSnapshot() {
        Snapshot latest = snapshotManager.getLatestSnapshot();
        
        if (latest == null) {
            return ResponseEntity.ok(new SnapshotResponse(
                "No snapshot available",
                null, 0, 0, 0, 0
            ));
        }
        
        return ResponseEntity.ok(new SnapshotResponse(
            "Latest snapshot retrieved",
            latest.toString(),
            latest.lastIncludedIndex,
            latest.lastIncludedTerm,
            latest.data.size(),
            latest.getSize()
        ));
    }
    
    /**
     * List all saved snapshots
     */
    @GetMapping("/list")
    public ResponseEntity<SnapshotListResponse> listSnapshots() {
        List<String> snapshots = persistenceLayer.listSnapshots();
        
        Map<String, Object> details = new HashMap<>();
        for (String name : snapshots) {
            details.put(name, "Snapshot: " + name);
        }
        
        return ResponseEntity.ok(new SnapshotListResponse(
            snapshots.size() + " snapshots found",
            snapshots,
            details
        ));
    }
    
    /**
     * Get snapshot metrics
     */
    @GetMapping("/metrics")
    public ResponseEntity<SnapshotMetricsResponse> getMetrics() {
        SnapshotMetrics snapshotMetrics = snapshotManager.getMetrics();
        PersistenceMetrics persistenceMetrics = persistenceLayer.getMetrics();
        
        long storageSizeBytes = persistenceLayer.getStorageSize();
        double storageSizeMB = storageSizeBytes / (1024.0 * 1024.0);
        
        return ResponseEntity.ok(new SnapshotMetricsResponse(
            "Snapshot metrics retrieved",
            snapshotMetrics.totalSnapshotsCreated,
            snapshotMetrics.totalSnapshotsLoaded,
            snapshotMetrics.totalLogEntriesCompacted,
            persistenceMetrics.totalSaved,
            persistenceMetrics.totalLoaded,
            persistenceMetrics.totalFailed,
            persistenceMetrics.successRate,
            storageSizeMB,
            snapshotMetrics.toString(),
            persistenceMetrics.toString()
        ));
    }
    
    /**
     * Get storage statistics
     */
    @GetMapping("/storage")
    public ResponseEntity<StorageStatsResponse> getStorageStats() {
        long totalBytes = persistenceLayer.getStorageSize();
        double totalMB = totalBytes / (1024.0 * 1024.0);
        int logSize = raftState.getLog().size();
        SnapshotMetrics metrics = snapshotManager.getMetrics();
        
        double compressionRatio = metrics.totalSnapshotBytes > 0 ? 
            ((double) metrics.totalLogEntriesCompacted / logSize * 100) : 0;
        
        return ResponseEntity.ok(new StorageStatsResponse(
            "Storage statistics retrieved",
            totalMB,
            totalBytes,
            logSize,
            (int) metrics.totalLogEntriesCompacted,
            compressionRatio,
            (int) metrics.snapshotCount
        ));
    }
    
    // ============ SNAPSHOT OPERATIONS ============
    
    /**
     * Force create snapshot now
     */
    @PostMapping("/create")
    public ResponseEntity<SnapshotCreateResponse> createSnapshot() {
        try {
            Map<String, String> stateData = new HashMap<>();
            // In production, get actual state from KeyVaultStore
            // For now, use empty map
            
            Snapshot snapshot = snapshotManager.createSnapshot(stateData);
            
            if (snapshot != null) {
                // Persist to disk
                boolean saved = persistenceLayer.saveSnapshot(snapshot, 
                    "snapshot-" + snapshot.lastIncludedIndex);
                
                if (saved) {
                    log.info("✓ Snapshot created and persisted: index {}", 
                        snapshot.lastIncludedIndex);
                    
                    return ResponseEntity.ok(new SnapshotCreateResponse(
                        "Snapshot created successfully",
                        snapshot.lastIncludedIndex,
                        snapshot.lastIncludedTerm,
                        snapshot.data.size(),
                        snapshot.getSize(),
                        true
                    ));
                }
            }
            
            return ResponseEntity.status(500).body(new SnapshotCreateResponse(
                "Failed to create snapshot",
                0, 0, 0, 0, false
            ));
            
        } catch (Exception e) {
            log.error("Error creating snapshot", e);
            return ResponseEntity.status(500).body(new SnapshotCreateResponse(
                "Error: " + e.getMessage(),
                0, 0, 0, 0, false
            ));
        }
    }
    
    /**
     * Compact log up to index
     */
    @PostMapping("/compact")
    public ResponseEntity<CompactionResponse> compactLog(@RequestParam long upToIndex) {
        try {
            int logSizeBefore = raftState.getLog().size();
            snapshotManager.compactLog(upToIndex);
            int logSizeAfter = raftState.getLog().size();
            int entriesRemoved = logSizeBefore - logSizeAfter;
            
            log.info("✓ Log compacted: removed {} entries", entriesRemoved);
            
            return ResponseEntity.ok(new CompactionResponse(
                "Log compacted successfully",
                upToIndex,
                logSizeBefore,
                logSizeAfter,
                entriesRemoved,
                true
            ));
            
        } catch (Exception e) {
            log.error("Error compacting log", e);
            return ResponseEntity.status(500).body(new CompactionResponse(
                "Error: " + e.getMessage(),
                upToIndex, 0, 0, 0, false
            ));
        }
    }
    
    /**
     * Should snapshot check
     */
    @GetMapping("/should-snapshot")
    public ResponseEntity<ShouldSnapshotResponse> shouldSnapshot() {
        boolean should = snapshotManager.shouldSnapshot();
        SnapshotMetrics metrics = snapshotManager.getMetrics();
        
        return ResponseEntity.ok(new ShouldSnapshotResponse(
            should ? "Snapshot recommended" : "No snapshot needed",
            should,
            raftState.getLog().size(),
            metrics.totalSnapshotsCreated,
            metrics.latestSnapshotIndex
        ));
    }
    
    /**
     * Delete snapshot
     */
    @DeleteMapping("/{snapshotName}")
    public ResponseEntity<DeleteResponse> deleteSnapshot(@PathVariable String snapshotName) {
        try {
            boolean deleted = persistenceLayer.deleteSnapshot(snapshotName);
            
            if (deleted) {
                log.info("✓ Snapshot deleted: {}", snapshotName);
                return ResponseEntity.ok(new DeleteResponse(
                    "Snapshot deleted successfully",
                    snapshotName,
                    true
                ));
            } else {
                return ResponseEntity.status(404).body(new DeleteResponse(
                    "Snapshot not found",
                    snapshotName,
                    false
                ));
            }
            
        } catch (Exception e) {
            log.error("Error deleting snapshot", e);
            return ResponseEntity.status(500).body(new DeleteResponse(
                "Error: " + e.getMessage(),
                snapshotName,
                false
            ));
        }
    }
    
    /**
     * Reset all snapshots
     */
    @PostMapping("/reset")
    public ResponseEntity<ResetResponse> reset() {
        try {
            snapshotManager.reset();
            persistenceLayer.reset();
            
            log.info("✓ All snapshots and persistence state reset");
            
            return ResponseEntity.ok(new ResetResponse(
                "All snapshots and persistence state reset successfully",
                true
            ));
            
        } catch (Exception e) {
            log.error("Error resetting snapshots", e);
            return ResponseEntity.status(500).body(new ResetResponse(
                "Error: " + e.getMessage(),
                false
            ));
        }
    }
    
    // ============ RESPONSE DTOs ============
    
    public static class SnapshotResponse {
        public String message;
        public String snapshotInfo;
        public long lastIncludedIndex;
        public long lastIncludedTerm;
        public int dataEntries;
        public long sizeBytes;
        
        public SnapshotResponse(String msg, String info, long idx, long term, int entries, long size) {
            this.message = msg;
            this.snapshotInfo = info;
            this.lastIncludedIndex = idx;
            this.lastIncludedTerm = term;
            this.dataEntries = entries;
            this.sizeBytes = size;
        }
    }
    
    public static class SnapshotListResponse {
        public String message;
        public List<String> snapshots;
        public Map<String, Object> details;
        
        public SnapshotListResponse(String msg, List<String> snaps, Map<String, Object> det) {
            this.message = msg;
            this.snapshots = snaps;
            this.details = det;
        }
    }
    
    public static class SnapshotMetricsResponse {
        public String message;
        public long snapshotsCreated;
        public long snapshotsLoaded;
        public long entriesCompacted;
        public long persistenceSaves;
        public long persistenceLoads;
        public long persistenceFailures;
        public long successRate;
        public double storageMB;
        public String snapshotMetricsStr;
        public String persistenceMetricsStr;
        
        public SnapshotMetricsResponse(String msg, long created, long loaded, long compacted,
                                       long saves, long loads, long failures, long success,
                                       double storageMB, String snapStr, String persStr) {
            this.message = msg;
            this.snapshotsCreated = created;
            this.snapshotsLoaded = loaded;
            this.entriesCompacted = compacted;
            this.persistenceSaves = saves;
            this.persistenceLoads = loads;
            this.persistenceFailures = failures;
            this.successRate = success;
            this.storageMB = storageMB;
            this.snapshotMetricsStr = snapStr;
            this.persistenceMetricsStr = persStr;
        }
    }
    
    public static class StorageStatsResponse {
        public String message;
        public double totalMB;
        public long totalBytes;
        public int logSize;
        public int entriesCompacted;
        public double compressionRatio;
        public int snapshotCount;
        
        public StorageStatsResponse(String msg, double mb, long bytes, int logSize, 
                                    int compacted, double ratio, int count) {
            this.message = msg;
            this.totalMB = mb;
            this.totalBytes = bytes;
            this.logSize = logSize;
            this.entriesCompacted = compacted;
            this.compressionRatio = ratio;
            this.snapshotCount = count;
        }
    }
    
    public static class SnapshotCreateResponse {
        public String message;
        public long index;
        public long term;
        public int dataSize;
        public long sizeBytes;
        public boolean success;
        
        public SnapshotCreateResponse(String msg, long idx, long t, int size, long bytes, boolean ok) {
            this.message = msg;
            this.index = idx;
            this.term = t;
            this.dataSize = size;
            this.sizeBytes = bytes;
            this.success = ok;
        }
    }
    
    public static class CompactionResponse {
        public String message;
        public long upToIndex;
        public int logSizeBefore;
        public int logSizeAfter;
        public int entriesRemoved;
        public boolean success;
        
        public CompactionResponse(String msg, long idx, int before, int after, int removed, boolean ok) {
            this.message = msg;
            this.upToIndex = idx;
            this.logSizeBefore = before;
            this.logSizeAfter = after;
            this.entriesRemoved = removed;
            this.success = ok;
        }
    }
    
    public static class ShouldSnapshotResponse {
        public String message;
        public boolean shouldSnapshot;
        public int logSize;
        public long snapshotsCreated;
        public long latestSnapshotIndex;
        
        public ShouldSnapshotResponse(String msg, boolean should, int logSize, 
                                      long created, long latestIdx) {
            this.message = msg;
            this.shouldSnapshot = should;
            this.logSize = logSize;
            this.snapshotsCreated = created;
            this.latestSnapshotIndex = latestIdx;
        }
    }
    
    public static class DeleteResponse {
        public String message;
        public String snapshotName;
        public boolean success;
        
        public DeleteResponse(String msg, String name, boolean ok) {
            this.message = msg;
            this.snapshotName = name;
            this.success = ok;
        }
    }
    
    public static class ResetResponse {
        public String message;
        public boolean success;
        
        public ResetResponse(String msg, boolean ok) {
            this.message = msg;
            this.success = ok;
        }
    }
}
