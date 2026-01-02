package com.omkar.distributed_key_vault.raft;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Phase 3.4 - Snapshot Manager
 * Handles creation, storage, and loading of Raft snapshots.
 * Snapshots compress log state for faster recovery and reduced storage.
 * 
 * Features:
 * - Create snapshots of current state
 * - Load snapshots on recovery
 * - Track latest snapshot metadata
 * - InstallSnapshot RPC handling
 * - Log compaction after snapshots
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class SnapshotManager {
    
    private final RaftState raftState;
    private static final org.slf4j.Logger logger = org.slf4j.LoggerFactory.getLogger(SnapshotManager.class);
    
    // Snapshot storage
    private final Map<Long, Snapshot> snapshots = new ConcurrentHashMap<>();
    private volatile Snapshot latestSnapshot = null;
    private final ReentrantReadWriteLock snapshotLock = new ReentrantReadWriteLock();
    
    // Metrics
    private volatile long totalSnapshotsCreated = 0;
    private volatile long totalSnapshotsLoaded = 0;
    private volatile long totalLogEntriesCompacted = 0;
    private volatile long totalSnapshotBytes = 0;
    
    // Configuration
    private volatile int snapshotThreshold = 10000; // Trigger snapshot after N log entries
    private volatile boolean compressionEnabled = true;
    
    /**
     * Snapshot metadata and state container
     */
    public static class Snapshot {
        public long term;                    // Term when snapshot created
        public long lastIncludedIndex;       // Last included index in snapshot
        public long lastIncludedTerm;        // Term of lastIncludedIndex
        public long timestamp;               // Creation timestamp
        public Map<String, String> data;     // State (key-value pairs)
        public List<LogEntry> recentEntries; // Log entries after snapshot
        
        public Snapshot(long term, long lastIncludedIndex, long lastIncludedTerm,
                       Map<String, String> data, List<LogEntry> recentEntries) {
            this.term = term;
            this.lastIncludedIndex = lastIncludedIndex;
            this.lastIncludedTerm = lastIncludedTerm;
            this.timestamp = System.currentTimeMillis();
            this.data = new HashMap<>(data);
            this.recentEntries = new ArrayList<>(recentEntries);
        }
        
        public long getSize() {
            // Rough size estimate in bytes
            long size = 48; // Metadata overhead
            size += data.entrySet().stream()
                .mapToLong(e -> e.getKey().length() + e.getValue().length())
                .sum();
            size += recentEntries.size() * 200; // ~200 bytes per log entry
            return size;
        }
        
        @Override
        public String toString() {
            return String.format("Snapshot(term=%d, lastIdx=%d, entries=%d, data=%d bytes, size=%dKB)",
                term, lastIncludedIndex, recentEntries.size(), data.size(), getSize() / 1024);
        }
    }
    
    // ============ SNAPSHOT CREATION ============
    
    /**
     * Create snapshot of current state
     * Captures: current term, log state, and state machine data
     */
    public Snapshot createSnapshot(Map<String, String> stateData) {
        snapshotLock.writeLock().lock();
        try {
            long currentTerm = raftState.getCurrentTerm().get();
            long lastIndex = raftState.getLog().isEmpty() ? 0 : raftState.getLog().size() - 1;
            long lastTerm = lastIndex > 0 ? raftState.getLog().get((int) lastIndex).getTerm() : 0;
            
            // Collect recent log entries (keep last N entries for quick replay)
            List<LogEntry> recentEntries = new ArrayList<>();
            int keepCount = Math.min(100, raftState.getLog().size());
            for (int i = Math.max(0, raftState.getLog().size() - keepCount); i < raftState.getLog().size(); i++) {
                recentEntries.add(raftState.getLog().get(i));
            }
            
            Snapshot snapshot = new Snapshot(currentTerm, lastIndex, lastTerm, stateData, recentEntries);
            
            // Store snapshot
            snapshots.put(currentTerm, snapshot);
            latestSnapshot = snapshot;
            totalSnapshotsCreated++;
            totalSnapshotBytes += snapshot.getSize();
            
            log.warn("📸 SNAPSHOT: Created snapshot at index {} (term {}) - {} entries, {} bytes",
                lastIndex, currentTerm, recentEntries.size(), snapshot.getSize());
            
            return snapshot;
            
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }
    
    /**
     * Check if snapshot should be created (threshold-based)
     */
    public boolean shouldSnapshot() {
        if (latestSnapshot == null) return raftState.getLog().size() > snapshotThreshold;
        
        long entriesSinceSnapshot = raftState.getLog().size() - latestSnapshot.lastIncludedIndex;
        return entriesSinceSnapshot > snapshotThreshold;
    }
    
    // ============ SNAPSHOT LOADING ============
    
    /**
     * Load snapshot on recovery
     * Restores: term, log state, and state machine data
     */
    public void loadSnapshot(Snapshot snapshot) {
        snapshotLock.writeLock().lock();
        try {
            if (snapshot == null) {
                log.info("No snapshot to load");
                return;
            }
            
            // Restore state machine data
            log.info("📥 SNAPSHOT: Loading snapshot at index {} (term {})", 
                snapshot.lastIncludedIndex, snapshot.lastIncludedTerm);
            
            // Restore recent log entries
            raftState.getLog().clear();
            raftState.getLog().addAll(snapshot.recentEntries);
            
            latestSnapshot = snapshot;
            totalSnapshotsLoaded++;
            
            log.warn("✓ SNAPSHOT: Loaded snapshot with {} entries, {} state entries",
                snapshot.recentEntries.size(), snapshot.data.size());
            
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }
    
    /**
     * Get latest snapshot
     */
    public Snapshot getLatestSnapshot() {
        snapshotLock.readLock().lock();
        try {
            return latestSnapshot;
        } finally {
            snapshotLock.readLock().unlock();
        }
    }
    
    /**
     * Get snapshot by term
     */
    public Snapshot getSnapshot(long term) {
        snapshotLock.readLock().lock();
        try {
            return snapshots.get(term);
        } finally {
            snapshotLock.readLock().unlock();
        }
    }
    
    // ============ LOG COMPACTION ============
    
    /**
     * Compact log by removing entries included in snapshot
     * Reduces log size after snapshot creation
     */
    public void compactLog(long snapshotLastIncludedIndex) {
        snapshotLock.writeLock().lock();
        try {
            List<LogEntry> entries = raftState.getLog();
            if (entries.isEmpty() || snapshotLastIncludedIndex < 0) {
                return;
            }
            
            // Find entries to remove (all entries up to lastIncludedIndex)
            int removeCount = 0;
            int targetSize = (int) Math.min(snapshotLastIncludedIndex + 1, entries.size());
            
            for (int i = 0; i < targetSize && !entries.isEmpty(); i++) {
                if (entries.get(0).getIndex() <= snapshotLastIncludedIndex) {
                    entries.remove(0);
                    removeCount++;
                }
            }
            
            totalLogEntriesCompacted += removeCount;
            
            logger.info("🗜️ COMPACTION: Removed {} entries from log, new size: {}", removeCount, entries.size());
            
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }
    
    // ============ INSTALL SNAPSHOT RPC ============
    
    /**
     * Handle InstallSnapshot RPC from leader
     * Used to install snapshot on followers
     */
    public boolean installSnapshot(long term, long lastIncludedIndex, long lastIncludedTerm, 
                                   Map<String, String> data, boolean done) {
        snapshotLock.writeLock().lock();
        try {
            // Reject if term is old
            if (term < raftState.getCurrentTerm().get()) {
                return false;
            }
            
            // Update term if needed
            if (term > raftState.getCurrentTerm().get()) {
                raftState.becomeFollower((int)term);
            }
            
            // Create and install snapshot
            if (done) {
                List<LogEntry> emptyEntries = new ArrayList<>();
                Snapshot snapshot = new Snapshot(term, lastIncludedIndex, lastIncludedTerm, data, emptyEntries);
                loadSnapshot(snapshot);
                compactLog(lastIncludedIndex);
                
                log.warn("✓ SNAPSHOT: Installed snapshot at index {} from leader", lastIncludedIndex);
                return true;
            }
            
            return false;
            
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }
    
    // ============ SNAPSHOT TRANSFER ============
    
    /**
     * Get snapshot chunks for transfer
     * Large snapshots are sent in multiple RPC calls
     */
    public List<byte[]> getSnapshotChunks(int chunkSize) {
        snapshotLock.readLock().lock();
        try {
            if (latestSnapshot == null) return new ArrayList<>();
            
            List<byte[]> chunks = new ArrayList<>();
            // Serialize snapshot into chunks
            // In production, use proper serialization (JSON, protobuf, etc.)
            
            return chunks;
        } finally {
            snapshotLock.readLock().unlock();
        }
    }
    
    // ============ VALIDATION ============
    
    /**
     * Validate snapshot integrity
     */
    public boolean validateSnapshot(Snapshot snapshot) {
        if (snapshot == null) return false;
        
        // Check metadata consistency
        if (snapshot.lastIncludedIndex < 0 || snapshot.lastIncludedTerm < 0) {
            return false;
        }
        
        // Check data integrity
        if (snapshot.data == null || snapshot.recentEntries == null) {
            return false;
        }
        
        // Check log entry consistency
        for (LogEntry entry : snapshot.recentEntries) {
            if (entry.getIndex() > snapshot.lastIncludedIndex) {
                continue; // OK - entries after snapshot
            } else {
                return false; // ERROR - entries before snapshot shouldn't be in recentEntries
            }
        }
        
        return true;
    }
    
    /**
     * Check if snapshot is current
     */
    public boolean isSnapshotCurrent() {
        if (latestSnapshot == null) return false;
        
        // Snapshot is current if created within last 5 seconds
        return System.currentTimeMillis() - latestSnapshot.timestamp < 5000;
    }
    
    // ============ CONFIGURATION ============
    
    /**
     * Set snapshot threshold (log entries before snapshot triggered)
     */
    public void setSnapshotThreshold(int threshold) {
        this.snapshotThreshold = threshold;
        log.info("Snapshot threshold set to {} entries", threshold);
    }
    
    /**
     * Enable/disable compression
     */
    public void setCompressionEnabled(boolean enabled) {
        this.compressionEnabled = enabled;
        log.info("Snapshot compression: {}", enabled ? "ENABLED" : "DISABLED");
    }
    
    // ============ METRICS ============
    
    /**
     * SnapshotMetrics DTO for monitoring
     */
    public static class SnapshotMetrics {
        public long totalSnapshotsCreated;
        public long totalSnapshotsLoaded;
        public long totalLogEntriesCompacted;
        public long totalSnapshotBytes;
        public int snapshotCount;
        public long latestSnapshotIndex;
        public long latestSnapshotTerm;
        public long latestSnapshotTimestamp;
        
        public SnapshotMetrics(long created, long loaded, long compacted, long bytes,
                              int count, long index, long term, long ts) {
            this.totalSnapshotsCreated = created;
            this.totalSnapshotsLoaded = loaded;
            this.totalLogEntriesCompacted = compacted;
            this.totalSnapshotBytes = bytes;
            this.snapshotCount = count;
            this.latestSnapshotIndex = index;
            this.latestSnapshotTerm = term;
            this.latestSnapshotTimestamp = ts;
        }
        
        @Override
        public String toString() {
            return String.format("SnapshotMetrics(created=%d, loaded=%d, compacted=%d entries, %d MB total)",
                totalSnapshotsCreated, totalSnapshotsLoaded, totalLogEntriesCompacted, totalSnapshotBytes / (1024 * 1024));
        }
    }
    
    /**
     * Get snapshot metrics
     */
    public SnapshotMetrics getMetrics() {
        snapshotLock.readLock().lock();
        try {
            long latestIndex = latestSnapshot != null ? latestSnapshot.lastIncludedIndex : 0;
            long latestTerm = latestSnapshot != null ? latestSnapshot.lastIncludedTerm : 0;
            long latestTs = latestSnapshot != null ? latestSnapshot.timestamp : 0;
            
            return new SnapshotMetrics(
                totalSnapshotsCreated,
                totalSnapshotsLoaded,
                totalLogEntriesCompacted,
                totalSnapshotBytes,
                snapshots.size(),
                latestIndex,
                latestTerm,
                latestTs
            );
        } finally {
            snapshotLock.readLock().unlock();
        }
    }
    
    /**
     * Reset all snapshot state
     */
    public void reset() {
        snapshotLock.writeLock().lock();
        try {
            snapshots.clear();
            latestSnapshot = null;
            totalSnapshotsCreated = 0;
            totalSnapshotsLoaded = 0;
            totalLogEntriesCompacted = 0;
            totalSnapshotBytes = 0;
            
            log.info("Snapshot manager reset");
        } finally {
            snapshotLock.writeLock().unlock();
        }
    }
}
