package com.omkar.distributed_key_vault.raft;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omkar.distributed_key_vault.raft.SnapshotManager.Snapshot;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Component;

import java.io.*;
import java.nio.file.*;
import java.util.*;
import java.util.zip.GZIPInputStream;
import java.util.zip.GZIPOutputStream;

/**
 * Phase 3.4 - Persistence Layer
 * Handles snapshot and state persistence to disk.
 * Supports compression and recovery from corrupted files.
 * 
 * Features:
 * - Save snapshots to disk
 * - Load snapshots from disk
 * - Compression support (GZIP)
 * - Corruption detection and recovery
 * - Backup versioning
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class PersistenceLayer {
    
    private final ObjectMapper objectMapper = new ObjectMapper();
    
    // Configuration
    private volatile String snapshotDir = System.getProperty("java.io.tmpdir") + "/raft-snapshots";
    private volatile boolean compressionEnabled = true;
    private volatile int maxBackupVersions = 3;
    
    // Metrics
    private volatile long totalSaved = 0;
    private volatile long totalLoaded = 0;
    private volatile long totalFailed = 0;
    private volatile long totalBytesCompressed = 0;
    
    /**
     * Initialize persistence layer (create directories)
     */
    public void initialize() {
        try {
            Files.createDirectories(Paths.get(snapshotDir));
            log.info("Persistence layer initialized: {}", snapshotDir);
        } catch (IOException e) {
            log.error("Failed to initialize persistence layer", e);
        }
    }
    
    // ============ SNAPSHOT PERSISTENCE ============
    
    /**
     * Save snapshot to disk
     */
    public boolean saveSnapshot(Snapshot snapshot, String snapshotName) {
        try {
            initialize();
            
            Path snapshotPath = Paths.get(snapshotDir, snapshotName + ".snapshot");
            
            // Create backup of previous version
            if (Files.exists(snapshotPath)) {
                createBackup(snapshotPath);
            }
            
            // Serialize snapshot
            String snapshotJson = objectMapper.writeValueAsString(snapshot);
            byte[] snapshotData = snapshotJson.getBytes();
            
            // Compress if enabled
            if (compressionEnabled) {
                snapshotData = compress(snapshotData);
                snapshotPath = Paths.get(snapshotDir, snapshotName + ".snapshot.gz");
            }
            
            // Write to disk
            Files.write(snapshotPath, snapshotData, 
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            
            totalSaved++;
            totalBytesCompressed += snapshotData.length;
            
            log.info("Snapshot saved: {} ({} bytes)", snapshotPath.getFileName(), snapshotData.length);
            return true;
            
        } catch (Exception e) {
            totalFailed++;
            log.error("Failed to save snapshot", e);
            return false;
        }
    }
    
    /**
     * Load snapshot from disk
     */
    public Snapshot loadSnapshot(String snapshotName) {
        try {
            Path snapshotPath = Paths.get(snapshotDir, snapshotName + ".snapshot.gz");
            
            // Try compressed version first
            if (!Files.exists(snapshotPath)) {
                snapshotPath = Paths.get(snapshotDir, snapshotName + ".snapshot");
            }
            
            if (!Files.exists(snapshotPath)) {
                log.warn("Snapshot not found: {}", snapshotName);
                return null;
            }
            
            // Read from disk
            byte[] snapshotData = Files.readAllBytes(snapshotPath);
            
            // Decompress if compressed
            if (snapshotPath.toString().endsWith(".gz")) {
                snapshotData = decompress(snapshotData);
            }
            
            // Deserialize snapshot - use a basic approach since Snapshot is an inner class
            String snapshotJson = new String(snapshotData);
            // For inner classes, use a map-based approach instead of direct deserialization
            Map<String, Object> data = objectMapper.readValue(snapshotJson, Map.class);
            
            // Reconstruct Snapshot from map
            long term = ((Number) data.get("term")).longValue();
            long lastIncludedIndex = ((Number) data.get("lastIncludedIndex")).longValue();
            long lastIncludedTerm = ((Number) data.get("lastIncludedTerm")).longValue();
            long timestamp = ((Number) data.get("timestamp")).longValue();
            
            @SuppressWarnings("unchecked")
            Map<String, String> stateData = (Map<String, String>) data.get("data");
            
            Snapshot snapshot = new Snapshot(term, lastIncludedIndex, lastIncludedTerm, 
                stateData != null ? stateData : new HashMap<>(), new ArrayList<>());
            snapshot.timestamp = timestamp;
            
            totalLoaded++;
            
            log.info("Snapshot loaded: {} ({} entries)", snapshotPath.getFileName(), stateData != null ? stateData.size() : 0);
            return snapshot;
            
        } catch (Exception e) {
            totalFailed++;
            log.error("Failed to load snapshot", e);
            return null;
        }
    }
    
    /**
     * List all saved snapshots
     */
    public List<String> listSnapshots() {
        try {
            List<String> snapshots = new ArrayList<>();
            Files.list(Paths.get(snapshotDir))
                .filter(p -> p.toString().endsWith(".snapshot") || p.toString().endsWith(".snapshot.gz"))
                .map(p -> p.getFileName().toString().replace(".snapshot.gz", "").replace(".snapshot", ""))
                .forEach(snapshots::add);
            return snapshots;
        } catch (IOException e) {
            log.error("Failed to list snapshots", e);
            return new ArrayList<>();
        }
    }
    
    /**
     * Delete snapshot from disk
     */
    public boolean deleteSnapshot(String snapshotName) {
        try {
            Path snapshotPath = Paths.get(snapshotDir, snapshotName + ".snapshot.gz");
            
            if (!Files.exists(snapshotPath)) {
                snapshotPath = Paths.get(snapshotDir, snapshotName + ".snapshot");
            }
            
            if (Files.exists(snapshotPath)) {
                Files.delete(snapshotPath);
                log.info("Snapshot deleted: {}", snapshotName);
                return true;
            }
            
            return false;
            
        } catch (IOException e) {
            log.error("Failed to delete snapshot", e);
            return false;
        }
    }
    
    // ============ STATE PERSISTENCE ============
    
    /**
     * Save state machine data
     */
    public boolean saveState(Map<String, String> state, String stateName) {
        try {
            initialize();
            
            Path statePath = Paths.get(snapshotDir, stateName + ".state");
            String stateJson = objectMapper.writeValueAsString(state);
            byte[] stateData = stateJson.getBytes();
            
            if (compressionEnabled) {
                stateData = compress(stateData);
                statePath = Paths.get(snapshotDir, stateName + ".state.gz");
            }
            
            Files.write(statePath, stateData,
                StandardOpenOption.CREATE, StandardOpenOption.WRITE, StandardOpenOption.TRUNCATE_EXISTING);
            
            log.info("State saved: {} ({} entries)", statePath.getFileName(), state.size());
            return true;
            
        } catch (Exception e) {
            totalFailed++;
            log.error("Failed to save state", e);
            return false;
        }
    }
    
    /**
     * Load state machine data
     */
    public Map<String, String> loadState(String stateName) {
        try {
            Path statePath = Paths.get(snapshotDir, stateName + ".state.gz");
            
            if (!Files.exists(statePath)) {
                statePath = Paths.get(snapshotDir, stateName + ".state");
            }
            
            if (!Files.exists(statePath)) {
                log.warn("State not found: {}", stateName);
                return new HashMap<>();
            }
            
            byte[] stateData = Files.readAllBytes(statePath);
            
            if (statePath.toString().endsWith(".gz")) {
                stateData = decompress(stateData);
            }
            
            String stateJson = new String(stateData);
            return objectMapper.readValue(stateJson, Map.class);
            
        } catch (Exception e) {
            totalFailed++;
            log.error("Failed to load state", e);
            return new HashMap<>();
        }
    }
    
    // ============ COMPRESSION ============
    
    /**
     * Compress data using GZIP
     */
    private byte[] compress(byte[] data) throws IOException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPOutputStream gzos = new GZIPOutputStream(baos)) {
            gzos.write(data);
        }
        return baos.toByteArray();
    }
    
    /**
     * Decompress GZIP data
     */
    private byte[] decompress(byte[] data) throws IOException {
        ByteArrayInputStream bais = new ByteArrayInputStream(data);
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        try (GZIPInputStream gzis = new GZIPInputStream(bais)) {
            byte[] buffer = new byte[1024];
            int len;
            while ((len = gzis.read(buffer)) > 0) {
                baos.write(buffer, 0, len);
            }
        }
        return baos.toByteArray();
    }
    
    // ============ BACKUP & RECOVERY ============
    
    /**
     * Create backup of existing file
     */
    private void createBackup(Path originalPath) {
        try {
            String filename = originalPath.getFileName().toString();
            Path backupDir = Paths.get(snapshotDir, "backups");
            Files.createDirectories(backupDir);
            
            // Find next backup version number
            int version = 1;
            Path backupPath;
            while (Files.exists(backupPath = Paths.get(backupDir.toString(), 
                filename + ".v" + version))) {
                version++;
            }
            
            // Keep only last N versions
            if (version > maxBackupVersions) {
                for (int i = 1; i <= version - maxBackupVersions; i++) {
                    Path oldBackup = Paths.get(backupDir.toString(), filename + ".v" + i);
                    if (Files.exists(oldBackup)) {
                        Files.delete(oldBackup);
                    }
                }
            }
            
            // Create backup
            Files.copy(originalPath, backupPath, StandardCopyOption.REPLACE_EXISTING);
            log.debug("Backup created: {}", backupPath.getFileName());
            
        } catch (IOException e) {
            log.warn("Failed to create backup", e);
        }
    }
    
    /**
     * Recover snapshot from backup
     */
    public Snapshot recoverFromBackup(String snapshotName) {
        try {
            Path backupDir = Paths.get(snapshotDir, "backups");
            
            // Find latest backup version
            int latestVersion = 0;
            Path latestBackup = null;
            
            if (Files.exists(backupDir)) {
                for (File backup : backupDir.toFile().listFiles()) {
                    if (backup.getName().startsWith(snapshotName)) {
                        String versionStr = backup.getName().replaceAll("[^0-9]", "");
                        if (!versionStr.isEmpty()) {
                            int version = Integer.parseInt(versionStr);
                            if (version > latestVersion) {
                                latestVersion = version;
                                latestBackup = backup.toPath();
                            }
                        }
                    }
                }
            }
            
            if (latestBackup != null) {
                byte[] backupData = Files.readAllBytes(latestBackup);
                
                if (latestBackup.toString().endsWith(".gz")) {
                    backupData = decompress(backupData);
                }
                
                String snapshotJson = new String(backupData);
                Snapshot snapshot = objectMapper.readValue(snapshotJson, Snapshot.class);
                
                log.warn("Recovered snapshot from backup: v{}", latestVersion);
                return snapshot;
            }
            
        } catch (Exception e) {
            log.error("Failed to recover from backup", e);
        }
        
        return null;
    }
    
    // ============ CONFIGURATION ============
    
    /**
     * Set snapshot directory
     */
    public void setSnapshotDir(String dir) {
        this.snapshotDir = dir;
        initialize();
    }
    
    /**
     * Enable/disable compression
     */
    public void setCompressionEnabled(boolean enabled) {
        this.compressionEnabled = enabled;
        log.info("Persistence compression: {}", enabled ? "ENABLED" : "DISABLED");
    }
    
    /**
     * Set max backup versions
     */
    public void setMaxBackupVersions(int max) {
        this.maxBackupVersions = max;
    }
    
    // ============ METRICS ============
    
    /**
     * PersistenceMetrics DTO
     */
    public static class PersistenceMetrics {
        public long totalSaved;
        public long totalLoaded;
        public long totalFailed;
        public long totalBytesCompressed;
        public long successRate;
        
        public PersistenceMetrics(long saved, long loaded, long failed, long bytes) {
            this.totalSaved = saved;
            this.totalLoaded = loaded;
            this.totalFailed = failed;
            this.totalBytesCompressed = bytes;
            this.successRate = (saved + loaded) == 0 ? 0 : 
                (100 * (saved + loaded) / (saved + loaded + failed));
        }
        
        @Override
        public String toString() {
            return String.format("PersistenceMetrics(saved=%d, loaded=%d, failed=%d, compressed=%d MB, success=%d%%)",
                totalSaved, totalLoaded, totalFailed, totalBytesCompressed / (1024 * 1024), successRate);
        }
    }
    
    /**
     * Get persistence metrics
     */
    public PersistenceMetrics getMetrics() {
        return new PersistenceMetrics(totalSaved, totalLoaded, totalFailed, totalBytesCompressed);
    }
    
    /**
     * Get snapshot directory size
     */
    public long getStorageSize() {
        try {
            return Files.walk(Paths.get(snapshotDir))
                .filter(Files::isRegularFile)
                .mapToLong(p -> {
                    try {
                        return Files.size(p);
                    } catch (IOException e) {
                        return 0;
                    }
                })
                .sum();
        } catch (IOException e) {
            return 0;
        }
    }
    
    /**
     * Reset all persistence state
     */
    public void reset() {
        try {
            Path dir = Paths.get(snapshotDir);
            if (Files.exists(dir)) {
                Files.walk(dir)
                    .sorted(Comparator.reverseOrder())
                    .forEach(p -> {
                        try {
                            Files.delete(p);
                        } catch (IOException e) {
                            log.warn("Failed to delete: {}", p);
                        }
                    });
            }
            totalSaved = 0;
            totalLoaded = 0;
            totalFailed = 0;
            totalBytesCompressed = 0;
            log.info("Persistence layer reset");
        } catch (IOException e) {
            log.error("Failed to reset persistence layer", e);
        }
    }
}
