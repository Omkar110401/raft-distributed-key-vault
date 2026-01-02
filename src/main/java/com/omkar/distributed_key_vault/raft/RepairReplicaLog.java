package com.omkar.distributed_key_vault.raft;

import org.springframework.stereotype.Component;
import java.util.List;

/**
 * Handles log divergence detection and repair in Phase 3.2
 * When follower logs diverge from leader, this component finds the conflict
 * point and helps repair the follower's log
 */
@Component
public class RepairReplicaLog {
    
    /**
     * Find the index where leader and follower logs diverge
     * Uses binary search for efficiency
     */
    public int findConflictIndex(List<LogEntry> leaderLog, List<LogEntry> followerLog) {
        if (followerLog == null || followerLog.isEmpty()) {
            return 0;
        }
        
        // Find highest index where logs match
        // Compare by entry index and term (not array position)
        int conflictIndex = 0;
        int maxLength = Math.min(leaderLog.size(), followerLog.size());
        
        for (int i = 0; i < maxLength; i++) {
            LogEntry leaderEntry = leaderLog.get(i);
            LogEntry followerEntry = followerLog.get(i);
            
            // Entries match if they have same index and term
            if (leaderEntry.getTerm() == followerEntry.getTerm() &&
                leaderEntry.getIndex() == followerEntry.getIndex()) {
                // This entry matches, conflict index is after this
                conflictIndex = leaderEntry.getIndex() + 1;
            } else {
                // First divergence found - return the entry index where they diverge
                return followerEntry.getIndex();
            }
        }
        
        // If all entries matched, conflict is after the last one
        // (or at the first entry beyond follower's log)
        if (maxLength == followerLog.size() && followerLog.size() < leaderLog.size()) {
            // Follower is behind leader
            return leaderLog.get(maxLength).getIndex();
        }
        
        return conflictIndex;
    }
    
    /**
     * Get entries to send to follower to repair its log
     * Starts from conflictIndex
     */
    public List<LogEntry> getEntriesToRepair(List<LogEntry> leaderLog, int conflictIndex) {
        if (conflictIndex >= leaderLog.size()) {
            return List.of(); // Follower is caught up
        }
        
        // Return entries from conflictIndex onwards
        return leaderLog.subList(conflictIndex, leaderLog.size());
    }
    
    /**
     * Check if entry at index matches term
     * Used in log verification
     */
    public boolean entryMatches(List<LogEntry> log, int index, int term) {
        if (index < 0 || index >= log.size()) {
            return false;
        }
        return log.get(index).getTerm() == term;
    }
    
    /**
     * Get the term of entry at index
     * Returns -1 if index out of bounds
     */
    public int getTermAt(List<LogEntry> log, int index) {
        if (index < 0 || index >= log.size()) {
            return -1;
        }
        return log.get(index).getTerm();
    }
    
    /**
     * Calculate how far behind a follower is (in entries)
     */
    public int calculateLag(int leaderLastIndex, int followerLastIndex) {
        return Math.max(0, leaderLastIndex - followerLastIndex);
    }
    
    /**
     * Determine next index to send to follower
     * After conflict detection, try sending from one position back
     */
    public int getNextRetryIndex(int conflictIndex) {
        // Back off by trying earlier in the log
        return Math.max(0, conflictIndex - 1);
    }
}
