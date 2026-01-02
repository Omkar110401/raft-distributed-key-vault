package com.omkar.distributed_key_vault.raft;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@AllArgsConstructor
@NoArgsConstructor
public class LogEntry {
    private int index;
    private int term;
    private CommandType commandType;
    private String key;
    private String value;
    private long createdAt;
    
    // Phase 3.2 - Replication tracking
    private boolean applied;
    private long appliedAt;

    public LogEntry(int index, int term, String command) {
        this.index = index;
        this.term = term;
        this.commandType = CommandType.NOOP;
        this.createdAt = System.currentTimeMillis();
        this.applied = false;
        this.appliedAt = 0;
    }
    
    public boolean isApplied() {
        return applied;
    }
    
    public void setApplied(boolean applied) {
        this.applied = applied;
    }
}
