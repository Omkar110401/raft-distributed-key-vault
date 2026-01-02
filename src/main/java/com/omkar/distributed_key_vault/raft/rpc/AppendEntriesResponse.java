package com.omkar.distributed_key_vault.raft.rpc;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppendEntriesResponse {
    private int term;
    private boolean success;
    
    // Phase 3.2 - Replication tracking
    private int matchIndex;           // Highest entry follower has
    private int lastLogIndex;         // Follower's highest log index
    private int lastLogTerm;          // Term of follower's last entry
    private int conflictIndex;        // Where logs diverge (for log repair)
}
