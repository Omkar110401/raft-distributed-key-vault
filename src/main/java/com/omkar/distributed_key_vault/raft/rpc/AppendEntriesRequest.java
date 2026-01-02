package com.omkar.distributed_key_vault.raft.rpc;

import com.omkar.distributed_key_vault.raft.LogEntry;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class AppendEntriesRequest {
    private int term;
    private String leaderId;
    private int leaderCommit;
    private int leaderLastLogIndex;
    private int leaderLastLogTerm;
    private List<LogEntry> entries;

    public AppendEntriesRequest(int term, String leaderId) {
        this.term = term;
        this.leaderId = leaderId;
        this.leaderCommit = 0;
        this.leaderLastLogIndex = 0;
        this.leaderLastLogTerm = 0;
        this.entries = List.of();
    }
}
