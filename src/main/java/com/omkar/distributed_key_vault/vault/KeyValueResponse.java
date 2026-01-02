package com.omkar.distributed_key_vault.vault;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class KeyValueResponse {
    private String key;
    private String value;
    private boolean found;
    private String leaderId;
    private int term;
    private String message;
    
    // Phase 3.2: Replication tracking fields
    private String replicationStatus;  // PENDING, REPLICATED, NOT_REPLICATED, ERROR
    private Integer logIndex;           // Index of entry in leader's log
    private Integer lastAppliedIndex;   // Highest index applied to state machine
    private Integer commitIndex;        // Current commit index
}

