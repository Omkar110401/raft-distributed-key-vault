package com.omkar.distributed_key_vault.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestVoteRequest {
    private int term;
    private String candidateId;
    private int lastLogIndex;
    private int lastLogTerm;
}
