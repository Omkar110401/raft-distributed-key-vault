package com.omkar.distributed_key_vault.raft.dto;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@AllArgsConstructor
@NoArgsConstructor
public class RequestVoteResponse {
    private int term;
    private boolean voteGranted;
}
