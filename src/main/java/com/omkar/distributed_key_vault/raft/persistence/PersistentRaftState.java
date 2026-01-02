package com.omkar.distributed_key_vault.raft.persistence;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
@AllArgsConstructor
public class PersistentRaftState {
    private int currentTerm;
    private String votedFor;
}
