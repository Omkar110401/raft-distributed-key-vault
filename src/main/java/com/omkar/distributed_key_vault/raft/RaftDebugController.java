package com.omkar.distributed_key_vault.raft;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class RaftDebugController {
    private final RaftState raftState;
    public RaftDebugController(RaftState raftState){
        this.raftState=raftState;
    }
    @GetMapping("/raft/state")
    public String state() {
        return "Role=" + raftState.getRole() +
                ", Term=" + raftState.getCurrentTerm().get();
    }
}
