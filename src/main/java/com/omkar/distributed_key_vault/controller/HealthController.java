package com.omkar.distributed_key_vault.controller;

import com.omkar.distributed_key_vault.config.NodeConfig;
import com.omkar.distributed_key_vault.raft.RaftState;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
public class HealthController {

    private final NodeConfig nodeConfig;
    private final RaftState raftState;

    public HealthController(NodeConfig nodeConfig, RaftState raftState){
        this.nodeConfig=nodeConfig;
        this.raftState=raftState;
    }

    @GetMapping("/health")
    public String health(){
        return "OK";
    }
}
