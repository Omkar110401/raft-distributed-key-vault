package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.raft.dto.RequestVoteRequest;
import com.omkar.distributed_key_vault.raft.dto.RequestVoteResponse;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/raft")
public class RaftController {
    private final ElectionService electionService;

    public RaftController(ElectionService electionService) {
        this.electionService = electionService;
    }
}
