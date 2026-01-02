package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.config.NodeConfig;
import com.omkar.distributed_key_vault.raft.rpc.AppendEntriesRequest;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class HeartbeatService {
    private final RaftState raftState;
    private final NodeConfig nodeConfig;
    private final RestTemplate restTemplate = new RestTemplate();

    public HeartbeatService(RaftState raftState, NodeConfig nodeConfig) {
        this.raftState = raftState;
        this.nodeConfig = nodeConfig;
    }

    @Scheduled(fixedDelay = 1000)
    public void sendHeartbeats() {
        if (!raftState.isLeader()) return;

        for (String peer : nodeConfig.getPeers()) {
            try {
                AppendEntriesRequest request =
                        new AppendEntriesRequest(
                                raftState.getCurrentTerm().get(),
                                nodeConfig.getNodeId()
                        );

                restTemplate.postForObject(
                        peer + "/raft/append-entries",
                        request,
                        Object.class
                );
            } catch (Exception ignored) {
                // Peer might be down
            }
        }
    }
}
