package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.config.NodeConfig;
import com.omkar.distributed_key_vault.metrics.MetricsCollector;
import jakarta.annotation.PostConstruct;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
public class RaftCoordinator {
    private final ElectionService electionService;
    private final ElectionTimer electionTimer;
    private final RaftState raftState;
    private final HeartbeatScheduler heartbeatScheduler;
    private final MetricsCollector metricsCollector;
    private final NodeConfig nodeConfig;
    
    @Autowired(required = false)
    private CommandExecutor commandExecutor;
    
    @Autowired(required = false)
    private ReplicationMetrics replicationMetrics;

    @PostConstruct
    public void init() {
        // Initialize replication state for Phase 3.2
        raftState.initializeReplicationState(nodeConfig.getPeers());
    }

    public void onElectionTimeout() {
        if (raftState.isLeader()) return;

        metricsCollector.recordElectionStart(raftState.getCurrentTerm().get() + 1, "timeout");
        
        ElectionResult result = electionService.startElection();

        switch (result) {
            case WON -> {
                metricsCollector.recordElectionEnd(raftState.getCurrentTerm().get(), "WON", null);
                metricsCollector.recordRoleChange("CANDIDATE", "LEADER", raftState.getCurrentTerm().get());
                
                // Initialize replication state for new leader (Phase 3.2)
                raftState.initializeReplicationState(nodeConfig.getPeers());
                
                electionTimer.stop();
                heartbeatScheduler.start(electionService::sendHeartbeats);
            }
            case LOST, STEPPED_DOWN -> {
                metricsCollector.recordElectionEnd(raftState.getCurrentTerm().get(), result.toString(), null);
                electionTimer.reset();
            }
        }
    }

    public void onHeartbeatReceived() {
        electionTimer.reset();
    }
}
