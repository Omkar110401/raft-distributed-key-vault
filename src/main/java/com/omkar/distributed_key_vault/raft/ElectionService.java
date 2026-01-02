package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.config.NodeConfig;
import com.omkar.distributed_key_vault.raft.dto.RequestVoteRequest;
import com.omkar.distributed_key_vault.raft.dto.RequestVoteResponse;
import com.omkar.distributed_key_vault.raft.rpc.AppendEntriesRequest;
import com.omkar.distributed_key_vault.raft.rpc.AppendEntriesResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import java.util.concurrent.atomic.AtomicInteger;
import java.util.ArrayList;
import java.util.List;

@Slf4j
@Service
public class ElectionService {
    private final RaftState raftState;
    private final NodeConfig nodeConfig;
    private final RestTemplate restTemplate = new RestTemplate();

    public ElectionService(RaftState raftState, NodeConfig nodeConfig) {
        this.raftState = raftState;
        this.nodeConfig = nodeConfig;
    }

    public ElectionResult startElection() {

        raftState.becomeCandidate(nodeConfig.getNodeId());
        int term = raftState.getCurrentTerm().get();

        AtomicInteger votes = new AtomicInteger(1); // self vote
        int totalNodes = nodeConfig.getPeers().size() + 1; // peers + self
        int majority = (totalNodes / 2) + 1;

        for (String peer : nodeConfig.getPeers()) {
            try {
                RequestVoteRequest request =
                        new RequestVoteRequest(
                                term, 
                                nodeConfig.getNodeId(),
                                raftState.getLastLogIndex(),
                                raftState.getLastLogTerm()
                        );

                RequestVoteResponse response =
                        restTemplate.postForObject(
                                peer + "/raft/request-vote",
                                request,
                                RequestVoteResponse.class
                        );

                if (response == null) continue;

                if (response.getTerm() > term) {
                    raftState.becomeFollower(response.getTerm());
                    return ElectionResult.STEPPED_DOWN;
                }

                // Only count votes for the current term
                if (response.isVoteGranted() && 
                        raftState.getCurrentTerm().get() == term &&
                        votes.incrementAndGet() >= majority) {
                    // Double-check term hasn't changed and we're still a candidate
                    if (raftState.getCurrentTerm().get() == term && raftState.isCandidate()) {
                        raftState.becomeLeader();
                        log.warn("Node {} became LEADER (term {})",
                                nodeConfig.getNodeId(), term);
                        return ElectionResult.WON;
                    }
                }

            } catch (Exception e) {
                log.warn("Failed to contact peer {}", peer);
            }
        }
        return ElectionResult.LOST;
    }

    public void sendHeartbeats() {
        if (!raftState.isLeader()) return;

        // Set our node ID as leader
        raftState.setLeaderId(nodeConfig.getNodeId());

        for (String peer : nodeConfig.getPeers()) {
            try {
                // Phase 3.2 - Get entries to send to this follower
                int nextIdx = raftState.getNextIndex(peer);
                List<LogEntry> entriesToSend = new ArrayList<>();
                
                // Collect entries starting from nextIndex
                if (nextIdx <= raftState.getLastLogIndex()) {
                    entriesToSend = raftState.getLog().subList(nextIdx, raftState.getLog().size());
                }
                
                AppendEntriesRequest request = new AppendEntriesRequest(
                        raftState.getCurrentTerm().get(),
                        nodeConfig.getNodeId()
                );
                request.setLeaderLastLogIndex(raftState.getLastLogIndex());
                request.setLeaderLastLogTerm(raftState.getLastLogTerm());
                request.setLeaderCommit(raftState.getCommitIndex());
                request.setEntries(entriesToSend);
                
                AppendEntriesResponse response = restTemplate.postForObject(
                        peer + "/raft/append-entries",
                        request,
                        AppendEntriesResponse.class
                );
                
                // Process response to update replication progress
                if (response != null && response.isSuccess()) {
                    raftState.updateMatchIndex(peer, response.getMatchIndex());
                    raftState.updateNextIndex(peer, response.getMatchIndex() + 1);
                    log.debug("Follower {} has entries up to index {}", peer, response.getMatchIndex());
                    
                    // Recalculate commit index based on replication progress
                    calculateAndUpdateCommitIndex();
                } else if (response != null && !response.isSuccess()) {
                    // Log divergence - back off and retry with earlier entries
                    int nextIndexForRetry = Math.max(0, raftState.getNextIndex(peer) - 1);
                    raftState.updateNextIndex(peer, nextIndexForRetry);
                    log.debug("Replication failed for {}, will retry from index {}", peer, nextIndexForRetry);
                }
                
            } catch (Exception e) {
                log.debug("Failed to send heartbeat to peer {}: {}", peer, e.getMessage());
            }
        }
    }
    
    /**
     * Calculate new commit index based on replication progress (Phase 3.2)
     * Advance commit index when majority of followers have entries
     */
    private void calculateAndUpdateCommitIndex() {
        if (!raftState.isLeader()) return;
        
        int newCommitIndex = raftState.getCommitIndex();
        int totalNodes = nodeConfig.getPeers().size() + 1;
        int majority = (totalNodes / 2) + 1;
        
        // Try to advance commit index
        for (int idx = raftState.getLastLogIndex(); idx > raftState.getCommitIndex(); idx--) {
            int replicaCount = 1; // self
            
            // Count how many followers have this entry
            for (String peer : nodeConfig.getPeers()) {
                if (raftState.getMatchIndex(peer) >= idx) {
                    replicaCount++;
                }
            }
            
            // If majority has this entry and it's from our term, we can commit it
            if (replicaCount >= majority && idx < raftState.getLog().size()) {
                LogEntry entry = raftState.getLog().get(idx);
                if (entry.getTerm() == raftState.getCurrentTerm().get()) {
                    newCommitIndex = idx;
                    break;
                }
            }
        }
        
        if (newCommitIndex > raftState.getCommitIndex()) {
            log.info("Advancing commitIndex from {} to {}", raftState.getCommitIndex(), newCommitIndex);
            raftState.setCommitIndex(newCommitIndex);
        }
    }
}
