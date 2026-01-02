package com.omkar.distributed_key_vault.raft.controller;

import com.omkar.distributed_key_vault.raft.*;
import com.omkar.distributed_key_vault.raft.dto.RequestVoteRequest;
import com.omkar.distributed_key_vault.raft.dto.RequestVoteResponse;
import com.omkar.distributed_key_vault.raft.rpc.AppendEntriesRequest;
import com.omkar.distributed_key_vault.raft.rpc.AppendEntriesResponse;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@Slf4j
@RestController
@RequestMapping("/raft")
public class RaftRpcController {
    private final RaftState raftState;
    private final RaftCoordinator coordinator;
    
    @Autowired(required = false)
    private CommandExecutor commandExecutor;
    
    @Autowired(required = false)
    private ReplicationMetrics replicationMetrics;
    
    @Autowired(required = false)
    private RepairReplicaLog repairReplicaLog;

    public RaftRpcController(RaftState raftState, RaftCoordinator coordinator) {
        this.raftState = raftState;
        this.coordinator = coordinator;
    }

    @PostMapping("/request-vote")
    public synchronized RequestVoteResponse requestVote(
            @RequestBody RequestVoteRequest request
    ) {
        int currentTerm = raftState.getCurrentTerm().get();

        // Reject stale candidate
        if (request.getTerm() < currentTerm) {
            return new RequestVoteResponse(currentTerm, false);
        }

        // Candidate has newer term → step down to follower immediately
        if (request.getTerm() > currentTerm) {
            raftState.becomeFollower(request.getTerm());
            coordinator.onHeartbeatReceived(); // reset timer for new term
        }

        // Check if candidate's log is at least as up-to-date as ours
        boolean logIsUpToDate = request.getLastLogTerm() > raftState.getLastLogTerm() ||
                (request.getLastLogTerm() == raftState.getLastLogTerm() && 
                 request.getLastLogIndex() >= raftState.getLastLogIndex());

        // Only vote if we haven't voted in this term or voting for the same candidate
        // AND candidate's log is at least as up-to-date as ours
        if (logIsUpToDate && raftState.canVoteFor(request.getCandidateId())) {
            raftState.recordVote(request.getCandidateId());
            coordinator.onHeartbeatReceived(); // reset timer
            log.info("Vote granted to {}", request.getCandidateId());
            return new RequestVoteResponse(
                    raftState.getCurrentTerm().get(),
                    true
            );
        }

        // Even if we deny the vote, reset timer because we've seen valid election activity
        coordinator.onHeartbeatReceived();
        return new RequestVoteResponse(
                raftState.getCurrentTerm().get(),
                false
        );
    }

    @PostMapping("/append-entries")
    public synchronized AppendEntriesResponse appendEntries(
            @RequestBody AppendEntriesRequest request
    ) {
        int currentTerm = raftState.getCurrentTerm().get();
        long startTime = System.currentTimeMillis();

        // Reject stale leader
        if (request.getTerm() < currentTerm) {
            log.debug("Rejecting heartbeat: stale term {} < {}", request.getTerm(), currentTerm);
            return buildAppendEntriesResponse(currentTerm, false);
        }

        // Leader has newer term → become follower
        if (request.getTerm() > currentTerm) {
            log.info("Heartbeat with newer term {}, stepping down from {} to FOLLOWER", request.getTerm(), currentTerm);
            raftState.becomeFollower(request.getTerm());
        }
        
        // Accept heartbeat from current or higher term leader
        if (request.getTerm() >= currentTerm) {
            // If we're a candidate, step down to follower when receiving valid heartbeat
            if (raftState.isCandidate()) {
                log.info("CANDIDATE accepting heartbeat from {} at term {}, becoming FOLLOWER", request.getLeaderId(), request.getTerm());
                raftState.becomeFollower(request.getTerm());
            }
            
            // Set leader ID
            raftState.setLeaderId(request.getLeaderId());
            
            // Phase 3.2 - Handle multi-entry replication
            List<LogEntry> entries = request.getEntries();
            if (entries != null && !entries.isEmpty()) {
                // Append entries to log
                for (LogEntry entry : entries) {
                    raftState.appendLogEntry(entry);
                }
                log.info("Appended {} entries from leader", entries.size());
            }
            
            // Update commit index based on leader's commit
            if (request.getLeaderCommit() > raftState.getCommitIndex()) {
                int newCommitIndex = Math.min(request.getLeaderCommit(), raftState.getLastLogIndex());
                raftState.setCommitIndex(newCommitIndex);
                log.info("Updating commitIndex from {} to {}", raftState.getCommitIndex(), newCommitIndex);
                
                // Apply committed entries to state machine
                applyCommittedEntries();
            }
            
            coordinator.onHeartbeatReceived(); // reset election timer
            
            // Record metrics
            if (replicationMetrics != null && entries != null && !entries.isEmpty()) {
                long latency = System.currentTimeMillis() - startTime;
                replicationMetrics.recordReplicationAck(request.getLeaderId(), raftState.getLastLogIndex(), latency);
            }
            
            return buildAppendEntriesResponse(raftState.getCurrentTerm().get(), true);
        }

        return buildAppendEntriesResponse(raftState.getCurrentTerm().get(), false);
    }
    
    /**
     * Build AppendEntriesResponse with replication tracking info (Phase 3.2)
     */
    private AppendEntriesResponse buildAppendEntriesResponse(int term, boolean success) {
        AppendEntriesResponse response = new AppendEntriesResponse();
        response.setTerm(term);
        response.setSuccess(success);
        response.setLastLogIndex(raftState.getLastLogIndex());
        response.setLastLogTerm(raftState.getLastLogTerm());
        response.setMatchIndex(raftState.getLastLogIndex());
        response.setConflictIndex(0);
        return response;
    }

    private synchronized void applyCommittedEntries() {
        int lastApplied = raftState.getLastAppliedIndex();
        int commitIndex = raftState.getCommitIndex();
        
        if (commitIndex > lastApplied && commandExecutor != null) {
            log.info("Applying entries from {} to {}", lastApplied + 1, commitIndex);
            
            // Apply each committed entry to state machine
            List<LogEntry> entries = raftState.getLog();
            for (int i = lastApplied; i < commitIndex && i < entries.size(); i++) {
                LogEntry entry = entries.get(i);
                
                // Skip if already applied
                if (entry.isApplied()) {
                    continue;
                }
                
                // Validate command
                if (!commandExecutor.validateCommand(entry)) {
                    log.warn("Invalid command in entry {}: {}", entry.getIndex(), entry.getCommandType());
                    continue;
                }
                
                // Execute command on state machine
                if (commandExecutor.executeCommand(entry)) {
                    log.debug("Applied entry {} to state machine", entry.getIndex());
                    if (replicationMetrics != null) {
                        replicationMetrics.recordCommandApplied(entry.getCommandType().toString());
                    }
                } else {
                    log.warn("Failed to execute entry {}", entry.getIndex());
                }
            }
            
            // Update lastAppliedIndex
            raftState.setLastAppliedIndex(commitIndex);
            
            if (replicationMetrics != null) {
                replicationMetrics.recordCommitIndexAdvance(lastApplied, commitIndex);
            }
        }
    }
}
