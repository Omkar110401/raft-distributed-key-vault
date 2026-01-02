package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.raft.persistence.PersistentRaftState;
import com.omkar.distributed_key_vault.raft.persistence.RaftStateStore;
import jakarta.annotation.PostConstruct;
import lombok.Getter;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

@Getter
@Component
public class RaftState {
    private volatile NodeRole role = NodeRole.FOLLOWER;
    private final AtomicInteger currentTerm=new AtomicInteger(0);
    private volatile String votedFor=null;
    private volatile int lastLogIndex = 0;
    private volatile int lastLogTerm = 0;
    private final List<LogEntry> log = new ArrayList<>();
    private final RaftStateStore store;
    private volatile int commitIndex = 0;
    private volatile int lastAppliedIndex = 0;
    private volatile String leaderId = null;
    
    // Phase 3.2 - Replication tracking
    private final Map<String, Integer> matchIndex = new ConcurrentHashMap<>();  // Track follower progress
    private final Map<String, Integer> nextIndex = new ConcurrentHashMap<>();   // Next entry to send to each follower

    public RaftState(RaftStateStore store) {
        this.store = store;
    }

    @PostConstruct
    public void init() {
        PersistentRaftState stored = store.load();
        currentTerm.set(stored.getCurrentTerm());
        votedFor = stored.getVotedFor();
    }

    public synchronized void becomeFollower(int term){
        role= NodeRole.FOLLOWER;
        currentTerm.set(term);
        votedFor=null;
        store.save(term, null);
        leaderId = null;
    }

    public synchronized void becomeCandidate(String selfId){
        role= NodeRole.CANDIDATE;
        currentTerm.incrementAndGet();
        votedFor=selfId;
        store.save(currentTerm.get(), votedFor);
    }

    public synchronized void becomeLeader(){
        role= NodeRole.LEADER;
        // Append a log entry when becoming leader to help commit previous terms' entries
        appendLogEntry(new LogEntry(lastLogIndex + 1, currentTerm.get(), "NOOP"));
    }

    public synchronized boolean canVoteFor(String candidateId){
        return votedFor==null || votedFor.equals(candidateId);
    }

    public synchronized void recordVote(String candidateId){
        votedFor=candidateId;
        store.save(currentTerm.get(), votedFor);
    }

    public synchronized void clearVote(){
        votedFor=null;
        store.save(currentTerm.get(), null);
    }

    public synchronized void updateLogIndex(int index, int term) {
        this.lastLogIndex = index;
        this.lastLogTerm = term;
    }

    public synchronized void appendLogEntry(LogEntry entry) {
        log.add(entry);
        lastLogIndex = entry.getIndex();
        lastLogTerm = entry.getTerm();
    }

    public synchronized void replicateEntry(int leaderIndex, int leaderTerm) {
        // Simulate receiving and applying a log entry from leader
        if (leaderIndex > lastLogIndex) {
            LogEntry entry = new LogEntry(leaderIndex, leaderTerm, "NOOP");
            log.add(entry);
            lastLogIndex = leaderIndex;
            lastLogTerm = leaderTerm;
        }
    }

    public synchronized void setCommitIndex(int index) {
        this.commitIndex = index;
    }

    public int getCommitIndex() {
        return commitIndex;
    }

    public synchronized void setLastAppliedIndex(int index) {
        this.lastAppliedIndex = index;
    }

    public int getLastAppliedIndex() {
        return lastAppliedIndex;
    }

    public String getLeaderId() {
        return leaderId;
    }

    public synchronized void setLeaderId(String leaderId) {
        this.leaderId = leaderId;
    }

    public boolean isLeader() {
        return role == NodeRole.LEADER;
    }

    public boolean isFollower() {
        return role == NodeRole.FOLLOWER;
    }

    public boolean isCandidate() {
        return role == NodeRole.CANDIDATE;
    }
    
    // Phase 3.2 - Replication tracking methods
    public synchronized void updateMatchIndex(String nodeId, int index) {
        matchIndex.put(nodeId, index);
    }
    
    public int getMatchIndex(String nodeId) {
        return matchIndex.getOrDefault(nodeId, 0);
    }
    
    public Map<String, Integer> getAllMatchIndices() {
        return new ConcurrentHashMap<>(matchIndex);
    }
    
    public synchronized void updateNextIndex(String nodeId, int index) {
        nextIndex.put(nodeId, index);
    }
    
    public int getNextIndex(String nodeId) {
        return nextIndex.getOrDefault(nodeId, lastLogIndex + 1);
    }
    
    public synchronized void initializeReplicationState(List<String> peers) {
        for (String peer : peers) {
            nextIndex.put(peer, lastLogIndex + 1);
            matchIndex.put(peer, 0);
        }
    }
}
