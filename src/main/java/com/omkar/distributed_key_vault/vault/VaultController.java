package com.omkar.distributed_key_vault.vault;

import com.omkar.distributed_key_vault.config.NodeConfig;
import com.omkar.distributed_key_vault.metrics.MetricsCollector;
import com.omkar.distributed_key_vault.raft.RaftState;
import com.omkar.distributed_key_vault.raft.CommandType;
import com.omkar.distributed_key_vault.raft.LogEntry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@Slf4j
@RestController
@RequestMapping("/vault")
@RequiredArgsConstructor
public class VaultController {
    private final KeyVaultStore keyVaultStore;
    private final RaftState raftState;
    private final NodeConfig nodeConfig;
    private final MetricsCollector metricsCollector;

    @PutMapping("/key")
    public ResponseEntity<KeyValueResponse> putKey(@RequestBody KeyValueRequest request) {
        long startTime = System.currentTimeMillis();
        
        if (!raftState.isLeader()) {
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordWriteRequest(request.getKey(), "REJECTED_NOT_LEADER", latency);
            
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(KeyValueResponse.builder()
                            .key(request.getKey())
                            .leaderId(raftState.getLeaderId() != null ? raftState.getLeaderId() : "UNKNOWN")
                            .term(raftState.getCurrentTerm().get())
                            .message("This node is not the leader. Write rejected.")
                            .replicationStatus("NOT_REPLICATED")
                            .build());
        }

        try {
            // Phase 3.2: Create a log entry with PUT command instead of executing immediately
            LogEntry entry = LogEntry.builder()
                    .term(raftState.getCurrentTerm().get())
                    .commandType(CommandType.PUT)
                    .key(request.getKey())
                    .value(request.getValue())
                    .createdAt(System.currentTimeMillis())
                    .applied(false)
                    .build();
            
            // Add to log - will be replicated to followers
            raftState.getLog().add(entry);
            
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordWriteRequest(request.getKey(), "REPLICATED", latency);
            
            log.info("PUT command added to log: {} = {} (term: {})", 
                    request.getKey(), request.getValue(), entry.getTerm());
            
            return ResponseEntity.accepted()
                    .body(KeyValueResponse.builder()
                            .key(request.getKey())
                            .value(request.getValue())
                            .found(true)
                            .leaderId(nodeConfig.getNodeId())
                            .term(raftState.getCurrentTerm().get())
                            .message("Key replication initiated")
                            .replicationStatus("PENDING")
                            .logIndex(raftState.getLog().size() - 1)
                            .build());
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordWriteRequest(request.getKey(), "ERROR", latency);
            
            log.error("Error adding PUT command to log: {}", request.getKey(), e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(KeyValueResponse.builder()
                            .key(request.getKey())
                            .message("Error replicating key: " + e.getMessage())
                            .build());
        }
    }

    @GetMapping("/key/{key}")
    public ResponseEntity<KeyValueResponse> getKey(@PathVariable String key) {
        long startTime = System.currentTimeMillis();
        
        // Phase 3.2: Only leader can serve reads (all data is committed via leader replication)
        if (!raftState.isLeader()) {
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordReadRequest(key, "REJECTED_NOT_LEADER", latency);
            
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(KeyValueResponse.builder()
                            .key(key)
                            .leaderId(raftState.getLeaderId() != null ? raftState.getLeaderId() : "UNKNOWN")
                            .term(raftState.getCurrentTerm().get())
                            .message("Reads only served by leader")
                            .build());
        }

        try {
            // Phase 3.2: Read from state machine - only returns applied values
            String value = keyVaultStore.get(key);
            boolean found = value != null;
            long latency = System.currentTimeMillis() - startTime;
            
            metricsCollector.recordReadRequest(key, found ? "SUCCESS" : "NOT_FOUND", latency);
            
            if (!found) {
                return ResponseEntity.status(HttpStatus.NOT_FOUND)
                        .body(KeyValueResponse.builder()
                                .key(key)
                                .found(false)
                                .leaderId(nodeConfig.getNodeId())
                                .term(raftState.getCurrentTerm().get())
                                .message("Key not found")
                                .lastAppliedIndex(raftState.getLastAppliedIndex())
                                .commitIndex(raftState.getCommitIndex())
                                .build());
            }

            return ResponseEntity.ok(KeyValueResponse.builder()
                    .key(key)
                    .value(value)
                    .found(true)
                    .leaderId(nodeConfig.getNodeId())
                    .term(raftState.getCurrentTerm().get())
                    .message("Key retrieved successfully")
                    .lastAppliedIndex(raftState.getLastAppliedIndex())
                    .commitIndex(raftState.getCommitIndex())
                    .build());
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordReadRequest(key, "ERROR", latency);
            
            log.error("Error retrieving key: {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(KeyValueResponse.builder()
                            .key(key)
                            .message("Error retrieving key: " + e.getMessage())
                            .build());
        }
    }

    @GetMapping("/all")
    public ResponseEntity<?> getAllKeys() {
        if (!raftState.isLeader()) {
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(new Object() {
                        public String message = "Reads only served by leader";
                        public String leaderId = raftState.getLeaderId() != null ? raftState.getLeaderId() : "UNKNOWN";
                    });
        }

        return ResponseEntity.ok(keyVaultStore.getAll());
    }

    @DeleteMapping("/key/{key}")
    public ResponseEntity<KeyValueResponse> deleteKey(@PathVariable String key) {
        long startTime = System.currentTimeMillis();
        
        if (!raftState.isLeader()) {
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordWriteRequest(key, "REJECTED_NOT_LEADER", latency);
            
            return ResponseEntity.status(HttpStatus.FORBIDDEN)
                    .body(KeyValueResponse.builder()
                            .key(key)
                            .leaderId(raftState.getLeaderId() != null ? raftState.getLeaderId() : "UNKNOWN")
                            .term(raftState.getCurrentTerm().get())
                            .message("Only leader can delete keys")
                            .replicationStatus("NOT_REPLICATED")
                            .build());
        }

        try {
            // Phase 3.2: Create a log entry with DELETE command instead of executing immediately
            LogEntry entry = LogEntry.builder()
                    .term(raftState.getCurrentTerm().get())
                    .commandType(CommandType.DELETE)
                    .key(key)
                    .createdAt(System.currentTimeMillis())
                    .applied(false)
                    .build();
            
            // Add to log - will be replicated to followers
            raftState.getLog().add(entry);
            
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordWriteRequest(key, "REPLICATED", latency);
            
            log.info("DELETE command added to log for key: {} (term: {})", key, entry.getTerm());
            
            return ResponseEntity.accepted()
                    .body(KeyValueResponse.builder()
                            .key(key)
                            .found(true)
                            .leaderId(nodeConfig.getNodeId())
                            .term(raftState.getCurrentTerm().get())
                            .message("Key deletion initiated")
                            .replicationStatus("PENDING")
                            .logIndex(raftState.getLog().size() - 1)
                            .build());
        } catch (Exception e) {
            long latency = System.currentTimeMillis() - startTime;
            metricsCollector.recordWriteRequest(key, "ERROR", latency);
            
            log.error("Error adding DELETE command to log for key: {}", key, e);
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                    .body(KeyValueResponse.builder()
                            .key(key)
                            .message("Error deleting key: " + e.getMessage())
                            .build());
        }
    }
}
