package com.omkar.distributed_key_vault.raft.persistence;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.omkar.distributed_key_vault.config.NodeConfig;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@Component
public class RaftStateStore {
    private final ObjectMapper mapper = new ObjectMapper();
    private final Path path;
    public RaftStateStore(NodeConfig config) {
        this.path = Paths.get("data/" + config.getNodeId() + "/raft-state.json");
        path.getParent().toFile().mkdirs();
    }

    public synchronized void save(int term, String votedFor) {
        try {
            mapper.writeValue(path.toFile(),
                    new PersistentRaftState(term, votedFor));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public synchronized PersistentRaftState load() {
        if (!Files.exists(path)) {
            return new PersistentRaftState(0, null);
        }
        try {
            return mapper.readValue(path.toFile(),
                    PersistentRaftState.class);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
