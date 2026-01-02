package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.metrics.MetricsCollector;
import com.omkar.distributed_key_vault.vault.KeyVaultStore;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class CommandService {
    private final KeyVaultStore keyVaultStore;
    private final MetricsCollector metricsCollector;

    public void applyCommand(LogEntry entry) {
        long startTime = System.currentTimeMillis();

        try {
            switch (entry.getCommandType()) {
                case PUT -> {
                    keyVaultStore.put(entry.getKey(), entry.getValue());
                    log.info("Applied PUT: {} = {}", entry.getKey(), entry.getValue());
                }
                case DELETE -> {
                    keyVaultStore.delete(entry.getKey());
                    log.info("Applied DELETE: {}", entry.getKey());
                }
                case NOOP -> {
                    log.debug("Applied NOOP entry at index {}", entry.getIndex());
                }
            }
        } catch (Exception e) {
            log.error("Error applying command: {}", entry, e);
            throw e;
        } finally {
            long duration = System.currentTimeMillis() - startTime;
            metricsCollector.recordStateApply(entry, duration);
        }
    }
}
