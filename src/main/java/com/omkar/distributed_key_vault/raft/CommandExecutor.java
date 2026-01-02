package com.omkar.distributed_key_vault.raft;

import com.omkar.distributed_key_vault.vault.KeyVaultStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Executes commands on the state machine (Phase 3.2)
 * Applies log entries to KeyVaultStore
 * Ensures atomicity and idempotency
 */
@Component
public class CommandExecutor {
    
    private static final Logger logger = LoggerFactory.getLogger(CommandExecutor.class);
    
    @Autowired
    private KeyVaultStore keyVaultStore;
    
    /**
     * Execute a command from log entry on the state machine
     * Returns true if execution was successful
     */
    public boolean executeCommand(LogEntry entry) {
        if (entry == null) {
            return false;
        }
        
        try {
            // Skip NOOP entries
            if (entry.getCommandType() == CommandType.NOOP) {
                entry.setApplied(true);
                entry.setAppliedAt(System.currentTimeMillis());
                return true;
            }
            
            // Execute PUT command
            if (entry.getCommandType() == CommandType.PUT) {
                return executePUT(entry);
            }
            
            // Execute DELETE command
            if (entry.getCommandType() == CommandType.DELETE) {
                return executeDELETE(entry);
            }
            
            logger.warn("Unknown command type: {}", entry.getCommandType());
            return false;
            
        } catch (Exception e) {
            logger.error("Error executing command from entry {}: {}", entry.getIndex(), e.getMessage());
            return false;
        }
    }
    
    /**
     * Execute PUT command: write key-value pair
     */
    private boolean executePUT(LogEntry entry) {
        try {
            String key = entry.getKey();
            String value = entry.getValue();
            
            if (key == null || key.isEmpty()) {
                logger.warn("Invalid PUT command: key is null or empty");
                return false;
            }
            
            // Write to state machine
            keyVaultStore.put(key, value);
            
            // Mark as applied
            entry.setApplied(true);
            entry.setAppliedAt(System.currentTimeMillis());
            
            logger.debug("Applied PUT command: key={}, value={}", key, value);
            return true;
            
        } catch (Exception e) {
            logger.error("Error executing PUT command: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Execute DELETE command: remove key-value pair
     */
    private boolean executeDELETE(LogEntry entry) {
        try {
            String key = entry.getKey();
            
            if (key == null || key.isEmpty()) {
                logger.warn("Invalid DELETE command: key is null or empty");
                return false;
            }
            
            // Remove from state machine
            keyVaultStore.delete(key);
            
            // Mark as applied
            entry.setApplied(true);
            entry.setAppliedAt(System.currentTimeMillis());
            
            logger.debug("Applied DELETE command: key={}", key);
            return true;
            
        } catch (Exception e) {
            logger.error("Error executing DELETE command: {}", e.getMessage());
            return false;
        }
    }
    
    /**
     * Validate command before execution
     * Ensures command is well-formed
     */
    public boolean validateCommand(LogEntry entry) {
        if (entry == null) {
            return false;
        }
        
        if (entry.getCommandType() == null) {
            logger.warn("Command type is null");
            return false;
        }
        
        // NOOP is always valid
        if (entry.getCommandType() == CommandType.NOOP) {
            return true;
        }
        
        // PUT and DELETE require key
        if (entry.getKey() == null || entry.getKey().isEmpty()) {
            logger.warn("Command requires key, but it's null or empty");
            return false;
        }
        
        // PUT requires value
        if (entry.getCommandType() == CommandType.PUT && 
            (entry.getValue() == null)) {
            logger.warn("PUT command requires value");
            return false;
        }
        
        return true;
    }
    
    /**
     * Get current state machine value for a key
     */
    public String getValue(String key) {
        if (key == null || key.isEmpty()) {
            return null;
        }
        return keyVaultStore.get(key);
    }
    
    /**
     * Check if entry is already applied
     */
    public boolean isApplied(LogEntry entry) {
        return entry != null && entry.isApplied();
    }
}
