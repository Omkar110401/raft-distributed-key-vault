package com.omkar.distributed_key_vault.vault;

import org.springframework.stereotype.Component;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

@Component
public class KeyVaultStore {
    private final ConcurrentHashMap<String, String> store=new ConcurrentHashMap<>();

    public synchronized void put(String key, String value){
        store.put(key, value);
    }

    public String get(String key){
        return store.get(key);
    }

    public synchronized void delete(String key){
        store.remove(key);
    }

    public Map<String, String> getAll(){
        return new ConcurrentHashMap<>(store);
    }

    public boolean exists(String key){
        return store.containsKey(key);
    }

    public synchronized void clear(){
        store.clear();
    }

    public int size(){
        return store.size();
    }
}
