package com.omkar.distributed_key_vault.vault;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

@Data
@NoArgsConstructor
@AllArgsConstructor
public class KeyValueRequest {
    private String key;
    private String value;
}
