package com.omkar.distributed_key_vault.config;

import lombok.Getter;
import lombok.Setter;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.List;
import java.util.stream.Collectors;

@Getter
@Setter
@Configuration
@ConfigurationProperties(prefix = "node")
public class NodeConfig {
    private String nodeId;
    private List<NodeInfo> allNodes;
    private List<String> peers;

    @Getter
    @Setter
    public static class NodeInfo {
        private String id;
        private String url;
    }

    public void setAllNodes(List<NodeInfo> allNodes) {
        this.allNodes = allNodes;
        // Dynamically compute peers by excluding current node
        if (allNodes != null && nodeId != null) {
            this.peers = allNodes.stream()
                    .filter(node -> !node.getId().equals(nodeId))
                    .map(NodeInfo::getUrl)
                    .collect(Collectors.toList());
        }
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
        // Recompute peers if allNodes is already set
        if (allNodes != null) {
            setAllNodes(allNodes);
        }
    }
}
