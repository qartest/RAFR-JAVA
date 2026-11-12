package t.boev.work.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.util.List;

@ConfigurationProperties(prefix = "raft")
public record RaftProperties(int nodeId, List<NodeAddress> nodes, Timing timing) {

}
