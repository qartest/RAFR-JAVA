package t.boev.work.config;

import org.springframework.boot.context.properties.ConfigurationProperties;

import java.time.Duration;

@ConfigurationProperties(prefix = "raft.timing")
public record Timing(Duration heartbeatInterval,
                     Duration electionTimeoutMin,
                     Duration electionTimeoutMax) {
}
